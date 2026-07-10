"""Dispatch per-job DI dispatch.

:func:`dispatch_one_job` composes :func:`build_actor_scope` with
:func:`consume_one_job` so the worker's per-job dispatch path is DI-aware:
actors with ``Annotated[T, Scope.X]`` parameters receive resolved instances
at dispatch time, scoped to their effective scope, with TRANSIENT teardown
running per invocation.

The dispatch SQL constants and the ``dispatch_batch`` asyncpg helper live
in :mod:`taskq.backend._dispatch_sql` (backend layer).  This module
imports them from there since worker → backend is the correct layer
direction.
"""

import asyncio
import time
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, build_actor_scope
from taskq.actor import ActorRef
from taskq.backend._protocol import Backend, JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import (
    ConsumedOutcome,
    bind_job_context,
    get_logger,
    record_consumed_message,
    record_process_duration,
    safe_start_span,
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.retry import ActorConfigLike
from taskq.worker._consumer import consume_one_job
from taskq.worker._handlers import (
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,  # pyright: ignore[reportPrivateUsage]  # Why: dispatch_one_job's direct-call path for _handle_generic_exception needs the same infra guard as _run_terminal_path to prevent false terminal Redis publishes and exception mislabeling.
    _handle_generic_exception,  # pyright: ignore[reportPrivateUsage]  # Why: _handle_generic_exception implements the same exception→retry/fail routing as consume_one_job's inner handlers; dispatch_one_job needs it for DI-resolution failures that escape consume_one_job's own try/except.
    _log_terminal_write_failed,  # pyright: ignore[reportPrivateUsage]  # Why: same rationale as _TERMINAL_WRITE_INFRA_EXCEPTIONS above.
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

if TYPE_CHECKING:
    import redis.asyncio as redis_async

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


def _to_consumed_outcome(attempt_outcome: str) -> ConsumedOutcome:
    """Map an AttemptOutcome to the semconv-valid ConsumedOutcome label set.

    ``AttemptOutcome`` includes ``"scheduled"`` for snooze/retry/reservation-denial
    which is not in the instrument 2 valid set ``{succeeded, failed, cancelled,
    abandoned}``.  From the consumer's perspective the job was released back to
    the queue without being completed — semantically ``"abandoned"``.
    """
    if attempt_outcome == "scheduled":
        return "abandoned"
    return attempt_outcome  # type: ignore[return-value]  # Why: AttemptOutcome is Literal["succeeded","failed","cancelled","scheduled"]; after the "scheduled" branch the remaining values are exactly the ConsumedOutcome union but pyright cannot narrow across the return-site coercion


async def dispatch_one_job(
    *,
    backend: Backend,
    deps: WorkerDeps,
    job: JobRow,
    worker_id: UUID,
    registry: ProviderRegistry,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
    actor_ref: ActorRef[BaseModel, BaseModel | None],
    actor_config: ActorConfigLike,
    clock: Clock,
    active_jobs: ActiveJobRegistry | None = None,
    max_retry_backoff: timedelta = timedelta(hours=24),
    logger_arg: structlog.stdlib.BoundLogger | None = None,
    enqueuer: SubJobEnqueuer,
) -> None:
    """Dispatch one job through the DI-resolved actor scope.

    1. Create the CONSUMER span with link to the PRODUCER span.
    2. Validate the payload against actor_ref's payload schema.
    3. Build the interim JobContext with the CONSUMER span.
    4. Open build_actor_scope to resolve DI kwargs.
    5. Hand the resolved kwargs to consume_one_job via a run_actor
       closure that injects them.
    6. TRANSIENT scope is closed on exit (regardless of outcome).
    7. Record consumer-path metrics outside the span body.
    """
    link_ctx: trace.SpanContext | None = None
    if job.trace_id and job.span_id:
        try:
            link_ctx = trace.SpanContext(
                trace_id=int(job.trace_id, 16),
                span_id=int(job.span_id, 16),
                is_remote=True,
                trace_flags=trace.TraceFlags(0x01),
            )
        except (ValueError, OverflowError):
            logger.warning(
                "otel-link-skipped",
                reason="malformed_trace_id",
                job_id=job.id,
            )
    links = [trace.Link(link_ctx)] if link_ctx else []

    batch_id: str = ""
    if job.metadata:
        raw_bid = job.metadata.get("batch_id")
        if raw_bid is not None:
            batch_id = str(raw_bid)

    consumer_attrs: dict[str, str | int] = {
        "messaging.system": "taskq",
        "messaging.destination.name": job.queue,
        "messaging.operation.type": "process",
        "messaging.message.id": str(job.id),
        "messaging.consumer.group.name": deps.settings.worker_group,
        "taskq.actor": job.actor,
        "taskq.attempt": job.attempt,
        "taskq.identity_key": job.identity_key or "",
        "taskq.batch_id": batch_id,
    }

    dispatch_log = logger_arg if logger_arg is not None else logger

    t0 = time.monotonic()
    outcome: str = "failed"

    try:
        with safe_start_span(
            f"process {job.actor}",
            kind=SpanKind.CONSUMER,
            attributes=consumer_attrs,
            links=links,
        ) as consumer_span:
            try:
                validated_payload = actor_ref.payload_type.model_validate(job.payload)

                span_ctx = consumer_span.get_span_context()
                dispatch_trace_id: str = ""
                if span_ctx.is_valid:
                    dispatch_trace_id = format(span_ctx.trace_id, "032x")

                interim_ctx: JobContext[BaseModel] = JobContext(
                    job_id=job.id,
                    actor=job.actor,
                    queue=job.queue,
                    attempt=job.attempt,
                    worker_id=worker_id,
                    payload=validated_payload,
                    jobs=enqueuer,
                    log=bind_job_context(
                        dispatch_log,
                        job_id=job.id,
                        actor=job.actor,
                        queue=job.queue,
                        attempt=job.attempt,
                        identity_key=job.identity_key,
                        trace_id=dispatch_trace_id,
                        batch_id=batch_id or None,
                    ),
                    span=consumer_span
                    if not isinstance(consumer_span, trace.NonRecordingSpan)
                    else None,
                )
                passthrough_kwargs: dict[str, object] = {
                    "payload": validated_payload,
                    "ctx": interim_ctx,
                }

                async with build_actor_scope(
                    registry=registry,
                    process_scope=process_scope,
                    thread_scope=thread_scope,
                    loop_scope=loop_scope,
                    actor_func=actor_ref.fn,  # type: ignore[arg-type]  # Why: actor_ref.fn is Callable[..., object] (covers both sync and async); build_actor_scope expects Callable[..., Awaitable[object]] for DI resolution but never calls the function — sync-vs-async dispatch is handled later via actor_ref.is_sync
                    actor_name=actor_ref.name,
                    passthrough_kwargs=passthrough_kwargs,
                ) as resolved:

                    async def run_actor_with_di(
                        job_row: JobRow,
                        ctx_arg: JobContext[BaseModel],
                    ) -> object:
                        del job_row
                        actor_kwargs: dict[str, object] = {
                            **resolved.di_kwargs,
                            "payload": ctx_arg.payload,
                        }
                        if actor_ref.wants_ctx:
                            actor_kwargs["ctx"] = ctx_arg
                        if actor_ref.is_sync:
                            return await asyncio.to_thread(actor_ref.fn, **actor_kwargs)
                        return await actor_ref.fn(**actor_kwargs)  # type: ignore[no-any-return]  # Why: actor_ref.fn is typed Callable[..., object]; runtime result is R.

                    loop_conn: asyncpg.Connection | None = None
                    raw_conn = loop_scope.resolved_cache().get(asyncpg.Connection)
                    if isinstance(raw_conn, asyncpg.Connection):
                        loop_conn = raw_conn  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg.Connection is generic (Connection[Record]); resolved_cache returns Mapping[type, object] so isinstance narrows to Connection[Unknown] — the record type is irrelevant for the transaction lifecycle.

                    rl_registry: RateLimitRegistry | None = None
                    raw_rl = loop_scope.resolved_cache().get(RateLimitRegistry)
                    if isinstance(raw_rl, RateLimitRegistry):
                        rl_registry = raw_rl

                    redis_client: redis_async.Redis | None = None
                    try:
                        import redis.asyncio as _redis_mod  # type: ignore[no-redef]  # Why: runtime import for DI lookup; TYPE_CHECKING import is for annotations only

                        raw_redis = loop_scope.resolved_cache().get(_redis_mod.Redis)
                        if isinstance(raw_redis, _redis_mod.Redis):
                            redis_client = raw_redis
                    except ImportError:
                        pass

                    result = await consume_one_job(
                        backend,
                        job,
                        worker_id,
                        deps=deps,
                        run_actor=run_actor_with_di,
                        actor_config=actor_config,
                        payload_type=actor_ref.payload_type,
                        clock=clock,
                        logger=logger_arg,
                        max_retry_backoff=max_retry_backoff,
                        active_jobs=active_jobs,
                        enqueuer=enqueuer,
                        loop_conn=loop_conn,
                        validated_payload=validated_payload,
                        rate_limit_registry=rl_registry,
                        rate_limits=actor_ref.rate_limits,
                        reservations=actor_ref.reservations,
                        redis_client=redis_client,
                        worker_pool=deps.worker_pool,
                        settings=deps.settings,
                    )
                    outcome = result

                if outcome == "succeeded":
                    consumer_span.set_status(StatusCode.OK)
                elif outcome == "failed":
                    consumer_span.set_status(StatusCode.ERROR)

            except asyncio.CancelledError:
                outcome = "cancelled"
                consumer_span.set_status(StatusCode.ERROR, "cancelled")
                raise
            except Exception as exc:
                outcome = "failed"
                consumer_span.set_status(StatusCode.ERROR)
                handler_log = bind_job_context(
                    dispatch_log,
                    job_id=job.id,
                    actor=job.actor,
                    queue=job.queue,
                    attempt=job.attempt,
                    identity_key=job.identity_key,
                    trace_id="",
                )
                try:
                    await _handle_generic_exception(
                        backend,
                        job,
                        worker_id,
                        exc,
                        actor_config,
                        clock,
                        max_retry_backoff,
                        consumer_span,
                        handler_log,
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    _log_terminal_write_failed(handler_log, job, exc, infra_exc)
    finally:
        elapsed = time.monotonic() - t0
        record_consumed_message(job.actor, job.queue, outcome=_to_consumed_outcome(outcome))
        record_process_duration(job.actor, job.queue, elapsed)
