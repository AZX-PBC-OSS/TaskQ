"""Per-job consumer exception handling and terminal-state writes.

Contains :func:`consume_one_job` that wraps the full exception-handling
sequence and helpers for the transactional and autonomous paths.  Every
backend write is wrapped in ``asyncio.shield`` so cancellation during
shutdown phase 2 cannot strand the row in ``running``.

The individual terminal exception handlers (timeout, snooze, retry_after,
reservation denied, generic) live in :mod:`taskq.worker._handlers`.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from pydantic import BaseModel

from taskq._json import dumps as _json_dumps
from taskq.backend._protocol import (
    Backend,
    CancelPhase,
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import MAX_RESULT_BYTES
from taskq.context import JobContext
from taskq.exceptions import (
    ReservationUnavailable,
    ResultTooLarge,
    RetryAfter,
    Snooze,
    SubEnqueueError,
)
from taskq.obs import (
    ErrorReporter,
    bind_job_context,
    get_logger,
    log_state_change,
    safe_start_span,
)
from taskq.progress._buffer import (
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
    _terminal_seq_and_state,
)
from taskq.progress._flush import _flush_buffer, _flush_buffer_immediate
from taskq.progress._publish import _publish_state_change_event
from taskq.ratelimit.composition import AcquiredResource
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.retry import (
    ActorConfigLike,
    invoke_on_success,
)
from taskq.settings import WorkerSettings
from taskq.worker._handlers import (
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,
    _dispatch_exception,
    _handle_reservation_class_denied,
    _log_terminal_write_failed,
    _TerminalWriteFailed,
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

if TYPE_CHECKING:
    import redis.asyncio as redis_async

type AttemptOutcome = Literal[
    "succeeded",
    "failed",
    "cancelled",
    "scheduled",
]

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_OK = object()


def _serialize_result(result: object) -> dict[str, object] | None:
    """Serialize an actor return value into a JSON-storable dict.

    ``BaseModel`` results are dumped via ``model_dump(mode="json")``;
    ``dict`` results are passed through as-is (the actor contract
    guarantees ``dict[str, object]``); all other types return ``None``
    (no result stored).
    """
    if isinstance(result, BaseModel):
        return result.model_dump(mode="json")
    if isinstance(result, dict):
        return result  # pyright: ignore[reportUnknownVariableType]  # Why: run_actor returns Awaitable[object]; isinstance narrows to dict[Unknown, Unknown] which is not assignable to dict[str, object]. At runtime the actor contract guarantees dict[str, object].
    return None


def _check_result_size(data: dict[str, object] | None) -> int:
    """Return the serialised byte size of *data*, raising if it exceeds the cap.

    Returns ``0`` when *data* is ``None`` (no result to store).  Raises
    :class:`ResultTooLarge` when the serialised size exceeds
    :data:`~taskq.constants.MAX_RESULT_BYTES`.
    """
    if data is None:
        return 0
    result_bytes = len(_json_dumps(data))
    if result_bytes > MAX_RESULT_BYTES:
        raise ResultTooLarge(
            f"result size {result_bytes} bytes exceeds {MAX_RESULT_BYTES} byte cap"
        )
    return result_bytes


async def _run_terminal_path(  # pyright: ignore[reportUnusedFunction]  # Why: called by _dispatch_exception in _handlers.py via lazy import
    *,
    job: JobRow,
    worker_id: UUID,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    worker_pool: "asyncpg.Pool | None",
    settings: WorkerSettings | None,
    redis_client: "redis_async.Redis | None",
    handler: Callable[..., Awaitable[None]],
    handler_args: tuple[object, ...],
    handler_kwargs: dict[str, object],
    status: str,
    terminal: bool,
    outcome: AttemptOutcome,
    job_exc: BaseException | None = None,
) -> AttemptOutcome:
    """Pre-terminal flush, handler call, dirty reset, publish, and outcome.

    Consolidates the identical 15-line block that was copy-pasted across
    ten exception handlers in ``consume_one_job`` and
    ``_consume_transactional``.

    Infra failures (DB/network) raised by *handler*'s terminal write are
    caught here — not re-dispatched into generic exception handling, which
    would misclassify the infra error as the actor's failure (*job_exc*).
    The job row stays ``running`` and is reclaimed via lock-lease expiry.
    """
    if progress_buffers is not None and worker_pool is not None and settings is not None:
        await asyncio.shield(
            _flush_buffer_immediate(
                worker_pool,
                settings.schema_name,
                job.id,
                worker_id,
                progress_buffers,
            )
        )
        _pbuf = progress_buffers.get(job.id)
        _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)
    else:
        _pseq, _pstate = _seq_and_state_after_flush_attempt(
            progress_buffers.get(job.id) if progress_buffers is not None else None
        )
    try:
        await handler(
            *handler_args,
            progress_seq=_pseq,
            progress_state=_pstate,
            **handler_kwargs,
        )
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
        _log_terminal_write_failed(
            _log,
            job,
            job_exc if job_exc is not None else infra_exc,
            infra_exc,
        )
        return outcome
    if progress_buffers is not None:
        _buf = progress_buffers.get(job.id)
        if _buf is not None:
            _buf.dirty = False
    if redis_client is not None and settings is not None:
        await _publish_state_change_event(
            redis_client,
            settings,
            job.id,
            job.actor,
            progress_buffers,
            status=status,
            terminal=terminal,
            _override_seq=_pseq,
            _override_pending_state=_pstate,
        )
    return outcome


async def consume_one_job(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    *,
    deps: WorkerDeps | None = None,
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    actor_config: ActorConfigLike,
    payload_type: type[BaseModel],
    clock: Clock,
    logger: structlog.stdlib.BoundLogger | None = None,
    max_retry_backoff: timedelta = timedelta(hours=24),
    active_jobs: ActiveJobRegistry | None = None,
    enqueuer: SubJobEnqueuer | None = None,
    loop_conn: asyncpg.Connection | None = None,
    validated_payload: BaseModel | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
    rate_limits: list[str] | None = None,
    reservations: list[str | KeyedReservationRef] | None = None,
    redis_client: "redis_async.Redis | None" = None,
    worker_pool: asyncpg.Pool | None = None,
    settings: WorkerSettings | None = None,
    error_reporter: ErrorReporter | None = None,
) -> AttemptOutcome:
    """Run one job's full  try/except sequence.

    The worker's dispatch_one_job helper wraps calls to this function
    with build_actor_scope to provide per-invocation TRANSIENT scope
    and DI resolution for the actor's declared dependencies.

    Returns the job's terminal outcome for span status and metric
    recording by the caller (``dispatch_one_job``).

    ``payload_type`` is the actor's payload model — the consumer
    re-validates the raw ``dict[str, object]`` row payload against this
    model so the :class:`JobContext` handed to the actor carries a
    typed, validated :class:`pydantic.BaseModel` instance. The bound is
    ``BaseModel`` here (the registry is heterogeneous); per-actor ``P``
    flows from the call site that selected ``payload_type``.

    ``enqueuer`` is the per-loop SubJobEnqueuer constructed in ``_main``
    after ``loop_scope.bootstrap()``. When provided, the live
    JobContext uses this enqueuer so sub-enqueues are transactional.

    ``error_reporter`` is an optional :class:`~taskq.obs.ErrorReporter`
    invoked when a job reaches a terminal failure state (retry exhausted
    or non-retryable error).  When ``None``, no error reporting occurs.
    The reporter call is wrapped in a try/except — a failing reporter
    never crashes the worker.

    ``loop_conn`` is the resolved LOOP-scope asyncpg.Connection (or
    None when no LOOP-scope connection provider is registered). When
    present, the consumer opens a transaction on it for the success
    path and wraps the entire block in ``asyncio.shield`` per G8.

    Rate-limit / reservation acquire-release wrapping ( through
    ): when ``rate_limit_registry`` is provided and the actor
    declares ``rate_limits`` or ``reservations``,
    :meth:`RateLimitRegistry.acquire_for_actor` is called before the
    actor body.  On denial (``ReservationUnavailable``), the job is
    snoozed and the actor body is NOT invoked.  After the actor body
    completes (success, failure, cancellation, or shutdown),
    :meth:`RateLimitRegistry.release_for_actor` is called in the
    ``finally`` block.  Release is best-effort (not shielded) per
    "Cancellation and shutdown boundary".
    """
    log = logger if logger is not None else _log
    consumer_span = trace.get_current_span()

    trace_id: str = ""
    span_context = consumer_span.get_span_context()
    if span_context.is_valid:
        trace_id = format(span_context.trace_id, "032x")

    batch_id: str | None = None
    if job.metadata:
        raw_bid = job.metadata.get("batch_id")
        if raw_bid is not None:
            batch_id = str(raw_bid)

    job_log = bind_job_context(
        log,
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        identity_key=job.identity_key,
        trace_id=trace_id,
        batch_id=batch_id,
    )

    _rl_limits = rate_limits if rate_limits is not None else []
    _rl_reservations = reservations if reservations is not None else []
    _needs_acquire = bool(_rl_limits or _rl_reservations) and rate_limit_registry is not None

    acquired: list[AcquiredResource] = []

    if _needs_acquire and rate_limit_registry is not None:
        try:
            acquired = await rate_limit_registry.acquire_for_actor(
                rate_limits=_rl_limits,
                reservations=_rl_reservations,
                job_id=job.id,
                worker_id=worker_id,
                payload=job.payload,
                redis_client=redis_client,
                pg_pool=worker_pool,
                clock=clock,
                settings=settings,
            )
        except ReservationUnavailable as e:
            if e.source == "reservation":
                await _handle_reservation_class_denied(
                    backend,
                    job,
                    worker_id,
                    e,
                    consumer_span,
                    job_log,
                    actor_config,
                    awaiting_prefix="reservation:",
                    outcome="reservation_denied",
                    debug_event="consume-reservation-denied-noop",
                )
            else:
                await _handle_reservation_class_denied(
                    backend,
                    job,
                    worker_id,
                    e,
                    consumer_span,
                    job_log,
                    actor_config,
                    awaiting_prefix="rate_limit:",
                    outcome="rate_limit_denied",
                    debug_event="consume-rate-limit-denied-noop",
                )
            return "scheduled"

    # ── Buffer registration ────────────────────────────────────────────────
    _effective_pool = deps.worker_pool if deps is not None else worker_pool
    _effective_settings = deps.settings if deps is not None else settings
    _effective_redis = deps.redis_client if deps is not None else redis_client
    _progress_buffers = deps.progress_buffers if deps is not None else None
    _pending_publish_tasks = getattr(deps, "pending_publish_tasks", None)

    if _progress_buffers is not None:
        _buf = _ProgressBuffer(job_id=job.id, base_seq=job.progress_seq)
        _progress_buffers[job.id] = _buf

    try:
        validated_payload = (
            validated_payload
            if validated_payload is not None
            else payload_type.model_validate(job.payload)
        )

        live_enqueuer = (
            enqueuer
            if enqueuer is not None
            else SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=None,
                backend=backend,
            )
        )

        ctx: JobContext[BaseModel] = JobContext(
            job_id=job.id,
            actor=job.actor,
            queue=job.queue,
            attempt=job.attempt,
            worker_id=worker_id,
            payload=validated_payload,
            jobs=live_enqueuer,
            log=job_log,
            span=consumer_span if not isinstance(consumer_span, trace.NonRecordingSpan) else None,
            _progress_buffers=_progress_buffers,
            _redis_client=_effective_redis,
            _worker_settings=_effective_settings,
            _pending_publish_tasks=_pending_publish_tasks,
        )

        if active_jobs is not None:
            task = asyncio.current_task()
            assert task is not None
            await active_jobs.register(job.id, task, ctx)

        _completion: object = None

        _effective_start_to_close = (
            job.start_to_close
            if job.start_to_close is not None
            else getattr(_effective_settings, "default_start_to_close", None)
        )
        timeout: float | None = (
            _effective_start_to_close.total_seconds()
            if _effective_start_to_close is not None
            else None
        )

        consumer_span.add_event(
            "lifecycle.running",
            attributes={"from_state": "pending", "to_state": "running"},
        )

        if _effective_redis is not None and _effective_settings is not None:
            await _publish_state_change_event(
                _effective_redis,
                _effective_settings,
                job.id,
                job.actor,
                _progress_buffers,
                status="running",
                terminal=False,
            )

        try:
            with safe_start_span(
                f"attempt.{job.attempt}",
                kind=SpanKind.INTERNAL,
            ):
                if loop_conn is not None:
                    tx_outcome = await _consume_transactional(
                        backend,
                        job,
                        worker_id,
                        ctx,
                        live_enqueuer,
                        loop_conn,
                        run_actor,
                        actor_config,
                        clock,
                        timeout,
                        max_retry_backoff,
                        active_jobs,
                        consumer_span,
                        job_log,
                        progress_buffers=_progress_buffers,
                        redis_client=_effective_redis,
                        settings=_effective_settings,
                        worker_pool=_effective_pool,
                        error_reporter=error_reporter,
                    )
                    _completion = _OK if tx_outcome == "succeeded" else None
                    if tx_outcome == "succeeded":
                        consumer_span.add_event(
                            "lifecycle.succeeded",
                            attributes={"from_state": "running", "to_state": "succeeded"},
                        )
                        log_state_change(ctx.log, from_state="running", to_state="succeeded")
                        return "succeeded"
                    return tx_outcome
                else:
                    await _consume_autonomous(
                        backend,
                        job,
                        worker_id,
                        ctx,
                        run_actor,
                        timeout,
                        active_jobs,
                        job_log,
                        actor_config,
                        deps=deps,
                        progress_buffers=_progress_buffers,
                        redis_client=_effective_redis,
                        settings=_effective_settings,
                        worker_pool=_effective_pool,
                    )
                    consumer_span.add_event(
                        "lifecycle.succeeded",
                        attributes={"from_state": "running", "to_state": "succeeded"},
                    )
                    log_state_change(ctx.log, from_state="running", to_state="succeeded")
                    return "succeeded"

        except asyncio.CancelledError:
            if _completion is _OK:
                raise
            if loop_conn is not None:
                live_enqueuer.discard_buffer()
            if active_jobs is not None:
                entry = active_jobs.get(job.id)
                if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                    raise
            consumer_span.add_event(
                "lifecycle.cancelled",
                attributes={"from_state": "running", "to_state": "cancelled"},
            )
            log_state_change(ctx.log, from_state="running", to_state="cancelled")
            _cancel_buf = (
                _progress_buffers.pop(job.id, None) if _progress_buffers is not None else None
            )
            _cancel_seq, _cancel_state = _terminal_seq_and_state(_cancel_buf)
            await asyncio.shield(
                backend.mark_cancelled(
                    job.id,
                    worker_id,
                    progress_seq=_cancel_seq,
                    progress_state=_cancel_state
                    if _cancel_buf is not None and _cancel_buf.dirty
                    else None,
                )
            )
            if _effective_redis is not None and _effective_settings is not None:
                await _publish_state_change_event(
                    _effective_redis,
                    _effective_settings,
                    job.id,
                    job.actor,
                    None,
                    status="cancelled",
                    terminal=True,
                    _override_seq=_cancel_seq,
                    _override_pending_state=_cancel_state,
                )
            raise

        except _TerminalWriteFailed:
            # Success-path terminal write failed with an infra error.
            # Already logged via _log_terminal_write_failed inside the
            # success path.  The job stays ``running`` — lock-lease expiry
            # reclaims it.  Do NOT re-dispatch into _handle_generic_exception
            # (that would mislabel the infra error as the actor's failure).
            return "failed"

        except (
            TimeoutError,
            Snooze,
            RetryAfter,
            ReservationUnavailable,
            ResultTooLarge,
            Exception,
        ) as e:
            return await _dispatch_exception(
                e,
                backend=backend,
                job=job,
                worker_id=worker_id,
                actor_config=actor_config,
                clock=clock,
                max_retry_backoff=max_retry_backoff,
                consumer_span=consumer_span,
                log=job_log,
                progress_buffers=_progress_buffers,
                worker_pool=_effective_pool,
                settings=_effective_settings,
                redis_client=_effective_redis,
                error_reporter=error_reporter,
            )

        finally:
            # Best-effort crash flush: ensures partial progress_state reaches PG
            # even when the actor raises unexpectedly ().
            if (
                _progress_buffers is not None
                and _effective_pool is not None
                and _effective_settings is not None
            ):
                _crash_buf = _progress_buffers.pop(job.id, None)
                if _crash_buf is not None and _crash_buf.dirty:
                    await asyncio.shield(
                        _flush_buffer(
                            _effective_pool,
                            _effective_settings.schema_name,
                            job.id,
                            worker_id,
                            _crash_buf,
                            _progress_buffers,
                        )
                    )
            elif _progress_buffers is not None:
                _progress_buffers.pop(job.id, None)

            if active_jobs is not None:
                await active_jobs.deregister(job.id)

    finally:
        if acquired and rate_limit_registry is not None:
            try:
                await asyncio.shield(
                    rate_limit_registry.release_for_actor(acquired, pg_pool=worker_pool)
                )
            except Exception:
                _log.warning(
                    "rate_limit_release_failed",
                    job_id=job.id,
                )


async def _consume_transactional(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    ctx: JobContext[BaseModel],
    enqueuer: SubJobEnqueuer,
    loop_conn: asyncpg.Connection,
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    actor_config: ActorConfigLike,
    clock: Clock,
    timeout: float | None,
    max_retry_backoff: timedelta,
    active_jobs: ActiveJobRegistry | None,
    consumer_span: trace.Span,
    log: structlog.stdlib.BoundLogger,
    *,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None" = None,
    redis_client: "redis_async.Redis | None" = None,
    settings: WorkerSettings | None = None,
    worker_pool: asyncpg.Pool | None = None,
    error_reporter: ErrorReporter | None = None,
) -> AttemptOutcome:
    """Transactional success/failure path when a LOOP-scope conn is available.

    Opens a transaction, runs the actor inside it, commits on success
    (shielded), and routes exceptions to the appropriate handler with
    ``discard_buffer()`` called before each terminal write.

    Returns the job outcome — ``"succeeded"`` on successful commit,
    ``"failed"`` or ``"scheduled"`` when an exception was handled
    internally.
    """
    completion: object = None
    _tx_result: object = None

    async def _run_actor_in_tx() -> object:
        nonlocal completion
        nonlocal _tx_result
        _preserved_exc: Snooze | RetryAfter | None = None
        _re_enqueue_list: list[EnqueueArgs] = []

        async with loop_conn.transaction():
            await loop_conn.execute("SAVEPOINT _tq_actor")
            result: object = None
            try:
                result = await asyncio.wait_for(
                    asyncio.shield(run_actor(job, ctx)), timeout=timeout
                )
                if active_jobs is not None:
                    entry = active_jobs.get(job.id)
                    if entry is not None and entry.cancel_phase >= CancelPhase.COOPERATIVE:
                        raise asyncio.CancelledError()
                if (
                    progress_buffers is not None
                    and worker_pool is not None
                    and settings is not None
                ):
                    await _flush_buffer_immediate(
                        worker_pool,
                        settings.schema_name,
                        job.id,
                        worker_id,
                        progress_buffers,
                    )
                _pbuf = progress_buffers.get(job.id) if progress_buffers is not None else None
                _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)
                result_dict = _serialize_result(result)
                _check_result_size(result_dict)
                try:
                    await backend.mark_succeeded_with_conn(
                        loop_conn,
                        job.id,
                        worker_id,
                        result_dict,
                        progress_seq=_pseq,
                        progress_state=_pstate,
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    _log_terminal_write_failed(log, job, None, infra_exc)
                    raise _TerminalWriteFailed(infra_exc) from infra_exc
                if _pbuf is not None:
                    _pbuf.dirty = False
                await loop_conn.execute("RELEASE SAVEPOINT _tq_actor")
            except (Snooze, RetryAfter) as exc:
                _preserved_exc = exc
                try:
                    await loop_conn.execute("ROLLBACK TO SAVEPOINT _tq_actor")
                except Exception:
                    log.warning(
                        "savepoint_rollback_failed",
                        kind="savepoint_rollback_failed",
                        job_id=job.id,
                    )
                _re_enqueue_list = enqueuer.drain_for_re_enqueue()

        if _preserved_exc is not None:
            _re_enqueue_failures: list[str] = []
            for args in _re_enqueue_list:
                try:
                    await backend.enqueue(args)
                except Exception as exc:
                    log.warning(
                        "sub_enqueue_re_enqueue_error",
                        kind="sub_enqueue_re_enqueue_error",
                        job_id=args.id,
                        message=str(exc),
                    )
                    _re_enqueue_failures.append(f"{args.id}: {exc}")
            if _re_enqueue_failures:
                raise RuntimeError(
                    f"re_enqueue_failed: {', '.join(_re_enqueue_failures)}"
                ) from _preserved_exc
            raise _preserved_exc
        try:
            await enqueuer.flush_buffer()
        except SubEnqueueError as sub_err:
            log.error(
                "sub_enqueue_flush_failed",
                kind="sub_enqueue_flush_failed",
                job_id=job.id,
                failed_count=len(sub_err.failed_items),
                failed_job_ids=[str(args.id) for args, _ in sub_err.failed_items],
            )
        completion = _OK
        _tx_result = result
        return _OK

    try:
        await asyncio.shield(_run_actor_in_tx())
        await invoke_on_success(
            actor_config.on_success,
            job,
            _tx_result,
            actor_config.on_success_timeout,
            log=log,
        )
        if redis_client is not None and settings is not None:
            _tx_pbuf = progress_buffers.get(job.id) if progress_buffers is not None else None
            _tx_pseq, _tx_pstate = _seq_and_state_after_flush_attempt(_tx_pbuf)
            await _publish_state_change_event(
                redis_client,
                settings,
                job.id,
                job.actor,
                progress_buffers,
                status="succeeded",
                terminal=True,
                _override_seq=_tx_pseq,
                _override_pending_state=_tx_pstate,
            )
        return "succeeded"
    except asyncio.CancelledError:
        # Why: asyncio.shield decouples outer cancellation from the
        # inner task. If the inner task already completed successfully
        # (commit happened), do NOT route to mark_cancelled — that would
        # mark a committed job as cancelled, violating //.
        if completion is _OK:
            raise
        # Why: not-yet-committed — transaction auto-rolled back by
        # asyncpg's transaction context manager on CancelledError.
        # Fall through to the outer CancelledError handler which calls
        # discard_buffer + mark_cancelled + raise.
        raise

    except (
        TimeoutError,
        Snooze,
        RetryAfter,
        ReservationUnavailable,
        ResultTooLarge,
        Exception,
    ) as e:
        return await _dispatch_exception(
            e,
            backend=backend,
            job=job,
            worker_id=worker_id,
            actor_config=actor_config,
            clock=clock,
            max_retry_backoff=max_retry_backoff,
            consumer_span=consumer_span,
            log=log,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            pre_handler=enqueuer.discard_buffer,
            error_reporter=error_reporter,
        )


async def _consume_autonomous(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    ctx: JobContext[BaseModel],
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    timeout: float | None,
    active_jobs: ActiveJobRegistry | None,
    log: structlog.stdlib.BoundLogger,
    actor_config: ActorConfigLike,
    *,
    deps: WorkerDeps | None = None,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None" = None,
    redis_client: "redis_async.Redis | None" = None,
    settings: WorkerSettings | None = None,
    worker_pool: asyncpg.Pool | None = None,
) -> None:
    """Autonomous success path — no LOOP-scope connection."""
    _auto_redis = (
        redis_client
        if redis_client is not None
        else (deps.redis_client if deps is not None else None)
    )
    _auto_settings = (
        settings if settings is not None else (deps.settings if deps is not None else None)
    )
    _auto_pool = (
        worker_pool if worker_pool is not None else (deps.worker_pool if deps is not None else None)
    )

    result = await asyncio.wait_for(run_actor(job, ctx), timeout=timeout)

    if active_jobs is not None:
        entry = active_jobs.get(job.id)
        if entry is not None and entry.cancel_phase >= CancelPhase.COOPERATIVE:
            _cancel_buf = (
                progress_buffers.pop(job.id, None) if progress_buffers is not None else None
            )
            _cancel_seq, _cancel_state = _terminal_seq_and_state(_cancel_buf)
            await asyncio.shield(
                backend.mark_cancelled(
                    job.id,
                    worker_id,
                    progress_seq=_cancel_seq,
                    progress_state=_cancel_state
                    if _cancel_buf is not None and _cancel_buf.dirty
                    else None,
                )
            )
            if _cancel_buf is not None:
                _cancel_buf.dirty = False
            if _auto_redis is not None and _auto_settings is not None:
                await _publish_state_change_event(
                    _auto_redis,
                    _auto_settings,
                    job.id,
                    job.actor,
                    None,
                    status="cancelled",
                    terminal=True,
                    _override_seq=_cancel_seq,
                    _override_pending_state=_cancel_state,
                )
            return

    if progress_buffers is not None and _auto_pool is not None and _auto_settings is not None:
        await asyncio.shield(
            _flush_buffer_immediate(
                _auto_pool,
                _auto_settings.schema_name,
                job.id,
                worker_id,
                progress_buffers,
            )
        )
        _pbuf = progress_buffers.get(job.id)
        _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)
    else:
        _pbuf = progress_buffers.get(job.id) if progress_buffers is not None else None
        _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)

    result_dict = _serialize_result(result)
    _check_result_size(result_dict)
    try:
        await asyncio.shield(
            backend.mark_succeeded(
                job.id,
                worker_id,
                result_dict,
                progress_seq=_pseq,
                progress_state=_pstate,
            )
        )
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
        _log_terminal_write_failed(log, job, None, infra_exc)
        raise _TerminalWriteFailed(infra_exc) from infra_exc
    if _pbuf is not None:
        _pbuf.dirty = False
    await invoke_on_success(
        actor_config.on_success,
        job,
        result,
        actor_config.on_success_timeout,
        log=log,
    )
    if _auto_redis is not None and _auto_settings is not None:
        await _publish_state_change_event(
            _auto_redis,
            _auto_settings,
            job.id,
            job.actor,
            progress_buffers,
            status="succeeded",
            terminal=True,
            _override_seq=_pseq,
            _override_pending_state=_pstate,
        )
