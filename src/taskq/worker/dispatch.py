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
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, build_actor_scope
from taskq._validation import validate_actor_payload
from taskq.actor import ActorRef
from taskq.backend._protocol import Backend, ConnLike, JobRow
from taskq.backend.clock import Clock
from taskq.batch import apply_batch_terminal_outcome
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import DEFAULT_MAX_RETRY_BACKOFF
from taskq.context import JobContext
from taskq.obs import (
    ConsumedOutcome,
    bind_job_context,
    get_logger,
    record_consumed_message,
    record_process_duration,
    record_slot_pool_acquire_failure,
    safe_start_span,
)
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry, queue_concurrency_reservation_name
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.retry import ActorConfigLike
from taskq.worker._bootstrap import (  # pyright: ignore[reportPrivateUsage]  # Why: _registered_connection_init_hook is the single reader of the registry half of the with_connection_init channel; bootstrap uses it at pool build and dispatch needs the same probe for pools bootstrap did not build. No cycle: _bootstrap does not import dispatch.
    _registered_connection_init_hook,
)
from taskq.worker._consumer import consume_one_job
from taskq.worker._handlers import (
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,  # pyright: ignore[reportPrivateUsage]  # Why: dispatch_one_job's direct-call path for _handle_generic_exception needs the same infra guard as _run_terminal_path to prevent false terminal Redis publishes and exception mislabeling.
    AttemptOutcome,
    _handle_generic_exception,  # pyright: ignore[reportPrivateUsage]  # Why: _handle_generic_exception implements the same exception→retry/fail routing as consume_one_job's inner handlers; dispatch_one_job needs it for DI-resolution failures that escape consume_one_job's own try/except.
    _log_terminal_write_failed,  # pyright: ignore[reportPrivateUsage]  # Why: same rationale as _TERMINAL_WRITE_INFRA_EXCEPTIONS above.
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import POOL_INFRA_EXCEPTIONS, WorkerDeps

if TYPE_CHECKING:
    import redis.asyncio as redis_async

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

# Why the shared pool-infra family (defined in taskq.worker.deps, the
# module that owns the pools): the acquire below runs no queries, so any
# member raised there is connect-time infrastructure, never a job
# outcome. Anything else propagates to the consumer loop's generic
# handler.


class SlotPoolAcquireError(Exception):
    """A consumer slot could not obtain a usable transaction connection.

    Infrastructure, not a job outcome: the job is already claimed and
    recovers by lock-lease expiry, so the consumer loop must not count
    this as a job failure. The bounded acquire (or slot-connection init
    hook) that raised is recorded and logged at the raise site; this type
    exists so the loop can route it around the failure accounting. The
    underlying cause stays chained (``__cause__``).
    """

    def __init__(self, *, acquire_timeout: float, detail: str | None = None) -> None:
        super().__init__(
            detail
            if detail is not None
            else f"could not acquire a slot-pool connection within {acquire_timeout}s"
        )


#: Marker set on a slot connection once the registration's declared init
#: hook has been applied to it by the dispatch repair path — the
#: exactly-once-per-physical-connection record for pools bootstrap did not
#: build (asyncpg connections are ``__slots__``-sealed with no read-back of
#: applied setup, so the marker is the only place the applied state can
#: live on an injected pool's connection).
_SLOT_CONN_INIT_APPLIED_ATTR: Final[str] = "taskq_slot_conn_init_applied"


def _to_consumed_outcome(attempt_outcome: str) -> ConsumedOutcome:
    """Map an AttemptOutcome to the semconv-valid ConsumedOutcome label set.

    ``AttemptOutcome`` includes ``"scheduled"`` for snooze/retry/reservation-denial
    and ``"noop"`` for a terminal write that matched nothing (the job moved
    underneath this worker), neither of which is in the instrument 2 valid set
    ``{succeeded, failed, cancelled, abandoned}``.  ``"noop`` outcomes never
    reach the consumed-message recorder — ``dispatch_one_job``'s finally
    block skips both job-outcome metrics for them (nothing was consumed;
    the re-dispatch records the real message and duration) — so this
    mapping exists for ``"scheduled"`` and as a defensive total map should
    any other caller pass a noop through.
    """
    if attempt_outcome in ("scheduled", "noop"):
        return "abandoned"
    return attempt_outcome  # type: ignore[return-value]  # Why: AttemptOutcome is Literal["succeeded","failed","cancelled","scheduled","noop"]; after the released-back-to-queue branch the remaining values are exactly the ConsumedOutcome union but pyright cannot narrow across the return-site coercion


def _effective_reservations(
    reservations: Sequence[str | KeyedReservationRef | ConcurrencyReservation],
    queue: str,
    rl_registry: RateLimitRegistry | None,
) -> Sequence[str | KeyedReservationRef | ConcurrencyReservation]:
    """Prepend the fleet-wide queue-cap reservation name, if one is registered.

    If the job's queue has a fleet-wide cap registered (set via the
    ``max_concurrent`` column on the queues table, read at worker startup),
    its reservation name is prepended to the actor-declared reservations so
    :meth:`RateLimitRegistry.acquire_for_actor` acquires a slot before the
    actor runs. This caps total concurrent jobs for this queue across all
    workers, not just this one. Transparent to actor code — no ``@actor``
    change needed.

    Hot-path notes: the membership test uses
    :meth:`RateLimitRegistry.has_reservation`, an O(1) lookup — NOT the
    ``reservations`` property, which defensively copies the whole dict and
    would make every job dispatch O(registry size). And when no cap is
    registered (the common case) the actor's own list is returned unchanged,
    so dispatch does no per-job list copying either.
    """
    if rl_registry is not None:
        queue_cap_name = queue_concurrency_reservation_name(queue)
        if rl_registry.has_reservation(queue_cap_name):
            return [queue_cap_name, *reservations]
    return reservations


async def _ensure_registered_init_on_slot_conn(
    conn: ConnLike,
    *,
    deps: WorkerDeps,
    registry: ProviderRegistry,
    acquire_timeout: float,
    job_id: UUID,
) -> None:
    """Carry the registration's declared init hook onto a slot connection the
    pool never applied it to — exactly once per physical connection.

    Bootstrap threads the hook declared on the LOOP-scope connection's
    factory registration (:func:`taskq.connections.with_connection_init`,
    or :func:`taskq.auth.make_dedicated_conn_factory`'s ``setup``) into the
    slot pool's ``init=``, so a bootstrap-built pool's connections carry it
    from connect time; ``deps.slot_pool_connection_init`` records that, and
    this function then returns on a single attribute read — the hook is
    never applied twice to one physical connection. A pool bootstrap did
    not open (an injected/foreign pool) leaves that record ``None`` while
    handing out bare connections: the actor's DI would resolve a connection
    missing the registered setup — codecs, session configuration — and
    silently diverge from what the application configured. For that shape
    the hook is applied here, once per physical connection, marked on the
    connection itself.

    Hot path: with no hook declared, or a pool that already carries it,
    this costs the attribute read plus the registry probe (dict lookups) —
    no await, no allocation, no wrapper around the connection.

    A hook failure is connect-time infrastructure, never a job outcome: the
    connection is terminated (never released back to serve a sibling slot
    half-configured — the same rule ``with_connection_init`` applies to the
    LOOP connection), the failure is logged, and the job recovers by
    lock-lease expiry via :class:`SlotPoolAcquireError`.
    """
    if deps.slot_pool_connection_init is not None:
        return
    hook = _registered_connection_init_hook(registry)
    if hook is None or getattr(conn, _SLOT_CONN_INIT_APPLIED_ATTR, False) is True:
        return
    # Why the cast: ConnLike is the object-typed runtime alias for
    # Connection | PoolConnectionProxy, and the hook's contract is the one
    # asyncpg's own init= receives — the proxy forwards it to the physical
    # connection. The marker setattr on the repair path targets injected
    # pools' connections (bootstrap-built pools never reach it); asyncpg's
    # __slots__-sealed Connection could not carry the marker, which is fine:
    # that shape always takes the deps-recorded early return above.
    target = cast(asyncpg.Connection, conn)
    try:
        await asyncio.wait_for(hook(target), timeout=acquire_timeout)
    except Exception as exc:
        target.terminate()
        logger.error(
            "slot-conn-init-hook-failed",
            kind="slot_conn_init_hook_failed",
            job_id=str(job_id),
            error_class=type(exc).__name__,
            error_message=str(exc),
        )
        raise SlotPoolAcquireError(
            acquire_timeout=acquire_timeout,
            detail=(
                "the registered connection's declared init hook failed on a "
                f"slot-pool connection ({type(exc).__name__}: {exc})"
            ),
        ) from exc
    setattr(conn, _SLOT_CONN_INIT_APPLIED_ATTR, True)


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
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
    logger_arg: structlog.stdlib.BoundLogger | None = None,
    enqueuer: SubJobEnqueuer,
) -> AttemptOutcome:
    """Dispatch one job through the DI-resolved actor scope.

    1. Acquire the job's transaction connection — from the worker's
       dedicated slot pool when one is open (the per-slot path), else
       the LOOP-scope connection, else none (autonomous consume). An
       acquire failure raises :class:`SlotPoolAcquireError` before any
       span or metric exists: infrastructure, not a job outcome. On the
       per-slot path the acquired connection also shadows the
       LOOP-registered ``asyncpg.Connection`` for the actor invocation
       (``loop_slot_values``), so the actor's own writes join this job's
       transaction and concurrent slots never share a connection.
    2. Create the CONSUMER span with link to the PRODUCER span.
    3. Validate the payload against actor_ref's payload schema.
    4. Build the interim JobContext with the CONSUMER span.
    5. Open build_actor_scope to resolve DI kwargs.
    6. Hand the resolved kwargs to consume_one_job via a run_actor
       closure that injects them.
    7. TRANSIENT scope is closed on exit (regardless of outcome).
    8. Record consumer-path metrics outside the span body.

    On the per-slot path the job's enqueuer is a per-job
    :class:`~taskq.client._enqueuer.SubJobEnqueuer` bound to the
    acquired connection, so transactional sub-enqueues and their
    flush/discard lifecycle are this job's alone.
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
                job_id=str(job.id),
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

    # ── The job's transaction connection ─────────────────────────────
    # When the worker runs a dedicated slot pool (bootstrap opens one
    # whenever a LOOP-scope connection is registered and
    # max_concurrency > 1), each job acquires its own connection for
    # the duration of the dispatch — one transaction per connection, so
    # concurrent slots can never nest savepoints on a shared one. The
    # acquire precedes the span/metrics block on purpose: a job that
    # cannot acquire is infrastructure, not a job outcome, so it must
    # produce no consumer span and no consumed-message record. The
    # slot connection is also what the actor receives: it shadows the
    # LOOP-scope cache for this actor invocation (loop_slot_values
    # below), so an actor's own writes join THIS job's transaction and
    # a registered LOOP-scope connection is never shared across
    # concurrent slots' actors (issue #116). The registered LOOP-scope
    # connection itself stays untouched — every non-actor reader
    # (bootstrap's activation check, the loop-level enqueuer's
    # provenance inference) still resolves it from the LOOP cache.
    async with AsyncExitStack() as conn_stack:
        job_enqueuer: SubJobEnqueuer = enqueuer
        transaction_conn: ConnLike | None = None
        actor_loop_slot_values: Mapping[type, object] | None = None
        if deps.slot_pool is not None:
            slot_pool = deps.slot_pool
            acquire_timeout = deps.settings.dispatcher_command_timeout
            try:
                acquired = await slot_pool.acquire(timeout=acquire_timeout)
            except POOL_INFRA_EXCEPTIONS as exc:
                record_slot_pool_acquire_failure()
                logger.error(
                    "slot-pool-acquire-failed",
                    kind="slot_pool_acquire_failed",
                    job_id=str(job.id),
                    pool="slot",
                    acquire_timeout=acquire_timeout,
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )
                raise SlotPoolAcquireError(acquire_timeout=acquire_timeout) from exc
            transaction_conn = acquired
            # Per-slot LOOP-scope semantics for the actor: the slot
            # connection shadows the LOOP-registered asyncpg.Connection
            # for this invocation (build_actor_scope wraps the LOOP
            # container in a LoopScopeSlotView). The actor's writes then
            # join this job's transaction on this connection, and two
            # concurrent slots' actors can never interleave operations
            # on one connection — asyncpg permits one operation per
            # connection, so the shared shape raised InterfaceError
            # inside healthy actors and burned their retry budget
            # (issue #116).
            actor_loop_slot_values = {asyncpg.Connection: transaction_conn}

            async def _release_slot_conn(
                # Why default-arg binding for pool/conn: the release must
                # return the connection to the pool that produced it even
                # if a credential reload swaps deps.slot_pool mid-dispatch.
                pool: asyncpg.Pool = slot_pool,
                conn: ConnLike = acquired,
            ) -> None:
                # Why custom release instead of the acquire context's
                # own __aexit__: external cancellation can detach the
                # job's transaction task (the consumer shields an
                # in-flight commit, so the task outlives the dispatch
                # call), and asyncpg's release-reset would then ROLL
                # BACK under that live transaction and hand the
                # connection to a sibling mid-flight — a false
                # `succeeded` row with its sub-jobs gone, and two tasks
                # on one connection. A connection still inside its
                # transaction at release time is terminated instead:
                # the server rolls the transaction back on disconnect,
                # the job row stays `running`, and lock-lease expiry
                # reclaims and retries it — the same loud, retryable
                # outcome a rotation-terminate produces, never a false
                # success.
                #
                # Why the whole body is guarded: this callback runs in
                # the AsyncExitStack unwind, where an exception it
                # raises replaces the job's real outcome — a committed
                # job reported as a dispatch failure. The pool's
                # bounded close (a credential-rotation drain or worker
                # teardown) can terminate the connection or release its
                # proxy underneath the dispatch, and a connection that
                # dead fails even the transaction probe: asyncpg nulls
                # a terminated connection's protocol object (the
                # forwarded probe raises AttributeError), and a proxy
                # the close released refuses the call (InterfaceError).
                # Those states are logged with their cause and
                # swallowed — the bounded close owns a dead
                # connection's disposal — so the unwind never
                # manufactures a failure the job did not have.
                # Anything outside these two families is a programming
                # error and stays loud.
                try:
                    if conn.is_in_transaction():  # pyright: ignore[reportAttributeAccessIssue]  # Why: ConnLike is the object-typed runtime alias for Connection | PoolConnectionProxy; both forward is_in_transaction() to the wrapped protocol.
                        conn.terminate()  # pyright: ignore[reportAttributeAccessIssue]  # Why: same forwarding as is_in_transaction.
                        logger.warning(
                            "slot-conn-terminated-transaction-in-flight",
                            kind="slot_conn_terminated_transaction_in_flight",
                            job_id=str(job.id),
                        )
                        return
                    await pool.release(conn)  # pyright: ignore[reportArgumentType]  # Why: ConnLike is the object-typed runtime alias; release accepts the proxy this acquire produced.
                except asyncpg.InterfaceError as exc:
                    # The pool closed underneath the dispatch: release
                    # is refused, or the close already released the
                    # proxy back — either way the close owns the
                    # connection now.
                    logger.warning(
                        "slot-pool-release-skipped-pool-closed",
                        kind="slot_pool_release_skipped_pool_closed",
                        job_id=str(job.id),
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                    )
                except AttributeError as exc:
                    # The connection was terminated underneath the
                    # dispatch: its protocol object is gone, so even
                    # the probe cannot run. Nothing left to release or
                    # terminate.
                    logger.warning(
                        "slot-pool-release-skipped-conn-dead",
                        kind="slot_pool_release_skipped_conn_dead",
                        job_id=str(job.id),
                        error_class=type(exc).__name__,
                        error_message=str(exc),
                    )

            conn_stack.push_async_callback(_release_slot_conn)
            # The acquired connection must carry the registered connection's
            # declared setup before the actor's DI can receive it. A
            # bootstrap-built pool applied it at connect time (recorded on
            # deps); an injected/foreign pool did not — repair that here,
            # once per physical connection. Armed release first: a failed
            # hook terminates the connection and the unwind must own it.
            await _ensure_registered_init_on_slot_conn(
                acquired,
                deps=deps,
                registry=registry,
                acquire_timeout=acquire_timeout,
                job_id=job.id,
            )
            # Per-job binding: this enqueuer's writes join the slot's
            # transaction and its buffers are this job's alone, so a
            # sibling slot's flush/discard/drain can never reach them.
            # The capacity snapshot is shared with the loop-level
            # enqueuer so per-job construction pays no extra backend
            # read.
            job_enqueuer = SubJobEnqueuer(
                loop_scope_resolved=None,
                worker_pool=deps.worker_pool,
                backend=backend,
                capacity_cache=enqueuer.capacity_cache,
                transaction_conn=transaction_conn,
            )
        else:
            # Why registry-trust instead of isinstance: the LOOP-scope
            # cache is keyed by type and populated through the typed
            # register_value seam, and every other reader of this seam
            # (bootstrap's activation, the enqueuer's provenance
            # inference) trusts that key — an isinstance here would
            # disagree with them and silently drop to the autonomous
            # path for a value the rest of the seam treats as the
            # transaction connection.
            raw_conn = loop_scope.resolved_cache().get(asyncpg.Connection)
            if raw_conn is not None:
                transaction_conn = cast(asyncpg.Connection, raw_conn)  # pyright: ignore[reportUnknownVariableType,reportAssignmentType]  # Why: resolved_cache returns Mapping[type, object]; the DI resolver guarantees the value registered under asyncpg.Connection is one, matching bootstrap's and the enqueuer's trust of the same key.
        t0 = time.monotonic()
        outcome: AttemptOutcome = "failed"

        try:
            with safe_start_span(
                f"process {job.actor}",
                kind=SpanKind.CONSUMER,
                attributes=consumer_attrs,
                links=links,
            ) as consumer_span:
                try:
                    validated_payload = validate_actor_payload(
                        actor_ref.payload_type,
                        job.payload,
                        job.actor,
                    )

                    span_ctx = consumer_span.get_span_context()
                    dispatch_trace_id: str = ""
                    if span_ctx.is_valid:
                        dispatch_trace_id = format(span_ctx.trace_id, "032x")

                    interim_ctx: JobContext[BaseModel] = JobContext(
                        job_id=job.id,
                        actor=job.actor,
                        queue=job.queue,
                        attempt=job.attempt,
                        snooze_count=job.snooze_count,
                        worker_id=worker_id,
                        payload=validated_payload,
                        jobs=job_enqueuer,
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
                        loop_slot_values=actor_loop_slot_values,
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

                        # Fleet-wide per-queue concurrency cap (see
                        # _effective_reservations): O(1) membership test, no
                        # per-job dict/list copies on the no-cap fast path.
                        effective_reservations = _effective_reservations(
                            actor_ref.reservations, job.queue, rl_registry
                        )

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
                            enqueuer=job_enqueuer,
                            transaction_conn=transaction_conn,
                            validated_payload=validated_payload,
                            rate_limit_registry=rl_registry,
                            rate_limits=actor_ref.rate_limits,
                            reservations=effective_reservations,
                            redis_client=redis_client,
                            worker_pool=deps.worker_pool,
                            settings=deps.settings,
                            fallback_result_ttl=actor_ref.result_ttl,
                        )
                        outcome = result

                        # Best-effort: a crash between the terminal write and
                        # the counter increment loses that increment.  See
                        # apply_batch_terminal_outcome docstring for full
                        # safety-net semantics (M7).
                        try:
                            await apply_batch_terminal_outcome(
                                backend, job, outcome, transaction_conn=transaction_conn
                            )
                        except Exception:
                            logger.exception("batch-policy-hook-failed", job_id=str(job.id))

                    if outcome == "succeeded":
                        consumer_span.set_status(StatusCode.OK)
                    elif outcome == "failed":
                        consumer_span.set_status(StatusCode.ERROR)

                except asyncio.CancelledError:
                    outcome = "cancelled"
                    consumer_span.set_status(StatusCode.ERROR, "cancelled")
                    # A batch completes on ANY terminal member: the
                    # batch-completion hook runs per-job for all terminal
                    # outcomes (succeeded, failed, cancelled). So a
                    # cancelled last member must complete its batch here,
                    # not a sweep-interval later. Best-effort for the same
                    # reason consume's own mark_cancelled on this path is
                    # best-effort (an infra failure there is logged inside
                    # consume and the row stays running for lock-lease
                    # reclaim): a hook failure here is logged and the M7
                    # stale-batch sweep remains the safety net for batch
                    # status. On the transactional consumer path the
                    # cancel has already aborted the slot's transaction,
                    # so the hook's writes on that connection fail, are
                    # logged, and the sweep recovers — immediate
                    # completion holds on the autonomous path, the normal
                    # case.
                    try:
                        await apply_batch_terminal_outcome(
                            backend, job, "cancelled", transaction_conn=transaction_conn
                        )
                    except Exception:
                        logger.exception("batch-policy-hook-failed", job_id=str(job.id))
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
                        handler_result: AttemptOutcome = await _handle_generic_exception(
                            backend,
                            job,
                            worker_id,
                            exc,
                            actor_config,
                            max_retry_backoff,
                            consumer_span,
                            handler_log,
                        )
                    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                        # An infra-failed terminal write leaves the row
                        # RUNNING — lock-lease expiry and the sweep are the
                        # recovery — so no batch counter may budge on a
                        # write that never landed: the same rule the hook
                        # itself applies to the clean-return path's "noop"
                        # (a terminal write that matched nothing). The hook
                        # call therefore lives in the else below, on a
                        # real terminal outcome only.
                        _log_terminal_write_failed(handler_log, job, exc, infra_exc)
                    else:
                        outcome = handler_result
                        # Best-effort, matching the hook call on consume's
                        # clean return above (M7 sweep semantics documented
                        # on apply_batch_terminal_outcome): non-terminal
                        # handler outcomes ("scheduled") return inside the
                        # hook without touching a counter.
                        try:
                            await apply_batch_terminal_outcome(
                                backend, job, outcome, transaction_conn=transaction_conn
                            )
                        except Exception:
                            logger.exception("batch-policy-hook-failed", job_id=str(job.id))
        finally:
            elapsed = time.monotonic() - t0
            # A noop means the row moved underneath this dispatch (a
            # reclaim race): nothing was consumed, and the re-dispatch
            # will record the real message and duration — recording here
            # would double-count the message and stretch the histogram
            # with a phantom process.
            if outcome != "noop":
                record_consumed_message(job.actor, job.queue, outcome=_to_consumed_outcome(outcome))
                record_process_duration(job.actor, job.queue, elapsed)

    return outcome
