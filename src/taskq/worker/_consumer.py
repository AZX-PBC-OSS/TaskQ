"""Per-job consumer exception handling and terminal-state writes.

Contains :func:`consume_one_job` that wraps the full exception-handling
sequence and helpers for the transactional and autonomous paths.  Every
backend write is wrapped in ``asyncio.shield`` (via
:func:`taskq._shield.shield_with_retrieval`, which also retrieves a
detached inner outcome when a second cancellation lands mid-write) so
cancellation during shutdown phase 2 cannot strand the row in ``running``.

The ``CancelledError`` handler routes on the cancel's ORIGIN (the
registry entry's ``cancel_origin``), not on the exception type: an
operator's request terminalises via ``mark_cancelled``; a shutdown
(SIGTERM / drain monitor) releases the attempt back to the fleet via
``mark_interrupted``; counted, never terminalised (the spent attempt
stands, no refund). The row is
the final arbiter between the two: the release's ``cancel_phase = 0``
fence declines a row an operator cancel already claimed, and the handler
falls through to the cancel write. A terminal write whose fence matches
no row is never read as success: the attempt reports ``"noop"`` (no hooks,
no terminal publish, no batch-counter movement), and on the transactional
path the actor's unit of work rolls back with it.

The individual terminal exception handlers (timeout, snooze, retry_after,
reservation denied, generic) live in :mod:`taskq.worker._handlers`.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from datetime import timedelta
from time import monotonic
from typing import TYPE_CHECKING, Final
from uuid import UUID

import asyncpg
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from pydantic import BaseModel

from taskq._forkguard import assert_own_process
from taskq._json import dumps as _json_dumps
from taskq._shield import shield_with_retrieval
from taskq._validation import validate_actor_payload
from taskq.backend._protocol import (
    Backend,
    CancelPhase,
    ConnLike,
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer, _parent_tags_var
from taskq.constants import (
    DEFAULT_MAX_RETRY_BACKOFF,
    DEFAULT_RESERVATION_BACKOFF,
    MAX_RESULT_BYTES,
)
from taskq.context import CancelOrigin, JobContext
from taskq.exceptions import (
    RateLimitDependencyUnavailable,
    ReservationUnavailable,
    ResultTooLarge,
    RetryAfter,
    Snooze,
    SubEnqueueError,
)
from taskq.obs import (
    ErrorReporter,
    ExceptionText,
    bind_job_context,
    get_logger,
    log_state_change,
    record_exception_text,
    record_ratelimit_acquire_dependency_failure,
    record_sub_enqueue_failure,
    render_exception,
    safe_start_span,
)
from taskq.progress._buffer import (
    _consume_state_change_seq,
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
    _terminal_seq_and_state,
)
from taskq.progress._flush import _flush_buffer, _flush_buffer_immediate
from taskq.progress._publish import _publish_state_change_event
from taskq.ratelimit.composition import AcquiredResource
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.retry import (
    ActorConfigLike,
    invoke_on_cancel,
    invoke_on_success,
)
from taskq.settings import WorkerSettings
from taskq.worker._handlers import (
    _TERMINAL_WRITE_BUDGET,  # pyright: ignore[reportPrivateUsage]  # Why: the release write's own bounded budget sizes the reserve the interrupted-actor exit park must leave before the shutdown deadline (see _interrupted_actor_hold).
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,
    AttemptOutcome,
    _ActorSystemExitAttemptError,
    _AttemptFencedOut,
    _disown_job,
    _dispatch_exception,
    _handle_reservation_class_denied,
    _log_terminal_write_failed,
    _StartToCloseExceededError,
    _terminal_write_with_retry,
    _TerminalWriteFailed,
)
from taskq.worker._watchdog import (  # pyright: ignore[reportPrivateUsage]  # Why: the tracked-handle registry registration for a detached tx unwind: the designated-writer contract mirrors the ctx stash beside it; no cycle (_watchdog imports nothing from the consumer side).
    register_tracked_actor_handle,
)
from taskq.worker.cancel import ActiveJobRegistry, _ActiveJob
from taskq.worker.deps import POOL_INFRA_EXCEPTIONS, WorkerDeps
from taskq.worker.shutdown import (  # pyright: ignore[reportPrivateUsage]  # Why: the consumer's release arm and the RELEASING phase are the two writers of the same interruption release; they must share the one hold computation rather than drift (see _interrupted_actor_hold).
    ShutdownPhase,
    _release_hold,
)

if TYPE_CHECKING:
    import redis.asyncio as redis_async

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_OK = object()

_TX_UNWIND_WAIT_BUDGET: Final[float] = 2.0
"""The transactional path's exit-wait budget, in loop-time seconds.

When the ``start_to_close`` deadline fires on the TRANSACTIONAL path, the
marker must not reach the transaction ``__aexit__``'s ROLLBACK while the
body task's unwind still awaits on the SHARED transaction connection:
the two collide on asyncpg's one-operation-at-a-time guard, the failing
ROLLBACK replaces the marker in ``__aexit__``, and the row records the
collision's ``InterfaceError`` instead of the truthful ``TimeoutError``
(the ``job_timeout`` log and the timeouts metric go with it). The
tx-path caller therefore bound-waits the unwind up to this budget before
letting the marker propagate, and the rollback runs on a quiesced
connection. A hostile unwind outliving the budget proceeds detached (the
deadline's win survives, bounded by the budget): the marker propagates
anyway, the ROLLBACK may still collide with the unwind's in-flight
statement, and that collision is translated back to the truthful timeout
disposition at the capture boundary (the rollback-collision window
stamped on the ctx -- only the enforcement's own rollback's collision
under the deadline's stamp is re-labeled; a genuine body-caused
``InterfaceError`` always records its own class). 2s covers every
legitimate unwind many times over while keeping the worst-case slot hold
past the deadline small. The autonomous path needs no such wait: its
connections are the body's own, never shared with the rollback.
"""


def _rate_limit_dependency_exceptions() -> tuple[type[BaseException], ...]:
    """The exception family a limiter acquire raises when its STORE
    failed to answer, Redis dead, or the PG fallback behind it dead or
    never wired.

    The PG members are the shared pool-infra classification
    (:data:`POOL_INFRA_EXCEPTIONS`, owned by worker.deps) plus the typed
    no-pool error the ratelimit PG delegates raise when the fallback is
    reached without an injected pool (a store that cannot answer, not a
    job defect, a ``RuntimeError`` subclass so the fail-loud pins hold);
    Redis joins only when the extra is installed, without redis-py no
    Redis error can occur on this path, so the ImportError narrows the
    family rather than weakening it.
    """
    family: list[type[BaseException]] = [
        *POOL_INFRA_EXCEPTIONS,
        RateLimitDependencyUnavailable,
    ]
    try:
        import redis as _redis_mod
    except ImportError:
        return tuple(family)
    family.append(_redis_mod.RedisError)
    return tuple(family)


_RATE_LIMIT_DEPENDENCY_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    _rate_limit_dependency_exceptions()
)
"""What a rate-limit acquire raises when a store dependency, not the
job, not the actor, failed to answer. Recognised at the acquire
boundary below and failed closed as the limiter's denial; anything else
from the composition stays loud through the generic handler."""

_DEPENDENCY_FAILURE_LOG_WINDOW_S: Final[float] = 60.0
"""Window gating the acquire dependency-failure WARNING. A sustained
store outage fails every rate-limited dispatch, and one warning line
per denial is a log flood, not a signal, the same bound the registry's
keyed heal-failure emission applies. The per-occurrence aggregate stays
on the ``ratelimit.acquire_dependency_failures`` counter."""

_dependency_failure_warned: dict[str, float] = {}
"""Monotonic stamp of the last emitted dependency-failure WARNING, keyed
by error class (bounded: the stores' exception vocabulary)."""


def bind_job_log(
    log: structlog.stdlib.BoundLogger, job: JobRow, *, span: trace.Span
) -> structlog.stdlib.BoundLogger:
    """Bind *job*'s fields onto *log*, the one logger every line of a
    job's dispatch and consumption carries.

    The trace id comes from *span* (the CONSUMER span) when it is valid
    and is the empty string otherwise; ``batch_id`` rides along when the
    row's metadata carries one. The dispatch path binds this once for the
    DI-resolution context and hands the result to :func:`consume_one_job`
    as ``job_log``; a direct caller gets the same binding by default.
    """
    trace_id = ""
    span_context = span.get_span_context()
    if span_context.is_valid:
        trace_id = format(span_context.trace_id, "032x")
    batch_id: str | None = None
    if job.metadata:
        raw_bid = job.metadata.get("batch_id")
        if raw_bid is not None:
            batch_id = str(raw_bid)
    return bind_job_context(
        log,
        job_id=job.id,
        actor=job.actor,
        queue=job.queue,
        attempt=job.attempt,
        identity_key=job.identity_key,
        trace_id=trace_id,
        batch_id=batch_id,
    )


def _encode_result(result: object, max_bytes: int = MAX_RESULT_BYTES) -> bytes | None:
    """Serialize an actor return value to orjson bytes exactly once.

    ``BaseModel`` results are dumped via ``model_dump(mode="json")``;
    ``dict`` results are dumped as-is (the actor contract guarantees
    ``dict[str, object]``); all other types return ``None`` (no result
    stored).  Raises :class:`ResultTooLarge` (non-retryable, a re-run
    returns the same oversized value) when the serialized size exceeds
    *max_bytes*, which the callers take from
    ``WorkerSettings.result_max_bytes`` and which defaults to
    :data:`~taskq.constants.MAX_RESULT_BYTES`.

    The returned bytes are the result's ONLY serialization: they reach the
    backend as the ``result_bytes`` terminal-write parameter, which binds
    them decoded and stores ``len`` as ``result_size_bytes`` without
    serializing again.  The NUL guard deliberately does not run here, it
    stays at the terminal write (the single consumption point), so a
    recording backend observes the exact bytes.
    """
    storable: object
    if isinstance(result, BaseModel):
        storable = result.model_dump(mode="json")
    elif isinstance(result, dict):
        storable = result  # pyright: ignore[reportUnknownVariableType]  # Why: run_actor returns Awaitable[object]; isinstance narrows to dict[Unknown, Unknown]. At runtime the actor contract guarantees dict[str, object]; the value flows through the object-typed storable and orjson's Any parameter unharmed.
    else:
        return None
    data = _json_dumps(storable)
    if len(data) > max_bytes:
        raise ResultTooLarge(f"result size {len(data)} bytes exceeds {max_bytes} byte cap")
    return data


async def _pre_terminal_flush(
    job: JobRow,
    worker_id: UUID,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    worker_pool: "asyncpg.Pool | None",
    settings: WorkerSettings | None,
) -> "_ProgressBuffer | None":
    """Flush the job's dirty progress buffer ahead of its terminal write
    and return the buffer the write's progress fields are read from.

    The flush is shielded so a cancel landing mid-statement cannot strand
    the progress row half-written, but the shield costs a Task per call,
    and the flush itself is a no-op for a clean buffer (the common case:
    an actor that reported no progress). The dirty check is made here so
    only a flush that will issue a statement pays for the shield; the
    buffer's own no-op guard still holds behind it.
    """
    if progress_buffers is None:
        return None
    buffer = progress_buffers.get(job.id)
    if buffer is None or not buffer.dirty or worker_pool is None or settings is None:
        return buffer
    await shield_with_retrieval(
        _flush_buffer_immediate(
            worker_pool,
            settings.schema_name,
            job.id,
            worker_id,
            progress_buffers,
        )
    )
    return progress_buffers.get(job.id)


async def _run_terminal_path(  # pyright: ignore[reportUnusedFunction]  # Why: called by _dispatch_exception in _handlers.py via lazy import
    *,
    job: JobRow,
    worker_id: UUID,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    worker_pool: "asyncpg.Pool | None",
    settings: WorkerSettings | None,
    redis_client: "redis_async.Redis | None",
    handler: Callable[..., Awaitable[AttemptOutcome]],
    handler_args: tuple[object, ...],
    handler_kwargs: dict[str, object],
    status: str,
    terminal: bool,
    outcome: AttemptOutcome,
    job_exc: BaseException | None = None,
    disowned_jobs: set[UUID] | None = None,
    job_log: structlog.stdlib.BoundLogger | None = None,
) -> AttemptOutcome:
    """Pre-terminal flush, handler call, dirty reset, publish, and outcome.

    Consolidates the identical 15-line block that was copy-pasted across
    ten exception handlers in ``consume_one_job`` and
    ``_consume_transactional``.

    Infra failures (DB/network) raised by *handler*'s terminal write are
    caught here, not re-dispatched into generic exception handling, which
    would misclassify the infra error as the actor's failure (*job_exc*).
    The job row stays ``running``; it is disowned into *disowned_jobs* so
    the heartbeat stops renewing it and lock-lease expiry reclaims it.
    """
    # The terminal double-write seam: every exception-routed terminal write
    # funnels here. A forked child that resumed mid-body reaches the SAME
    # write through the parent's sockets; the guard refuses it before the
    # handler runs, so one job's ledger can never be written by two
    # processes. (The guarded connection classes cover the wire itself;
    # this check covers the outcome BEFORE the wire can be reached.)
    assert_own_process(f"worker terminal write (job {job.id})")
    _pbuf = await _pre_terminal_flush(job, worker_id, progress_buffers, worker_pool, settings)
    _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)
    try:
        handler_result: AttemptOutcome = await handler(
            *handler_args,
            progress_seq=_pseq,
            progress_state=_pstate,
            **handler_kwargs,
        )
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
        _log_terminal_write_failed(
            job_log if job_log is not None else _log,
            job,
            job_exc if job_exc is not None else infra_exc,
            infra_exc,
        )
        _disown_job(disowned_jobs, job)
        return outcome
    if progress_buffers is not None:
        _buf = progress_buffers.get(job.id)
        if _buf is not None:
            _buf.dirty = False
    # A noop means no transition happened, the row moved underneath
    # this dispatch (a reclaim race), so publishing the requested
    # state change would announce a move the row never made.
    if redis_client is not None and settings is not None and handler_result != "noop":
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
    return handler_result


_BARE_CALL_HOLD_FALLBACK_SECS: Final[float] = 60.0
"""Hold for an actor that never provably exited when neither deps nor
settings reached the consumer (a bare direct ``consume_one_job`` call). The
production dispatch path always carries one of the two; the fallback is the
default ``lock_lease``: the lease-expiry bound a stranded row already
imposes, so a naked call stays honest rather than assuming the actor gone
(hold=0) with no evidence either way."""


async def _interrupted_actor_hold(
    ctx: JobContext[BaseModel],
    *,
    deps: WorkerDeps | None,
    settings: WorkerSettings | None,
) -> timedelta:
    """Wait, bounded, for an interrupted actor to provably exit; return the
    hold its release must carry.

    An async actor has provably unwound by the time the cancellation handler
    runs: the ``CancelledError`` propagated through its frames to get here,
    and a sync actor that honoured the stop raised from its own thread,
    leaving its tracked handle done. Both earn ``hold=timedelta(0)``: the
    row is genuinely free the moment the release write lands, exactly the
    fast path a responsive actor has always had.

    A sync actor still executing in its executor thread (``task.cancel()``
    cancels the await, never the thread), a transactional actor whose tx
    task is still unwinding its rollback, and an async actor whose body
    task is still unwinding a start_to_close cancel (the deadline
    enforcement runs the body in its own task, see
    :func:`_enforce_start_to_close`) are NOT exited. Their tracked
    handles on *ctx* are the only proof available, so this parks on them:
    bounded by the remaining termination budget minus the release write's
    own retry budget, so the write still fits before the watchdog's
    deadline, and releases with hold=0 only on a provable exit inside that
    window. An actor that outlives the window (the unbounded sync
    actor) is still released, never stranded: the hold is the rest of this
    process's exit window plus the watchdog's exit tail
    (:func:`taskq.worker.shutdown._release_hold`), so the row becomes
    claimable only once this process is provably gone.

    A cancellation landing on the park (the TaskGroup teardown after
    RELEASING, a second forced cancel) ends the waiting, never the release:
    the hold below still covers the process's exit window.
    """
    loop = asyncio.get_running_loop()
    # The dispatch layer, the transactional consumer and the start_to_close
    # enforcement are the designated writers of these handles
    # (JobContext._set_sync_actor_task / _set_tx_unwind_task /
    # _set_actor_body_task); this shutdown arm is their designated reader.
    sync_handle = ctx._sync_actor_task  # pyright: ignore[reportPrivateUsage]  # Why: reading the dispatch layer's tracked thread handle: see the setter's contract.
    tx_handle = ctx._tx_unwind_task  # pyright: ignore[reportPrivateUsage]  # Why: reading the transactional consumer's unwind handle: see the setter's contract.
    body_handle = ctx._actor_body_task  # pyright: ignore[reportPrivateUsage]  # Why: reading the deadline enforcement's body-task handle: see the setter's contract.
    pending: list[asyncio.Task[object]] = [
        handle
        for handle in (sync_handle, tx_handle, body_handle)
        if handle is not None and not handle.done()
    ]
    if not pending:
        return timedelta(0)
    effective_settings = (
        settings if settings is not None else (deps.settings if deps is not None else None)
    )
    if effective_settings is None:
        return timedelta(seconds=_BARE_CALL_HOLD_FALLBACK_SECS)
    wait_budget = _actor_exit_wait_budget(
        deps,
        effective_settings,
        loop,
        reserve=_TERMINAL_WRITE_BUDGET.total_seconds(),  # pyright: ignore[reportPrivateUsage]  # Why: the release write's own bounded budget: the park must leave room for the write before the deadline.
    )
    still_pending: set[asyncio.Task[object]] | None = None
    if wait_budget > 0:
        try:
            _done, still_pending = await asyncio.wait(pending, timeout=wait_budget)
        except asyncio.CancelledError:
            # The park's patience ended early (a second cancel, the group
            # teardown after RELEASING): stop waiting, keep the release.
            still_pending = set(pending)
        if not still_pending:
            _log.info(
                "interrupted-actor-exited-in-window",
                kind="interrupted_actor_exit",
                job_id=str(ctx.job_id),
                actor=ctx.actor,
                waited_budget_seconds=wait_budget,
                outcome="exited",
            )
            return timedelta(0)
    _log.warning(
        "interrupted-actor-outlived-exit-window",
        kind="interrupted_actor_exit",
        job_id=str(ctx.job_id),
        actor=ctx.actor,
        waited_budget_seconds=wait_budget,
        outcome="held",
        still_alive_handles=sum(1 for t in (still_pending or set(pending)) if not t.done()),
    )
    return _release_hold(  # pyright: ignore[reportPrivateUsage]  # Why: the one hold computation both release writers share: the consumer arm and the RELEASING phase must never disagree on what "held until this process is provably gone" means.
        deps,
        effective_settings,
        loop,
    )


def _actor_exit_wait_budget(
    deps: WorkerDeps | None,
    settings: WorkerSettings,
    loop: asyncio.AbstractEventLoop,
    *,
    reserve: float,
) -> float:
    """Bounded patience, in loop-time seconds, for an interrupted actor's exit.

    Deadline-anchored when the shutdown started and the watchdog enforces
    the deadline: the remaining termination budget minus *reserve* (the
    release write's own bounded budget: parking to the last second would
    leave the release itself racing the watchdog trip), then CAPPED by the
    lease: the heartbeat (the row's only lease renewer) stops at
    ``shutdown_event``, so a park that outlived the lease would let the
    reclaim sweep re-pend a row whose actor thread is still executing (a
    single infra-failed RELEASING write away from the double-run). The cap
    (``lock_lease - heartbeat - reserve``, see
    ``WorkerSettings.release_park_lease_cap``) is the exact bound that
    makes the parked consumer's release write land before the earliest
    reclaim for ANY loadable config: safety by construction rather than
    by a cross-field rejection, which is why no validator enforces the
    lease/budget pair. Without an anchor or a guaranteed exit (watchdog
    disabled, a bare call, a shutdown that never stamped its start) the
    bound is the orchestrator's own post-cancel patience: the cleanup
    grace, after which RELEASING releases the row regardless, so parking
    longer buys nothing.
    """
    if not settings.watchdog_enabled:
        return settings.cleanup_grace_period
    started_at = deps.shutdown_started_at if deps is not None else None
    if started_at is None:
        return settings.cleanup_grace_period
    remaining = settings.termination_grace_period - (loop.time() - started_at) - reserve
    return max(0.0, min(remaining, settings.release_park_lease_cap))


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
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
    active_jobs: ActiveJobRegistry | None = None,
    enqueuer: SubJobEnqueuer | None = None,
    transaction_conn: ConnLike | None = None,
    validated_payload: BaseModel | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
    rate_limits: Sequence[str | KeyedRateLimitRef | TokenBucket | SlidingWindow] | None = None,
    reservations: Sequence[str | KeyedReservationRef | ConcurrencyReservation] | None = None,
    redis_client: "redis_async.Redis | None" = None,
    worker_pool: asyncpg.Pool | None = None,
    settings: WorkerSettings | None = None,
    error_reporter: ErrorReporter | None = None,
    fallback_result_ttl: timedelta | None = None,
    job_log: structlog.stdlib.BoundLogger | None = None,
) -> AttemptOutcome:
    """Run one job's full  try/except sequence.

    The worker's dispatch_one_job helper wraps calls to this function
    with build_actor_scope to provide per-invocation TRANSIENT scope
    and DI resolution for the actor's declared dependencies.

    Returns the job's terminal outcome for span status and metric
    recording by the caller (``dispatch_one_job``).

    ``payload_type`` is the actor's payload model. The typed model is
    resolved BEFORE rate-limit acquisition: on the dispatch path
    (``dispatch_one_job``), ``validated_payload`` is already set when this
    function is called; a direct caller that passes
    ``validated_payload=None`` gets the fallback
    ``validate_actor_payload`` call, whose
    :class:`~taskq.exceptions.PayloadValidationError` propagates to the
    caller BEFORE any token is consumed, an invalid payload must not
    acquire (and non-refundably burn) a rate-limit token for an actor body
    that can never run. ``acquire_for_actor`` then receives the validated
    ``BaseModel`` (not the raw row dict): a ``KeyedRateLimitRef`` /
    ``KeyedReservationRef`` whose ``payload_type`` is the actor's model hits
    the registry's isinstance fast path, while a stricter cross-model ref
    re-validates the model's dump, which carries the actor model's applied
    defaults and aliases. The :class:`JobContext` handed to the actor always
    carries a typed, validated :class:`pydantic.BaseModel` instance. The
    bound is ``BaseModel`` here (the registry is heterogeneous); per-actor
    ``P`` flows from the call site that selected ``payload_type``.

    ``enqueuer`` is the SubJobEnqueuer the dispatch path selected, the
    per-job instance bound to the slot transaction connection on the
    per-slot path, or the per-loop instance ``_main`` constructs after
    ``loop_scope.bootstrap()``. When provided, the live JobContext uses
    this enqueuer so sub-enqueues are transactional.

    ``error_reporter`` is an optional :class:`~taskq.obs.ErrorReporter`
    invoked when a job reaches a terminal failure state (retry exhausted
    or non-retryable error).  When ``None``, no error reporting occurs.

    ``fallback_result_ttl`` is the worker-side ``@actor(result_ttl=...)``
    literal, forwarded to the success terminal write so a cleared stored
    override still computes ``result_expires_at`` from completion rather
    than keeping the enqueue-pinned value.
    The reporter call is wrapped in a try/except, a failing reporter
    never crashes the worker.

    ``job_log`` is the job-bound logger a caller has already built (the
    dispatch path binds one for the DI-resolution context and hands it
    here so the job's lines come from one logger, bound once); when
    ``None`` it is bound here from ``logger`` and the current span.

    ``transaction_conn`` is the connection the job's transaction runs
    on, the connection this dispatch acquired from the worker's slot
    pool on the per-slot path, or the resolved LOOP-scope
    asyncpg.Connection (None when no LOOP-scope connection provider is
    registered). When present, the consumer opens a transaction on it
    for the success path and wraps the entire block in
    ``asyncio.shield`` per G8.

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
    consumer_span = trace.get_current_span()
    if job_log is None:
        job_log = bind_job_log(logger if logger is not None else _log, job, span=consumer_span)

    # The fork guard at the attempt boundary: a forked child that resumed
    # the worker's loop and picked up its own job claims it here, through
    # the parent's sockets. Fail loud before anything else - no claim
    # write, no rate-limit token, no ledger entry, the child gets the
    # refusal and the parent's job row is untouched by it.
    assert_own_process(f"worker job attempt (job {job.id})")

    _rl_limits: Sequence[str | KeyedRateLimitRef | TokenBucket | SlidingWindow] = (
        rate_limits if rate_limits is not None else ()
    )
    _rl_reservations: Sequence[str | KeyedReservationRef | ConcurrencyReservation] = (
        reservations if reservations is not None else ()
    )
    _needs_acquire = bool(_rl_limits or _rl_reservations) and rate_limit_registry is not None

    # Resolve the typed model BEFORE rate-limit acquisition: an invalid payload must not
    # acquire, and non-refundably burn, a rate-limit token for an actor
    # body that can never run. acquire_for_actor then receives the validated
    # BaseModel, so keyed refs either hit the registry's isinstance fast path
    # (same model) or re-validate the model's dump, which carries the actor
    # model's applied defaults/aliases, not the raw row dict. The wrapped
    # PayloadValidationError propagates to the caller, exactly as it did
    # from the in-try fallback and as non-dependency acquire-path errors
    # still do (a wiring or programming defect must stay loud): callers
    # (dispatch_one_job's outer except, the in-memory runner's catch) own
    # the terminal write for pre-actor failures. A STORE-dependency failure
    # is the exception, the acquire boundary below fails it closed as the
    # limiter's own denial, because an infrastructure outage is not a job
    # outcome either.
    if validated_payload is None:
        # The row's stored version rides the raise, not the helper's
        # current-version default, so a row that predates a payload
        # migration is distinguishable from a malformed caller payload.
        validated_payload = validate_actor_payload(
            payload_type,
            job.payload,
            job.actor,
            payload_schema_ver=str(job.payload_schema_ver),
        )

    acquired: list[AcquiredResource] = []

    if _needs_acquire and rate_limit_registry is not None:
        try:
            acquired = await rate_limit_registry.acquire_for_actor(
                rate_limits=_rl_limits,
                reservations=_rl_reservations,
                job_id=job.id,
                worker_id=worker_id,
                payload=validated_payload,
                redis_client=redis_client,
                pg_pool=worker_pool,
                clock=clock,
                settings=settings,
            )
        except ReservationUnavailable as e:
            # The handler owns the outcome tri-state (a snooze, a
            # deadline failure, a budget-exhaustion failure, or a noop
            # when the job moved underneath us), its result is this
            # dispatch's result, not a hardcoded reschedule. Routed
            # through _run_terminal_path exactly as _dispatch_exception
            # routes the in-actor denial for the same handler: the
            # snooze write's infra failures surface as
            # terminal-write-failed (never re-classified as the actor's
            # failure by an outer generic catch), and the scheduled
            # transition reaches Redis like every other requeue.
            if e.source == "reservation":
                handler_kwargs: dict[str, object] = {
                    "awaiting_prefix": "reservation:",
                    "outcome": "reservation_denied",
                    "debug_event": "consume-reservation-denied-noop",
                }
            else:
                handler_kwargs = {
                    "awaiting_prefix": "rate_limit:",
                    "outcome": "rate_limit_denied",
                    "debug_event": "consume-rate-limit-denied-noop",
                }
            handler_kwargs["error_reporter"] = error_reporter
            return await _run_terminal_path(
                job=job,
                worker_id=worker_id,
                progress_buffers=deps.progress_buffers if deps is not None else None,
                worker_pool=deps.worker_pool if deps is not None else worker_pool,
                settings=deps.settings if deps is not None else settings,
                redis_client=deps.redis_client if deps is not None else redis_client,
                disowned_jobs=deps.disowned_jobs if deps is not None else None,
                job_log=job_log,
                handler=_handle_reservation_class_denied,
                handler_args=(backend, job, worker_id, e, consumer_span, job_log, actor_config),
                handler_kwargs=handler_kwargs,
                status="scheduled",
                terminal=False,
                outcome="scheduled",
                job_exc=e,
            )
        except _RATE_LIMIT_DEPENDENCY_EXCEPTIONS as exc:
            # The limiter's store could not answer. This try block wraps
            # ONLY the acquire composition, so a store-failure-family
            # exception here has exactly one provenance, and one
            # response: the limiter's own fail-closed denial, never the
            # actor-failure accounting an escapee falls into (a retry
            # attempt burnt and the store's error persisted as the job's
            # error_class). The snooze write runs through the same
            # _run_terminal_path as an ordinary denial, so its infra
            # failures surface as terminal-write-failed and the row is
            # reclaimed by lock-lease expiry.
            #
            # Distinguishability: the denial carries an awaiting
            # annotation naming the unavailability and its cause
            # (rate_limit:unavailable:<error_type>), the
            # acquire_dependency_failures counter rises beside the
            # denials counter (an operator scaling a bucket on denials
            # alone would chase an outage with capacity), and the
            # WARNING is window-gated, a sustained outage denies every
            # rate-limited dispatch, and a warning per denial is a log
            # flood, not a signal.
            #
            # Non-consuming: the denial is infra backpressure about a
            # job whose actor never ran, so it rides mark_snoozed's
            # 'unavailable' arm (attempt refunded, no terminal arm) ,
            # never the budget-consuming bounded loop a saturation
            # denial deliberately takes. The reason is passed here,
            # explicitly: the store's unavailability is proven only at
            # this synthesis site, never inferred downstream.
            error_type = type(exc).__name__
            record_ratelimit_acquire_dependency_failure(error_type)
            now = monotonic()
            last_warned = _dependency_failure_warned.get(error_type)
            if last_warned is None or now - last_warned >= _DEPENDENCY_FAILURE_LOG_WINDOW_S:
                _dependency_failure_warned[error_type] = now
                job_log.warning(
                    "rate-limit-dependency-failure",
                    kind="rate_limit_dependency_failure",
                    error_class=error_type,
                    error_message=str(exc),
                )
            denial = ReservationUnavailable(
                bucket_name=f"unavailable:{error_type}",
                retry_after=DEFAULT_RESERVATION_BACKOFF,
                source="rate_limit",
            )
            # Why a direct __cause__ assignment: the synthetic denial is
            # constructed, not raised, so no except-context chains the
            # store failure automatically, the chain is what the
            # terminal-write infra log and any traceback reader see.
            denial.__cause__ = exc
            return await _run_terminal_path(
                job=job,
                worker_id=worker_id,
                progress_buffers=deps.progress_buffers if deps is not None else None,
                worker_pool=deps.worker_pool if deps is not None else worker_pool,
                settings=deps.settings if deps is not None else settings,
                redis_client=deps.redis_client if deps is not None else redis_client,
                disowned_jobs=deps.disowned_jobs if deps is not None else None,
                job_log=job_log,
                handler=_handle_reservation_class_denied,
                handler_args=(
                    backend,
                    job,
                    worker_id,
                    denial,
                    consumer_span,
                    job_log,
                    actor_config,
                ),
                handler_kwargs={
                    "awaiting_prefix": "rate_limit:",
                    "outcome": "rate_limit_denied",
                    "debug_event": "consume-rate-limit-dependency-failure-noop",
                    "error_reporter": error_reporter,
                    "denial_reason": "unavailable",
                },
                status="scheduled",
                terminal=False,
                outcome="scheduled",
                job_exc=denial,
            )

    # ── Buffer registration ────────────────────────────────────────────────
    _effective_pool = deps.worker_pool if deps is not None else worker_pool
    _effective_settings = deps.settings if deps is not None else settings
    _effective_redis = deps.redis_client if deps is not None else redis_client
    _progress_buffers = deps.progress_buffers if deps is not None else None
    _pending_publish_tasks = getattr(deps, "pending_publish_tasks", None)
    _disowned_jobs = deps.disowned_jobs if deps is not None else None

    # This attempt's own buffer, captured for the exit paths below (the
    # issue-461 class): the buffer is installed eagerly here and the same
    # key can be overwritten by a later attempt's buffer when the lease
    # lapses mid-run and the same worker re-claims the job, so the exits
    # must remove and flush the entry THIS attempt installed, never a
    # bare-id pop of whatever the key holds by then.
    _buf: _ProgressBuffer | None = None

    if _progress_buffers is not None:
        # attempt seeds the buffer's flush-fence epoch: a stale flush
        # landing after a same-worker redispatch to a later attempt
        # no-ops instead of clobbering the new epoch's progress.
        _buf = _ProgressBuffer(job_id=job.id, base_seq=job.progress_seq, attempt=job.attempt)
        _progress_buffers[job.id] = _buf
        # The running transition is itself an event on the job's stream:
        # it consumes the next seq (the seq is a strict total order over
        # progress and state-change events alike), recorded on the buffer
        # so the running publish below, every later ctx.progress call,
        # the flush deltas, and the terminal helpers all stack on it and
        # no later event can repeat the seq it carried. The consumption
        # rides the next flush delta or the next mark_* absolute SET to
        # the durable row, so the next attempt's buffer seeds past it.
        _consume_state_change_seq(_buf)

    _parent_tags_token = _parent_tags_var.set(tuple(job.tags))

    # This attempt's own registration, captured for the exit path below
    # (issue 461): the lease can lapse mid-run, the same worker can
    # re-claim the job, and the re-claimed attempt overwrites the
    # registry key with ITS entry. The finally must deregister the entry
    # this attempt registered, never a bare-id pop of whatever the key
    # holds by then, or the stale exit evicts the live attempt's
    # registration and every held_ids() reader misreads the map.
    _active_entry: _ActiveJob | None = None

    try:
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
            claim_epoch=job.claim_epoch,
            snooze_count=job.snooze_count,
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
            _active_entry = await active_jobs.register(job.id, task, ctx)

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

        # The attempt span's rendering of the failure, carried across the
        # re-raise to the terminal handler below so the traceback is rendered
        # and scrubbed once, not once per sink.
        attempt_text: ExceptionText | None = None
        try:
            with safe_start_span(
                f"attempt.{job.attempt}",
                kind=SpanKind.INTERNAL,
            ) as attempt_span:
                try:
                    if transaction_conn is not None:
                        tx_outcome = await _consume_transactional(
                            backend,
                            job,
                            worker_id,
                            ctx,
                            live_enqueuer,
                            transaction_conn,
                            run_actor,
                            actor_config,
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
                            fallback_result_ttl=fallback_result_ttl,
                            disowned_jobs=_disowned_jobs,
                            deps=deps,
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
                        _auto_outcome = await _consume_autonomous(
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
                            fallback_result_ttl=fallback_result_ttl,
                        )
                        if _auto_outcome == "succeeded":
                            consumer_span.add_event(
                                "lifecycle.succeeded",
                                attributes={"from_state": "running", "to_state": "succeeded"},
                            )
                            log_state_change(ctx.log, from_state="running", to_state="succeeded")
                        return _auto_outcome
                except Exception as exc:
                    # Recorded here rather than by safe_start_span's fallback
                    # (which renders afresh) so the handler can share the text.
                    # Only a recording span is worth a rendering: with tracing
                    # off, a Snooze must not pay for a traceback nobody reads,
                    # and the handlers that need one render it themselves.
                    # ``Exception``, not ``BaseException``: cancellation and
                    # the terminal-write sentinels are not attempt failures
                    # (and recording a cancel on the attempt span would
                    # misstate it as an exception). A non-Exception
                    # BaseException from an actor body skips this recording;
                    # the generic handler renders its traceback itself when
                    # *text* arrives None.
                    if attempt_span.is_recording():
                        attempt_text = render_exception(exc)
                        record_exception_text(attempt_span, attempt_text)
                    raise

        except asyncio.CancelledError:
            if _completion is _OK:
                raise
            if transaction_conn is not None:
                live_enqueuer.discard_buffer()
            entry = active_jobs.get(job.id) if active_jobs is not None else None
            if entry is not None and entry.cancel_phase >= CancelPhase.ABANDON_PENDING:
                raise
            if (
                entry is not None
                and entry.cancel_origin is CancelOrigin.NONE
                and deps is not None
                and deps.shutdown_phase is not ShutdownPhase.NONE
            ):
                # A registration that landed after CANCELLING's stamping
                # pass (a slow slot-pool acquire or DI resolution held the
                # take-to-register window open past every phase snapshot)
                # is torn down by the worker's own teardown cancellation
                # with no origin recorded. While the orchestration runs it
                # is the only canceler that could be driving this task:
                # stamp SHUTDOWN so the routing below releases the attempt
                # back to the fleet instead of terminalising a deploy's
                # work. The row stays the final arbiter: an operator cancel
                # already on the row fences the release (cancel_phase != 0)
                # and the fall-through to mark_cancelled below keeps the
                # operator's verdict.
                entry.cancel_origin = CancelOrigin.SHUTDOWN
                entry.ctx._set_cancel_origin(CancelOrigin.SHUTDOWN)  # pyright: ignore[reportPrivateUsage]  # Why: the consumer is the designated writer of its own context's origin stamp when it completes the orchestration's missed stamp (set alongside the registry entry's, per the field's contract).
            # Identity-scoped (the issue-461 class, the same fence the
            # deregister below applies): the terminal override reads and
            # removes THIS attempt's buffer, never a bare-id pop of the
            # key. A same-worker re-claim overwrote the key with the live
            # attempt's buffer; a stale attempt must neither consume the
            # live attempt's seq as its terminal override nor evict the
            # live buffer from the flush loop's dirty snapshots.
            _cancel_buf = _buf
            if _progress_buffers is not None and _progress_buffers.get(job.id) is _buf:
                del _progress_buffers[job.id]
            _cancel_seq, _cancel_state = _terminal_seq_and_state(_cancel_buf)
            _cancel_state_for_write = (
                _cancel_state if _cancel_buf is not None and _cancel_buf.dirty else None
            )
            if entry is not None and entry.cancel_origin is CancelOrigin.SHUTDOWN:
                # Infrastructure interruption (SIGTERM / drain monitor),
                # not an operator cancel: release the attempt back to the
                # fleet instead of terminalising it (the spent attempt
                # stands; no refund). The hold is EARNED, never assumed:
                # an async actor
                # has provably unwound by the time this handler runs (the
                # cancellation propagated through its frames to get
                # here), and a sync actor that honoured the stop raised
                # from its own thread: both are gone, hold=0, and the
                # row lands pending at the head of the order: available
                # immediately, no error recorded. A
                # sync actor still executing in its executor thread
                # (task.cancel() cancels the await, never the thread) and
                # a transactional actor still unwinding its rollback are
                # NOT gone: _interrupted_actor_hold parks on their
                # tracked handles, bounded by the remaining termination
                # budget, and holds the release behind this process's
                # exit window when they never finish: the row must not
                # be claimable while this process might still touch it
                #
                #
                # The row is the final arbiter: a "noop" means the fence
                # declined the release: an operator cancel raced the
                # deploy onto the row (cancel_phase != 0), the RELEASING
                # phase already released it while this consumer parked,
                # or the row moved underneath the attempt (a reclaim),
                # and the attempt falls through to the ordinary cancel
                # write below, whose own fence decides what is left to
                # write. Retried like every other state write (the
                # bounded budget of _terminal_write_with_retry): see the
                # mark_cancelled arm below for why a retry is safe inside
                # this handler and what a second cancel does to it.
                release_hold = await _interrupted_actor_hold(
                    ctx,
                    deps=deps,
                    settings=_effective_settings,
                )
                interrupt_outcome: str | None = None
                try:
                    interrupt_outcome = await _terminal_write_with_retry(
                        lambda: backend.mark_interrupted(
                            job.id,
                            worker_id,
                            attempt=job.attempt,
                            claim_epoch=job.claim_epoch,
                            hold=release_hold,
                            progress_seq=_cancel_seq,
                            progress_state=_cancel_state_for_write,
                        ),
                        log=job_log,
                        job=job,
                        write_name="mark_interrupted",
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    # Best-effort, exactly like the cancel write below: the
                    # row stays 'running', disowned so lock-lease expiry
                    # reclaims it. Do NOT fall through to mark_cancelled ,
                    # the row carries no operator cancel, so a cancel write
                    # here would terminalise an infrastructure interruption.
                    # The infra error is swallowed, never re-raised: a bare
                    # `raise` here re-raises infra_exc, REPLACING the
                    # CancelledError this handler is handling, so the
                    # interruption escapes consume_one_job as a job
                    # exception and dispatch's generic handler spends the
                    # attempt's budget on a deploy: the exact mislabel the
                    # mark_cancelled arm's comment below describes (an
                    # eaten cancellation). Logged and disowned
                    # above; the `raise` at the end of this handler
                    # propagates the cancellation.
                    _log_terminal_write_failed(job_log, job, None, infra_exc)
                    _disown_job(_disowned_jobs, job)
                except asyncio.CancelledError:
                    _disown_job(_disowned_jobs, job)
                    raise
                if interrupt_outcome is None:
                    # Infra-failed release write: logged and disowned
                    # above, and nothing further may write (the comment in
                    # the except arm says why). The handler's final raise
                    # below is what leaves this arm.
                    raise
                if interrupt_outcome != "noop":
                    _interrupted_status = (
                        "failed"
                        if interrupt_outcome == "failed:DeadlineExceeded"
                        else interrupt_outcome
                    )
                    consumer_span.add_event(
                        "lifecycle.interrupted",
                        attributes={"from_state": "running", "to_state": _interrupted_status},
                    )
                    log_state_change(
                        ctx.log,
                        from_state="running",
                        to_state=_interrupted_status,
                        reason="interrupted",
                    )
                    if _effective_redis is not None and _effective_settings is not None:
                        await _publish_state_change_event(
                            _effective_redis,
                            _effective_settings,
                            job.id,
                            job.actor,
                            None,
                            status=_interrupted_status,
                            terminal=interrupt_outcome == "failed:DeadlineExceeded",
                            _override_seq=_cancel_seq,
                            _override_pending_state=_cancel_state,
                        )
                    raise
                # "noop": the fence declined, an operator cancel landed on
                # the row first (cancel_phase != 0), or the row already
                # moved (released by RELEASING, reclaimed, terminalised).
                # The operator's request owns the terminal state; fall
                # through to mark_cancelled.
            consumer_span.add_event(
                "lifecycle.cancelled",
                attributes={"from_state": "running", "to_state": "cancelled"},
            )
            cancel_landed: bool | None
            # The write is retried inside the cancellation handler on the
            # same bounded budget as every other terminal write: the
            # task's cancellation has already been delivered, so awaiting
            # the retry waits here is ordinary, and the whole window
            # (about a second of waits, five seconds of wall time at most)
            # sits inside the cleanup grace the forcing phase allows
            # before the shutdown's own release write competes, a fenced
            # write, so at most one of the two lands and the other reads
            # a fence outcome. A second cancel (a forced escalation, the
            # shutdown's FORCING phase) interrupts a retry wait as a
            # CancelledError: no write is in flight then (each attempt is
            # shielded to completion) and the row is still this worker's,
            # so it is disowned before the cancellation propagates.
            try:
                cancel_landed = await _terminal_write_with_retry(
                    lambda: backend.mark_cancelled(
                        job.id,
                        worker_id,
                        progress_seq=_cancel_seq,
                        progress_state=_cancel_state_for_write,
                        attempt=job.attempt,
                        claim_epoch=job.claim_epoch,
                    ),
                    log=job_log,
                    job=job,
                    write_name="mark_cancelled",
                )
            except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                # Why: the terminal write is best-effort on this path, the
                # row stays 'running', disowned so lock-lease expiry
                # reclaims it (identical to the success-path infra
                # failure). The CancelledError MUST still propagate below:
                # routing the infra error into generic job-failure handling
                # eats a TaskGroup cancellation and hangs __aexit__ forever.
                cancel_landed = None
                _log_terminal_write_failed(job_log, job, None, infra_exc)
                _disown_job(_disowned_jobs, job)
            except asyncio.CancelledError:
                _disown_job(_disowned_jobs, job)
                raise
            # Announce only a transition the row actually took: on a
            # fenced-out write (the row moved to another owner mid-cancel)
            # or an infra-failed one (the row is still 'running'), a
            # 'cancelled' state-change log line and a terminal publish
            # would each report a move the row never made.
            if cancel_landed:
                log_state_change(ctx.log, from_state="running", to_state="cancelled")
                # Best-effort, bounded, and deliberately after the write:
                # a hook that hangs or raises must not be able to leave the
                # row 'running' behind a lease only the sweep clears. Fires
                # only here, a job cancelled before it ever ran never
                # enters a worker, so no hook can run for it.
                await invoke_on_cancel(
                    actor_config.on_cancel,
                    job,
                    actor_config.on_cancel_timeout,
                    log=job_log,
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
            elif cancel_landed is False:
                _log.debug(
                    "consume-cancel-noop",
                    job_id=str(job.id),
                    from_state="running",
                    to_state="noop",
                )
            raise

        except _TerminalWriteFailed:
            # Success-path terminal write failed with an infra error.
            # Already logged via _log_terminal_write_failed inside the
            # success path.  The job stays ``running``, disowned here so
            # the heartbeat stops renewing it and lock-lease expiry
            # reclaims it.  Do NOT re-dispatch into
            # _handle_generic_exception (that would mislabel the infra
            # error as the actor's failure).
            _disown_job(_disowned_jobs, job)
            return "failed"

        except (
            TimeoutError,
            Snooze,
            RetryAfter,
            ReservationUnavailable,
            ResultTooLarge,
            BaseException,
        ) as e:
            # ``BaseException``, not ``Exception``: an actor body that raises a
            # non-Exception BaseException (a custom BaseException subclass from a
            # dependency, ``SystemExit`` from a sync actor calling ``sys.exit`` in
            # its executor thread) is an ATTEMPT OUTCOME, not worker death. The
            # two delivery shapes differ, and only together cover the contract:
            # a BaseException raised in THIS task's frame (an async actor's
            # body, or the _ActorSystemExitAttemptError carrier the sync-actor
            # thread boundary raises for a raw thread SystemExit, and the tx
            # task boundary likewise) is delivered into this frame by the
            # awaiting task's normal wakeup and lands here; a raw SystemExit
            # left inside a TASK's own step would kill the loop before any
            # wake-up runs, which is why both task boundaries convert it.
            # The previous breadth let such an exception escape this function,
            # strand
            # the row ``running`` until lease expiry (which then relabelled it
            # ``WorkerCrashed`` — a false audit trail: the worker never crashed,
            # the actor raised), and kill the consumer loop task, cancelling every
            # in-flight sibling job. Every mature queue runtime converges on
            # capturing this at the per-job boundary: the failure is recorded
            # truthfully (error_class is the exception's own type name) and the
            # worker survives to run the rest of its fleet. ``KeyboardInterrupt``
            # is deliberately NOT captured: it is interpreter/operator intent,
            # never an actor outcome, and swallowing it would break in-process
            # shutdown flows; it re-raises into the loop-level handler, and the
            # stranded row still has the lease-reclaim path as its recovery.
            if isinstance(e, KeyboardInterrupt):
                raise
            return await _dispatch_exception(
                e,
                backend=backend,
                job=job,
                worker_id=worker_id,
                actor_config=actor_config,
                max_retry_backoff=max_retry_backoff,
                consumer_span=consumer_span,
                log=job_log,
                progress_buffers=_progress_buffers,
                worker_pool=_effective_pool,
                settings=_effective_settings,
                redis_client=_effective_redis,
                error_reporter=error_reporter,
                text=attempt_text,
                disowned_jobs=_disowned_jobs,
                ctx=ctx,
                deps=deps,
            )

        finally:
            # Best-effort crash flush: ensures partial progress_state reaches PG
            # even when the actor raises unexpectedly ().
            if _progress_buffers is not None and _buf is not None:
                # Identity-scoped crash flush (the issue-461 class): the
                # buffer leaves the map only when the key still holds THIS
                # attempt's buffer, and the flushed buffer is the one this
                # attempt installed. A stale attempt whose key a re-claim
                # overwrote must not evict the live attempt's buffer (the
                # flush loop would stop draining the live attempt's
                # progress) nor crash-flush it as its own; the stale
                # buffer's own flush, when it happens, is fenced per-row by
                # its attempt epoch and no-ops against the row.
                if _progress_buffers.get(job.id) is _buf:
                    del _progress_buffers[job.id]
                if _effective_pool is not None and _effective_settings is not None and _buf.dirty:
                    await shield_with_retrieval(
                        _flush_buffer(
                            _effective_pool,
                            _effective_settings.schema_name,
                            job.id,
                            worker_id,
                            _buf,
                            _progress_buffers,
                        )
                    )
            elif _progress_buffers is not None and _buf is not None:
                if _progress_buffers.get(job.id) is _buf:
                    del _progress_buffers[job.id]

            if active_jobs is not None and _active_entry is not None:
                await active_jobs.deregister(job.id, _active_entry)

    finally:
        _parent_tags_var.reset(_parent_tags_token)
        # Why unconditional, even when the terminal write failed: the actor
        # body has stopped either way, so the resource it was holding really
        # is free, keeping the slot until its lease expires would throttle
        # the bucket for no benefit. It is safe to release here because a
        # reservation release is fenced to the exact lease acquired above
        # (taskq.ratelimit.reservation.SlotLease): if this attempt's lease
        # already expired and the redispatched attempt re-acquired the slot,
        # this release frees nothing rather than stealing the live holder's
        # slot.
        if acquired and rate_limit_registry is not None:
            try:
                await shield_with_retrieval(
                    rate_limit_registry.release_for_actor(acquired, pg_pool=worker_pool)
                )
            except Exception as exc:
                _log.warning(
                    "rate_limit_release_failed",
                    job_id=str(job.id),
                    error_class=type(exc).__name__,
                    error_message=str(exc),
                )


def _retrieve_body_task_outcome(task: "asyncio.Task[object]") -> None:
    """Retrieve a detached body task's eventual outcome.

    The one shared retriever for the deadline enforcement's detached tasks
    (the same discipline ``_retrieve_detached_outcome`` applies to the tx
    task and ``shield_with_retrieval`` to a detached terminal write): a
    body whose ``finally`` outlives the deadline keeps unwinding after the
    attempt is already terminal, and without this its eventual exception
    is asyncio "Task exception was never retrieved" noise burying the real
    signal. The outcome itself is deliberately discarded: the attempt is
    terminal, the row is fenced by attempt epoch, and a late failure is
    the expected shape of a hostile unwind.
    """
    with contextlib.suppress(asyncio.CancelledError):
        task.exception()


async def _enforce_start_to_close(
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    job: JobRow,
    ctx: JobContext[BaseModel],
    timeout: float | None,
    unwind_wait: float | None = None,
) -> object:
    """Run the actor body under its ``start_to_close`` deadline.

    Replaces a bare ``asyncio.wait_for(run_actor(...), timeout)``, which
    had three body-side holes this mechanism closes:

    1. THE #791 CONFLATION. ``wait_for`` propagates a body-raised
       ``TimeoutError`` and its own deadline expiry through the same
       exception type, so the consumer routed every body ``TimeoutError``
       (a nested ``wait_for``, a socket read, a futures timeout) into the
       timeout handler: a false ``job_timeout`` log, a false
       ``taskq.jobs.timeouts{kind="start_to_close"}`` increment, the
       deadline's routing for an error that never touched the deadline.
       Here the expiry alone raises ``_StartToCloseExceededError`` (a
       private ``TimeoutError`` subclass the dispatcher routes to the
       timeout handler); the body's own ``TimeoutError`` propagates
       unchanged and takes the ordinary generic-failure path.

    2. THE ABSORBABLE DEADLINE. Since 3.12 ``wait_for`` documents: "If the
       task suppresses the cancellation and returns a value instead, that
       value is returned." A body that caught the deadline's cancellation
       and returned was marked SUCCEEDED, past its own time limit. Here
       the deadline is a first-completed race: a body that returns after
       the deadline fired has its result discarded and the marker raised.

    3. THE DEFERRABLE DEADLINE. ``wait_for`` waits out the body's unwind
       unboundedly: a ``finally`` that awaits (worse, ``await
       asyncio.shield(cleanup())``) deferred the hard limit for as long as
       it liked while the slot sat occupied. Here the body runs in its own
       task and the attempt ends at the deadline, however the body is
       still unwinding: the task is detached tracked (the shutdown
       watchdog accounts for it) and stashed on the ctx (the exit-proof
       hold parks on it, bounded, before the re-pend).

    *unwind_wait* scopes the unwind guarantee to the path that needs it.
    On the AUTONOMOUS path (the default, ``None``) the unwind is never
    sabotaged: no re-cancel is delivered into a task that is honouring
    the first one's cleanup, a legitimate ``finally`` gets to finish, and
    a body that absorbed the cancel and kept WORKING is exactly the
    tracked zombie the watchdog's hard rung exists for -- and no
    sabotage is possible, the autonomous body's connections are its own,
    never shared with the machinery. On the TRANSACTIONAL path
    (``unwind_wait`` set, the exit-wait budget) that unconditional claim
    was once falsified, and the budget is what restores it: the
    transactional body runs inside ``transaction_conn.transaction()``,
    and a ``finally`` awaiting on the SHARED transaction connection was
    still in flight when the marker reached the ``__aexit__``'s ROLLBACK
    -- the two collided on asyncpg's one-operation-at-a-time guard, the
    failing ROLLBACK replaced the marker, and the row recorded the
    collision's ``InterfaceError`` instead of the truthful
    ``TimeoutError`` (the ``job_timeout`` log and the timeouts metric
    lost with it). The tx-path caller therefore passes the budget and
    this enforcement bound-waits the unwind BEFORE the marker
    propagates into the ``__aexit__``: the rollback runs on a quiesced
    connection, and a legitimate ``finally`` on the shared connection
    gets to finish. A hostile unwind outliving the budget proceeds
    detached -- the deadline's win survives, bounded by the budget -- and
    the budget's expiry is exactly the rollback-collision window: the
    marker propagates into the ``__aexit__`` while the connection may
    still be busy, so this enforcement stamps the window on the ctx and
    the transactional consumer's capture boundary translates an
    ``InterfaceError`` arriving under the stamp (only the enforcement's
    own rollback's collision can arrive under it) back to the truthful
    timeout disposition. A body-caused ``InterfaceError`` never crosses
    that boundary under the stamp -- the body's exceptions are confined
    to its detached task once the deadline has fired -- and one arriving
    before the deadline (surfacing through ``body_task.result()``) keeps
    its own class.

    The body-task boundary also converts ``SystemExit`` to the
    ``_ActorSystemExitAttemptError`` carrier, the third task boundary
    after the sync executor thread and the tx task (CPython's
    ``Task.__step`` re-raises that pair bare and would kill the loop).

    External cancellation (shutdown interrupt, operator phase 2) is
    forwarded to the body task and re-raised: the consumer's cancel arm
    runs while the body unwinds detached, and the interrupt hold reads the
    ctx handle instead of the old "the cancellation propagated through the
    body's frames, so an async actor has provably unwound" assumption.
    """
    if timeout is None:
        return await run_actor(job, ctx)

    loop = asyncio.get_running_loop()

    async def _body() -> object:
        try:
            return await run_actor(job, ctx)
        except SystemExit as exc:
            # This coroutine is a task body now: Task.__step re-raises
            # exactly (KeyboardInterrupt, SystemExit) bare after
            # set_exception, killing the loop before the wait below can
            # run (the same conversion _run_actor_in_tx_tracked and
            # _run_sync_actor_tracked._thread_body apply at their task
            # boundaries). _dispatch_exception unwraps the carrier, so the
            # row records the actor's own SystemExit.
            raise _ActorSystemExitAttemptError(exc) from exc

    body_task: asyncio.Task[object] = asyncio.ensure_future(_body())
    ctx._set_actor_body_task(body_task)  # pyright: ignore[reportPrivateUsage]  # Why: the deadline enforcement is the designated writer of the body-task handle (see the setter's contract).

    expired = False
    deadline_latch: asyncio.Future[None] = loop.create_future()

    def _fire() -> None:
        nonlocal expired
        expired = True
        body_task.cancel()
        deadline_latch.set_result(None)

    deadline_handle = loop.call_later(timeout, _fire)
    try:
        await asyncio.wait(
            {body_task, deadline_latch},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if expired and unwind_wait is not None and not body_task.done():
            # THE TRANSACTIONAL PATH'S BOUND UNWIND WAIT. The deadline won
            # the race and the unwind (the body's except/finally frames)
            # is still running. On the transactional path that unwind may
            # await on the SHARED transaction connection, and the marker
            # raised below reaches the transaction __aexit__'s ROLLBACK:
            # letting them race once collided on asyncpg's one-operation-
            # at-a-time guard, the failing ROLLBACK replaced the marker in
            # __aexit__, and the row recorded InterfaceError instead of
            # the truthful TimeoutError (the job_timeout log and the
            # timeouts metric lost with it). Wait the unwind out, up to
            # the exit-wait budget: a legitimate finally finishes and the
            # rollback runs on a quiesced connection. At budget expiry the
            # marker proceeds anyway -- the deadline's win survives,
            # bounded by the budget -- and the still-hostile unwind is
            # detached tracked by the finally below.
            await asyncio.wait({body_task}, timeout=unwind_wait)
    except asyncio.CancelledError:
        # An external cancellation of THIS task (a shutdown interrupt, an
        # operator phase 2): re-raise and let the finally below forward
        # the cancellation to the body task, detach it tracked, and leave
        # the unwind proof to the ctx handle the interrupt hold reads.
        raise
    finally:
        deadline_handle.cancel()
        deadline_latch.cancel()
        if body_task.done():
            with contextlib.suppress(asyncio.CancelledError):
                # Retrieve a raced-out body outcome (an exception landing
                # in the same loop iteration the deadline fired) so
                # asyncio never reports it unretrieved.
                body_task.exception()
        else:
            if not expired:
                # An external cancellation of THIS task (a shutdown
                # interrupt, an operator phase 2): the consumer's cancel
                # arm owns the row now and the body must get the cancel
                # it owes. The unwind is detached (below), so the arm
                # runs while the body unwinds and the exit-proof hold
                # reads the ctx handle instead of the old inline
                # propagation assumption.
                body_task.cancel()
            # Detach tracked, retrieval pinned: the exit-proof hold /
            # shutdown watchdog own the unwind from here. The attempt's
            # terminal path does not wait on it, and an unwinder is never
            # re-cancelled: a finally honouring the deadline's first
            # cancellation gets to finish; a body that swallowed it and
            # kept working is the tracked zombie the watchdog's hard rung
            # owns.
            body_task.add_done_callback(_retrieve_body_task_outcome)
            register_tracked_actor_handle(body_task)

    if expired:
        # The deadline won the race. The body may have finished in the
        # same loop iteration (its outcome is raced out and discarded),
        # may have finished inside the tx path's bound unwind wait (the
        # connection the marker is about to reach is quiesced), or may
        # still be unwinding (detached above): either way the attempt is
        # a timeout, never a success, however the body feels about it.
        if unwind_wait is not None:
            # THE TX PATH'S ROLLBACK-COLLISION WINDOW, OPEN. The marker
            # raised below is now the exception in flight through the
            # transaction __aexit__, and the __aexit__'s own ROLLBACK is
            # the only statement the tx path issues on the SHARED
            # connection from here. If the unwind still holds that
            # connection (a shielded conn op past deadline + budget), the
            # ROLLBACK collides on asyncpg's one-operation-at-a-time
            # guard and the collision's InterfaceError replaces the
            # marker -- and the body's own exceptions are confined to its
            # detached task, unable to cross the capture boundary once
            # the deadline has fired. So stamp the window: the
            # transactional consumer's capture boundary re-labels an
            # InterfaceError arriving under this stamp to the truthful
            # TimeoutError (the row, the job_timeout log and the metric
            # all stay the deadline's), and never touches one arriving
            # without it (a body-caused InterfaceError keeps its own
            # class). Read the stamp's contract on the ctx setter.
            ctx._set_tx_rollback_collision_window()
        raise _StartToCloseExceededError from None
    # The body finished before the deadline: its return value, or its own
    # exception (including its own TimeoutError, which now routes as the
    # ordinary failure it is), surfaced by the result() read.
    return body_task.result()


async def _consume_transactional(
    backend: Backend,
    job: JobRow,
    worker_id: UUID,
    ctx: JobContext[BaseModel],
    enqueuer: SubJobEnqueuer,
    transaction_conn: ConnLike,
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    actor_config: ActorConfigLike,
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
    fallback_result_ttl: timedelta | None = None,
    disowned_jobs: set[UUID] | None = None,
    deps: WorkerDeps | None = None,
) -> AttemptOutcome:
    """Transactional success/failure path when a transaction conn is available.

    Opens a transaction on the job's transaction connection (one
    connection per job on the per-slot path, so concurrent slots never
    nest), runs the actor inside it, commits on success (shielded), and
    routes exceptions to the appropriate handler with
    ``discard_buffer()`` called before each terminal write.

    Returns the job outcome, ``"succeeded"`` on successful commit,
    ``"failed"`` or ``"scheduled"`` when an exception was handled
    internally.

    ``fallback_result_ttl`` is forwarded to ``mark_succeeded_with_conn``
    on the success path, see ``consume_one_job``.
    """
    completion: object = None
    _tx_result: object = None
    actor_finished = False

    async def _run_actor_in_tx() -> object:
        nonlocal completion
        nonlocal _tx_result
        nonlocal actor_finished
        _preserved_exc: Snooze | RetryAfter | None = None
        _re_enqueue_list: list[EnqueueArgs] = []

        async with transaction_conn.transaction():
            await transaction_conn.execute("SAVEPOINT _tq_actor")
            result: object = None
            try:
                # Why no shield here: the deadline enforcement below runs
                # the actor in its own task and the deadline wins over
                # whatever the body does after expiry (a cancellation-
                # absorbing return, a hostile finally): see
                # _enforce_start_to_close for the three holes a bare
                # wait_for left (the #791 TimeoutError conflation, the
                # absorbable deadline, the deferrable unwind). The
                # start_to_close cancellation reaches the actor exactly as
                # on the autonomous path, PLUS the tx path's bound unwind
                # wait: this actor runs inside
                # `transaction_conn.transaction()`, whose __aexit__ answers
                # the marker with a ROLLBACK on the SHARED connection, so
                # the unwind is waited out (up to _TX_UNWIND_WAIT_BUDGET)
                # before the marker propagates -- the rollback runs on a
                # quiesced connection instead of colliding with the
                # unwind's own statement on the asyncpg one-operation-at-
                # a-time guard and replacing the marker (the row once
                # recorded InterfaceError instead of the truthful
                # TimeoutError). PAST the budget the marker propagates
                # anyway and the collision can still happen; the capture
                # boundary at the shield below translates our own
                # rollback's collision (only it can arrive under the
                # rollback-collision window the enforcement stamps) back
                # to the truthful TimeoutError. Transaction integrity is the
                # OUTER shield's job (`shield(
                # _run_actor_in_tx())` below): that one decouples EXTERNAL
                # cancellation from an in-flight commit.  A cancel landing
                # mid-statement on transaction_conn is safe, asyncpg sends a
                # CancelRequest, leaves the connection usable and puts the
                # transaction in a failed state, which the enclosing
                # `async with transaction_conn.transaction()` then rolls back; the
                # timeout's own terminal write goes through the worker pool,
                # not this connection.
                result = await _enforce_start_to_close(
                    run_actor, job, ctx, timeout, unwind_wait=_TX_UNWIND_WAIT_BUDGET
                )
                # Why a second flag beside `completion`: the outer shield's
                # CancelledError handler must distinguish "the actor
                # attempt is still running" (the external cancel has to be
                # delivered to it) from "the actor returned and the commit
                # machinery is in flight" (the shield's documented job: an
                # in-flight commit survives the external cancel). No await
                # sits between the actor's return and this assignment, so
                # the two windows cannot blur.
                actor_finished = True
                # Why NO cancel-phase check here: a cancel request observed
                # while the actor ran does not decide the terminal state ,
                # the actor's own outcome does. The actor returned a value,
                # so the attempt succeeded; an actor that abandons its unit
                # of work signals it by RAISING CancelledError (handled by
                # the outer handler), never by returning. Cancelling a
                # returned actor here would discard a computed result from
                # a terminal job nothing re-runs (and roll back the writes
                # it completed). That is the resolution the
                # cooperative-cancel contract dictates: a job that returns
                # after a stop or cancel request completes keeps its
                # result.
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
                result_bytes = _encode_result(
                    result,
                    settings.result_max_bytes if settings is not None else MAX_RESULT_BYTES,
                )
                try:
                    succeeded_landed = await backend.mark_succeeded_with_conn(
                        transaction_conn,
                        job.id,
                        worker_id,
                        result_bytes=result_bytes,
                        progress_seq=_pseq,
                        progress_state=_pstate,
                        fallback_result_ttl=fallback_result_ttl,
                        attempt=job.attempt,
                        claim_epoch=job.claim_epoch,
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    _log_terminal_write_failed(log, job, None, infra_exc)
                    raise _TerminalWriteFailed(infra_exc) from infra_exc
                if not succeeded_landed:
                    # Fenced out: the row moved underneath this attempt
                    # (reclaimed and re-claimed at a newer attempt epoch).
                    # The actor's writes must NOT join this transaction's
                    # commit, they are a stale attempt's side effects, and
                    # the live attempt will produce its own. Raising rolls
                    # the transaction back; the buffered sub-enqueues go
                    # with it (their claims belong to this attempt's unit
                    # of work, and the flush below never runs).
                    log.warning(
                        "terminal-write-fenced-out",
                        kind="terminal_write_fenced_out",
                        job_id=str(job.id),
                        worker_id=str(worker_id),
                        attempt=job.attempt,
                        write="mark_succeeded_with_conn",
                    )
                    enqueuer.discard_buffer()
                    raise _AttemptFencedOut()
                if _pbuf is not None:
                    _pbuf.dirty = False
                await transaction_conn.execute("RELEASE SAVEPOINT _tq_actor")
            except (Snooze, RetryAfter) as exc:
                _preserved_exc = exc
                try:
                    await transaction_conn.execute("ROLLBACK TO SAVEPOINT _tq_actor")
                except Exception as exc:
                    log.warning(
                        "savepoint_rollback_failed",
                        kind="savepoint_rollback_failed",
                        job_id=str(job.id),
                        error_class=type(exc).__name__,
                        error_message=str(exc),
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
                        job_id=str(args.id),
                        error_class=type(exc).__name__,
                        error_message=str(exc),
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
            # The parent has already been reported as succeeded, so every
            # failed child enqueue is a job the caller believes exists but
            # does not, count them before the log line, so the catch can
            # never lose the signal.
            record_sub_enqueue_failure(job.actor, len(sub_err.failed_items))
            log.error(
                "sub_enqueue_flush_failed",
                kind="sub_enqueue_flush_failed",
                job_id=str(job.id),
                failed_count=len(sub_err.failed_items),
                failed_job_ids=[str(args.id) for args, _ in sub_err.failed_items],
                failed_details=[
                    {
                        "job_id": str(args.id),
                        "error_class": type(exc).__name__,
                        "error_message": str(exc),
                    }
                    for args, exc in sub_err.failed_items
                ],
            )
        completion = _OK
        _tx_result = result
        return _OK

    def _retrieve_detached_outcome(task: asyncio.Task[object]) -> None:
        # Why: on external cancellation the shield leaves the transaction
        # task running detached (an in-flight commit must survive the
        # cancel). Nobody awaits it afterwards, so its eventual outcome
        # must be retrieved here or asyncio reports "Task exception was
        # never retrieved", noise that buries the real signal. The
        # outcome itself is deliberately discarded: the dispatch path's
        # connection release terminates a still-open transaction (see
        # _release_slot_conn), so a detached task's late failure is
        # expected, and its late success is superseded by the
        # cancellation handling below. task.exception() raises
        # CancelledError when the task ended cancelled, the only thing
        # suppressed here.
        with contextlib.suppress(asyncio.CancelledError):
            task.exception()

    # Why an explicit task instead of shielding the coroutine directly:
    # the handle is needed on the cancellation path to retrieve the
    # detached outcome (see _retrieve_detached_outcome).
    #
    # Why the SystemExit conversion wraps the task body: this coroutine IS
    # the tx task's step, and CPython's Task.__step re-raises exactly
    # (KeyboardInterrupt, SystemExit) bare after set_exception, killing the
    # loop before the shielded await below can run (the same mechanism
    # _run_sync_actor_tracked's thread boundary converts for a sync actor).
    # An async actor's sys.exit() is a job-level failure, not worker death:
    # the carrier crosses as an ordinary Exception and _dispatch_exception
    # unwraps it, so the row records the actor's own SystemExit.
    # KeyboardInterrupt stays raw (operator intent, never an actor
    # outcome); the fenced/terminal-write sentinels and CancelledError are
    # not SystemExit and pass untouched.
    async def _run_actor_in_tx_tracked() -> object:
        try:
            return await _run_actor_in_tx()
        except SystemExit as exc:
            raise _ActorSystemExitAttemptError(exc) from exc

    tx_task: asyncio.Task[object] = asyncio.create_task(_run_actor_in_tx_tracked())
    try:
        try:
            await asyncio.shield(tx_task)
        except asyncpg.exceptions.InterfaceError as exc:
            # THE CAPTURE BOUNDARY OF THE ROLLBACK-COLLISION WINDOW. Under
            # the stamp (see the window's opener in _enforce_start_to_close)
            # the deadline's marker was the exception in flight through the
            # transaction __aexit__, whose ROLLBACK is the only statement
            # the tx path issues on the SHARED connection from there: an
            # InterfaceError arriving now can only be that ROLLBACK
            # colliding with a body-owned statement still in flight (the
            # hostile unwind holding the connection past the exit-wait
            # budget), never the body's own -- the body's exceptions are
            # confined to its detached task once the deadline has fired.
            # So re-label the collision to the truthful timeout
            # disposition: the marker routes to the timeout handler and
            # the row, the job_timeout log and the timeouts metric all
            # record the deadline, not the collision. Without the stamp
            # (no deadline, or the body's own InterfaceError surfacing
            # through body_task.result() before the deadline fired) the
            # InterfaceError propagates unchanged: the re-label must never
            # launder a genuine body-caused one. The stamp is taken (and
            # closed) exactly once, so one window justifies one re-label.
            if ctx._take_tx_rollback_collision_window():
                raise _StartToCloseExceededError(
                    "the start_to_close deadline fired; the transaction rollback"
                    " collided with the body's still-hostile unwind on the shared"
                    " connection and the collision was re-labeled to the deadline"
                ) from exc
            raise
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
        # (commit happened), do NOT route to mark_cancelled, the row is
        # already terminal, and a cancel write against it must not land.
        if completion is not _OK:
            if not actor_finished:
                # The actor attempt is still running and nothing else
                # ever delivers this cancellation to it, with no
                # start_to_close bound (timeout None) it would run to
                # completion detached, committing side effects after the
                # row already says cancelled. Cancelling tx_task here
                # mirrors the autonomous path, where the same external
                # cancel propagates through wait_for into the actor and
                # the enclosing transaction rolls back.
                tx_task.cancel()
            # The unwind is ASYNCHRONOUS: savepoint rollback, the
            # enclosing transaction's rollback, and, when the actor is
            # a sync def, an executor thread the cancel cannot reach at
            # all. The re-raise below lands in the consumer's
            # cancellation handler while tx_task is still unwinding, so
            # the handle is stashed on the ctx: that handler parks on
            # it, bounded, before releasing the row, and holds the
            # release back when the unwind (or the thread inside it)
            # never finishes.
            ctx._set_tx_unwind_task(tx_task)  # pyright: ignore[reportPrivateUsage]  # Why: the transactional consumer is the designated writer of the unwind handle (see the setter's contract); the consumer's shutdown arm is its designated reader.
            register_tracked_actor_handle(tx_task)  # pyright: ignore[reportPrivateUsage]  # Why: the process-wide twin of the ctx stash: a tx task still unwinding past the TaskGroup keeps the shutdown watchdog armed (see await_tracked_actor_reap), the same as a live sync-actor thread.
            # Past the actor (commit machinery in flight) the detached
            # task is deliberately left to finish: its eventual outcome
            # must be retrieved here or asyncio reports "Task exception
            # was never retrieved". The outcome itself is discarded, the
            # dispatch path's connection release terminates a still-open
            # transaction (see _release_slot_conn), and a detached
            # task's late success is superseded by the cancellation
            # handling below. task.exception() raises CancelledError when
            # the task ended cancelled, the only outcome suppressed.
            tx_task.add_done_callback(_retrieve_detached_outcome)
        raise

    except _AttemptFencedOut:
        # The success write's fence matched no row (the attempt was
        # reclaimed; a later attempt owns the row). The raise already
        # rolled the actor's transaction back, nothing here terminated
        # the job, so the outcome is the one batch policy and dispatch
        # metrics treat as "not this dispatch's to move".
        return "noop"

    except (
        TimeoutError,
        Snooze,
        RetryAfter,
        ReservationUnavailable,
        ResultTooLarge,
        BaseException,
    ) as e:
        # ``BaseException``, not ``Exception``: the same per-attempt
        # capture contract consume_one_job's final arm applies (see its
        # comment). Without it a non-Exception BaseException from the
        # actor body would skip the pre_handler below, leaking this
        # attempt's buffered sub-enqueues past the transaction rollback,
        # and skip the truthful error stamping entirely. KeyboardInterrupt
        # keeps its interpreter/operator semantics, as there.
        # ``SystemExit`` from an async actor arrives as the
        # _ActorSystemExitAttemptError carrier (the tx task's own step would
        # otherwise re-raise it bare and kill the loop, see
        # _run_actor_in_tx_tracked above); _dispatch_exception unwraps it,
        # so the row records the actor's own SystemExit.
        if isinstance(e, KeyboardInterrupt):
            raise
        return await _dispatch_exception(
            e,
            backend=backend,
            job=job,
            worker_id=worker_id,
            actor_config=actor_config,
            max_retry_backoff=max_retry_backoff,
            consumer_span=consumer_span,
            log=log,
            progress_buffers=progress_buffers,
            worker_pool=worker_pool,
            settings=settings,
            redis_client=redis_client,
            pre_handler=enqueuer.discard_buffer,
            error_reporter=error_reporter,
            disowned_jobs=disowned_jobs,
            ctx=ctx,
            deps=deps,
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
    fallback_result_ttl: timedelta | None = None,
) -> AttemptOutcome:
    """Autonomous success path, no LOOP-scope connection.

    Returns ``"succeeded"`` when the terminal write landed and
    ``"noop"`` when its fence matched no row (the attempt was reclaimed
    and a later attempt owns the row, nothing here terminated it).

    ``fallback_result_ttl`` is forwarded to ``mark_succeeded``, see
    ``consume_one_job``.
    """
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

    result = await _enforce_start_to_close(run_actor, job, ctx, timeout)

    # Why NO cancel-phase check between the actor's return and the success
    # write: the actor returned a value, so the attempt's outcome is
    # success, a cancel REQUEST observed while it ran is not a verdict
    # over the work it completed (cancellation is cooperative: the actor
    # that abandons its unit of work raises CancelledError and lands in
    # the outer handler; the actor that degrades gracefully and returns
    # has finished). Discarding a returned result here wrote 'cancelled'
    # over completed work on a terminal row nothing re-runs, and reported
    # a different outcome than the caller was handed.

    _pbuf = await _pre_terminal_flush(job, worker_id, progress_buffers, _auto_pool, _auto_settings)
    _pseq, _pstate = _seq_and_state_after_flush_attempt(_pbuf)

    result_bytes = _encode_result(
        result,
        _auto_settings.result_max_bytes if _auto_settings is not None else MAX_RESULT_BYTES,
    )
    try:
        succeeded_landed = await _terminal_write_with_retry(
            lambda: backend.mark_succeeded(
                job.id,
                worker_id,
                result_bytes=result_bytes,
                progress_seq=_pseq,
                progress_state=_pstate,
                fallback_result_ttl=fallback_result_ttl,
                attempt=job.attempt,
                claim_epoch=job.claim_epoch,
            ),
            log=log,
            job=job,
            write_name="mark_succeeded",
        )
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
        _log_terminal_write_failed(log, job, None, infra_exc)
        raise _TerminalWriteFailed(infra_exc) from infra_exc
    if not succeeded_landed:
        # Fenced out: the row moved underneath this attempt (a lease
        # reclaim re-pended it; a later attempt owns it now). The actor's
        # result is NOT the job's outcome, no success hook (a hook with
        # external side effects would fire once here and once for the
        # attempt that wins the row), no terminal publish, and the
        # outcome reported is "noop" so batch policy and dispatch metrics
        # never move for a write that matched nothing.
        log.warning(
            "terminal-write-fenced-out",
            kind="terminal_write_fenced_out",
            job_id=str(job.id),
            worker_id=str(worker_id),
            attempt=job.attempt,
            write="mark_succeeded",
        )
        return "noop"
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
    return "succeeded"
