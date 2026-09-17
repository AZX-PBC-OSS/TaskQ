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
``mark_interrupted`` — refunded, counted, never terminalised. The row is
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
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,
    AttemptOutcome,
    _AttemptFencedOut,
    _dispatch_exception,
    _handle_reservation_class_denied,
    _log_terminal_write_failed,
    _terminal_write_with_retry,
    _TerminalWriteFailed,
)
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import POOL_INFRA_EXCEPTIONS, WorkerDeps

if TYPE_CHECKING:
    import redis.asyncio as redis_async

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_OK = object()


def _rate_limit_dependency_exceptions() -> tuple[type[BaseException], ...]:
    """The exception family a limiter acquire raises when its STORE
    failed to answer — Redis dead, or the PG fallback behind it dead or
    never wired.

    The PG members are the shared pool-infra classification
    (:data:`POOL_INFRA_EXCEPTIONS`, owned by worker.deps) plus the typed
    no-pool error the ratelimit PG delegates raise when the fallback is
    reached without an injected pool (a store that cannot answer, not a
    job defect — a ``RuntimeError`` subclass so the fail-loud pins hold);
    Redis joins only when the extra is installed — without redis-py no
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
"""What a rate-limit acquire raises when a store dependency — not the
job, not the actor — failed to answer. Recognised at the acquire
boundary below and failed closed as the limiter's denial; anything else
from the composition stays loud through the generic handler."""

_DEPENDENCY_FAILURE_LOG_WINDOW_S: Final[float] = 60.0
"""Window gating the acquire dependency-failure WARNING. A sustained
store outage fails every rate-limited dispatch, and one warning line
per denial is a log flood, not a signal — the same bound the registry's
keyed heal-failure emission applies. The per-occurrence aggregate stays
on the ``ratelimit.acquire_dependency_failures`` counter."""

_dependency_failure_warned: dict[str, float] = {}
"""Monotonic stamp of the last emitted dependency-failure WARNING, keyed
by error class (bounded: the stores' exception vocabulary)."""


def _encode_result(result: object, max_bytes: int = MAX_RESULT_BYTES) -> bytes | None:
    """Serialize an actor return value to orjson bytes exactly once.

    ``BaseModel`` results are dumped via ``model_dump(mode="json")``;
    ``dict`` results are dumped as-is (the actor contract guarantees
    ``dict[str, object]``); all other types return ``None`` (no result
    stored).  Raises :class:`ResultTooLarge` (non-retryable — a re-run
    returns the same oversized value) when the serialized size exceeds
    *max_bytes*, which the callers take from
    ``WorkerSettings.result_max_bytes`` and which defaults to
    :data:`~taskq.constants.MAX_RESULT_BYTES`.

    The returned bytes are the result's ONLY serialization: they reach the
    backend as the ``result_bytes`` terminal-write parameter, which binds
    them decoded and stores ``len`` as ``result_size_bytes`` without
    serializing again.  The NUL guard deliberately does not run here — it
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
        await shield_with_retrieval(
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
        handler_result: AttemptOutcome = await handler(
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
    # A noop means no transition happened — the row moved underneath
    # this dispatch (a reclaim race) — so publishing the requested
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
    caller BEFORE any token is consumed — an invalid payload must not
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

    ``enqueuer`` is the SubJobEnqueuer the dispatch path selected — the
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
    The reporter call is wrapped in a try/except — a failing reporter
    never crashes the worker.

    ``transaction_conn`` is the connection the job's transaction runs
    on — the connection this dispatch acquired from the worker's slot
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

    _rl_limits: Sequence[str | KeyedRateLimitRef | TokenBucket | SlidingWindow] = (
        rate_limits if rate_limits is not None else ()
    )
    _rl_reservations: Sequence[str | KeyedReservationRef | ConcurrencyReservation] = (
        reservations if reservations is not None else ()
    )
    _needs_acquire = bool(_rl_limits or _rl_reservations) and rate_limit_registry is not None

    # Resolve the typed model BEFORE rate-limit acquisition: an invalid payload must not
    # acquire — and non-refundably burn — a rate-limit token for an actor
    # body that can never run. acquire_for_actor then receives the validated
    # BaseModel, so keyed refs either hit the registry's isinstance fast path
    # (same model) or re-validate the model's dump, which carries the actor
    # model's applied defaults/aliases — not the raw row dict. The wrapped
    # PayloadValidationError propagates to the caller, exactly as it did
    # from the in-try fallback and as non-dependency acquire-path errors
    # still do (a wiring or programming defect must stay loud): callers
    # (dispatch_one_job's outer except, the in-memory runner's catch) own
    # the terminal write for pre-actor failures. A STORE-dependency failure
    # is the exception — the acquire boundary below fails it closed as the
    # limiter's own denial, because an infrastructure outage is not a job
    # outcome either.
    if validated_payload is None:
        # The row's stored version rides the raise — not the helper's
        # current-version default — so a row that predates a payload
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
            # when the job moved underneath us) — its result is this
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
            # exception here has exactly one provenance — and one
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
            # WARNING is window-gated — a sustained outage denies every
            # rate-limited dispatch, and a warning per denial is a log
            # flood, not a signal.
            #
            # Non-consuming: the denial is infra backpressure about a
            # job whose actor never ran, so it rides mark_snoozed's
            # 'unavailable' arm (attempt refunded, no terminal arm) —
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
            # store failure automatically — the chain is what the
            # terminal-write infra log and any traceback reader see.
            denial.__cause__ = exc
            return await _run_terminal_path(
                job=job,
                worker_id=worker_id,
                progress_buffers=deps.progress_buffers if deps is not None else None,
                worker_pool=deps.worker_pool if deps is not None else worker_pool,
                settings=deps.settings if deps is not None else settings,
                redis_client=deps.redis_client if deps is not None else redis_client,
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

    if _progress_buffers is not None:
        # attempt seeds the buffer's flush-fence epoch: a stale flush
        # landing after a same-worker redispatch to a later attempt
        # no-ops instead of clobbering the new epoch's progress.
        _buf = _ProgressBuffer(job_id=job.id, base_seq=job.progress_seq, attempt=job.attempt)
        _progress_buffers[job.id] = _buf

    _parent_tags_token = _parent_tags_var.set(tuple(job.tags))

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
                    # the terminal-write sentinels are not attempt failures.
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
            _cancel_buf = (
                _progress_buffers.pop(job.id, None) if _progress_buffers is not None else None
            )
            _cancel_seq, _cancel_state = _terminal_seq_and_state(_cancel_buf)
            _cancel_state_for_write = (
                _cancel_state if _cancel_buf is not None and _cancel_buf.dirty else None
            )
            if entry is not None and entry.cancel_origin is CancelOrigin.SHUTDOWN:
                # Infrastructure interruption (SIGTERM / drain monitor),
                # not an operator cancel: release the attempt back to the
                # fleet with its budget refunded instead of terminalising
                # it. hold=0 — the actor already unwound, so the row is
                # genuinely free and lands pending at the head of the
                # order (River's JobSetStateInterrupted shape: available
                # immediately, attempt refunded, no error recorded). The
                # row is the final arbiter: a "noop" means an operator
                # cancel raced the deploy onto the row, and the attempt
                # falls through to the ordinary cancel write below.
                try:
                    interrupt_outcome = await shield_with_retrieval(
                        backend.mark_interrupted(
                            job.id,
                            worker_id,
                            attempt=job.attempt,
                            hold=timedelta(0),
                            progress_seq=_cancel_seq,
                            progress_state=_cancel_state_for_write,
                        )
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    # Best-effort, exactly like the cancel write below: the
                    # row stays 'running' and lock-lease expiry reclaims
                    # it. Do NOT fall through to mark_cancelled — the row
                    # carries no operator cancel, so a cancel write here
                    # would terminalise an infrastructure interruption.
                    _log_terminal_write_failed(_log, job, None, infra_exc)
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
                # "noop": the fence declined — an operator cancel landed on
                # the row first (cancel_phase != 0). The operator's request
                # owns the terminal state; fall through to mark_cancelled.
            consumer_span.add_event(
                "lifecycle.cancelled",
                attributes={"from_state": "running", "to_state": "cancelled"},
            )
            cancel_landed: bool | None
            try:
                cancel_landed = await shield_with_retrieval(
                    backend.mark_cancelled(
                        job.id,
                        worker_id,
                        progress_seq=_cancel_seq,
                        progress_state=_cancel_state_for_write,
                        attempt=job.attempt,
                    )
                )
            except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                # Why: the terminal write is best-effort on this path — the
                # row stays 'running' and lock-lease expiry reclaims it
                # (identical to the success-path infra failure). The
                # CancelledError MUST still propagate below: routing the
                # infra error into generic job-failure handling eats a
                # TaskGroup cancellation and hangs __aexit__ forever.
                cancel_landed = None
                _log_terminal_write_failed(_log, job, None, infra_exc)
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
                # only here — a job cancelled before it ever ran never
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
                max_retry_backoff=max_retry_backoff,
                consumer_span=consumer_span,
                log=job_log,
                progress_buffers=_progress_buffers,
                worker_pool=_effective_pool,
                settings=_effective_settings,
                redis_client=_effective_redis,
                error_reporter=error_reporter,
                text=attempt_text,
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
                    await shield_with_retrieval(
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
        _parent_tags_var.reset(_parent_tags_token)
        # Why unconditional, even when the terminal write failed: the actor
        # body has stopped either way, so the resource it was holding really
        # is free — keeping the slot until its lease expires would throttle
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
) -> AttemptOutcome:
    """Transactional success/failure path when a transaction conn is available.

    Opens a transaction on the job's transaction connection (one
    connection per job on the per-slot path, so concurrent slots never
    nest), runs the actor inside it, commits on success (shielded), and
    routes exceptions to the appropriate handler with
    ``discard_buffer()`` called before each terminal write.

    Returns the job outcome — ``"succeeded"`` on successful commit,
    ``"failed"`` or ``"scheduled"`` when an exception was handled
    internally.

    ``fallback_result_ttl`` is forwarded to ``mark_succeeded_with_conn``
    on the success path — see ``consume_one_job``.
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
                # Why no shield here: asyncio.shield leaves the shielded
                # awaitable running when its waiter is cancelled, so
                # wait_for(shield(actor)) enforced the deadline on the WAIT
                # and not on the actor — the attempt was marked timed out and
                # became retryable elsewhere while the actor body carried on,
                # duplicating every side effect past the timeout point.  The
                # start_to_close cancellation must reach the actor, exactly as
                # on the autonomous path (_consume_autonomous).  Transaction
                # integrity is the OUTER shield's job (`shield(
                # _run_actor_in_tx())` below): that one decouples EXTERNAL
                # cancellation from an in-flight commit.  A cancel landing
                # mid-statement on transaction_conn is safe — asyncpg sends a
                # CancelRequest, leaves the connection usable and puts the
                # transaction in a failed state, which the enclosing
                # `async with transaction_conn.transaction()` then rolls back; the
                # timeout's own terminal write goes through the worker pool,
                # not this connection.
                result = await asyncio.wait_for(run_actor(job, ctx), timeout=timeout)
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
                # while the actor ran does not decide the terminal state —
                # the actor's own outcome does. The actor returned a value,
                # so the attempt succeeded; an actor that abandons its unit
                # of work signals it by RAISING CancelledError (handled by
                # the outer handler), never by returning. Cancelling a
                # returned actor here would discard a computed result from
                # a terminal job nothing re-runs (and roll back the writes
                # it completed). This is the resolution the vendored
                # references implement: a job that returns after a stop or
                # cancel request completes (River's executor reports the
                # result of a soft-stopped job that returned;
                # vendor/river/internal/jobexecutor/job_executor.go).
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
                    )
                except _TERMINAL_WRITE_INFRA_EXCEPTIONS as infra_exc:
                    _log_terminal_write_failed(log, job, None, infra_exc)
                    raise _TerminalWriteFailed(infra_exc) from infra_exc
                if not succeeded_landed:
                    # Fenced out: the row moved underneath this attempt
                    # (reclaimed and re-claimed at a newer attempt epoch).
                    # The actor's writes must NOT join this transaction's
                    # commit — they are a stale attempt's side effects, and
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
            # does not — count them before the log line, so the catch can
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
        # never retrieved" — noise that buries the real signal. The
        # outcome itself is deliberately discarded: the dispatch path's
        # connection release terminates a still-open transaction (see
        # _release_slot_conn), so a detached task's late failure is
        # expected, and its late success is superseded by the
        # cancellation handling below. task.exception() raises
        # CancelledError when the task ended cancelled — the only thing
        # suppressed here.
        with contextlib.suppress(asyncio.CancelledError):
            task.exception()

    # Why an explicit task instead of shielding the coroutine directly:
    # the handle is needed on the cancellation path to retrieve the
    # detached outcome (see _retrieve_detached_outcome).
    tx_task: asyncio.Task[object] = asyncio.create_task(_run_actor_in_tx())
    try:
        await asyncio.shield(tx_task)
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
        # (commit happened), do NOT route to mark_cancelled — the row is
        # already terminal, and a cancel write against it must not land.
        if completion is not _OK:
            if not actor_finished:
                # The actor attempt is still running and nothing else
                # ever delivers this cancellation to it — with no
                # start_to_close bound (timeout None) it would run to
                # completion detached, committing side effects after the
                # row already says cancelled. Cancelling tx_task here
                # mirrors the autonomous path, where the same external
                # cancel propagates through wait_for into the actor and
                # the enclosing transaction rolls back.
                tx_task.cancel()
            # Past the actor (commit machinery in flight) the detached
            # task is deliberately left to finish: its eventual outcome
            # must be retrieved here or asyncio reports "Task exception
            # was never retrieved". The outcome itself is discarded — the
            # dispatch path's connection release terminates a still-open
            # transaction (see _release_slot_conn), and a detached
            # task's late success is superseded by the cancellation
            # handling below. task.exception() raises CancelledError when
            # the task ended cancelled — the only outcome suppressed.
            tx_task.add_done_callback(_retrieve_detached_outcome)
        raise

    except _AttemptFencedOut:
        # The success write's fence matched no row (the attempt was
        # reclaimed; a later attempt owns the row). The raise already
        # rolled the actor's transaction back — nothing here terminated
        # the job, so the outcome is the one batch policy and dispatch
        # metrics treat as "not this dispatch's to move".
        return "noop"

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
    fallback_result_ttl: timedelta | None = None,
) -> AttemptOutcome:
    """Autonomous success path — no LOOP-scope connection.

    Returns ``"succeeded"`` when the terminal write landed and
    ``"noop"`` when its fence matched no row (the attempt was reclaimed
    and a later attempt owns the row — nothing here terminated it).

    ``fallback_result_ttl`` is forwarded to ``mark_succeeded`` — see
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

    result = await asyncio.wait_for(run_actor(job, ctx), timeout=timeout)

    # Why NO cancel-phase check between the actor's return and the success
    # write: the actor returned a value, so the attempt's outcome is
    # success — a cancel REQUEST observed while it ran is not a verdict
    # over the work it completed (cancellation is cooperative: the actor
    # that abandons its unit of work raises CancelledError and lands in
    # the outer handler; the actor that degrades gracefully and returns
    # has finished). Discarding a returned result here wrote 'cancelled'
    # over completed work on a terminal row nothing re-runs, and reported
    # a different outcome than the caller was handed. River resolves the
    # same race the same way (a job that returns after its soft-stop
    # completes; vendor/river/internal/jobexecutor/job_executor.go).

    if progress_buffers is not None and _auto_pool is not None and _auto_settings is not None:
        await shield_with_retrieval(
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
        # result is NOT the job's outcome — no success hook (a hook with
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
