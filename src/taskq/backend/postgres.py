"""PostgresBackend, production backend backed by Postgres.

Schema identifier is baked into pre-rendered SQL strings at backend
construction time.  All user-supplied values use asyncpg ``$N``
positional parameter binding, no f-string interpolation of user data.

Decode helpers (:mod:`taskq.backend._records`), maintenance sweeps
(:mod:`taskq.backend._sweeps`), cron schedule CRUD
(:mod:`taskq.backend._schedules`), terminal writes
(:mod:`taskq.backend._terminal`), enqueue
(:mod:`taskq.backend._enqueue`), reads (:mod:`taskq.backend._reads`),
bulk cancel (:mod:`taskq.backend._cancel_bulk`), filter SQL builder
(:mod:`taskq.backend._filter_sql`), and dispatch
(:mod:`taskq.backend._dispatch`) live in companion submodules; this
module holds the cohesive core: ``__init__``, heartbeat, cancel
signals, NOTIFY, and schedule CRUD wiring.
"""

import asyncio
from collections.abc import Awaitable, Callable, Collection, Iterable
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from datetime import datetime, timedelta
from typing import ClassVar, Literal
from uuid import UUID

import asyncpg
import structlog

from taskq._advisory import DEADLINE_ERRORS
from taskq._json import dumps_str
from taskq.backend._batch_sql import (
    BatchSql,
    render_batch_sql,
)
from taskq.backend._batch_sql import (
    abort_batch as _abort_batch,
)
from taskq.backend._batch_sql import (
    complete_batch as _complete_batch,
)
from taskq.backend._batch_sql import (
    count_batch_non_terminal as _count_batch_non_terminal,
)
from taskq.backend._batch_sql import (
    create_batch as _create_batch,
)
from taskq.backend._batch_sql import (
    enqueue_batch_atomic as _enqueue_batch_atomic,
)
from taskq.backend._batch_sql import (
    get_batch as _get_batch,
)
from taskq.backend._batch_sql import (
    increment_batch_failures as _increment_batch_failures,
)
from taskq.backend._batch_sql import (
    list_batches as _list_batches,
)
from taskq.backend._batch_sql import (
    prune_old_batches as _prune_old_batches,
)
from taskq.backend._batch_sql import (
    reset_batch_failures as _reset_batch_failures,
)
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._dispatch import (
    QueueModeCache,
    _resolve_queue_modes,
)
from taskq.backend._dispatch import (
    _dispatch_batch as _dispatch,
)
from taskq.backend._enqueue import (
    DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS,
    DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
    _enqueue,
    _enqueue_batch,
    _enqueue_batch_fast,
    _enqueue_with_conn,
)
from taskq.backend._notify import _SubscriberContext
from taskq.backend._protocol import (
    BACKEND_PROTOCOL_VERSION,
    AttemptRow,
    BackendDeps,
    BatchCounts,
    BatchFilter,
    BatchRow,
    BulkCancelResult,
    CancelFlag,
    ConnLike,
    DenialReason,
    EnqueueArgs,
    ErrorInfo,
    EventRow,
    JobFilter,
    JobId,
    JobRow,
    LongRunningJobEventsWriter,
    ScheduleCreateArgs,
    ScheduleRecord,
    ScheduleUpdateArgs,
    SnoozeOutcome,
    parse_cancel_phase,
)
from taskq.backend._reads import (
    _check_reclaim_visibility_risk,
    _count_active_jobs,
    _count_pending_jobs,
    _get,
    _get_actor_max_pending,
    _get_attempts,
    _get_events,
    _list_jobs,
    _poll_reclaim_events,
)
from taskq.backend._records import parse_rowcount
from taskq.backend._schedules import (
    ScheduleSql,
    schedule_record_from_record,
)
from taskq.backend._schedules import (
    create_schedule as _create_schedule,
)
from taskq.backend._schedules import (
    delete_schedule as _delete_schedule,
)
from taskq.backend._schedules import (
    list_schedules as _list_schedules,
)
from taskq.backend._schedules import (
    update_schedule as _update_schedule,
)
from taskq.backend._sql import (
    UPDATE_JOBS_LOCK_SQL_TEMPLATE,
    UPDATE_RESERVATION_LEASES_SQL_TEMPLATE,
)
from taskq.backend._sql_templates import SqlTemplates, render
from taskq.backend._sweeps import (
    _SWEEP_1_SQL,
    _SWEEP_2_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _SWEEP_EVENT_TTL_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
    _SWEEP_RESULT_TTL_SQL,
    SweepBatchSizer,
    sweep_deadline_exceeded,
    sweep_expired_events,
    sweep_expired_locks,
    sweep_expired_results,
    sweep_idle_keyed_rows,
    sweep_leaked_reservation_slots,
    sweep_scheduled_to_pending,
)
from taskq.backend._terminal import (
    _insert_cancel_request_event,
    _insert_state_change_event,
    _mark_abandoned,
    _mark_cancelled,
    _mark_failed_or_retry,
    _mark_interrupted,
    _mark_retry_after,
    _mark_snoozed,
    _mark_succeeded,
    _mark_succeeded_on_conn,
    _write_attempt,
    _write_cancel_escalation,
)
from taskq.backend.clock import Clock
from taskq.connections import (
    _bounded_checkout,  # pyright: ignore[reportPrivateUsage] # Why: the one implementation of the bounded pool checkout (release carries _POOL_RELEASE_RESET_TIMEOUT_SECS and never raises); a local copy would drift from the discipline it documents.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_CHUNK_SIZE,
    DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
    DEFAULT_MAX_RETRY_BACKOFF,
    DEFAULT_RECLAIM_POLL_LIMIT,
    RECLAIM_EVENT_VISIBILITY_DELAY,
    events_channel,
    wake_channel,
    worker_channel,
)
from taskq.obs import (
    get_logger,
    get_meter,
    log_cancel_phase_change,
    log_state_change,
    record_sweep_batch_size,
    record_sweep_batch_size_configured,
)

__all__ = [
    "BACKEND_PROTOCOL_VERSION",
    "_SWEEP_1_SQL",
    "_SWEEP_2_SQL",
    "_SWEEP_3_SQL",
    "_SWEEP_4_SQL",
    "_SWEEP_EVENT_TTL_SQL",
    "_SWEEP_IDLE_KEYED_BUCKETS_SQL",
    "_SWEEP_IDLE_KEYED_SLOTS_SQL",
    "_SWEEP_RESULT_TTL_SQL",
    "PostgresBackend",
    "SweepBatchSizer",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()
_cancel_notify_sent_counter = _meter.create_counter(
    name="taskq.cancel.notify_sent",
    description="Total pg_notify calls fired for running-job cancel requests.",
)

_EXPECTED_PROTOCOL_VERSION = 3
if BACKEND_PROTOCOL_VERSION != _EXPECTED_PROTOCOL_VERSION:
    raise RuntimeError(
        f"PostgresBackend was built for protocol v{_EXPECTED_PROTOCOL_VERSION}; "
        f"current BACKEND_PROTOCOL_VERSION is {BACKEND_PROTOCOL_VERSION}. "
        "Update the implementation."
    )

# The event-writer batch knobs (event_writer_batch_size,
# event_writer_statement_timeout_ms, event_writer_reduced_batch_divisor,
# sweep_breaker_failure_threshold, sweep_breaker_window_secs) are declared
# fields on the ``BackendSettings`` protocol, so every settings object that
# reaches a PostgresBackend carries them, read directly at the use sites
# below, no per-read fallbacks.


def _cancel_notify_payload(job_id: UUID, worker_id: UUID) -> str:
    return dumps_str({"type": "cancel", "job_id": str(job_id), "worker_id": str(worker_id)})


def _cancel_notify_channels(schema: str, worker_id: UUID) -> list[str]:
    return [events_channel(schema), worker_channel(schema, str(worker_id))]


class PostgresBackend:
    """Production backend backed by Postgres.

     Constructor accepts ``deps`` typed as :class:`object` rather than
     :class:`WorkerDeps` to avoid creating a circular dependency between the
     ``taskq.backend`` and ``taskq.worker`` packages.  At runtime the caller
     passes a ``WorkerDeps`` instance; method bodies access its fields by
     name (e.g. ``self._deps.worker_pool``).  Rationale for the
     single-struct pattern over individual pools: ``WorkerDeps`` is already
     the stable named handle passed through the worker main loop (see
     ``taskq.worker.deps``); unpacking its fields at this layer would
     duplicate the wiring and make it fragile to pool additions.

     ``clock`` is used for Python-side timestamp computations in the
     enqueue path (e.g. comparing ``scheduled_at`` against "now" to decide
     whether to send it as a SQL parameter, and computing
     ``retry_after`` on a singleton collision), never as a substitute
     for a database timestamp. It is unused in the terminal-write and
     sweep methods, all of which use server-side ``clock_timestamp()``
     for every timestamp value, both WHERE comparisons and SET clauses
    , including ``scheduled_to_pending``, whose ``now`` parameter is
     accepted only for API-surface consistency and is otherwise ignored.

     ``cancellation_grace_period`` and ``cleanup_grace_period`` are the
     ``timedelta`` values used by :meth:`reclaim_expired_locks`.

     ``reclaim_event_visibility_delay`` is the default trailing-watermark
     margin :meth:`poll_reclaim_events` applies when a caller does not pass
     an explicit ``visibility_delay``, see
     ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`` and
     ``WorkerSettings.reclaim_event_visibility_delay`` for the correctness
     assumption this encodes.
    """

    BACKEND_PROTOCOL_VERSION: ClassVar[int] = BACKEND_PROTOCOL_VERSION

    def __init__(
        self,
        deps: BackendDeps,
        clock: Clock,
        cancellation_grace_period: timedelta,
        cleanup_grace_period: timedelta,
        reclaim_event_visibility_delay: timedelta = RECLAIM_EVENT_VISIBILITY_DELAY,
    ) -> None:
        self._deps = deps
        self._clock = clock
        self._cancellation_grace_period = cancellation_grace_period
        self._cleanup_grace_period = cleanup_grace_period
        self._reclaim_event_visibility_delay = reclaim_event_visibility_delay

        _schema: str = deps.settings.schema_name
        if not _IDENT_RE.match(_schema):
            raise ValueError(f"invalid schema identifier: {_schema!r}")
        self._schema_name: str = _schema

        # Pools are accessed dynamically via self._deps so that
        # reload_credentials() hot-swaps are visible to the backend without
        # needing to re-construct it. The properties below delegate to
        # self._deps at every access.
        self._wake_subscribers: set[asyncio.Event] = set()
        # Queue-scoped wake filtering: each subscriber records the queue set
        # it claims (None = wake on everything), and the notify callback
        # skips wakes for queues the subscriber would never claim. The
        # trigger carries the inserted row's queue as its payload.
        self._wake_queues: dict[asyncio.Event, frozenset[str] | None] = {}
        self._wake_lock: asyncio.Lock = asyncio.Lock()

        self._cancel_subscribers: set[asyncio.Event] = set()
        self._cancel_lock: asyncio.Lock = asyncio.Lock()

        self._sql: SqlTemplates = render(self._schema_name)
        self._schedule_sql = ScheduleSql.build(self._schema_name)
        self._batch_sql: BatchSql = render_batch_sql(self._schema_name)

        # Per-sweep batch sizers, built lazily on first use (see
        # _sweep_sizer): the breaker's latch state must survive across
        # calls, so the objects are cached for the backend's lifetime.
        self._sweep_sizers: dict[str, SweepBatchSizer] = {}

        # Worker-side queue-mode cache, owned by this backend instance:
        # the worker's single dispatch loop hits it every round, the
        # queue-ops seam clears it (see QueueModeCache's docstring for
        # the per-instance and concurrency contract). The TTL clock is
        # the backend's own clock, so tests drive expiry through
        # FakeClock exactly like every other clocked seam.
        self._queue_mode_cache = QueueModeCache(clock=self._clock.monotonic)

    # ── Pool accessors (dynamic via self._deps for hot-reload) ────────

    @property
    def _worker_pool(self) -> "asyncpg.Pool":
        return self._deps.worker_pool

    @property
    def _heartbeat_pool(self) -> "asyncpg.Pool":
        return self._deps.heartbeat_pool

    @property
    def _dispatcher_pool(self) -> "asyncpg.Pool | None":
        return getattr(self._deps, "dispatcher_pool", None)

    @property
    def _notify_pool(self) -> "asyncpg.Pool":
        _dp = self._dispatcher_pool
        return _dp if _dp is not None else self._worker_pool

    # ── Enqueue ────────────────────────────────────────────────────────

    supports_transactional_simulation: ClassVar[bool] = False

    def _enqueue_lock_budgets(self) -> tuple[float, float, float]:
        """The three single-enqueue advisory-lock wait budgets, read off the
        deps' settings object at the enqueue use sites (the
        ``dispatch_oversample`` plumbing pattern: WorkerSettings field ->
        BackendSettings protocol -> backend reads ``self._deps.settings``
        where the lock wait runs).

        Why a defensive ``getattr`` with the module-constant fallback rather
        than the direct read every other BackendSettings knob takes: the
        protocol's settings object is satisfied by structural duck-typing,
        so a settings implementation written OUTSIDE this repo's settings
        classes (an embedder's own BackendSettings stand-in) may predate
        these fields, a direct read would AttributeError that object's
        every enqueue. The fallback is the same 5 s constant the module
        functions defaulted to before the knob existed, so an undeclared
        settings object behaves exactly as it did yesterday, and the moment
        it declares the field the operator's value flows through.
        """
        settings = self._deps.settings
        return (
            getattr(settings, "max_pending_lock_timeout_ms", DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS),
            getattr(settings, "unique_for_lock_timeout_ms", DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS),
            getattr(settings, "idempotency_lock_timeout_ms", DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS),
        )

    async def enqueue_with_conn(
        self,
        conn: ConnLike,
        args: EnqueueArgs,
    ) -> JobRow:
        max_pending_ms, unique_for_ms, idempotency_ms = self._enqueue_lock_budgets()
        return await _enqueue_with_conn(
            conn,
            self._sql,
            self._schema_name,
            self._clock,
            args,
            max_pending_lock_timeout_ms=max_pending_ms,
            unique_for_lock_timeout_ms=unique_for_ms,
            idempotency_lock_timeout_ms=idempotency_ms,
        )

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        max_pending_ms, unique_for_ms, idempotency_ms = self._enqueue_lock_budgets()
        return await _enqueue(
            self._worker_pool,
            self._sql,
            self._schema_name,
            self._clock,
            args,
            max_pending_lock_timeout_ms=max_pending_ms,
            unique_for_lock_timeout_ms=unique_for_ms,
            idempotency_lock_timeout_ms=idempotency_ms,
        )

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: ConnLike | None = None,
        enforce_max_pending: bool = True,
    ) -> list[JobRow]:
        return await _enqueue_batch(
            self._worker_pool,
            self._sql,
            self._schema_name,
            args_list,
            connection=connection,
            enforce_max_pending=enforce_max_pending,
        )

    async def enqueue_batch_fast(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: ConnLike | None = None,
        enforce_max_pending: bool = True,
    ) -> int:
        return await _enqueue_batch_fast(
            self._worker_pool,
            self._sql,
            self._schema_name,
            args_list,
            connection=connection,
            enforce_max_pending=enforce_max_pending,
        )

    # ── Dispatch ────────────────────────────────────────────────────────

    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]:
        assert self._dispatcher_pool is not None, (
            "dispatcher_pool must be set before dispatch_batch"
        )
        return await _dispatch(
            self._dispatcher_pool,
            self._sql,
            self._deps.settings.dispatch_oversample,
            self._deps.settings.dispatcher_command_timeout,
            self._schema_name,
            worker_id,
            queues,
            limit,
            lock_lease,
            queue_mode_cache=self._queue_mode_cache,
        )

    @staticmethod
    async def resolve_queue_modes(
        conn: ConnLike,
        queues: list[str],
        schema: str,
    ) -> set[str]:
        return await _resolve_queue_modes(conn, queues, schema)

    # ── Heartbeat ───────────────────────────────────────────────────────

    async def heartbeat_jobs(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
        *,
        disowned: Collection[UUID] = (),
    ) -> int:
        sql = UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=self._schema_name)
        async with _bounded_checkout(self._heartbeat_pool, "heartbeat_jobs") as conn:
            tag = await conn.execute(sql, worker_id, lock_lease, list(disowned))
        return parse_rowcount(tag)

    async def extend_reservation_leases(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
        *,
        disowned: Collection[UUID] = (),
    ) -> int:
        sql = UPDATE_RESERVATION_LEASES_SQL_TEMPLATE.format(schema=self._schema_name)
        async with _bounded_checkout(self._heartbeat_pool, "extend_reservation_leases") as conn:
            tag = await conn.execute(sql, worker_id, lock_lease, list(disowned))
        return parse_rowcount(tag)

    # ── Terminal writes ─────────────────────────────────────────────────

    async def mark_succeeded_with_conn(
        self,
        conn: ConnLike,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        return await _mark_succeeded_on_conn(
            conn,
            self._sql,
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            self._deps.settings.result_max_bytes,
            result_bytes=result_bytes,
            attempt=attempt,
        )

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
    ) -> bool:
        return await _mark_succeeded(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            self._deps.settings.result_max_bytes,
            result_bytes=result_bytes,
            attempt=attempt,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> JobRow:
        return await _mark_failed_or_retry(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            error_info,
            retry_delay,
            progress_seq,
            progress_state,
            attempt=attempt,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
    ) -> bool:
        # Why _worker_pool (supersession of the original heartbeat routing,
        # which had no documented rationale, original v0.1.0 wiring): the
        # heartbeat pool is sized heartbeat_pool_size (default 4) for the
        # liveness loop's one-connection-per-tick cadence, and routing a
        # per-job terminal write onto it let a cancel storm of concurrent
        # consumer mark_cancelled calls exhaust the very pool the
        # heartbeat loop's own bounded acquire waits on, starving the
        # loop into isolate_self while the worker was merely cancelling
        # jobs (the spiral pinned by
        # tests/test_rt_locks_terminal_write_pool_starvation.py). The
        # routing's plausible original motive, a shielded cancel-path
        # write outliving the worker pool's LIFO close (deps.py opens
        # heartbeat_pool BEFORE worker_pool, so teardown closes
        # worker_pool first), is superseded by the bounded acquire
        # threaded above: a closing or closed pool is exactly the
        # wedged-checkout case the bound converts from a hang into the
        # designed infra failure, and the cancel path already treats that
        # outcome as best-effort (worker/_consumer.py: the row stays
        # running and lock-lease expiry reclaims it), the same contract
        # every other shielded terminal write already accepts on the
        # worker pool.
        return await _mark_cancelled(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            progress_seq,
            progress_state,
            attempt=attempt,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool:
        return await _write_cancel_escalation(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            phase,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_abandoned(
            self._worker_pool,
            self._sql,
            job_id,
            progress_seq,
            progress_state,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_snoozed(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: SnoozeOutcome = "snoozed",
        attempt: int | None = None,
        denial_reason: DenialReason = "capacity",
    ) -> Literal["scheduled", "failed", "noop"]:
        return await _mark_snoozed(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            delay,
            metadata_update=metadata_update,
            progress_seq=progress_seq,
            progress_state=progress_state,
            outcome=outcome,
            attempt=attempt,
            denial_reason=denial_reason,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_retry_after(
        self,
        job_id: JobId,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        attempt: int | None = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
        return await _mark_retry_after(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            delay,
            consume_budget=consume_budget,
            progress_seq=progress_seq,
            progress_state=progress_state,
            attempt=attempt,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def mark_interrupted(
        self,
        job_id: JobId,
        worker_id: UUID,
        *,
        attempt: int,
        hold: timedelta,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> Literal["pending", "scheduled", "failed:DeadlineExceeded", "noop"]:
        return await _mark_interrupted(
            self._worker_pool,
            self._sql,
            job_id,
            worker_id,
            attempt=attempt,
            hold=hold,
            progress_seq=progress_seq,
            progress_state=progress_state,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    # ── Attempt history ─────────────────────────────────────────────────

    async def write_attempt(self, attempt: AttemptRow) -> None:
        await _write_attempt(
            self._worker_pool,
            self._sql,
            attempt,
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]:
        return await _get_attempts(self._worker_pool, self._sql, job_id)

    async def get_events(self, job_id: JobId) -> list[EventRow]:
        return await _get_events(self._worker_pool, self._sql, job_id)

    async def poll_reclaim_events(
        self,
        after_id: int,
        limit: int = DEFAULT_RECLAIM_POLL_LIMIT,
        *,
        visibility_delay: timedelta | None = None,
    ) -> list[EventRow]:
        delay = (
            visibility_delay
            if visibility_delay is not None
            else self._reclaim_event_visibility_delay
        )
        return await _poll_reclaim_events(
            self._worker_pool, self._sql, after_id, limit, visibility_delay=delay
        )

    async def check_reclaim_visibility_delay_risk(
        self,
        *,
        visibility_delay: timedelta | None = None,
    ) -> list[LongRunningJobEventsWriter]:
        """Diagnostic (not part of ``Backend``): report transactions that
        have held a lock on ``job_events`` for longer than the configured
        visibility-delay margin, a candidate cause of a silently missed
        ``poll_reclaim_events`` event (see
        ``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``).

        Postgres-only: it is deliberately not on the ``Backend`` protocol,
        so ``InMemoryBackend`` has no equivalent and a monitoring loop
        consuming this must branch on backend type (or ``getattr``-probe,
        as ``taskq.client._taskq._probe_visibility_risk`` does).

        Run automatically by every ``TaskQ.watch_reclaims`` consumer on a
        slow cadence (``taskq.client._taskq._VISIBILITY_RISK_CHECK_INTERVAL``,
        default 60s) so a violated margin assumption is loud by default;
        also callable directly from a dedicated monitoring/alerting loop
        that wants a tighter cadence or its own sink.  Either way this is
        not for the per-poll hot path, it issues its own query against
        ``pg_locks`` / ``pg_stat_activity`` and is not free.  A non-empty
        result is a proxy warning, not proof of an actual missed event.
        """
        delay = (
            visibility_delay
            if visibility_delay is not None
            else self._reclaim_event_visibility_delay
        )
        return await _check_reclaim_visibility_risk(
            self._worker_pool, self._sql, visibility_delay=delay
        )

    # ── Cancel signals ──────────────────────────────────────────────────

    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool:
        _prev_status: str | None = None
        _cancel_phase: int | None = None
        _locked_by_worker: UUID | None = None

        async with _bounded_checkout(self._worker_pool, "write_cancel_request") as conn:
            async with conn.transaction():
                rec = await conn.fetchrow(self._sql.cancel_pending_scheduled, job_id)
                if rec is not None:
                    prev_status: str = rec["prev_status"]
                    _prev_status = prev_status
                    await _insert_state_change_event(
                        conn, self._sql, job_id, prev_status, "cancelled"
                    )
                    await _insert_cancel_request_event(conn, self._sql, job_id, reason)
                else:
                    cancel_rec = await conn.fetchrow(self._sql.cancel_running, job_id)
                    if cancel_rec is not None:
                        _cancel_phase = 1
                        _locked_by_worker = cancel_rec["locked_by_worker"]
                        await _insert_cancel_request_event(conn, self._sql, job_id, reason)
                    else:
                        return False

        if _prev_status is not None:
            log_state_change(
                logger,
                from_state=_prev_status,
                to_state="cancelled",
                job_id=str(job_id),
            )
        elif _cancel_phase is not None:
            log_cancel_phase_change(
                logger,
                from_phase=0,
                to_phase=_cancel_phase,
                job_id=str(job_id),
            )
            if _locked_by_worker is not None:
                payload = _cancel_notify_payload(job_id, _locked_by_worker)
                fleet_ch, worker_ch = _cancel_notify_channels(self._schema_name, _locked_by_worker)
                try:
                    async with _bounded_checkout(
                        self._worker_pool, "write_cancel_request_notify"
                    ) as notify_conn:
                        await notify_conn.execute(
                            "SELECT pg_notify($1, $2), pg_notify($3, $4)",
                            fleet_ch,
                            payload,
                            worker_ch,
                            payload,
                        )
                    _cancel_notify_sent_counter.add(1, {"schema": self._schema_name})
                except Exception:
                    # The cancel flag is already committed, the transaction
                    # block above has exited by the time the NOTIFY fires ,
                    # so a NOTIFY failure must not surface to the caller as
                    # a failed cancel: the request IS recorded. Same
                    # swallow-and-warn as cancel_where below; the heartbeat
                    # poll remains authoritative for signal delivery.
                    logger.warning(
                        "cancel-request-notify-failed",
                        job_id=str(job_id),
                        exc_info=True,
                    )
        return True

    async def poll_cancel_flags(
        self,
        worker_id: UUID,
    ) -> list[CancelFlag]:
        async with _bounded_checkout(self._worker_pool, "poll_cancel_flags") as conn:
            recs = await conn.fetch(
                self._sql.poll_cancel_flags,
                worker_id,
            )
        return [
            CancelFlag(
                job_id=JobId(rec["id"]), cancel_phase=parse_cancel_phase(rec["cancel_phase"])
            )
            for rec in recs
        ]

    async def cancel_where(
        self,
        filter: JobFilter,
        reason: str | None,
    ) -> BulkCancelResult:
        # batch_size and the timeout are declared BackendSettings fields
        # (see _protocol.BackendSettings): bounded, typed reads, the
        # int() is the numeric conversion for SET LOCAL statement_timeout's
        # integer parameter, not a defensive coercion. No breaker wraps
        # this call (unlike _run_bounded_sweep): cancel_where is an
        # interactive operator call, not loop policy, so a timeout abort
        # propagates to the caller instead of degrading a standing loop.
        result, notify_targets = await _cancel_where(
            self._worker_pool,
            self._schema_name,
            self._sql,
            filter,
            reason,
            batch_size=self._deps.settings.event_writer_batch_size,
            statement_timeout_ms=int(self._deps.settings.event_writer_statement_timeout_ms),
        )
        if notify_targets:
            channels: list[str] = []
            payloads: list[str] = []
            for target in notify_targets:
                payload = _cancel_notify_payload(target.job_id, target.worker_id)
                ch = _cancel_notify_channels(self._schema_name, target.worker_id)
                channels.extend(ch)
                payloads.extend([payload, payload])
            try:
                async with _bounded_checkout(
                    self._worker_pool, "cancel_where_notify"
                ) as notify_conn:
                    await notify_conn.execute(
                        "SELECT pg_notify(channel, payload) "
                        "FROM unnest($1::text[], $2::text[]) AS t(channel, payload)",
                        channels,
                        payloads,
                    )
                _cancel_notify_sent_counter.add(len(notify_targets), {"schema": self._schema_name})
            except Exception:
                logger.warning(
                    "cancel-where-notify-failed",
                    notify_count=len(notify_targets),
                    exc_info=True,
                )
        return result

    # ── Admin operations ──────────────────────────────────────────────

    async def retry_job(self, job_id: JobId) -> bool:
        async with _bounded_checkout(self._worker_pool, "retry_job") as conn:
            async with conn.transaction():
                rec = await conn.fetchrow(self._sql.retry_job, job_id)
                if rec is None:
                    return False
                await conn.execute(
                    self._sql.wake_notify,
                    wake_channel(self._schema_name),
                )
        # The statement's reopened CTE (see retry_job in
        # _sql_templates.py) reconciles a terminal batch row back to
        # 'active' when the retried job is one of its members -- an
        # operator who watched a batch complete and then saw it go active
        # again needs the admin retry that caused it named in the log,
        # not a mystery status flip.
        if rec["reopened_batch"]:
            logger.info(
                "retry_job_reopened_batch",
                kind="batch",
                job_id=str(job_id),
                batch_id=str(UUID(str(rec["batch_id"]))),
            )
        return True

    # ── Scheduling / sweeps ─────────────────────────────────────────────
    # No `now` parameter: every predicate is evaluated server-side
    # (clock_timestamp()), the server clock is the arbiter.  One call
    # transitions at most one bounded batch of rows; repeated calls
    # drain.  The bound and its degradation tier come from settings via
    # the per-sweep SweepBatchSizer; an explicit batch_size overrides the
    # tier for that call only.

    def _sweep_sizer(self, sweep_name: str) -> SweepBatchSizer:
        """The per-sweep batch sizer, built lazily from current settings.

        Lazy (not in ``__init__``) for two reasons: a backend only needs a
        sizer for the sweeps it actually runs, and reading the knobs at
        first use (not construction) picks up settings mutated after the
        backend was built, the seam tests use to shrink sweep intervals
        on an already-constructed deps. The knobs themselves are declared
        fields on ``BackendSettings``, read directly below.  Built once per
        sweep name, then cached for the backend's lifetime, the breaker's
        latch state is exactly the state that must survive across calls.
        """
        sizer = self._sweep_sizers.get(sweep_name)
        if sizer is None:
            settings = self._deps.settings
            sizer = SweepBatchSizer(
                default_size=settings.event_writer_batch_size,
                divisor=settings.event_writer_reduced_batch_divisor,
                failure_threshold=settings.sweep_breaker_failure_threshold,
                window_secs=settings.sweep_breaker_window_secs,
            )
            self._sweep_sizers[sweep_name] = sizer
        return sizer

    async def _run_bounded_sweep(
        self,
        sweep_name: str,
        batch_size: int | None,
        run: Callable[[int, int], Awaitable[int]],
    ) -> int:
        """Run one sweep call at a bounded batch size, breaker-wrapped.

        The size the sweep actually uses (the sizer's effective tier, or
        an explicit ``batch_size`` override) is recorded before the call
        so the gauge reports reality rather than configuration.  An
        aborted batch, server-side ``statement_timeout`` arriving as
        ``asyncpg.QueryCanceledError`` (SQLSTATE 57014), or a client
        ``command_timeout`` arriving as ``TimeoutError``; the two shapes
        an aborted batch produces, counts against the breaker and
        re-raises for the caller's transient-error handling.
        """
        sizer = self._sweep_sizer(sweep_name)
        size = batch_size if batch_size is not None else sizer.effective_size()
        record_sweep_batch_size(sweep_name, size)
        # Emitted at the same call site so the used and configured gauges
        # stay label-matched for the gauge-to-gauge sweep-degraded alert.
        record_sweep_batch_size_configured(sweep_name, self._deps.settings.event_writer_batch_size)
        # Declared BackendSettings field (float ms); int() is the numeric
        # conversion for SET LOCAL statement_timeout's integer parameter,
        # not a defensive coercion.
        timeout_ms = int(self._deps.settings.event_writer_statement_timeout_ms)
        try:
            count = await run(size, timeout_ms)
        except DEADLINE_ERRORS:
            sizer.on_timeout()
            raise
        sizer.on_success()
        return count

    async def scheduled_to_pending(self, *, batch_size: int | None = None) -> int:
        # Bounded acquire (the sweep twin's contract,
        # tests/test_rt_locks_sweep_notify_pool_unbounded.py): _notify_pool
        # delegates to the dispatcher pool, which a prune drain holds for
        # its whole multi-batch drain, an unbounded checkout queues the
        # sweep indefinitely behind it. The dispatcher command timeout is
        # the prune loop's own acquire convention, and the resulting
        # TimeoutError is transient-classified by the leader loops that
        # call these entrypoints (worker/_transient.py).
        async with _bounded_checkout(
            self._notify_pool,
            "scheduled_to_pending",
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        ) as conn:
            return await self._run_bounded_sweep(
                "scheduled_to_pending",
                batch_size,
                lambda size, timeout_ms: sweep_scheduled_to_pending(
                    conn,
                    schema=self._schema_name,
                    batch_size=size,
                    statement_timeout_ms=timeout_ms,
                ),
            )

    async def deadline_sweep(self, *, batch_size: int | None = None) -> int:
        # Bounded acquire, same contract and rationale as
        # scheduled_to_pending above.
        async with _bounded_checkout(
            self._notify_pool,
            "deadline_sweep",
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        ) as conn:
            return await self._run_bounded_sweep(
                "deadline_exceeded",
                batch_size,
                lambda size, timeout_ms: sweep_deadline_exceeded(
                    conn,
                    schema=self._schema_name,
                    batch_size=size,
                    statement_timeout_ms=timeout_ms,
                ),
            )

    async def reclaim_expired_locks(
        self,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
        *,
        batch_size: int | None = None,
    ) -> int:
        # Bounded acquire, same contract and rationale as
        # scheduled_to_pending above.
        async with _bounded_checkout(
            self._notify_pool,
            "reclaim_expired_locks",
            acquire_timeout=self._deps.settings.dispatcher_command_timeout,
        ) as conn:
            return await self._run_bounded_sweep(
                "expired_locks",
                batch_size,
                lambda size, timeout_ms: sweep_expired_locks(
                    conn,
                    cancel_grace,
                    cleanup_grace,
                    schema=self._schema_name,
                    batch_size=size,
                    statement_timeout_ms=timeout_ms,
                    # The operator's global backoff ceiling reaches the
                    # reclaim path here, the same value the consumer's
                    # failure path hands compute_backoff.
                    max_retry_backoff=self._deps.settings.max_retry_backoff,
                ),
            )

    @staticmethod
    async def sweep_expired_locks(
        conn: ConnLike,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
        *,
        schema: str,
        batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
        statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
        max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
    ) -> int:
        return await sweep_expired_locks(
            conn,
            cancel_grace,
            cleanup_grace,
            schema=schema,
            batch_size=batch_size,
            statement_timeout_ms=statement_timeout_ms,
            max_retry_backoff=max_retry_backoff,
        )

    @staticmethod
    async def sweep_deadline_exceeded(
        conn: ConnLike,
        *,
        schema: str,
        batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
        statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    ) -> int:
        return await sweep_deadline_exceeded(
            conn,
            schema=schema,
            batch_size=batch_size,
            statement_timeout_ms=statement_timeout_ms,
        )

    @staticmethod
    async def sweep_scheduled_to_pending(
        conn: ConnLike,
        *,
        schema: str,
        batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
        statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    ) -> int:
        return await sweep_scheduled_to_pending(
            conn,
            schema=schema,
            batch_size=batch_size,
            statement_timeout_ms=statement_timeout_ms,
        )

    @staticmethod
    async def sweep_leaked_reservation_slots(
        conn: ConnLike,
        *,
        schema: str,
        batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    ) -> int:
        return await sweep_leaked_reservation_slots(conn, schema=schema, batch_size=batch_size)

    @staticmethod
    async def sweep_expired_results(
        conn: ConnLike,
        *,
        schema: str,
        batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    ) -> int:
        return await sweep_expired_results(conn, schema=schema, batch_size=batch_size)

    @staticmethod
    async def sweep_expired_events(
        conn: ConnLike,
        *,
        schema: str,
        retention: timedelta,
        batch_size: int = DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    ) -> int:
        return await sweep_expired_events(
            conn,
            schema=schema,
            retention=retention,
            batch_size=batch_size,
        )

    @staticmethod
    async def sweep_idle_keyed_rows(
        conn: ConnLike,
        *,
        schema: str,
        horizon: timedelta,
        batch_size: int = DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
    ) -> int:
        return await sweep_idle_keyed_rows(
            conn,
            schema=schema,
            horizon=horizon,
            batch_size=batch_size,
        )

    # ── Read ────────────────────────────────────────────────────────────

    async def get(self, job_id: JobId) -> JobRow | None:
        return await _get(self._worker_pool, self._sql, job_id)

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        return await _list_jobs(self._worker_pool, self._schema_name, filters)

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        return await _count_pending_jobs(self._worker_pool, self._sql, actors)

    async def count_active_jobs(self, queues: list[str]) -> int:
        return await _count_active_jobs(self._worker_pool, self._sql, queues)

    async def get_actor_max_pending(self) -> dict[str, int | None]:
        return await _get_actor_max_pending(self._worker_pool, self._sql)

    # ── NOTIFY hook ─────────────────────────────────────────────────────

    def subscribe_wake(
        self, queues: Iterable[str] | None = None
    ) -> AsyncContextManager[asyncio.Event]:
        """Subscribe to insert wakes, optionally scoped to the caller's queues.

        ``queues=None`` (or an empty set) wakes on every insert, the
        historical contract. A queue set wakes only when the inserted
        row's queue (the NOTIFY payload from the trigger) is served by
        this subscriber, or when the payload is empty (an older trigger
        still in a rolling deploy, or the COPY fixup's bulk wake).
        """
        event = asyncio.Event()
        queue_set = frozenset(queues) if queues else None
        return _SubscriberContext(
            event,
            self._wake_subscribers,
            self._wake_lock,
            queue_registry=self._wake_queues,
            queues=queue_set,
        )

    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]:
        event = asyncio.Event()
        return _SubscriberContext(event, self._cancel_subscribers, self._cancel_lock)

    # ── Schedule CRUD ────────────────────────────────────────────────────

    @staticmethod
    def _schedule_record_from_record(rec: "asyncpg.Record") -> ScheduleRecord:
        return schedule_record_from_record(rec)

    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord:
        return await _create_schedule(self._worker_pool, self._schedule_sql, args)

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]:
        return await _list_schedules(
            self._worker_pool, self._schedule_sql, actor=actor, enabled=enabled
        )

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord:
        return await _update_schedule(self._worker_pool, self._schedule_sql, schedule_id, args)

    async def delete_schedule(self, schedule_id: UUID) -> None:
        await _delete_schedule(self._worker_pool, self._schedule_sql, schedule_id)

    # ── Batch operations ──────────────────────────────────────────────

    async def enqueue_batch_atomic(
        self,
        items: Iterable[EnqueueArgs],
        *,
        batch_id: UUID,
        queue: str,
        batch_row: BatchRow | None,
        finalizer_args: EnqueueArgs | None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> list[JobRow]:
        return await _enqueue_batch_atomic(
            self._worker_pool,
            self._schema_name,
            self._sql,
            self._batch_sql,
            items,
            batch_id=batch_id,
            queue=queue,
            batch_row=batch_row,
            finalizer_args=finalizer_args,
            chunk_size=chunk_size,
        )

    async def create_batch(
        self,
        batch_id: UUID,
        queue: str,
        expected_size: int,
        failure_threshold: int | None,
        finalizer_job_id: UUID | None,
        originating_actor: str | None,
        *,
        connection: ConnLike | None = None,
    ) -> None:
        if connection is not None:
            await _create_batch(
                connection,
                self._batch_sql,
                batch_id,
                queue,
                expected_size,
                failure_threshold,
                finalizer_job_id,
                originating_actor,
            )
        else:
            async with _bounded_checkout(self._worker_pool, "create_batch") as conn:
                await _create_batch(
                    conn,
                    self._batch_sql,
                    batch_id,
                    queue,
                    expected_size,
                    failure_threshold,
                    finalizer_job_id,
                    originating_actor,
                )

    async def increment_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: ConnLike | None = None,
    ) -> tuple[int, int | None, int]:
        if connection is not None:
            return await _increment_batch_failures(connection, self._batch_sql, batch_id)
        async with _bounded_checkout(self._worker_pool, "increment_batch_failures") as conn:
            return await _increment_batch_failures(conn, self._batch_sql, batch_id)

    async def reset_batch_failures(
        self,
        batch_id: UUID,
        *,
        connection: ConnLike | None = None,
    ) -> int:
        if connection is not None:
            return await _reset_batch_failures(connection, self._batch_sql, batch_id)
        async with _bounded_checkout(self._worker_pool, "reset_batch_failures") as conn:
            return await _reset_batch_failures(conn, self._batch_sql, batch_id)

    async def abort_batch(
        self,
        batch_id: UUID,
        *,
        connection: ConnLike | None = None,
    ) -> int:
        if connection is not None:
            return await _abort_batch(connection, self._batch_sql, batch_id)
        # No outer transaction here: _abort_batch drains the member set
        # as bounded, self-committed pages (each with its own
        # statement_timeout and deadlock retry), and wrapping the drain
        # in one transaction would re-create the whole-member write set
        # the drain exists to bound.
        async with _bounded_checkout(self._worker_pool, "abort_batch") as conn:
            return await _abort_batch(conn, self._batch_sql, batch_id)

    async def complete_batch(
        self,
        batch_id: UUID,
        *,
        connection: ConnLike | None = None,
    ) -> None:
        if connection is not None:
            await _complete_batch(connection, self._batch_sql, batch_id)
        else:
            async with _bounded_checkout(self._worker_pool, "complete_batch") as conn:
                await _complete_batch(conn, self._batch_sql, batch_id)

    async def get_batch(self, batch_id: UUID) -> BatchRow | None:
        async with _bounded_checkout(self._worker_pool, "get_batch") as conn:
            return await _get_batch(conn, self._batch_sql, batch_id)

    async def list_batches(
        self,
        filter: BatchFilter,
    ) -> list[tuple[BatchRow, BatchCounts]]:
        async with _bounded_checkout(self._worker_pool, "list_batches") as conn:
            return await _list_batches(conn, self._batch_sql, filter)

    async def count_batch_non_terminal(
        self,
        batch_id: UUID,
        *,
        connection: ConnLike | None = None,
    ) -> int:
        if connection is not None:
            return await _count_batch_non_terminal(connection, self._batch_sql, batch_id)
        async with _bounded_checkout(self._worker_pool, "count_batch_non_terminal") as conn:
            return await _count_batch_non_terminal(conn, self._batch_sql, batch_id)

    async def prune_old_batches(self, cutoff: datetime) -> int:
        async with _bounded_checkout(self._worker_pool, "prune_old_batches") as conn:
            return await _prune_old_batches(conn, self._batch_sql, cutoff)
