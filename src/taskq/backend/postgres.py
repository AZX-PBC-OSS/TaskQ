"""PostgresBackend — production backend backed by Postgres.

Schema identifier is baked into pre-rendered SQL strings at backend
construction time.  All user-supplied values use asyncpg ``$N``
positional parameter binding — no f-string interpolation of user data.

Decode helpers (:mod:`taskq.backend._records`), maintenance sweeps
(:mod:`taskq.backend._sweeps`), cron schedule CRUD
(:mod:`taskq.backend._schedules`), terminal writes
(:mod:`taskq.backend._terminal`), enqueue
(:mod:`taskq.backend._enqueue`), reads (:mod:`taskq.backend._reads`),
and dispatch (:mod:`taskq.backend._dispatch`) live in companion
submodules; this module holds the cohesive core: ``__init__``, heartbeat,
cancel signals, NOTIFY, and schedule CRUD wiring.
"""

import asyncio
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, Literal
from uuid import UUID

import structlog

from taskq._json import dumps_str
from taskq.backend._dispatch import (
    _dispatch_batch as _dispatch,
)
from taskq.backend._dispatch import (
    _resolve_queue_modes,
)
from taskq.backend._enqueue import (
    _enqueue,
    _enqueue_batch,
    _enqueue_batch_fast,
    _enqueue_with_conn,
)
from taskq.backend._notify import _SubscriberContext
from taskq.backend._protocol import (
    BACKEND_PROTOCOL_VERSION,
    AttemptOutcome,
    AttemptRow,
    BackendDeps,
    CancelFlag,
    ConnLike,
    EnqueueArgs,
    ErrorInfo,
    EventRow,
    JobFilter,
    JobId,
    JobRow,
    ScheduleCreateArgs,
    ScheduleRecord,
    ScheduleUpdateArgs,
    parse_cancel_phase,
)
from taskq.backend._reads import (
    _count_pending_jobs,
    _get,
    _get_attempts,
    _get_events,
    _list_jobs,
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
    _SWEEP_RESULT_TTL_SQL,
    sweep_deadline_exceeded,
    sweep_expired_locks,
    sweep_expired_results,
    sweep_leaked_reservation_slots,
    sweep_scheduled_to_pending,
)
from taskq.backend._terminal import (
    _insert_cancel_request_event,
    _insert_state_change_event,
    _mark_abandoned,
    _mark_cancelled,
    _mark_failed_or_retry,
    _mark_retry_after,
    _mark_snoozed,
    _mark_succeeded,
    _mark_succeeded_on_conn,
    _write_attempt,
    _write_cancel_escalation,
)
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    events_channel,
    wake_channel,
    worker_channel,
)
from taskq.obs import (
    get_logger,
    get_meter,
    log_cancel_phase_change,
    log_state_change,
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "BACKEND_PROTOCOL_VERSION",
    "_SWEEP_1_SQL",
    "_SWEEP_2_SQL",
    "_SWEEP_3_SQL",
    "_SWEEP_4_SQL",
    "_SWEEP_RESULT_TTL_SQL",
    "PostgresBackend",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()
_cancel_notify_sent_counter = _meter.create_counter(
    name="taskq.cancel.notify_sent",
    description="Total pg_notify calls fired for running-job cancel requests.",
)

_EXPECTED_PROTOCOL_VERSION = 2
if BACKEND_PROTOCOL_VERSION != _EXPECTED_PROTOCOL_VERSION:
    raise RuntimeError(
        f"PostgresBackend was built for protocol v{_EXPECTED_PROTOCOL_VERSION}; "
        f"current BACKEND_PROTOCOL_VERSION is {BACKEND_PROTOCOL_VERSION}. "
        "Update the implementation."
    )


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

    ``clock`` is stored for future SQL paths that need wall-clock ``now()``
    (e.g. ``scheduled_to_pending``), but is unused in the terminal-write
    methods which use server-side ``clock_timestamp()`` for WHERE
    comparisons and ``now()`` for SET values.

    ``cancellation_grace_period`` and ``cleanup_grace_period`` are the
    ``timedelta`` values used by :meth:`reclaim_expired_locks`.
    """

    BACKEND_PROTOCOL_VERSION: ClassVar[int] = BACKEND_PROTOCOL_VERSION

    def __init__(
        self,
        deps: BackendDeps,
        clock: Clock,
        cancellation_grace_period: timedelta,
        cleanup_grace_period: timedelta,
    ) -> None:
        self._deps = deps
        self._clock = clock
        self._cancellation_grace_period = cancellation_grace_period
        self._cleanup_grace_period = cleanup_grace_period

        _schema: str = deps.settings.schema_name
        if not _IDENT_RE.match(_schema):
            raise ValueError(f"invalid schema identifier: {_schema!r}")
        self._schema_name: str = _schema

        # Pools are accessed dynamically via self._deps so that
        # reload_credentials() hot-swaps are visible to the backend without
        # needing to re-construct it. The properties below delegate to
        # self._deps at every access.
        self._wake_subscribers: set[asyncio.Event] = set()
        self._wake_lock: asyncio.Lock = asyncio.Lock()

        self._cancel_subscribers: set[asyncio.Event] = set()
        self._cancel_lock: asyncio.Lock = asyncio.Lock()

        self._sql: SqlTemplates = render(self._schema_name)
        self._schedule_sql = ScheduleSql.build(self._schema_name)

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

    async def enqueue_with_conn(
        self,
        conn: ConnLike,
        args: EnqueueArgs,
    ) -> JobRow:
        return await _enqueue_with_conn(conn, self._sql, self._schema_name, self._clock, args)

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        return await _enqueue(self._worker_pool, self._sql, self._schema_name, self._clock, args)

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> list[JobRow]:
        return await _enqueue_batch(
            self._worker_pool,
            self._sql,
            self._schema_name,
            self._clock,
            args_list,
            connection=connection,
        )

    async def enqueue_batch_fast(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: "asyncpg.Connection | None" = None,
    ) -> int:
        return await _enqueue_batch_fast(
            self._worker_pool,
            self._sql,
            self._schema_name,
            self._clock,
            args_list,
            connection=connection,
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
            self._schema_name,
            worker_id,
            queues,
            limit,
            lock_lease,
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
    ) -> int:
        sql = UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=self._schema_name)
        async with self._heartbeat_pool.acquire() as conn:
            tag = await conn.execute(sql, worker_id, lock_lease)
        return parse_rowcount(tag)

    async def extend_reservation_leases(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
    ) -> int:
        sql = UPDATE_RESERVATION_LEASES_SQL_TEMPLATE.format(schema=self._schema_name)
        async with self._heartbeat_pool.acquire() as conn:
            tag = await conn.execute(sql, worker_id, lock_lease)
        return parse_rowcount(tag)

    # ── Terminal writes ─────────────────────────────────────────────────

    async def mark_succeeded_with_conn(
        self,
        conn: ConnLike,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_succeeded_on_conn(
            conn, self._sql, job_id, worker_id, result, progress_seq, progress_state
        )

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_succeeded(
            self._worker_pool, self._sql, job_id, worker_id, result, progress_seq, progress_state
        )

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        next_scheduled_at: datetime | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow:
        return await _mark_failed_or_retry(
            self._worker_pool,
            self._sql,
            self._clock,
            job_id,
            worker_id,
            error_info,
            next_scheduled_at,
            progress_seq,
            progress_state,
        )

    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_cancelled(
            self._heartbeat_pool, self._sql, job_id, worker_id, progress_seq, progress_state
        )

    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool:
        return await _write_cancel_escalation(
            self._worker_pool, self._sql, job_id, worker_id, phase
        )

    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_abandoned(
            self._worker_pool, self._sql, job_id, progress_seq, progress_state
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
        outcome: AttemptOutcome = "snoozed",
    ) -> Literal["scheduled", "failed", "noop"]:
        return await _mark_snoozed(
            self._worker_pool,
            self._sql,
            self._clock,
            job_id,
            worker_id,
            delay,
            metadata_update=metadata_update,
            progress_seq=progress_seq,
            progress_state=progress_state,
            outcome=outcome,
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
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
        return await _mark_retry_after(
            self._worker_pool,
            self._sql,
            self._clock,
            job_id,
            worker_id,
            delay,
            consume_budget=consume_budget,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )

    # ── Attempt history ─────────────────────────────────────────────────

    async def write_attempt(self, attempt: AttemptRow) -> None:
        await _write_attempt(self._worker_pool, self._sql, attempt)

    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]:
        return await _get_attempts(self._worker_pool, self._sql, job_id)

    async def get_events(self, job_id: JobId) -> list[EventRow]:
        return await _get_events(self._worker_pool, self._sql, job_id)

    # ── Cancel signals ──────────────────────────────────────────────────

    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool:
        _prev_status: str | None = None
        _cancel_phase: int | None = None
        _locked_by_worker: UUID | None = None

        async with self._worker_pool.acquire() as conn:
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
                payload = dumps_str(
                    {
                        "type": "cancel",
                        "job_id": str(job_id),
                        "worker_id": str(_locked_by_worker),
                    }
                )
                fleet_ch = events_channel(self._schema_name)
                worker_ch = worker_channel(self._schema_name, str(_locked_by_worker))
                async with self._worker_pool.acquire() as notify_conn:
                    await notify_conn.execute(
                        "SELECT pg_notify($1, $2), pg_notify($3, $4)",
                        fleet_ch,
                        payload,
                        worker_ch,
                        payload,
                    )
                _cancel_notify_sent_counter.add(1, {"schema": self._schema_name})
        return True

    async def poll_cancel_flags(
        self,
        worker_id: UUID,
    ) -> list[CancelFlag]:
        async with self._worker_pool.acquire() as conn:
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

    # ── Admin operations ──────────────────────────────────────────────

    async def retry_job(self, job_id: JobId) -> bool:
        async with self._worker_pool.acquire() as conn:
            async with conn.transaction():
                rec = await conn.fetchrow(self._sql.retry_job, job_id)
                if rec is None:
                    return False
                await conn.execute(
                    self._sql.enqueue_notify,
                    wake_channel(self._schema_name),
                )
        return True

    # ── Scheduling / sweeps ─────────────────────────────────────────────

    async def scheduled_to_pending(self, now: datetime) -> int:
        async with self._notify_pool.acquire() as conn:
            return await sweep_scheduled_to_pending(conn, now, schema=self._schema_name)

    async def deadline_sweep(self, now: datetime) -> int:
        async with self._notify_pool.acquire() as conn:
            return await sweep_deadline_exceeded(conn, now, schema=self._schema_name)

    async def reclaim_expired_locks(
        self,
        now: datetime,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
    ) -> int:
        async with self._notify_pool.acquire() as conn:
            return await sweep_expired_locks(
                conn, now, cancel_grace, cleanup_grace, schema=self._schema_name
            )

    @staticmethod
    async def sweep_expired_locks(
        conn: ConnLike,
        now: datetime,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
        *,
        schema: str,
    ) -> int:
        return await sweep_expired_locks(conn, now, cancel_grace, cleanup_grace, schema=schema)

    @staticmethod
    async def sweep_deadline_exceeded(
        conn: ConnLike,
        now: datetime,
        *,
        schema: str,
    ) -> int:
        return await sweep_deadline_exceeded(conn, now, schema=schema)

    @staticmethod
    async def sweep_scheduled_to_pending(
        conn: ConnLike,
        now: datetime,
        *,
        schema: str,
    ) -> int:
        return await sweep_scheduled_to_pending(conn, now, schema=schema)

    @staticmethod
    async def sweep_leaked_reservation_slots(
        conn: ConnLike,
        now: datetime,
        *,
        schema: str,
    ) -> int:
        return await sweep_leaked_reservation_slots(conn, now, schema=schema)

    @staticmethod
    async def sweep_expired_results(
        conn: ConnLike,
        now: datetime,
        *,
        schema: str,
    ) -> int:
        return await sweep_expired_results(conn, now, schema=schema)

    # ── Read ────────────────────────────────────────────────────────────

    async def get(self, job_id: JobId) -> JobRow | None:
        return await _get(self._worker_pool, self._sql, job_id)

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        return await _list_jobs(self._worker_pool, self._schema_name, filters)

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        return await _count_pending_jobs(self._worker_pool, self._sql, actors)

    # ── NOTIFY hook ─────────────────────────────────────────────────────

    def subscribe_wake(self) -> AsyncContextManager[asyncio.Event]:
        event = asyncio.Event()
        return _SubscriberContext(event, self._wake_subscribers, self._wake_lock)

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
