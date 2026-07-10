"""InMemoryBackend — deterministic, single-threaded backend for tests.

The class owns storage and the Backend protocol method bodies (enqueue,
dispatch, heartbeat, terminal writes, attempts, cancel signals, sweeps,
read) plus the sync ``subscribe_wake`` / ``subscribe_cancel_wake``.
Test-runner helpers (``run_until_drained``, ``tick_cancel_polling``,
stubs, events, ``wait_for_batch``) live in :mod:`taskq.testing._runner`;
this module re-exports them as the public testing API.
Reservation slots live in :mod:`taskq.testing._slots`.

Terminal writes (:mod:`taskq.testing._terminal`), enqueue
(:mod:`taskq.testing._enqueue`), reads (:mod:`taskq.testing._reads`),
and dispatch (:mod:`taskq.testing._dispatch`) live in companion
submodules; this module holds the cohesive core: ``__init__``, heartbeat,
cancel signals, sweeps, and schedule CRUD.

Single-threaded by contract — do not share across threads or event loops.
"""

import asyncio
import random
from collections.abc import Iterable
from contextlib import AbstractAsyncContextManager as AsyncContextManager
from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, ClassVar, Literal
from uuid import UUID

import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._cursor import decode_cursor, encode_cursor
from taskq.backend._notify import _SubscriberContext
from taskq.backend._protocol import (
    BACKEND_PROTOCOL_VERSION,
    AttemptOutcome,
    AttemptRow,
    CancelFlag,
    CancelPhase,
    EnqueueArgs,
    ErrorInfo,
    EventRow,
    IdempotencyKey,
    JobFilter,
    JobId,
    JobRow,
    QueueMode,
    ScheduleCreateArgs,
    ScheduleRecord,
    ScheduleUpdateArgs,
)
from taskq.backend.clock import Clock
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy
from taskq.testing._dispatch import _dispatch_batch, _set_queue_mode
from taskq.testing._enqueue import (
    _enqueue,
    _enqueue_batch,
    _enqueue_batch_fast,
    _enqueue_with_conn,
)
from taskq.testing._reads import (
    _count_pending_jobs,
    _get,
    _get_attempts,
    _get_events,
    _list_jobs,
)
from taskq.testing._runner import (
    PassthroughPayload,
    StubFn,
    _ArchivedJobRow,  # pyright: ignore[reportPrivateUsage]  # Why: co-located test types shared between in_memory.py and _runner.py
    _InMemoryActorConfig,  # pyright: ignore[reportPrivateUsage]  # Why: co-located test types shared between in_memory.py and _runner.py
    wait_for_batch,
)
from taskq.testing._runner import (
    advance_clock_to as _advance_clock_to,
)
from taskq.testing._runner import (
    archive_terminal_jobs as _archive_terminal_jobs,
)
from taskq.testing._runner import (
    expire_archived_jobs as _expire_archived_jobs,
)
from taskq.testing._runner import (
    get_archived as _get_archived,
)
from taskq.testing._runner import (
    register_actor_config as _register_actor_config,
)
from taskq.testing._runner import (
    register_actor_configs as _register_actor_configs,
)
from taskq.testing._runner import (
    register_cancel_event as _register_cancel_event,
)
from taskq.testing._runner import (
    register_stub as _register_stub,
)
from taskq.testing._runner import (
    run_until_drained as _run_until_drained,
)
from taskq.testing._runner import (
    tick_cancel_polling as _tick_cancel_polling,
)
from taskq.testing._slots import _SlotTable
from taskq.testing._sweeps import (
    _deadline_sweep,
    _reclaim_expired_locks,
    _scheduled_to_pending,
)
from taskq.testing._terminal import (
    _mark_abandoned,
    _mark_cancelled,
    _mark_failed_or_retry,
    _mark_retry_after,
    _mark_snoozed,
    _mark_succeeded,
    _mark_succeeded_with_conn,
    _write_attempt,
    _write_cancel_escalation,
)
from taskq.worker.actor_config import ActorConfig

if TYPE_CHECKING:
    from taskq.worker.leader import ArchiveExpiryResult, PruneResult

__all__ = [
    "BACKEND_PROTOCOL_VERSION",
    "InMemoryBackend",
    "PassthroughPayload",
    "StubFn",
    "decode_cursor",
    "encode_cursor",
    "wait_for_batch",
]

logger = structlog.get_logger("taskq.testing.in_memory")

_EXPECTED_PROTOCOL_VERSION = 2
if BACKEND_PROTOCOL_VERSION != _EXPECTED_PROTOCOL_VERSION:
    raise RuntimeError(
        f"InMemoryBackend was built for protocol v{_EXPECTED_PROTOCOL_VERSION}; "
        f"current BACKEND_PROTOCOL_VERSION is {BACKEND_PROTOCOL_VERSION}. "
        "Update the implementation."
    )


class InMemoryBackend:
    """Deterministic, in-memory backend for unit tests.

    All state is held as per-instance attributes: no module-level
    mutable state, no class-level caches.  Two ``InMemoryBackend`` instances
    created in the same test session are fully isolated.

    Single-threaded by contract — do not share across threads or event loops.
    Intra-coroutine re-entry within a single event loop is acceptable;
    cross-thread use is a caller bug.
    """

    BACKEND_PROTOCOL_VERSION: ClassVar[int] = BACKEND_PROTOCOL_VERSION

    supports_transactional_simulation: ClassVar[bool] = (
        True  # Why: in-memory backend has no real transactions; SubJobEnqueuer buffers and flushes/discards to simulate rollback semantics.
    )

    def __init__(
        self,
        clock: Clock,
        cancellation_grace_period: timedelta = timedelta(seconds=30),
        cleanup_grace_period: timedelta = timedelta(seconds=30),
        rng: random.Random | None = None,
        *,
        actor_configs: Iterable[ActorConfig] | None = None,
    ) -> None:
        self._clock = clock
        self._cancellation_grace = cancellation_grace_period
        self._cleanup_grace = cleanup_grace_period
        self._worker_id: UUID = new_uuid()
        self._rng = rng

        self._jobs: dict[JobId, JobRow] = {}
        self._attempts: dict[JobId, list[AttemptRow]] = {}
        self._events: list[EventRow] = []
        self._idempotency_index: dict[IdempotencyKey, JobId] = {}
        self._event_seq: int = 0
        self._cancel_observed_at: dict[JobId, datetime] = {}
        self._cancel_events: dict[JobId, asyncio.Event] = {}
        self._wake_subscribers: set[asyncio.Event] = set()
        self._cancel_wake_subscribers: set[asyncio.Event] = set()
        self._actor_stubs: dict[str, StubFn] = {}
        self._actor_configs: dict[str, _InMemoryActorConfig] = {}
        self._actor_configs_meta: dict[str, ActorConfig] = {}
        if actor_configs is not None:
            for cfg in actor_configs:
                self._actor_configs_meta[cfg.actor] = cfg
        self._slot_table: _SlotTable | None = None
        self._archive: dict[JobId, _ArchivedJobRow] = {}
        self._archive_attempts: dict[JobId, list[AttemptRow]] = {}
        self._schedules: dict[UUID, ScheduleRecord] = {}
        self._queues: dict[str, QueueMode] = {}

    # ── Test helpers ────────────────────────────────────────────────────

    @property
    def slot_table(self) -> _SlotTable:
        if self._slot_table is None:
            self._slot_table = _SlotTable()
        return self._slot_table

    def advance_clock_to(self, when: datetime) -> None:
        _advance_clock_to(self, when)

    def register_stub(
        self,
        actor_name: str,
        fn: StubFn,
        *,
        retry: RetryPolicy | None = None,
        non_retryable_exceptions: tuple[type[BaseException], ...] = (),
        retry_classifier: RetryClassifierHook | None = None,
        on_retry_exhausted: OnRetryExhausted | None = None,
        on_retry_exhausted_timeout: float = 3.0,
        on_success: OnSuccess | None = None,
        on_success_timeout: float = 3.0,
        payload_type: type[BaseModel] | None = None,
    ) -> None:
        _register_stub(
            self,
            actor_name,
            fn,
            retry=retry,
            non_retryable_exceptions=non_retryable_exceptions,
            retry_classifier=retry_classifier,
            on_retry_exhausted=on_retry_exhausted,
            on_retry_exhausted_timeout=on_retry_exhausted_timeout,
            on_success=on_success,
            on_success_timeout=on_success_timeout,
            payload_type=payload_type,
        )

    def register_cancel_event(self, job_id: JobId, event: asyncio.Event) -> None:
        _register_cancel_event(self, job_id, event)

    # ── Event and attempt helpers ────────────────────────

    async def get_events(self, job_id: JobId) -> list[EventRow]:
        return await _get_events(self, job_id)

    def _append_attempt(
        self,
        job_id: JobId,
        attempt: int,
        started_at: datetime | None,
        now: datetime,
        outcome: AttemptOutcome,
        error_class: str | None,
        error_message: str | None,
        error_traceback: str | None,
        worker_id: UUID | None,
    ) -> None:
        duration_ms: int | None = None
        if started_at is not None:
            duration_ms = int((now - started_at).total_seconds() * 1000)
        self._attempts.setdefault(job_id, []).append(
            AttemptRow(
                job_id=job_id,
                attempt=attempt,
                started_at=started_at if started_at is not None else now,
                finished_at=now,
                outcome=outcome,
                error_class=error_class,
                error_message=error_message,
                error_traceback=error_traceback,
                duration_ms=duration_ms,
                worker_id=worker_id,
                metadata={},
            )
        )

    def _append_state_change_event(
        self,
        job_id: JobId,
        from_state: str,
        to_state: str,
        now: datetime,
        error_class: str | None = None,
        worker_id: UUID | None = None,
        **extra_detail: object,
    ) -> None:
        self._event_seq += 1
        detail: dict[str, object] = {"from_state": from_state, "to_state": to_state}
        if error_class is not None:
            detail["error_class"] = error_class
        if worker_id is not None:
            detail["worker_id"] = worker_id
        detail.update(extra_detail)
        self._events.append(
            EventRow(
                event_id=self._event_seq,
                job_id=job_id,
                occurred_at=now,
                kind="state_change",
                detail=detail,
            )
        )

    def _append_cancel_request_event(
        self,
        job_id: JobId,
        now: datetime,
        reason: str | None,
    ) -> None:
        self._event_seq += 1
        detail: dict[str, object] = {}
        if reason is not None:
            detail["reason"] = reason
        self._events.append(
            EventRow(
                event_id=self._event_seq,
                job_id=job_id,
                occurred_at=now,
                kind="cancel_request",
                detail=detail,
            )
        )

    # ── Enqueue ────────────────────────────────────────────────────────

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        return await _enqueue(self, args)

    async def enqueue_with_conn(
        self,
        conn: object,
        args: EnqueueArgs,
    ) -> JobRow:
        return await _enqueue_with_conn(self, conn, args)

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: object = None,
    ) -> list[JobRow]:
        return await _enqueue_batch(self, args_list, connection=connection)

    async def enqueue_batch_fast(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: object = None,
    ) -> int:
        return await _enqueue_batch_fast(self, args_list, connection=connection)

    # ── Actor-config registry helpers ───────────────────────────────────

    def register_actor_config(
        self,
        *,
        actor: str,
        max_concurrent: int | None = None,
        queue: str = "default",
        metadata: dict[str, object] | None = None,
    ) -> None:
        _register_actor_config(
            self,
            actor=actor,
            max_concurrent=max_concurrent,
            queue=queue,
            metadata=metadata,
        )

    def register_actor_configs(self, configs: Iterable[ActorConfig]) -> None:
        _register_actor_configs(self, configs)

    # ── Dispatch ────────────────────────────────────────────────────────

    def set_queue_mode(self, queue_name: str, mode: QueueMode) -> None:
        _set_queue_mode(self, queue_name, mode)

    async def dispatch_batch(
        self,
        worker_id: UUID,
        queues: list[str],
        limit: int,
        lock_lease: timedelta,
    ) -> list[JobRow]:
        return await _dispatch_batch(self, worker_id, queues, limit, lock_lease)

    # ── Heartbeat ──────────────────────────────────────────────────────

    async def heartbeat_jobs(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
    ) -> int:
        now = self._clock.now()
        count = 0
        for job_id, row in list(self._jobs.items()):
            if row.status == "running" and row.locked_by_worker == worker_id:
                self._jobs[job_id] = replace(
                    row,
                    lock_expires_at=now + lock_lease,
                    last_heartbeat_at=now,
                )
                count += 1
        return count

    async def extend_reservation_leases(
        self,
        worker_id: UUID,
        lock_lease: timedelta,
    ) -> int:
        now = self._clock.now()
        count = 0
        for job_id, row in list(self._jobs.items()):
            if row.status == "running" and row.locked_by_worker == worker_id:
                if self._slot_table is not None:
                    count += self._slot_table.extend_leases_for_job(job_id, now, lock_lease)
                else:
                    count += 1
        return count

    # ── Terminal writes ────────────────────────────────────────────────

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_succeeded(self, job_id, worker_id, result, progress_seq, progress_state)

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_succeeded_with_conn(
            self, conn, job_id, worker_id, result, progress_seq, progress_state
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
            self, job_id, worker_id, error_info, next_scheduled_at, progress_seq, progress_state
        )

    async def mark_cancelled(
        self,
        job_id: JobId,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_cancelled(self, job_id, worker_id, progress_seq, progress_state)

    async def write_cancel_escalation(
        self,
        job_id: JobId,
        worker_id: UUID,
        phase: Literal[2],
    ) -> bool:
        return await _write_cancel_escalation(self, job_id, worker_id, phase)

    async def mark_abandoned(
        self,
        job_id: JobId,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await _mark_abandoned(self, job_id, progress_seq, progress_state)

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
            self,
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
            self,
            job_id,
            worker_id,
            delay,
            consume_budget=consume_budget,
            progress_seq=progress_seq,
            progress_state=progress_state,
        )

    # ── Attempt history ────────────────────────────────────────────────

    async def write_attempt(self, attempt: AttemptRow) -> None:
        await _write_attempt(self, attempt)

    async def get_attempts(self, job_id: JobId) -> list[AttemptRow]:
        return await _get_attempts(self, job_id)

    # ── Cancel signals ─────────────────────────────────────────────────

    async def write_cancel_request(
        self,
        job_id: JobId,
        reason: str | None,
    ) -> bool:
        row = self._jobs.get(job_id)
        if row is None:
            return False

        if row.status == "running" and row.cancel_phase == 0:
            now = self._clock.now()
            self._jobs[job_id] = replace(
                row,
                cancel_requested_at=now,
                cancel_phase=1,
            )
            self._append_cancel_request_event(job_id, now, reason)
            for event in self._cancel_wake_subscribers:
                event.set()
            logger.debug(
                "cancel_requested",
                kind="state_change",
                from_state="running",
                to_state="running",
                job_id=job_id,
                cancel_phase=1,
            )
            return True

        if row.status in ("pending", "scheduled"):
            now = self._clock.now()
            prev_status = row.status
            self._jobs[job_id] = replace(
                row,
                status="cancelled",
                finished_at=now,
            )
            self._append_state_change_event(
                job_id=job_id,
                from_state=prev_status,
                to_state="cancelled",
                now=now,
            )
            self._append_cancel_request_event(job_id, now, reason)
            logger.debug(
                "state_change",
                kind="state_change",
                from_state=prev_status,
                to_state="cancelled",
                job_id=job_id,
            )
            return True

        return False

    async def poll_cancel_flags(
        self,
        worker_id: UUID,
    ) -> list[CancelFlag]:
        return [
            CancelFlag(job_id=row.id, cancel_phase=row.cancel_phase)
            for row in self._jobs.values()
            if row.cancel_requested_at is not None
            and row.status == "running"
            and row.locked_by_worker == worker_id
        ]

    # ── Admin operations ──────────────────────────────────────────────

    async def retry_job(self, job_id: JobId) -> bool:
        row = self._jobs.get(job_id)
        if row is None or row.status not in ("failed", "crashed", "cancelled"):
            return False
        self._jobs[job_id] = replace(
            row,
            status="pending",
            attempt=0,
            cancel_phase=CancelPhase.NONE,
            error_class=None,
            error_message=None,
            error_traceback=None,
            scheduled_at=self._clock.now(),
            finished_at=None,
            result=None,
            result_size_bytes=None,
            result_expires_at=None,
        )
        for event in self._wake_subscribers:
            event.set()
        return True

    # ── Scheduling / sweeps ────────────────────────────────────────────

    async def scheduled_to_pending(self, now: datetime) -> int:
        return await _scheduled_to_pending(self, now)

    async def deadline_sweep(self, now: datetime) -> int:
        return await _deadline_sweep(self, now)

    async def reclaim_expired_locks(
        self,
        now: datetime,
        cancel_grace: timedelta,
        cleanup_grace: timedelta,
    ) -> int:
        return await _reclaim_expired_locks(self, now, cancel_grace, cleanup_grace)

    # ── Archive and expiry simulation ─────────────────────────────────

    def archive_terminal_jobs(
        self,
        retention: timedelta,
        archive_retention: timedelta,
        *,
        statuses: frozenset[str] | None = None,
    ) -> "PruneResult":
        return _archive_terminal_jobs(self, retention, archive_retention, statuses=statuses)

    def expire_archived_jobs(self) -> "ArchiveExpiryResult":
        return _expire_archived_jobs(self)

    async def get_archived(self, job_id: JobId) -> _ArchivedJobRow | None:
        return await _get_archived(self, job_id)

    # ── Read ───────────────────────────────────────────────────────────

    async def get(self, job_id: JobId) -> JobRow | None:
        return await _get(self, job_id)

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        return await _list_jobs(self, filters)

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        return await _count_pending_jobs(self, actors)

    # ── NOTIFY hook ────────────────────────────────────────────────────

    def subscribe_wake(self) -> AsyncContextManager[asyncio.Event]:
        event = asyncio.Event()
        return _SubscriberContext(event, self._wake_subscribers)

    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]:
        event = asyncio.Event()
        return _SubscriberContext(event, self._cancel_wake_subscribers)

    # ── Cancel polling ─────────────────────────────────────────────────

    async def tick_cancel_polling(self) -> None:
        await _tick_cancel_polling(self)

    # ── Run until drained ──────────────────────────────────────────────

    async def run_until_drained(self) -> None:
        await _run_until_drained(self)

    # ── Schedule CRUD ──────────────────────────────────────────────────

    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord:
        for rec in self._schedules.values():
            if rec.actor == args.actor and rec.name == args.name:
                raise ValueError(
                    f"schedule for actor {args.actor!r} name {args.name!r} already exists"
                )
        sid = new_uuid()
        record = ScheduleRecord(
            id=sid,
            actor=args.actor,
            name=args.name,
            cron_expr=args.cron_expr,
            timezone=args.timezone,
            dst_strategy=args.dst_strategy,
            payload_factory=args.payload_factory,
            identity_key=args.identity_key,
            enabled=args.enabled,
            last_fired_at=None,
            last_fire_error=None,
            consecutive_failures=0,
            next_fire_at=args.next_fire_at,
            metadata=args.metadata,
        )
        self._schedules[sid] = record
        return record

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]:
        results: list[ScheduleRecord] = []
        for rec in self._schedules.values():
            if actor is not None and rec.actor != actor:
                continue
            if enabled is not None and rec.enabled != enabled:
                continue
            results.append(rec)
        return results

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord:
        rec = self._schedules.get(schedule_id)
        if rec is None:
            raise KeyError(f"schedule {schedule_id} not found")

        updates: dict[str, object] = {}
        if args.cron_expr is not None:
            updates["cron_expr"] = args.cron_expr
        if args.next_fire_at is not None:
            updates["next_fire_at"] = args.next_fire_at
        if args.enabled is not None:
            updates["enabled"] = args.enabled
            if args.enabled:
                updates["consecutive_failures"] = 0
                updates["last_fire_error"] = None
        if args.payload_factory is not None:
            updates["payload_factory"] = args.payload_factory
        elif args.clear_payload_factory:
            updates["payload_factory"] = None
        if args.metadata is not None:
            updates["metadata"] = args.metadata
        if args.consecutive_failures is not None:
            updates["consecutive_failures"] = args.consecutive_failures
        if args.last_fire_error is not None:
            updates["last_fire_error"] = args.last_fire_error

        updated = rec.model_copy(update=updates)
        self._schedules[schedule_id] = updated
        return updated

    async def delete_schedule(self, schedule_id: UUID) -> None:
        self._schedules.pop(schedule_id, None)
