from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import (
    AttemptOutcome,
    AttemptRow,
    Backend,
    CancelFlag,
    CancelPhase,
    EnqueueArgs,
    ErrorInfo,
    EventRow,
    JobFilter,
    JobRow,
    ScheduleCreateArgs,
    ScheduleUpdateArgs,
)
from taskq.retry import OnRetryExhausted, OnSuccess, RetryClassifierHook, RetryPolicy

__all__ = [
    "EmptyPayload",
    "FakeBackend",
    "StubActorConfig",
    "as_backend",
    "default_actor_config",
]


@dataclass(frozen=True, slots=True)
class StubActorConfig:
    retry: RetryPolicy
    non_retryable_exceptions: tuple[type[Exception], ...] = ()
    retry_classifier: RetryClassifierHook | None = None
    on_retry_exhausted: OnRetryExhausted | None = None
    on_retry_exhausted_timeout: float = 3.0
    on_success: OnSuccess | None = None
    on_success_timeout: float = 3.0


def default_actor_config() -> StubActorConfig:
    return StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))


class EmptyPayload(BaseModel):
    pass


def _make_job_row() -> JobRow:
    return JobRow(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2026, 1, 1, tzinfo=UTC),
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=new_uuid(),
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=CancelPhase.NONE,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )


class FakeBackend:
    """Minimal backend recording method calls for assertions."""

    BACKEND_PROTOCOL_VERSION: int = 2
    supports_transactional_simulation: bool = False

    def __init__(
        self,
        *,
        mark_snoozed_return: Literal["scheduled", "failed", "noop"] = "scheduled",
        mark_retry_after_return: Literal[
            "scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"
        ] = "scheduled",
    ) -> None:
        self.mark_succeeded_calls: list[tuple[UUID, UUID, dict[str, object] | None]] = []
        self.mark_cancelled_calls: list[dict[str, object]] = []
        self.mark_snoozed_calls: list[dict[str, object]] = []
        self.mark_retry_after_calls: list[dict[str, object]] = []
        self.mark_failed_or_retry_calls: list[dict[str, object]] = []
        self._mark_snoozed_return: Literal["scheduled", "failed", "noop"] = mark_snoozed_return
        self._mark_retry_after_return: Literal[
            "scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"
        ] = mark_retry_after_return

    async def enqueue(self, args: EnqueueArgs) -> JobRow:
        raise NotImplementedError

    async def enqueue_with_conn(self, conn: object, args: EnqueueArgs) -> JobRow:
        raise NotImplementedError

    async def dispatch_batch(
        self, worker_id: UUID, queues: list[str], limit: int, lock_lease: timedelta
    ) -> list[JobRow]:
        raise NotImplementedError

    async def heartbeat_jobs(self, worker_id: UUID, lock_lease: timedelta) -> int:
        return 0

    async def extend_reservation_leases(self, worker_id: UUID, lock_lease: timedelta) -> int:
        return 0

    async def mark_succeeded(
        self,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        self.mark_succeeded_calls.append((job_id, worker_id, result))
        return True

    async def mark_succeeded_with_conn(
        self,
        conn: object,
        job_id: UUID,
        worker_id: UUID,
        result: dict[str, object] | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return await self.mark_succeeded(job_id, worker_id, result, progress_seq, progress_state)

    async def mark_failed_or_retry(
        self,
        job_id: UUID,
        worker_id: UUID,
        error_info: ErrorInfo,
        next_scheduled_at: datetime | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> JobRow:
        self.mark_failed_or_retry_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "error_info": error_info,
                "next_scheduled_at": next_scheduled_at,
            }
        )
        return _make_job_row()

    async def mark_cancelled(
        self,
        job_id: UUID,
        worker_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        self.mark_cancelled_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "progress_seq": progress_seq,
                "progress_state": progress_state,
            }
        )
        return True

    async def write_cancel_escalation(
        self, job_id: UUID, worker_id: UUID, phase: Literal[2]
    ) -> bool:
        return False

    async def mark_abandoned(
        self,
        job_id: UUID,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> bool:
        return False

    async def mark_snoozed(
        self,
        job_id: UUID,
        worker_id: UUID,
        delay: timedelta,
        *,
        metadata_update: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        outcome: AttemptOutcome = "snoozed",
    ) -> Literal["scheduled", "failed", "noop"]:
        self.mark_snoozed_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "delay": delay,
                "metadata_update": metadata_update,
                "progress_seq": progress_seq,
                "progress_state": progress_state,
                "outcome": outcome,
            }
        )
        return self._mark_snoozed_return

    async def mark_retry_after(
        self,
        job_id: UUID,
        worker_id: UUID,
        delay: timedelta,
        *,
        consume_budget: bool = True,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
    ) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
        self.mark_retry_after_calls.append(
            {
                "job_id": job_id,
                "worker_id": worker_id,
                "delay": delay,
                "consume_budget": consume_budget,
                "progress_seq": progress_seq,
                "progress_state": progress_state,
            }
        )
        return self._mark_retry_after_return

    async def write_attempt(self, attempt: AttemptRow) -> None:
        pass

    async def get_attempts(self, job_id: UUID) -> list[AttemptRow]:
        return []

    async def get_events(self, job_id: UUID) -> list[EventRow]:
        return []

    async def write_cancel_request(self, job_id: UUID, reason: str | None) -> bool:
        return False

    async def poll_cancel_flags(self, worker_id: UUID) -> list[CancelFlag]:
        return []

    async def scheduled_to_pending(self, now: datetime) -> int:
        return 0

    async def deadline_sweep(self, now: datetime) -> int:
        return 0

    async def reclaim_expired_locks(
        self, now: datetime, cancel_grace: timedelta, cleanup_grace: timedelta
    ) -> int:
        return 0

    async def get(self, job_id: UUID) -> JobRow | None:
        return None

    async def list_jobs(self, filters: JobFilter) -> list[JobRow]:
        return []

    async def count_pending_jobs(self, actors: list[str]) -> dict[str, int]:
        return {}

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: object = None,
    ) -> list[JobRow]:
        raise NotImplementedError

    def subscribe_wake(self) -> AbstractAsyncContextManager[asyncio.Event]:
        raise NotImplementedError

    async def create_schedule(self, args: ScheduleCreateArgs) -> object:
        raise NotImplementedError

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[object]:
        raise NotImplementedError

    async def update_schedule(self, schedule_id: UUID, args: ScheduleUpdateArgs) -> object:
        raise NotImplementedError

    async def delete_schedule(self, schedule_id: UUID) -> None:
        raise NotImplementedError


def as_backend(fb: FakeBackend) -> Backend:
    return cast(Backend, fb)
