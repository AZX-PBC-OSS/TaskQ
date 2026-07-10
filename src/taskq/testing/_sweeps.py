"""Sweep operations for InMemoryBackend.

``scheduled_to_pending``, ``deadline_sweep``, and ``reclaim_expired_locks``
live here as module-level functions taking ``self: InMemoryBackend`` as
the first parameter, following the :mod:`taskq.testing._runner` pattern.
"""

from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from taskq.backend._protocol import AttemptRow
from taskq.obs import record_deadline_exceeded_swept

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_deadline_sweep",
    "_reclaim_expired_locks",
    "_scheduled_to_pending",
]

logger = structlog.get_logger("taskq.testing.in_memory")


async def _scheduled_to_pending(self: "InMemoryBackend", now: datetime) -> int:
    count = 0
    for job_id, row in list(self._jobs.items()):
        if row.status == "scheduled" and row.scheduled_at <= now:
            self._jobs[job_id] = replace(row, status="pending")
            self._append_state_change_event(
                job_id=job_id,
                from_state="scheduled",
                to_state="pending",
                now=now,
            )
            logger.debug(
                "state_change",
                kind="state_change",
                from_state="scheduled",
                to_state="pending",
                job_id=job_id,
            )
            count += 1
    if count > 0:
        for event in self._wake_subscribers:
            event.set()
    return count


async def _deadline_sweep(self: "InMemoryBackend", now: datetime) -> int:
    count = 0
    for job_id, row in list(self._jobs.items()):
        if (
            row.status in ("pending", "scheduled")
            and row.schedule_to_close is not None
            and row.schedule_to_close < now
        ):
            self._jobs[job_id] = replace(
                row,
                status="failed",
                finished_at=now,
                error_class="DeadlineExceeded",
                error_message="schedule_to_close reached before next dispatch",
            )
            attempt_row = AttemptRow(
                job_id=job_id,
                attempt=row.attempt,
                started_at=row.started_at if row.started_at is not None else now,
                finished_at=now,
                outcome="failed",
                error_class="DeadlineExceeded",
                error_message="schedule_to_close reached before next dispatch",
                error_traceback=None,
                duration_ms=None,
                worker_id=None,
                metadata={},
            )
            self._attempts.setdefault(job_id, []).append(attempt_row)
            self._append_state_change_event(
                job_id=job_id,
                from_state=row.status,
                to_state="failed",
                now=now,
                error_class="DeadlineExceeded",
            )
            record_deadline_exceeded_swept(actor=row.actor)
            logger.debug(
                "state_change",
                kind="state_change",
                from_state=row.status,
                to_state="failed",
                job_id=job_id,
            )
            count += 1
    return count


async def _reclaim_expired_locks(
    self: "InMemoryBackend",
    now: datetime,
    cancel_grace: timedelta,
    cleanup_grace: timedelta,
) -> int:
    count = 0
    for job_id, row in list(self._jobs.items()):
        if (
            row.status == "running"
            and row.lock_expires_at is not None
            and row.lock_expires_at < now
            and row.cancel_phase == 0
        ):
            duration_ms: int | None = None
            if row.started_at is not None:
                delta = now - row.started_at
                duration_ms = int(delta.total_seconds() * 1000)

            attempt_row = AttemptRow(
                job_id=row.id,
                attempt=row.attempt,
                started_at=row.started_at if row.started_at is not None else now,
                finished_at=now,
                outcome="crashed",
                error_class="WorkerCrashed",
                error_message="lock expired before worker reported terminal state",
                error_traceback=None,
                duration_ms=duration_ms,
                worker_id=row.locked_by_worker,
                metadata={},
            )
            self._attempts.setdefault(job_id, []).append(attempt_row)

            if row.attempt < row.max_attempts and row.retry_kind != "non_retryable":
                new_scheduled = self._clock.now() + timedelta(seconds=5)
                self._jobs[job_id] = replace(
                    row,
                    status="pending",
                    scheduled_at=new_scheduled,
                    locked_by_worker=None,
                    lock_expires_at=None,
                )
                self._append_state_change_event(
                    job_id,
                    from_state="running",
                    to_state="pending",
                    now=now,
                    worker_id=row.locked_by_worker,
                    reason="lock_expired",
                )
                logger.debug(
                    "state_change",
                    kind="state_change",
                    from_state="running",
                    to_state="pending",
                    job_id=job_id,
                )
                for event in self._wake_subscribers:
                    event.set()
            else:
                self._jobs[job_id] = replace(
                    row,
                    status="crashed",
                    finished_at=now,
                )
                self._append_state_change_event(
                    job_id,
                    from_state="running",
                    to_state="crashed",
                    now=now,
                    worker_id=row.locked_by_worker,
                    reason="lock_expired",
                )
                logger.debug(
                    "state_change",
                    kind="state_change",
                    from_state="running",
                    to_state="crashed",
                    job_id=job_id,
                )
            count += 1
    return count
