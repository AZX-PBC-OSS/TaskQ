"""Terminal-write operations for InMemoryBackend.

All ``mark_*`` methods and ``write_attempt`` live here as module-level
functions taking ``self: InMemoryBackend`` as the first parameter,
following the :mod:`taskq.testing._runner` pattern.
"""

from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog

from taskq._json import dumps_str as _json_dumps_str
from taskq.backend._protocol import (
    AttemptOutcome,
    AttemptRow,
    CancelPhase,
    ErrorInfo,
    JobId,
    JobRow,
)
from taskq.constants import MAX_RESULT_BYTES
from taskq.exceptions import (
    ResultTooLarge,
    WorkerOwnershipMismatch,
)

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_mark_abandoned",
    "_mark_cancelled",
    "_mark_failed_or_retry",
    "_mark_retry_after",
    "_mark_snoozed",
    "_mark_succeeded",
    "_mark_succeeded_with_conn",
    "_merge_progress",
    "_write_attempt",
    "_write_cancel_escalation",
]

logger: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.testing.in_memory")


def _merge_progress(
    current: dict[str, object] | None,
    update: dict[str, object] | None,
) -> dict[str, object] | None:
    """Mirror PG ``COALESCE(progress_state,'{}') || new`` for terminal writes."""
    if update is not None:
        return (current or {}) | update
    return current


async def _mark_succeeded(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.locked_by_worker != worker_id:
        return False

    now = self._clock.now()
    result_size_bytes: int | None = (
        len(_json_dumps_str(result).encode("utf-8")) if result is not None else None
    )
    if result_size_bytes is not None and result_size_bytes > MAX_RESULT_BYTES:
        raise ResultTooLarge(
            f"result size {result_size_bytes} bytes exceeds {MAX_RESULT_BYTES} byte cap"
        )
    new_result_expires_at = row.result_expires_at
    actor_cfg = self._actor_configs_meta.get(row.actor)
    if actor_cfg is not None and actor_cfg.result_ttl is not None:
        new_result_expires_at = now + timedelta(seconds=actor_cfg.result_ttl)
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status="succeeded",
        result=result,
        result_size_bytes=result_size_bytes,
        result_expires_at=new_result_expires_at,
        finished_at=now,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="succeeded",
        error_class=None,
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="succeeded",
        now=now,
        worker_id=worker_id,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="succeeded",
        job_id=job_id,
    )
    return True


async def _mark_succeeded_with_conn(
    self: "InMemoryBackend",
    conn: object,
    job_id: JobId,
    worker_id: UUID,
    result: dict[str, object] | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    return await _mark_succeeded(self, job_id, worker_id, result, progress_seq, progress_state)


async def _mark_failed_or_retry(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    error_info: ErrorInfo,
    next_scheduled_at: datetime | None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> JobRow:
    row = self._jobs.get(job_id)
    if row is None:
        raise KeyError(f"Job {job_id} not found")

    if row.status != "running":
        raise WorkerOwnershipMismatch(job_id, worker_id, row.locked_by_worker)

    if row.locked_by_worker != worker_id:
        raise WorkerOwnershipMismatch(job_id, worker_id, row.locked_by_worker)

    if next_scheduled_at is not None:
        now = self._clock.now()
        retry_status: Literal["scheduled", "pending"] = (
            "scheduled" if next_scheduled_at > now else "pending"
        )
        merged_progress = _merge_progress(row.progress_state, progress_state)
        updated = replace(
            row,
            status=retry_status,
            scheduled_at=next_scheduled_at,
            finished_at=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            error_class=error_info.error_class,
            error_message=error_info.error_message,
            error_traceback=error_info.error_traceback,
            cancel_phase=row.cancel_phase,
            cancel_requested_at=row.cancel_requested_at,
            progress_seq=progress_seq,
            progress_state=merged_progress,
        )
        self._jobs[job_id] = updated
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class=error_info.error_class,
            error_message=error_info.error_message,
            error_traceback=error_info.error_traceback,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="scheduled",
            now=now,
            error_class=error_info.error_class,
            worker_id=worker_id,
        )
        logger.debug(
            "state_change",
            kind="state_change",
            from_state="running",
            to_state="scheduled",
            job_id=job_id,
        )
        return updated

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    updated = replace(
        row,
        status="failed",
        finished_at=now,
        locked_by_worker=None,
        lock_expires_at=None,
        error_class=error_info.error_class,
        error_message=error_info.error_message,
        error_traceback=error_info.error_traceback,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._jobs[job_id] = updated
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="failed",
        error_class=error_info.error_class,
        error_message=error_info.error_message,
        error_traceback=error_info.error_traceback,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="failed",
        now=now,
        error_class=error_info.error_class,
        worker_id=worker_id,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="failed",
        job_id=job_id,
    )
    return updated


async def _mark_cancelled(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:
    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.locked_by_worker != worker_id:
        return False

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status="cancelled",
        finished_at=now,
        locked_by_worker=None,
        lock_expires_at=None,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="cancelled",
        error_class=None,
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="cancelled",
        now=now,
        worker_id=worker_id,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="cancelled",
        job_id=job_id,
    )
    return True


async def _write_cancel_escalation(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    phase: Literal[2],
) -> bool:

    if phase != 2:
        raise ValueError(
            "write_cancel_escalation only accepts phase=2; "
            "phase=1 is written by write_cancel_request"
        )

    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.locked_by_worker != worker_id:
        return False
    if row.cancel_phase != CancelPhase.COOPERATIVE:
        return False

    now = self._clock.now()
    self._jobs[job_id] = replace(row, cancel_phase=CancelPhase.FORCED)
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="running",
        now=now,
        cancel_phase_from=CancelPhase.COOPERATIVE,
        cancel_phase_to=CancelPhase.FORCED,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="running",
        job_id=job_id,
        cancel_phase=CancelPhase.FORCED,
    )
    return True


async def _mark_abandoned(
    self: "InMemoryBackend",
    job_id: JobId,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> bool:

    row = self._jobs.get(job_id)
    if row is None:
        return False
    if row.status != "running" or row.cancel_phase != CancelPhase.FORCED:
        return False

    now = self._clock.now()
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status="abandoned",
        finished_at=now,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="cancelled",
        error_class=None,
        error_message=None,
        error_traceback=None,
        worker_id=row.locked_by_worker,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="abandoned",
        now=now,
        worker_id=row.locked_by_worker,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="abandoned",
        job_id=job_id,
    )
    return True


async def _mark_snoozed(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    *,
    metadata_update: dict[str, object] | None = None,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
    outcome: AttemptOutcome = "snoozed",
) -> Literal["scheduled", "failed", "noop"]:
    row = self._jobs.get(job_id)
    if row is None or row.status != "running" or row.locked_by_worker != worker_id:
        return "noop"

    now = self._clock.now()
    new_scheduled_at = now + delay

    if row.schedule_to_close is not None and new_scheduled_at > row.schedule_to_close:
        deadline_merged_progress = _merge_progress(row.progress_state, progress_state)
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message="schedule_to_close reached before next dispatch",
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            progress_seq=progress_seq,
            progress_state=deadline_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="DeadlineExceeded",
            error_message="schedule_to_close reached before next dispatch",
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="DeadlineExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state_change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=job_id,
        )
        return "failed"

    new_metadata = row.metadata if metadata_update is None else {**row.metadata, **metadata_update}
    snooze_status: Literal["scheduled", "pending"] = (
        "scheduled" if new_scheduled_at > now else "pending"
    )
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status=snooze_status,
        scheduled_at=new_scheduled_at,
        finished_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        last_heartbeat_at=None,
        max_attempts=row.max_attempts + 1,
        metadata=new_metadata,
        cancel_phase=row.cancel_phase,
        cancel_requested_at=row.cancel_requested_at,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome=outcome,
        error_class=None,
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="scheduled",
        now=now,
        worker_id=worker_id,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="scheduled",
        job_id=job_id,
    )
    return "scheduled"


async def _mark_retry_after(
    self: "InMemoryBackend",
    job_id: JobId,
    worker_id: UUID,
    delay: timedelta,
    *,
    consume_budget: bool = True,
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> Literal["scheduled", "failed:DeadlineExceeded", "failed:MaxAttemptsExceeded", "noop"]:
    row = self._jobs.get(job_id)
    if row is None or row.status != "running" or row.locked_by_worker != worker_id:
        return "noop"

    now = self._clock.now()
    new_scheduled_at = now + delay

    if row.schedule_to_close is not None and new_scheduled_at > row.schedule_to_close:
        deadline_merged_progress = _merge_progress(row.progress_state, progress_state)
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message="schedule_to_close reached before next dispatch",
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            progress_seq=progress_seq,
            progress_state=deadline_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="DeadlineExceeded",
            error_message="schedule_to_close reached before next dispatch",
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="DeadlineExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state_change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=job_id,
            worker_id=worker_id,
            attempt=row.attempt,
            cause="retry_after",
        )
        return "failed:DeadlineExceeded"

    if consume_budget and row.retry_kind == "transient" and row.attempt >= row.max_attempts:
        maxatt_merged_progress = _merge_progress(row.progress_state, progress_state)
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="MaxAttemptsExceeded",
            error_message="retry budget exhausted",
            error_traceback=None,
            locked_by_worker=None,
            lock_expires_at=None,
            last_heartbeat_at=None,
            attempt=row.attempt,
            progress_seq=progress_seq,
            progress_state=maxatt_merged_progress,
        )
        self._append_attempt(
            job_id=job_id,
            attempt=row.attempt,
            started_at=row.started_at,
            now=now,
            outcome="failed",
            error_class="MaxAttemptsExceeded",
            error_message="retry budget exhausted",
            error_traceback=None,
            worker_id=worker_id,
        )
        self._append_state_change_event(
            job_id=job_id,
            from_state="running",
            to_state="failed",
            now=now,
            error_class="MaxAttemptsExceeded",
            worker_id=worker_id,
        )
        logger.debug(
            "state_change",
            kind="state_change",
            from_state="running",
            to_state="failed",
            job_id=job_id,
            worker_id=worker_id,
            attempt=row.attempt,
            cause="retry_after",
        )
        return "failed:MaxAttemptsExceeded"

    new_attempt = row.attempt
    new_max_attempts = row.max_attempts if consume_budget else row.max_attempts + 1
    retry_status: Literal["scheduled", "pending"] = (
        "scheduled" if new_scheduled_at > now else "pending"
    )
    merged_progress = _merge_progress(row.progress_state, progress_state)
    self._jobs[job_id] = replace(
        row,
        status=retry_status,
        scheduled_at=new_scheduled_at,
        finished_at=None,
        attempt=new_attempt,
        max_attempts=new_max_attempts,
        locked_by_worker=None,
        lock_expires_at=None,
        last_heartbeat_at=None,
        cancel_phase=row.cancel_phase,
        cancel_requested_at=row.cancel_requested_at,
        progress_seq=progress_seq,
        progress_state=merged_progress,
    )
    self._append_attempt(
        job_id=job_id,
        attempt=row.attempt,
        started_at=row.started_at,
        now=now,
        outcome="snoozed",
        error_class="RetryAfter",
        error_message=None,
        error_traceback=None,
        worker_id=worker_id,
    )
    self._append_state_change_event(
        job_id=job_id,
        from_state="running",
        to_state="scheduled",
        now=now,
        worker_id=worker_id,
    )
    logger.debug(
        "state_change",
        kind="state_change",
        from_state="running",
        to_state="scheduled",
        job_id=job_id,
        worker_id=worker_id,
        attempt=new_attempt,
        cause="retry_after",
    )
    return "scheduled"


async def _write_attempt(self: "InMemoryBackend", attempt: AttemptRow) -> None:
    self._attempts.setdefault(attempt.job_id, []).append(attempt)
