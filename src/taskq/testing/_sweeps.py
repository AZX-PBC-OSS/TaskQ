"""Sweep operations for InMemoryBackend.

``scheduled_to_pending``, ``deadline_sweep``, and ``reclaim_expired_locks``
live here as module-level functions taking ``self: InMemoryBackend`` as
the first parameter, following the :mod:`taskq.testing._runner` pattern.

No caller-supplied ``now``: the backend's injected ``Clock`` is the single
arbiter — the InMemory mirror of PG's server-side ``clock_timestamp()``
predicates (parity by construction).

Each twin processes at most ``batch_size`` eligible rows per call,
mirroring the Postgres sweeps' bounded batch: one call makes a bounded
amount of progress, repeated calls drain.  The twins walk the corpus in
dict-iteration order while the Postgres snaps drain oldest-eligible-first
(their ORDER BY pins) — row ORDER is not a parity property, so the twins
deliberately do not fake one; the parity contract is the total rows
drained and the per-row state and audit trail left behind (pinned by
``tests/test_rt_sweeps_parity.py``).  ``batch_size`` is validated at the
same boundary and with the same ValueError the Postgres sweeps raise, so
one input cannot mean three things across the two backends.
"""

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING

import structlog

from taskq.backend._protocol import AttemptRow, CancelPhase
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the twins must enforce the identical boundary contract the Postgres sweeps enforce — one validator, one seam, no drift.
    _validate_positive,
)
from taskq.constants import DEFAULT_EVENT_WRITER_BATCH_SIZE
from taskq.obs import record_deadline_exceeded_swept

if TYPE_CHECKING:
    from taskq.testing.in_memory import InMemoryBackend

__all__ = [
    "_deadline_sweep",
    "_reclaim_expired_locks",
    "_scheduled_to_pending",
]

logger = structlog.get_logger("taskq.testing.in_memory")


async def _scheduled_to_pending(
    self: "InMemoryBackend",
    *,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    _validate_positive("batch_size", batch_size)
    now = self._clock.now()
    count = 0
    for job_id, row in list(self._jobs.items()):
        if count >= batch_size:
            break
        if row.status == "scheduled" and row.scheduled_at <= now:
            self._jobs[job_id] = replace(row, status="pending")
            self._append_state_change_event(
                job_id=job_id,
                from_state="scheduled",
                to_state="pending",
                now=now,
            )
            logger.debug(
                "state-change",
                kind="state_change",
                from_state="scheduled",
                to_state="pending",
                job_id=str(job_id),
            )
            count += 1
    if count > 0:
        for event in self._wake_subscribers:
            event.set()
    return count


async def _deadline_sweep(
    self: "InMemoryBackend",
    *,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    _validate_positive("batch_size", batch_size)
    now = self._clock.now()
    count = 0
    for job_id, row in list(self._jobs.items()):
        if count >= batch_size:
            break
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
                "state-change",
                kind="state_change",
                from_state=row.status,
                to_state="failed",
                job_id=str(job_id),
            )
            count += 1
    return count


async def _reclaim_expired_locks(
    self: "InMemoryBackend",
    cancel_grace: timedelta,
    cleanup_grace: timedelta,
    *,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    # Mirrors PostgresBackend._SWEEP_1_SQL exactly, in both directions:
    # * carve-out — a job with an in-flight cancel request
    #   (cancel_phase != 0) is normally left for the cancellation
    #   protocol to finish, but is still reclaimed once its lock has been
    #   expired for cancel_grace + cleanup_grace + a flat 60s safety
    #   margin (see _sweeps.py's _SWEEP_1_SQL comment) — otherwise a
    #   worker that died mid-cancellation would never be recovered;
    # * terminal labels — the retry branch resets cancel state (clean
    #   slate for the next dispatch); the exhausted branch lands on
    #   'cancelled' when a cancel was in-flight, 'crashed' otherwise,
    #   while the attempt row records outcome='crashed' either way.
    _validate_positive("batch_size", batch_size)
    now = self._clock.now()
    deep_expiry_margin = cancel_grace + cleanup_grace + timedelta(seconds=60)
    count = 0
    for job_id, row in list(self._jobs.items()):
        if count >= batch_size:
            break
        if (
            row.status == "running"
            and row.lock_expires_at is not None
            and row.lock_expires_at < now
            and (row.cancel_phase == 0 or row.lock_expires_at < now - deep_expiry_margin)
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
                new_scheduled = now + timedelta(seconds=5)
                self._jobs[job_id] = replace(
                    row,
                    status="pending",
                    scheduled_at=new_scheduled,
                    locked_by_worker=None,
                    lock_expires_at=None,
                    cancel_phase=CancelPhase.NONE,
                    cancel_requested_at=None,
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
                    "state-change",
                    kind="state_change",
                    from_state="running",
                    to_state="pending",
                    job_id=str(job_id),
                )
            else:
                # Exhausted: an in-flight cancel request makes 'cancelled'
                # the honest terminal label (mirrors _SWEEP_1_SQL's CASE).
                new_status = "cancelled" if row.cancel_phase != CancelPhase.NONE else "crashed"
                # locked_by_worker/lock_expires_at are cleared on EVERY
                # branch by _SWEEP_1_SQL's single SET clause list; the
                # twin must match or a terminal row keeps pointing at a
                # dead holder for every locked_by_worker-scoped reader.
                self._jobs[job_id] = replace(
                    row,
                    status=new_status,
                    finished_at=now,
                    locked_by_worker=None,
                    lock_expires_at=None,
                    cancel_phase=CancelPhase.NONE,
                    cancel_requested_at=None,
                )
                self._append_state_change_event(
                    job_id,
                    from_state="running",
                    to_state=new_status,
                    now=now,
                    worker_id=row.locked_by_worker,
                    reason="lock_expired",
                )
                logger.debug(
                    "state-change",
                    kind="state_change",
                    from_state="running",
                    to_state=new_status,
                    job_id=str(job_id),
                )
            for event in self._wake_subscribers:
                event.set()
            count += 1
    return count
