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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from taskq.backend._protocol import AttemptRow, CancelPhase, JobId, JobRow
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the twins must enforce the identical contract the Postgres sweeps enforce — one validator, one message map, one seam, no drift.
    _ATTEMPT_MESSAGES,
    _validate_positive,
)
from taskq.constants import DEFAULT_EVENT_WRITER_BATCH_SIZE
from taskq.obs import record_deadline_exceeded_swept
from taskq.retry import RetryPolicy, compute_backoff

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
            # Mirrors PG's sweep_scheduled_to_pending: no job_events row —
            # scheduled→pending is scheduler bookkeeping, and it is one of
            # the two acts every admission-denial cycle repeats (claim +
            # promote), so a row per promotion is the same
            # unbounded-growth vector the denial counters replaced.
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
    # Select the whole bounded batch FIRST: on PG the transition UPDATE,
    # the batched job_attempts INSERT, and the event INSERT share one
    # transaction (sweep_deadline_exceeded), so an attempt-row primary-key
    # collision on ANY selected row aborts the ENTIRE batch — every
    # selected job stays pending/scheduled, nothing is written. The twin
    # selects, validates every planned (job_id, attempt) key against the
    # stored attempt rows and in-batch duplicates, and only then mutates:
    # a collision raises the same typed UniqueViolationError PG's batched
    # INSERT raises (job_attempts_pkey), never a silent duplicate append
    # that completes the transition PG would leave torn down.
    selected: list[tuple[JobId, JobRow]] = []
    for job_id, row in list(self._jobs.items()):
        if len(selected) >= batch_size:
            break
        if (
            row.status in ("pending", "scheduled")
            and row.schedule_to_close is not None
            and row.schedule_to_close < now
        ):
            selected.append((job_id, row))

    if selected:
        # Why a function-level import: the driver-free import-surface
        # convention (taskq.testing imports no asyncpg at module scope);
        # this raise path only ever runs where the driver is installed.
        from asyncpg.exceptions import UniqueViolationError

        _existing = {(a.job_id, a.attempt) for rows in self._attempts.values() for a in rows}
        _seen: set[tuple[JobId, int]] = set()
        for job_id, row in selected:
            _key = (job_id, row.attempt)
            if _key in _existing or _key in _seen:
                raise UniqueViolationError(
                    'duplicate key value violates unique constraint "job_attempts_pkey" '
                    f"(job {job_id} attempt {row.attempt} already has an attempt row)"
                )
            _seen.add(_key)

    count = 0
    for job_id, row in selected:
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
    # * eligibility arms — the lease arm (lock_expires_at passed) and the
    #   heartbeat arm (a per-job heartbeat_timeout whose holder has been
    #   silent past it while the lease is still valid), disjoint by the
    #   same lock_expires_at >= now exclusion the SQL's UNION ALL uses,
    #   with NULL last_heartbeat_at never eligible (NULL + interval is
    #   NULL in PG; the twin's None-guard mirrors it) and a beat not in
    #   the past never eligible either (the SQL's
    #   last_heartbeat_at < statement_timestamp() index range bound —
    #   the twin states the conjunct itself, not just the deadline
    #   arithmetic it implies for positive timeouts);
    # * bounded batches — each arm transitions at most batch_size rows
    #   per call (the SQL's per-arm LIMIT), so one call reclaims at most
    #   2 x batch_size rows, exactly like the UNION ALL;
    # * carve-out — a job with an in-flight cancel request
    #   (cancel_phase != 0) is normally left for the cancellation
    #   protocol to finish, but is still reclaimed once its arm's
    #   deadline has been past for cancel_grace + cleanup_grace + a flat
    #   60s safety margin (see _sweeps.py's _SWEEP_1_SQL comment) —
    #   otherwise a worker that died mid-cancellation would never be
    #   recovered;
    # * terminal labels — the retry branch resets cancel state (clean
    #   slate for the next dispatch); the exhausted branch lands on
    #   'cancelled' when a cancel was in-flight, 'crashed' otherwise,
    #   while the attempt row records outcome='crashed' either way, its
    #   error_message naming the deadline that fired (the same
    #   _ATTEMPT_MESSAGES map the PG sweep feeds its batched INSERT —
    #   no drift surface);
    # * outbox channel — both arms' events carry reason='lock_expired'
    #   (the slice poll_reclaim_events tails) with a cause key naming
    #   which deadline fired.
    _validate_positive("batch_size", batch_size)
    now = self._clock.now()
    deep_expiry_margin = cancel_grace + cleanup_grace + timedelta(seconds=60)
    arm_counts: dict[str, int] = {"lock_expired": 0, "heartbeat_timeout": 0}
    count = 0
    for job_id, row in list(self._jobs.items()):
        if all(c >= batch_size for c in arm_counts.values()):
            break
        # The heartbeat arm's row-exact deadline, or None when the row
        # carries no heartbeat_timeout / no heartbeat yet (NULL +
        # interval is NULL in PG; the ternary's guard mirrors that
        # None-propagation for the arithmetic below).
        # A non-positive stored timeout is inert, mirroring the SQL's
        # `heartbeat_timeout > interval '0'`: enqueue validation refuses
        # one but it is the only gate and the column carries no CHECK, so
        # a direct write can leave a zero or negative interval behind,
        # and with one the deadline is already past the instant the row
        # is written — a healthy holder beating right now would be
        # reclaimed as a false crash. The lease governs instead.
        heartbeat_deadline: datetime | None = (
            row.last_heartbeat_at + row.heartbeat_timeout
            if row.last_heartbeat_at is not None
            and row.heartbeat_timeout is not None
            and row.heartbeat_timeout > timedelta(0)
            else None
        )
        # One if/elif, conjuncts ordered so each None-guard precedes the
        # arithmetic it guards — the same inline-narrowing shape the
        # original single-conjunction predicate used.
        cause: str | None = None
        if (
            row.status == "running"
            and row.lock_expires_at is not None
            and row.lock_expires_at < now
            and (row.cancel_phase == 0 or row.lock_expires_at < now - deep_expiry_margin)
        ):
            cause = "lock_expired"
        elif (
            row.status == "running"
            and heartbeat_deadline is not None
            and row.lock_expires_at is not None
            and row.lock_expires_at >= now
            # The SQL's index range bound, stated as its own conjunct: a
            # beat not in the past is never holder silence, however the
            # row-exact deadline arithmetic reads (a FUTURE-stamped beat
            # plus a degenerate negative timeout — direct-SQL-reachable —
            # makes the deadline alone admit a row the SQL provably never
            # reclaims). The None-guard is implied by heartbeat_deadline
            # but kept so the conjunct stays SQL-verbatim and narrows the
            # comparison's type.
            and row.last_heartbeat_at is not None
            and row.last_heartbeat_at < now
            and heartbeat_deadline < now
            and (row.cancel_phase == 0 or heartbeat_deadline < now - deep_expiry_margin)
        ):
            cause = "heartbeat_timeout"
        if cause is None or arm_counts[cause] >= batch_size:
            continue
        arm_counts[cause] += 1
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
            # Names the deadline that fired (the PG batched INSERT's $6
            # array comes from the same map), never the sibling arm's.
            error_message=_ATTEMPT_MESSAGES[cause],
            error_traceback=None,
            duration_ms=duration_ms,
            worker_id=row.locked_by_worker,
            metadata={},
        )
        self._attempts.setdefault(job_id, []).append(attempt_row)

        # The same budget question the SQL asks (see
        # _sweeps._RECLAIM_HAS_BUDGET_SQL): 'indefinite' has no attempt
        # ceiling — its schedule_to_close deadline is its budget — while
        # every other kind is bounded by max_attempts, and
        # 'non_retryable' has no second attempt at all.
        if row.retry_kind == "indefinite" or (
            row.attempt < row.max_attempts and row.retry_kind != "non_retryable"
        ):
            # The row's own stamped RetryPolicy curve, mirroring
            # _RECLAIM_RAW_BACKOFF_SQL / _RECLAIM_DELAY_SQL exactly:
            # compute_backoff is the single implementation every other
            # retry path (failure, Retry-After, snooze) shares, so this
            # is the SAME curve a live actor's failure backoff would
            # produce for this attempt — jittered per row, mirroring the
            # SQL's per-row random(): a fleet-wide event hands a whole
            # cohort back at once, and one instant for all of them would
            # land on the recovering fleet as a synchronised wave. The
            # ceiling is the operator's max_retry_backoff, the value the
            # PG statement binds as its {max_backoff_seconds} parameter —
            # min(row cap, ceiling) on both sides.
            row_policy = RetryPolicy(
                backoff=row.retry_backoff,
                base=row.retry_base,
                cap=row.retry_cap,
                jitter=row.retry_jitter,
            )
            # compute_backoff requires attempt >= 1 (its own domain is
            # 1-indexed); a running row with attempt=0 is reachable only by
            # direct construction (dispatch always stamps attempt >= 1 —
            # see _dispatch_sql.py's `attempt = j.attempt + 1`), the same
            # "direct-SQL-reachable, not production-reachable" class the
            # heartbeat arm's NULL last_heartbeat_at guard documents. The
            # SQL side floors the exponent at GREATEST(attempt - 1, 0)
            # rather than raising, so attempt=0 mirrors attempt=1's curve
            # here too instead of crashing the sweep.
            new_scheduled = now + compute_backoff(
                row_policy, max(row.attempt, 1), max_retry_backoff=self._max_retry_backoff
            )
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
                cause=cause,
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
                cause=cause,
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
