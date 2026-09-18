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
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the twins must enforce the identical contract the Postgres sweeps enforce: one validator, one message map, one disposition map and total lookup, one seam, no drift.
    _ATTEMPT_MESSAGES,
    _reclaim_disposition,
    _validate_positive,
)
from taskq.constants import DEFAULT_EVENT_WRITER_BATCH_SIZE
from taskq.obs import record_deadline_exceeded_swept, record_reclaimed_jobs
from taskq.retry import (  # pyright: ignore[reportPrivateUsage]  # Why: the twin must compute the identical reclaim delay the SQL fragment computes — one curve twin, one hash fraction, no drift surface.
    RetryPolicy,
    _compute_reclaim_backoff,
)
from taskq.testing._terminal import (  # pyright: ignore[reportPrivateUsage]  # Why: the sweep twin's attempt write must carry the identical keep-first guard the PG batched INSERT carries — one guarded write, no drift surface.
    _write_attempt,
)

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
    # A pending/scheduled row can legitimately sit at an attempt number that
    # already ran: the transient-retry arm, a crash reclaim, and an operator
    # retry all leave the spent attempt's row behind and keep the counter
    # where it is. Dispatch excludes rows past schedule_to_close, so that
    # counter can never climb again and this sweep is the only thing that can
    # resolve the job. The existing row is the truthful record of what the
    # actor actually did; the deadline lapsing afterwards is not a second
    # execution, so the synthetic row yields to it — the twin of PG's
    # ON CONFLICT (job_id, attempt) DO NOTHING, which also keeps one
    # already-attempted row from rolling back every sibling swept with it.
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

    written_keys = {(a.job_id, a.attempt) for rows in self._attempts.values() for a in rows}

    count = 0
    for job_id, row in selected:
        self._jobs[job_id] = replace(
            row,
            status="failed",
            finished_at=now,
            error_class="DeadlineExceeded",
            error_message="schedule_to_close reached before next dispatch",
        )
        if (job_id, row.attempt) not in written_keys:
            written_keys.add((job_id, row.attempt))
            self._attempts.setdefault(job_id, []).append(
                AttemptRow(
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
            )
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
    # * reclaimed-jobs counter: both backends aggregate (actor,
    #   disposition) pairs over the reclaimed rows and record
    #   taskq.jobs.reclaimed after the transition loop, the disposition
    #   derived from the written status through the one shared total
    #   lookup (backend._sweeps._reclaim_disposition over the one
    #   _RECLAIM_DISPOSITIONS map), so the metric's label set cannot drift
    #   between backends and an unmapped status counts as "unknown"
    #   instead of dying mid-loop.
    _validate_positive("batch_size", batch_size)
    now = self._clock.now()
    deep_expiry_margin = cancel_grace + cleanup_grace + timedelta(seconds=60)
    arm_counts: dict[str, int] = {"lock_expired": 0, "heartbeat_timeout": 0}
    # (actor, disposition) -> rows reclaimed, for the same
    # taskq.jobs.reclaimed counter the PG sweep emits from its RETURNING.
    # Aggregated on the loop and recorded after it, mirroring the PG
    # side's after-the-transaction placement, so per-row metric calls
    # never sit on the row-transition path and the two backends emit the
    # identical label set.
    reclaim_counts: dict[tuple[str, str], int] = {}
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
        # Through the shared keep-first write (the twin of the batched
        # INSERT's ON CONFLICT (job_id, attempt) DO NOTHING — see
        # _SWEEP_1_ATTEMPTS_BATCH_SQL): an attempt number can already have
        # its row when the reclaim fires — a claim-clamped repeat at the
        # smallint ceiling, a spent attempt left behind by a re-pend — and
        # the existing record is the truthful one, so the synthetic crash
        # row yields to it rather than accumulating a duplicate PG refused
        # to store. The same guard the deadline twin states inline above
        # and _terminal._write_attempt carries for the terminal paths —
        # one doctrine, one guarded write. Pinned by
        # tests/test_rt_sweeps_parity.py::test_sweep1_double_reclaim_keeps_one_attempt_row_on_both_backends.
        await _write_attempt(self, attempt_row)

        # The same budget question the SQL asks (see
        # _sweeps._RECLAIM_HAS_BUDGET_SQL): 'indefinite' has no attempt
        # ceiling — its schedule_to_close deadline is its budget — while
        # every other kind is bounded by max_attempts, and
        # 'non_retryable' has no second attempt at all.
        if row.retry_kind == "indefinite" or (
            row.attempt < row.max_attempts and row.retry_kind != "non_retryable"
        ):
            # The row's own stamped RetryPolicy curve, mirroring
            # _RECLAIM_RAW_BACKOFF_SQL / _RECLAIM_DELAY_SQL exactly through
            # the shared twin _compute_reclaim_backoff: same three-way raw
            # branch, same min(row cap, max_retry_backoff) effective
            # ceiling (the value the PG statement binds as its
            # {max_backoff_seconds} parameter), and the SAME jitter
            # fraction the SQL derives — a deterministic md5 of the row's
            # own '<id>:<attempt>', never an RNG draw. A reclaim delay is
            # computed by more than one path for the same row (the leader's
            # sweep, a partitioned worker's isolate_self, a replayed sweep,
            # this mirror), and every one must stamp the same instant —
            # replay idempotence the old per-statement random() could not
            # provide. A fleet-wide event still spreads: a whole cohort
            # handed back at once lands across the jitter band because
            # distinct ids hash to distinct fractions, rather than at one
            # synchronised instant.
            row_policy = RetryPolicy(
                backoff=row.retry_backoff,
                base=row.retry_base,
                cap=row.retry_cap,
                jitter=row.retry_jitter,
            )
            # The raw stamped attempt goes in UNCLAMPED: the twin floors
            # the exponential arm's exponent itself (mirroring the SQL's
            # GREATEST(j.attempt - 1, 0)) and hashes the raw attempt exactly
            # as j.attempt::text does — clamping here (the old
            # max(row.attempt, 1) for compute_backoff's attempt >= 1 guard)
            # would hash a different attempt than the SQL for a
            # direct-construction attempt=0 row (dispatch always stamps
            # attempt >= 1 — see _dispatch_sql.py's `attempt = j.attempt +
            # 1` — so the distinction is direct-SQL-reachable only, the
            # same class the heartbeat arm's NULL last_heartbeat_at guard
            # documents), and for the linear arm it would also miscompute
            # the raw value (SQL multiplies by j.attempt itself).
            new_scheduled = now + _compute_reclaim_backoff(
                row_policy,
                row.attempt,
                job_id=row.id,
                max_retry_backoff=self._max_retry_backoff,
            )
            self._jobs[job_id] = replace(
                row,
                status="pending",
                scheduled_at=new_scheduled,
                locked_by_worker=None,
                lock_expires_at=None,
                cancel_phase=CancelPhase.NONE,
                cancel_requested_at=None,
                # A reclaim hands the row back to the fleet, so it routes
                # by the actor's current assignment from here on.
                assignment_routed=True,
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
            crashed = row.cancel_phase == CancelPhase.NONE
            new_status = "crashed" if crashed else "cancelled"
            # locked_by_worker/lock_expires_at are cleared on EVERY
            # branch by _SWEEP_1_SQL's single SET clause list; the
            # twin must match or a terminal row keeps pointing at a
            # dead holder for every locked_by_worker-scoped reader.
            # assignment_routed is set on every arm for the same reason
            # (the SQL sets it unconditionally): on a terminal row the
            # flag is inert — the row is never dispatchable again — but
            # the stored value must match the contract source.
            self._jobs[job_id] = replace(
                row,
                status=new_status,
                finished_at=now,
                locked_by_worker=None,
                lock_expires_at=None,
                cancel_phase=CancelPhase.NONE,
                cancel_requested_at=None,
                assignment_routed=True,
                # Twin of _SWEEP_1_SQL's crashed-arm SET: a crashed row
                # self-describes (WorkerCrashed plus the deadline that
                # fired, drawn from the same _ATTEMPT_MESSAGES map the
                # attempt row uses — one map, no drift). The
                # cancel-honoured arm stamps nothing: no cancel-origin
                # marker describes a worker that died mid-protocol, so
                # the attempt row and the event's cause carry the
                # explanation there.
                error_class="WorkerCrashed" if crashed else row.error_class,
                error_message=_ATTEMPT_MESSAGES[cause] if crashed else row.error_message,
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
        # The disposition derives from the row's own post-transition
        # status, the same one-map doctrine the PG sweep follows with the
        # status its RETURNING carries, through the same TOTAL lookup: a
        # KeyError here died mid-loop and left a half-drained corpus (some
        # rows transitioned, the rest still running, nothing counted), so
        # an unmapped status counts as the explicit "unknown" disposition
        # instead (see backend._sweeps._reclaim_disposition).
        updated_status = self._jobs[job_id].status
        reclaim_key = (row.actor, _reclaim_disposition(updated_status))
        reclaim_counts[reclaim_key] = reclaim_counts.get(reclaim_key, 0) + 1
        for event in self._wake_subscribers:
            event.set()
        count += 1
    for (actor, disposition), reclaimed_n in reclaim_counts.items():
        record_reclaimed_jobs(actor=actor, disposition=disposition, count=reclaimed_n)
    return count
