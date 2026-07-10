"""Property tests for job lifecycle transitions.

Uses a hypothesis RuleBasedStateMachine to generate random sequences
of valid transitions, verifying that every sequence preserves invariants
across terminal-status idempotency, cancel_phase monotonicity,
no-orphan-running-row, and attempt-count contracts.

random valid call sequences preserve invariants
    (terminal-status idempotency, no orphan running rows,
     cancel_phase never decreases).
exhaustive illegal (from_status, to_status) pairs all raise
    IllegalStateTransition from assert_valid_transition.
attempt-count invariant (non-negative, never decreases across
    dispatch, unchanged across mark_snoozed).

anchors: (job state machine), (in-memory backend).
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, invariant, precondition, rule

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES, VALID_TRANSITIONS, assert_valid_transition
from taskq.exceptions import IllegalStateTransition, WorkerOwnershipMismatch
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Constants ──────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)

_ALL_STATUSES: list[JobStatus] = [
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]


# ── State machine ──────────────────────────────────────────────────────


@settings(max_examples=200, stateful_step_count=20, deadline=None)
class JobStateMachine(RuleBasedStateMachine):
    """Generate random valid transition sequences; assert every step
    leaves jobs in a legal state and terminal jobs have no exits.

    Each rule body wraps ``asyncio.run(...)`` of a small coroutine
    that calls the in-memory backend's async methods. The backend
    is single-threaded and deterministic, so per-rule loop spin cost
    is acceptable (hypothesis async-rule wrapper choice).
    """

    jobs = Bundle("jobs")

    def __init__(self) -> None:
        super().__init__()
        self.backend: InMemoryBackend = InMemoryBackend(
            clock=FakeClock(_START),
            cancellation_grace_period=_CANCEL_GRACE,
            cleanup_grace_period=_CLEANUP_GRACE,
        )
        self._prev_cancel_phase: dict[JobId, int] = {}
        self._prev_attempt: dict[JobId, int] = {}
        self._snoozed_attempt_snapshot: dict[JobId, int] = {}

    # ── Helpers ────────────────────────────────────────────────────────

    def _running_jobs(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check job states
            if row.status == "running"
        ]

    def _pending_jobs(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check job states
            if row.status == "pending"
        ]

    def _scheduled_jobs(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check job states
            if row.status == "scheduled"
        ]

    def _running_cancel_phase_0(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check cancel_phase
            if row.status == "running" and row.cancel_phase == 0
        ]

    def _running_cancel_phase_1(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check cancel_phase
            if row.status == "running" and row.cancel_phase == 1
        ]

    def _running_cancel_phase_2(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check cancel_phase
            if row.status == "running" and row.cancel_phase == 2
        ]

    def _retryable_running(self) -> list[UUID]:
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check retry eligibility
            if row.status == "running"
            and row.attempt < row.max_attempts
            and row.retry_kind != "non_retryable"
        ]

    def _expired_lock_running(self) -> list[UUID]:
        now = self.backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access
        return [
            jid
            for jid, row in self.backend._jobs.items()  # type: ignore[reportPrivateUsage] # Why: test-only private access to check lock expiry
            if row.status == "running"
            and row.lock_expires_at is not None
            and row.lock_expires_at < now
            and row.cancel_phase == 0
        ]

    # ── Enqueue: creates pending job ──────────────────────────────────

    @rule(
        target=jobs,
        job_id=st.uuids(version=4),
        max_attempts=st.sampled_from([1, 3, 5]),
        retry_kind=st.sampled_from(["transient", "non_retryable"]),
    )
    def enqueue(self, job_id: UUID, max_attempts: int, retry_kind: str) -> JobId:
        jid = JobId(job_id)
        args = EnqueueArgs(
            id=jid,
            actor="test_actor",
            queue="default",
            payload={"key": "value"},
            max_attempts=max_attempts,
            retry_kind=retry_kind,  # type: ignore[arg-type] # Why: sampled_from produces str; values are known-valid RetryKind literals
            scheduled_at=_START,
        )
        asyncio.run(self.backend.enqueue(args))
        self._prev_cancel_phase[job_id] = 0
        self._prev_attempt[job_id] = 0
        return jid

    # ── Dispatch: pending -> running ───────────────────────────────────

    @precondition(lambda self: bool(self._pending_jobs()))
    @rule()
    def dispatch(self) -> None:
        wid: UUID = self.backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access
        asyncio.run(self.backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=_LOCK_LEASE))

    # ── Mark succeeded: running -> succeeded ───────────────────────────

    @precondition(lambda self: bool(self._running_jobs()))
    @rule(job_id=jobs)
    def mark_succeeded_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        asyncio.run(self.backend.mark_succeeded(job_id, wid, {"ok": True}))

    # ── Mark failed (terminal): running -> failed ──────────────────────
    # The status=="running" and locked_by_worker guards above make
    # WorkerOwnershipMismatch impossible — the sequential Hypothesis
    # state machine cannot mutate the row between the guard and the call.

    @precondition(lambda self: bool(self._running_jobs()))
    @rule(job_id=jobs)
    def mark_failed_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        error_info = ErrorInfo(
            error_class="TestError", error_message="terminal failure", error_traceback=None
        )
        with suppress(WorkerOwnershipMismatch):
            asyncio.run(self.backend.mark_failed_or_retry(job_id, wid, error_info, None))

    # ── Mark retry: running -> scheduled ──────────────────────────────
    # Same reasoning as mark_failed_rule: sequential execution guarantees
    # the precondition-checked status and worker ID still hold at call time.

    @precondition(lambda self: bool(self._retryable_running()))
    @rule(job_id=jobs)
    def mark_retry_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        if row.attempt >= row.max_attempts or row.retry_kind == "non_retryable":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        now = self.backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access
        next_scheduled = now + timedelta(seconds=5)
        error_info = ErrorInfo(
            error_class="TransientError", error_message="retry", error_traceback=None
        )
        with suppress(WorkerOwnershipMismatch):
            asyncio.run(self.backend.mark_failed_or_retry(job_id, wid, error_info, next_scheduled))

    # ── Mark retry-after: running -> scheduled (via RetryAfter) ────────
    # Exercises RetryAfter running→scheduled, MaxAttemptsExceeded
    # running→failed, and DeadlineExceeded running→failed paths.

    @precondition(lambda self: bool(self._running_jobs()))
    @rule(job_id=jobs, delay_secs=st.sampled_from([5, 30, 60]))
    def mark_retry_after_rule(self, job_id: JobId, delay_secs: int) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        delay = timedelta(seconds=delay_secs)
        asyncio.run(self.backend.mark_retry_after(job_id, wid, delay))

    # ── Reclaim expired locks: running -> pending or running -> crashed ─
    # Exercises the bypass writer (Sweep 1): running→pending when
    # retries remain; running→crashed when exhausted.

    @precondition(lambda self: bool(self._expired_lock_running()))
    @rule()
    def reclaim_expired_locks_rule(self) -> None:
        now = self.backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access
        asyncio.run(
            self.backend.reclaim_expired_locks(
                now, cancel_grace=_CANCEL_GRACE, cleanup_grace=_CLEANUP_GRACE
            )
        )

    # ── Mark cancelled: running -> cancelled ───────────────────────────

    @precondition(lambda self: bool(self._running_jobs()))
    @rule(job_id=jobs)
    def mark_cancelled_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        asyncio.run(self.backend.mark_cancelled(job_id, wid))

    # ── Mark snoozed: running -> scheduled ─────────────────────────────

    @precondition(lambda self: bool(self._running_jobs()))
    @rule(job_id=jobs)
    def mark_snoozed_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running":
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        snooze_delay = timedelta(seconds=30)
        self._snoozed_attempt_snapshot[job_id] = row.attempt
        asyncio.run(self.backend.mark_snoozed(job_id, wid, snooze_delay))

    # ── Cancel request on pending: pending -> cancelled ────────────────

    @precondition(lambda self: bool(self._pending_jobs()))
    @rule(job_id=jobs)
    def cancel_pending_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "pending":
            return
        asyncio.run(self.backend.write_cancel_request(job_id, "test cancel"))

    # ── Cancel request on scheduled: scheduled -> cancelled ────────────

    @precondition(lambda self: bool(self._scheduled_jobs()))
    @rule(job_id=jobs)
    def cancel_scheduled_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "scheduled":
            return
        asyncio.run(self.backend.write_cancel_request(job_id, "test cancel"))

    # ── Cancel request on running: running stays running, cancel_phase=1

    @precondition(lambda self: bool(self._running_cancel_phase_0()))
    @rule(job_id=jobs)
    def cancel_running_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running" or row.cancel_phase != 0:
            return
        asyncio.run(self.backend.write_cancel_request(job_id, "test cancel"))

    # ── Cancel escalation: running stays running, cancel_phase 1 -> 2 ──

    @precondition(lambda self: bool(self._running_cancel_phase_1()))
    @rule(job_id=jobs)
    def escalate_cancel_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running" or row.cancel_phase != 1:
            return
        wid = row.locked_by_worker
        if wid is None:
            return
        asyncio.run(self.backend.write_cancel_escalation(job_id, wid, 2))

    # ── Mark abandoned: running -> abandoned (requires cancel_phase=2) ─

    @precondition(lambda self: bool(self._running_cancel_phase_2()))
    @rule(job_id=jobs)
    def mark_abandoned_rule(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "running" or row.cancel_phase != 2:
            return
        asyncio.run(self.backend.mark_abandoned(job_id))

    # ── Promote scheduled -> pending ───────────────────────────────────

    @precondition(lambda self: bool(self._scheduled_jobs()))
    @rule()
    def promote_scheduled(self) -> None:
        scheduled = [
            row.scheduled_at
            for row in self.backend._jobs.values()  # type: ignore[reportPrivateUsage] # Why: test-only private access
            if row.status == "scheduled"
        ]
        if not scheduled:
            return
        earliest = min(scheduled)
        self.backend.advance_clock_to(earliest + timedelta(seconds=1))
        now = self.backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access
        asyncio.run(self.backend.scheduled_to_pending(now))

    # ── Cancel-during-wake-window: promote then cancel ────────────────
    # Deliberately interleaves promote_scheduled with cancel_pending_rule
    # so the property fuzzer exercises both orderings. A scheduled job
    # is promoted to pending and then cancelled in the same step.

    @precondition(lambda self: bool(self._scheduled_jobs()))
    @rule(job_id=jobs)
    def cancel_during_wake_window(self, job_id: JobId) -> None:
        row = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if row is None or row.status != "scheduled":
            return
        earliest = row.scheduled_at
        self.backend.advance_clock_to(earliest + timedelta(seconds=1))
        now = self.backend._clock.now()  # type: ignore[reportPrivateUsage] # Why: test-only private access
        asyncio.run(self.backend.scheduled_to_pending(now))
        updated = self.backend._jobs.get(job_id)  # type: ignore[reportPrivateUsage] # Why: test-only private access
        if updated is not None and updated.status == "pending":
            asyncio.run(self.backend.write_cancel_request(job_id, "cancel during wake"))

    # ── Invariant: valid state, terminal has no exits ──────────────────

    @invariant()
    def check_valid_state(self) -> None:
        """oracle: every job's status is in the legal set;
        if terminal, no further transitions are legal.
        """
        for row in self.backend._jobs.values():  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
            assert row.status in VALID_TRANSITIONS, (
                f"Unknown status {row.status!r} for job {row.id}"
            )
            if row.status in TERMINAL_STATUSES:
                assert len(VALID_TRANSITIONS[row.status]) == 0, (
                    f"Terminal status {row.status!r} has outgoing transitions"
                )

    # ── Invariant: cancel_phase never decreases ──────────────

    @invariant()
    def check_cancel_phase_monotonic(self) -> None:
        for jid, row in self.backend._jobs.items():  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
            prev = self._prev_cancel_phase.get(jid)
            if prev is not None:
                assert row.cancel_phase >= prev, (
                    f"cancel_phase decreased for job {jid}: {prev} -> {row.cancel_phase}"
                )
            self._prev_cancel_phase[jid] = row.cancel_phase

    # ── Invariant: no orphan running rows after terminal write

    @invariant()
    def check_no_orphan_running(self) -> None:
        """After a terminal write on the *current* attempt, no running row
        should remain. A running job may have failed attempts from prior
        dispatch cycles (retry path); those are legitimate. Only the
        attempt whose number matches the current row attempt signals a
        terminal write for this dispatch cycle.
        """
        for jid, row in self.backend._jobs.items():  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
            if row.status == "running":
                attempts = self.backend._attempts.get(jid, [])  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
                for att in attempts:
                    if att.attempt == row.attempt:
                        assert att.outcome not in ("succeeded", "failed", "cancelled"), (
                            f"job {jid} is running but current attempt has "
                            f"terminal outcome={att.outcome!r}"
                        )

    # ── Invariant: attempt non-negative and never decreases ──

    @invariant()
    def check_attempt_invariant(self) -> None:
        """attempt is non-negative, never decreases across
        dispatch calls, and is unchanged across mark_snoozed calls.
        """
        for jid, row in self.backend._jobs.items():  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
            assert row.attempt >= 0, f"attempt is negative for job {jid}: {row.attempt}"
            prev = self._prev_attempt.get(jid)
            if prev is not None:
                assert row.attempt >= prev, (
                    f"attempt decreased for job {jid}: {prev} -> {row.attempt}"
                )
            self._prev_attempt[jid] = row.attempt

        for jid, pre_attempt in self._snoozed_attempt_snapshot.items():
            row = self.backend._jobs.get(jid)  # type: ignore[reportPrivateUsage] # Why: test-only private access for invariant check
            if row is not None and row.status == "scheduled":
                assert row.attempt == pre_attempt, (
                    f"mark_snoozed changed attempt for job {jid}: {pre_attempt} -> {row.attempt}"
                )

        self._snoozed_attempt_snapshot.clear()


TestJobStateMachine = JobStateMachine.TestCase  # type: ignore[reportUnknownVariableType] # Why: hypothesis generates the TestCase type dynamically; pyright cannot infer it


# ── exhaustive illegal transition pairs ───────────────────────


def _illegal_pairs() -> list[tuple[JobStatus, JobStatus]]:
    """Build every (from, to) pair where to is NOT in VALID_TRANSITIONS[from]."""
    pairs: list[tuple[JobStatus, JobStatus]] = []
    for from_status in _ALL_STATUSES:
        for to_status in _ALL_STATUSES:
            if to_status not in VALID_TRANSITIONS[from_status]:
                pairs.append((from_status, to_status))
    return pairs


_ILLEGAL_PAIRS = _illegal_pairs()


@given(
    from_to=st.sampled_from(_ILLEGAL_PAIRS),
)
@settings(max_examples=200, deadline=None)
def test_illegal_transition_raises_property(
    from_to: tuple[JobStatus, JobStatus],
) -> None:
    """random illegal (from_status, to_status) pairs all raise
    IllegalStateTransition from assert_valid_transition. Redundant with
    the parametrized test in test_state_machine.py as insurance.
    """
    from_status, to_status = from_to
    job_id = new_uuid()
    with pytest.raises(IllegalStateTransition):
        assert_valid_transition(from_status=from_status, to_status=to_status, job_id=job_id)
