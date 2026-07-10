"""Tests for InMemoryBackend data layer.

Covers ``src/taskq/testing/in_memory.py``:
- BACKEND_PROTOCOL_VERSION, bool-returning terminal writes
- heartbeat_jobs with wrong worker_id returns 0
- isinstance(InMemoryBackend, Backend) — xfail until - Terminal-write happy-path and no-op (False return) tests
- write_cancel_request three cases (a)/(b)/(c)
- Sweep methods: scheduled_to_pending, deadline_sweep, reclaim_expired_locks
- isolation: two InMemoryBackend instances do not share state
- enqueue initial-status bifurcation
- list_jobs filtering
- progress_state and progress_seq round-trip
- list_jobs cursor (keyset pagination)
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id, new_uuid
from taskq.backend import (
    AttemptRow,
    Backend,
    EnqueueArgs,
    ErrorInfo,
    JobFilter,
    JobRow,
)
from taskq.backend._protocol import JobId, RetryKind
from taskq.backend.clock import Clock
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.exceptions import (
    BackpressureError,
    MaxPendingExceededError,
    WorkerOwnershipMismatch,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import (
    BACKEND_PROTOCOL_VERSION,
    InMemoryBackend,
    decode_cursor,
    encode_cursor,
)
from taskq.testing.jobs import make_enqueue_args

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


def _make_backend(
    clock: Clock | None = None,
    cancellation_grace: timedelta = _GRACE,
    cleanup_grace: timedelta = _GRACE,
) -> InMemoryBackend:
    """Construct an InMemoryBackend with a FakeClock at the standard start time."""
    clk = clock or FakeClock(_START)
    return InMemoryBackend(
        clock=clk,
        cancellation_grace_period=cancellation_grace,
        cleanup_grace_period=cleanup_grace,
    )


def _enqueue_args(
    actor: str = "test_actor",
    queue: str = "default",
    scheduled_at: datetime | None = None,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    priority: int = 0,
    schedule_to_close: datetime | None = None,
) -> EnqueueArgs:
    """Build minimal EnqueueArgs for testing."""
    return make_enqueue_args(
        actor=actor,
        queue=queue,
        payload={"key": "value"},
        scheduled_at=scheduled_at or _START,
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        priority=priority,
        schedule_to_close=schedule_to_close,
    )


async def _make_running_row(
    backend: InMemoryBackend,
    worker_id: UUID | None = None,
    actor: str = "test_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
) -> tuple[JobId, JobRow]:
    """Enqueue a job and manually set it to running, returning (job_id, row).

    This bypasses dispatch_batch so we can test terminal writes
    in isolation.
    """
    from dataclasses import replace as _replace

    args = _enqueue_args(actor=actor, queue=queue, max_attempts=max_attempts, retry_kind=retry_kind)
    row = await backend.enqueue(args)
    job_id = row.id

    wid = worker_id or backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access to set up running row fixture
    now = backend._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test-only private access to set up running row fixture
    running_row = _replace(
        row,
        status="running",
        locked_by_worker=wid,
        lock_expires_at=now + timedelta(seconds=30),
        started_at=now,
        attempt=1,
    )
    backend._jobs[job_id] = running_row  # type: ignore[reportPrivateUsage]  # Why: test-only private access to set up running row fixture
    return job_id, running_row


# ── BACKEND_PROTOCOL_VERSION ──────────────────────────────────────────


class TestProtocolVersion:
    def test_version_is_two(self) -> None:
        assert BACKEND_PROTOCOL_VERSION == 2

    def test_version_matches_backend(self) -> None:
        from taskq.backend import BACKEND_PROTOCOL_VERSION as backend_ver  # noqa: N811, I001  # Why: alias avoids shadowing the un-aliased import above; inline import avoids name collision

        assert backend_ver == BACKEND_PROTOCOL_VERSION

    def test_version_is_int(self) -> None:
        assert isinstance(BACKEND_PROTOCOL_VERSION, int)


# ── Construction ───────────────────────────────────────────────────────


class TestConstruction:
    def test_constructs_with_fake_clock(self) -> None:
        backend = _make_backend()
        assert isinstance(backend, InMemoryBackend)

    def test_stores_clock(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock=clock)
        assert backend._clock is clock  # type: ignore[reportPrivateUsage]  # Why: test-only private access

    def test_stores_grace_periods(self) -> None:
        cancel_grace = timedelta(seconds=10)
        cleanup_grace = timedelta(seconds=20)
        backend = _make_backend(
            cancellation_grace=cancel_grace,
            cleanup_grace=cleanup_grace,
        )
        assert backend._cancellation_grace == cancel_grace  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        assert backend._cleanup_grace == cleanup_grace  # type: ignore[reportPrivateUsage]  # Why: test-only private access

    def test_generates_worker_id(self) -> None:
        backend = _make_backend()
        assert backend._worker_id is not None  # type: ignore[reportPrivateUsage]  # Why: test-only private access

    def test_two_backends_different_worker_ids(self) -> None:
        a = _make_backend()
        b = _make_backend()
        assert a._worker_id != b._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access


# ── isolation ──────────────────────────────────────────────────


class TestInstanceIsolation:
    async def test_two_backends_isolated(self) -> None:
        """Two InMemoryBackend instances do not share state."""
        a = _make_backend()
        b = _make_backend()

        args_a = _enqueue_args(actor="a_actor")
        row_a = await a.enqueue(args_a)

        # Backend a has the job, backend b does not
        assert await a.get(row_a.id) is not None
        assert await b.get(row_a.id) is None

    async def test_isolated_attempts(self) -> None:
        """Attempts in one backend are not visible in another."""
        a = _make_backend()
        b = _make_backend()

        args = _enqueue_args()
        row = await a.enqueue(args)

        attempt = AttemptRow(
            job_id=row.id,
            attempt=1,
            started_at=_START,
            finished_at=_START + timedelta(seconds=1),
            outcome="succeeded",
            error_class=None,
            error_message=None,
            error_traceback=None,
            duration_ms=1000,
            worker_id=a._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            metadata={},
        )
        await a.write_attempt(attempt)

        assert len(await a.get_attempts(row.id)) == 1
        assert len(await b.get_attempts(row.id)) == 0


# ── Enqueue ───────────────────────────────────────────────────────────


class TestEnqueue:
    async def test_returns_job_row(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert isinstance(row, JobRow)
        assert row.id == args.id
        assert row.actor == args.actor

    async def test_stores_in_jobs_dict(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert await backend.get(args.id) == row

    async def test_pending_when_scheduled_at_now(self) -> None:
        """immediate job has status=pending."""
        backend = _make_backend()
        args = _enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        assert row.status == "pending"

    async def test_scheduled_when_scheduled_at_future(self) -> None:
        """future job has status=scheduled."""
        backend = _make_backend()
        future = _START + timedelta(hours=1)
        args = _enqueue_args(scheduled_at=future)
        row = await backend.enqueue(args)
        assert row.status == "scheduled"

    async def test_defaults(self) -> None:
        """Enqueue sets correct default values."""
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert row.attempt == 0
        assert row.cancel_phase == 0
        assert row.progress_state == {}
        assert row.progress_seq == 0
        assert row.started_at is None
        assert row.finished_at is None
        assert row.locked_by_worker is None
        assert row.lock_expires_at is None


# ── progress_state and progress_seq round-trip ──────────────


class TestProgressFieldsRoundTrip:
    async def test_progress_defaults_after_enqueue(self) -> None:
        """progress_state and progress_seq survive enqueue→get."""
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        fetched = await backend.get(row.id)
        assert fetched is not None
        assert fetched.progress_state == {}
        assert fetched.progress_seq == 0


# ── Heartbeat ─────────────────────────────────────────────────────────


class TestHeartbeat:
    async def test_heartbeat_extends_lock(self) -> None:
        backend = _make_backend()
        job_id, _row = await _make_running_row(backend)

        lease = timedelta(seconds=60)
        count = await backend.heartbeat_jobs(
            worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            lock_lease=lease,
        )
        assert count == 1
        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.last_heartbeat_at is not None

    async def test_heartbeat_wrong_worker_returns_zero(self) -> None:
        """heartbeat with wrong worker_id returns 0, no raise."""
        backend = _make_backend()
        await _make_running_row(backend)

        wrong_worker = new_uuid()
        count = await backend.heartbeat_jobs(
            worker_id=wrong_worker,
            lock_lease=timedelta(seconds=60),
        )
        assert count == 0


# ── extend_reservation_leases (placeholder stub) ──────────────────────


class TestExtendReservationLeases:
    async def test_returns_zero(self) -> None:
        backend = _make_backend()
        count = await backend.extend_reservation_leases(
            worker_id=new_uuid(),
            lock_lease=timedelta(seconds=30),
        )
        assert count == 0


class TestExtendLeasesForJobCount:
    """Regression: extend_leases_for_job must count matching slots, not buckets."""

    async def test_counts_slots_not_buckets(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        slots = backend.slot_table
        # Two buckets so the old bucket-counting bug would over-count.
        slots.ensure_slots("bucket_a", 2)
        slots.ensure_slots("bucket_b", 2)
        acquired = slots.acquire(
            "bucket_a",
            job_id,
            worker_id,
            timedelta(seconds=30),
            _START,
        )
        assert acquired >= 0

        count = await backend.extend_reservation_leases(worker_id, timedelta(seconds=60))
        assert count == 1

    def test_multiple_slots_for_same_job(self) -> None:
        from taskq.testing.in_memory import _SlotTable

        table = _SlotTable()
        table.ensure_slots("bucket_a", 4)
        job = new_uuid()
        worker = new_uuid()
        lease = timedelta(seconds=30)

        # Acquire two slots in the same bucket for the same job.
        first = table.acquire("bucket_a", job, worker, lease, _START)
        second = table.acquire("bucket_a", job, worker, lease, _START)
        assert first >= 0
        assert second >= 0
        assert first != second

        # A third bucket with no matching slot must not inflate the count.
        table.ensure_slots("bucket_b", 2)

        count = table.extend_leases_for_job(
            job, _START + timedelta(seconds=1), timedelta(seconds=60)
        )
        assert count == 2


# ── Terminal writes ────────────────────────────────────────────────────


class TestMarkSucceeded:
    async def test_happy_path_returns_true(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)

        result = await backend.mark_succeeded(
            job_id,
            backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            result={"ok": True},
        )
        assert result is True

        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "succeeded"
        assert row.result == {"ok": True}
        assert row.finished_at is not None

    async def test_noop_returns_false(self) -> None:
        """second call returns False (idempotent retry)."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert await backend.mark_succeeded(job_id, wid, None) is True
        assert await backend.mark_succeeded(job_id, wid, None) is False

    async def test_wrong_worker_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wrong_worker = new_uuid()
        assert await backend.mark_succeeded(job_id, wrong_worker, None) is False

    async def test_non_running_returns_false(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        # Job is pending, not running
        assert await backend.mark_succeeded(row.id, new_uuid(), None) is False


class TestMarkFailedOrRetry:
    async def test_retry_path_returns_scheduled_row(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=3, retry_kind="transient")

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="test error",
            error_traceback=None,
        )
        next_scheduled = _START + timedelta(seconds=10)
        result = await backend.mark_failed_or_retry(
            job_id,
            backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            error_info,
            next_scheduled,
        )
        assert result.status == "scheduled"
        assert result.attempt == 1
        assert result.scheduled_at == next_scheduled
        assert result.locked_by_worker is None

    async def test_terminal_failure_path(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=1, retry_kind="transient")

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="test error",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            error_info,
            next_scheduled_at=None,
        )
        assert result.status == "failed"
        assert result.finished_at is not None
        assert result.error_class == "ValueError"

    async def test_non_retryable_goes_to_failed(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=3, retry_kind="non_retryable")

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="non-retryable",
            error_traceback=None,
        )
        result = await backend.mark_failed_or_retry(
            job_id,
            backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            error_info,
            next_scheduled_at=None,
        )
        assert result.status == "failed"

    async def test_ownership_mismatch_raises(self) -> None:
        """wrong worker_id raises WorkerOwnershipMismatch."""
        backend = _make_backend()
        job_id, _original = await _make_running_row(backend)
        wrong_worker = new_uuid()

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="test",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(
                job_id,
                wrong_worker,
                error_info,
                None,
            )

    async def test_already_terminal_raises_ownership_mismatch(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        await backend.mark_succeeded(job_id, wid, None)

        error_info = ErrorInfo(
            error_class="ValueError",
            error_message="test",
            error_traceback=None,
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(job_id, wid, error_info, None)


class TestMarkCancelled:
    async def test_happy_path_returns_true(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert await backend.mark_cancelled(job_id, wid) is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "cancelled"

    async def test_noop_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert await backend.mark_cancelled(job_id, wid) is True
        assert await backend.mark_cancelled(job_id, wid) is False

    async def test_wrong_worker_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        assert await backend.mark_cancelled(job_id, new_uuid()) is False


class TestWriteCancelEscalation:
    async def test_phase2_returns_true(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        # First set cancel_phase=1 via write_cancel_request
        await backend.write_cancel_request(job_id, None)

        result = await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        assert result is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_phase == 2

    async def test_noop_already_phase2(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        await backend.write_cancel_request(job_id, None)
        await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        assert await backend.write_cancel_escalation(job_id, wid, 2) is False  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright

    async def test_phase1_raises_valueerror(self) -> None:
        """write_cancel_escalation only accepts phase=2; phase=1 raises ValueError."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        with pytest.raises(ValueError, match="phase=2"):
            await backend.write_cancel_escalation(job_id, wid, 1)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright

    async def test_wrong_worker_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        await backend.write_cancel_request(job_id, None)
        wrong_worker = new_uuid()
        assert await backend.write_cancel_escalation(job_id, wrong_worker, 2) is False  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright


class TestMarkAbandoned:
    async def test_happy_path_returns_true(self) -> None:
        """mark_abandoned requires cancel_phase=2."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        # Set cancel_phase=2 to satisfy the new guard
        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, cancel_phase=2)  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert await backend.mark_abandoned(job_id) is True
        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.status == "abandoned"

    async def test_noop_returns_false(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, cancel_phase=2)  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert await backend.mark_abandoned(job_id) is True
        assert await backend.mark_abandoned(job_id) is False

    async def test_non_running_returns_false(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert await backend.mark_abandoned(row.id) is False

    async def test_cancel_phase_not_2_returns_false(self) -> None:
        """cancel_phase must be 2 for mark_abandoned to succeed."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        # cancel_phase is 0 by default — mark_abandoned returns False
        assert await backend.mark_abandoned(job_id) is False


class TestMarkSnoozed:
    async def test_happy_path_returns_scheduled(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        delay = timedelta(seconds=30)
        assert await backend.mark_snoozed(job_id, wid, delay) == "scheduled"
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "scheduled"
        assert row.scheduled_at == _START + timedelta(seconds=30)
        assert row.locked_by_worker is None

    async def test_noop_returns_noop(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        delay = timedelta(seconds=30)
        assert await backend.mark_snoozed(job_id, wid, delay) == "scheduled"
        assert await backend.mark_snoozed(job_id, wid, delay) == "noop"

    async def test_metadata_update(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        delay = timedelta(seconds=30)
        assert (
            await backend.mark_snoozed(
                job_id, wid, delay, metadata_update={"snooze_reason": "busy"}
            )
            == "scheduled"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.metadata.get("snooze_reason") == "busy"

    async def test_wrong_worker_returns_noop(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        delay = timedelta(seconds=30)
        assert await backend.mark_snoozed(job_id, new_uuid(), delay) == "noop"


# ── Attempt history ────────────────────────────────────────────────────


class TestAttemptHistory:
    async def test_write_and_get(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)

        attempt = AttemptRow(
            job_id=row.id,
            attempt=1,
            started_at=_START,
            finished_at=_START + timedelta(seconds=1),
            outcome="succeeded",
            error_class=None,
            error_message=None,
            error_traceback=None,
            duration_ms=1000,
            worker_id=new_uuid(),
            metadata={},
        )
        await backend.write_attempt(attempt)

        result = await backend.get_attempts(row.id)
        assert len(result) == 1
        assert result[0].attempt == 1
        assert result[0].outcome == "succeeded"

    async def test_multiple_attempts_sorted(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)

        for i in [3, 1, 2]:
            attempt = AttemptRow(
                job_id=row.id,
                attempt=i,
                started_at=_START,
                finished_at=None,
                outcome="failed",
                error_class="Err",
                error_message="fail",
                error_traceback=None,
                duration_ms=None,
                worker_id=new_uuid(),
                metadata={},
            )
            await backend.write_attempt(attempt)

        result = await backend.get_attempts(row.id)
        assert len(result) == 3
        assert [a.attempt for a in result] == [1, 2, 3]

    async def test_missing_job_returns_empty(self) -> None:
        backend = _make_backend()
        assert await backend.get_attempts(new_job_id()) == []


# ── Cancel signals ────────────────────────────────────────────────────


class TestWriteCancelRequest:
    async def test_case_a_running_phase0(self) -> None:
        """Case (a): running job with cancel_phase=0 → sets cancel_requested_at, phase=1."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)

        assert await backend.write_cancel_request(job_id, "test reason") is True
        row = await backend.get(job_id)
        assert row is not None
        assert row.cancel_requested_at is not None
        assert row.cancel_phase == 1
        assert row.status == "running"

    async def test_case_b_pending(self) -> None:
        """Case (b): pending job → transition to cancelled."""
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert row.status == "pending"

        assert await backend.write_cancel_request(row.id, None) is True
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "cancelled"
        assert updated.finished_at is not None

    async def test_case_b_scheduled(self) -> None:
        """Case (b): scheduled job → transition to cancelled."""
        backend = _make_backend()
        future = _START + timedelta(hours=1)
        args = _enqueue_args(scheduled_at=future)
        row = await backend.enqueue(args)
        assert row.status == "scheduled"

        assert await backend.write_cancel_request(row.id, None) is True
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "cancelled"

    async def test_case_c_terminal_returns_false(self) -> None:
        """Case (c): terminal status → no-op, return False."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        await backend.mark_succeeded(job_id, wid, None)

        assert await backend.write_cancel_request(job_id, None) is False

    async def test_case_c_already_phase1(self) -> None:
        """Case (c): cancel_phase > 0 → no-op, return False."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        await backend.write_cancel_request(job_id, None)

        assert await backend.write_cancel_request(job_id, None) is False


class TestPollCancelFlags:
    async def test_returns_flags_for_worker(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        await backend.write_cancel_request(job_id, None)

        flags = await backend.poll_cancel_flags(wid)
        assert len(flags) == 1
        assert flags[0].job_id == job_id
        assert flags[0].cancel_phase == 1

    async def test_wrong_worker_returns_empty(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        await backend.write_cancel_request(job_id, None)

        flags = await backend.poll_cancel_flags(new_uuid())
        assert flags == []

    async def test_no_cancel_requested(self) -> None:
        backend = _make_backend()
        await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        flags = await backend.poll_cancel_flags(wid)
        assert flags == []


# ── Sweep methods ──────────────────────────────────────────────────────


class TestScheduledToPending:
    async def test_promotes_due_jobs(self) -> None:
        backend = _make_backend()
        future = _START + timedelta(hours=1)
        args = _enqueue_args(scheduled_at=future)
        row = await backend.enqueue(args)
        assert row.status == "scheduled"

        count = await backend.scheduled_to_pending(future)
        assert count == 1
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "pending"

    async def test_skips_future_jobs(self) -> None:
        backend = _make_backend()
        far_future = _START + timedelta(hours=2)
        args = _enqueue_args(scheduled_at=far_future)
        await backend.enqueue(args)

        count = await backend.scheduled_to_pending(_START + timedelta(hours=1))
        assert count == 0

    async def test_skips_non_scheduled(self) -> None:
        backend = _make_backend()
        args = _enqueue_args(scheduled_at=_START)
        row = await backend.enqueue(args)
        assert row.status == "pending"

        count = await backend.scheduled_to_pending(_START)
        assert count == 0


class TestDeadlineSweep:
    async def test_fails_pending_past_deadline(self) -> None:
        backend = _make_backend()
        deadline = _START + timedelta(hours=1)
        args = _enqueue_args(schedule_to_close=deadline)
        row = await backend.enqueue(args)
        assert row.status == "pending"

        now = _START + timedelta(hours=2)
        count = await backend.deadline_sweep(now)
        assert count == 1
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "failed"
        assert updated.error_class == "DeadlineExceeded"

    async def test_fails_scheduled_past_deadline(self) -> None:
        backend = _make_backend()
        future = _START + timedelta(hours=3)
        deadline = _START + timedelta(hours=1)
        args = _enqueue_args(scheduled_at=future, schedule_to_close=deadline)
        row = await backend.enqueue(args)

        now = _START + timedelta(hours=2)
        count = await backend.deadline_sweep(now)
        assert count == 1
        updated = await backend.get(row.id)
        assert updated is not None
        assert updated.status == "failed"

    async def test_writes_synthetic_attempt_row_for_undispatched(self) -> None:
        """Deadline sweep writes one AttemptRow and one EventRow per swept
        job, matching ``PostgresBackend.sweep_deadline_exceeded`` which uses
        ``COALESCE(started_at, now())`` so never-dispatched jobs still have
        an attempt history row with ``started_at = now``.
        """
        backend = _make_backend()
        deadline = _START + timedelta(hours=1)
        args = _enqueue_args(schedule_to_close=deadline)
        row = await backend.enqueue(args)

        now = _START + timedelta(hours=2)
        await backend.deadline_sweep(now)

        attempts = await backend.get_attempts(row.id)
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt.started_at is not None
        assert attempt.started_at == now
        assert attempt.worker_id is None
        assert attempt.outcome == "failed"
        assert attempt.error_class == "DeadlineExceeded"
        assert attempt.error_message == "schedule_to_close reached before next dispatch"

    async def test_skips_jobs_without_deadline(self) -> None:
        backend = _make_backend()
        args = _enqueue_args(schedule_to_close=None)
        await backend.enqueue(args)

        count = await backend.deadline_sweep(_START + timedelta(days=1))
        assert count == 0

    async def test_skips_running_jobs(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=3, retry_kind="transient")
        # Set a past deadline
        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, schedule_to_close=_START - timedelta(hours=1))  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        count = await backend.deadline_sweep(_START + timedelta(hours=1))
        assert count == 0

    async def test_writes_synthetic_attempt_with_null_duration_for_previously_dispatched(
        self,
    ) -> None:
        """A pending job with a non-None ``started_at`` (reclaimed from a
        prior dispatch) produces an AttemptRow with ``duration_ms = None``,
        matching the PG path which unconditionally writes None for
        sweep-synthesised attempt rows.
        """
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=3, retry_kind="transient")

        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        # Simulate reclaim_expired_locks returning the job to pending
        # without clearing started_at (PG row shape preserved).
        backend._jobs[job_id] = _replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            row,
            status="pending",
            schedule_to_close=_START - timedelta(hours=1),
        )

        count = await backend.deadline_sweep(_START + timedelta(hours=1))
        assert count == 1

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].duration_ms is None

    async def test_skips_future_deadline(self) -> None:
        """jobs with a non-null, not-yet-expired
        ``schedule_to_close`` are left untouched by the sweep.
        """
        backend = _make_backend()
        future_deadline = _START + timedelta(hours=2)
        args = _enqueue_args(schedule_to_close=future_deadline)
        await backend.enqueue(args)

        now = _START + timedelta(hours=1)
        count = await backend.deadline_sweep(now)
        assert count == 0

    async def test_idempotent_double_sweep(self) -> None:
        """re-sweep idempotence — after a first
        ``deadline_sweep`` transitions the job to ``failed``, a second
        sweep returns 0 and writes no duplicate ``AttemptRow`` or
        ``EventRow``.  The ``'failed'`` status is excluded by the
        ``pending|scheduled`` predicate guard.
        """
        backend = _make_backend()
        deadline = _START + timedelta(hours=1)
        args = _enqueue_args(schedule_to_close=deadline)
        row = await backend.enqueue(args)

        now = _START + timedelta(hours=2)

        c1 = await backend.deadline_sweep(now)
        assert c1 == 1

        c2 = await backend.deadline_sweep(now)
        assert c2 == 0

        attempts = await backend.get_attempts(row.id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "failed"
        assert attempts[0].error_class == "DeadlineExceeded"

        events = await backend.get_events(row.id)
        state_changes = [e for e in events if e.kind == "state_change"]
        assert len(state_changes) == 1
        assert state_changes[0].detail["from_state"] == "pending"
        assert state_changes[0].detail["to_state"] == "failed"
        assert state_changes[0].detail.get("error_class") == "DeadlineExceeded"


class TestReclaimExpiredLocks:
    async def test_reclaim_retryable_to_pending(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=3, retry_kind="transient")

        # Expire the lock
        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, lock_expires_at=_START - timedelta(seconds=1))  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        now = _START + timedelta(seconds=1)
        count = await backend.reclaim_expired_locks(now, _GRACE, _GRACE)
        assert count == 1
        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.status == "pending"
        assert updated.attempt == 1

    async def test_reclaim_non_retryable_to_crashed(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=1, retry_kind="non_retryable")

        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, lock_expires_at=_START - timedelta(seconds=1))  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        now = _START + timedelta(seconds=1)
        count = await backend.reclaim_expired_locks(now, _GRACE, _GRACE)
        assert count == 1
        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.status == "crashed"
        assert updated.error_class is None

    async def test_crashed_writes_attempt_row(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend, max_attempts=1, retry_kind="non_retryable")

        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, lock_expires_at=_START - timedelta(seconds=1))  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        now = _START + timedelta(seconds=1)
        await backend.reclaim_expired_locks(now, _GRACE, _GRACE)

        attempts = await backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == "crashed"

    async def test_skips_non_expired_locks(self) -> None:
        backend = _make_backend()
        await _make_running_row(backend)

        # Lock is still valid (expires in the future)
        count = await backend.reclaim_expired_locks(
            _START + timedelta(seconds=1),
            _GRACE,
            _GRACE,
        )
        assert count == 0

    async def test_skips_cancel_phase_nonzero(self) -> None:
        """Jobs with cancel_phase > 0 are handled by tick_cancel_polling, not here."""
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)

        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            row,
            lock_expires_at=_START - timedelta(seconds=1),
            cancel_requested_at=_START,
            cancel_phase=1,
        )

        count = await backend.reclaim_expired_locks(
            _START + timedelta(seconds=1),
            _GRACE,
            _GRACE,
        )
        assert count == 0


# ── Read methods ──────────────────────────────────────────────────────


class TestGet:
    async def test_existing_job(self) -> None:
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        result = await backend.get(row.id)
        assert result is not None
        assert result.id == row.id

    async def test_missing_job_returns_none(self) -> None:
        backend = _make_backend()
        result = await backend.get(new_job_id())
        assert result is None


class TestListJobs:
    async def test_returns_all_when_no_filter(self) -> None:
        backend = _make_backend()
        for i in range(3):
            args = _enqueue_args(actor=f"actor_{i}")
            await backend.enqueue(args)

        results = await backend.list_jobs(JobFilter())
        assert len(results) == 3

    async def test_filter_by_queue(self) -> None:
        """filter by queue."""
        backend = _make_backend()
        args_q1 = _enqueue_args(queue="q1", actor="a1")
        args_q2 = _enqueue_args(queue="q2", actor="a2")
        await backend.enqueue(args_q1)
        await backend.enqueue(args_q2)

        results = await backend.list_jobs(JobFilter(queue="q1"))
        assert len(results) == 1
        assert results[0].queue == "q1"

    async def test_filter_by_status(self) -> None:
        """filter by status."""
        backend = _make_backend()
        args_now = _enqueue_args(scheduled_at=_START)
        args_future = _enqueue_args(scheduled_at=_START + timedelta(hours=1))
        await backend.enqueue(args_now)
        await backend.enqueue(args_future)

        results = await backend.list_jobs(JobFilter(status="pending"))
        assert all(r.status == "pending" for r in results)

    async def test_filter_by_actor(self) -> None:
        """filter by actor."""
        backend = _make_backend()
        args_a1 = _enqueue_args(actor="a1")
        args_a2 = _enqueue_args(actor="a2")
        await backend.enqueue(args_a1)
        await backend.enqueue(args_a2)

        results = await backend.list_jobs(JobFilter(actor="a1"))
        assert len(results) == 1
        assert results[0].actor == "a1"

    async def test_combined_filter(self) -> None:
        """combined filter narrows correctly."""
        backend = _make_backend()
        await backend.enqueue(_enqueue_args(actor="a1", queue="q1"))
        await backend.enqueue(_enqueue_args(actor="a1", queue="q2"))
        await backend.enqueue(_enqueue_args(actor="a2", queue="q1"))

        results = await backend.list_jobs(JobFilter(actor="a1", queue="q1"))
        assert len(results) == 1
        assert results[0].actor == "a1"
        assert results[0].queue == "q1"

    async def test_limit(self) -> None:
        backend = _make_backend()
        for _ in range(5):
            await backend.enqueue(_enqueue_args())

        results = await backend.list_jobs(JobFilter(limit=2))
        assert len(results) == 2

    async def test_sorted_by_priority_desc(self) -> None:
        backend = _make_backend()
        for p in [0, 5, 3]:
            await backend.enqueue(_enqueue_args(priority=p))

        results = await backend.list_jobs(JobFilter())
        priorities = [r.priority for r in results]
        assert priorities == sorted(priorities, reverse=True)


# ── list_jobs cursor (keyset pagination) ─────────────────────────────


class TestListJobsCursor:
    async def test_cursor_pagination(self) -> None:
        """Keyset pagination: cursor encodes (priority, scheduled_at, id)."""
        backend = _make_backend()
        ids = []
        for p in [5, 3, 1, 4, 2]:
            args = _enqueue_args(priority=p)
            await backend.enqueue(args)
            ids.append(args.id)

        # First page
        page1 = await backend.list_jobs(JobFilter(limit=2))
        assert len(page1) == 2

        # Build cursor from last row of page1
        last = page1[-1]
        cursor = encode_cursor(last.priority, last.scheduled_at, last.id)

        # Second page
        page2 = await backend.list_jobs(JobFilter(limit=2, cursor=cursor))
        assert len(page2) == 2

        # No overlap
        page1_ids = {r.id for r in page1}
        page2_ids = {r.id for r in page2}
        assert page1_ids.isdisjoint(page2_ids)

        # Third page (last row)
        last2 = page2[-1]
        cursor2 = encode_cursor(last2.priority, last2.scheduled_at, last2.id)
        page3 = await backend.list_jobs(JobFilter(limit=2, cursor=cursor2))
        assert len(page3) == 1  # only one remaining

    async def test_cursor_with_no_more_rows(self) -> None:
        backend = _make_backend()
        await backend.enqueue(_enqueue_args(priority=1))

        page1 = await backend.list_jobs(JobFilter(limit=10))
        assert len(page1) == 1

        last = page1[-1]
        cursor = encode_cursor(last.priority, last.scheduled_at, last.id)
        page2 = await backend.list_jobs(JobFilter(limit=10, cursor=cursor))
        assert page2 == []

    async def test_cursor_id_tiebreaker(self) -> None:
        """Cursor pagination uses ``id`` as the tie-breaker when ``priority``
        and ``scheduled_at`` are identical.  Without this test, a bug in
        ``decode_cursor`` or the tuple comparison logic that only manifests
        when the first two keyset columns are equal would go undetected.
        """
        backend = _make_backend()
        # Enqueue two jobs with identical priority and scheduled_at
        same_time = _START
        args_a = _enqueue_args(priority=5, scheduled_at=same_time)
        args_b = _enqueue_args(priority=5, scheduled_at=same_time)
        row_a = await backend.enqueue(args_a)
        row_b = await backend.enqueue(args_b)

        # Sort order is (-priority, scheduled_at, id); since priority and
        # scheduled_at are identical, ordering is by id.
        all_rows = await backend.list_jobs(JobFilter())
        assert len(all_rows) == 2
        sorted_ids = [r.id for r in all_rows]
        assert sorted_ids == sorted(sorted_ids)  # id-ordered

        # Page 1: first row only
        page1 = await backend.list_jobs(JobFilter(limit=1))
        assert len(page1) == 1

        # Cursor from page 1's last row
        last = page1[-1]
        cursor = encode_cursor(last.priority, last.scheduled_at, last.id)

        # Page 2: second row, no overlap with page 1
        page2 = await backend.list_jobs(JobFilter(limit=1, cursor=cursor))
        assert len(page2) == 1
        page1_ids = {r.id for r in page1}
        page2_ids = {r.id for r in page2}
        assert page1_ids.isdisjoint(page2_ids)

        # Together they cover all rows
        assert page1_ids | page2_ids == {row_a.id, row_b.id}


# ── Cursor encoding ───────────────────────────────────────────────────


class TestCursorEncoding:
    def test_encode_decode_roundtrip(self) -> None:
        job_id = new_uuid()
        scheduled = _START + timedelta(minutes=5)
        cursor = encode_cursor(3, scheduled, job_id)
        p, s, i = decode_cursor(cursor)
        assert p == 3
        assert s == scheduled
        assert i == job_id

    def test_invalid_cursor_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor("bad")


# ── bool-returning terminal writes ─────────────────────


class TestBoolReturningTerminalWrites:
    """verify bool return types and True→False pattern."""

    async def test_mark_succeeded_bool(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        r1 = await backend.mark_succeeded(job_id, wid, None)
        r2 = await backend.mark_succeeded(job_id, wid, None)
        assert r1 is True
        assert r2 is False

    async def test_mark_cancelled_bool(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        r1 = await backend.mark_cancelled(job_id, wid)
        r2 = await backend.mark_cancelled(job_id, wid)
        assert r1 is True
        assert r2 is False

    async def test_mark_abandoned_bool(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        # : mark_abandoned requires cancel_phase=2
        from dataclasses import replace as _replace

        row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[job_id] = _replace(row, cancel_phase=2)  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        r1 = await backend.mark_abandoned(job_id)
        r2 = await backend.mark_abandoned(job_id)
        assert r1 is True
        assert r2 is False

    async def test_mark_snoozed_bool(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        delay = timedelta(seconds=30)
        r1 = await backend.mark_snoozed(job_id, wid, delay)
        r2 = await backend.mark_snoozed(job_id, wid, delay)
        assert r1 == "scheduled"
        assert r2 == "noop"

    async def test_write_cancel_escalation_bool(self) -> None:
        backend = _make_backend()
        job_id, _ = await _make_running_row(backend)
        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        await backend.write_cancel_request(job_id, None)
        r1 = await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        r2 = await backend.write_cancel_escalation(job_id, wid, 2)  # type: ignore[arg-type]  # Why: Literal[2] not narrowed from int literal by pyright
        assert r1 is True
        assert r2 is False


# ── isinstance(InMemoryBackend, Backend) ───────────────


class TestRuntimeCheckable:
    async def test_isinstance_backend(self) -> None:
        """InMemoryBackend satisfies Backend at runtime.

        All methods are fully implemented; isinstance returns True and
        the protocol check passes.
        """
        backend = _make_backend()
        assert isinstance(backend, Backend)

    async def test_static_type_compatibility(self) -> None:
        """InMemoryBackend satisfies Backend at the static type level.

        pyright verifies this call site.
        """

        def takes_backend(b: Backend) -> None:
            pass

        takes_backend(_make_backend())


# ── No forbidden imports ──────────────────────────────────────────────


class TestNoForbiddenImports:
    def test_no_asyncpg(self) -> None:
        import taskq.testing.in_memory as im_mod

        source_file = im_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            content = f.read()
        assert "import asyncpg" not in content
        assert "from asyncpg" not in content

    def test_no_redis(self) -> None:
        import taskq.testing.in_memory as im_mod

        source_file = im_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            content = f.read()
        assert "import redis" not in content
        assert "from redis" not in content

    def test_no_testcontainers(self) -> None:
        import taskq.testing.in_memory as im_mod

        source_file = im_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            content = f.read()
        assert "import testcontainers" not in content
        assert "from testcontainers" not in content

    def test_no_future_annotations(self) -> None:
        """no ``from __future__ import annotations``."""
        import taskq.testing.in_memory as im_mod

        source_file = im_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            for line in f:
                assert "from __future__ import annotations" not in line

    def test_no_future_annotations_in_clock(self) -> None:
        import taskq.testing.clock as clock_mod

        source_file = clock_mod.__spec__.origin
        assert source_file is not None
        with open(source_file) as f:
            for line in f:
                assert "from __future__ import annotations" not in line


# ── Single-threaded guard ──────────────────────────────────────────────


# ── register_stub ─────────────────────────────────────────────────────


class TestRegisterStub:
    def test_register_and_overwrite(self) -> None:
        backend = _make_backend()

        def stub_a(payload: object) -> object:
            return "a"

        def stub_b(payload: object) -> object:
            return "b"

        backend.register_stub("actor_a", stub_a)
        assert backend._actor_stubs["actor_a"] is stub_a  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        backend.register_stub("actor_a", stub_b)
        assert backend._actor_stubs["actor_a"] is stub_b  # type: ignore[reportPrivateUsage]  # Why: test-only private access


# ── max_pending enforcement ────────────────────────────────────────────


class TestMaxPendingEnforcement:
    async def test_smoke_max_pending_exceeded(self) -> None:
        """Smoke test: actor with max_pending=1, two enqueues; second raises
        MaxPendingExceededError with current_count=1, max_pending=1."""
        backend = _make_backend()
        args1 = _enqueue_args(actor="capped_actor")
        args2 = _enqueue_args(actor="capped_actor")
        object.__setattr__(args1, "max_pending", 1)
        object.__setattr__(args2, "max_pending", 1)

        await backend.enqueue(args1)

        with pytest.raises(MaxPendingExceededError) as exc_info:
            await backend.enqueue(args2)

        assert exc_info.value.actor == "capped_actor"
        assert exc_info.value.current_count == 1
        assert exc_info.value.max_pending == 1
        assert exc_info.value.pending == 1
        assert isinstance(exc_info.value, BackpressureError)

    async def test_max_pending_none_is_unbounded(self) -> None:
        """max_pending=None performs no count check; enqueue always succeeds."""
        backend = _make_backend()
        for _ in range(10):
            args = _enqueue_args(actor="unbounded_actor")
            row = await backend.enqueue(args)
            assert row.status == "pending"

    async def test_max_pending_zero_rejects_immediately(self) -> None:
        """max_pending=0 rejects the first enqueue (count=0 >= 0)."""
        backend = _make_backend()
        args = _enqueue_args(actor="zero_actor")
        object.__setattr__(args, "max_pending", 0)

        with pytest.raises(MaxPendingExceededError) as exc_info:
            await backend.enqueue(args)

        assert exc_info.value.current_count == 0
        assert exc_info.value.max_pending == 0

    async def test_running_jobs_not_counted(self) -> None:
        """Running jobs are excluded from the pending count."""
        backend = _make_backend()
        # Enqueue and dispatch to running
        args = _enqueue_args(actor="runner_actor")
        object.__setattr__(args, "max_pending", 1)
        row = await backend.enqueue(args)

        from dataclasses import replace as _replace

        now = backend._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        backend._jobs[row.id] = _replace(  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            row,
            status="running",
            locked_by_worker=backend._worker_id,  # type: ignore[reportPrivateUsage]  # Why: test-only private access
            lock_expires_at=now + timedelta(seconds=30),
            started_at=now,
            attempt=1,
        )

        # Second enqueue should succeed because running is not counted
        args2 = _enqueue_args(actor="runner_actor")
        object.__setattr__(args2, "max_pending", 1)
        row2 = await backend.enqueue(args2)
        assert row2.status == "pending"


# ── lock expires after FakeClock advance ──────


class TestLockExpiryAfterClockAdvance:
    async def test_lock_expires_after_clock_advance(self) -> None:
        """Lock expires after FakeClock advance past lock_expires_at."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock,
            cancellation_grace_period=_GRACE,
            cleanup_grace_period=_GRACE,
        )

        args = _enqueue_args(actor="test_actor")
        row = await backend.enqueue(args)
        job_id = row.id

        wid = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        now = backend._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test-only private access
        lock_lease = timedelta(seconds=30)
        running_row = _replace(
            row,
            status="running",
            locked_by_worker=wid,
            lock_expires_at=now + lock_lease,
            started_at=now,
            attempt=1,
        )
        backend._jobs[job_id] = running_row  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        assert backend._jobs[job_id].status == "running"  # type: ignore[reportPrivateUsage]  # Why: test-only private access

        clock.advance(lock_lease + timedelta(seconds=1))
        now_after = clock.now()

        count = await backend.reclaim_expired_locks(now_after, _GRACE, _GRACE)
        assert count == 1

        updated = await backend.get(job_id)
        assert updated is not None
        assert updated.status != "running"
        assert updated.status in ("pending", "crashed")


# ── Archive terminal jobs ───────────────────────


class TestArchiveTerminalJobs:
    async def test_old_succeeded_archived_recent_untouched(self) -> None:
        """5 old succeeded jobs archived; 5 recent untouched; attempts present in archive."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        old_time = _START - timedelta(days=31)
        recent_time = _START - timedelta(hours=1)

        old_ids: list[JobId] = []
        recent_ids: list[JobId] = []
        for _ in range(5):
            args = _enqueue_args()
            row = await backend.enqueue(args)
            backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=old_time)  # type: ignore[reportPrivateUsage]
            backend._attempts[row.id] = [  # type: ignore[reportPrivateUsage]
                AttemptRow(
                    job_id=row.id,
                    attempt=1,
                    started_at=old_time,
                    finished_at=old_time,
                    outcome="succeeded",
                    error_class=None,
                    error_message=None,
                    error_traceback=None,
                    duration_ms=1000,
                    worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]
                    metadata={},
                )
            ]
            old_ids.append(row.id)

        for _ in range(5):
            args = _enqueue_args()
            row = await backend.enqueue(args)
            backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=recent_time)  # type: ignore[reportPrivateUsage]
            recent_ids.append(row.id)

        result = backend.archive_terminal_jobs(
            retention=timedelta(days=30),
            archive_retention=timedelta(days=365),
        )

        assert result.total_deleted == 5
        assert result.archived == 5
        assert result.by_status == {"succeeded": 5}

        for jid in old_ids:
            assert await backend.get(jid) is None
            archived = await backend.get_archived(jid)
            assert archived is not None
            assert archived.row.status == "succeeded"
            assert archived.expire_at == _START + timedelta(days=365)
            assert backend._archive_attempts.get(jid) is not None  # type: ignore[reportPrivateUsage]
            assert len(backend._archive_attempts[jid]) == 1  # type: ignore[reportPrivateUsage]

        for jid in recent_ids:
            assert await backend.get(jid) is not None
            assert await backend.get_archived(jid) is None

    async def test_per_status_retention(self) -> None:
        """per-status retention — succeeded/cancelled archived at 35d,
        failed retained at 35d when failure retention=90d. Uses the `statuses`
        parameter to simulate per-status retention calls."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        ago_35d = _START - timedelta(days=35)

        args_s = _enqueue_args(actor="s_actor")
        row_s = await backend.enqueue(args_s)
        backend._jobs[row_s.id] = _replace(row_s, status="succeeded", finished_at=ago_35d)  # type: ignore[reportPrivateUsage]
        succeeded_id = row_s.id

        args_f = _enqueue_args(actor="f_actor")
        row_f = await backend.enqueue(args_f)
        backend._jobs[row_f.id] = _replace(row_f, status="failed", finished_at=ago_35d)  # type: ignore[reportPrivateUsage]
        failed_id = row_f.id

        args_c = _enqueue_args(actor="c_actor")
        row_c = await backend.enqueue(args_c)
        backend._jobs[row_c.id] = _replace(row_c, status="cancelled", finished_at=ago_35d)  # type: ignore[reportPrivateUsage]
        cancelled_id = row_c.id

        result_sc = backend.archive_terminal_jobs(
            retention=timedelta(days=30),
            archive_retention=timedelta(days=365),
            statuses=frozenset({"succeeded", "cancelled"}),
        )
        assert result_sc.total_deleted == 2
        assert succeeded_id not in backend._jobs  # type: ignore[reportPrivateUsage]
        assert cancelled_id not in backend._jobs  # type: ignore[reportPrivateUsage]
        assert failed_id in backend._jobs  # type: ignore[reportPrivateUsage]

        result_f = backend.archive_terminal_jobs(
            retention=timedelta(days=90),
            archive_retention=timedelta(days=365),
            statuses=frozenset({"failed"}),
        )
        assert result_f.total_deleted == 0
        assert failed_id in backend._jobs  # type: ignore[reportPrivateUsage]

    async def test_archive_retention_zero(self) -> None:
        """archive_retention=timedelta(0) results in expire_at ≈ clock.now()."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        ago_31d = _START - timedelta(days=31)
        args = _enqueue_args()
        row = await backend.enqueue(args)
        backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=ago_31d)  # type: ignore[reportPrivateUsage]

        backend.archive_terminal_jobs(
            retention=timedelta(days=30),
            archive_retention=timedelta(0),
        )

        archived = await backend.get_archived(row.id)
        assert archived is not None
        assert archived.expire_at == _START

    async def test_non_terminal_jobs_not_archived(self) -> None:
        """Only terminal jobs are moved to archive; pending/running/scheduled stay."""
        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        assert row.status == "pending"

        result = backend.archive_terminal_jobs(
            retention=timedelta(days=0),
            archive_retention=timedelta(days=365),
        )
        assert result.total_deleted == 0
        assert await backend.get(row.id) is not None

    async def test_jobs_without_finished_at_not_archived(self) -> None:
        """Terminal jobs with finished_at=None are not archived."""
        from dataclasses import replace as _replace

        backend = _make_backend()
        args = _enqueue_args()
        row = await backend.enqueue(args)
        backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=None)  # type: ignore[reportPrivateUsage]

        result = backend.archive_terminal_jobs(
            retention=timedelta(days=0),
            archive_retention=timedelta(days=365),
        )
        assert result.total_deleted == 0

    async def test_by_actor_counts(self) -> None:
        """PruneResult.by_actor counts are correct across different actors."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )
        ago_31d = _START - timedelta(days=31)

        for actor_name in ("a1", "a1", "a2"):
            args = _enqueue_args(actor=actor_name)
            row = await backend.enqueue(args)
            backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=ago_31d)  # type: ignore[reportPrivateUsage]

        result = backend.archive_terminal_jobs(
            retention=timedelta(days=30),
            archive_retention=timedelta(days=365),
        )
        assert result.by_actor == {"a1": 2, "a2": 1}

    async def test_cutoffs_populated(self) -> None:
        """PruneResult.cutoffs contains each archived status mapped to the cutoff."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )
        ago_31d = _START - timedelta(days=31)
        retention = timedelta(days=30)
        cutoff = _START - retention

        args = _enqueue_args()
        row = await backend.enqueue(args)
        backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=ago_31d)  # type: ignore[reportPrivateUsage]

        result = backend.archive_terminal_jobs(
            retention=retention, archive_retention=timedelta(days=365)
        )
        assert result.cutoffs == {"succeeded": cutoff}


# ── Expire archived jobs ──────────────────────────────────────


class TestExpireArchivedJobs:
    async def test_expired_rows_deleted(self) -> None:
        """Seed 3 rows in _archive with expire_at = clock.now() - 1s;
        call expire_archived_jobs(); assert 0 rows remain."""
        from dataclasses import replace as _replace

        from taskq.testing.in_memory import _ArchivedJobRow

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        ago_31d = _START - timedelta(days=31)
        expired_ids: list[JobId] = []
        for _ in range(3):
            args = _enqueue_args()
            row = await backend.enqueue(args)
            job_row = _replace(row, status="succeeded", finished_at=ago_31d)
            backend._archive[row.id] = _ArchivedJobRow(  # type: ignore[reportPrivateUsage]
                row=job_row,
                archived_at=ago_31d,
                expire_at=_START - timedelta(seconds=1),
            )
            backend._archive_attempts[row.id] = [  # type: ignore[reportPrivateUsage]
                AttemptRow(
                    job_id=row.id,
                    attempt=1,
                    started_at=ago_31d,
                    finished_at=ago_31d,
                    outcome="succeeded",
                    error_class=None,
                    error_message=None,
                    error_traceback=None,
                    duration_ms=1000,
                    worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]
                    metadata={},
                )
            ]
            expired_ids.append(row.id)

        result = backend.expire_archived_jobs()

        assert result.total_deleted == 3
        assert result.by_status == {"succeeded": 3}
        assert result.expire_before == _START

        for jid in expired_ids:
            assert jid not in backend._archive  # type: ignore[reportPrivateUsage]
            assert jid not in backend._archive_attempts  # type: ignore[reportPrivateUsage]

    async def test_unexpired_rows_remain(self) -> None:
        """Rows with expire_at in the future are not deleted."""
        from dataclasses import replace as _replace

        from taskq.testing.in_memory import _ArchivedJobRow

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        ago_31d = _START - timedelta(days=31)
        args = _enqueue_args()
        row = await backend.enqueue(args)
        job_row = _replace(row, status="succeeded", finished_at=ago_31d)
        backend._archive[row.id] = _ArchivedJobRow(  # type: ignore[reportPrivateUsage]
            row=job_row,
            archived_at=ago_31d,
            expire_at=_START + timedelta(days=1),
        )

        result = backend.expire_archived_jobs()
        assert result.total_deleted == 0
        assert row.id in backend._archive  # type: ignore[reportPrivateUsage]

    async def test_empty_archive_returns_zero(self) -> None:
        backend = _make_backend()
        result = backend.expire_archived_jobs()
        assert result.total_deleted == 0
        assert result.by_status == {}


# ── Archive fallback lookup ──────────────────────────────────


class TestArchiveFallbackLookup:
    async def test_archived_job_retrievable(self) -> None:
        """After archive_terminal_jobs(), a job absent from _jobs
        but present in _archive is retrievable via get_archived."""
        from dataclasses import replace as _replace

        from taskq.testing.clock import FakeClock

        clock = FakeClock(_START)
        backend = InMemoryBackend(
            clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
        )

        ago_31d = _START - timedelta(days=31)
        args = _enqueue_args()
        row = await backend.enqueue(args)
        backend._jobs[row.id] = _replace(row, status="succeeded", finished_at=ago_31d)  # type: ignore[reportPrivateUsage]

        backend.archive_terminal_jobs(
            retention=timedelta(days=30),
            archive_retention=timedelta(days=365),
        )

        assert await backend.get(row.id) is None
        archived = await backend.get_archived(row.id)
        assert archived is not None
        assert archived.row.id == row.id
        assert archived.row.status == "succeeded"
        assert archived.archived_at is not None
        assert archived.expire_at is not None


# ── prune invariant ────────────────────────────────────────────


_TERMINAL_STATUS_STRATEGY = st.sampled_from(sorted(TERMINAL_STATUSES))

_DAYS_AGO_STRATEGY = st.floats(min_value=0, max_value=200)
_RETENTION_DAYS_STRATEGY = st.floats(min_value=0, max_value=180)


@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
@given(
    status=_TERMINAL_STATUS_STRATEGY,
    days_ago=_DAYS_AGO_STRATEGY,
    retention_days=_RETENTION_DAYS_STRATEGY,
)
async def test_prune_invariant_archives_old_keeps_recent(
    status: str,
    days_ago: float,
    retention_days: float,
) -> None:
    """For any terminal job with finished_at < now - retention,
    exactly one archive_terminal_jobs call archives it. For any row with
    finished_at >= now - retention, zero jobs archived.

    Hypothesis generates random finished_at (up to 200 days ago) and
    retention (0 to 180 days, non-negative). The property holds for all
    five terminal statuses.
    """
    from dataclasses import replace as _replace

    from taskq.testing.clock import FakeClock

    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock, cancellation_grace_period=_GRACE, cleanup_grace_period=_GRACE
    )

    finished_at = _START - timedelta(days=days_ago)
    retention = timedelta(days=retention_days)
    archive_retention = timedelta(days=365)

    args = _enqueue_args()
    row = await backend.enqueue(args)
    backend._jobs[row.id] = _replace(row, status=status, finished_at=finished_at)  # type: ignore[reportPrivateUsage]

    result = backend.archive_terminal_jobs(
        retention=retention,
        archive_retention=archive_retention,
    )

    should_archive = finished_at < _START - retention

    if should_archive:
        assert result.total_deleted == 1
        assert result.archived == 1
        assert await backend.get(row.id) is None
        archived = await backend.get_archived(row.id)
        assert archived is not None
        assert archived.row.status == status
        assert archived.expire_at == _START + archive_retention
    else:
        assert result.total_deleted == 0
        assert result.archived == 0
        assert await backend.get(row.id) is not None
        assert await backend.get_archived(row.id) is None
