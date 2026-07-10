"""Negative tests for illegal state transitions ().

Covers..from the test plan. Each test asserts that
an illegal transition is rejected at one of the two enforcement layers:
the application-side ``assert_valid_transition`` fast-path or the
SQL-guard (in-memory WHERE-equivalent) rowcount=0 layer.

All tests use the ``memory_jobs`` fixture (in-memory backend).
PG-equivalence is handled by integration tests where applicable.
"""

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import (
    ErrorInfo,
    JobId,
    JobStatus,
    RetryKind,
)
from taskq.backend.statemachine import (
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    assert_valid_transition,
)
from taskq.exceptions import IllegalStateTransition, WorkerOwnershipMismatch
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    *,
    max_attempts: int = 3,
    retry_kind: RetryKind = "transient",
    schedule_to_close: datetime | None = None,
    scheduled_at: datetime = _START,
) -> tuple[JobId, UUID]:
    args = make_enqueue_args(
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        schedule_to_close=schedule_to_close,
        scheduled_at=scheduled_at,
    )
    await backend.enqueue(args)
    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for dispatch_batch
    dispatched = await backend.dispatch_batch(
        worker_id,
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


# ── pending → succeeded blocked ──────────────────────────────────


async def test_pending_to_succeeded_blocked(memory_jobs: InMemoryBackend) -> None:
    """pending → succeeded is blocked by assert_valid_transition.

    A pending job cannot skip directly to succeeded; it must go through
    running first ().
    """
    job_id = new_uuid()
    with pytest.raises(IllegalStateTransition):
        assert_valid_transition(
            from_status="pending",
            to_status="succeeded",
            job_id=job_id,
        )


# ── pending → failed blocked (except deadline sweep) ─────────────


async def test_pending_to_failed_blocked_by_mark_failed_or_retry(
    memory_jobs: InMemoryBackend,
) -> None:
    """pending → failed is blocked at the mark_failed_or_retry layer.

    ``pending → failed`` IS in VALID_TRANSITIONS (the sweep is an
    authorized path), so assert_valid_transition does not
    raise. The enforcement gate is the mark_failed_or_retry WHERE
    guard: it requires status='running', so calling it on a pending
    row raises WorkerOwnershipMismatch. The only authorized
    pending → failed path is the deadline-exceeded sweep, which
    bypasses both assert_valid_transition and mark_failed_or_retry.
    """
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=_START)
    await memory_jobs.enqueue(args)

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "pending"

    worker_id = memory_jobs._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for worker_id
    with pytest.raises(WorkerOwnershipMismatch):
        await memory_jobs.mark_failed_or_retry(
            args.id,
            worker_id,
            ErrorInfo(error_class="TestError", error_message="test", error_traceback=None),
            next_scheduled_at=None,
        )


async def test_pending_to_failed_allowed_via_deadline_sweep(
    memory_jobs: InMemoryBackend,
) -> None:
    """deadline_sweep IS the authorized pending → failed path.

    Enqueue a pending job with a past schedule_to_close, run
    deadline_sweep, and assert status='failed' with
    error_class='DeadlineExceeded'.
    """
    deadline = _START + timedelta(seconds=10)
    args = make_enqueue_args(
        payload={"key": "value"}, schedule_to_close=deadline, scheduled_at=_START
    )
    await memory_jobs.enqueue(args)

    memory_jobs.advance_clock_to(_START + timedelta(seconds=20))
    count = await memory_jobs.deadline_sweep(datetime(2025, 1, 1, 0, 0, 20, tzinfo=UTC))
    assert count == 1

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"


# ── scheduled → running blocked by dispatch query ────────────────


async def test_scheduled_to_running_blocked_by_dispatch(
    memory_jobs: InMemoryBackend,
) -> None:
    """scheduled → running is blocked by the dispatch query.

    Enqueue a job with future scheduled_at (so it lands in 'scheduled'),
    call dispatch_batch, and assert the job is NOT in the dispatch result.
    The dispatch CTE selects WHERE status='pending'; a scheduled row
    must not appear.
    """
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=future)
    await memory_jobs.enqueue(args)

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "scheduled"

    worker_id = memory_jobs._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for dispatch_batch
    dispatched = await memory_jobs.dispatch_batch(
        worker_id,
        ["default"],
        limit=10,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 0


# ── scheduled → failed blocked (except deadline sweep) ────────────


async def test_scheduled_to_failed_blocked_by_mark_failed_or_retry(
    memory_jobs: InMemoryBackend,
) -> None:
    """scheduled → failed is blocked at the mark_failed_or_retry layer.

    ``scheduled → failed`` IS in VALID_TRANSITIONS (the sweep is an
    authorized path), so assert_valid_transition does not
    raise. The enforcement gate is the mark_failed_or_retry WHERE
    guard: it requires status='running', so calling it on a scheduled
    row raises WorkerOwnershipMismatch. The only authorized
    scheduled → failed path is the deadline-exceeded sweep, which
    bypasses both assert_valid_transition and mark_failed_or_retry.
    """
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(payload={"key": "value"}, scheduled_at=future)
    await memory_jobs.enqueue(args)

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "scheduled"

    worker_id = memory_jobs._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access for worker_id
    with pytest.raises(WorkerOwnershipMismatch):
        await memory_jobs.mark_failed_or_retry(
            args.id,
            worker_id,
            ErrorInfo(error_class="TestError", error_message="test", error_traceback=None),
            next_scheduled_at=None,
        )


async def test_scheduled_to_failed_allowed_via_deadline_sweep(
    memory_jobs: InMemoryBackend,
) -> None:
    """deadline_sweep IS the authorized scheduled → failed path.

    Enqueue a scheduled job with a past schedule_to_close, run
    deadline_sweep, and assert status='failed' with
    error_class='DeadlineExceeded'.
    """
    deadline = _START + timedelta(seconds=10)
    future = _START + timedelta(hours=1)
    args = make_enqueue_args(
        payload={"key": "value"}, scheduled_at=future, schedule_to_close=deadline
    )
    await memory_jobs.enqueue(args)

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "scheduled"

    memory_jobs.advance_clock_to(_START + timedelta(seconds=20))
    count = await memory_jobs.deadline_sweep(datetime(2025, 1, 1, 0, 0, 20, tzinfo=UTC))
    assert count == 1

    row = await memory_jobs.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "DeadlineExceeded"


# ── terminal → anything blocked ───────────────────────────────────

_NON_TERMINAL_TARGETS: list[JobStatus] = cast(
    list[JobStatus],
    [s for s in sorted(TERMINAL_STATUSES) if s != "succeeded"]
    + ["pending", "scheduled", "running"],
)


@pytest.mark.parametrize(
    ("terminal_status", "target"),
    [
        (ts, target)
        for ts in sorted(TERMINAL_STATUSES)
        for target in _NON_TERMINAL_TARGETS
        if target not in VALID_TRANSITIONS.get(ts, frozenset())
    ],
    ids=[
        f"{ts}->{t}"
        for ts in sorted(TERMINAL_STATUSES)
        for t in _NON_TERMINAL_TARGETS
        if t not in VALID_TRANSITIONS.get(ts, frozenset())
    ],
)
async def test_terminal_to_anything_blocked(
    terminal_status: JobStatus,
    target: JobStatus,
) -> None:
    """Terminal status → anything is blocked by assert_valid_transition.

    Every transition out of a terminal status raises
    IllegalStateTransition.
    """
    job_id = new_uuid()
    with pytest.raises(IllegalStateTransition):
        assert_valid_transition(
            from_status=terminal_status,
            to_status=target,
            job_id=job_id,
        )


async def test_succeeded_mark_succeeded_noop(memory_jobs: InMemoryBackend) -> None:
    """mark_succeeded on a succeeded row returns False (no mutation).

    The SQL-guard layer (in-memory WHERE-equivalent) rejects the write
    with a no-op return, confirming idempotent terminal-write semantics.
    """
    job_id, worker_id = await _enqueue_and_dispatch(memory_jobs)
    ok = await memory_jobs.mark_succeeded(job_id, worker_id, result={"v": 1})
    assert ok is True

    row = await memory_jobs.get(job_id)
    assert row is not None
    assert row.status == "succeeded"

    ok2 = await memory_jobs.mark_succeeded(job_id, worker_id, result={"v": 2})
    assert ok2 is False


# ── running → running not in VALID_TRANSITIONS ────────────────────


async def test_running_to_running_blocked() -> None:
    """running → running is not in VALID_TRANSITIONS.

    assert_valid_transition rejects it. cancel_phase escalation
    (running/cp=0 → running/cp=1 → running/cp=2) writes a
    cancel_phase column update, which is NOT a status transition —
    the status column remains 'running' throughout.
    """
    job_id = new_uuid()
    with pytest.raises(IllegalStateTransition):
        assert_valid_transition(
            from_status="running",
            to_status="running",
            job_id=job_id,
        )
