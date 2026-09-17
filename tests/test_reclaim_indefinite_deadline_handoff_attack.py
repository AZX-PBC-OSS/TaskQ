"""Red-team attack: an ``indefinite`` job reclaimed with a backoff delay
that lands its ``scheduled_at`` past its own ``schedule_to_close``.

Hypothesis under test: crash-reclaim's hand-back branch does not clamp
``scheduled_at`` against ``schedule_to_close`` (unlike ``mark_snoozed``,
which refuses to snooze past the deadline at all). If nothing else picks
up the slack, a job could sit forever at ``pending``/``scheduled`` with a
``scheduled_at`` beyond its own deadline — never dispatched (dispatch
excludes rows past ``schedule_to_close``) and never resolved (nothing
else visits it) — a silent stall, not a crash-loss, but still a violation
of "no work stalled forever".

The actual design (confirmed by reading ``_SWEEP_2_SQL`` /
``_deadline_sweep``): a second, independent sweep scans
``status IN ('pending', 'scheduled') AND schedule_to_close < now`` and
fails any such row with ``DeadlineExceeded``, regardless of how it got
into that status. So reclaim handing a job back with a scheduled_at past
its deadline is not a stall — the deadline sweep resolves it on its next
tick. This test exercises that handoff for real, on the in-memory twin,
to confirm the two sweeps compose correctly rather than asserting it from
reading the code.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.asyncio

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)


async def test_reclaim_near_deadline_indefinite_job_is_resolved_by_the_deadline_sweep_not_stalled() -> (
    None
):
    """An ``indefinite`` job crashes with its ``schedule_to_close`` only
    seconds away — closer than the reclaim backoff delay its own retry
    policy computes. Reclaim hands it back (attempt budget is open); the
    resulting ``scheduled_at`` lands after the deadline. The job must not
    become a permanent stall: the deadline sweep must terminalise it once
    the clock passes ``schedule_to_close``, and it must do so without a
    second crash-reclaim ever being needed."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )
    backend.register_actor_config(actor="actor_a")
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend, mirrors tests/test_reclaim_retry_budget_parity.py

    job_id = new_job_id()
    # Deadline just 5 seconds out; a base backoff of 60s with no cap
    # guarantees the reclaim delay lands well past it.
    deadline = _START + timedelta(seconds=5)
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=1,
            retry_kind="indefinite",
            retry_base=timedelta(seconds=60),
            retry_backoff="fixed",
            retry_jitter=0.0,
            scheduled_at=_START,
            schedule_to_close=deadline,
        )
    )

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LOCK_LEASE
    )
    assert job_id in {row.id for row in dispatched}

    # Simulate a worker crash: age the lease into the past.
    row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing the crashed-holder state; mirrors tests/test_reclaim_retry_budget_parity.py's _expire_lease
    from dataclasses import replace

    backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
        row, lock_expires_at=clock.now() - timedelta(seconds=1)
    )

    reclaimed = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert reclaimed == 1, "the crashed indefinite job must be reclaimed, not skipped"

    after_reclaim = await backend.get(job_id)
    assert after_reclaim is not None
    assert after_reclaim.status in ("pending", "scheduled"), (
        f"an indefinite job with attempt budget left must be handed back, got "
        f"{after_reclaim.status!r}"
    )
    assert after_reclaim.scheduled_at is not None
    assert after_reclaim.scheduled_at > deadline, (
        "the scenario requires the reclaim delay to overshoot the deadline — "
        f"got scheduled_at={after_reclaim.scheduled_at!r}, deadline={deadline!r}; "
        "if this fails, the backoff formula changed and no longer produces the "
        "overshoot this test needs to attack"
    )

    # Advance the clock past the deadline (but the row's scheduled_at
    # hand-back time may still be in the future) and run the deadline
    # sweep — the second, independent sweep that watches (pending,
    # scheduled) rows for an expired schedule_to_close.
    clock.advance(timedelta(seconds=10))
    swept = await backend.deadline_sweep()
    assert swept == 1, (
        "the deadline sweep must resolve a (pending/scheduled) row whose "
        "schedule_to_close has passed, regardless of how it reached that "
        "status — reclaim's hand-back must not create a row the deadline "
        "sweep's predicate fails to match"
    )

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "failed"
    assert final.error_class == "DeadlineExceeded"
    assert final.finished_at is not None
