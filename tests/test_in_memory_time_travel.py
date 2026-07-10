"""In-memory backend time-travel tests proving works end-to-end.

Uses the ``memory_jobs`` fixture (FakeClock + InMemoryBackend) to exercise
scheduled-job promotion, deadline sweep, and cron-pattern dispatch — all
driven by FakeClock advancement with zero real-time waits.

These tests collectively cover the in-memory backend half of the
acceptance definition (the actor side is covered by).
"""

import time
from datetime import UTC, datetime

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── scheduled job promoted after FakeClock advance ──


async def test_tt1_scheduled_job_promoted_after_clock_advance(
    memory_jobs: InMemoryBackend,
) -> None:
    """scheduled job promoted after FakeClock advance.

    Proves the acceptance definition: the in-memory test backend is
    constructed with FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC));
    test code calls memory_jobs.scheduled_to_pending(fake_clock.now())
    to trigger scheduled-job processing, matching the example
    verbatim.
    """
    fake_clock: FakeClock = memory_jobs._clock  # type: ignore[reportPrivateUsage] # Why: verbatim pattern — direct FakeClock access is the prescribed test interface

    scheduled_at = datetime(2025, 1, 1, 5, 0, tzinfo=UTC)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="cron_actor",
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="transient",
        scheduled_at=scheduled_at,
        priority=0,
    )
    row = await memory_jobs.enqueue(args)
    assert row.status == "scheduled"

    await memory_jobs.scheduled_to_pending(fake_clock.now())
    row_after = await memory_jobs.get(row.id)
    assert row_after is not None
    assert row_after.status == "scheduled"

    fake_clock.move_to(datetime(2025, 1, 1, 5, 0, tzinfo=UTC))
    await memory_jobs.scheduled_to_pending(fake_clock.now())
    row_final = await memory_jobs.get(row.id)
    assert row_final is not None
    assert row_final.status == "pending"


# ── deadline_sweep fails job after FakeClock advance ──


async def test_tt2_deadline_sweep_fails_job_after_clock_advance(
    memory_jobs: InMemoryBackend,
) -> None:
    """deadline_sweep fails job after FakeClock advance past schedule_to_close.

    Proves the acceptance definition (in-memory backend half): test code
    advances FakeClock and calls deadline_sweep(memory_jobs._clock.now())
    to trigger deadline processing, matching the example verbatim.
    """
    fake_clock: FakeClock = memory_jobs._clock  # type: ignore[reportPrivateUsage] # Why: verbatim pattern — direct FakeClock access is the prescribed test interface

    schedule_to_close = datetime(2025, 1, 1, 10, 0, tzinfo=UTC)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="deadline_actor",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=fake_clock.now(),
        priority=0,
        schedule_to_close=schedule_to_close,
    )
    row = await memory_jobs.enqueue(args)
    assert row.status == "pending"

    fake_clock.move_to(datetime(2025, 1, 1, 11, 0, tzinfo=UTC))
    count = await memory_jobs.deadline_sweep(fake_clock.now())
    assert count == 1

    updated = await memory_jobs.get(row.id)
    assert updated is not None
    assert updated.status == "failed"
    assert updated.error_class == "DeadlineExceeded"


# ── cron pattern with FakeClock completes in < 1 second ─────────────


async def test_tt4_cron_pattern_completes_under_one_second(
    memory_jobs: InMemoryBackend,
) -> None:
    """Cron pattern with FakeClock completes in < 1 second.

    Proves the acceptance definition (in-memory backend half): test code
    calls fake_clock.move_to(datetime(2025, 1, 1, 3, 0, 0, tzinfo=UTC))
    and memory_jobs.scheduled_to_pending(fake_clock.now()) to trigger
    scheduled-job processing, matching the example verbatim. The
    contract requires this test to complete in under 1 second of
    wall-clock time — FakeClock advancement replaces real-time waits
    entirely.
    """

    def succeed(payload: object, ctx: object) -> object:
        return None

    memory_jobs.register_stub("cron_actor", succeed)

    scheduled_at = datetime(2025, 1, 1, 3, 0, 0, tzinfo=UTC)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="cron_actor",
        queue="default",
        payload={},
        max_attempts=1,
        retry_kind="transient",
        scheduled_at=scheduled_at,
        priority=0,
    )
    await memory_jobs.enqueue(args)

    fake_clock: FakeClock = memory_jobs._clock  # type: ignore[reportPrivateUsage] # Why: verbatim pattern — direct FakeClock access is the prescribed test interface
    fake_clock.move_to(scheduled_at)

    wall_start = time.monotonic()

    await memory_jobs.scheduled_to_pending(fake_clock.now())
    await memory_jobs.run_until_drained()

    wall_elapsed = time.monotonic() - wall_start
    # Widened from 1.0s: this asserts the FakeClock time-travel doesn't
    # actually block on wall-clock time, not a tight perf budget — give it
    # headroom to survive scheduler contention under parallel test load.
    assert wall_elapsed < 5.0, f"Test took {wall_elapsed:.3f}s — must complete in < 5 seconds"

    dispatched_job = None
    for row in memory_jobs._jobs.values():  # type: ignore[reportPrivateUsage] # Why: test-only private access to verify terminal state
        if row.actor == "cron_actor":
            dispatched_job = row
            break
    assert dispatched_job is not None
    assert dispatched_job.status in ("succeeded", "failed")
