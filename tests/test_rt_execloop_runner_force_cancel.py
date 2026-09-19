"""Red-team: the in-memory runner has no force-cancel seam (E4).

Contract under attack: after both cancel graces elapse, the in-memory test
runner must TERMINATE a non-cooperative attempt - ``run_until_drained``
completes - mirroring production phase 2, which hard-cancels the actor task
(``active.task.cancel()`` at src/taskq/worker/cancel.py:287).

Hypothesis (verified against the current tree): the runner's
``tick_cancel_polling`` (src/taskq/testing/_runner.py:523-582) only fires the
cooperative cancel event, escalates the row's ``cancel_phase`` to 2, and
marks the row ``abandoned`` - no task cancellation exists anywhere under
``taskq/testing/``. A never-cooperative stub therefore hangs
``run_until_drained`` forever while the row already says abandoned.

The bounded ``asyncio.wait_for`` around the drain task IS the proof: today it
times out (the hang), when the runner gains the production-parity cancel it
completes.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


async def test_runner_terminates_noncooperative_attempt_after_force_grace() -> None:
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    async def stubborn(payload: object, ctx: object) -> None:
        # ctx is the runner's duck-typed stub context; this stub pins the
        # NON-cooperative shape - it never reads ctx.cancel_event, exactly
        # like a hung actor that no cooperative signal can reach.
        await asyncio.sleep(3600.0)

    backend.register_stub("rt_stubborn", stubborn)
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="rt_stubborn",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    drain_task = asyncio.create_task(backend.run_until_drained())

    # Bounded real-time probe: the runner must dispatch and start the attempt
    # (row running) before a cancel request can attach.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while True:
        row = await backend.get(job_id)
        if row is not None and row.status == "running":
            break
        assert loop.time() < deadline, "setup: the runner never dispatched the job"
        await asyncio.sleep(0.02)

    cancel_event = asyncio.Event()
    backend.register_cancel_event(job_id, cancel_event)
    await backend.write_cancel_request(job_id, None)
    await backend.tick_cancel_polling()  # first observation: cooperative event fires
    assert cancel_event.is_set(), "setup: first tick must fire the cooperative event"
    clock.advance(_GRACE + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past cancellation grace: phase-2 escalation
    clock.advance(_GRACE + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past both graces: row marked abandoned

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "abandoned", (
        f"row-level escalation parity: after both graces the row must say "
        f"abandoned (it does today); got status={row.status!r}"
    )

    # DESIRED: the runner terminates the attempt - the drain task completes
    # within a bounded real-time budget.
    terminated: bool
    try:
        await asyncio.wait_for(drain_task, timeout=2.0)
        terminated = True
    except asyncio.CancelledError:
        # The runner ended its own drain task to kill the attempt - a
        # termination shape, not a hang.
        terminated = True
    except TimeoutError:
        terminated = False
        with contextlib.suppress(asyncio.CancelledError):
            await drain_task  # let the cancelled drain unwind before asserting
    assert terminated, (
        "CONTRACT: after both cancel graces the in-memory runner must TERMINATE "
        "the attempt (run_until_drained completes), mirroring production phase 2 "
        "which hard-cancels the actor task (src/taskq/worker/cancel.py:287 "
        "`active.task.cancel()`). VIOLATION: tick_cancel_polling "
        "(src/taskq/testing/_runner.py:523-582) only sets the cooperative "
        "event, escalates the row's cancel_phase, and marks the row abandoned - "
        "no task cancellation exists anywhere under taskq/testing/ - so the "
        "non-cooperative stub (asyncio.sleep(3600)) hangs run_until_drained "
        "forever while the row already says abandoned; this wait_for timeout "
        "is the proof of the hang."
    )
