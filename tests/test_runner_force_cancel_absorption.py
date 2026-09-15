"""The in-memory runner must clean up after force-cancelling itself.

In production, phase 2 of cancellation (both graces elapsed) hard-cancels only
the offending job's attempt task while a separate dispatch loop keeps serving
the queue. The in-memory single-task runner has no such separation: each
dispatch registers ``asyncio.current_task()`` as the backend's inflight
attempt, so the phase-2 force-cancel targets the drain task itself.

That makes ``run_until_drained`` both the requester and the consumer of a
cancellation, and asyncio's contract for that situation has two halves. The
``CancelledError`` must be absorbed rather than surfaced to a caller who never
asked for it, and the self-inflicted ``task.cancel()`` must be balanced with
``task.uncancel()`` so the task's cancel count returns to where it started.
Skipping the second half is not cosmetic: an elevated ``cancelling()`` count
follows the task forever, so a surrounding ``asyncio.TaskGroup`` re-raises at
its next checkpoint even though every child succeeded, and repeated drains on
the same task push the count higher until an unrequested cancellation surfaces
somewhere unrelated. CPython's own ``asyncio.timeout`` and ``TaskGroup`` call
``uncancel()`` after absorbing a cancellation they injected, for exactly this
reason.

Absorption alone is not the whole contract. Production's dispatch loop
survives cancelling one attempt task and goes on to claim the next row, so
the runner must likewise resume dispatching: a job queued behind the
force-cancelled one still runs. A runner that swallows the cancellation but
stops draining turns a stuck job into silently skipped work.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_GRACE = timedelta(seconds=30)


async def _force_cancel_stubborn_job(
    backend: InMemoryBackend, clock: FakeClock, job_id: object
) -> None:
    """Drive a job through cancel-request -> both-grace escalation so
    ``tick_cancel_polling`` reaches the phase-2 force-cancel arm."""
    cancel_event = asyncio.Event()
    backend.register_cancel_event(job_id, cancel_event)  # type: ignore[arg-type]
    await backend.write_cancel_request(job_id, None)  # type: ignore[arg-type]
    await backend.tick_cancel_polling()  # observe: fires cooperative event
    clock.advance(_GRACE + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past cancellation grace: phase-2 escalation
    clock.advance(_GRACE + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past both graces: mark abandoned + force-cancel


async def test_drain_leaves_callers_cancel_count_unchanged_after_absorbing_force_cancel() -> None:
    """After ``run_until_drained`` absorbs the force-cancel it inflicted on
    its own task, the caller's ``cancelling()`` counter must be back to its
    pre-drain value and an ordinary subsequent await must not raise.

    The drain is awaited directly in the current task rather than through
    ``asyncio.create_task``, which is what makes the leak observable: the task
    that requested the cancellation is the same task that has to live with a
    cancel count that was never undone.
    """
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    stubborn_started = asyncio.Event()

    async def stubborn(payload: object, ctx: object) -> None:
        stubborn_started.set()
        # Non-cooperative: never reads ctx.cancel_event.
        await asyncio.sleep(3600.0)

    backend.register_stub("stubborn_uncancel", stubborn)

    job_a = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_a,
            actor="stubborn_uncancel",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )

    current_task = asyncio.current_task()
    assert current_task is not None
    cancelling_before = current_task.cancelling()

    # A concurrent helper drives the cancel-escalation ticks once the job is
    # actually running, racing against run_until_drained's own await of the
    # stubborn actor (which never yields in a way that would let the
    # escalation driver run inline). It runs as a background task so the
    # drain, awaited directly below in THIS task, can be force-cancelled.
    async def escalate_once_running() -> None:
        await asyncio.wait_for(stubborn_started.wait(), timeout=2.0)
        # Give run_until_drained's inline await a chance to actually park on
        # the stubborn actor's sleep before escalating.
        await asyncio.sleep(0)
        await _force_cancel_stubborn_job(backend, clock, job_a)

    escalator = asyncio.ensure_future(escalate_once_running())
    drain_escaped: BaseException | None = None
    try:
        await backend.run_until_drained()
    except asyncio.CancelledError as exc:
        # Recorded rather than re-raised, because the cancel-count assertion
        # below is the point of this test: a fix that stops the escape by
        # swallowing unconditionally, without calling uncancel(), still
        # leaves the count elevated.
        drain_escaped = exc
    finally:
        if not escalator.done():
            escalator.cancel()
        else:
            # Surface any error raised inside the escalation helper.
            escalator.result()

    row_a = await backend.get(job_a)
    assert row_a is not None
    assert row_a.status == "abandoned", (
        f"setup: the job must be abandoned after both graces; got {row_a.status!r}"
    )

    assert drain_escaped is None, (
        "run_until_drained must absorb the force-cancel it inflicted on its "
        "own task rather than let CancelledError escape to a caller who "
        f"never requested one. CancelledError escaped: {drain_escaped!r}"
    )

    cancelling_after = current_task.cancelling()
    assert cancelling_after == cancelling_before, (
        "The runner's self-inflicted force-cancel of its own task must be "
        "balanced with task.uncancel() once absorbed, the way asyncio.timeout "
        "and asyncio.TaskGroup balance every cancel() they inject. The "
        f"caller's task.cancelling() went from {cancelling_before} to "
        f"{cancelling_after} and stays elevated for every subsequent await "
        "on this task."
    )

    # Prove the leak is not just cosmetic: a subsequent plain await on this
    # same task must not spuriously raise CancelledError now that the
    # force-cancel episode is over and nothing new was requested.
    try:
        await asyncio.sleep(0)
    except asyncio.CancelledError:
        raise AssertionError(
            "A leftover elevated cancelling() count on the caller's task made "
            "an ordinary subsequent await raise CancelledError with no new "
            "cancellation ever requested on this task."
        ) from None


async def test_drain_awaited_inline_still_runs_the_job_queued_behind_a_forced_one() -> None:
    """Absorbing the force-cancel is only half the contract: the drain must
    also keep going and serve the work queued behind the abandoned job.

    Production cancels only the offending job's attempt task while the
    dispatch loop survives and claims the next row. The in-memory runner
    registers the drain task itself as the inflight attempt, so the same
    force-cancel lands on the drain. Absorbing it without resuming dispatch
    would leave the following job silently unrun, which is the failure mode
    that makes a test suite pass while the queue behind a stuck job stalls.

    Awaiting the drain inline in the current task is the strict shape: the
    task that requested the cancellation is the one that has to carry on
    dispatching afterwards.
    """
    clock = FakeClock(_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    stubborn_started = asyncio.Event()

    async def stubborn(payload: object, ctx: object) -> None:
        stubborn_started.set()
        # Non-cooperative: never reads ctx.cancel_event.
        await asyncio.sleep(3600.0)

    def follower(payload: object, ctx: object) -> object:
        return {"ok": True}

    backend.register_stub("absorb_stubborn", stubborn)
    backend.register_stub("absorb_follower", follower)

    job_a = new_job_id()
    job_b = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_a,
            actor="absorb_stubborn",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    await backend.enqueue(
        EnqueueArgs(
            id=job_b,
            actor="absorb_follower",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START + timedelta(seconds=1),
        )
    )

    async def escalate_once_running() -> None:
        await asyncio.wait_for(stubborn_started.wait(), timeout=2.0)
        await asyncio.sleep(0)
        await _force_cancel_stubborn_job(backend, clock, job_a)

    escalator = asyncio.ensure_future(escalate_once_running())
    drain_escaped: BaseException | None = None
    try:
        await backend.run_until_drained()
    except asyncio.CancelledError as exc:
        drain_escaped = exc
    finally:
        if not escalator.done():
            escalator.cancel()
        else:
            escalator.result()

    row_a = await backend.get(job_a)
    assert row_a is not None
    assert row_a.status == "abandoned", (
        f"setup: the job must be abandoned after both graces; got {row_a.status!r}"
    )

    assert drain_escaped is None, (
        "run_until_drained must absorb the force-cancel it inflicted on its "
        f"own task rather than surface it to the caller. Escaped: {drain_escaped!r}"
    )

    row_b = await backend.get(job_b)
    assert row_b is not None
    assert row_b.status == "succeeded", (
        "A job queued behind a force-cancelled job must still run: the drain "
        "continues after the abandoned attempt ends, the way a production "
        "worker keeps dispatching once it has cancelled only the offending "
        f"attempt task. Job B ended at status={row_b.status!r} instead."
    )
