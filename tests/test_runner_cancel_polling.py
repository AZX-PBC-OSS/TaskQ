"""The opt-in ``cancel_polling`` flag on ``run_until_drained``.

Drives the documented cancellation recipe in one move: register the
cancel event, pass ``cancel_polling=True``, and cancel mid-drain. The
drain ticks the cancel poller itself (between iterations and, while an
attempt is in flight, from a concurrent ticker), so a test no longer
needs the three undocumented manual moves (``write_cancel_request`` plus
hand-rolled ``tick_cancel_polling`` calls around clock advances) to
cooperate with the drain.
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_cancel_polling_flag_ends_a_mid_drain_cancel_as_cancelled() -> None:
    """A job cancelled while the drain is executing it ends terminal
    ``cancelled`` when the drain runs with ``cancel_polling=True``: the
    tick fires the registered cooperative event, the stub exits through
    the documented ``check_cancelled()`` style, and the drain returns
    (the cancellation is absorbed, not raised to the caller)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    async def victim(payload: object, ctx: object) -> object:
        await ctx.cancel_event.wait()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the actor parks until the cancel poller fires the event.
        ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: the documented cooperative-exit style.
        return {"unreachable": True}

    backend.register_stub("victim", victim)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="victim",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    cancel_event = asyncio.Event()
    backend.register_cancel_event(args.id, cancel_event)
    await backend.enqueue(args)

    drain_task = asyncio.create_task(backend.run_until_drained(cancel_polling=True))

    # Cancel mid-drain: wait until the victim is genuinely running, then
    # write the cancel request the way a JobsClient.cancel would.
    deadline = asyncio.get_running_loop().time() + 5.0
    while True:
        row = await backend.get(args.id)
        if row is not None and row.status == "running":
            break
        assert asyncio.get_running_loop().time() < deadline, (
            "setup: the runner never dispatched the victim"
        )
        await asyncio.sleep(0.01)
    assert await backend.write_cancel_request(args.id, None) is True

    await asyncio.wait_for(drain_task, timeout=5.0)

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "cancelled", (
        "a mid-drain cooperative cancel under cancel_polling=True must "
        f"end the job terminal cancelled; got status={row.status} "
        f"error_class={row.error_class}"
    )


async def test_default_drain_still_never_ticks_cancel_polling() -> None:
    """Default behaviour is unchanged: without the flag the drain does
    not drive the cancel poller, so a cancel request written mid-drain
    stays phase 1 with its event unfired (the manual-move contract the
    flag exists to replace)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    async def parked(payload: object, ctx: object) -> object:
        await asyncio.sleep(3600.0)
        return {"unreachable": True}

    backend.register_stub("parked", parked)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="parked",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    cancel_event = asyncio.Event()
    backend.register_cancel_event(args.id, cancel_event)
    await backend.enqueue(args)

    drain_task = asyncio.create_task(backend.run_until_drained())

    deadline = asyncio.get_running_loop().time() + 5.0
    while True:
        row = await backend.get(args.id)
        if row is not None and row.status == "running":
            break
        assert asyncio.get_running_loop().time() < deadline, (
            "setup: the runner never dispatched the parked job"
        )
        await asyncio.sleep(0.01)
    assert await backend.write_cancel_request(args.id, None) is True

    # Give a (nonexistent) default tick every chance to run, then assert
    # the event was never fired by the drain itself.
    for _ in range(5):
        await asyncio.sleep(0.01)

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "running"
    assert row.cancel_phase == 1
    assert not cancel_event.is_set(), (
        "the default drain must keep the historical behaviour: no cancel "
        "polling, the registered event stays unfired"
    )

    drain_task.cancel()
    with suppress(asyncio.CancelledError):
        await drain_task
