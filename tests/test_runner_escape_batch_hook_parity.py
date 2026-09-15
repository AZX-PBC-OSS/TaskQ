"""The runner's escape paths apply the batch hook, mirroring production's
escape handlers — a batch whose last member ends through an escape
completes immediately.

Production's ``dispatch_one_job`` applies
:func:`apply_batch_terminal_outcome` on the clean return from
``consume_one_job`` and on every escape, best-effort before the
re-raise/return: the CancelledError handler calls the hook with
``cancelled`` before re-raising, and the generic-exception handler calls
it with ``_handle_generic_exception``'s terminal outcome (a handler
whose terminal write infra-failed leaves the row RUNNING and stays
hook-silent — the sweep is the recovery, and no batch counter may budge
on a non-terminal write). The batch-completion check runs on ANY terminal
member (including those that were discarded), so a batch whose last
member ends through an escape completes immediately — never
sweep-deferred on a normal flow.

The runner's escape mirrors (``PayloadValidationError``, the
cooperative-cancel absorb in ``run_until_drained``, and the phase-2
force-cancel that lands on the drain task itself) hold the same
contract: the escape sets the terminal outcome and falls through to the
shared hook call. These pins hold it — the member's row reaches its
terminal state AND the batch row reaches ``complete``; re-skipping the
hook on any mirror strands the batch ``active`` until the stale-batch
sweep and turns these pins red.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_cooperative_cancel_escape_completes_the_batch() -> None:
    """A cooperatively-cancelled batch member reaches terminal
    ``cancelled`` and its batch completes on the escape itself — the
    runner's absorb mirror falls through to the shared batch hook with
    ``cancelled``, exactly as production's CancelledError handler
    applies the hook best-effort before its re-raise. The
    batch-completion check runs on any terminal member, discarded
    included. Re-skipping the hook call on the absorb path strands the
    batch ``active`` until the stale-batch sweep and turns this pin red."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    batch_id = new_uuid()
    await backend.create_batch(batch_id, "default", 1, None, None, None)

    def cooperative(payload: object, ctx: object) -> object:
        ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the bare cooperative-exit style is the documented contract under pin.
        return {"unreachable": True}

    backend.register_stub("cooperative", cooperative)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="cooperative",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"batch_id": str(batch_id)},
    )
    cancel_event = asyncio.Event()
    cancel_event.set()
    backend.register_cancel_event(args.id, cancel_event)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "cancelled", f"the member must reach terminal cancelled; got {row.status}"
    batch = await backend.get_batch(batch_id)
    assert batch is not None
    assert batch.status == "complete", (
        "the escape must apply the batch hook: the batch-completion check "
        "runs on any terminal member, so a batch whose last member ends "
        f"through the escape completes immediately — got {batch.status}"
    )


async def test_payload_validation_escape_completes_the_batch() -> None:
    """A pre-actor payload-validation failure reaches terminal ``failed``
    and its batch completes on the escape itself — the runner's
    validation mirror falls through to the shared batch hook with
    ``failed``, mirroring production's generic-exception escape, where
    the handler's terminal outcome reaches the hook. Re-skipping the
    hook call on the validation escape strands the batch ``active``
    until the stale-batch sweep and turns this pin red."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    batch_id = new_uuid()
    await backend.create_batch(batch_id, "default", 1, None, None, None)

    def never_runs(payload: object, ctx: object) -> object:
        raise AssertionError("actor body must not run on validation failure")

    class StrictPayload(BaseModel):
        name: str

    backend.register_stub("strict_actor", never_runs, payload_type=StrictPayload)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="strict_actor",
        queue="default",
        payload={"wrong_field": "nope"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"batch_id": str(batch_id)},
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "PayloadValidationError"
    batch = await backend.get_batch(batch_id)
    assert batch is not None
    assert batch.status == "complete", (
        "the escape must apply the batch hook: the batch-completion check "
        "runs on any terminal member, so a batch whose last member ends "
        f"through the escape completes immediately — got {batch.status}"
    )


async def test_force_cancelled_member_completes_the_batch_and_the_drain_continues() -> None:
    """A batch member force-cancelled after both cancel graces elapse
    reaches terminal ``abandoned``, its batch completes on that escape,
    and the drain keeps serving the queue behind it.

    This is the third escape mirror, and the one production separates by
    construction: the worker's phase-2 escalation cancels only the
    offending job's attempt task while the dispatch loop lives on, so the
    batch hook still runs for the abandoned member and the next job is
    still claimed. The in-memory runner awaits each attempt inline in the
    drain task, so phase 2 targets the drain itself; absorbing that
    self-inflicted cancellation is what keeps the two backends
    observably equivalent. Letting it escape instead diverges twice over
    — the batch strands ``active`` until the stale-batch sweep, and every
    job queued behind the abandoned one silently never runs, which in a
    test suite reads as a passing drain over work that was dropped.

    The abandoned row is written by the escalation tick before the
    cancellation is delivered, so the absorbing path must apply the batch
    hook without issuing a second terminal write over it.
    """
    grace = timedelta(seconds=30)
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(
        clock=clock,
        cancellation_grace_period=grace,
        cleanup_grace_period=grace,
    )
    batch_id = new_uuid()
    await backend.create_batch(batch_id, "default", 2, None, None, None)

    async def stubborn(payload: object, ctx: object) -> None:
        # Non-cooperative: never reads the cancel event, so only the
        # phase-2 force-cancel can end this attempt.
        await asyncio.sleep(3600.0)

    def follower(payload: object, ctx: object) -> object:
        return {"ok": True}

    backend.register_stub("stubborn_member", stubborn)
    backend.register_stub("follower_member", follower)

    stubborn_args = EnqueueArgs(
        id=new_job_id(),
        actor="stubborn_member",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"batch_id": str(batch_id)},
    )
    follower_args = EnqueueArgs(
        id=new_job_id(),
        actor="follower_member",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START + timedelta(seconds=1),
        metadata={"batch_id": str(batch_id)},
    )
    await backend.enqueue(stubborn_args)
    await backend.enqueue(follower_args)

    drain_task = asyncio.create_task(backend.run_until_drained())

    # Bounded real-time probe: escalate only once the stubborn member is
    # genuinely running, otherwise the cancel request races the claim.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 2.0
    while True:
        row = await backend.get(stubborn_args.id)
        if row is not None and row.status == "running":
            break
        assert loop.time() < deadline, "setup: the runner never dispatched the stubborn member"
        await asyncio.sleep(0.02)

    cancel_event = asyncio.Event()
    backend.register_cancel_event(stubborn_args.id, cancel_event)
    await backend.write_cancel_request(stubborn_args.id, None)
    await backend.tick_cancel_polling()  # fires the cooperative event
    clock.advance(grace + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past the cancellation grace
    clock.advance(grace + timedelta(seconds=1))
    await backend.tick_cancel_polling()  # past both graces: abandon + force-cancel

    drain_escaped: BaseException | None = None
    try:
        await asyncio.wait_for(drain_task, timeout=2.0)
    except asyncio.CancelledError as exc:
        drain_escaped = exc
    except TimeoutError:
        drain_task.cancel()
        raise AssertionError(
            "setup: the drain neither completed nor raised within budget"
        ) from None

    stubborn_row = await backend.get(stubborn_args.id)
    assert stubborn_row is not None
    assert stubborn_row.status == "abandoned", (
        "setup: the force-cancelled member must be abandoned after both "
        f"graces; got {stubborn_row.status}"
    )

    assert drain_escaped is None, (
        "the force-cancel escape must be absorbed: production cancels only "
        "the abandoned job's attempt task and its dispatch loop keeps "
        "running, so the in-memory mirror must not surface a "
        f"CancelledError its caller never requested — got {drain_escaped!r}"
    )

    follower_row = await backend.get(follower_args.id)
    assert follower_row is not None
    assert follower_row.status == "succeeded", (
        "a job queued behind a force-cancelled job must still run — "
        "production keeps dispatching after cancelling one attempt task, "
        f"so the mirror must too; got {follower_row.status}"
    )

    batch = await backend.get_batch(batch_id)
    assert batch is not None
    assert batch.status == "complete", (
        "the force-cancel escape must apply the batch hook like the other "
        "two escapes: the batch-completion check runs on any terminal "
        "member, abandoned included, so skipping the hook here strands the "
        f"batch active until the stale-batch sweep — got {batch.status}"
    )
