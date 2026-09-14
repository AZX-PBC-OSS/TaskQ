"""The uncaught cooperative-exit style of ``check_cancelled()`` through
``run_until_drained``.

The sibling pins (``tests/test_stub_context_actor_surface.py``) exercise
the method surface with the stub *catching* what it raises or calling it
on a quiet dispatch. But the documented purpose of ``check_cancelled()``
is "Convenience for cooperative exit inside actor loops"
(``docs/guides/actors.md``) — the actor calls it bare and lets the
:class:`asyncio.CancelledError` propagate; the system, not the actor,
ends the job. The canonical documented actor uses exactly that style
(``tests/e2e/actors.py``: ``ctx.check_cancelled()`` bare at each stage
boundary), and the production e2e pins its observable outcome
(``tests/e2e/test_cancellation.py::test_cancel_long_running_job``): the
job reaches terminal ``cancelled`` and the worker keeps running — a
cooperative cancel of one job never stops the worker.

This file pins the same observable through the in-memory runner. The
consumer half is shared production code (``consume_one_job`` marks the
row cancelled on the CancelledError path), so the row half must hold;
the runner half is what this pin guards: ``run_until_drained`` is the
in-memory stand-in for "the worker runs until the queue drains", so a
cooperative cancel must be absorbed there exactly as the production
TaskGroup absorbs it — the drain returns, the cancelled job stays
cancelled, and the jobs behind it still run.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_uncaught_cooperative_cancel_drains_the_runner_and_serves_the_rest() -> None:
    """A stub using the documented uncaught cooperative-exit style —
    ``ctx.check_cancelled()`` bare, the raise propagating — must be
    exercisable through ``run_until_drained``: the job ends terminal
    ``cancelled``, the drain returns to the caller instead of raising,
    and a job queued behind the cancelled one still runs. Production
    ground truth for every one of those observables is the e2e
    cancellation suite: the cooperative raise ends the job cancelled and
    the worker continues dispatching."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def cooperative(payload: object, ctx: object) -> object:
        ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is the documented uncaught cooperative-exit style — the raise is the point.
        return {"unreachable": True}

    def follower(payload: object, ctx: object) -> object:
        return {"ok": True}

    backend.register_stub("cooperative", cooperative)
    backend.register_stub("follower", follower)
    cancelled_args = EnqueueArgs(
        id=new_job_id(),
        actor="cooperative",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    follower_args = EnqueueArgs(
        id=new_job_id(),
        actor="follower",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START + timedelta(seconds=1),
    )
    cancel_event = asyncio.Event()
    cancel_event.set()
    backend.register_cancel_event(cancelled_args.id, cancel_event)
    await backend.enqueue(cancelled_args)
    await backend.enqueue(follower_args)

    drain_escaped: BaseException | None = None
    try:
        await backend.run_until_drained()
    except asyncio.CancelledError as exc:
        drain_escaped = exc

    cancelled_row = await backend.get(cancelled_args.id)
    assert cancelled_row is not None
    assert cancelled_row.status == "cancelled", (
        "the cooperative raise must end the job terminal cancelled, as the "
        "shared consumer path does in production; "
        f"got status={cancelled_row.status} error_class={cancelled_row.error_class}"
    )
    assert drain_escaped is None, (
        "run_until_drained must absorb a cooperative actor cancellation, not "
        "raise asyncio.CancelledError to the caller: in production the "
        "cancel-then-reraise is absorbed at the worker task boundary and the "
        "worker keeps running, so a stub using the documented uncaught "
        "check_cancelled() style must be exercisable through the runner — "
        "an escaping CancelledError is the self-misattributing crash the "
        "harness exists to prevent"
    )
    follower_row = await backend.get(follower_args.id)
    assert follower_row is not None
    assert follower_row.status == "succeeded", (
        "a cooperative cancel of one job must not stop the runner serving "
        "the jobs behind it — the production worker continues dispatching "
        f"after a cooperative cancel; got status={follower_row.status}"
    )
