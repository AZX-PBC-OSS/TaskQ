"""The runner's cooperative-cancel absorb discriminator, at its two mixed
cells.

``run_until_drained``'s per-dispatch ``except asyncio.CancelledError``
classifies the raise by the job's registered cancel event: set → absorb
and keep draining; unset or absent → propagate as the caller's
cancellation of the drain task itself. Two cells of that classification
are pinned by the existing suites - (event set, actor-originated) the
bare ``check_cancelled()`` style, and (no event, caller-originated) the
job-handle timeout suite's cancel-the-drain usage. The two *mixed* cells
are what this file attacks, because production treats them differently
than the discriminator does:

* **Actor-originated, no event** - an actor that raises
  :class:`asyncio.CancelledError` itself (its own cooperative exit
  without ``check_cancelled()``, or a child-task cancellation leaking
  through an await) is indistinguishable, inside
  ``consume_one_job``, from the cooperative style: the same shielded
  mark-cancelled, the same re-raise. Production's
  ``dispatch_one_job`` then sets ``outcome = "cancelled"`` and re-raises
  (``src/taskq/worker/dispatch.py``), the dispatch task boundary
  absorbs the raise, and the worker keeps dispatching - the same
  mechanism that absorbs the documented cooperative style. The runner's
  discriminator classifies it as the caller's cancel and kills the
  drain.

* **Caller-originated, event set** - a caller that requested the cancel,
  watched the actor ignore it, and cancelled the drain task to give up
  has its cancellation absorbed whenever the job's cancel event is set:
  the drain keeps dispatching the jobs behind and returns normally.
  A task whose explicit ``cancel()`` is swallowed is the asyncio
  shutdown-hang antipattern - the external-cancel pin's own docstring
  names it for the no-event arm; the mixed arm swallows exactly that.
  In production the caller-side cancellation always wins - the worker
  stops - so the harness must let the drain task's own cancellation
  take precedence over the per-job event.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_actor_raised_cancelled_error_without_an_event_drains_the_runner() -> None:
    """An actor ending itself with its own ``asyncio.CancelledError`` -
    no registered cancel event, so the raise is actor-originated - must
    be exercisable through ``run_until_drained``: the job ends terminal
    ``cancelled`` (the shared consumer marks it before re-raising,
    identical to the cooperative style), the drain returns instead of
    raising, and the job queued behind it still runs. Production's
    dispatch treats this raise exactly like the cooperative one - same
    consumer path, same task-boundary absorption, worker keeps
    dispatching."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def self_cancelling(payload: object, ctx: object) -> object:
        raise asyncio.CancelledError()

    def follower(payload: object, ctx: object) -> object:
        return {"ok": True}

    backend.register_stub("self_cancelling", self_cancelling)
    backend.register_stub("follower", follower)
    cancelled_args = EnqueueArgs(
        id=new_job_id(),
        actor="self_cancelling",
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
        "the actor-originated raise must end the job terminal cancelled - "
        "the shared consumer's CancelledError path marks it before "
        "re-raising, identical to the cooperative style; "
        f"got status={cancelled_row.status}"
    )
    assert drain_escaped is None, (
        "an actor-originated asyncio.CancelledError with no registered "
        "cancel event is not the caller's cancellation: production's "
        "dispatch re-raises it past the task boundary exactly like the "
        "cooperative style and the worker keeps dispatching, so the "
        "runner must absorb it and keep draining - an escaping "
        "CancelledError kills the drain and the jobs behind it, the "
        "crash class the absorb was added to prevent, one trigger narrower"
    )
    follower_row = await backend.get(follower_args.id)
    assert follower_row is not None
    assert follower_row.status == "succeeded", (
        "an actor-originated cancellation of one job must not stop the "
        "runner serving the jobs behind it - production's worker keeps "
        f"dispatching; got status={follower_row.status}"
    )


async def test_caller_cancellation_of_the_drain_task_wins_over_a_set_cancel_event() -> None:
    """With the job's cancel event set (cancellation requested) and the
    actor ignoring it, the caller cancelling the drain task itself must
    stop the drain: the cancellation propagates out of ``await
    drain_task``, the cancelled job's row is terminal ``cancelled`` (the
    shared consumer's shielded write completed before the re-raise), and
    the job queued behind is left undispatched. An absorb keyed only on
    the event being set swallows the caller's cancellation - the drain
    keeps working past an explicit stop request and returns normally,
    which is swallowed cancellation, the asyncio antipattern the
    external-cancel pin's own docstring names for the no-event arm."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    async def ignoring(payload: object, ctx: object) -> object:
        await asyncio.sleep(100)
        return {"unreachable": True}

    def follower(payload: object, ctx: object) -> object:
        return {"ok": True}

    backend.register_stub("ignoring", ignoring)
    backend.register_stub("follower", follower)
    ignoring_args = EnqueueArgs(
        id=new_job_id(),
        actor="ignoring",
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
    backend.register_cancel_event(ignoring_args.id, cancel_event)
    await backend.enqueue(ignoring_args)
    await backend.enqueue(follower_args)

    drain_task = asyncio.create_task(backend.run_until_drained())
    await asyncio.sleep(0.05)  # let the dispatch reach the ignoring actor
    drain_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await drain_task

    ignored_row = await backend.get(ignoring_args.id)
    assert ignored_row is not None
    assert ignored_row.status == "cancelled", (
        "the shared consumer marks the row cancelled on the "
        "CancelledError path (its shielded write exists so shutdown "
        "cancellation cannot strand the row in running) before the "
        f"runner sees the raise; got status={ignored_row.status}"
    )
    follower_row = await backend.get(follower_args.id)
    assert follower_row is not None
    assert follower_row.status == "scheduled", (
        "the caller's cancellation of the drain task must stop the drain "
        "- the job queued behind the cancelled one stays undispatched; "
        "an absorb that keeps draining after the caller said stop is "
        f"swallowed cancellation; got status={follower_row.status}"
    )
