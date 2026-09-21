"""A job that is merely slow must survive the deploy that interrupts it.

Shutdown escalates: the running job is asked to stop, then force-cancelled, and
once the cancellation and cleanup grace periods have both elapsed the worker
writes a terminal state for it. That state carries no retry, so nothing
re-dispatches the job and nothing re-computes its work.

The job that reaches that last phase has done nothing wrong. It is not stuck,
not looping, not failing: it is taking longer than the two grace periods
allow, which at the shipped defaults is under a minute. Any job legitimately
longer than that - a large export, a batch of remote calls, a slow migration
step - is destroyed by every rolling deploy that lands on it, and a fleet
deploys constantly.

What makes it expensive is that it is silent and it is biased. Nothing failed,
so nothing alerts; the row records ``abandoned`` with no error, so an operator
reading it cannot tell whether the work mattered. And the jobs selected for
destruction are exactly the long ones - the expensive work, the work most
costly to lose and least likely to be re-submitted by whatever enqueued it.

A worker restart is an operational event the queue chooses to have, not a
verdict on the job. The contract pinned here is that it costs the job nothing
permanent: after the deploy the work is back with the fleet and a surviving pod
can run it. Whether the interrupted attempt is refunded or spent is a separate
question - the assertions below allow either, and pin only that the work
survives and stays runnable.

These run real pods against one schema, because the defect only exists in the
interaction between a pod's shutdown and the fleet that outlives it.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobId
from taskq.context import JobContext
from taskq.testing.assertions import wait_for
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_QUEUE = "fleet_slowjob_q"
_ACTOR = "fleet_slowjob_actor"

#: Grace periods short enough that the escalation completes within a test, and
#: an actor slower than both of them together. The ratio is what matters: this
#: is the shipped shape (a job outliving cancellation + cleanup grace), not a
#: pathological one.
_IMPATIENT_SHUTDOWN = {
    "cancellation_grace_period": "0.5",
    "cleanup_grace_period": "0.5",
}

#: Every state that ends a job's life without another dispatch. ``succeeded``
#: is absent deliberately: a job that finished its work before the deploy
#: landed is the one outcome that costs nothing.
_TERMINAL_WITHOUT_RETRY = frozenset({"failed", "crashed", "abandoned", "cancelled"})


async def _status_of(fleet: Fleet, job_id: JobId) -> str:
    rows = await fleet.fetch('SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    return str(rows[0]["status"])


async def test_a_job_slower_than_the_grace_periods_survives_a_deploy(
    pg_dsn: str,
) -> None:
    """A slow job interrupted by a restart is still the fleet's to run.

    The pod is stopped while the actor is mid-work and stays busy past both
    grace periods - the ordinary case of a long job meeting a rolling deploy.
    Afterwards the job must be recoverable by the surviving fleet, not written
    off: nothing about being slow is a reason to discard work permanently.
    """
    schema = f"fleet_slowjob_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("departing", "survivor"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_IMPATIENT_SHUTDOWN,
    ) as fleet:
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=3)
        job_id = job_ids[0]

        departing = fleet.pod("departing")
        claimed = await departing.claim([_QUEUE], 1)
        assert len(claimed) == 1, (
            "the scenario requires the departing pod to be holding the job when "
            "the deploy reaches it"
        )

        running = asyncio.Event()

        async def slow_work(_payload: FleetPayload, _ctx: JobContext[FleetPayload]) -> object:
            """Work that outlives both grace periods without misbehaving.

            The sleep is the thing under test - a job legitimately longer than
            the shutdown allows - and it is bounded by the shutdown itself.
            """
            running.set()
            await asyncio.sleep(30)
            return {"done": True}

        attempt = asyncio.create_task(
            departing.run(claimed[0], slow_work, actor_config=fleet_actor_config())
        )
        await wait_for(running, timeout=5.0, description="the actor started its work")

        # The deploy arrives. The pod drains, cancels, and escalates through
        # every phase while the actor is still working.
        await fleet.stop_pod("departing", graceful=True)
        attempt.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await attempt

        status = await _status_of(fleet, job_id)
        assert status not in _TERMINAL_WITHOUT_RETRY, (
            f"a job that was merely slower than the grace periods "
            f"({_IMPATIENT_SHUTDOWN}) was left {status!r} by a routine restart. "
            f"That state is terminal and carries no retry, so the work is "
            f"discarded and never re-dispatched - and the jobs this selects are "
            f"the long ones, the expensive work an operator is least able to "
            f"afford losing. Nothing failed, so nothing alerts; the row carries "
            f"no error explaining why the work stopped. A worker restart is an "
            f"event the fleet chooses to have, not a verdict on the job"
        )


async def test_the_surviving_fleet_can_run_the_interrupted_job(
    pg_dsn: str,
) -> None:
    """The work actually completes after the deploy that interrupted it.

    Surviving as a row is not enough - the point is that the work happens. A
    pod that was not part of the deploy must be able to claim the job and run
    it to completion, which is what makes the restart cost nothing but time.
    """
    schema = f"fleet_slowjob_run_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("departing", "survivor"),
        actors=((_ACTOR, _QUEUE),),
        settings_overrides=_IMPATIENT_SHUTDOWN,
    ) as fleet:
        job_ids = await fleet.enqueue(1, actor=_ACTOR, queue=_QUEUE, max_attempts=3)
        job_id = job_ids[0]

        departing = fleet.pod("departing")
        claimed = await departing.claim([_QUEUE], 1)
        assert len(claimed) == 1, "the scenario requires the departing pod to be holding the job"

        running = asyncio.Event()

        async def slow_work(_payload: FleetPayload, _ctx: JobContext[FleetPayload]) -> object:
            running.set()
            await asyncio.sleep(30)
            return {"done": True}

        attempt = asyncio.create_task(
            departing.run(claimed[0], slow_work, actor_config=fleet_actor_config())
        )
        await wait_for(running, timeout=5.0, description="the actor started its work")

        await fleet.stop_pod("departing", graceful=True)
        attempt.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await attempt

        # Everything the fleet does on its own to recover orphaned work: the
        # lease the departed pod can no longer renew is aged out, the reclaim
        # sweep runs, and anything it defers is promoted. If the job is
        # recoverable at all, it is recoverable after this.
        await fleet.fetch(
            'UPDATE "{schema}".jobs '
            "SET lock_expires_at = clock_timestamp() - interval '1 second' "
            "WHERE id = $1",
            job_id,
        )
        survivor = fleet.pod("survivor")
        await survivor.backend.reclaim_expired_locks(timedelta(seconds=0), timedelta(seconds=0))
        await survivor.backend.scheduled_to_pending()

        reclaimed = await survivor.claim([_QUEUE], 1)
        assert [row.id for row in reclaimed] == [job_id], (
            f"the surviving pod could not claim the interrupted job; it reads as "
            f"{await _status_of(fleet, job_id)!r}. The deploy has taken the work "
            f"out of the fleet's reach permanently, so it will never run"
        )

        async def quick_work(_payload: FleetPayload, _ctx: JobContext[FleetPayload]) -> object:
            return {"done": True}

        await survivor.run(reclaimed[0], quick_work, actor_config=fleet_actor_config())

        final = await _status_of(fleet, job_id)
        assert final == "succeeded", (
            f"the interrupted job did not complete on the surviving pod, ending "
            f"{final!r} instead. A restart must cost the work time, not the work "
            f"itself"
        )
