"""A throttled actor's growing backlog must be attributable to that actor.

The most common queue misconfiguration is an arrival rate above a
throttle: work is submitted at five a second and a concurrency or rate
limit lets one a second through. Nothing is broken - every component is
doing exactly what it was configured to do - and the backlog grows
without bound. The only defect is the configuration, and the only way an
operator finds it is if the queue can say *which* actor is accumulating.

That is the whole difficulty. Total queue depth does not answer it: the
throttled actor usually shares a queue with healthy ones, so the queue's
depth grows while every actor on it looks equally plausible as the
cause. An operator who can see only the queue is reduced to guessing,
and the usual guess - add more pods - cannot help, because the limit is
per-actor and the fleet is already respecting it.

These tests drive the real shape: sustained arrivals, a capacity gate
that admits a fraction of them, and a healthy neighbour on the same
queue. They then require that the backlog and its age are readable per
actor, so the answer to "what is falling behind, and since when" comes
from the system rather than from intuition.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_base62
from tests._fleet import Fleet, FleetPayload, fleet_actor_config, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_throttle_q"
_THROTTLED = "throttled_actor"
_HEALTHY = "healthy_actor"

# Arrivals well above what the gate admits, sustained for several
# rounds: the ratio is the misconfiguration, and the backlog it builds
# is what has to become visible.
_ARRIVALS_PER_ROUND = 5
_ADMITTED_PER_ROUND = 1
_ROUNDS = 6


async def _pending_by_actor(fleet: Fleet, actors: list[str]) -> dict[str, int]:
    """Per-actor backlog, read through the production counting path."""
    return await fleet.any_pod().backend.count_pending_jobs(actors)


async def _oldest_pending_age(fleet: Fleet, actor: str) -> float | None:
    rows = await fleet.fetch(
        "SELECT EXTRACT(EPOCH FROM (now() - min(scheduled_at))) AS age "
        "FROM \"{schema}\".jobs WHERE actor = $1 AND status IN ('pending', 'scheduled')",
        actor,
    )
    age = rows[0]["age"]
    return None if age is None else float(age)


async def _drive_throttled_load(fleet: Fleet) -> None:
    """Arrivals outpacing admission, for several rounds.

    Each round submits ``_ARRIVALS_PER_ROUND`` jobs for the throttled
    actor and lets ``_ADMITTED_PER_ROUND`` of them through, which is the
    misconfiguration under test. The healthy actor's work is submitted
    and fully drained in the same rounds, so the queue carries both a
    growing and a healthy cohort at once - the condition that makes the
    diagnosis hard.
    """

    async def _work(_payload: FleetPayload, _ctx: object) -> str:
        return "done"

    pod = fleet.pod("pod-1")
    for _ in range(_ROUNDS):
        await fleet.enqueue(_ARRIVALS_PER_ROUND, actor=_THROTTLED, queue=_QUEUE)
        await fleet.enqueue(1, actor=_HEALTHY, queue=_QUEUE)

        admitted = 0
        claimed = await pod.claim([_QUEUE], _ARRIVALS_PER_ROUND + 1)
        for job in claimed:
            if job.actor == _THROTTLED:
                if admitted >= _ADMITTED_PER_ROUND:
                    # Over the limit: put it back the way a capacity
                    # denial does, without spending the job's budget.
                    await pod.backend.mark_snoozed(
                        job.id,
                        pod.worker_id,
                        timedelta(seconds=0),
                        outcome="rate_limit_denied",
                        attempt=job.attempt,
                    )
                    continue
                admitted += 1
            await pod.run(job, _work, actor_config=fleet_actor_config())


async def test_a_throttled_actors_backlog_is_visible_as_its_own(
    pg_dsn: str,
) -> None:
    """The actor that is falling behind can be named from the queue's data.

    After sustained over-arrival, the throttled actor holds a backlog
    and its healthy neighbour on the same queue does not. The counting
    surface must distinguish them.

    If it cannot - if the only available number is the queue's total -
    then every actor on a deep queue is equally suspect, and the
    operator's fastest available action, adding pods, cannot fix a
    per-actor limit the fleet is already honouring.
    """
    schema = f"fleet_throttle_vis_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_THROTTLED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        await _drive_throttled_load(fleet)

        depths = await _pending_by_actor(fleet, [_THROTTLED, _HEALTHY])
        throttled_depth = depths.get(_THROTTLED, 0)
        healthy_depth = depths.get(_HEALTHY, 0)

        assert throttled_depth > 0, (
            f"after {_ROUNDS} rounds of {_ARRIVALS_PER_ROUND} arrivals against "
            f"{_ADMITTED_PER_ROUND} admitted, the throttled actor's pending count reads "
            f"{throttled_depth}. The backlog is real - the jobs exist and are waiting - "
            "so a count that does not show it leaves the operator with no evidence of "
            "the condition at all."
        )
        assert throttled_depth > healthy_depth, (
            f"the throttled actor's backlog ({throttled_depth}) is not distinguishable "
            f"from its healthy neighbour's ({healthy_depth}) on the same queue. An "
            "operator looking at this queue cannot tell which actor is accumulating, so "
            "the misconfigured limit cannot be found from the queue's own data."
        )


async def test_the_healthy_actor_on_a_backlogged_queue_reads_as_healthy(
    pg_dsn: str,
) -> None:
    """A deep queue must not make its healthy actors look unhealthy.

    This is the other half of attribution, and the half that wastes an
    operator's time. If the throttled actor's backlog contaminated every
    actor's reading, the investigation would start by examining actors
    that are entirely fine, and the real cause would be found last.
    """
    schema = f"fleet_throttle_clean_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_THROTTLED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        await _drive_throttled_load(fleet)

        depths = await _pending_by_actor(fleet, [_THROTTLED, _HEALTHY])
        assert depths.get(_HEALTHY, 0) == 0, (
            f"the healthy actor reads a backlog of {depths.get(_HEALTHY, 0)} although "
            "every job submitted for it was dispatched and completed in the round it "
            "arrived. Sharing a queue with a throttled actor has made a healthy actor "
            "look like a failing one, and an investigation starting from this data looks "
            "at the wrong actor first."
        )


async def test_a_growing_backlog_carries_its_age(pg_dsn: str) -> None:
    """How long work has been waiting is answerable, not just how much.

    Depth alone cannot separate a queue that is briefly busy from one
    that is permanently behind: both show a number. The age of the
    oldest waiting job is what distinguishes them, and it is what tells
    an operator whether a limit is merely tight or has been wrong since
    a particular deploy.
    """
    schema = f"fleet_throttle_age_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_THROTTLED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        # The oldest waiting job is explicit, so the age below is a
        # known quantity rather than whatever the test run happened to
        # take: this is the row an operator's alert would be keyed on.
        await fleet.enqueue(3, actor=_THROTTLED, queue=_QUEUE)
        await fleet.fetch(
            'UPDATE "{schema}".jobs SET scheduled_at = $1 WHERE actor = $2 RETURNING id',
            datetime.now(UTC) - timedelta(minutes=30),
            _THROTTLED,
        )

        age = await _oldest_pending_age(fleet, _THROTTLED)
        assert age is not None, (
            "a queue holding jobs that have been waiting half an hour reports no oldest "
            "pending age for the actor. Depth alone cannot distinguish a queue that is "
            "briefly busy from one that has been behind since a bad deploy."
        )
        assert age >= 60 * 25, (
            f"the oldest waiting job for this actor reads as {age:.0f}s old; it has been "
            "queued for thirty minutes. An age that under-reports lets a permanently "
            "behind queue stay inside an alert threshold indefinitely."
        )

        healthy_age = await _oldest_pending_age(fleet, _HEALTHY)
        assert healthy_age is None, (
            f"an actor with nothing queued reports an oldest-pending age of "
            f"{healthy_age}. Age must be attributable per actor too, or the throttled "
            "actor's staleness is read as the whole queue being stale."
        )


async def test_the_fleet_respects_the_limit_rather_than_the_arrival_rate(
    pg_dsn: str,
) -> None:
    """Throttling is the system working, and must look different from failure.

    The denied jobs must still be queued, unspent and undamaged: the
    configuration is wrong, but no work may be lost to it. An operator
    who fixes the limit must find the backlog intact and drainable, not
    a pile of jobs that expired while waiting for a slot.

    This is what separates a diagnosable misconfiguration from an
    incident: the evidence is still there, and so is the work.
    """
    schema = f"fleet_throttle_intact_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_THROTTLED, _QUEUE), (_HEALTHY, _QUEUE)),
    ) as fleet:
        await _drive_throttled_load(fleet)

        rows = await fleet.fetch(
            'SELECT status, count(*) AS n FROM "{schema}".jobs WHERE actor = $1 GROUP BY status',
            _THROTTLED,
        )
        by_status = {str(row["status"]): int(row["n"]) for row in rows}
        lost = {
            status: count
            for status, count in by_status.items()
            if status in {"failed", "crashed", "cancelled"}
        }
        assert lost == {}, (
            f"jobs held back by a capacity limit reached terminal states: {lost}. The "
            "limit is a misconfiguration, not a failure of the work - an operator who "
            "corrects it must find the backlog intact and drainable, not destroyed by "
            "the wait."
        )

        # The queue is the limit's backlog, and correcting the limit
        # must drain it: the work is still claimable and still runs.
        await fleet.fetch(
            "UPDATE \"{schema}\".jobs SET scheduled_at = $1, status = 'pending' "
            "WHERE actor = $2 AND status IN ('pending', 'scheduled') RETURNING id",
            datetime.now(UTC) - timedelta(seconds=1),
            _THROTTLED,
        )

        async def _work(_payload: FleetPayload, _ctx: object) -> str:
            return "drained"

        drained = 0
        for _ in range(_ROUNDS * _ARRIVALS_PER_ROUND):
            batch = await fleet.pod("pod-1").claim([_QUEUE], 10)
            if not batch:
                break
            for job in batch:
                await fleet.pod("pod-1").run(job, _work, actor_config=fleet_actor_config())
                drained += 1

        assert drained > 0, (
            "once the limit was lifted the accumulated backlog did not drain: no job "
            "held back by the throttle was claimable afterwards. Correcting the "
            "configuration has to recover the work, or the misconfiguration was "
            "destructive after all."
        )
        remaining = await _pending_by_actor(fleet, [_THROTTLED])
        assert remaining.get(_THROTTLED, 0) == 0, (
            f"{remaining.get(_THROTTLED, 0)} jobs remain queued for the throttled actor "
            "after the limit was lifted and the fleet drained what it could. Some of the "
            "backlog is not recoverable by fixing the configuration that caused it."
        )
