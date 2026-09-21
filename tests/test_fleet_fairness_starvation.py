"""No tenant waits for ever because a louder one keeps arriving.

A fleet serves many queues and many actors at once, and the queue's job
is to keep all of them moving. The failure mode that matters is not
slowness but starvation: a cohort that never dispatches at all, while
its siblings drain, for as long as the noisy neighbour keeps producing.

Starvation is uniquely hard to see from outside. Overall throughput is
healthy, every pod reports successful rounds, and the only symptom is
that one customer's work is old - which reads as a slow actor, not as a
queue that has stopped considering it. These tests give the starved
cohort a voice: they assert that every cohort is dispatched within a
bounded number of rounds, under both queue modes, so the fleet cannot
quietly serve a subset for ever.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq._ids import new_base62
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

# Enough actors that a dispatcher which only ever looks at its
# first-ranked few leaves a clear majority untouched, and small enough
# that the whole scenario is a handful of rounds against real Postgres.
_ACTOR_COUNT = 20
_JOBS_PER_ACTOR = 30
_ROUND_LIMIT = 5

# Each actor holds far more work than one round can take, which is what
# makes the ordering observable: if a round is free to spend all its
# slots on its first-ranked cohorts, those cohorts never run out and the
# rest are never reached.
#
# The bound is deliberately not "however long the backlog takes to
# drain" - with this much work per actor that would take hundreds of
# rounds and would pass even for a queue that serves its actors strictly
# one after another, which is the thing being caught. It is instead the
# rounds needed to give every actor a turn if turns were taken at all:
# with _ACTOR_COUNT cohorts and _ROUND_LIMIT slots a round, four passes
# over the whole set is generous, and an actor absent from all of it is
# not waiting, it is being skipped.
_ROUNDS = (_ACTOR_COUNT * 4 + _ROUND_LIMIT - 1) // _ROUND_LIMIT

_QUEUE_FAST = "fleet_fair_fast"
_QUEUE_SLOW = "fleet_fair_slow"


def _actor_names(prefix: str, count: int) -> list[str]:
    return [f"{prefix}_{index:02d}" for index in range(count)]


async def _set_queue_mode(fleet: Fleet, queue: str, mode: str) -> None:
    await fleet.fetch(
        'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode RETURNING name",
        queue,
        mode,
    )


@pytest.mark.parametrize("mode", ["strict_fifo", "round_robin"])
async def test_every_actor_is_dispatched_within_a_bounded_number_of_rounds(
    pg_dsn: str, mode: str
) -> None:
    """A queue's actors all get served, not just the ones ranked first.

    Every actor holds an identical backlog, so nothing distinguishes
    them but their rank. Over several rounds a fleet that ranks fairly
    reaches all of them; a fleet that re-derives the same ordering every
    round reaches the same handful for ever and never looks past them.

    Round-robin is included deliberately: it is the mode an operator
    picks precisely to get this property, so a starving round-robin
    queue is worse than a starving FIFO one - the setting is doing
    nothing, and the operator believes the problem is solved.
    """
    actors = _actor_names("fair", _ACTOR_COUNT)
    schema = f"fleet_fair_{mode}_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=tuple((actor, _QUEUE_FAST) for actor in actors),
    ) as fleet:
        await _set_queue_mode(fleet, _QUEUE_FAST, mode)
        for actor in actors:
            await fleet.enqueue(_JOBS_PER_ACTOR, actor=actor, queue=_QUEUE_FAST)

        served: set[str] = set()
        for _ in range(_ROUNDS):
            claimed = await fleet.pod("pod-1").claim([_QUEUE_FAST], _ROUND_LIMIT)
            served.update(job.actor for job in claimed)

        starved = sorted(set(actors) - served)
        assert starved == [], (
            f"in {mode} mode, {len(starved)} of {_ACTOR_COUNT} actors were never "
            f"dispatched in {_ROUNDS} rounds of {_ROUND_LIMIT} while each held "
            f"{_JOBS_PER_ACTOR} due jobs: {starved}. Their work does not age out, it "
            "never runs, and the queue's overall throughput looks healthy "
            "throughout - the only visible symptom is one tenant's jobs getting older."
        )


async def test_a_hot_actor_does_not_monopolise_the_fleet(pg_dsn: str) -> None:
    """One actor's flood must not consume every slot the fleet has.

    The shape is the everyday incident: one tenant submits a large batch,
    and every other tenant on the queue stops moving. A fair queue
    interleaves them, so the quiet actors keep draining while the flood
    drains more slowly.

    The assertion is deliberately weak - the quiet actors need only be
    dispatched at all, not equally - because the property worth pinning
    is the absence of starvation rather than a particular ratio, which
    would break on any legitimate change to the ranking.
    """
    quiet_actors = _actor_names("quiet", 4)
    hot_actor = "hot_actor"
    schema = f"fleet_hot_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=(
            *((actor, _QUEUE_FAST) for actor in quiet_actors),
            (hot_actor, _QUEUE_FAST),
        ),
    ) as fleet:
        await fleet.enqueue(200, actor=hot_actor, queue=_QUEUE_FAST)
        for actor in quiet_actors:
            await fleet.enqueue(2, actor=actor, queue=_QUEUE_FAST)

        served: set[str] = set()
        for _ in range(_ROUNDS):
            claimed = await fleet.pod("pod-1").claim([_QUEUE_FAST], _ROUND_LIMIT)
            served.update(job.actor for job in claimed)

        starved = sorted(set(quiet_actors) - served)
        assert starved == [], (
            f"{len(starved)} actors holding two jobs each were never dispatched in "
            f"{_ROUNDS} rounds while one actor's backlog of 200 was being served: "
            f"{starved}. A single large submission has stopped every other tenant on "
            "the queue, and nothing in the queue's metrics distinguishes that from "
            "those tenants having sent no work."
        )


async def test_one_queue_does_not_starve_another(pg_dsn: str) -> None:
    """A pod polling two queues keeps both moving.

    Workers commonly subscribe to several queues, one of them much
    busier than the rest. The quiet queue is usually the one that
    matters - an operational or interactive queue riding alongside a
    bulk one. It must keep dispatching while the bulk queue drains.
    """
    schema = f"fleet_queues_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=(("bulk_actor", _QUEUE_FAST), ("interactive_actor", _QUEUE_SLOW)),
    ) as fleet:
        await fleet.enqueue(200, actor="bulk_actor", queue=_QUEUE_FAST)
        await fleet.enqueue(4, actor="interactive_actor", queue=_QUEUE_SLOW)

        served_queues: set[str] = set()
        for _ in range(_ROUNDS):
            claimed = await fleet.pod("pod-1").claim([_QUEUE_FAST, _QUEUE_SLOW], _ROUND_LIMIT)
            served_queues.update(job.queue for job in claimed)

        assert _QUEUE_SLOW in served_queues, (
            f"a pod polling both queues dispatched nothing from {_QUEUE_SLOW!r} in "
            f"{_ROUNDS} rounds while {_QUEUE_FAST!r} held a backlog of 200. The quiet "
            "queue is starved by the busy one it shares a worker with, so the work an "
            "operator put on a separate queue to keep it responsive is the work that "
            "stops first."
        )


async def test_fairness_holds_when_several_pods_dispatch_at_once(
    pg_dsn: str,
) -> None:
    """Adding pods must not narrow the set of actors the fleet serves.

    Each pod ranks cohorts independently. If they all reach the same
    conclusion, a larger fleet concentrates on the same few actors
    rather than covering more of them - scaling out makes starvation
    worse, which is the opposite of what an operator adding pods to a
    backlog expects.
    """
    actors = _actor_names("multi", _ACTOR_COUNT)
    schema = f"fleet_fair_multi_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1", "pod-2", "pod-3"),
        actors=tuple((actor, _QUEUE_FAST) for actor in actors),
    ) as fleet:
        for actor in actors:
            await fleet.enqueue(_JOBS_PER_ACTOR, actor=actor, queue=_QUEUE_FAST)

        served: set[str] = set()
        for _ in range(_ROUNDS):
            rounds = await asyncio.gather(
                *(
                    fleet.pod(name).claim([_QUEUE_FAST], _ROUND_LIMIT)
                    for name in ("pod-1", "pod-2", "pod-3")
                )
            )
            for claimed in rounds:
                served.update(job.actor for job in claimed)

        starved = sorted(set(actors) - served)
        assert starved == [], (
            f"{len(starved)} of {_ACTOR_COUNT} actors were never dispatched by any of "
            f"three pods over {_ROUNDS} concurrent rounds, each actor holding "
            f"{_JOBS_PER_ACTOR} due jobs: {starved}. Three pods are converging on the "
            "same cohorts instead of covering more of them, so scaling the fleet out "
            "leaves the starved tenants exactly as starved."
        )
