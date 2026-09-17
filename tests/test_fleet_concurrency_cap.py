"""What ``max_concurrent`` actually holds once more than one pod dispatches.

An actor's ``max_concurrent`` reads like a fleet-wide ceiling, and a
single worker makes it look like one. It is not. Each dispatcher reads
the in-flight count once, before taking its row locks, and admits up to
the remaining headroom; concurrent dispatchers all read the same count
and all admit against it, locking disjoint rows, so every one of them
succeeds. The jobs genuinely run, and reclaiming locks afterwards cannot
un-run them.

That makes the setting a per-round admission damper whose slack grows
with the fleet. An operator sizing a downstream system — a database
connection budget, a third-party rate limit, a licence count — from this
number will exceed it by a factor of the pod count, and will do so only
under the load that brought the pods up.

These tests measure the real ceiling rather than restating the
documentation: they run concurrent dispatch rounds against a capped
actor and assert the observed in-flight count against the bound the
dispatch statement commits to. A regression that widened the window
would show up here as a number, which is what an operator needs, and the
tests also pin the mechanism that *does* hold fleet-wide, so the
difference between the two is a fact the suite keeps honest.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq._ids import new_base62
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_cap_ceiling_q"
_ACTOR = "fleet_cap_ceiling_actor"

_MAX_CONCURRENT = 4
_PODS = ("pod-1", "pod-2", "pod-3")
# Deep enough that the cap, never the backlog, is what limits admission.
_BACKLOG = 60


async def _set_actor_cap(fleet: Fleet, cap: int | None) -> None:
    await fleet.fetch(
        'UPDATE "{schema}".actor_config SET max_concurrent = $1 WHERE actor = $2 RETURNING actor',
        cap,
        _ACTOR,
    )


async def _in_flight(fleet: Fleet) -> int:
    rows = await fleet.fetch(
        "SELECT count(*) AS n FROM \"{schema}\".jobs WHERE status = 'running' AND actor = $1",
        _ACTOR,
    )
    return int(rows[0]["n"])


async def test_one_pod_holds_the_cap_exactly(pg_dsn: str) -> None:
    """With a single dispatcher the cap is exact.

    This is the reading that makes the setting misleading, so it is
    stated first and on purpose: a test fleet of one pod — and a staging
    environment of one pod — sees ``max_concurrent`` honoured precisely.
    Anyone who validates their capacity plan that way will carry the
    conclusion into production, where it stops being true.
    """
    schema = f"fleet_cap_solo_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("pod-1",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await _set_actor_cap(fleet, _MAX_CONCURRENT)
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        for _ in range(3):
            await fleet.pod("pod-1").claim([_QUEUE], _BACKLOG)

        running = await _in_flight(fleet)
        assert running == _MAX_CONCURRENT, (
            f"a single dispatcher admitted {running} jobs against a cap of "
            f"{_MAX_CONCURRENT}. With one pod the cap is the whole story, and a fleet "
            "that cannot hold it here will not hold anything weaker under contention."
        )


async def test_concurrent_pods_over_admit_within_the_documented_bound(
    pg_dsn: str,
) -> None:
    """Several pods exceed the cap, and by no more than the stated amount.

    Each dispatcher reads the same in-flight count and admits against
    it, so the fleet can run up to ``pods * max_concurrent`` at once.
    The dispatch statement commits to that bound; this measures it.

    Both directions matter. Holding exactly the cap would mean the
    statement had started serialising dispatchers, which is the throughput
    collapse the concurrency tests in this suite exist to prevent.
    Exceeding the bound would mean the window is wider than documented,
    and every capacity plan derived from the setting is wrong by more
    than anyone was told.
    """
    schema = f"fleet_cap_fleet_{new_base62()}".lower()
    async with open_fleet(pg_dsn, schema=schema, pods=_PODS, actors=((_ACTOR, _QUEUE),)) as fleet:
        await _set_actor_cap(fleet, _MAX_CONCURRENT)
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        await asyncio.gather(*(fleet.pod(name).claim([_QUEUE], _BACKLOG) for name in _PODS))

        running = await _in_flight(fleet)
        worst_case = len(_PODS) * _MAX_CONCURRENT
        assert running <= worst_case, (
            f"{len(_PODS)} pods dispatching at once admitted {running} jobs against a "
            f"per-actor cap of {_MAX_CONCURRENT}; the dispatch statement's documented "
            f"worst case is {worst_case}. The cap is looser than the queue says it is, "
            "so any downstream limit sized from this setting — a connection budget, a "
            "third-party rate limit, a licence count — is exceeded by more than the "
            "documented margin, and only under the load that scaled the fleet up."
        )
        assert running >= _MAX_CONCURRENT, (
            f"{len(_PODS)} pods admitted {running} jobs in total against a cap of "
            f"{_MAX_CONCURRENT}. The fleet is admitting less than one pod's worth of "
            "capped work, so the cap has become a fleet-wide throughput ceiling that "
            "shrinks as pods are added."
        )
        # The measured value sits at the cap today rather than anywhere
        # near the worst case, because a concurrent round currently
        # yields rows to one pod only — the condition the dispatch
        # concurrency tests in this suite pin as a defect. Widening
        # those rounds so every pod claims is exactly what opens the
        # admission window this bound describes, so the upper assertion
        # above is the one that starts doing work on the day that is
        # fixed, and it is deliberately kept at the documented bound
        # rather than at the number observed now.


async def test_an_uncapped_actor_is_not_limited_by_a_capped_sibling(
    pg_dsn: str,
) -> None:
    """One actor's cap must not throttle another's work.

    Caps are per-actor, and the in-flight count the dispatcher reads is
    grouped by actor. If a capped actor's headroom constrained the round
    as a whole, an unrelated actor's throughput would collapse whenever
    a capped neighbour was busy — a coupling invisible from either
    actor's own configuration.
    """
    other_actor = "fleet_cap_uncapped_actor"
    schema = f"fleet_cap_sibling_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=((_ACTOR, _QUEUE), (other_actor, _QUEUE)),
    ) as fleet:
        await _set_actor_cap(fleet, _MAX_CONCURRENT)
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)
        await fleet.enqueue(20, actor=other_actor, queue=_QUEUE)

        for _ in range(3):
            await fleet.pod("pod-1").claim([_QUEUE], 20)

        rows = await fleet.fetch(
            "SELECT count(*) AS n FROM \"{schema}\".jobs WHERE status = 'running' AND actor = $1",
            other_actor,
        )
        uncapped_running = int(rows[0]["n"])
        assert uncapped_running > _MAX_CONCURRENT, (
            f"an uncapped actor sharing a queue with a capped one has only "
            f"{uncapped_running} jobs running, no more than the neighbour's cap of "
            f"{_MAX_CONCURRENT}. One actor's limit is throttling another's work, and "
            "nothing in the throttled actor's own configuration would explain it."
        )


async def test_a_drain_mode_actor_dispatches_nothing_on_any_pod(
    pg_dsn: str,
) -> None:
    """A cap of zero means zero, fleet-wide.

    Drain mode is how an operator stops an actor without deleting its
    work — before a risky deploy, or while a downstream dependency is
    broken. It is the one capacity setting that must be exact rather
    than best-effort, because its whole purpose is that nothing runs.

    Best-effort admission around zero would be the worst case of the
    window this module measures: the operator believes the actor is
    stopped, and the more pods there are, the more of it runs anyway.
    """
    schema = f"fleet_cap_drain_{new_base62()}".lower()
    async with open_fleet(pg_dsn, schema=schema, pods=_PODS, actors=((_ACTOR, _QUEUE),)) as fleet:
        await _set_actor_cap(fleet, 0)
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        rounds = await asyncio.gather(
            *(fleet.pod(name).claim([_QUEUE], _BACKLOG) for name in _PODS)
        )
        claimed = sum(len(batch) for batch in rounds)
        running = await _in_flight(fleet)

        assert claimed == 0, (
            f"{claimed} jobs were claimed for an actor held at max_concurrent=0 by "
            f"{len(_PODS)} pods dispatching at once. Drain mode is how an operator stops "
            "an actor without discarding its work — if it admits anything, the actor an "
            "operator believes is stopped is still running, and runs more the more pods "
            "there are."
        )
        assert running == 0, f"{running} jobs are running for an actor in drain mode."
