"""Throughput must come from the fleet, not from whichever pod wins the race.

The queue's depth oracles vary backlog depth and cohort count against a
single dispatcher, so they measure how much work one round does - never
whether a second dispatcher contributes any. That axis is the one an
operator changes: the response to a growing backlog is to add pods.

These tests hold the backlog fixed and vary the number of pods claiming
concurrently. The properties are the ones a capacity decision rests on:
claims are disjoint, so no job runs twice; a round comes back empty only
when there is genuinely nothing left to claim, not merely because a peer
holds this pod's first-ranked candidates; and the fleet's per-round yield
grows when a pod joins. A fleet that fails these looks healthy on every
per-pod metric - each pod reports successful rounds - while the backlog
drains at the rate of one pod no matter how many are paid for.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq._ids import new_base62
from taskq.backend._protocol import JobRow
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_dispatch_q"
_ACTOR = "fleet_dispatch_actor"

# Deep enough that every pod in the widest scenario can fill its limit
# from rows no peer is holding, so an empty round is never explained by
# an exhausted backlog.
_BACKLOG = 40
_ROUND_LIMIT = 5


async def _concurrent_round(fleet: Fleet, pod_names: list[str]) -> list[list[JobRow]]:
    """One claim round per pod, all in flight at the same time.

    ``gather`` is what makes this a fleet test rather than a sequence of
    single-dispatcher rounds: the pods' claim statements overlap inside
    Postgres, which is the condition under which one pod's held locks are
    visible to another's candidate selection.
    """
    return list(
        await asyncio.gather(*(fleet.pod(name).claim([_QUEUE], _ROUND_LIMIT) for name in pod_names))
    )


def _distinct_ids(claims: list[list[JobRow]]) -> set[object]:
    return {job.id for claim in claims for job in claim}


async def test_concurrent_pods_never_claim_the_same_job(pg_dsn: str) -> None:
    """Two pods claiming at once hold disjoint sets of rows.

    This is the at-most-once foundation every other fleet property rests
    on. If two pods ever hold the same row, both will run the actor and
    both will attempt a terminal write, and the operator sees a job whose
    side effects happened twice with nothing in its history to say so.
    """
    schema = f"fleet_disjoint_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("a", "b"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        seen: set[object] = set()
        for round_index in range(4):
            claims = await _concurrent_round(fleet, ["a", "b"])
            ids_a = {job.id for job in claims[0]}
            ids_b = {job.id for job in claims[1]}

            overlap = ids_a & ids_b
            assert overlap == set(), (
                f"round {round_index}: pods a and b both hold {len(overlap)} of the same "
                f"jobs ({sorted(str(i) for i in overlap)}). Each will run the actor and "
                "attempt a terminal write, so the work happens twice and the job's own "
                "history records only one of the two runs."
            )

            repeats = (ids_a | ids_b) & seen
            assert repeats == set(), (
                f"round {round_index}: {len(repeats)} jobs were handed out in an earlier "
                "round and claimed again while still held. A claimed row must not be "
                "claimable until its lease expires or it is released."
            )
            seen |= ids_a | ids_b


async def test_a_pod_returns_empty_only_when_no_claimable_work_remains(
    pg_dsn: str,
) -> None:
    """A pod must not go idle while claimable rows sit unlocked.

    Postgres hands a contending claim ``SKIP LOCKED`` precisely so it can
    slide past a peer's held rows and take the next unlocked ones. A pod
    that instead fixes its candidate window before locking finds that
    window taken and reports an empty round, while the backlog behind the
    window is untouched and claimable.

    Operationally the pod looks healthy - it is polling, its rounds
    return, it logs no error - and it does no work. An operator scaling
    out to drain a backlog sees the new pods idle and the backlog fall at
    its old rate, with no signal anywhere naming the cause.
    """
    schema = f"fleet_nonempty_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=schema, pods=("a", "b"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)

        claims = await _concurrent_round(fleet, ["a", "b"])
        held = len(_distinct_ids(claims))
        remaining = _BACKLOG - held

        for pod_index, claim in enumerate(claims):
            assert claim, (
                f"pod {['a', 'b'][pod_index]} returned an empty round with {remaining} "
                f"pending jobs still unlocked and due out of a backlog of {_BACKLOG}. "
                "An empty round tells the worker there is nothing to do, so it backs "
                "off and waits - adding this pod bought the fleet no throughput at all."
            )


async def test_fleet_round_yield_grows_when_a_pod_joins(pg_dsn: str) -> None:
    """Per-round throughput scales with pod count while work remains.

    The measurement is on rows claimed per concurrent round, not on wall
    time: row counts are exact and deterministic, while timing on a
    shared container is neither. Each arm starts from its own untouched
    backlog, because the comparison is only honest if both fleets face
    the same queue - a round run against a backlog some earlier round
    already disturbed is measuring a different, easier question.

    One pod at limit 5 claims 5. Two pods claiming at the same time must
    claim more than 5 between them, with 40 rows pending and nothing
    capping the actor. Anything less means a capacity plan built on 'add
    a pod, get a pod's worth of throughput' is wrong, and the spend on
    the extra pod buys nothing.
    """
    solo_schema = f"fleet_scale_solo_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=solo_schema, pods=("a",), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)
        solo_yield = len(await fleet.pod("a").claim([_QUEUE], _ROUND_LIMIT))

    assert solo_yield == _ROUND_LIMIT, (
        f"a single pod claimed {solo_yield} of its limit of {_ROUND_LIMIT} against a "
        f"backlog of {_BACKLOG}; the scaling comparison below is only meaningful once "
        "one pod can fill its own round."
    )

    paired_schema = f"fleet_scale_pair_{new_base62()}".lower()
    async with open_fleet(
        pg_dsn, schema=paired_schema, pods=("a", "b"), actors=((_ACTOR, _QUEUE),)
    ) as fleet:
        await fleet.enqueue(_BACKLOG, actor=_ACTOR, queue=_QUEUE)
        paired = await _concurrent_round(fleet, ["a", "b"])
        paired_yield = len(_distinct_ids(paired))

    assert paired_yield > solo_yield, (
        f"one pod claims {solo_yield} rows per round; two pods claiming at the same time "
        f"claim {paired_yield} between them, against an identical backlog of {_BACKLOG} "
        "with ample unlocked rows for both. Throughput does not grow with the fleet: "
        "doubling the pods leaves the drain rate unchanged, so a backlog that is growing "
        "cannot be caught up by scaling out."
    )
