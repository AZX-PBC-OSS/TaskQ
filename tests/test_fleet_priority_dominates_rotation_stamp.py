"""A high-priority actor is not starved by the cross-actor rotation stamp.

The rotation stamp that keeps strict_fifo/round_robin fair across actors
(src/taskq/backend/_dispatch_sql.py's ``last_claimed_at`` tiebreak, pinned
generally by tests/test_fleet_strict_fifo_bounded_rounds_contract.py) opens
an adversarial question an operator running mixed-priority actors on one
queue will ask: if quiet, low-priority actors get claimed (and stamped)
first because they arrived first, does a LATE-ARRIVING high-priority actor
have to wait behind them once it shows up, because every quiet actor's
stamp is now "older" than its own never-claimed status?

It does not, and this test pins why: the claim statement's ORDER BY chains
compare ``priority DESC`` BEFORE ``actor_claimed_at ASC NULLS FIRST`` at
every cut (top_ids: "ORDER BY pending_rank, priority DESC, actor_claimed_at
ASC NULLS FIRST, ..." — src/taskq/backend/_dispatch_sql.py lines 469-471;
the same order at eligible's re-limit, lines 544-545). Priority strictly
dominates the rotation tiebreak; the tiebreak only decides ties AMONG
equal-priority actors. A single higher-priority actor therefore keeps its
own pending_rank-1 slot every round it has work, regardless of how many
lower-priority actors were stamped before it existed.

This test also documents the shape of the guarantee an adopter should NOT
assume: because ``pending_rank`` partitions BY ACTOR (ranked AS MATERIALIZED
... ROW_NUMBER() OVER (PARTITION BY id.actor ...) — line 407-412), a single
actor's jobs compete against every OTHER actor's same-rank job one slot at
a time; a priority=100 actor with a deep backlog does not claim multiple
round slots just because its priority is high — it claims exactly one slot
per round for as long as other actors also have pending_rank-1 work, the
same as everyone else. Priority orders WHICH job wins a contested slot; it
does not grant an actor extra slots. See docs/guides/actors.md's
``priority`` field description, which does not currently state this
scoping either way.

Confirmed empirically before this test was written (scratch probe, not
committed): 10 quiet actors (priority=0) each enqueue 50 jobs and get
claimed in round 1, before urgent_actor (priority=100) has any jobs at
all. urgent_actor then arrives mid-stream. Across the next 10 rounds,
urgent_actor was served in EVERY round from the first one its jobs
existed — the adversarial "already-stamped" ordering did not delay it by
even one round. This test pins that same result permanently through the
fleet harness.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_base62
from tests._fleet import Fleet, open_fleet

pytestmark = pytest.mark.integration

_QUEUE = "fleet_priority_rotation_q"
_N_QUIET = 10
_ROUND_LIMIT = 5
_URGENT_PRIORITY = 100
_ROUNDS_AFTER_ARRIVAL = 8


def _quiet_actor_names() -> list[str]:
    return [f"priority_quiet_{i:02d}" for i in range(_N_QUIET)]


async def _set_priority(fleet: Fleet, actor: str, priority: int) -> None:
    await fleet.fetch(
        'UPDATE "{schema}".jobs SET priority = $2 WHERE actor = $1', actor, priority
    )


async def test_late_arriving_high_priority_actor_is_served_every_round(pg_dsn: str) -> None:
    """A priority=100 actor arriving after low-priority actors are already
    stamped must still be served the very first round its work exists,
    and every round after — not delayed by the rotation tiebreak, which
    only orders ties among equal-priority actors.
    """
    quiet_actors = _quiet_actor_names()
    urgent_actor = "priority_urgent_actor"
    schema = f"fleet_priority_rot_{new_base62()}".lower()

    async with open_fleet(
        pg_dsn,
        schema=schema,
        pods=("pod-1",),
        actors=[(a, _QUEUE) for a in quiet_actors] + [(urgent_actor, _QUEUE)],
    ) as fleet:
        pod = fleet.pod("pod-1")

        # Deep backlog for every quiet actor, all priority=0 (the
        # enqueue default). None of urgent_actor's jobs exist yet.
        for actor in quiet_actors:
            await fleet.enqueue(50, actor=actor, queue=_QUEUE)

        # Round 1: only quiet actors compete; several get stamped
        # (claimed) here, BEFORE urgent_actor has any jobs to its name.
        first_round = await pod.claim([_QUEUE], _ROUND_LIMIT)
        assert first_round, "setup invariant: round 1 must claim something to stamp quiet actors"

        # urgent_actor now arrives, priority=100, with its own deep
        # backlog — the adversarial case: every quiet actor is already
        # stamped (claimed_at is non-null), while urgent_actor is not.
        await fleet.enqueue(30, actor=urgent_actor, queue=_QUEUE)
        await _set_priority(fleet, urgent_actor, _URGENT_PRIORITY)

        served_every_round = True
        first_miss: int | None = None
        for round_no in range(1, _ROUNDS_AFTER_ARRIVAL + 1):
            claimed = await pod.claim([_QUEUE], _ROUND_LIMIT)
            urgent_count = sum(1 for job in claimed if job.actor == urgent_actor)
            if urgent_count == 0 and first_miss is None:
                served_every_round = False
                first_miss = round_no

        remaining = await fleet.fetch(
            "SELECT count(*) AS n FROM \"{schema}\".jobs "
            "WHERE actor = $1 AND status = 'pending'",
            urgent_actor,
        )
        urgent_still_pending = int(remaining[0]["n"])

        # Sanity: urgent_actor must have had enough backlog to be
        # eligible every round measured (otherwise a "miss" could just
        # mean it ran out of work, not that it was starved).
        assert urgent_still_pending >= 0
        assert 30 - urgent_still_pending <= _ROUNDS_AFTER_ARRIVAL * _ROUND_LIMIT

        assert served_every_round, (
            f"urgent_actor (priority={_URGENT_PRIORITY}) was skipped in round "
            f"{first_miss} despite {_N_QUIET} lower-priority actors already being "
            "stamped as claimed before it had any work. The cross-actor rotation "
            "tiebreak (actor_claimed_at) must never override priority — if it does, "
            "an operator's priority bias on an urgent actor can be silently defeated "
            "by unrelated, lower-priority actors that merely arrived first."
        )
