"""Differential attacks on dispatch selection.

Axes: strict-FIFO ordering and priority ties, scheduled_at ties (normalized
against each side's own id order), oversample truncation, identity dedup,
the two RCA-era FIXED divergences (empty queues list; NULL fairness_key in
round-robin - both must hold as GREEN differentials), actor_config gating,
and per-actor max_concurrent admission.
"""

from __future__ import annotations

import pytest

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

_FIFO_ACTORS = ("aaa", "zzz", "test_actor")


async def _fifo_priority_order(side: DiffSide) -> None:
    # Full-batch RETURNING order is plan-dependent on PG (UPDATE..FROM), so
    # priority selection is pinned through LIMIT-starved rounds: which jobs
    # the round SELECTS is the contract, and the sets must agree.
    await side.enqueue("p0", scheduled_in=-5.0, priority=0)
    await side.enqueue("p10", scheduled_in=-4.0, priority=10)
    await side.enqueue("p5", scheduled_in=-3.0, priority=5)
    await side.enqueue("p10b", scheduled_in=-2.0, priority=10)
    await side.enqueue("p0b", scheduled_in=-1.0, priority=0)
    side.record("round1", sorted(await side.dispatch("w1", ["default"], limit=2)))
    side.record("round2", sorted(await side.dispatch("w1", ["default"], limit=2)))
    side.record("round3", sorted(await side.dispatch("w1", ["default"], limit=5)))


async def test_diff_dispatch_fifo_priority_order(pg_dsn: str) -> None:
    """Strict FIFO must SELECT the highest-priority jobs first on both backends."""
    mem, pg = await run_differential(_fifo_priority_order, pg_dsn=pg_dsn)
    assert_mirror(
        "strict-FIFO dispatch selects jobs by priority DESC, then "
        "scheduled_at, then id - the same jobs win each LIMIT-bounded round "
        "on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "round1": ["p10", "p10b"],
        "round2": ["p0", "p5"],
        "round3": ["p0b"],
    }


async def _scheduled_ties_same_timestamp(side: DiffSide) -> None:
    # Identical priority AND identical scheduled_at: the only legal selector
    # is id, and each side's ids differ - so the observable is each side's
    # dispatch expressed as ranks in its OWN id order.
    await side.enqueue("t1", scheduled_in=-1.0)
    await side.enqueue("t2", scheduled_in=-1.0)
    await side.enqueue("t3", scheduled_in=-1.0)
    await side.enqueue("t4", scheduled_in=-1.0)
    round1 = await side.dispatch("w1", ["default"], limit=2)
    round2 = await side.dispatch("w1", ["default"], limit=2)
    side.record("round1_ranks", side.id_rank(round1))
    side.record("round2_ranks", side.id_rank(round2))
    side.record("counts", [len(round1), len(round2)])


async def test_diff_dispatch_scheduled_at_ties(pg_dsn: str) -> None:
    """Full ties break on id: each side's rounds take its own id-first jobs."""
    mem, pg = await run_differential(_scheduled_ties_same_timestamp, pg_dsn=pg_dsn)
    assert_mirror(
        "a full (priority, scheduled_at) tie selects by id: each round takes "
        "the id-first jobs of the remaining set, identically on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {
        "round1_ranks": [0, 1],
        "round2_ranks": [2, 3],
        "counts": [2, 2],
    }


async def _cross_actor_priority_interleave(side: DiffSide) -> None:
    # One job per actor at rank 1, limit 1: PG's eligible ORDER BY settles
    # the cross-actor tie on priority DESC; the mirror's interleave settles
    # it on the actor NAME alphabetically.
    await side.enqueue("low", actor="aaa", scheduled_in=-1.0, priority=0)
    await side.enqueue("high", actor="zzz", scheduled_in=-1.0, priority=10)
    dispatched = await side.dispatch("w1", ["default"], limit=1)
    side.record("dispatched", dispatched)


async def test_diff_dispatch_cross_actor_priority_tie(pg_dsn: str) -> None:
    """At a shared rank with limit 1, the higher-priority job must win on BOTH
    backends - the mirror may not substitute alphabetical actor order."""
    mem, pg = await run_differential(
        _cross_actor_priority_interleave,
        pg_dsn=pg_dsn,
        actors=_FIFO_ACTORS,
    )
    assert_mirror(
        "when a dispatch round's limit cuts inside a rank shared by jobs of "
        "different actors, the winner is the highest-priority job (PG's "
        "eligible ORDER BY: pending_rank, fairness_rank NULLS LAST, priority "
        "DESC, scheduled_at) - never the alphabetically-first actor",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == ["high"]


async def _empty_queues_list(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0)
    await side.enqueue("j2", scheduled_in=-1.0)
    side.record("dispatched", await side.dispatch("w1", [], limit=5))
    # And the control: the same round with the queue named dispatches both.
    side.record("control", await side.dispatch("w1", ["default"], limit=5))


async def test_diff_dispatch_empty_queues_list_matches_nothing(pg_dsn: str) -> None:
    """KNOWN-FIXED divergence, verified green: an empty queues list means match
    NOTHING (PG's unnest annihilates every candidate), never 'no filter'."""
    mem, pg = await run_differential(_empty_queues_list, pg_dsn=pg_dsn)
    assert_mirror(
        "queues=[] selects no candidates on either backend - the mirror must "
        "not dispatch work a real worker polling the same empty list never "
        "would (the fixed RCA-era divergence, now pinned green)",
        mem,
        pg,
    )
    assert pg["records"] == {"dispatched": [], "control": ["j1", "j2"]}


async def _round_robin_null_fairness(side: DiffSide) -> None:
    # KNOWN-FIXED divergence, verified green: every unkeyed job shares ONE
    # "__null__" fairness partition (ranks 1..N), so a bounded round must
    # interleave unkeyed and keyed cohorts instead of starving the keyed one.
    await side.set_round_robin("default")
    await side.enqueue("u1", scheduled_in=-6.0)
    await side.enqueue("u2", scheduled_in=-5.0)
    await side.enqueue("u3", scheduled_in=-4.0)
    await side.enqueue("u4", scheduled_in=-3.0)
    await side.enqueue("k1", scheduled_in=-2.0, fairness_key="fk1")
    await side.enqueue("k2", scheduled_in=-1.0, fairness_key="fk2")
    dispatched = await side.dispatch("w1", ["default"], limit=4)
    # UPDATE..FROM RETURNING order is plan-dependent on PG, so the contract
    # is the SELECTED set: both unkeyed ranks 1-2 AND both keyed cohorts.
    side.record("dispatched_set", sorted(dispatched))
    side.record("dispatched_count", len(dispatched))


async def test_diff_dispatch_round_robin_null_fairness_shared_partition(pg_dsn: str) -> None:
    """KNOWN-FIXED divergence, verified green: unkeyed jobs share one
    ``__null__`` partition and rank 1..N, yielding their surplus slots to
    keyed cohorts in a bounded round."""
    mem, pg = await run_differential(_round_robin_null_fairness, pg_dsn=pg_dsn)
    assert_mirror(
        "round_robin partitions every unkeyed job into ONE shared __null__ "
        "fairness cohort (ranks 1..N) exactly like PG's PARTITION BY "
        "COALESCE(fairness_key, '__null__') - a bounded round selects "
        "unkeyed AND keyed cohort jobs on both backends (the fixed RCA-era "
        "starvation, now pinned green)",
        mem,
        pg,
    )
    assert pg["records"] == {
        "dispatched_set": ["k1", "k2", "u1", "u2"],
        "dispatched_count": 4,
    }


async def _identity_dedup_running_blocker(side: DiffSide) -> None:
    await side.plant(
        "running-k",
        status="running",
        worker_token="wholder",
        identity_key="blocked",
    )
    await side.enqueue("k-a", identity_key="blocked", scheduled_in=-2.0)
    await side.enqueue("k-b", identity_key="blocked", scheduled_in=-1.0)
    await side.enqueue("free", identity_key="other", scheduled_in=-3.0)
    dispatched = await side.dispatch("w1", ["default"], limit=5)
    side.record("dispatched", dispatched)


async def test_diff_dispatch_identity_dedup_running_blocker(pg_dsn: str) -> None:
    """A running job's identity_key blocks every pending job of the same
    identity; only other identities dispatch."""
    mem, pg = await run_differential(_identity_dedup_running_blocker, pg_dsn=pg_dsn)
    assert_mirror(
        "identity dedup: pending jobs whose (actor, identity_key) matches a "
        "running job are skipped; unrelated identities dispatch",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == ["free"]


async def _identity_dedup_picks_best(side: DiffSide) -> None:
    await side.enqueue("low", identity_key="same", scheduled_in=-2.0, priority=0)
    await side.enqueue("high", identity_key="same", scheduled_in=-1.0, priority=10)
    dispatched = await side.dispatch("w1", ["default"], limit=1)
    side.record("dispatched", dispatched)


async def test_diff_dispatch_identity_dedup_best_candidate(pg_dsn: str) -> None:
    """With two pending jobs of one identity and room for one, the best-ranked
    candidate (priority DESC) wins on both backends."""
    mem, pg = await run_differential(_identity_dedup_picks_best, pg_dsn=pg_dsn)
    assert_mirror(
        "identity dedup admits the best-ranked candidate per identity "
        "(priority DESC, scheduled_at, id) on both backends",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == ["high"]


async def _oversample_window_blocked_by_running_identity(side: DiffSide) -> None:
    # Four pending jobs share the identity of a RUNNING job and sort ahead
    # of the fifth (different identity). The strict-FIFO lateral's base
    # window reads only residual * oversample = 2 * 2 = 4 candidates per
    # queue - exactly the four blocked-identity jobs - so identity dedup
    # drops every candidate in the first pass. With a claimable row still
    # pending behind the window, the round widens the window and the
    # deeper row dispatches: an empty round is only legitimate when NO
    # claimable rows remain. Both backends must walk the same expansion
    # schedule or the differential diverges on exactly this shape.
    await side.plant(
        "running-k",
        status="running",
        worker_token="wholder",
        identity_key="blocked",
    )
    await side.enqueue("k1", identity_key="blocked", scheduled_in=-8.0)
    await side.enqueue("k2", identity_key="blocked", scheduled_in=-7.0)
    await side.enqueue("k3", identity_key="blocked", scheduled_in=-6.0)
    await side.enqueue("k4", identity_key="blocked", scheduled_in=-5.0)
    await side.enqueue("free", identity_key="other", scheduled_in=-1.0)
    dispatched = await side.dispatch("w1", ["default"], limit=2)
    side.record("dispatched", dispatched)


async def test_diff_dispatch_oversample_window_expansion(pg_dsn: str) -> None:
    """A fully-blocked base window does not end the round: the window widens
    and the claimable job behind the blocked cohort dispatches - identically
    on both backends."""
    mem, pg = await run_differential(_oversample_window_blocked_by_running_identity, pg_dsn=pg_dsn)
    assert_mirror(
        "the dispatch candidate set is bounded by residual * oversample per "
        "(actor, queue); when identity dedup removes that entire base window "
        "and claimable rows remain behind it, the round re-claims with a "
        "widened window - the mirror walks the same doubling schedule and "
        "reaches the same row",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == ["free"]


async def _zero_actor_config(side: DiffSide) -> None:
    await side.enqueue("j1", scheduled_in=-1.0)
    await side.enqueue("j2", scheduled_in=-1.0)
    dispatched = await side.dispatch("w1", ["default"], limit=5)
    side.record("dispatched", dispatched)


async def test_diff_dispatch_zero_actor_config_rows(pg_dsn: str) -> None:
    """With NO actor_config rows, PG dispatches nothing (per_actor_capacity is
    empty); the mirror must not silently treat 'no actors registered' as 'no
    filter'."""
    mem, pg = await run_differential(_zero_actor_config, pg_dsn=pg_dsn, actors=())
    assert_mirror(
        "dispatch candidates come from the actor_config registry: zero "
        "registered actors means zero candidates on PG, and the mirror must "
        "agree - 'no actors registered' must never read as 'no filter'",
        mem,
        pg,
    )
    assert pg["records"]["dispatched"] == []


async def _max_concurrent_admission(side: DiffSide) -> None:
    await side.register_actor_config(actor="capped", max_concurrent=1)
    await side.enqueue("c1", actor="capped", scheduled_in=-3.0)
    await side.enqueue("c2", actor="capped", scheduled_in=-2.0)
    await side.enqueue("c3", actor="capped", scheduled_in=-1.0)
    side.record("first_round", await side.dispatch("w1", ["default"], limit=5))
    # Without a terminal write the running job still holds the slot.
    side.record("second_round", await side.dispatch("w1", ["default"], limit=5))


async def test_diff_dispatch_max_concurrent_admission(pg_dsn: str) -> None:
    """A max_concurrent=1 actor admits one job per round while one is running."""
    mem, pg = await run_differential(
        _max_concurrent_admission,
        pg_dsn=pg_dsn,
        actors=("test_actor",),
    )
    assert_mirror(
        "per-actor max_concurrent admission: one dispatch with the slot held, "
        "zero while the running job holds it - identical on both backends",
        mem,
        pg,
    )
    assert pg["records"] == {"first_round": ["c1"], "second_round": []}
