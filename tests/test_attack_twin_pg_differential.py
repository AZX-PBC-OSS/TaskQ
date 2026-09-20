"""Differential attacks on the twin's idempotency contract (branch
fix/twin-batch-atomic): identical enqueue/batch/idempotency scripts
driven through both backends, observables compared token-for-token.

Postgres is the contract source. Every divergence recorded here is a
mirror that certifies code behaving differently in production.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from taskq.exceptions import BatchMaxPendingExceededError

from .test_rt_diff_enqueue_batch_failures import _batch_item, _capped_args, _keyed_batch_item
from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential


async def _in_batch_repeat_pair_alias(side: DiffSide) -> None:
    """One batch, one pair repeated by the SAME actor, plus a fresh key.

    The repeated item must dedup onto the FIRST item's just-inserted row:
    the returned handle aliases the holder's id (PG's ``ON CONFLICT``
    plus post-abort fetch), the repeated item's own id stores nothing.
    """
    a1 = _keyed_batch_item(side, "a1", "actor_a", "dup-key")
    a2 = _keyed_batch_item(side, "a2", "actor_a", "dup-key")
    a3 = _keyed_batch_item(side, "a3", "actor_a", "fresh-key")
    rows = await side.backend.enqueue_batch([a1, a2, a3])
    side.record("returned", [side.token_of(r.id) for r in rows])
    stored = [
        token
        for token, args in (("a1", a1), ("a2", a2), ("a3", a3))
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", stored)


@pytest.mark.integration
async def test_diff_in_batch_repeat_pair_aliases_holder_id(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _in_batch_repeat_pair_alias, pg_dsn=pg_dsn, actors=("actor_a",)
    )
    assert_mirror(
        "an in-batch same-actor repeat of an idempotency pair aliases the "
        "first item's row id and stores nothing for the repeat, identically",
        mem,
        pg,
    )
    assert pg["records"]["returned"] == ["a1", "a1", "a3"]
    assert pg["records"]["stored"] == ["a1", "a3"]


async def _stored_holder_dedup_hit(side: DiffSide) -> None:
    """A pair stored BEFORE the batch: the batch's repeat item dedups onto
    the stored holder and returns ITS id, and nothing new is written."""
    holder = await side.enqueue("holder", idempotency_key="k", actor="actor_a")
    side.record("holder_id", side.token_of(holder.id))
    later = _keyed_batch_item(side, "later", "actor_a", "k")
    rows = await side.backend.enqueue_batch([later])
    side.record("returned", [side.token_of(r.id) for r in rows])
    side.record("later_stored", await side.backend.get(later.id) is not None)


@pytest.mark.integration
async def test_diff_stored_holder_dedup_returns_holder_id(pg_dsn: str) -> None:
    mem, pg = await run_differential(_stored_holder_dedup_hit, pg_dsn=pg_dsn, actors=("actor_a",))
    assert_mirror(
        "a batch item whose pair is already stored dedups onto the stored "
        "holder and returns its id, identically",
        mem,
        pg,
    )
    assert pg["records"]["returned"] == ["holder"]
    assert pg["records"]["later_stored"] is False


async def _stored_cross_actor_holder_refusal(side: DiffSide) -> None:
    """A pair held by ANOTHER actor's stored row: the batch refuses the
    whole call before any insert, naming the stored holder's id."""
    await side.enqueue("holder", idempotency_key="k", actor="actor_a")
    good = _batch_item(side, "good")
    intruder = _keyed_batch_item(side, "intruder", "actor_b", "k")
    try:
        await side.backend.enqueue_batch([good, intruder])
        side.record("batch", "admitted-all")
    except Exception as exc:  # Why: the typed outcome is the observable.
        side.record("batch", type(exc).__name__)
        side.record("mismatch_actor", getattr(exc, "actor", None))
        side.record("mismatch_existing_actor", getattr(exc, "existing_actor", None))
        holder_id = getattr(exc, "existing_job_id", None)
        side.record(
            "mismatch_existing_job_id",
            side.token_of(holder_id) if holder_id is not None else None,
        )
    stored = [
        token
        for token, args in (("good", good), ("intruder", intruder))
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", stored)


@pytest.mark.integration
async def test_diff_stored_cross_actor_holder_refuses_whole_call(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _stored_cross_actor_holder_refusal, pg_dsn=pg_dsn, actors=("actor_a", "actor_b")
    )
    assert_mirror(
        "a batch item whose pair is stored under another actor refuses the "
        "whole call with the stored holder's id named, identically",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "IdempotencyKeyActorMismatchError"
    assert pg["records"]["mismatch_existing_job_id"] == "holder"
    assert pg["records"]["stored"] == []


async def _fast_tier_duplicate_pair(side: DiffSide) -> None:
    """The COPY tier with a repeated pair: the whole batch aborts with the
    typed duplicate error, nothing written."""
    f1 = _keyed_batch_item(side, "f1", "actor_a", "dk")
    f2 = _keyed_batch_item(side, "f2", "actor_a", "dk")
    try:
        n = await side.backend.enqueue_batch_fast([f1, f2])
        side.record("fast", f"ok:{n}")
    except Exception as exc:  # Why: the typed outcome is the observable.
        side.record("fast", type(exc).__name__)
    stored = [
        token
        for token, args in (("f1", f1), ("f2", f2))
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", stored)


@pytest.mark.integration
async def test_diff_fast_tier_duplicate_aborts_with_typed_error(pg_dsn: str) -> None:
    mem, pg = await run_differential(_fast_tier_duplicate_pair, pg_dsn=pg_dsn, actors=("actor_a",))
    assert_mirror(
        "a repeated idempotency pair aborts the whole fast-tier batch with "
        "the typed duplicate error and nothing stored, identically",
        mem,
        pg,
    )
    assert pg["records"]["fast"] == "DuplicateIdempotencyKeyError"
    assert pg["records"]["stored"] == []


async def _cap_partition_and_discount(side: DiffSide) -> None:
    """Cap admission: over-cap actor refused as a group (other actors'
    rows still stored, typed refusal after commit), and a keyed item
    whose pair is already stored is DISCOUNTED from the cap count."""
    # actor_a sits at its cap of 1 with one stored pair "dk".
    await side.enqueue("holder", idempotency_key="dk", actor="actor_a", max_pending=1)
    discounted = replace(_keyed_batch_item(side, "discounted", "actor_a", "dk"), max_pending=1)
    over1 = _capped_args(side, "over1", "actor_a", max_pending=1)
    over2 = _capped_args(side, "over2", "actor_a", max_pending=1)
    other = _capped_args(side, "other", "actor_b", max_pending=100)
    try:
        rows = await side.backend.enqueue_batch([discounted, over1, over2, other])
        side.record("returned", [side.token_of(r.id) for r in rows])
        side.record("cap", "admitted-all")
    except BatchMaxPendingExceededError as exc:
        side.record("returned", [side.token_of(r.id) for r in []])
        side.record("cap", "refused")
        side.record(
            "refused_indices",
            sorted((actor, tuple(indices)) for actor, indices in exc.refused_indices.items()),
        )
        side.record("admitted_count", exc.admitted_count)
    stored = [
        token
        for token, args in (
            ("discounted", discounted),
            ("over1", over1),
            ("over2", over2),
            ("other", other),
        )
        if await side.backend.get(args.id) is not None
    ]
    side.record("stored", stored)


@pytest.mark.integration
async def test_diff_cap_partition_and_keyed_discount(pg_dsn: str) -> None:
    mem, pg = await run_differential(
        _cap_partition_and_discount, pg_dsn=pg_dsn, actors=("actor_a", "actor_b")
    )
    assert_mirror(
        "cap admission partitions the over-cap actor's items as a group, "
        "stores the other actors' rows, discounts a stored pair from the "
        "cap count, and raises the typed refusal with the same indices "
        "after the admitted rows commit, identically",
        mem,
        pg,
    )
    assert pg["records"]["cap"] == "refused"
    assert pg["records"]["refused_indices"] == [["actor_a", [0, 1, 2]]]
    assert pg["records"]["admitted_count"] == 1
    assert sorted(pg["records"]["stored"]) == ["other"]
