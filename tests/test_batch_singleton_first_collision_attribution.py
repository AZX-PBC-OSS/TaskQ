"""Two distinct singleton actors colliding in one batch name the FIRST
collision in batch order - deterministically, on both backends, on both
bulk tiers.

``jobs_singleton_uniq`` is keyed on ``(actor)`` over live singleton-flagged
rows, and a bulk write visits items in batch order, so the violating actor
is the first singleton item whose actor repeats an earlier singleton item or
already holds a live singleton row. The refusal names that actor through the
shared pure rule (``first_singleton_collision_actor``) applied to the
batch's own contents plus a post-abort lookup of the stored holders - never
parsed from the driver's detail text, which renders values raw and
unquoted.

The single-collision pins (same-actor in-batch repeats and stored-collision
attribution, on both tiers) already exist
(tests/test_batch_fast.py::TestTISingletonCollisionTypedError and the
in-memory siblings). What they cannot catch is an attribution that drifts
from batch order to something else - alphabetical, hash order, last-write -
because with one colliding actor every order names it. With TWO distinct
colliding actors the orders disagree, and the idempotency-pair analog is
already pinned (tests/test_batch_fast.py "two distinct duplicate pairs name
the first repeat"). This module is the singleton variant: forward and
reversed batch orderings name different actors (so only batch order
satisfies both), and a stored holder colliding ahead of an in-batch repeat
wins by position, on the unnest tier and the COPY tier alike.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.exceptions import SingletonCollisionError

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration

_ACTORS = ("actor_a", "actor_b")


def _singleton_item(side: DiffSide, token: str, actor: str) -> EnqueueArgs:
    """One due singleton-flagged batch item, token-registered."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        metadata={"singleton": True},
    )
    side.register_job_id(token, args.id)
    return args


async def _run_batch_and_record(
    side: DiffSide, items: list[EnqueueArgs], tokens: list[str], *, fast: bool
) -> None:
    """Run one colliding batch on this side; record the refusal's actor and
    the surviving store."""
    try:
        if fast:
            await side.backend.enqueue_batch_fast(items)
        else:
            await side.backend.enqueue_batch(items)
        side.record("batch", "admitted-all")
    except SingletonCollisionError as exc:
        # The typed refusal's whole contract here: WHICH actor it names.
        side.record("batch", f"refused:{exc.actor}")
    stored: list[str] = []
    for token, args in zip(tokens, items, strict=True):
        if await side.backend.get(args.id) is not None:
            stored.append(token)
    side.record("stored_from_batch", stored)


def _two_actor_repeat_batch(side: DiffSide, *, first: str, second: str) -> list[EnqueueArgs]:
    """[first, second, first, second]: both actors repeat in-batch; the
    collision the statement reaches first is the second item of *first*'s
    pair (position 2), so the refusal must name *first*."""
    return [
        _singleton_item(side, "x1", first),
        _singleton_item(side, "y1", second),
        _singleton_item(side, "x2", first),
        _singleton_item(side, "y2", second),
    ]


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_two_distinct_singleton_actors_colliding_name_the_first_in_batch_order(
    pg_dsn: str, fast: bool
) -> None:
    """Batch [A, B, A, B] - both actors repeat - refuses naming actor A:
    the repeat the write reaches first in batch order."""

    async def scenario(side: DiffSide) -> None:
        batch = _two_actor_repeat_batch(side, first="actor_a", second="actor_b")
        await _run_batch_and_record(side, batch, ["x1", "y1", "x2", "y2"], fast=fast)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "two distinct singleton actors repeating in one batch refuse the "
        "whole call naming the first repeat in batch order, with nothing "
        "stored, identically on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "refused:actor_a", (
        "the collision the write reaches first is actor_a's repeat "
        f"(position 2 of 4); the refusal named {pg['records']['batch']!r}"
    )
    assert pg["records"]["stored_from_batch"] == []
    assert pg["status_counts"] == {}


@pytest.mark.parametrize("fast", [False, True], ids=["unnest", "copy"])
async def test_reversed_order_names_the_other_actor(pg_dsn: str, fast: bool) -> None:
    """Batch [B, A, B, A] refuses naming actor B - the mirror image of the
    forward case, so only genuine batch-order attribution (never
    alphabetical or hash order) satisfies both."""

    async def scenario(side: DiffSide) -> None:
        batch = _two_actor_repeat_batch(side, first="actor_b", second="actor_a")
        await _run_batch_and_record(side, batch, ["x1", "y1", "x2", "y2"], fast=fast)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "the reversed batch names actor_b - the first repeat in ITS batch "
        "order - identically on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "refused:actor_b"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["status_counts"] == {}


async def test_a_stored_holder_colliding_ahead_of_an_in_batch_repeat_wins_by_position(
    pg_dsn: str,
) -> None:
    """A live singleton holder for actor B is already stored; the batch
    [A, B, A] carries B's stored collision at position 1, ahead of A's
    in-batch repeat at position 2 - so the refusal names B, and the stored
    holder is the only row that survives."""

    async def scenario(side: DiffSide) -> None:
        holder = _singleton_item(side, "holder_b", "actor_b")
        await side.backend.enqueue(holder)
        batch = [
            _singleton_item(side, "a1", "actor_a"),
            _singleton_item(side, "b1", "actor_b"),
            _singleton_item(side, "a2", "actor_a"),
        ]
        await _run_batch_and_record(side, batch, ["a1", "b1", "a2"], fast=False)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=_ACTORS)
    assert_mirror(
        "a stored singleton holder colliding ahead of an in-batch repeat is "
        "the collision the batch order reaches first - the refusal names "
        "the stored holder's actor, the holder survives, nothing from the "
        "batch is admitted, identically on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "refused:actor_b", (
        "actor_b's item at position 1 hits the stored holder before "
        "actor_a's in-batch repeat at position 2 is ever reached; the "
        f"refusal named {pg['records']['batch']!r}"
    )
    assert pg["records"]["stored_from_batch"] == []
    assert pg["jobs"]["holder_b"]["present"] is True
    assert pg["jobs"]["holder_b"]["status"] == "pending"
