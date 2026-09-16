"""A batch enqueue failing mid-way under a CALLER-OWNED transaction leaves
the caller's transaction usable.

The bulk tiers' singleton arm is a documented catch-and-continue refusal:
``SingletonCollisionError`` is a typed backpressure signal a caller is meant
to catch and move on from. But the ``jobs_singleton_uniq`` violation that
produces it is a STATEMENT error — on a caller-owned open transaction it
aborts the whole transaction, and converting it to the typed error does not
un-abort anything. The batch tiers therefore wrap the INSERT/COPY in a
savepoint when the batch carries singleton items
(``_optional_savepoint`` in src/taskq/backend/_enqueue.py): the violation
rolls back to the savepoint, the post-abort attribution SELECT runs inside
the caller's restored scope, and the typed refusal raises with the caller's
transaction exactly as TaskQ found it.

The single-enqueue twin of this contract is pinned in
tests/test_rt_pools_enqueue_caller_conn.py; the unit shape of the batch
savepoint is pinned in
tests/test_singleton_violation_savepoint_isolation.py. What neither pins is
the batch tier driven end to end against the real engine: the typed refusal
out, the caller's transaction still answering statements, a follow-up
enqueue INSIDE the same transaction committing, and nothing from the
poisoned batch surviving — with the in-memory twin recording the identical
caller-visible outcome (same typed error, same attributed actor, same
stored state) so a suite validated against it sees the refusal shape
production produces.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.exceptions import SingletonCollisionError

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


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


def _followup_args(side: DiffSide) -> EnqueueArgs:
    """One plain job enqueued after the refusal — the proof the caller's
    scope kept working."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="actor_a",
        queue="default",
        payload={"value": 9},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
    )
    side.register_job_id("followup", args.id)
    return args


def _poisoned_batch(side: DiffSide) -> list[EnqueueArgs]:
    """[singleton A, singleton A]: the in-batch repeat violates
    ``jobs_singleton_uniq`` mid-statement on PG; the mirror's batch
    preflight refuses the same call before storing a row."""
    return [
        _singleton_item(side, "b1", "actor_a"),
        _singleton_item(side, "b2", "actor_a"),
    ]


async def _record_outcome(side: DiffSide, batch: list[EnqueueArgs], followup: EnqueueArgs) -> None:
    stored: list[str] = []
    for token, args in (("b1", batch[0]), ("b2", batch[1])):
        if await side.backend.get(args.id) is not None:
            stored.append(token)
    side.record("stored_from_batch", stored)
    side.record("followup_stored", await side.backend.get(followup.id) is not None)


async def test_diff_batch_singleton_refusal_inside_caller_transaction_keeps_it_usable(
    pg_dsn: str,
) -> None:
    """The unnest tier: the typed refusal raises, the caller's open
    transaction still answers, the follow-up enqueue inside it commits, and
    nothing from the batch survives — on both backends."""

    async def scenario(side: DiffSide) -> None:
        batch = _poisoned_batch(side)
        followup = _followup_args(side)
        if side.kind == "pg":
            import asyncpg

            caller = await asyncpg.connect(pg_dsn)
            try:
                async with caller.transaction():
                    try:
                        await side.backend.enqueue_batch(batch, connection=caller)
                        side.record("batch", "admitted-all")
                    except SingletonCollisionError as exc:
                        side.record("batch", f"refused:{exc.actor}")
                    # The refusal is one a caller is meant to catch and move
                    # on from: the transaction it arrived inside must still
                    # accept statements.
                    alive = await caller.fetchval("SELECT 1")
                    assert alive == 1, (
                        "the typed refusal aborted the caller's transaction — "
                        "the savepoint the batch tier owes the singleton arm "
                        "did not restore the caller's scope"
                    )
                    await side.backend.enqueue_with_conn(caller, followup)
            finally:
                await caller.close()
        else:
            try:
                await side.backend.enqueue_batch(batch)
                side.record("batch", "admitted-all")
            except SingletonCollisionError as exc:
                side.record("batch", f"refused:{exc.actor}")
            await side.backend.enqueue(followup)
        await _record_outcome(side, batch, followup)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=("actor_a",))
    assert_mirror(
        "a mid-batch singleton collision under a caller-owned transaction "
        "raises the same typed refusal naming the same actor, stores nothing "
        "from the batch, and leaves the caller's scope usable for the "
        "follow-up write, on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "refused:actor_a"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["records"]["followup_stored"] is True
    assert pg["status_counts"] == {"pending": 1}


async def test_diff_copy_singleton_refusal_inside_caller_transaction_keeps_it_usable(
    pg_dsn: str,
) -> None:
    """The COPY tier carries the same savepoint discipline: the typed
    refusal raises with the caller's transaction usable and the whole batch
    aborted — COPY has no ON CONFLICT arbiter, so all-or-nothing is the
    documented bulk-import semantics."""

    async def scenario(side: DiffSide) -> None:
        batch = _poisoned_batch(side)
        followup = _followup_args(side)
        if side.kind == "pg":
            import asyncpg

            caller = await asyncpg.connect(pg_dsn)
            try:
                async with caller.transaction():
                    try:
                        await side.backend.enqueue_batch_fast(batch, connection=caller)
                        side.record("batch", "admitted-all")
                    except SingletonCollisionError as exc:
                        side.record("batch", f"refused:{exc.actor}")
                    alive = await caller.fetchval("SELECT 1")
                    assert alive == 1, (
                        "the COPY tier's typed refusal aborted the caller's "
                        "transaction — the savepoint around the COPY did not "
                        "restore the caller's scope"
                    )
                    await side.backend.enqueue_with_conn(caller, followup)
            finally:
                await caller.close()
        else:
            try:
                await side.backend.enqueue_batch_fast(batch)
                side.record("batch", "admitted-all")
            except SingletonCollisionError as exc:
                side.record("batch", f"refused:{exc.actor}")
            await side.backend.enqueue(followup)
        await _record_outcome(side, batch, followup)

    mem, pg = await run_differential(scenario, pg_dsn=pg_dsn, actors=("actor_a",))
    assert_mirror(
        "a mid-COPY singleton collision under a caller-owned transaction "
        "raises the same typed refusal naming the same actor, stores "
        "nothing, and leaves the caller's scope usable, on both backends",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == "refused:actor_a"
    assert pg["records"]["stored_from_batch"] == []
    assert pg["records"]["followup_stored"] is True
    assert pg["status_counts"] == {"pending": 1}
