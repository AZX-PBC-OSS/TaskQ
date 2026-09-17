# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team pins: the dispatch CTE under TWO concurrent dispatchers.

Hunted interleavings, worked out on paper from ``_DISPATCH_SQL_TEMPLATE``
before any test was written:

1. ``identity_dedup`` DISTINCT ON under concurrency — "two dispatchers each
   take a different identity sibling in the same round".  The candidates
   lateral (``ORDER BY j2.priority DESC, j2.scheduled_at, j2.id LIMIT
   pac.residual * oversample``) yields a PREFIX of the pending rows in
   exactly the order the dedup uses (``ORDER BY c.actor, c.identity_key,
   c.priority DESC, c.scheduled_at, c.id``), so every dispatcher whose
   window contains ANY sibling of an identity agrees on the SAME winner —
   the globally highest-priority sibling.  The ``locked`` stage then takes
   ``FOR UPDATE OF j SKIP LOCKED`` on that one row: a concurrent holder is
   skipped, and a committed dispatch is dropped by the stage's own
   ``WHERE j.status = 'pending'`` re-check.  Two same-queue dispatchers
   therefore cannot each admit a different sibling in one round — a
   STRONGER safety than the ``running_identities`` TOCTOU comment claims
   ("~<= num_concurrent dispatchers admitted per identity_key per round,
   not a hard 1").  The over-admission the comment documents IS still
   reachable by other windows (round-robin siblings in different fairness
   partitions under divergent residuals, or dispatchers subscribed to
   different queue sets), so this file pins only the same-queue case and
   does not contradict the documented tradeoff.  Nothing pinned this
   before (``tests/test_rt_dispatch_fairness_attack.py`` is explicitly
   single-dispatcher), so it gets green pins.

2. The oversample x SKIP LOCKED slide — "can the eligible re-cap
   under-select so a job is starved across rounds?"  The lateral window is
   computed BEFORE the skip, so a dispatcher whose entire
   ``residual * oversample`` window is row-locked by another dispatcher
   takes ZERO jobs for that actor this round even though deeper unlocked
   rows exist.  The ``eligible`` re-cap cannot under-select by itself: it
   recomputes ``actor_rank`` over the post-skip ``locked`` set and reads
   the same stale-but-never-high ``in_flight`` the candidates stage used.
   The under-selection is round-bounded: committed dispatches leave the
   pending set, the window slides, and the cap arithmetic guarantees
   progress.  Pinned green: one empty round, then progress — never
   cross-round starvation.

All rows are seeded through the production enqueue path and every dispatch
runs the production CTE (``DISPATCH_STRICT_FIFO_SQL``) via the production
``dispatch_batch`` helper on two real pool connections.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobRow
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "default"
_BOUNDED_WAIT_SECS = 20.0


def _ident_args(
    actor: str,
    *,
    identity_key: IdentityKey | None = None,
    priority: int = 0,
    metadata: dict[str, object] | None = None,
) -> EnqueueArgs:
    """One due, pending, batch-enqueueable job for *actor*."""
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        priority=priority,
        identity_key=identity_key,
        metadata=metadata if metadata is not None else {},
    )


async def _seed_actor(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    *,
    max_concurrent: int | None = None,
) -> None:
    """One actor_config row — the dispatch CTE only considers registered actors."""
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "VALUES ($1, $2, $3) ON CONFLICT (actor) DO NOTHING",
        actor,
        _QUEUE,
        max_concurrent,
    )


async def _dispatch(
    conn: asyncpg.Connection,
    schema: str,
    *,
    limit_n: int,
    oversample: int = 2,
) -> list[asyncpg.Record]:
    """One dispatch round via the narrowest production entry point."""
    return await dispatch_batch_sql(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=[_QUEUE],
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
        oversample=oversample,
    )


async def _running_count_for_identity(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    identity_key: IdentityKey,
) -> int:
    val: Any = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE actor = $1 AND identity_key = $2 AND status::text = 'running'",
        actor,
        identity_key,
    )
    assert isinstance(val, int)
    return val


async def _test_teardown(stack: Any, pg_dsn: str, schema: str) -> None:
    await stack.aclose()
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


async def test_second_dispatcher_cannot_take_a_different_identity_sibling(
    pg_dsn: str,
) -> None:
    """Held-open interleaving: D1 locks the DISTINCT ON winner and has not
    committed; D2 dispatches the same round and must come back empty-handed
    for that identity — then the winner's commit and terminal write each
    hand the identity back exactly once."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "tqr_ident_actor"
        identity = IdentityKey(f"tqr-ident-{new_base62()}")
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            args_list = [
                _ident_args(actor, identity_key=identity, priority=priority)
                for priority in (30, 20, 10)
            ]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert [r.id for r in rows] == [a.id for a in args_list], "fixture broken: seeding"
        winner = args_list[0].id
        runner_up = args_list[1].id

        conn1 = await deps.worker_pool.acquire()
        conn2 = await deps.worker_pool.acquire()
        try:
            tx1 = conn1.transaction()
            await tx1.start()
            try:
                d1 = await _dispatch(conn1, schema, limit_n=1)
                assert [r["id"] for r in d1] == [winner], (
                    "fixture broken: the identity_dedup DISTINCT ON winner must be the "
                    "highest-priority sibling"
                )
                d2 = await _dispatch(conn2, schema, limit_n=1)
                assert d2 == [], (
                    "CONTRACT: two dispatchers on the same queue set must not admit two "
                    "siblings of one identity_key in the same round. The candidates "
                    "lateral is a prefix in the dedup's own (priority DESC, scheduled_at, "
                    "id) order, so both dispatchers' identity_dedup picked the SAME "
                    "winner; D2's locked stage must SKIP LOCKED it (D1 holds it) — there "
                    "is no other sibling in the dedup output to fall back to. If a second "
                    "sibling came back, the DISTINCT ON determinism or the SKIP LOCKED "
                    "arbitration is broken."
                )
            except BaseException:
                await tx1.rollback()
                raise
            else:
                await tx1.commit()

            d3 = await _dispatch(conn2, schema, limit_n=1)
            assert d3 == [], (
                "CONTRACT: while one sibling is RUNNING, running_identities "
                "(status = 'running' AND identity_key IS NOT NULL) must exclude the "
                "identity from every later round — single-flight per identity_key across "
                "rounds, not just within one."
            )

            await conn2.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                "finished_at = clock_timestamp() WHERE id = $1",
                winner,
            )
            d4 = await _dispatch(conn2, schema, limit_n=1)
            assert [r["id"] for r in d4] == [runner_up], (
                "CONTRACT: once the running sibling is terminal the identity is eligible "
                "again and the NEXT-highest sibling must dispatch — the exclusion must "
                "never become permanent starvation of the remaining siblings."
            )
            assert await _running_count_for_identity(conn2, schema, actor, identity) == 1
        finally:
            await deps.worker_pool.release(conn1)
            await deps.worker_pool.release(conn2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_gathered_dispatchers_admit_exactly_one_job_per_identity(
    pg_dsn: str,
) -> None:
    """Natural driver: two dispatchers racing on the same queue with
    asyncio.gather. Whatever the interleaving (full overlap, partial, or
    fully serial), the identity admits exactly one job fleet-wide."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "tqr_ident_gather_actor"
        identity = IdentityKey(f"tqr-gather-{new_base62()}")
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            args_list = [
                _ident_args(actor, identity_key=identity, priority=priority)
                for priority in (40, 30, 20, 10)
            ]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == 4, "fixture broken: seeding"

        conn1 = await deps.worker_pool.acquire()
        conn2 = await deps.worker_pool.acquire()
        try:

            async def _one_round(conn: asyncpg.Connection) -> list[asyncpg.Record]:
                async with conn.transaction():
                    return await _dispatch(conn, schema, limit_n=2)

            d1, d2 = await asyncio.wait_for(
                asyncio.gather(_one_round(conn1), _one_round(conn2)),
                timeout=_BOUNDED_WAIT_SECS,
            )
        finally:
            await deps.worker_pool.release(conn1)
            await deps.worker_pool.release(conn2)

        admitted: Sequence[asyncpg.Record] = d1 + d2
        assert len(admitted) == 1, (
            f"CONTRACT: two concurrent same-queue dispatchers must admit exactly ONE "
            f"job for one identity_key per round (same DISTINCT ON winner + FOR UPDATE "
            f"SKIP LOCKED + the locked stage's status='pending' re-check arbitrate); "
            f"got {len(admitted)}: {[r['id'] for r in admitted]}"
        )
        assert all(r["identity_key"] == identity for r in admitted)
        async with deps.worker_pool.acquire() as conn:
            assert await _running_count_for_identity(conn, schema, actor, identity) == 1
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_oversample_window_skip_is_round_bounded_not_cross_round_starvation(
    pg_dsn: str,
) -> None:
    """The slide: D1's eligible stage caps admissions but its locked stage
    already row-locked the whole oversample window; D2's window is computed
    pre-skip from the same top rows, finds every one SKIP LOCKED, and
    returns nothing — even though deeper unlocked rows exist. The pin: that
    under-selection lasts exactly as long as the cap arithmetic says it
    must, and the next eligible round makes progress."""
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "tqr_slide_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor, max_concurrent=1)
            args_list = [_ident_args(actor, priority=priority) for priority in (30, 20, 10, 5)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == 4, "fixture broken: seeding"
        top, second, third, fourth = (a.id for a in args_list)

        conn1 = await deps.worker_pool.acquire()
        conn2 = await deps.worker_pool.acquire()
        try:
            tx1 = conn1.transaction()
            await tx1.start()
            try:
                d1 = await _dispatch(conn1, schema, limit_n=2, oversample=2)
                assert [r["id"] for r in d1] == [top], (
                    "fixture broken: locked takes the top-2 window rows but eligible's "
                    "actor_rank <= max_concurrent - in_flight (1) admits only rank 1"
                )
                d2 = await _dispatch(conn2, schema, limit_n=2, oversample=2)
                deeper_pending: Any = await conn2.fetchval(
                    f'SELECT count(*) FROM "{schema}".jobs '
                    "WHERE id = ANY($1::uuid[]) AND status::text = 'pending'",
                    [third, fourth],
                )
                assert deeper_pending == 2, (
                    "fixture broken: the deeper rows must still be unlocked and pending, "
                    "since they are the capacity D2 is entitled to reach"
                )
                assert d2 == [], (
                    "this actor's cap is saturated: D1 holds the single in-flight slot "
                    f"max_concurrent = 1 allows, so D2 correctly admits nothing. Got {d2!r}. "
                    "The empty result here is cap arithmetic, NOT the candidate window "
                    "failing to widen past locked rows -- an uncapped actor with free rows "
                    "deeper in the backlog must never come back empty, which "
                    "test_fleet_throughput_grows_when_a_second_dispatcher_is_added pins"
                )
            except BaseException:
                await tx1.rollback()
                raise
            else:
                await tx1.commit()

            d_cap = await _dispatch(conn2, schema, limit_n=2, oversample=2)
            assert d_cap == [], (
                "PIN: after D1 commits, in_flight = 1 saturates max_concurrent = 1, so "
                "residual = 0 and the actor is excluded — the over-dispatch damper bound "
                "held this round because D2's window was blocked."
            )

            await conn2.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                "finished_at = clock_timestamp() WHERE id = $1",
                top,
            )
            d3 = await _dispatch(conn2, schema, limit_n=2, oversample=2)
            assert [r["id"] for r in d3] == [second], (
                "CONTRACT: the slide must be round-bounded — once the window's rows "
                "leave the pending set (dispatched/terminal) the window slides down and "
                "the next row must dispatch. A miss here is cross-round starvation of a "
                "row no other dispatcher holds."
            )
        finally:
            await deps.worker_pool.release(conn1)
            await deps.worker_pool.release(conn2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_second_dispatcher_claims_deeper_unlocked_rows(pg_dsn: str) -> None:
    """A dispatcher whose leading candidates are all held by a peer claims
    from the unlocked rows behind them, rather than coming back empty.

    This is what makes a fleet scale: throughput must grow with worker
    count. A dispatcher that returns nothing while claimable work sits
    unlocked in the backlog contributes no throughput, so an operator who
    adds pods pays for capacity that stays idle while one pod saturates.

    The shape here is one large single-actor backlog with ``limit_n`` far
    smaller than the backlog, and a high ``oversample``. The actor is
    uncapped, so there is no concurrency arithmetic that could justify an
    empty result: every row the first dispatcher does not hold is work the
    second is entitled to claim. Contrast
    ``test_oversample_window_skip_is_round_bounded_not_cross_round_starvation``,
    where an empty second result IS correct because the actor's cap is
    saturated.
    """
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "tqr_oversample_actor"
        limit_n = 10
        backlog = 200
        async with deps.worker_pool.acquire() as conn:
            # No max_concurrent cap: this isolates the top_ids/SKIP LOCKED
            # ordering defect from the actor_rank <= residual admission cap
            # exercised by the slide test above.
            await _seed_actor(conn, schema, actor, max_concurrent=None)
            args_list = [_ident_args(actor, priority=backlog - i) for i in range(backlog)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == backlog, "fixture broken: seeding"

        conn1 = await deps.worker_pool.acquire()
        conn2 = await deps.worker_pool.acquire()
        try:
            tx1 = conn1.transaction()
            await tx1.start()
            try:
                d1 = await _dispatch(conn1, schema, limit_n=limit_n, oversample=10)
                assert len(d1) == limit_n, (
                    "fixture broken: D1 must claim exactly limit_n rows from the "
                    "uncontended backlog"
                )
                d1_ids = {r["id"] for r in d1}

                d2 = await _dispatch(conn2, schema, limit_n=limit_n, oversample=10)

                # conn2 runs in its own transaction/snapshot and cannot see D1's
                # uncommitted status UPDATE, so the row-lock (not a status change)
                # is what makes these rows unreachable to D2 -- exactly the
                # SKIP LOCKED behavior under test. The unlocked remainder behind
                # D1's held window is every backlog row D2 did NOT just claim.
                remaining_due = backlog - len(d1_ids | {r["id"] for r in d2})
                assert remaining_due > 0, (
                    "fixture broken: the unlocked remainder of the backlog must still "
                    "be pending and due — this is the work oversample is documented to "
                    "let a second dispatcher reach"
                )

                assert len(d2) > 0, (
                    f"the second dispatcher claimed {len(d2)} rows while "
                    f"{remaining_due} due rows sat unlocked behind the window the "
                    "first dispatcher holds. An uncapped actor imposes no concurrency "
                    "arithmetic that could justify an empty result, so every one of "
                    "those rows was claimable. A dispatcher that comes back empty here "
                    "adds no throughput to the fleet: adding workers cannot increase "
                    "the rate at which the backlog drains"
                )
                assert d1_ids.isdisjoint({r["id"] for r in d2}), (
                    "fixture broken: D1 and D2 must never double-claim the same row"
                )
            except BaseException:
                await tx1.rollback()
                raise
            else:
                await tx1.commit()
        finally:
            await deps.worker_pool.release(conn1)
            await deps.worker_pool.release(conn2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


# ── Fleet throughput across several actors ─────────────────────────────


async def _seed_multi_actor_backlog(
    deps: Any,
    backend: Any,
    schema: str,
    actors: Sequence[str],
    depth: int,
) -> None:
    """Register each actor uncapped on the round's queue and give it *depth*
    due, equal-priority pending jobs, through the production enqueue path.

    Uncapped throughout: no actor's own configuration limits how much of it
    may run, so anything a round leaves unclaimed was left by selection.
    """
    async with deps.worker_pool.acquire() as conn:
        for actor in actors:
            await _seed_actor(conn, schema, actor, max_concurrent=None)
        args_list = [_ident_args(actor) for actor in actors for _ in range(depth)]
        rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
    assert len(rows) == len(actors) * depth, "fixture broken: seeding"


async def _complete(conn: asyncpg.Connection, schema: str, ids: Sequence[Any]) -> None:
    """Take the round's claims terminal, as workers that keep up would."""
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() WHERE id = ANY($1::uuid[])",
        list(ids),
    )


async def test_second_dispatcher_claims_work_from_other_actors(pg_dsn: str) -> None:
    """A second pod must be able to claim work belonging to actors whose jobs
    the first pod is not holding.

    Operator shape: several actors share a queue, all with deep backlogs, and
    a second worker is added to drain it faster. Whatever rows the first
    worker holds for the duration of its round, the other actors' jobs are
    untouched and unlocked. If the second worker comes back with nothing, the
    pod costs money and moves no work -- and because dispatch reports a
    successful round of zero rows, nothing distinguishes it from an empty
    queue.
    """
    schema = f"tqr_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actors = [f"tqr_fleet_{i}" for i in range(4)]
        depth = 40
        await _seed_multi_actor_backlog(deps, backend, schema, actors, depth)

        conn1 = await deps.worker_pool.acquire()
        conn2 = await deps.worker_pool.acquire()
        try:
            tx1 = conn1.transaction()
            await tx1.start()
            try:
                d1 = await _dispatch(conn1, schema, limit_n=8)
                assert len(d1) > 0, "fixture broken: the first dispatcher claimed nothing"
                held_actors = {r["actor"] for r in d1}
                untouched = [a for a in actors if a not in held_actors]

                d2 = await _dispatch(conn2, schema, limit_n=8)

                still_pending: Any = await conn2.fetchval(
                    f"SELECT count(*) FROM \"{schema}\".jobs WHERE status::text = 'pending'"
                )
                assert still_pending >= len(actors) * depth - len(d1), (
                    "fixture broken: the backlog must still be deep and pending"
                )
                assert len(d2) > 0, (
                    f"a second concurrent dispatcher claimed nothing while "
                    f"{still_pending} jobs were pending across {len(actors)} uncapped "
                    f"actors and the first dispatcher held only {len(d1)} rows "
                    f"(actors {sorted(held_actors)}; actors {sorted(untouched)} were not "
                    f"touched at all). Adding a worker adds no throughput, and the idle "
                    f"worker's dispatch rounds are indistinguishable from an empty queue."
                )
                assert {r["id"] for r in d1}.isdisjoint({r["id"] for r in d2}), (
                    "fixture broken: two dispatchers must never claim the same row"
                )
            except BaseException:
                await tx1.rollback()
                raise
            else:
                await tx1.commit()
        finally:
            await deps.worker_pool.release(conn1)
            await deps.worker_pool.release(conn2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_fleet_throughput_grows_when_a_second_dispatcher_is_added(
    pg_dsn: str,
) -> None:
    """Two dispatchers against a deep multi-actor backlog must drain it
    substantially faster than one.

    This is the capacity question an operator actually asks before scaling:
    if I double the workers, do I drain roughly twice as fast? Both runs get
    the same backlog, the same per-round limit and the same number of rounds,
    and every round's claims are completed so no run is held back by its own
    in-flight jobs. The single-dispatcher run is the baseline; the two-
    dispatcher run must beat it by a clear margin. Throughput that does not
    move with worker count means the queue cannot be scaled out at all, and
    the only remaining lever is a bigger per-worker batch.
    """
    actors = [f"tqr_scale_{i}" for i in range(4)]
    depth = 60
    limit_n = 5
    rounds = 10

    async def _drain_with_one(schema: str) -> int:
        stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
        try:
            await _seed_multi_actor_backlog(deps, backend, schema, actors, depth)
            conn = await deps.worker_pool.acquire()
            try:
                total = 0
                for _ in range(rounds):
                    async with conn.transaction():
                        claimed = await _dispatch(conn, schema, limit_n=limit_n)
                    total += len(claimed)
                    await _complete(conn, schema, [r["id"] for r in claimed])
                return total
            finally:
                await deps.worker_pool.release(conn)
        finally:
            await _test_teardown(stack, pg_dsn, schema)

    async def _drain_with_two(schema: str) -> int:
        stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
        try:
            await _seed_multi_actor_backlog(deps, backend, schema, actors, depth)
            conn1 = await deps.worker_pool.acquire()
            conn2 = await deps.worker_pool.acquire()
            try:

                async def _round(conn: asyncpg.Connection) -> list[asyncpg.Record]:
                    async with conn.transaction():
                        return await _dispatch(conn, schema, limit_n=limit_n)

                total = 0
                for _ in range(rounds):
                    r1, r2 = await asyncio.wait_for(
                        asyncio.gather(_round(conn1), _round(conn2)),
                        timeout=_BOUNDED_WAIT_SECS,
                    )
                    claimed_ids = {r["id"] for r in r1} | {r["id"] for r in r2}
                    total += len(claimed_ids)
                    await _complete(conn1, schema, list(claimed_ids))
                return total
            finally:
                await deps.worker_pool.release(conn1)
                await deps.worker_pool.release(conn2)
        finally:
            await _test_teardown(stack, pg_dsn, schema)

    one = await _drain_with_one(f"tqr_{new_base62()}".lower())
    two = await _drain_with_two(f"tqr_{new_base62()}".lower())

    assert one > 0, "fixture broken: the single-dispatcher baseline drained nothing"
    assert one < len(actors) * depth, (
        "fixture broken: the backlog must outlast the round budget, or there is no "
        "headroom for a second dispatcher to use"
    )
    assert two >= one * 3 // 2, (
        f"doubling the dispatchers barely moved throughput: one dispatcher drained "
        f"{one} jobs in {rounds} rounds at limit {limit_n}; two dispatchers drained "
        f"{two} over the same {rounds} rounds against an identical "
        f"{len(actors) * depth}-job backlog across {len(actors)} uncapped actors. "
        f"The second worker is paid for and idle, and the backlog clears at very "
        f"nearly the single-worker rate."
    )
