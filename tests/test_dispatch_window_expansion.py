# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Window expansion: an empty dispatch round with claimable rows behind it.

The claim statement's candidate window is deliberately bounded at
``residual * oversample`` rows per (actor, queue) cohort probe, and its
SKIP LOCKED slide ranges only within that materialized window
(``sliding_locked`` in ``taskq.backend._dispatch_sql``). With more than
``oversample`` concurrent dispatchers holding rows of one (actor, queue),
the next dispatcher's whole window is row-locked: without expansion it
returns an empty round while deeper rows sit unlocked - a worker that
reports successful empty rounds and moves no work.

The contract pinned here: a dispatcher may come back empty only when no
unlocked claimable rows remain, and the remedy for a locked-out window -
probe for remaining routable rows, then re-claim with a doubled window -
stays bounded in both directions:

* an idle round (nothing pending) pays one claim statement plus one
  LIMIT-1 probe, never a second claim;
* a round whose backlog is entirely locked by peers re-claims at most
  ``1 + _MAX_DISPATCH_WINDOW_EXPANSIONS`` times, then reports empty and
  defers to the next tick.

Holders are driven through the production claim statement (the
``dispatch_batch`` SQL helper) on raw connections with held-open
transactions; the dispatcher under test goes through the production
round seam (``_dispatch_batch``, or ``PostgresBackend.dispatch_batch``
for the end-to-end wiring) so the seam the fleet actually runs is what
gets pinned.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from asyncpg.transaction import Transaction

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._dispatch import (
    _MAX_DISPATCH_WINDOW_EXPANSIONS,
    _dispatch_batch,
)
from taskq.backend._dispatch_sql import DISPATCH_ROUND_ROBIN_SQL, DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "default"
_ACQUIRE_TIMEOUT = 5.0

# Unique substrings that tell the three statement shapes apart in the
# recorded stream: the claim CTE, the claimable-rows probe, and the
# queue-mode resolve.
_CLAIM_MARKER = "WITH RECURSIVE params AS ("
_PROBE_MARKER = "ac.queue = ANY($1::text[])"

_VARIANT_SQL = {
    "strict_fifo": DISPATCH_STRICT_FIFO_SQL,
    "round_robin": DISPATCH_ROUND_ROBIN_SQL,
}


def _job_args(actor: str, *, priority: int) -> EnqueueArgs:
    """One due, pending, identity-free job for *actor*."""
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        priority=priority,
        metadata={},
    )


async def _seed_actor(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    queue: str = _QUEUE,
) -> None:
    """One uncapped actor_config row - the claim CTE only sees registered actors."""
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
        "ON CONFLICT (actor) DO NOTHING",
        actor,
        queue,
    )


async def _claim_with_open_locks(
    conn: asyncpg.Connection,
    schema: str,
    *,
    limit_n: int,
    oversample: int,
    variant: str = "strict_fifo",
) -> list[asyncpg.Record]:
    """One claim round inside a transaction the CALLER keeps open.

    The caller owns the commit/rollback so the claimed rows stay locked
    (and, being uncommitted, still read as pending to other sessions) for
    the duration of the scenario - the peer-lockout condition under test.
    """
    return await dispatch_batch_sql(
        conn,
        sql=_VARIANT_SQL[variant].format(schema=schema),
        queues=[_QUEUE],
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
        oversample=oversample,
    )


class _CountingConn:
    """Connection proxy that records every statement text it carries.

    Wraps a real pooled connection: the SQL under test runs against the
    real engine, and the test reads back how many claim/probe statements
    one dispatch round actually spent - the round-trip budget is the
    observable cost contract of window expansion.
    """

    def __init__(self, inner: asyncpg.Connection, statements: list[str]) -> None:
        self._inner = inner
        self._statements = statements

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        self._statements.append(sql)
        return await self._inner.fetch(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        return await self._inner.execute(sql, *args)

    def transaction(self, *args: object, **kwargs: object) -> Transaction:
        return self._inner.transaction(*args, **kwargs)


class _CountingPool:
    """Pool proxy vending :class:`_CountingConn` wrappers."""

    def __init__(self, inner: asyncpg.Pool) -> None:
        self._inner = inner
        self.statements: list[str] = []

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire, which _dispatch_batch calls with timeout=.
    ) -> AsyncGenerator[_CountingConn]:
        async with self._inner.acquire(timeout=timeout) as conn:
            yield _CountingConn(conn, self.statements)


async def _dispatch_with_counting(
    pool: _CountingPool,
    schema: str,
    *,
    limit: int,
    oversample: int = 2,
) -> list[JobRow]:
    """One dispatch round through the production round seam."""
    return await _dispatch_batch(
        pool,  # type: ignore[arg-type]  # Why: duck-typed counting proxy delegates acquire/fetch/execute/transaction to the real pool.
        render(schema),
        oversample,
        _ACQUIRE_TIMEOUT,
        schema,
        new_uuid(),
        [_QUEUE],
        limit,
        _LEASE,
        queue_mode_cache=None,
    )


def _claim_statement_count(pool: _CountingPool) -> int:
    return sum(1 for sql in pool.statements if _CLAIM_MARKER in sql)


def _probe_statement_count(pool: _CountingPool) -> int:
    return sum(1 for sql in pool.statements if _PROBE_MARKER in sql)


async def _test_teardown(stack: Any, pg_dsn: str, schema: str) -> None:
    await stack.aclose()
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


@pytest.mark.parametrize("queue_mode", ["strict_fifo", "round_robin"])
async def test_locked_out_window_expands_and_claims_deeper_rows(
    pg_dsn: str, queue_mode: str
) -> None:
    """Two peers hold the whole oversample window; the third dispatcher's
    round must reach the unlocked rows behind it rather than return empty.

    Window arithmetic: limit 5 x oversample 2 covers the top 10 candidates,
    exactly the rows the two holders lock. The production seam answers the
    lockout by doubling the window, so its round comes back with the next
    five rows - the throughput the third pod was added to provide. Runs on
    both queue modes: the expansion is round orchestration and must hold
    whichever claim statement the mode selects.
    """
    schema = f"twe_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "twe_expansion_actor"
        backlog = 40
        limit_n = 5
        async with deps.worker_pool.acquire() as conn:
            if queue_mode == "round_robin":
                await conn.execute(
                    f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
                    "ON CONFLICT (name) DO NOTHING",
                    _QUEUE,
                    queue_mode,
                )
            await _seed_actor(conn, schema, actor)
            args_list = [_job_args(actor, priority=backlog - i) for i in range(backlog)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert [r.id for r in rows] == [a.id for a in args_list], "fixture broken: seeding"
        async with deps.worker_pool.acquire() as conn:
            resolved = await backend.resolve_queue_modes(conn, [_QUEUE], schema)
        assert resolved == {queue_mode}, (
            f"fixture broken: the round must run the {queue_mode} claim variant, "
            f"resolved {resolved}"
        )

        holder1 = await deps.worker_pool.acquire()
        holder2 = await deps.worker_pool.acquire()
        tx1 = holder1.transaction()
        tx2 = holder2.transaction()
        await tx1.start()
        await tx2.start()
        try:
            d1 = await _claim_with_open_locks(
                holder1, schema, limit_n=limit_n, oversample=2, variant=queue_mode
            )
            assert {r["id"] for r in d1} == {a.id for a in args_list[0:5]}, (
                "fixture broken: the first holder must take the top-5 window rows"
            )
            d2 = await _claim_with_open_locks(
                holder2, schema, limit_n=limit_n, oversample=2, variant=queue_mode
            )
            assert {r["id"] for r in d2} == {a.id for a in args_list[5:10]}, (
                "fixture broken: the second holder must slide to rows 6-10, "
                "completing the window lockout"
            )

            dispatched = await backend.dispatch_batch(new_uuid(), [_QUEUE], limit_n, _LEASE)

            assert {r.id for r in dispatched} == {a.id for a in args_list[10:15]}, (
                f"a third dispatcher facing a fully locked window must expand and "
                f"claim the next {limit_n} unlocked rows (ids 11-15 by priority); "
                f"got {[r.id for r in dispatched]}. An empty round here means a pod "
                f"that reports successful polls while moving no work - throughput "
                f"that does not grow when a worker is added."
            )
        finally:
            await tx1.rollback()
            await tx2.rollback()
            await deps.worker_pool.release(holder1)
            await deps.worker_pool.release(holder2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_idle_round_pays_one_probe_and_never_a_second_claim(pg_dsn: str) -> None:
    """The commonest empty round - nothing pending at all - must stay cheap:
    one claim statement and one LIMIT-1 probe, and no widened re-claim."""
    schema = f"twe_{new_base62()}".lower()
    stack, deps, _backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        pool = _CountingPool(deps.dispatcher_pool)
        dispatched = await _dispatch_with_counting(pool, schema, limit=5)

        assert dispatched == [], "fixture broken: an empty schema must dispatch nothing"
        assert _claim_statement_count(pool) == 1, (
            f"an idle round must execute the claim statement exactly once; got "
            f"{_claim_statement_count(pool)} - expansion must only fire when the "
            f"probe sees rows remain"
        )
        assert _probe_statement_count(pool) == 1, (
            f"an idle round must pay exactly one claimable-rows probe; got "
            f"{_probe_statement_count(pool)}"
        )
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_expansion_is_bounded_when_peers_hold_the_whole_backlog(pg_dsn: str) -> None:
    """A round that can never make progress (every pending row is locked by
    a peer) must stop widening after the expansion bound and report empty -
    bounded wasted work, never an unbounded retry burn against a saturated
    queue."""
    schema = f"twe_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "twe_bounded_actor"
        backlog = 40
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            args_list = [_job_args(actor, priority=backlog - i) for i in range(backlog)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == backlog, "fixture broken: seeding"

        holder = await deps.worker_pool.acquire()
        tx = holder.transaction()
        await tx.start()
        try:
            held = await _claim_with_open_locks(holder, schema, limit_n=backlog, oversample=2)
            assert len(held) == backlog, "fixture broken: the holder must lock the entire backlog"

            pool = _CountingPool(deps.dispatcher_pool)
            dispatched = await _dispatch_with_counting(pool, schema, limit=5)

            assert dispatched == [], (
                "every pending row is locked by the peer, so the round must report "
                "empty once the expansion bound is reached"
            )
            assert _claim_statement_count(pool) == 1 + _MAX_DISPATCH_WINDOW_EXPANSIONS, (
                f"the round must stop after {_MAX_DISPATCH_WINDOW_EXPANSIONS} "
                f"expansions (initial claim + re-claims); got "
                f"{_claim_statement_count(pool)} claim executions"
            )
            assert _probe_statement_count(pool) == _MAX_DISPATCH_WINDOW_EXPANSIONS, (
                f"each expansion is gated by one probe; got {_probe_statement_count(pool)} probes"
            )
        finally:
            await tx.rollback()
            await deps.worker_pool.release(holder)
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_probe_ignores_rows_of_unregistered_actors(pg_dsn: str) -> None:
    """Pending rows whose actor has no actor_config row are not dispatchable
    (candidates come FROM the registry), so they must not keep the probe
    alive - otherwise a stranded orphan backlog would tax every empty round
    with wasted expansions."""
    schema = f"twe_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        orphan_actor = "twe_orphan_actor"
        async with deps.worker_pool.acquire() as conn:
            args_list = [_job_args(orphan_actor, priority=i) for i in range(10)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == 10, "fixture broken: seeding"

        pool = _CountingPool(deps.dispatcher_pool)
        dispatched = await _dispatch_with_counting(pool, schema, limit=5)

        assert dispatched == [], (
            "the claim CTE only considers registered actors; the orphan rows must never dispatch"
        )
        assert _claim_statement_count(pool) == 1, (
            f"rows of an unregistered actor must not trigger expansion; got "
            f"{_claim_statement_count(pool)} claim executions"
        )
        assert _probe_statement_count(pool) == 1
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_probe_ignores_rows_on_unsubscribed_queues(pg_dsn: str) -> None:
    """A round is scoped to its own queues: pending rows on a queue the
    worker does not poll are someone else's work, and must not fire
    expansion on this worker's empty round."""
    schema = f"twe_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "twe_elsewhere_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            args_list = [
                EnqueueArgs(
                    id=new_job_id(),
                    actor=actor,
                    queue="elsewhere",
                    payload={"probe": actor},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
                    priority=i,
                    metadata={},
                )
                for i in range(10)
            ]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
        assert len(rows) == 10, "fixture broken: seeding"

        pool = _CountingPool(deps.dispatcher_pool)
        dispatched = await _dispatch_with_counting(pool, schema, limit=5)

        assert dispatched == [], (
            "the round polls [default]; the rows live on [elsewhere] and must not dispatch here"
        )
        assert _claim_statement_count(pool) == 1, (
            f"rows on unsubscribed queues must not trigger expansion; got "
            f"{_claim_statement_count(pool)} claim executions"
        )
        assert _probe_statement_count(pool) == 1
    finally:
        await _test_teardown(stack, pg_dsn, schema)


async def test_expansion_reaches_rows_routed_by_actor_assignment(pg_dsn: str) -> None:
    """The re-pended population is claimable work too: rows that were
    claimed once and handed back (pending with started_at set) route by
    their actor's current assignment, and the probe's assignment-routed
    arm is what licenses expansion when peers hold this window.

    Two holders lock the whole two-row window of re-pended rows; the
    dispatcher's round must expand and claim the third."""
    schema = f"twe_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        actor = "twe_repend_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            args_list = [_job_args(actor, priority=40 - i) for i in range(4)]
            rows: list[JobRow] = await backend.enqueue_batch(args_list, connection=conn)
            first_pass = await _claim_with_open_locks(conn, schema, limit_n=4, oversample=2)
            assert len(first_pass) == 4, "fixture broken: first claim pass"
            # Hand all four back the way every re-pend path does: status
            # returns to pending, the started_at "was claimed" marker stays.
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'pending', "
                "locked_by_worker = NULL, lock_expires_at = NULL",
            )
        assert [r.id for r in rows] == [a.id for a in args_list], "fixture broken: seeding"

        holder1 = await deps.worker_pool.acquire()
        holder2 = await deps.worker_pool.acquire()
        tx1 = holder1.transaction()
        tx2 = holder2.transaction()
        await tx1.start()
        await tx2.start()
        try:
            # Window arithmetic for the re-pended arm: residual (limit 1)
            # x oversample 2 probes the top-2 re-pended rows, so the two
            # holders lock exactly the whole window.
            h1 = await _claim_with_open_locks(holder1, schema, limit_n=1, oversample=2)
            assert [r["id"] for r in h1] == [args_list[0].id], "fixture broken: holder 1"
            h2 = await _claim_with_open_locks(holder2, schema, limit_n=1, oversample=2)
            assert [r["id"] for r in h2] == [args_list[1].id], (
                "fixture broken: holder 2 must slide to the second re-pended row, "
                "completing the window lockout"
            )

            pool = _CountingPool(deps.dispatcher_pool)
            dispatched = await _dispatch_with_counting(pool, schema, limit=1)

            assert [r.id for r in dispatched] == [args_list[2].id], (
                f"with the two-row re-pended window locked by peers, expansion must "
                f"reach the third re-pended row; got {[r.id for r in dispatched]}. "
                f"Re-pended rows are claimable work - an empty round here strands "
                f"retries behind a locked window."
            )
            assert _claim_statement_count(pool) > 1, (
                "the claim must have been re-executed with a widened window for "
                "the third row to be reached"
            )
        finally:
            await tx1.rollback()
            await tx2.rollback()
            await deps.worker_pool.release(holder1)
            await deps.worker_pool.release(holder2)
    finally:
        await _test_teardown(stack, pg_dsn, schema)
