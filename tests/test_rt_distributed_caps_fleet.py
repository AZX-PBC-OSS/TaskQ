# ruff: noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream; every value is $-bound.
"""Red-team pins: every count-bounded statement under TWO concurrent workers.

The hunt class: a statement whose cap arithmetic is per-worker but whose
semantics are fleet-wide (or vice versa). The dispatch claim's capped-actor
CTE is the anchor (#435): its ``max_concurrent`` is a documented per-round
admission damper whose over-dispatch bound ``(num_producers - 1) *
max_concurrent`` is pinned by ``test_fleet_concurrency_cap.py`` and
``test_fleet_dispatch_cap_overadmission.py``. This file attacks the OTHER
count-bounded statements the anchor's doctrine does not cover, each with a
real two-worker fleet shape: two pools, a synchronized start, and every
clock predicate anchored server-side (statement_timestamp() /
clock_timestamp()), never Python time.

The statements and the fleet shape each pin:

1. The bulk-cancel drain (``_cancel_bulk.py``), two operators cancelling the
   same predicate concurrently. The drain's fences: the keyset cursor, the
   ``matching`` CTE's snapshot window, the UPDATE's EPQ status re-check, the
   WINDOW-count termination. Two concurrent drains must cancel every matching
   row EXACTLY once - the loser's batches ride EPQ drops to zero, and the
   exact-once property is what the event stream records. The single-drain
   pins (``test_cancel_where_bounded.py``) cannot see the two-drainer shape:
   within one drain the cursor alone prevents re-walks, so only a concurrent
   second drain can expose a missing EPQ re-check as a double cancel.

2. The crash-reclaim sweep (``_sweeps.py`` ``_SWEEP_1_SQL``), two sweepers
   racing during a leadership flap (the sweep comments name this shape:
   "possible during a rolling deploy before the leader lock names converge").
   Each call is bounded by its arms' LIMITs; the SKIP LOCKED snap must make
   two concurrent sweepers DISJOINT: every eligible row reclaimed exactly
   once across both loops, one attempt row and one event per reclaim.

3. The force-deregister drain (``actor_config_ops.py``), two operators
   deregistering the same actor concurrently. The flip's concurrent-delete
   race is handled (one ``ActorNotFoundError``); the DRAIN both operators run
   must still cancel exactly once, the same EPQ doctrine the cancel drain
   carries.

4. The single-enqueue ``max_pending`` cap (``_enqueue.py``), two producer
   pools racing capped enqueues. The count-then-insert is serialized per
   actor by a transaction-scoped advisory lock, so the cap is STRICT
   fleet-wide: pending rows for the actor may never exceed the cap, no
   matter which producer's pool wins the race. (The BULK tier's
   count-then-insert race is documented residual, deliberately unlocked for
   throughput; the single path is the strict one and is what this pins.)

All rows are seeded through direct SQL with server-side clocks; every
candidate predicate the statements under test read is anchored to
``statement_timestamp()``/``clock_timestamp()``, so the repros do not depend
on host clock alignment.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config_ops import deregister_actor
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._enqueue import _enqueue
from taskq.backend._protocol import EnqueueArgs, JobFilter
from taskq.backend._sql_templates import render
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import (
    ActorNotFoundError,
    MaxPendingExceededError,
    MaxPendingLockTimeoutError,
)
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)
# Small enough that several batches run per drain, large enough that the
# boundary (a final under-full window) is reached by BOTH drains.
_BATCH = 8
# 2 full batches + a partial tail: the last window both drains race on is
# the under-full boundary.
_MATCH_SET = 2 * _BATCH + 7
_TAG = "distributed-caps"


async def _seed_pending_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    actor: str = "caps_actor",
) -> None:
    """Seed *job_ids* pending and due, tagged for the cancel filters."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, $2, 'default', '{{}}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds', $3::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        actor,
        [_TAG],
    )


async def _seed_reclaimable_running(
    conn: asyncpg.Connection, schema: str, count: int
) -> tuple[UUID, list[UUID]]:
    """Seed *count* running jobs whose lock expired, held by one worker row."""
    worker_id = new_uuid()
    job_ids = [new_uuid() for _ in range(count)]
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        worker_id,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
        " scheduled_at, locked_by_worker, lock_expires_at, started_at) "
        "SELECT t.id, 'caps_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', "
        "1, clock_timestamp(), $2, "
        "clock_timestamp() - interval '10 seconds', clock_timestamp() - interval '30 seconds' "
        "FROM unnest($1::uuid[]) AS t(id)",
        job_ids,
        worker_id,
    )
    return worker_id, job_ids


async def _open_second_pool(pg_dsn: str) -> asyncpg.Pool:
    """Worker B's own pool against the same schema: the fleet shape."""
    return await asyncpg.create_pool(pg_dsn, min_size=1, max_size=8)


async def _event_count(conn: asyncpg.Connection, schema: str, job_ids: Sequence[UUID]) -> int:
    """state_change events written for *job_ids*: the exact-once ledger."""
    val = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events '
        "WHERE job_id = ANY($1::uuid[]) AND kind = 'state_change'",
        list(job_ids),
    )
    assert isinstance(val, int)
    return val


# ── Pin 1: two concurrent bulk-cancel drains ─────────────────────────────


async def test_two_concurrent_cancel_drains_cancel_exactly_once(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Two operators bulk-cancel the same predicate at the same instant.

    Contract: every matching row reaches terminal 'cancelled' exactly once,
    the event stream carries exactly one state_change per row (no drain's
    batch re-cancels a row the other drain cancelled), and both drains
    terminate on the WINDOW count without hanging or double-counting.
    """
    schema = module_pg_schema.schema_name
    job_ids = [new_uuid() for _ in range(_MATCH_SET)]
    await _seed_pending_jobs(clean_pg_conn, schema, job_ids)
    pool_a = await _open_second_pool(module_pg_schema.pg_dsn)
    pool_b = await _open_second_pool(module_pg_schema.pg_dsn)
    barrier = asyncio.Barrier(2)
    sql = render(schema)

    async def one_drain(pool: asyncpg.Pool) -> tuple[int, int]:
        result, _notify = await _cancel_where(
            pool,
            schema,
            sql,
            JobFilter(tags=(_TAG,)),
            "offboard",
            batch_size=_BATCH,
        )
        return result.cancelled_directly, len(result.cancelled_ids)

    async def raced(pool: asyncpg.Pool) -> tuple[int, int]:
        async with barrier:
            return await one_drain(pool)

    try:
        results = await asyncio.gather(raced(pool_a), raced(pool_b))
    finally:
        await pool_a.close()
        await pool_b.close()

    total = sum(r[0] for r in results)
    assert total == _MATCH_SET, (
        f"the two concurrent drains reported {total} cancellations for a "
        f"{_MATCH_SET}-row match set: the drains must partition the set "
        "exactly once across them (the loser's overlapping batches ride EPQ "
        "drops to zero), never double-count"
    )
    assert all(r[0] == r[1] for r in results), (
        "cancelled_directly and cancelled_ids must agree per drain: the "
        "reported count and the id list are the same rows"
    )
    events = await _event_count(clean_pg_conn, schema, job_ids)
    assert events == _MATCH_SET, (
        f"{events} state_change events for {_MATCH_SET} cancelled rows: a "
        "missing EPQ status re-check on the driving UPDATE lets the second "
        "drain re-cancel rows the first committed, and the event stream "
        "doubles - the exact-once ledger is the fleet-wide contract"
    )
    pending = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE tags @> $1::text[] AND status::text <> 'cancelled'",
        [_TAG],
    )
    assert pending == 0, "every matching row must be terminal 'cancelled' after both drains"


# ── Pin 2: two concurrent crash-reclaim sweepers ─────────────────────────


async def test_two_concurrent_reclaim_sweeps_reclaim_exactly_once(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Two sweepers race during a leadership flap over one expired-lock pool.

    Contract: the SKIP LOCKED snap makes the two loops' claims disjoint,
    every eligible row is reclaimed exactly once across both loops (one
    attempt row, one event each), each call stays within its arm LIMITs, and
    both loops terminate on the drained set.
    """
    schema = module_pg_schema.schema_name
    _worker_id, job_ids = await _seed_reclaimable_running(clean_pg_conn, schema, _MATCH_SET)
    pool_a = await _open_second_pool(module_pg_schema.pg_dsn)
    pool_b = await _open_second_pool(module_pg_schema.pg_dsn)
    barrier = asyncio.Barrier(2)

    async def one_sweeper(pool: asyncpg.Pool) -> int:
        """Loop the sweep until it reports a drained eligible set."""
        total = 0
        async with barrier:
            while True:
                async with pool.acquire() as conn:
                    count = await PostgresBackend.sweep_expired_locks(
                        conn,
                        _CANCEL_GRACE,
                        _CLEANUP_GRACE,
                        schema=schema,
                        batch_size=_BATCH,
                    )
                total += count
                if count == 0:
                    return total

    try:
        totals = await asyncio.gather(one_sweeper(pool_a), one_sweeper(pool_b))
    finally:
        await pool_a.close()
        await pool_b.close()

    assert sum(totals) == _MATCH_SET, (
        f"the two concurrent sweepers reclaimed {sum(totals)} rows for a "
        f"{_MATCH_SET}-row eligible set: the snaps must partition the set "
        "exactly once across both loops (SKIP LOCKED disjointness), never "
        "double-reclaim"
    )
    for total in totals:
        assert total <= _MATCH_SET, (
            f"one sweeper loop reclaimed {total} rows: no loop may exceed the "
            "eligible set, a count above it means a row was transitioned twice"
        )
    attempts = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts '
        "WHERE job_id = ANY($1::uuid[]) AND outcome = 'crashed'",
        job_ids,
    )
    events = await _event_count(clean_pg_conn, schema, job_ids)
    assert isinstance(attempts, int)
    assert attempts == _MATCH_SET, (
        f"{attempts} crash attempt rows for {_MATCH_SET} reclaimed rows: a "
        "sweeper that re-transitioned a row the other sweeper reclaimed "
        "would add a second event even where the (job_id, attempt) PK hides "
        "it - the event ledger is the exact-once check"
    )
    assert events == _MATCH_SET, (
        f"{events} state_change events for {_MATCH_SET} reclaims: exactly one "
        "per reclaimed row across both concurrent loops"
    )
    still_running = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE id = ANY($1::uuid[]) AND status = 'running'",
        job_ids,
    )
    assert still_running == 0, "no eligible row may survive both concurrent loops running"


# ── Pin 3: two concurrent force-deregister drains ────────────────────────


async def test_two_concurrent_force_deregisters_cancel_exactly_once(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Two operators deregister the same actor (force=True) at once.

    Contract: the flip's concurrent-delete race is arbitrated (exactly one
    operator completes, the other gets the documented ActorNotFoundError),
    and the two concurrent drains still cancel every pending row exactly
    once - one state_change event per row, no row left behind, the same
    exact-once doctrine the cancel drain carries.
    """
    schema = module_pg_schema.schema_name
    actor = "caps_deregister_actor"
    job_ids = [new_uuid() for _ in range(_MATCH_SET)]
    await _seed_pending_jobs(clean_pg_conn, schema, job_ids, actor=actor)
    await clean_pg_conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_concurrent) '
        "VALUES ($1, 'default', NULL) ON CONFLICT (actor) DO NOTHING",
        actor,
    )
    pool_a = await _open_second_pool(module_pg_schema.pg_dsn)
    pool_b = await _open_second_pool(module_pg_schema.pg_dsn)
    barrier = asyncio.Barrier(2)

    async def one_operator(pool: asyncpg.Pool) -> str:
        async with barrier, pool.acquire() as conn:
            try:
                await deregister_actor(conn, actor, force=True, schema=schema)
            except ActorNotFoundError:
                # The flip's concurrent-delete race: exactly one
                # operator's finalize deletes the row, the loser's
                # finalize raises the documented error.
                return "lost-the-flip"
            return "completed"

    try:
        outcomes = await asyncio.gather(one_operator(pool_a), one_operator(pool_b))
    finally:
        await pool_a.close()
        await pool_b.close()

    assert sorted(outcomes) == ["completed", "lost-the-flip"], (
        f"exactly one concurrent deregister may complete (the flip's delete "
        f"arbitrates), got {outcomes!r}"
    )
    events = await _event_count(clean_pg_conn, schema, job_ids)
    assert events == _MATCH_SET, (
        f"{events} state_change events for {_MATCH_SET} rows: two concurrent "
        "force-drains must cancel exactly once each row, the same EPQ "
        "doctrine the bulk-cancel drain carries"
    )
    cancelled = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status::text = 'cancelled'",
        actor,
    )
    assert cancelled == _MATCH_SET, "every one of the actor's pending rows must be cancelled"
    config_gone = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor,
    )
    assert config_gone == 0, "the actor_config row must be gone after the fleet outcome settles"


# ── Pin 4: two producer pools against one strict max_pending cap ─────────


async def test_two_producer_pools_max_pending_cap_is_fleet_strict(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Two producer pools race capped single enqueues for one actor.

    Contract: the single path's count-then-insert is serialized per actor by
    a transaction-scoped advisory lock, so the cap is EXACT fleet-wide: the
    number of live pending rows for the actor may never exceed the cap no
    matter which pool wins the races. A per-worker cap arithmetic (each pool
    counting only its own rows) would land at num_pools x cap here.
    """
    schema = module_pg_schema.schema_name
    actor = "caps_max_pending_actor"
    cap = 10
    per_pool = 15
    pool_a = await _open_second_pool(module_pg_schema.pg_dsn)
    pool_b = await _open_second_pool(module_pg_schema.pg_dsn)
    sql = render(schema)
    clock = SystemClock()

    def one_arg() -> EnqueueArgs:
        return EnqueueArgs(
            id=new_job_id(),
            actor=actor,
            queue="default",
            payload={"probe": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
            max_pending=cap,
        )

    async def produce(pool: asyncpg.Pool, n: int) -> list[int]:
        """Fire *n* CONCURRENT single enqueues from *pool*; (accepted, refused).

        The racers must overlap for the fleet shape to be real: two pools
        each with many in-flight capped enquiries is the production burst
        the advisory lock exists to serialize. A sequential caller never
        overlaps its own pool's count-then-insert windows, so it cannot see
        a per-worker cap arithmetic even when one is broken.
        """
        counts = [0, 0]

        async def one() -> None:
            try:
                await _enqueue(pool, sql, schema, clock, one_arg())
                counts[0] += 1
            except MaxPendingExceededError:
                counts[1] += 1
            except MaxPendingLockTimeoutError:
                # The bounded advisory wait exhausted under the two-pool
                # contention this test creates: the documented typed
                # backpressure treatment, a refusal like any other.
                counts[1] += 1

        await asyncio.gather(*(one() for _ in range(n)))
        return counts

    try:
        outcome = await asyncio.gather(
            produce(pool_a, per_pool),
            produce(pool_b, per_pool),
        )
    finally:
        await pool_a.close()
        await pool_b.close()

    assert outcome[0][0] + outcome[1][0] > 0, "fixture broken: no enqueue succeeded at all"
    pending = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE actor = $1 AND status::text IN ('pending', 'scheduled')",
        actor,
    )
    assert isinstance(pending, int)
    assert pending <= cap, (
        f"{pending} live pending rows against a cap of {cap}: the single "
        "path's advisory-lock serialization makes the cap exact fleet-wide, "
        "so two racing producer pools must not land at num_pools x cap"
    )
    refused = outcome[0][1] + outcome[1][1]
    assert outcome[0][0] + outcome[1][0] + refused == 2 * per_pool, (
        "every enqueue either committed or was refused with the typed "
        "backpressure error - nothing may vanish between the two outcomes"
    )
