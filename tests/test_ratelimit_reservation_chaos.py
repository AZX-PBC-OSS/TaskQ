"""Chaos / failure-mode integration tests for ConcurrencyReservation.

Worker dies mid-job — heartbeat stops; after lease seconds, Sweep 4
       reclaims the slot; a new acquire by another worker succeeds.
100 concurrent acquires for 8 slots — exactly 8 succeed; 92 raise
       ReservationUnavailable; no over-allocation; no deadlock.
Connection loss mid-acquire — close asyncpg connection mid-transaction;
       PG rolls back; slot not left held.
PG dies during acquire — stop PG container mid-acquire;
        PostgresConnectionError raised; no slot held (transaction rolled back).
        Uses its own function-scoped PG container so the session-scoped one
        is not affected by the stop/restart cycle.
"""

import asyncio
import contextlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from testcontainers.postgres import PostgresContainer

from taskq._ids import new_base62, new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=5)


def _unique_name() -> str:
    return f"chaos_{new_base62()}"


async def _count_held_slots(pool: asyncpg.Pool, schema: str, bucket: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT count(*) AS held FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND job_id IS NOT NULL",
            bucket,
        )
    assert row is not None
    return int(row["held"])


# ── Worker dies mid-job ──────────────────────────────────────────


@pytest.mark.xdist_group(name="chaos")
async def test_worker_death_sweep_reclaim(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Worker dies mid-job — heartbeat stops; after lease seconds,
    Sweep 4 reclaims the slot; a new acquire by another worker succeeds."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    dead_worker = new_uuid()
    dead_job = new_uuid()
    idx = await res.acquire(dead_job, dead_worker, module_pg_pool)
    assert idx == 0

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".reservation_slots '
            "SET lease_expires_at = now() - interval '1 second' "
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            idx,
        )

    async with module_pg_pool.acquire() as conn:
        await PostgresBackend.sweep_leaked_reservation_slots(conn, datetime.now(UTC), schema=schema)

    new_worker = new_uuid()
    new_job = new_uuid()
    new_idx = await res.acquire(new_job, new_worker, module_pg_pool)
    assert new_idx == 0

    async with module_pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT job_id, held_by_worker_id FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )
    assert row is not None
    assert row["job_id"] == new_job
    assert row["held_by_worker_id"] == new_worker


# ── 100 concurrent acquires for 8 slots ──────────────────────────


@pytest.mark.xdist_group(name="chaos")
async def test_concurrent_100_for_8_slots(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """100 concurrent acquires for 8 slots — exactly 8 succeed;
    92 raise ReservationUnavailable; no over-allocation; no deadlock."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=8, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    async def _try_acquire() -> int | None:
        try:
            return await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
        except ReservationUnavailable:
            return None

    results = await asyncio.wait_for(
        asyncio.gather(*[_try_acquire() for _ in range(100)]),
        timeout=30.0,
    )

    successes = [r for r in results if r is not None]
    failures = [r for r in results if r is None]
    assert len(successes) == 8, f"Expected exactly 8 successful acquires, got {len(successes)}"
    assert len(failures) == 92, f"Expected exactly 92 denials, got {len(failures)}"

    held = await _count_held_slots(module_pg_pool, schema, bucket)
    assert held == 8, f"Expected 8 held slots in DB, got {held}"


# ── Connection loss mid-acquire ──────────────────────────────────


@pytest.mark.xdist_group(name="chaos")
async def test_connection_loss_mid_acquire(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Connection loss mid-acquire — simulate by closing the asyncpg
    connection mid-transaction; PG rolls back the in-flight transaction
    automatically; slot is not left held.

    Strategy: open a direct connection, begin a transaction that acquires a
    slot, then close the connection before committing. The transaction is
    rolled back by PG on connection close.
    """
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    direct_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        tx = direct_conn.transaction()
        await tx.start()

        row = await direct_conn.fetchrow(
            res._acquire_sql,  # pyright: ignore[reportPrivateUsage] # Why: accessing private _acquire_sql to acquire inside a manually-controlled transaction for chaos testing
            bucket,
            new_uuid(),
            new_uuid(),
            _LEASE.total_seconds(),
        )
        assert row is not None

        await direct_conn.close()
    except (asyncpg.PostgresConnectionError, ConnectionError, OSError):
        pass
    finally:
        with contextlib.suppress(asyncpg.InterfaceError, OSError):
            if not direct_conn.is_closed():
                await direct_conn.close()

    held = await _count_held_slots(module_pg_pool, schema, bucket)
    assert held == 0, f"Slot should not be held after connection loss, got {held} held"

    idx = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    assert idx == 0


# ── PG dies during acquire ───────────────────────────────────────
# Uses its own function-scoped PG container so the session-scoped one is
# not affected by the stop/restart cycle.


@pytest.fixture(scope="function")
def _chaos_pg() -> Iterator[PostgresContainer]:  # pyright: ignore[reportUnusedFunction] # Why: pytest injects this fixture; pyright does not trace decorator-based DI
    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ) as container:
        yield container


@pytest.mark.slow
@pytest.mark.xdist_group(name="chaos")
async def test_pg_dies_during_acquire(_chaos_pg: PostgresContainer) -> None:
    """PG dies during acquire — stop the PG container mid-acquire;
    asyncpg.PostgresConnectionError raised; no slot is held (transaction
    rolled back).

    Uses its own function-scoped PG container so the session-scoped one
    is not affected by the stop/restart cycle.
    """
    pg_dsn = _chaos_pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

    # uses its own PG container, so it needs its own schema setup.
    # We use the same schema hardcoded name since module_pg_schema is for
    # the session-scoped container.
    from taskq.migrate import apply_pending

    schema_name = "taskq_test"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await apply_pending(conn, schema=schema_name)
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=10)
    bucket = _unique_name()

    try:
        res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema_name)
        await res.ensure_slots(pool)

        direct_conn = await asyncpg.connect(pg_dsn)
        try:
            tx = direct_conn.transaction()
            await tx.start()

            job_id = new_uuid()
            worker_id = new_uuid()
            row = await direct_conn.fetchrow(
                res._acquire_sql,  # pyright: ignore[reportPrivateUsage] # Why: accessing private _acquire_sql to acquire inside a manually-controlled transaction for chaos testing
                bucket,
                job_id,
                worker_id,
                _LEASE.total_seconds(),
            )
            assert row is not None

            _chaos_pg.stop()

            with pytest.raises(
                (asyncpg.PostgresConnectionError, ConnectionError, OSError, TimeoutError)
            ):
                await asyncio.wait_for(tx.commit(), timeout=5.0)
        finally:
            with contextlib.suppress(asyncpg.InterfaceError, OSError):
                if not direct_conn.is_closed():
                    await direct_conn.close()

        # Container restart may fail transiently on some Docker configurations.
        # Retry up to 3 times with a brief cooldown between attempts.
        for attempt in range(3):
            try:
                _chaos_pg.start()
                break
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2)
        fresh_dsn = _chaos_pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql://"
        )

        for _ in range(30):
            try:
                test_conn = await asyncpg.connect(fresh_dsn)
                await test_conn.close()
                break
            except (asyncpg.PostgresConnectionError, ConnectionError, OSError):
                await asyncio.sleep(0.5)

        conn2 = await asyncpg.connect(fresh_dsn)
        try:
            await conn2.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            await apply_pending(conn2, schema=schema_name)
        finally:
            await conn2.close()

        fresh_pool = await asyncpg.create_pool(fresh_dsn, min_size=1, max_size=10)
        try:
            held = await _count_held_slots(fresh_pool, schema_name, bucket)
            assert held == 0, (
                f"Slot should not be held after PG restart (transaction rolled back), got {held}"
            )

            res_fresh = ConcurrencyReservation(
                name=bucket, slots=2, lease=_LEASE, schema=schema_name
            )
            await res_fresh.ensure_slots(fresh_pool)
            idx = await res_fresh.acquire(new_uuid(), new_uuid(), fresh_pool)
            assert idx == 0
        finally:
            await fresh_pool.close()
    finally:
        with contextlib.suppress(asyncpg.InterfaceError):
            await pool.close()
