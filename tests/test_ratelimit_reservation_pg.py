"""Integration tests for ConcurrencyReservation PG backend against testcontainers Postgres.

through These verify the
PG acquire/release lifecycle, SKIP LOCKED under contention, heartbeat
lease extension, Sweep 4 reclamation, sync_slots against real PG, and
release-gate with wrong worker_id.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._sql import build_heartbeat_sql as _build_heartbeat_sql
from taskq.backend.postgres import PostgresBackend
from taskq.ratelimit.reservation import ConcurrencyReservation, sync_slots
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=10)


def _unique_name() -> str:
    return f"test_{new_base62()}"


type _Conn = asyncpg.Connection | asyncpg.pool.PoolConnectionProxy


# ── Full acquire/release lifecycle ────────────────────────────────


async def test_acquire_release_lifecycle(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Full acquire/release lifecycle — pre-allocate 2 slots, acquire
    both, release one, acquire again; released slot is reused."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    idx0 = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    assert idx0 == 0

    idx1 = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    assert idx1 == 1

    worker_0 = await _get_held_worker(module_pg_pool, schema, bucket, 0)
    assert worker_0 is not None
    await res.release(0, worker_0, module_pg_pool)

    idx_reuse = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    assert idx_reuse == 0


async def _get_held_worker(
    pool: asyncpg.Pool, schema: str, bucket: str, slot_index: int
) -> UUID | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT held_by_worker_id FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            slot_index,
        )
    if row is None or row["held_by_worker_id"] is None:
        return None
    return row["held_by_worker_id"]


# ── SKIP LOCKED under contention ──────────────────────────────────


async def test_skip_locked_contention(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """SKIP LOCKED under contention — two concurrent asyncio.gather
    acquires on the same bucket (2 slots); both succeed on slot_index=0 and
    slot_index=1; no deadlock."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    idx_a, idx_b = await asyncio.gather(
        res.acquire(new_uuid(), new_uuid(), module_pg_pool),
        res.acquire(new_uuid(), new_uuid(), module_pg_pool),
    )

    indices = {idx_a, idx_b}
    assert indices == {0, 1}


# ── Expired lease inline reclamation ──────────────────────────────


async def test_expired_lease_inline_reclaim(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Expired lease inline reclamation — acquire, manually set
    lease_expires_at = now() - interval '1 second' in PG, then acquire;
    expired slot reclaimed inline."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=1, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    await res.acquire(new_uuid(), new_uuid(), module_pg_pool)

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".reservation_slots '
            "SET lease_expires_at = now() - interval '1 second' "
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )

    idx_reuse = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    assert idx_reuse == 0


# ── Heartbeat extends lease ────────────────────────────────────────


_UPD_JOBS_LOCK_SQL_TEMPLATE = (
    'UPDATE "{schema}".jobs '
    "SET last_heartbeat_at = now(), lock_expires_at = now() + $2 "
    "WHERE locked_by_worker = $1 AND status = 'running'"
)


async def test_heartbeat_extends_lease(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Heartbeat extends lease — acquire a slot, run two heartbeat ticks
    (via SQL template from heartbeat.py); lease_expires_at advances on each
    tick."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()
    worker_id = new_uuid()
    job_id = new_uuid()

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )

    res = ConcurrencyReservation(name=bucket, slots=1, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    async with module_pg_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_running_job(conn, schema, worker_id, job_id=job_id)

    await res.acquire(job_id, worker_id, module_pg_pool)

    _, _, update_reservation_leases_sql, _ = _build_heartbeat_sql(settings.schema_name)
    lock_lease = timedelta(seconds=settings.lock_lease)
    update_jobs_lock_sql = _UPD_JOBS_LOCK_SQL_TEMPLATE.format(schema=schema)

    async with module_pg_pool.acquire() as conn:
        row_before = await conn.fetchrow(
            f'SELECT lease_expires_at FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )
    assert row_before is not None
    expires_before = row_before["lease_expires_at"]
    assert expires_before is not None

    async with module_pg_pool.acquire() as conn, conn.transaction():
        await conn.execute(update_jobs_lock_sql, worker_id, lock_lease)
        await conn.execute(update_reservation_leases_sql, worker_id, lock_lease)

    async with module_pg_pool.acquire() as conn:
        row_tick1 = await conn.fetchrow(
            f'SELECT lease_expires_at FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )
    assert row_tick1 is not None
    expires_tick1 = row_tick1["lease_expires_at"]
    assert expires_tick1 is not None
    assert expires_tick1 > expires_before

    async with module_pg_pool.acquire() as conn, conn.transaction():
        await conn.execute(update_jobs_lock_sql, worker_id, lock_lease)
        await conn.execute(update_reservation_leases_sql, worker_id, lock_lease)

    async with module_pg_pool.acquire() as conn:
        row_tick2 = await conn.fetchrow(
            f'SELECT lease_expires_at FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )
    assert row_tick2 is not None
    expires_tick2 = row_tick2["lease_expires_at"]
    assert expires_tick2 is not None
    assert expires_tick2 > expires_tick1


# ── Sweep 4 reclaims expired slots ────────────────────────────────


async def test_sweep_4_reclaims_expired_slots(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Sweep 4 reclaims expired slots — acquire, set lease_expires_at
    to the past, run Sweep 4 SQL; all expired slots freed (job_id = NULL,
    held_by_worker_id = NULL)."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
    await res.acquire(new_uuid(), new_uuid(), module_pg_pool)

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".reservation_slots '
            "SET lease_expires_at = now() - interval '1 second' "
            "WHERE bucket_name = $1",
            bucket,
        )

    async with module_pg_pool.acquire() as conn:
        count = await PostgresBackend.sweep_leaked_reservation_slots(
            conn,
            datetime.now(UTC),
            schema=schema,
        )

    assert count == 2

    async with module_pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT job_id, held_by_worker_id FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1",
            bucket,
        )

    for row in rows:
        assert row["job_id"] is None
        assert row["held_by_worker_id"] is None


# ── sync_slots() insertion ────────────────────────────────────────


async def test_sync_slots_insertion(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """sync_slots() insertion — register slots=4, call sync_slots();
    4 rows inserted; SyncResult.inserted has 4 entries."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=4, lease=_LEASE, schema=schema)
    result = await sync_slots([res], module_pg_pool, schema=schema)

    assert len(result.inserted) == 4
    assert result.deleted == []
    assert result.skipped_held == []
    assert sorted(i for _, i in result.inserted) == [0, 1, 2, 3]

    async with module_pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT slot_index FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 ORDER BY slot_index",
            bucket,
        )
    assert [r["slot_index"] for r in rows] == [0, 1, 2, 3]


# ── sync_slots() deletion ──────────────────────────────────────────


async def test_sync_slots_deletion(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """sync_slots() deletion — seed 4 rows (config says 2 slots); call
    sync_slots(); 2 excess free rows deleted."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res_original = ConcurrencyReservation(name=bucket, slots=4, lease=_LEASE, schema=schema)
    await res_original.ensure_slots(module_pg_pool)

    res_reduced = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    result = await sync_slots([res_reduced], module_pg_pool, schema=schema)

    assert result.inserted == []
    assert len(result.deleted) == 2
    assert result.skipped_held == []
    assert sorted(i for _, i in result.deleted) == [2, 3]

    async with module_pg_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT slot_index FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 ORDER BY slot_index",
            bucket,
        )
    assert [r["slot_index"] for r in rows] == [0, 1]


# ── sync_slots() skips held slots ──────────────────────────────────


async def test_sync_slots_skips_held(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """sync_slots() skips held slots — 4 rows in DB (config says 2),
    1 excess is held; 1 deleted, 1 in SyncResult.skipped_held."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res_original = ConcurrencyReservation(name=bucket, slots=4, lease=_LEASE, schema=schema)
    await res_original.ensure_slots(module_pg_pool)

    await res_original.acquire(new_uuid(), new_uuid(), module_pg_pool)
    await res_original.acquire(new_uuid(), new_uuid(), module_pg_pool)

    held_worker = new_uuid()
    idx = await res_original.acquire(new_uuid(), held_worker, module_pg_pool)
    assert idx in (2, 3)

    res_reduced = ConcurrencyReservation(name=bucket, slots=2, lease=_LEASE, schema=schema)
    result = await sync_slots([res_reduced], module_pg_pool, schema=schema)

    assert result.inserted == []
    assert len(result.deleted) == 1
    assert len(result.skipped_held) == 1

    deleted_indices = sorted(i for _, i in result.deleted)
    held_indices = sorted(i for _, i in result.skipped_held)
    assert len(deleted_indices) == 1
    assert len(held_indices) == 1
    assert set(deleted_indices) | set(held_indices) == {2, 3}


# ── Release with wrong worker_id ───────────────────────────────────


async def test_release_wrong_worker_id(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Release with wrong worker_id — acquire with worker A, call
    release with worker B's UUID; UPDATE matches 0 rows; slot still held by
    A."""
    schema = module_pg_schema.schema_name
    bucket = _unique_name()

    res = ConcurrencyReservation(name=bucket, slots=1, lease=_LEASE, schema=schema)
    await res.ensure_slots(module_pg_pool)

    worker_a = new_uuid()
    job_a = new_uuid()
    idx = await res.acquire(job_a, worker_a, module_pg_pool)
    assert idx == 0

    worker_b = new_uuid()
    await res.release(0, worker_b, module_pg_pool)

    async with module_pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT held_by_worker_id, job_id FROM "{schema}".reservation_slots '
            "WHERE bucket_name = $1 AND slot_index = $2",
            bucket,
            0,
        )
    assert row is not None
    assert row["held_by_worker_id"] == worker_a
    assert row["job_id"] == job_a
