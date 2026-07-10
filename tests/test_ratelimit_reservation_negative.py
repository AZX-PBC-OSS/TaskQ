"""Negative integration tests for ConcurrencyReservation.

``ensure_slots()`` before migration applied — the
       ``reservation_slots`` table does not exist; asyncpg raises
       ``UndefinedTableError`` (or ``PostgresError``) and the error message
       mentions the missing table.

       The ``held_by_worker_id`` column ships in the initial DDL — there is
       no intermediate migration state where the table exists but the column
       doesn't. The real failure mode is deploying M3 code without running
       ``taskq migrate up``, so the ``reservation_slots`` table is entirely
       absent.
"""

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.ratelimit.reservation import ConcurrencyReservation

pytestmark = pytest.mark.integration

_NEG_SCHEMA = "taskq_test_neg"


async def _setup_schema_without_reservations(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_NEG_SCHEMA}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{_NEG_SCHEMA}"')
        await conn.execute(
            f'CREATE TABLE "{_NEG_SCHEMA}".schema_migrations ('
            "  version    text PRIMARY KEY,"
            "  applied_at timestamptz NOT NULL DEFAULT now(),"
            "  checksum   text NOT NULL"
            ")"
        )
    finally:
        await conn.close()


# ── ensure_slots / acquire before migration applied ──────────────


async def test_ensure_slots_before_migration(pg_dsn: str) -> None:
    """``ensure_slots()`` before migration applied — the
    ``reservation_slots`` table does not exist; asyncpg raises an error
    mentioning the missing table.

    Simulates deploying M3 code without running ``taskq migrate up``.
    The schema exists (other tables present) but ``reservation_slots`` is
    absent, so ``ensure_slots()`` raises ``asyncpg.UndefinedTableError``.
    """
    await _setup_schema_without_reservations(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)

    try:
        res = ConcurrencyReservation(name="neg_bucket", slots=2, lease=5.0, schema=_NEG_SCHEMA)

        with pytest.raises((asyncpg.UndefinedTableError, asyncpg.PostgresError)) as exc_info:
            await res.ensure_slots(pool)

        msg = str(exc_info.value).lower()
        assert "reservation_slots" in msg, (
            f"Expected error message to mention 'reservation_slots', got: {exc_info.value}"
        )
    finally:
        await pool.close()


async def test_acquire_before_migration(pg_dsn: str) -> None:
    """companion: ``acquire()`` before migration applied — same
    scenario, verifying ``acquire()`` also raises a clear error when the
    table is absent."""
    await _setup_schema_without_reservations(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)

    try:
        res = ConcurrencyReservation(name="neg_bucket", slots=2, lease=5.0, schema=_NEG_SCHEMA)

        with pytest.raises((asyncpg.UndefinedTableError, asyncpg.PostgresError)) as exc_info:
            await res.acquire(new_uuid(), new_uuid(), pool)

        msg = str(exc_info.value).lower()
        assert "reservation_slots" in msg, (
            f"Expected error message to mention 'reservation_slots', got: {exc_info.value}"
        )
    finally:
        await pool.close()
