"""Integration tests for KeyedReservationRef's lazy PG-backed registration.

A freshly-keyed reservation (see ``RateLimitRegistry._resolve_reservation_name``
in src/taskq/ratelimit/registry.py) is registered *during job processing*,
strictly after the worker-startup ``ensure_slots()`` loop
(src/taskq/worker/_bootstrap.py) has already run over whatever reservations
were present in the registry at that time. ``_resolve_reservation_name``
therefore calls ``ensure_slots()`` itself immediately after registering a new
keyed reservation, so its ``reservation_slots`` rows exist before the name is
ever handed to ``acquire()``. It also builds the reservation with
``schema=settings.schema_name`` rather than the ``ConcurrencyReservation``
default, so it targets the same schema as every other primitive on the
worker — both are exercised here against a real Postgres instance.
"""

from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration


async def test_keyed_reservation_lazy_registration_against_real_pg(
    pg_dsn: str,
) -> None:
    """A freshly-keyed reservation acquires successfully against real PG —
    ensure_slots() runs as part of lazy registration, not only at startup."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute('DROP SCHEMA IF EXISTS "taskq" CASCADE')
        await apply_pending(conn, schema="taskq")
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef(
            base_name="keyed-refs-pg-probe",
            key_fn=lambda p: str(p["session_id"]),
            slots=2,
            lease=timedelta(seconds=10),
        )

        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "s1"},
            pg_pool=pool,
        )

        assert len(acquired) == 1
        assert acquired[0].name == "keyed-refs-pg-probe:s1"
    finally:
        await pool.close()


async def test_keyed_reservation_lazy_registration_respects_worker_schema(
    pg_dsn: str,
) -> None:
    """A keyed reservation is registered against settings.schema_name, not
    the ConcurrencyReservation default — acquiring against a non-default
    schema must not raise UndefinedTableError against the default one."""
    schema = "taskq_keyed_refs_schema_test"
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef(
            base_name="keyed-refs-schema-probe",
            key_fn=lambda p: str(p["session_id"]),
            slots=1,
            lease=timedelta(seconds=10),
        )
        settings = WorkerSettings.load_from_dict(
            {
                "PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
            }
        )

        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "s1"},
            pg_pool=pool,
            settings=settings,
        )

        assert len(acquired) == 1
        assert reg.get_reservation("keyed-refs-schema-probe:s1") is not None
    finally:
        await pool.close()


async def test_eviction_while_holder_active_does_not_over_admit(
    pg_dsn: str,
) -> None:
    """A keyed reservation evicted from the registry while a job still HOLDS
    its only slot must not over-admit on re-materialization.

    Eviction removes only the in-memory registry entry; the PG
    ``reservation_slots`` rows (the source of truth) stay put. When the same
    key is acquired again afterwards, the key is re-registered and
    ``ensure_slots`` runs again — ``INSERT ... ON CONFLICT DO NOTHING`` keeps
    the existing rows, including the still-held one. A second acquisition
    for the same key must therefore be DENIED (the slot is genuinely still
    held), and no duplicate slot rows may appear.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute('DROP SCHEMA IF EXISTS "taskq" CASCADE')
        await apply_pending(conn, schema="taskq")
    finally:
        await conn.close()

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef(
            base_name="keyed-refs-evict-probe",
            key_fn=lambda p: str(p["session_id"]),
            slots=1,
            lease=timedelta(minutes=10),
        )

        # Job 1 acquires the only slot for key s1 and keeps holding it.
        holder_worker = new_uuid()
        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=holder_worker,
            payload={"session_id": "s1"},
            pg_pool=pool,
        )
        assert len(acquired) == 1
        concrete = "keyed-refs-evict-probe:s1"

        # Evict the registry entry while the holder is still active —
        # simulates the leader sweep reclaiming an entry it believes idle.
        reg._reservations.pop(concrete, None)  # pyright: ignore[reportPrivateUsage]  # Why: simulating evict_idle_keyed_reservations without needing monotonic control.
        reg._keyed_reservation_last_used.pop(concrete, None)  # pyright: ignore[reportPrivateUsage]
        assert not reg.has_reservation(concrete)

        # A new job with the same key re-materializes the reservation
        # (re-register + ensure_slots no-op) and tries to acquire.
        from taskq.exceptions import ReservationUnavailable

        with pytest.raises(ReservationUnavailable):
            await reg.acquire_for_actor(
                rate_limits=[],
                reservations=[ref],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                payload={"session_id": "s1"},
                pg_pool=pool,
            )

        # The slot table was not mutated by re-registration: exactly one
        # row, still held by the original worker.
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT slot_index, held_by_worker_id FROM "taskq".reservation_slots '
                "WHERE bucket_name = $1",
                concrete,
            )
        assert len(rows) == 1
        assert rows[0]["held_by_worker_id"] == holder_worker

        # Sanity: after the original holder releases, the slot is free and
        # the same key can be acquired again (no permanent wedge).
        await reg.release_for_actor(acquired, pg_pool=pool)
        reacquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "s1"},
            pg_pool=pool,
        )
        assert len(reacquired) == 1
    finally:
        await pool.close()
