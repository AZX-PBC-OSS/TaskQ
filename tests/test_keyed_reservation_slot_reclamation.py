# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Red-team pins for keyed-reservation slot reclamation.

Each distinct ``base_name:key`` materialises its own ``slots`` rows in
``reservation_slots`` (``ratelimit/registry.py:661`` → ``ensure_slots``).
When the key goes idle, ``evict_idle_keyed_reservations``
(``registry.py:1247-1275``) drops only the in-process dict entry, on the
strength of a docstring claiming the rows "are already reclaimed
independently by the existing lock-expiry sweep". That claim is false:
the lock-expiry sweep is ``UPDATE ... SET job_id = NULL``
(``backend/_sweeps.py:330-337``) — it clears the row but leaves it — and
the only DELETE path, ``sync_slots``, iterates *currently registered*
reservations, so an evicted bucket is invisible to it by construction.
The rows are orphaned with no code path able to delete them, ever, and
steady-state cardinality is ``slots x every key ever seen`` — unbounded
in the caller-supplied key space.

The contract these tests pin: once a keyed bucket is evicted AND idle
(no slot held), its ``reservation_slots`` rows are reclaimed; and a key
that becomes active again afterwards re-materialises and acquires
cleanly. Deliberately NOT pinned: the actively-held case — a slot still
held by a live job must survive eviction, and
``tests/test_ratelimit_keyed_refs_pg.py::test_eviction_while_holder_active_does_not_over_admit``
guards that; these tests release the slot before evicting so the two
cases cannot be conflated.
"""

from datetime import timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_keyed_reclaim_test"


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": _schema()},
        validate=False,
    )


async def _fresh_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_schema()}" CASCADE')
        await apply_pending(conn, schema=_schema())
    finally:
        await conn.close()


async def _slot_rows(pool: asyncpg.Pool, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def test_evicted_idle_keyed_reservation_slots_are_reclaimed(
    pg_dsn: str,
) -> None:
    """Evicting an idle keyed bucket must delete its ``reservation_slots``
    rows — the key space is caller-controlled, so retaining them is
    permanent, unbounded growth."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="keyed-reclaim-probe",
            key_fn=lambda p: p.session_id,
            slots=2,
            lease=timedelta(seconds=10),
        )

        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(acquired) == 1
        bucket = "keyed-reclaim-probe:s1"
        assert await _slot_rows(pool, bucket) == 2, "fixture broken: ensure_slots did not run"

        # Release the only slot, THEN evict: the bucket is now genuinely
        # idle — the case no correctness argument protects.
        await reg.release_for_actor(acquired)
        evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"

        remaining = await _slot_rows(pool, bucket)
        assert remaining == 0, (
            f"evicting idle keyed bucket {bucket!r} left {remaining} "
            "reservation_slots rows behind. No code path can ever delete "
            "them: the lock-expiry sweep is an UPDATE that clears job_id "
            "but keeps the row (backend/_sweeps.py:330-337), and sync_slots "
            "iterates only currently-registered reservations, which an "
            "evicted bucket no longer is. Steady-state cardinality is "
            "slots x every key ever seen."
        )
    finally:
        await pool.close()


async def test_reclaimed_key_rematerialises_on_next_acquire(
    pg_dsn: str,
) -> None:
    """Control: reclamation must not break re-materialisation. A key that
    becomes active again after its idle eviction must re-register and
    acquire cleanly — this must stay green both before and after the fix."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="keyed-reclaim-remat",
            key_fn=lambda p: p.session_id,
            slots=1,
            lease=timedelta(seconds=10),
        )
        settings = _settings(pg_dsn)

        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=settings,
        )
        await reg.release_for_actor(acquired)
        reg.evict_idle_keyed_reservations(idle_for=timedelta(0))

        reacquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=settings,
        )
        assert len(reacquired) == 1
        assert reacquired[0].name == "keyed-reclaim-remat:s1"
    finally:
        await pool.close()
