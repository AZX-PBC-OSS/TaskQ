# ruff: noqa: S608  # Why: schema is a per-test fixture identifier, not user input; every value is $-bound.

"""Red-team pins: a keyed rate limit's ``rate_limit_buckets`` row must be
recorded for reclamation by EVERY flow that can create it, not only by a
publish that happened to succeed.

The eviction-drain landed for evicted keyed buckets
(``evict_idle_keyed_rate_limits`` records the pending reclaim;
``drain_pending_reservation_reclaims`` deletes the row) - but the
recording consults ``_keyed_rate_limit_row_schemas``, a capture written
ONLY on the ``else`` arm of the materialisation publish
(``registry.py::_resolve_rate_limit_name``): the capture lands when the
best-effort publish succeeds, and never otherwise. Two row-creating
flows therefore bypass the capture entirely:

1. **The publish fails, the acquire still creates the row.** The publish
   is best-effort by design (a publish failure "must NOT fail the
   acquisition"), but the acquisition itself creates the row it was
   supposed to annotate: a ``backend="postgres"`` keyed bucket's acquire
   preseeds ``INSERT INTO rate_limit_buckets ... ON CONFLICT DO
   NOTHING`` before reading state (``token_bucket.py::_acquire_pg``),
   and the redis backend's PG fallback does the same. The row exists,
   the capture does not, so the idle eviction proceeds *unrecorded*
   (``schema is None`` → "nothing to reclaim") and the row orphans
   permanently - the one-row-per-key-ever-seen growth the eviction-drain
   was built to stop, surviving it by one branch.
2. **The first resolution has no pool, later ones do.** The publish runs
   only in the first-materialisation branch; the reuse branch refreshes
   recency and never publishes. A bucket materialised pool-less (an
   in-process or memory acquire) that is later resolved with a pool gets
   its row created by the acquire's preseed - or, for metadata-only
   backends, never gets the admin-UI row at all - and no capture either
   way, so the eviction again records nothing.

The contract these pins assert, stated once: **any pool-bearing
resolution of a keyed rate limit owns its PG row** - the row is
published (idempotently, best-effort) and its schema is captured for the
reclaim drain, whatever the publish's outcome. A captured name whose row
never materialised drains as a one-statement no-op DELETE; an uncaptured
row is permanent.
"""

import sys
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _TenantPayload(BaseModel):
    tenant_id: str


async def _fresh_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict({"pg_dsn": pg_dsn, "schema_name": schema}, validate=False)


async def _bucket_rows(pool: asyncpg.Pool, schema: str, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


def _pg_ref(base_name: str) -> KeyedRateLimitRef:
    """A keyed rate limit on the PG backend - the acquire itself creates
    the ``rate_limit_buckets`` row via the preseed."""
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="postgres",
    )


def _memory_ref(base_name: str) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="memory",
    )


async def test_publish_failure_row_created_by_acquire_is_still_reclaimed(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A publish that fails must not orphan the row the acquire creates.

    The materialisation publish is best-effort (a transient PG blip warns
    and the acquisition proceeds) - and the ``backend="postgres"``
    acquire then preseeds the very row the publish was meant to create.
    The reclamation bookkeeping must not depend on the side quest's
    outcome: the idle eviction has to record the bucket for the
    reclaim drain, or the row outlives every deletion path in the
    package."""
    schema = f"tkbcap_{new_base62()}".lower()
    await _fresh_schema(pg_dsn, schema)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()

        # The publish seam is the MODULE taskq.ratelimit.registry; the
        # package's __init__ re-exports the `registry` singleton under the
        # name the submodule would bind, so `import ... as` hands back the
        # singleton - the module is reached through sys.modules, which the
        # RateLimitRegistry import above has already loaded.
        registry_mod = sys.modules["taskq.ratelimit.registry"]

        # Exactly the transient the best-effort publish guards: the first
        # publish raises, every later one delegates to the real (idempotent)
        # statement.
        real_upsert = registry_mod._upsert_rate_limit_bucket_row  # pyright: ignore[reportPrivateUsage]  # Why: reaching the module-level publish seam to inject one transient failure, the exact blip the best-effort guard exists for.
        publish_attempts = {"n": 0}

        async def _failing_first(pool_: asyncpg.Pool, schema_: str, name_: str, kind_: str) -> None:
            if publish_attempts["n"] == 0:
                publish_attempts["n"] += 1
                raise asyncpg.PostgresConnectionError("transient publish outage")
            await real_upsert(pool_, schema_, name_, kind_)

        monkeypatch.setattr(registry_mod, "_upsert_rate_limit_bucket_row", _failing_first)

        acquired = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("cap-gap")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn, schema),
        )
        assert len(acquired) == 1

        bucket = "cap-gap:acme"
        assert await _bucket_rows(pool, schema, bucket) == 1, (
            "fixture broken: the PG-backend acquire's preseed did not create the row"
        )

        # The job terminates; the key goes idle; the worker's sweep evicts
        # the registry entry and drains the pending reclaims.
        evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"
        assert reg.has_pending_reservation_reclaims, (
            "an eviction whose bucket HAS a PG row (the acquire's preseed created "
            "it after the publish failed) recorded no pending reclaim - the "
            "capture must ride the resolution, not the publish's outcome, or "
            "the row orphans with the registry entry gone and no code path left "
            "that can ever name it"
        )

        await reg.drain_pending_reservation_reclaims(pool)

        remaining = await _bucket_rows(pool, schema, bucket)
        assert remaining == 0, (
            f"evicting idle keyed bucket {bucket!r} left {remaining} "
            "rate_limit_buckets rows behind although the acquire created the "
            "row: the publish failed (best-effort, by design), the preseed "
            "created the row anyway, and the eviction proceeded unrecorded "
            "because the schema capture only lands on a successful publish. "
            "Steady-state cardinality is one row per key whose first publish "
            "failed - unbounded in the caller-controlled key space."
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn, schema)


async def test_poolless_materialisation_captures_on_pool_bearing_reuse(
    pg_dsn: str,
) -> None:
    """A bucket first materialised without a pool must publish and capture
    on its first pool-bearing resolution.

    The publish today runs only in the first-materialisation branch; the
    reuse branch refreshes recency and never publishes. A worker that
    resolves the key pool-less (an in-process acquire) and later resolves
    it with a pool therefore never surfaces the admin-UI row AND never
    captures the schema - so the idle eviction records nothing while the
    acquire's preseed (PG backend) or the republish (metadata backends)
    stands the row up. Every pool-bearing resolution must own the row:
    publish idempotently and capture."""
    schema = f"tkbcap_{new_base62()}".lower()
    await _fresh_schema(pg_dsn, schema)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _memory_ref("cap-reuse")
        settings = _settings(pg_dsn, schema)
        bucket = "cap-reuse:acme"

        # First resolution: pool-less (the bucket is process-local; no row,
        # no capture - correct for a resolution that cannot touch PG).
        acquired = await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=None,
            clock=FakeClock(_START),
            settings=settings,
        )
        assert len(acquired) == 1
        assert await _bucket_rows(pool, schema, bucket) == 0

        # The same key resolved again, now with a pool - a worker whose
        # dispatch carries the pool, or a later acquire after a pool was
        # registered. This resolution must publish and capture.
        reacquired = await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            clock=FakeClock(_START),
            settings=settings,
        )
        assert len(reacquired) == 1
        assert await _bucket_rows(pool, schema, bucket) == 1, (
            "a pool-bearing REUSE of a keyed bucket published no rate_limit_buckets "
            "row: the publish runs only on first materialisation, so a bucket "
            "materialised pool-less is invisible to a standalone admin process "
            "forever, and its schema is never captured for the reclaim drain"
        )

        evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"
        assert reg.has_pending_reservation_reclaims, (
            "the pool-bearing reuse captured no reclaim schema - the eviction "
            "records nothing and the row the reuse should own orphans"
        )

        await reg.drain_pending_reservation_reclaims(pool)
        assert await _bucket_rows(pool, schema, bucket) == 0, (
            f"evicting idle keyed bucket {bucket!r} left rows behind: the reuse "
            "path never captured the schema, so the drain cannot name the bucket"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn, schema)
