# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team pin: an evicted keyed rate limit's published
``rate_limit_buckets`` row must be reclaimed.

The pre-I-03 ``reservation_slots`` leak, one class over: each distinct
``base_name:key`` rate limit materialises an in-memory bucket AND
publishes a ``rate_limit_buckets`` row to PG (best-effort, so a
standalone admin process can see the limiter — pinned by
``tests/test_ratelimit_keyed_rate_limits_pg.py``). When the key goes
idle, ``evict_idle_keyed_rate_limits``
(``src/taskq/ratelimit/registry.py``) removes only the in-memory
registry entry — its docstring justifies the store side solely via the
Redis ``EXPIRE`` TTL, which governs Redis hashes, not the PG row the
publish wrote. No code path deletes an evicted keyed bucket's
``rate_limit_buckets`` row: the only ``rate_limit_buckets`` DELETEs in
the tree are ``reset()``-by-name, and the pending-reclaim drain handles
``reservation_slots`` only. Steady-state cardinality is one row per key
ever seen — unbounded in the caller-supplied key space, exactly the
shape the reservation fix closed.

Vendor convention (solid_queue — the closest keyed-PG prior art): one
row per payload key with a TTL column, reclaimed by exactly one batched
sweep (``vendor/solid_queue/app/models/solid_queue/semaphore.rb`` —
``expires_at`` derived from the job; the maintenance sweep deletes
expired rows in batches). Any of the three shapes — TTL column + sweep,
pending-reclaim reuse, or publish-on-demand without a persistent row —
closes this; the pin asserts the observable, not the shape.
"""

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


async def _bucket_rows(pool: asyncpg.Pool, schema: str, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


async def test_evicted_idle_keyed_rate_limit_bucket_row_is_reclaimed(
    pg_dsn: str,
) -> None:
    """Evicting an idle keyed rate limit must reclaim its published
    ``rate_limit_buckets`` row — the key space is caller-controlled, so
    retaining rows is permanent, unbounded growth, the same defect
    class the reservation-side reclamation closed."""
    schema = f"tkblr_{new_base62()}".lower()
    await _fresh_schema(pg_dsn, schema)
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": pg_dsn, "schema_name": schema}, validate=False
    )

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="rt-bucket-reclaim",
            key_fn=lambda p: p.tenant_id,
            capacity=5,
            refill_per_second=0.5,
            backend="memory",
        )

        acquired = await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            clock=FakeClock(_START),
            settings=settings,
        )
        assert len(acquired) == 1

        bucket = "rt-bucket-reclaim:acme"
        assert await _bucket_rows(pool, schema, bucket) == 1, (
            "fixture broken: the keyed bucket's publish did not land"
        )

        evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"

        # The sweep-side reclamation step the reservation path runs —
        # included so the pin is fair to every reclamation the registry
        # already offers.
        await reg.drain_pending_reservation_reclaims(pool)

        remaining = await _bucket_rows(pool, schema, bucket)
        assert remaining == 0, (
            f"evicting idle keyed bucket {bucket!r} left {remaining} "
            "rate_limit_buckets rows behind. No code path can ever delete them: "
            "the eviction docstring justifies the store side only via the Redis "
            "EXPIRE TTL, the publish wrote a PG row unconditionally, the only "
            "rate_limit_buckets DELETEs are reset()-by-name, and the "
            "pending-reclaim drain covers reservation_slots only. Steady-state "
            "cardinality is one row per key ever seen."
        )
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()
        await pool.close()
