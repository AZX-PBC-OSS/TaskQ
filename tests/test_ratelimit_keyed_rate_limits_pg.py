"""Integration tests for KeyedRateLimitRef's lazy PG publish-on-materialization.

Statically registered rate limits are published to ``rate_limit_buckets`` at
worker startup by ``sync_rate_limit_buckets`` so the admin UI can discover
them from PG without sharing the worker's in-memory registry. A keyed bucket
materialized lazily on first acquisition (long after startup) would
otherwise never reach that table — invisible to a standalone admin process
(``taskq ui serve``), making an active per-tenant throttle look like "no
limiter configured". ``_resolve_rate_limit_name`` therefore publishes each
freshly materialized keyed bucket best-effort — exercised here against a
real Postgres instance, mirroring
``tests/test_ratelimit_keyed_refs_pg.py`` (the reservation twin).
"""

from datetime import UTC, datetime

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


async def _prepare_schema(pg_dsn: str, schema: str) -> None:
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
    return WorkerSettings.load_from_dict({"pg_dsn": pg_dsn, "schema_name": schema})


async def test_keyed_rate_limit_materialization_publishes_bucket_row(pg_dsn: str) -> None:
    """Materializing a keyed bucket with a pg_pool publishes a
    ``rate_limit_buckets`` row for the concrete name — what the admin
    rate-limits page reads from in a standalone topology."""
    schema = f"tkrlpg_{new_base62()}".lower()
    await _prepare_schema(pg_dsn, schema)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    clock = FakeClock(_START)
    try:
        reg = RateLimitRegistry()
        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="api-per-tenant",
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
            clock=clock,
            settings=_settings(pg_dsn, schema),
        )
        assert len(acquired) == 1

        # The admin page's PG half reads exactly this table.
        row = await pool.fetchrow(
            f'SELECT bucket_name, kind FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            "api-per-tenant:acme",
        )
        assert row is not None, (
            "materialized keyed bucket was not published to rate_limit_buckets — "
            "a standalone admin process could not see the active limiter"
        )
        assert row["kind"] == "token_bucket"

        # Re-acquisition of the same key is idempotent (ON CONFLICT DO
        # NOTHING) — no error, still exactly one row.
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            clock=clock,
            settings=_settings(pg_dsn, schema),
        )
        count = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            "api-per-tenant:acme",
        )
        assert count == 1
    finally:
        await pool.close()
        await _drop_schema(pg_dsn, schema)


async def test_keyed_rate_limit_publish_scoped_to_worker_schema(pg_dsn: str) -> None:
    """The publish targets ``settings.schema_name`` (like every other
    primitive on the worker), not the hardcoded default schema."""
    schema = f"tkrlpg_{new_base62()}".lower()
    await _prepare_schema(pg_dsn, schema)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    clock = FakeClock(_START)
    try:
        reg = RateLimitRegistry()
        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=5,
            refill_per_second=0.5,
            backend="memory",
        )

        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="globex"),
            pg_pool=pool,
            clock=clock,
            settings=_settings(pg_dsn, schema),
        )

        in_own_schema = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".rate_limit_buckets WHERE bucket_name = $1',
            "api-per-tenant:globex",
        )
        assert in_own_schema == 1

        # Nothing leaked into the default "taskq" schema (which does not
        # even exist here — the row must be in the worker's own schema).
        taskq_schema_exists = await pool.fetchval(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'taskq'"
        )
        if taskq_schema_exists:
            in_default_schema = await pool.fetchval(
                'SELECT count(*) FROM "taskq".rate_limit_buckets WHERE bucket_name = $1',
                "api-per-tenant:globex",
            )
            assert in_default_schema == 0
    finally:
        await pool.close()
        await _drop_schema(pg_dsn, schema)
