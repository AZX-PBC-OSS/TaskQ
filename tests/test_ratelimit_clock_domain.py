"""C8: rate-limit admission state must be owned by the data-store clock.

Every limiter's window predicate, TAT, and token epoch ran on the calling
node's Python clock while the state it measured was shared across nodes —
with N nodes diverging by S, combined admission exceeds the configured
limit (over-admission up to S/window). These tests skew exactly one
caller's Python clock (``tests._clock_skew.SkewedClock``) and assert the
store-side outcome is unchanged: the skewed caller is measured by the
store's own clock (PG ``clock_timestamp()`` / Redis ``TIME``), not its own.

Single-node behavior is unchanged (S=0); these pin the multi-node property.
"""

from datetime import timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from tests._clock_skew import SkewedClock

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = "taskq_clock_domain"


def _pg_settings(schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": schema.pg_dsn, "schema_name": schema.schema_name},
    )


def _redis_settings(redis_url: str, schema_name: str = _SCHEMA_LABEL) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": schema_name},
    )


# ── PG sliding window (log): window predicate is server-side ──────────


async def test_sliding_window_pg_nodes_share_the_server_clock(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """C8 pin (PG log style): limit=1/60s shared by two callers whose Python
    clocks diverge by 90s. Node A (unskewed) is admitted. Node B, whose
    clock is 90s AHEAD, must be DENIED — the window boundary and the entry
    timestamps are ``clock_timestamp()``-domain, so node B's own clock
    cannot shrink the window it is measured against.

    Pre-fix: node B's boundary was ``clock.now() - window`` (90s ahead of
    the server) and node A's entry — stamped in A's domain — fell outside
    it; the delete even evicted A's entry outright. Node B was admitted:
    2 events inside the true 60s window (over-admission)."""
    settings = _pg_settings(module_pg_schema)
    name = f"sw_cd_{new_base62()}"

    node_a = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="postgres", style="log"
    )
    node_b = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="postgres", style="log"
    )

    a = await node_a.acquire(pg_pool=module_pg_pool, clock=SystemClock(), settings=settings)
    b = await node_b.acquire(
        pg_pool=module_pg_pool,
        clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
        settings=settings,
    )

    assert a.allowed is True
    assert b.allowed is False, "a skewed-ahead caller must not shrink the shared window"
    assert b.retry_after is not None
    assert b.retry_after > timedelta(seconds=25), (
        f"retry_after must track the oldest SERVER-stamped entry (~60s), got {b.retry_after}"
    )


# ── PG sliding window (GCRA): the shared TAT is server-domain ────────


async def test_sliding_window_pg_gcra_tat_not_poisoned_by_skewed_caller(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """C8 pin (PG GCRA): limit=60/60s (emission 1s, tolerance 60s). A
    caller whose clock is 90s AHEAD acquires (allowed either way), then an
    unskewed caller acquires immediately and must ALSO be allowed.

    Pre-fix: the skewed caller computed and stored its TAT in its own
    Python epoch (server+90+1s), poisoning the shared state — the next
    caller measured ``allow_at`` 32s in its future and was denied for ~90s
    (under-admission caused by another node's skew). Post-fix the TAT is
    ``EXTRACT(EPOCH FROM clock_timestamp())``-domain, so a skewed node
    cannot move the shared admission boundary."""
    settings = _pg_settings(module_pg_schema)
    name = f"sw_gcra_cd_{new_base62()}"

    node_a = SlidingWindow(
        name=name, limit=60, window=timedelta(seconds=60), backend="postgres", style="gcra"
    )
    node_b = SlidingWindow(
        name=name, limit=60, window=timedelta(seconds=60), backend="postgres", style="gcra"
    )

    a = await node_a.acquire(
        pg_pool=module_pg_pool,
        clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
        settings=settings,
    )
    b = await node_b.acquire(pg_pool=module_pg_pool, clock=SystemClock(), settings=settings)

    assert a.allowed is True
    assert b.allowed is True, "a skewed node must not poison the shared GCRA tat"


# ── PG token bucket: refill elapsed is server-domain ─────────────────


async def test_token_bucket_pg_refill_measured_by_server_clock(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """C8 pin (PG token bucket): capacity=1, refill=0.02/s, a row pre-seeded
    with tokens=0.0 and a SERVER-domain ts (``EXTRACT(EPOCH FROM
    clock_timestamp())``). A caller whose clock is 90s AHEAD acquires 1
    token and must be DENIED — the elapsed-refill step must use the server
    epoch, so 90 seconds that never passed cannot mint a token.

    Pre-fix: ``elapsed = clock.now().timestamp() - ts`` read the skewed
    caller's epoch → 90s of phantom refill → the bucket refilled to full
    and the caller was admitted (over-admission)."""
    settings = _pg_settings(module_pg_schema)
    schema = module_pg_schema.schema_name
    name = f"tb_cd_{new_base62()}"

    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema is fixture-derived; values are $1-bound
            f"VALUES ($1, 'token_bucket', "
            f"jsonb_build_object('tokens', 0.0::float8, "
            f"'ts', EXTRACT(EPOCH FROM clock_timestamp())), now())",
            name,
        )

    tb = TokenBucket(name=name, capacity=1.0, refill_per_second=0.02, backend="postgres")
    r = await tb.acquire(
        count=1.0,
        pg_pool=module_pg_pool,
        clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
        settings=settings,
    )

    assert r.allowed is False, "phantom elapsed refill from a skewed clock must not admit"
    assert r.retry_after is not None
    assert r.retry_after > timedelta(seconds=25), (
        f"retry_after must be ~50s (1 token at 0.02/s from empty), got {r.retry_after}"
    )


# ── Redis: the scripts read TIME, not the caller's clock ─────────────
#
# D5 (approved): the documented deviation ("client-supplied now instead of
# TIME") is reversed — every script derives now from redis.call('TIME'),
# so multi-node fleets share the Redis clock. These are the Redis analogs
# of the PG pins above.


@pytest.mark.redis
async def test_sliding_window_redis_nodes_share_the_time_clock(redis_url: str) -> None:
    """C8 pin (Redis log style): limit=1/60s shared by two callers whose
    Python clocks diverge by 90s. Node A (unskewed) is admitted; node B,
    whose clock is 90s AHEAD, must be DENIED — the script's window
    boundary and ZADD scores are TIME-domain.

    Pre-fix: node B passed its own now_ms as ARGV — its ZREMRANGEBYSCORE
    boundary (now-90s ahead) evicted node A's entry outright and node B
    was admitted (over-admission)."""
    settings = _redis_settings(redis_url)
    name = f"sw_cd_{new_base62()}"

    node_a = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
    )
    node_b = SlidingWindow(
        name=name, limit=1, window=timedelta(seconds=60), backend="redis", style="log"
    )

    client = redis_async.from_url(redis_url, decode_responses=False)
    try:
        a = await node_a.acquire(redis_client=client, clock=SystemClock(), settings=settings)
        b = await node_b.acquire(
            redis_client=client,
            clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
            settings=settings,
        )

        assert a.allowed is True
        assert b.allowed is False, "a skewed-ahead caller must not shrink the shared window"
        assert b.retry_after is not None
        assert b.retry_after > timedelta(seconds=25), (
            f"retry_after must track the oldest TIME-stamped entry (~60s), got {b.retry_after}"
        )
    finally:
        await client.aclose()


@pytest.mark.redis
async def test_sliding_window_redis_gcra_tat_not_poisoned_by_skewed_caller(
    redis_url: str,
) -> None:
    """C8 pin (Redis GCRA): limit=60/60s. A caller whose clock is 90s AHEAD
    acquires (allowed either way), then an unskewed caller acquires
    immediately and must ALSO be allowed — the script's TAT is TIME-domain,
    so a skewed node cannot shove the shared admission boundary into
    another node's future."""
    settings = _redis_settings(redis_url)
    name = f"sw_gcra_cd_{new_base62()}"

    node_a = SlidingWindow(
        name=name, limit=60, window=timedelta(seconds=60), backend="redis", style="gcra"
    )
    node_b = SlidingWindow(
        name=name, limit=60, window=timedelta(seconds=60), backend="redis", style="gcra"
    )

    client = redis_async.from_url(redis_url, decode_responses=False)
    try:
        a = await node_a.acquire(
            redis_client=client,
            clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
            settings=settings,
        )
        b = await node_b.acquire(redis_client=client, clock=SystemClock(), settings=settings)

        assert a.allowed is True
        assert b.allowed is True, "a skewed node must not poison the shared GCRA tat"
    finally:
        await client.aclose()


@pytest.mark.redis
async def test_token_bucket_redis_refill_measured_by_redis_time(redis_url: str) -> None:
    """C8 pin (Redis token bucket): capacity=1, refill=0.02/s. Node A
    (unskewed) consumes the only token; node B, whose clock is 90s AHEAD,
    must be DENIED — the script's elapsed-refill step reads TIME, so 90
    seconds that never passed cannot mint a token.

    Pre-fix: node B passed its own now as ARGV — elapsed = its skewed now
    minus A's ts = ~90s of phantom refill → admitted (over-admission)."""
    settings = _redis_settings(redis_url)
    name = f"tb_cd_{new_base62()}"

    node_a = TokenBucket(name=name, capacity=1.0, refill_per_second=0.02, backend="redis")
    node_b = TokenBucket(name=name, capacity=1.0, refill_per_second=0.02, backend="redis")

    client = redis_async.from_url(redis_url, decode_responses=False)
    try:
        a = await node_a.acquire(
            count=1.0, redis_client=client, clock=SystemClock(), settings=settings
        )
        b = await node_b.acquire(
            count=1.0,
            redis_client=client,
            clock=SkewedClock(SystemClock(), timedelta(seconds=90)),
            settings=settings,
        )

        assert a.allowed is True
        assert b.allowed is False, "phantom elapsed refill from a skewed clock must not admit"
        assert b.retry_after is not None
        assert b.retry_after > timedelta(seconds=25), (
            f"retry_after must be ~50s (1 token at 0.02/s from empty), got {b.retry_after}"
        )
    finally:
        await client.aclose()
