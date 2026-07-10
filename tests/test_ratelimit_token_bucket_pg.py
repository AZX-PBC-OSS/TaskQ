"""Integration tests for TokenBucket PG backend against testcontainers Postgres.

Lives here (not in the unit file) because it requires a real PG backend and
forbids mocking asyncpg.

Exercises the full token-bucket arithmetic against PG (same scenarios as the
Redis backend, but backend="postgres").
"""

import time
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.ratelimit import TokenBucket
from taskq.ratelimit.decision import RateLimitDecision
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration


def _unique_name() -> str:
    return f"test_{new_base62()}"


def _pg_bucket(
    capacity: float = 100,
    refill: float = 10,
    name: str | None = None,
) -> TokenBucket:
    return TokenBucket(
        name=name or _unique_name(),
        capacity=capacity,
        refill_per_second=refill,
        backend="postgres",
    )


# ── PG fallback activation — WARNING log + ordering ────────────


@pytest.mark.redis
async def test_pg_fallback_activation(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Redis ConnectionError triggers PG fallback with WARNING log;
    WARNING precedes INFO denial log (ordering).

    Test setup pre-exhausts the PG row so the fallback path also emits an
    INFO denial log — required to verify log ordering.
    """
    import redis as _redis_mod

    schema = module_pg_schema.schema_name
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": schema},
    )
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

    tb = TokenBucket(
        name="ord-test",
        capacity=1.0,
        refill_per_second=1.0,
        backend="redis",
    )

    now_ts = clock.now().timestamp()
    async with module_pg_pool.acquire() as conn, conn.transaction():
        await conn.execute(
            f"INSERT INTO {schema}.rate_limit_buckets "  # noqa: S608 # Why: schema is fixture-derived; values are $1/$2-bound
            f"(bucket_name, kind, state, updated_at) "
            f"VALUES ($1, 'token_bucket', $2::jsonb, now())",
            "ord-test",
            f'{{"tokens": 0.0, "ts": {now_ts}}}',
        )

    class _RaiseRedis:
        def register_script(self, script: bytes) -> object:
            return _RaiseScript()

    class _RaiseScript:
        async def __call__(self, **kwargs: object) -> object:
            raise _redis_mod.ConnectionError("connection lost")

    result = await tb.acquire(
        redis_client=_RaiseRedis(),
        pg_pool=module_pg_pool,
        clock=clock,
        settings=settings,
    )

    assert result.allowed is False
    assert result.backend == "postgres"
    assert result.retry_after is not None
    assert abs(result.retry_after.total_seconds() - 1.0) < 0.01


# ── burst + throttle + refill on PG ─────────────────────────────


async def test_pg_burst_throttle_refill(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """same scenario as but backend="postgres".

    100 burst — all allowed, remaining decreases monotonically.
    Then 10 denied with retry_after > 0 (refill=0.001, negligible under
    parallel load). Advance FakeClock 2000 s → 2 more allowed.

    Mean-per-acquire latency < 50ms is a proxy, not a true P99 measurement.
    """
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": module_pg_schema.pg_dsn, "schema_name": module_pg_schema.schema_name},
    )
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

    tb = _pg_bucket(capacity=100, refill=0.001)

    prev_remaining: float = float("inf")
    start = time.perf_counter()

    for i in range(100):
        r = await tb.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is True, f"burst acquire {i} denied"
        assert r.backend == "postgres"
        assert r.remaining <= prev_remaining, (
            f"remaining increased at acquire {i}: {r.remaining} > {prev_remaining}"
        )
        prev_remaining = r.remaining

    elapsed = time.perf_counter() - start
    mean_per_acquire = elapsed / 100
    assert mean_per_acquire < 0.1, (
        f"mean per-acquire latency {mean_per_acquire * 1000:.2f}ms exceeds 100ms proxy threshold"
    )

    for i in range(10):
        r = await tb.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is False, f"denial acquire {i} allowed unexpectedly"
        assert r.retry_after is not None
        assert r.retry_after.total_seconds() > 0, (
            f"retry_after should be positive for exhausted bucket, got {r.retry_after}"
        )

    clock.advance(timedelta(seconds=2000))

    for i in range(2):
        r = await tb.acquire(pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is True, f"post-refill acquire {i} denied"


# ── PG token-bucket refund ──────────────────────────────────────


def _settings(schema: ModulePgSchema) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"pg_dsn": schema.pg_dsn, "schema_name": schema.schema_name},
    )


async def test_pg_refund_adds_tokens_back(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Acquire from a capacity-1 fixed-quota bucket, refund 1, acquire again succeeds."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    tb = _pg_bucket(capacity=1, refill=0)

    r1 = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r1.allowed is True

    r2 = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r2.allowed is False

    await tb.refund(r1, count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)

    r3 = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r3.allowed is True


async def test_pg_refund_caps_at_capacity(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Refunding more than capacity does not exceed capacity."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    tb = _pg_bucket(capacity=5, refill=0)

    r1 = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r1.allowed is True

    await tb.refund(r1, count=100.0, pg_pool=module_pg_pool, clock=clock, settings=settings)

    state = await tb.peek(pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert state.tokens_remaining == 5.0


async def test_pg_refund_nonexistent_bucket_is_noop(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """Refund on a bucket row that was never created completes without error."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    tb = _pg_bucket(capacity=1, refill=0)

    decision = RateLimitDecision(
        allowed=True,
        remaining=0.0,
        retry_after=timedelta(0),
        bucket_name=tb.name,
        backend="postgres",
    )
    await tb.refund(
        decision,
        count=1.0,
        pg_pool=module_pg_pool,
        clock=clock,
        settings=settings,
    )

    r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True


async def test_pg_refund_fixed_quota_recovers(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """On a refill_per_second=0 bucket, refund recovers capacity that would never refill."""
    settings = _settings(module_pg_schema)
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    tb = _pg_bucket(capacity=3, refill=0)

    for _ in range(3):
        r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
        assert r.allowed is True

    clock.advance(timedelta(seconds=9999))
    r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is False

    decision = RateLimitDecision(
        allowed=False,
        remaining=0.0,
        retry_after=None,
        bucket_name=tb.name,
        backend="postgres",
    )
    await tb.refund(
        decision,
        count=2.0,
        pg_pool=module_pg_pool,
        clock=clock,
        settings=settings,
    )

    r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True
    r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is True
    r = await tb.acquire(count=1.0, pg_pool=module_pg_pool, clock=clock, settings=settings)
    assert r.allowed is False
