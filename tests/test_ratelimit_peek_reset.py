"""Tests for TokenBucket/SlidingWindow peek() and reset() across all backends.

Covers:
- In-memory peek/reset (unit tests with FakeClock)
- Redis peek/reset (integration tests)
- PG peek/reset (integration tests)
- Registry peek/peek_all/reset
- RateLimitState dataclass fields
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq.backend.clock import SystemClock
from taskq.ratelimit import SlidingWindow, TokenBucket
from taskq.ratelimit.decision import RateLimitState
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)
_UNIT_SCHEMA_LABEL = "taskq_test"


def _tb_memory(
    capacity: float = 100,
    refill: float = 10,
    name: str = "test",
) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=refill, backend="memory")


def _sw_memory(
    name: str = "sw",
    limit: int = 10,
    window: timedelta = timedelta(seconds=60),
    style: str = "log",
) -> SlidingWindow:
    return SlidingWindow(name=name, limit=limit, window=window, backend="memory", style=style)  # type: ignore[arg-type] # Why: style is validated at runtime


# ════════════════════════════════════════════════════════════════════
# TokenBucket — in-memory peek tests
# ════════════════════════════════════════════════════════════════════


async def test_peek_does_not_consume_token() -> None:
    """peek() 3x on a bucket with 1 token → all return is_exhausted=False, tokens_remaining=1."""
    tb = _tb_memory(capacity=10, refill=10)
    clock = FakeClock(_START)

    # Consume all but 1
    for _ in range(9):
        await tb.acquire(clock=clock)

    for _ in range(3):
        state = await tb.peek(clock=clock)
        assert state.is_exhausted is False
        assert state.tokens_remaining == 1.0
        assert state.backend == "memory"

    # acquire still works
    r = await tb.acquire(clock=clock)
    assert r.allowed is True
    assert r.remaining == 0.0


async def test_peek_on_empty_bucket() -> None:
    """peek() on exhausted bucket → is_exhausted=True, tokens_remaining=0."""
    tb = _tb_memory(capacity=5, refill=0)
    clock = FakeClock(_START)

    for _ in range(5):
        await tb.acquire(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.tokens_remaining == 0.0


async def test_peek_refill_computation() -> None:
    """Wait, then peek() — tokens have refilled (but not consumed)."""
    tb = _tb_memory(capacity=100, refill=10)
    clock = FakeClock(_START)

    # consume 50
    await tb.acquire(count=50, clock=clock)

    # peek sees refill
    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == 50.0

    # advance 5 seconds → should see 50 more tokens refilled
    clock.advance(timedelta(seconds=5))
    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == pytest.approx(100.0, rel=1e-9)

    # but acquire still shows we haven't consumed
    r = await tb.acquire(count=50, clock=clock)
    assert r.allowed is True
    assert r.remaining == 50.0


async def test_peek_retry_after() -> None:
    """peek() on exhausted bucket → retry_after matches the time until next token."""
    tb = _tb_memory(capacity=1, refill=10)
    clock = FakeClock(_START)

    # consume the single token
    await tb.acquire(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.retry_after is not None
    assert abs(state.retry_after.total_seconds() - 0.1) < 0.01


async def test_peek_on_fresh_bucket() -> None:
    """peek() on never-used bucket shows full capacity."""
    tb = _tb_memory(capacity=100, refill=10)
    clock = FakeClock(_START)

    state = await tb.peek(clock=clock)
    assert state.is_exhausted is False
    assert state.tokens_remaining == 100.0
    assert state.capacity == 100.0
    assert state.refill_per_second == 10.0


async def test_peek_capacity_and_refill_fields() -> None:
    """RateLimitState for TB has capacity and refill_per_second set."""
    tb = _tb_memory(capacity=50, refill=5)
    clock = FakeClock(_START)

    state = await tb.peek(clock=clock)
    assert state.capacity == 50.0
    assert state.refill_per_second == 5.0
    assert state.limit is None
    assert state.window is None
    assert state.style is None


# ════════════════════════════════════════════════════════════════════
# TokenBucket — in-memory reset tests
# ════════════════════════════════════════════════════════════════════


async def test_reset_restores_full_capacity() -> None:
    """acquire 9/10 tokens → reset() → peek() shows 10 tokens."""
    tb = _tb_memory(capacity=10, refill=10)
    clock = FakeClock(_START)

    await tb.acquire(count=9, clock=clock)
    await tb.reset(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == 10.0
    assert state.is_exhausted is False


async def test_reset_idempotent() -> None:
    """reset() twice → no error, bucket at full capacity."""
    tb = _tb_memory(capacity=10, refill=10)
    clock = FakeClock(_START)

    await tb.acquire(count=5, clock=clock)
    await tb.reset(clock=clock)
    await tb.reset(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == 10.0


async def test_reset_on_nonexistent_bucket() -> None:
    """reset() on never-acquired bucket → no error."""
    tb = _tb_memory(capacity=10, refill=10)
    clock = FakeClock(_START)
    await tb.reset(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == 10.0


async def test_acquire_after_reset() -> None:
    """reset() → acquire() → allowed=True with full tokens remaining."""
    tb = _tb_memory(capacity=10, refill=10)
    clock = FakeClock(_START)

    await tb.acquire(count=10, clock=clock)  # exhaust
    await tb.reset(clock=clock)

    r = await tb.acquire(count=1, clock=clock)
    assert r.allowed is True
    assert r.remaining == 9.0


async def test_reset_fixed_quota() -> None:
    """reset() on fixed-quota bucket restores full tokens."""
    tb = _tb_memory(capacity=5, refill=0)
    clock = FakeClock(_START)

    for _ in range(5):
        await tb.acquire(clock=clock)

    await tb.reset(clock=clock)

    state = await tb.peek(clock=clock)
    assert state.tokens_remaining == 5.0
    assert state.is_exhausted is False


# ════════════════════════════════════════════════════════════════════
# SlidingWindow — in-memory log-style peek/reset tests
# ════════════════════════════════════════════════════════════════════


async def test_sw_log_peek_no_consumption() -> None:
    """peek() on log-style SW does not consume capacity."""
    sw = _sw_memory(limit=5, style="log")
    clock = FakeClock(_START)

    for _ in range(3):
        await sw.acquire(clock=clock)

    for _ in range(3):
        state = await sw.peek(clock=clock)
        assert state.remaining == 2.0
        assert state.is_exhausted is False

    # acquire still works for remaining
    r = await sw.acquire(clock=clock)
    assert r.allowed is True


async def test_sw_log_peek_exhausted() -> None:
    """peek() on exhausted log-style SW returns is_exhausted=True."""
    sw = _sw_memory(limit=3, style="log")
    clock = FakeClock(_START)

    for _ in range(3):
        await sw.acquire(clock=clock)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.remaining == 0.0
    assert state.retry_after is not None
    assert state.limit == 3
    assert state.style == "log"


async def test_sw_log_peek_retry_after() -> None:
    """Log-style peek retry_after computed from oldest entry."""
    sw = _sw_memory(limit=2, window=timedelta(seconds=60), style="log")
    clock = FakeClock(_START)

    await sw.acquire(clock=clock)
    # advance a bit then acquire second
    clock.advance(timedelta(seconds=10))
    await sw.acquire(clock=clock)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.retry_after is not None
    # oldest was at t=0, window=60s, now=10s → retry=50s
    assert abs(state.retry_after.total_seconds() - 50.0) < 1.0


async def test_sw_log_reset_clears_state() -> None:
    """reset() on log-style SW clears all entries."""
    sw = _sw_memory(limit=5, style="log")
    clock = FakeClock(_START)

    for _ in range(4):
        await sw.acquire(clock=clock)

    await sw.reset(clock=clock)

    state = await sw.peek(clock=clock)
    assert state.remaining == 5.0
    assert state.is_exhausted is False


async def test_sw_log_peek_fields() -> None:
    """Log-style peek populates limit, window, style; not capacity."""
    sw = _sw_memory(limit=7, window=timedelta(seconds=30), style="log")
    clock = FakeClock(_START)

    state = await sw.peek(clock=clock)
    assert state.limit == 7
    assert state.window == timedelta(seconds=30)
    assert state.style == "log"
    assert state.capacity is None
    assert state.refill_per_second is None
    assert state.tokens_remaining == 0.0


# ════════════════════════════════════════════════════════════════════
# SlidingWindow — in-memory GCRA peek/reset tests
# ════════════════════════════════════════════════════════════════════


async def test_sw_gcra_peek_no_consumption() -> None:
    """peek() on GCRA does not advance TAT or consume capacity."""
    sw = _sw_memory(limit=5, style="gcra")
    clock = FakeClock(_START)

    await sw.acquire(clock=clock)

    for _ in range(3):
        state = await sw.peek(clock=clock)
        assert state.is_exhausted is False
        # remaining should be consistent
        assert state.remaining >= 0.0

    # acquire still works
    r = await sw.acquire(clock=clock)
    assert r.allowed is True


async def test_sw_gcra_peek_exhausted() -> None:
    """peek() on exhausted GCRA returns is_exhausted=True."""
    sw = _sw_memory(limit=2, window=timedelta(seconds=60), style="gcra")
    clock = FakeClock(_START)

    for _ in range(2):
        await sw.acquire(clock=clock)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is True
    assert state.style == "gcra"
    assert state.limit == 2


async def test_sw_gcra_reset_clears_tat() -> None:
    """reset() on GCRA clears TAT and log, restoring full capacity."""
    sw = _sw_memory(limit=3, style="gcra")
    clock = FakeClock(_START)

    for _ in range(3):
        await sw.acquire(clock=clock)

    await sw.reset(clock=clock)

    state = await sw.peek(clock=clock)
    assert state.is_exhausted is False
    assert state.remaining > 0


async def test_sw_gcra_peek_fields() -> None:
    """GCRA peek populates limit, window, style; not capacity."""
    sw = _sw_memory(limit=10, window=timedelta(seconds=120), style="gcra")
    clock = FakeClock(_START)

    state = await sw.peek(clock=clock)
    assert state.limit == 10
    assert state.window == timedelta(seconds=120)
    assert state.style == "gcra"
    assert state.capacity is None
    assert state.refill_per_second is None


# ════════════════════════════════════════════════════════════════════
# Registry peek/peek_all/reset tests
# ════════════════════════════════════════════════════════════════════


async def test_registry_peek_returns_state() -> None:
    """registry.peek() returns RateLimitState for a registered TB."""
    reg = RateLimitRegistry()
    tb = _tb_memory(capacity=10, refill=2)
    reg.register(tb)
    clock = FakeClock(_START)

    # consume some tokens
    await tb.acquire(count=3, clock=clock)

    state = await reg.peek("test", clock=clock)
    assert state.tokens_remaining == 7.0
    assert state.is_exhausted is False
    assert state.bucket_name == "test"


async def test_registry_peek_unknown_raises() -> None:
    """registry.peek() raises KeyError for unregistered name."""
    reg = RateLimitRegistry()
    clock = FakeClock(_START)
    with pytest.raises(KeyError):
        await reg.peek("unknown", clock=clock)


async def test_registry_peek_all_returns_all_buckets() -> None:
    """Register 3 buckets → peek_all() returns 3 entries."""
    reg = RateLimitRegistry()
    reg.register(_tb_memory(capacity=10, refill=1, name="tb1"))
    reg.register(_tb_memory(capacity=10, refill=1, name="tb2"))
    reg.register(_sw_memory(name="sw1", limit=5))
    clock = FakeClock(_START)

    results = await reg.peek_all(clock=clock)
    assert len(results) == 3
    assert "tb1" in results
    assert "tb2" in results
    assert "sw1" in results

    for state in results.values():
        assert isinstance(state, RateLimitState)


async def test_registry_reset_clears_bucket() -> None:
    """registry.reset() restores full capacity."""
    reg = RateLimitRegistry()
    tb = _tb_memory(capacity=10, refill=2)
    reg.register(tb)
    clock = FakeClock(_START)

    await tb.acquire(count=8, clock=clock)
    await reg.reset("test", clock=clock)

    state = await reg.peek("test", clock=clock)
    assert state.tokens_remaining == 10.0


async def test_registry_peek_all_includes_reservations() -> None:
    """peek_all() only peeks rate limits, not reservations."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    reg = RateLimitRegistry()
    reg.register(_tb_memory(capacity=10, refill=1, name="tb1"))

    clock_fake = FakeClock(_START)
    res = ConcurrencyReservation(name="gpu", slots=4, lease=timedelta(seconds=10), clock=clock_fake)
    reg.register(res)

    results = await reg.peek_all(clock=clock_fake)
    # Should contain only rate limits, not reservations
    assert "tb1" in results
    assert "gpu" not in results


# ════════════════════════════════════════════════════════════════════
# TokenBucket peek/reset — memory backend error paths
# ════════════════════════════════════════════════════════════════════


async def test_peek_memory_without_clock_raises() -> None:
    """peek() without clock raises RuntimeError for memory backend."""
    tb = _tb_memory()
    with pytest.raises(RuntimeError, match="clock not injected"):
        await tb.peek()


async def test_reset_memory_without_clock_raises() -> None:
    """reset() without clock raises RuntimeError for memory backend."""
    tb = _tb_memory()
    with pytest.raises(RuntimeError, match="clock not injected"):
        await tb.reset()


# ════════════════════════════════════════════════════════════════════
# RateLimitState dataclass tests
# ════════════════════════════════════════════════════════════════════


def test_ratelimit_state_is_frozen() -> None:
    """RateLimitState is frozen — cannot assign attributes."""
    state = RateLimitState(
        bucket_name="test",
        backend="memory",
        is_exhausted=False,
    )
    with pytest.raises(AttributeError):
        state.is_exhausted = True  # type: ignore[misc]


def test_ratelimit_state_defaults() -> None:
    """RateLimitState has sensible defaults for all optional fields."""
    state = RateLimitState(
        bucket_name="test",
        backend="redis",
        is_exhausted=False,
    )
    assert state.tokens_remaining == 0.0
    assert state.remaining == 0.0
    assert state.retry_after is None
    assert state.capacity is None
    assert state.limit is None
    assert state.window is None
    assert state.style is None
    assert state.refill_per_second is None


# ════════════════════════════════════════════════════════════════════
# Redis integration tests (require testcontainers)
# ════════════════════════════════════════════════════════════════════


@pytest.mark.integration
@pytest.mark.redis
async def test_reset_on_tb_redis(redis_url: str) -> None:
    """Redis DEL clears the hash; peek shows full capacity."""
    import redis.asyncio as redis_async

    from taskq._ids import new_base62

    name = f"tb_peek_{new_base62()}"
    tb = TokenBucket(name=name, capacity=20, refill_per_second=5, backend="redis")
    client = redis_async.from_url(redis_url, decode_responses=False)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _UNIT_SCHEMA_LABEL,
        },
    )
    clock = SystemClock()

    # acquire tokens
    await tb.acquire(count=15, redis_client=client, clock=clock, settings=settings)

    # peek should show reduced tokens
    state = await tb.peek(redis_client=client, clock=clock, settings=settings)
    assert state.tokens_remaining < 20.0

    # reset
    await tb.reset(redis_client=client, settings=settings)

    # peek should show full capacity
    state = await tb.peek(redis_client=client, clock=clock, settings=settings)
    assert state.tokens_remaining == 20.0

    # acquire after reset works from full
    r = await tb.acquire(count=1, redis_client=client, clock=clock, settings=settings)
    assert r.allowed is True

    await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_reset_on_sw_gcra_redis(redis_url: str) -> None:
    """GCRA reset clears TAT string key."""
    import redis.asyncio as redis_async

    from taskq._ids import new_base62

    name = f"sw_gcra_{new_base62()}"
    sw = SlidingWindow(
        name=name, limit=3, window=timedelta(seconds=60), backend="redis", style="gcra"
    )
    client = redis_async.from_url(redis_url, decode_responses=False)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _UNIT_SCHEMA_LABEL,
        },
    )
    clock = SystemClock()

    # exhaust the window
    for _ in range(3):
        await sw.acquire(redis_client=client, clock=clock, settings=settings)

    # peek should show exhausted
    state = await sw.peek(redis_client=client, clock=clock, settings=settings)
    assert state.is_exhausted is True

    # reset
    await sw.reset(redis_client=client, settings=settings)

    # peek should show not exhausted
    state = await sw.peek(redis_client=client, clock=clock, settings=settings)
    assert state.is_exhausted is False

    await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_reset_on_sw_log_redis(redis_url: str) -> None:
    """Log-style SW reset clears sorted set."""
    import redis.asyncio as redis_async

    from taskq._ids import new_base62

    name = f"sw_log_{new_base62()}"
    sw = SlidingWindow(
        name=name, limit=2, window=timedelta(seconds=60), backend="redis", style="log"
    )
    client = redis_async.from_url(redis_url, decode_responses=False)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _UNIT_SCHEMA_LABEL,
        },
    )
    clock = SystemClock()

    for _ in range(2):
        await sw.acquire(redis_client=client, clock=clock, settings=settings)

    state = await sw.peek(redis_client=client, clock=clock, settings=settings)
    assert state.is_exhausted is True

    await sw.reset(redis_client=client, settings=settings)

    state = await sw.peek(redis_client=client, clock=clock, settings=settings)
    assert state.is_exhausted is False
    assert state.remaining == 2.0

    await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_reset_idempotent_redis(redis_url: str) -> None:
    """reset() twice on Redis-backed bucket → no error, full capacity."""
    import redis.asyncio as redis_async

    from taskq._ids import new_base62

    name = f"tb_idem_{new_base62()}"
    tb = TokenBucket(name=name, capacity=30, refill_per_second=10, backend="redis")
    client = redis_async.from_url(redis_url, decode_responses=False)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _UNIT_SCHEMA_LABEL,
        },
    )
    clock = SystemClock()

    await tb.acquire(count=5, redis_client=client, clock=clock, settings=settings)

    await tb.reset(redis_client=client, settings=settings)
    await tb.reset(redis_client=client, settings=settings)

    state = await tb.peek(redis_client=client, clock=clock, settings=settings)
    assert state.tokens_remaining == 30.0

    await client.aclose()


@pytest.mark.integration
@pytest.mark.redis
async def test_reset_nonexistent_key_redis(redis_url: str) -> None:
    """reset() on never-acquired Redis bucket → no error."""
    import redis.asyncio as redis_async

    from taskq._ids import new_base62

    name = f"tb_nonex_{new_base62()}"
    tb = TokenBucket(name=name, capacity=10, refill_per_second=1, backend="redis")
    client = redis_async.from_url(redis_url, decode_responses=False)

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _UNIT_SCHEMA_LABEL,
        },
    )

    # reset on nonexistent key — DEL returns 0, no error
    await tb.reset(redis_client=client, settings=settings)

    # peek still shows full
    clock = SystemClock()
    state = await tb.peek(redis_client=client, clock=clock, settings=settings)
    assert state.tokens_remaining == 10.0

    await client.aclose()


# ════════════════════════════════════════════════════════════════════
# PG integration tests (require testcontainers)
# ════════════════════════════════════════════════════════════════════


@pytest.mark.integration
async def test_reset_on_tb_pg(clean_pg_conn: object, module_pg_schema: object) -> None:
    """PG DELETE removes the row; peek shows full capacity."""
    from taskq._ids import new_base62

    name = f"tb_pg_{new_base62()}"

    schema_name: str = module_pg_schema.schema_name  # type: ignore[union-attr]
    pg_dsn: str = module_pg_schema.pg_dsn  # type: ignore[union-attr]

    tb = TokenBucket(name=name, capacity=15, refill_per_second=5, backend="postgres")

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": pg_dsn, "schema_name": schema_name},
    )

    import asyncpg

    pool: asyncpg.Pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)

    try:
        clock = SystemClock()

        # acquire tokens
        await tb.acquire(count=10, pg_pool=pool, clock=clock, settings=settings)

        # peek should show reduced tokens
        state = await tb.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.tokens_remaining < 15.0

        # reset
        await tb.reset(pg_pool=pool, settings=settings)

        # peek should show full capacity (row was deleted)
        state = await tb.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.tokens_remaining == 15.0

        # acquire from fresh
        r = await tb.acquire(count=1, pg_pool=pool, clock=clock, settings=settings)
        assert r.allowed is True
    finally:
        await pool.close()


@pytest.mark.integration
async def test_reset_on_sw_gcra_pg(clean_pg_conn: object, module_pg_schema: object) -> None:
    """PG GCRA reset deletes the row."""
    from taskq._ids import new_base62

    name = f"sw_gcra_{new_base62()}"

    schema_name: str = module_pg_schema.schema_name  # type: ignore[union-attr]
    pg_dsn: str = module_pg_schema.pg_dsn  # type: ignore[union-attr]

    sw = SlidingWindow(
        name=name, limit=3, window=timedelta(seconds=60), backend="postgres", style="gcra"
    )

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": pg_dsn, "schema_name": schema_name},
    )

    import asyncpg

    pool: asyncpg.Pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)

    try:
        clock = SystemClock()

        for _ in range(3):
            await sw.acquire(pg_pool=pool, clock=clock, settings=settings)

        state = await sw.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.is_exhausted is True

        await sw.reset(pg_pool=pool, settings=settings)

        state = await sw.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.is_exhausted is False
    finally:
        await pool.close()


@pytest.mark.integration
async def test_reset_on_sw_log_pg(clean_pg_conn: object, module_pg_schema: object) -> None:
    """PG log-style SW reset deletes window entries."""
    from taskq._ids import new_base62

    name = f"sw_log_{new_base62()}"

    schema_name: str = module_pg_schema.schema_name  # type: ignore[union-attr]
    pg_dsn: str = module_pg_schema.pg_dsn  # type: ignore[union-attr]

    sw = SlidingWindow(
        name=name, limit=2, window=timedelta(seconds=60), backend="postgres", style="log"
    )

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": pg_dsn, "schema_name": schema_name},
    )

    import asyncpg

    pool: asyncpg.Pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)

    try:
        clock = SystemClock()

        for _ in range(2):
            await sw.acquire(pg_pool=pool, clock=clock, settings=settings)

        state = await sw.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.is_exhausted is True

        await sw.reset(pg_pool=pool, settings=settings)

        state = await sw.peek(pg_pool=pool, clock=clock, settings=settings)
        assert state.is_exhausted is False
        assert state.remaining == 2.0
    finally:
        await pool.close()
