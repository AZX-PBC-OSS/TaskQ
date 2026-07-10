"""Integration tests for TokenBucket Redis backend against testcontainers Redis.

100-burst acceptance — all allowed, remaining decreases monotonically;
        then 10 denied with retry_after≈1s; sleep 2s → 2 more allowed.
EVALSHA caching — register_script called exactly once across acquires.
TTL set correctly after one acquire.
Key format matches ``taskq:{schema}:rl:tb:{bucket_name}`` with hash tag.
"""

import asyncio
import math
import time

import pytest
import redis.asyncio as redis_async

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.ratelimit import TokenBucket
from taskq.settings import WorkerSettings

pytestmark = [pytest.mark.integration, pytest.mark.redis]

_SCHEMA_LABEL = "taskq_test"


def _unique_name() -> str:
    return f"test_{new_base62()}"


def _redis_bucket(
    capacity: float = 100,
    refill: float = 10,
    name: str | None = None,
) -> TokenBucket:
    return TokenBucket(
        name=name or _unique_name(),
        capacity=capacity,
        refill_per_second=refill,
        backend="redis",
    )


async def _make_client(redis_url: str) -> redis_async.Redis:
    return redis_async.from_url(redis_url, decode_responses=False)


def _settings(redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _SCHEMA_LABEL,
        },
    )


# ── acceptance definition — 100 burst + 10 denied + 10 after refill ──


async def test_burst_acceptance(redis_url: str) -> None:
    """100 burst all allowed with monotonically decreasing remaining;
    then denied acquires have retry_after≈1s (refill=1 makes this robust
    against Docker/TCP latency); sleep 2s → 2 more allowed.

    Mean-per-acquire latency < 1 ms is an smoke test, not a true P99.
    """
    tb = _redis_bucket(capacity=100, refill=1)
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        prev_remaining: float = float("inf")
        start = time.perf_counter()

        for i in range(100):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"burst acquire {i} denied"
            assert r.backend == "redis"
            assert r.remaining <= prev_remaining, (
                f"remaining increased at acquire {i}: {r.remaining} > {prev_remaining}"
            )
            prev_remaining = r.remaining

        elapsed = time.perf_counter() - start
        mean_per_acquire = elapsed / 100
        assert mean_per_acquire < 0.001, (
            f"mean per-acquire latency {mean_per_acquire * 1000:.2f}ms exceeds 1ms proxy threshold"
        )

        for i in range(10):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is False, f"denial acquire {i} allowed unexpectedly"
            assert r.retry_after is not None
            assert abs(r.retry_after.total_seconds() - 1.0) < 0.5

        await asyncio.sleep(2.0)

        for i in range(2):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"post-refill acquire {i} denied"
    finally:
        await client.aclose()


# ── EVALSHA caching — register_script called once ─────────────


async def test_evalsha_caching(redis_url: str) -> None:
    """register_script is called exactly once across two acquires."""
    tb = _redis_bucket()
    client = redis_async.from_url(redis_url, decode_responses=False)
    settings = _settings(redis_url)
    clock = SystemClock()

    register_count = 0
    original_register = client.register_script

    def _counting_register(script: bytes) -> object:
        nonlocal register_count
        register_count += 1
        return original_register(script)

    client.register_script = _counting_register  # type: ignore[assignment] # Why: test spy wraps the real register_script to count calls

    try:
        r1 = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        r2 = await tb.acquire(redis_client=client, clock=clock, settings=settings)

        assert r1.allowed is True
        assert r2.backend == "redis"
        assert register_count == 1
    finally:
        await client.aclose()


# ── TTL set correctly ──────────────────────────────────────────


async def test_ttl_set_correctly(redis_url: str) -> None:
    """after one acquire, the key TTL is within ±1s of the computed value."""
    capacity = 100
    refill = 10
    tb = _redis_bucket(capacity=capacity, refill=refill)
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        await tb.acquire(redis_client=client, clock=clock, settings=settings)

        key = f"taskq:{_SCHEMA_LABEL}:rl:tb:{{{tb.name}}}"
        actual_ttl = await client.ttl(key)
        expected_ttl = math.ceil(capacity / refill * 2) + 60

        assert actual_ttl >= expected_ttl - 1
        assert actual_ttl <= expected_ttl + 1
    finally:
        await client.aclose()


# ── key format ─────────────────────────────────────────────────


async def test_key_format(redis_url: str) -> None:
    """the key in Redis is exactly ``taskq:{schema}:rl:tb:{bucket_name}``
    with literal curly braces (Cluster hash tag).
    """
    tb = _redis_bucket()
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        await tb.acquire(redis_client=client, clock=clock, settings=settings)

        expected_key = f"taskq:{_SCHEMA_LABEL}:rl:tb:{{{tb.name}}}"
        exists = await client.exists(expected_key)
        assert exists == 1
    finally:
        await client.aclose()


# ── Regression: fractional tokens_remaining / retry_after preserved ────


async def test_fractional_values_preserved_through_redis(redis_url: str) -> None:
    """Regression: Lua tostring() preserves fractional tokens_remaining and
    retry_after_seconds that Redis RESP2 would otherwise truncate to integers.

    Uses capacity=1.5, refill=0.25, count=1.0 so that after one allowed
    acquire, remaining=0.5 (fractional). Without tostring(), Redis would
    truncate 0.5→0, breaking the fractional remaining.
    """
    tb = _redis_bucket(capacity=1.5, refill=0.25)
    client = await _make_client(redis_url)
    settings = _settings(redis_url)
    clock = SystemClock()

    try:
        r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is True
        assert r.remaining == 0.5

        r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
        assert r.allowed is False
        assert r.retry_after is not None
        assert r.retry_after.total_seconds() > 0
        assert r.remaining != int(r.remaining)
    finally:
        await client.aclose()
