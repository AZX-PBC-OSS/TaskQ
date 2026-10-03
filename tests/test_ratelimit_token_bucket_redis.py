"""Integration tests for TokenBucket Redis backend against testcontainers Redis.

100-burst acceptance - all allowed, remaining decreases monotonically;
        then 10 denied with retry_after≈1s; sleep 2s → 2 more allowed.
EVALSHA caching - register_script called exactly once across acquires.
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
from tests.conftest import interpreter_is_traced

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
    return redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)


def _settings(redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _SCHEMA_LABEL,
        },
    )


# ── acceptance definition - 100 burst + 10 denied + 10 after refill ──


async def test_burst_acceptance(private_redis_url: str) -> None:
    """100 burst all allowed with monotonically decreasing remaining;
    then denied acquires have retry_after≈1s (refill=1 makes this robust
    against Docker/TCP latency); sleep 2s → 2 more allowed.

    Mean-per-acquire latency < 1 ms is an smoke test, not a true P99.
    Timed on a private broker: a mean-per-acquire assert times the
    broker's replies, and the tenancy contract reserves every
    broker-latency assert for a per-test private Dragonfly.

    The denial hint is pinned STATE-NOT-TIME, not to a wall-clock
    window: the script computes the hint and the remaining count from
    the same reply (hint == (count - remaining) / refill, exactly), so
    the pin relates the decision's own two fields and holds at ANY
    runner speed. The retired ``±0.5s around 1.0`` window was the
    hope-timing member of this test: it priced the TEST's own
    round-trip gaps, and the coverage leg's stall band (CI run
    37092146919, attempt 1) landed a ~0.8s gap between two denial
    round trips - the 1 token/s refill eroded the deficit to 0.212s and
    the window read ``assert 0.788 < 0.5`` on a bucket that was
    arithmetic-dead honest. The invariant keeps every real hint defect
    red at any speed: a deficit applied twice, a refill unit bug, a
    capacity-scaled hint, a truncated or zeroed hint all break the
    equality; the refill-erosion case satisfies it by construction.

    The wall-clock legs that remain (the mean smoke gate, the
    monotonic-remaining walk, the exhaustion premise that all ten
    denials land) hold only on an untraced interpreter - under the
    tracer the measurement is the tracer's, not the bucket's (the same
    ``interpreter_is_traced`` guard the #630 redaction budget and the
    fleet pins carry; the hint invariant below is asserted either way,
    which is what keeps this test's discrimination duty on the coverage
    lane, not a skip).
    """
    traced = interpreter_is_traced()
    tb = _redis_bucket(capacity=100, refill=1)
    client = await _make_client(private_redis_url)
    settings = _settings(private_redis_url)
    clock = SystemClock()

    try:
        prev_remaining: float = float("inf")
        start = time.perf_counter()

        for i in range(100):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"burst acquire {i} denied"
            assert r.backend == "redis"
            # Monotonicity bets no stall outlasts the refill window
            # inside the walk (a >1s gap lets the 1 token/s refill
            # out-earn the 1-token spend); under the tracer that gap is
            # the tracer's, so the walk holds on the untraced lanes
            # only.
            if not traced:
                assert r.remaining <= prev_remaining, (
                    f"remaining increased at acquire {i}: {r.remaining} > {prev_remaining}"
                )
            prev_remaining = r.remaining

        elapsed = time.perf_counter() - start
        mean_per_acquire = elapsed / 100
        # Smoke gate against catastrophic regressions only (e.g. one
        # EVALSHA round trip accidentally becoming N sequential calls) -
        # NOT a perf gate: under full-suite parallel load the Docker VM
        # is contended enough that a 1ms mean flakes. 10ms still catches
        # a 20x+ round-trip regression. Traced-guarded: the per-line
        # tracer tax is orders of magnitude above the gate.
        if not traced:
            assert mean_per_acquire < 0.010, (
                f"mean per-acquire latency {mean_per_acquire * 1000:.2f}ms exceeds 10ms smoke threshold"
            )

        for i in range(10):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            # The exhaustion premise: the bucket spent the whole burst
            # and ten round trips at ~1 token/s refill cannot earn one
            # back between them - on an untraced runner. Under the
            # tracer a stall CAN legitimately refill a full token
            # mid-loop (correct arithmetic, void premise), so the
            # premise is held on the untraced lanes only; the hint
            # invariant below asserts on every denial either way.
            if not traced:
                assert r.allowed is False, f"denial acquire {i} allowed unexpectedly"
            if r.allowed:
                continue
            assert r.retry_after is not None
            # STATE-NOT-TIME: the hint against the decision's own
            # remaining, the exact arithmetic the script runs. refill=1
            # makes the deficit the hint in seconds; the equality is
            # exact up to the timedelta's microsecond round trip.
            expected_hint = (1.0 - r.remaining) / 1.0
            assert abs(r.retry_after.total_seconds() - expected_hint) <= 1e-6, (
                f"denial acquire {i}: retry_after {r.retry_after.total_seconds()}s does not "
                f"match the deficit over refill from the decision's own state "
                f"({r.remaining} remaining): expected {expected_hint}s"
            )
            # Absolute anchor, timing-free: a denial's deficit is
            # (0, 1] tokens against refill=1, so the hint is bounded by
            # one full refill window - a hint past 1s has scaled by
            # something that is not the deficit.
            assert 0 < r.retry_after.total_seconds() <= 1.0, (
                f"denial acquire {i}: retry_after {r.retry_after.total_seconds()}s outside "
                f"the (0, 1] refill-window bound"
            )

        await asyncio.sleep(2.0)

        for i in range(2):
            r = await tb.acquire(redis_client=client, clock=clock, settings=settings)
            assert r.allowed is True, f"post-refill acquire {i} denied"
    finally:
        await client.aclose()


# ── EVALSHA caching - register_script called once ─────────────


async def test_evalsha_caching(redis_url: str) -> None:
    """register_script is called exactly once across two acquires."""
    tb = _redis_bucket()
    client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
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
