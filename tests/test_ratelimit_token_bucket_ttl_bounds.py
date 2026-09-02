"""A very slow refill rate must not blow up the bucket's TTL arithmetic.

The default key TTL is derived as "twice the time to refill from empty,
plus a minute". That quantity is unbounded as ``refill_per_second``
approaches zero, and ``timedelta`` tops out around 2.7 million years, so
any path that lets an operator configure an arbitrarily small positive
rate -- a trickling monthly quota, a value read from config -- raised
``OverflowError`` out of the constructor.

The rate itself stays valid: it is a legal ``refill_per_second >= 0`` and
the admission arithmetic handles it exactly. Only the TTL -- a storage
eviction hint that the token math never reads -- is clamped.
"""

from datetime import timedelta

import pytest

from taskq.ratelimit import TokenBucket

_TINY = 1e-15

# An operator-visible upper bound asserted as behaviour rather than by
# importing the implementation's constant: whatever ceiling the primitive
# picks, a bucket's key TTL must stay inside something a human would
# recognise as a retention policy.
_TTL_CEILING = timedelta(days=366)


def test_tiny_refill_rate_constructs() -> None:
    """The reported repro: a rate small enough to overflow timedelta."""
    bucket = TokenBucket("tiny", capacity=10.0, refill_per_second=_TINY, backend="memory")

    assert bucket.refill_per_second == _TINY
    assert bucket.capacity == 10.0
    assert timedelta(0) < bucket.ttl <= _TTL_CEILING


def test_denormal_refill_rate_constructs() -> None:
    """capacity / refill can overflow to inf before timedelta ever sees it."""
    bucket = TokenBucket("denormal", capacity=1e308, refill_per_second=5e-324, backend="memory")

    assert timedelta(0) < bucket.ttl <= _TTL_CEILING


def test_tiny_refill_bucket_still_admits_and_denies() -> None:
    """Clamping the TTL must not touch the admission arithmetic."""
    import asyncio

    from taskq.backend.clock import SystemClock

    bucket = TokenBucket("tiny-admit", capacity=2.0, refill_per_second=_TINY, backend="memory")
    clock = SystemClock()

    async def _run() -> list[bool]:
        return [(await bucket.acquire(clock=clock)).allowed for _ in range(3)]

    assert asyncio.run(_run()) == [True, True, False]


@pytest.mark.parametrize(
    ("capacity", "refill"),
    [(100.0, 10.0), (1.0, 1.0), (10.0, 0.001)],
)
def test_ordinary_rates_keep_their_computed_ttl(capacity: float, refill: float) -> None:
    """The clamp must not disturb any rate an operator would plausibly set."""
    bucket = TokenBucket("ordinary", capacity=capacity, refill_per_second=refill, backend="memory")

    expected = timedelta(seconds=int(capacity / refill * 2) + 60)
    assert bucket.ttl == expected
    assert bucket.ttl < _TTL_CEILING


def test_fixed_quota_ttl_is_unchanged() -> None:
    """A zero refill rate keeps its own 24 h policy, not the clamp."""
    bucket = TokenBucket("fixed", capacity=5.0, refill_per_second=0.0, backend="memory")

    assert bucket.ttl == timedelta(seconds=86400)


# ── Redis key TTL ────────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_key_ttl_honours_an_explicit_ttl(redis_url: str) -> None:
    """An explicit ``ttl=`` must reach the Redis key.

    ``TokenBucket.ttl`` reported the operator's value while the Redis EXPIRE
    was re-derived from capacity/refill, so the key outlived (or undershot)
    the configured retention on the one backend where the TTL decides whether
    consumed quota survives. ``SlidingWindow`` already used its own ``ttl``.
    """
    import redis.asyncio as redis_async

    from taskq.backend.clock import SystemClock
    from taskq.settings import WorkerSettings

    schema = "taskq_test"
    bucket = TokenBucket(
        "explicit-ttl",
        capacity=100.0,
        refill_per_second=10.0,
        backend="redis",
        ttl=timedelta(seconds=123),
    )
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": schema},
    )
    client = redis_async.from_url(redis_url, decode_responses=False)
    try:
        await bucket.acquire(redis_client=client, clock=SystemClock(), settings=settings)
        ttl = await client.ttl(f"taskq:{schema}:rl:tb:{{{bucket.name}}}")
    finally:
        await client.aclose()

    assert 122 <= ttl <= 123, f"expected the configured 123 s TTL on the key, got {ttl}"


@pytest.mark.integration
@pytest.mark.redis
async def test_redis_key_ttl_is_bounded_for_a_tiny_refill(redis_url: str) -> None:
    """The clamped default reaches Redis as a sane EXPIRE, not an overflow."""
    import redis.asyncio as redis_async

    from taskq.backend.clock import SystemClock
    from taskq.settings import WorkerSettings

    schema = "taskq_test"
    bucket = TokenBucket("tiny-redis", capacity=10.0, refill_per_second=_TINY, backend="redis")
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url, "schema_name": schema},
    )
    client = redis_async.from_url(redis_url, decode_responses=False)
    try:
        await bucket.acquire(redis_client=client, clock=SystemClock(), settings=settings)
        ttl = await client.ttl(f"taskq:{schema}:rl:tb:{{{bucket.name}}}")
    finally:
        await client.aclose()

    assert 0 < ttl <= int(_TTL_CEILING.total_seconds())
