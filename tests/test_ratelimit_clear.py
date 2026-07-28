"""Tests for RateLimitRegistry.clear() — the public test-isolation aid."""

from datetime import timedelta
from time import monotonic

from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket


def _bucket(name: str, capacity: float = 10.0) -> TokenBucket:
    return TokenBucket(name=name, capacity=capacity, refill_per_second=1.0, backend="memory")


def _reservation(name: str) -> ConcurrencyReservation:
    return ConcurrencyReservation(name=name, slots=2, lease=timedelta(seconds=30))


async def test_clear_resets_all_six_state_fields() -> None:
    reg = RateLimitRegistry()
    reg.register(_bucket("tb"))
    reg.register(_reservation("res"))
    reg._keyed_reservation_last_used["res:k1"] = monotonic()  # pyright: ignore[reportPrivateUsage]  # Why: seeding state to verify clear()
    reg._keyed_rate_limit_last_used["tb:k1"] = monotonic()  # pyright: ignore[reportPrivateUsage]  # Why: seeding state to verify clear()
    reg._keyed_reservation_last_eviction_scan = monotonic()  # pyright: ignore[reportPrivateUsage]  # Why: seeding state to verify clear()
    reg._keyed_rate_limit_last_eviction_scan = monotonic()  # pyright: ignore[reportPrivateUsage]  # Why: seeding state to verify clear()

    reg.clear()

    assert reg.rate_limits == {}
    assert reg.reservations == {}
    assert reg._keyed_reservation_last_used == {}  # pyright: ignore[reportPrivateUsage]
    assert reg._keyed_rate_limit_last_used == {}  # pyright: ignore[reportPrivateUsage]
    assert reg._keyed_reservation_last_eviction_scan == float("-inf")  # pyright: ignore[reportPrivateUsage]
    assert reg._keyed_rate_limit_last_eviction_scan == float("-inf")  # pyright: ignore[reportPrivateUsage]


async def test_clear_allows_reregistration_under_same_name_with_new_config() -> None:
    """After clear(), a name can be re-registered with a DIFFERENT config
    (no _same_config conflict), proving the primitive dicts were emptied."""
    reg = RateLimitRegistry()
    reg.register(_bucket("tb", capacity=10.0))

    reg.clear()
    reg.register(_bucket("tb", capacity=99.0))

    bucket = reg.rate_limits["tb"]
    assert isinstance(bucket, TokenBucket)  # narrows TokenBucket | SlidingWindow for pyright
    assert bucket.capacity == 99.0
