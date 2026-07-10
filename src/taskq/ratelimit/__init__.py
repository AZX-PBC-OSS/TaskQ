"""Rate-limiting primitives for TaskQ."""

from taskq.backend._protocol import RateLimitBackend
from taskq.ratelimit._provider import (
    get_redis_pool,
    register_rate_limit_registry,
    register_redis_pool,
)
from taskq.ratelimit.composition import (
    AcquiredResource,
    RateLimitHandle,
    ReservationHandle,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState
from taskq.ratelimit.refs import KeyedReservationRef, RateLimitRef, ReservationRef
from taskq.ratelimit.registry import RateLimitRegistry, registry, sync_rate_limit_buckets
from taskq.ratelimit.reservation import ConcurrencyReservation, SyncResult, sync_slots
from taskq.ratelimit.sliding_window import SlidingWindow
from taskq.ratelimit.token_bucket import TokenBucket

__all__ = [
    "AcquiredResource",
    "ConcurrencyReservation",
    "KeyedReservationRef",
    "RateLimitBackend",
    "RateLimitDecision",
    "RateLimitHandle",
    "RateLimitRef",
    "RateLimitRegistry",
    "RateLimitState",
    "ReservationHandle",
    "ReservationRef",
    "SlidingWindow",
    "SyncResult",
    "TokenBucket",
    "get_redis_pool",
    "register_rate_limit_registry",
    "register_redis_pool",
    "registry",
    "sync_rate_limit_buckets",
    "sync_slots",
]
