"""Rate-limit decision result returned by every backend.

Consumed by ``TokenBucket.acquire()``, the sliding-window primitive,
the concurrency reservation, and the unified AND-composition registry
.  The shape is frozen — callers read fields, never mutate them.
"""

from dataclasses import dataclass
from datetime import timedelta

from taskq.backend._protocol import RateLimitBackend

__all__ = ["RateLimitDecision", "RateLimitState"]


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: float
    retry_after: timedelta | None
    bucket_name: str
    backend: RateLimitBackend
    request_id: str | None = None
    previous_state: dict[str, object] | None = None


@dataclass(frozen=True, slots=True)
class RateLimitState:
    """Read-only snapshot of a rate-limit bucket's current state.

    Returned by ``TokenBucket.peek()``, ``SlidingWindow.peek()``,
    and ``ConcurrencyReservation.peek()``.  Fields are backend-agnostic:
    TB backends populate ``tokens_remaining`` and ``capacity``; SW backends
    populate ``remaining``, ``limit``, ``window``, and ``style``.
    """

    bucket_name: str
    backend: RateLimitBackend
    is_exhausted: bool
    tokens_remaining: float = 0.0
    remaining: float = 0.0
    retry_after: timedelta | None = None
    capacity: float | None = None
    limit: int | None = None
    window: timedelta | None = None
    style: str | None = None
    refill_per_second: float | None = None
