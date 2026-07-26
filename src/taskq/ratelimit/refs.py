"""Typed references to rate-limit and reservation primitives.

``RateLimitRef`` and ``ReservationRef`` are optional typed helpers for callers
that need structured metadata.  The ``@actor`` decorator stores plain
``list[str]`` name lists; resolution against the registry happens at dispatch
time, not at decoration time.

``KeyedReservationRef`` is the dynamic counterpart: instead of a single
fixed name, it carries a ``key_fn`` that derives a concrete reservation
name per job from the validated payload. This is for session/tenant-scoped
concurrency caps layered on top of a static global cap — e.g. an actor
declares ``reservations=["geocode-global", KeyedReservationRef(base_name="geocode-session", key_fn=lambda p: p["session_id"], slots=3, lease=timedelta(minutes=5))]``
to cap total concurrent geocode calls globally *and* per import session,
with each session's cap materializing as its own
:class:`~taskq.ratelimit.reservation.ConcurrencyReservation` on first use.

``KeyedRateLimitRef`` mirrors ``KeyedReservationRef`` for token buckets:
instead of a single fixed rate-limit name, it derives a per-key
:class:`~taskq.ratelimit.token_bucket.TokenBucket` from the payload — e.g.
an actor declares ``rate_limits=[KeyedRateLimitRef(base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0)]``
to give each tenant its own independent token budget, with each tenant's
bucket materializing on first use.
"""

from collections.abc import Callable
from datetime import timedelta

from pydantic import BaseModel, ConfigDict, field_validator

from taskq.backend._protocol import RateLimitBackend

__all__ = ["KeyedRateLimitRef", "KeyedReservationRef", "RateLimitRef", "ReservationRef"]


class RateLimitRef(BaseModel):
    """Typed reference to a rate-limit primitive by name."""

    name: str
    count: float = 1.0


class ReservationRef(BaseModel):
    """Typed reference to a concurrency reservation primitive by name."""

    name: str


class KeyedReservationRef(BaseModel):
    """Reference to a per-key concurrency reservation, derived from the payload.

    ``base_name`` namespaces the derived reservations (the concrete name
    registered for a given key is ``f"{base_name}:{key}"``) so distinct
    ``KeyedReservationRef`` declarations never collide. ``key_fn`` receives
    the actor's validated payload (as a ``dict[str, object]``, the same
    shape stored on the job row) and must return a non-empty string —
    typically a tenant, session, or account identifier already present on
    the payload.

    ``slots`` and ``lease`` configure every reservation derived from this
    ref identically (all keys share the same per-key cap and lease
    duration); use a separate ``KeyedReservationRef`` if different keys
    need different caps.

    Concrete per-key reservations are registered lazily on first
    acquisition and are not automatically removed — see
    :meth:`~taskq.ratelimit.registry.RateLimitRegistry.evict_idle_keyed_reservations`
    for bounding registry growth under high key cardinality.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_name: str
    key_fn: Callable[[dict[str, object]], str]
    slots: int
    lease: timedelta

    @field_validator("base_name")
    @classmethod
    def _validate_base_name(cls, v: str) -> str:
        if not v:
            raise ValueError("base_name must not be empty")
        return v

    @field_validator("slots")
    @classmethod
    def _validate_slots(cls, v: int) -> int:
        if v < 1:
            raise ValueError(f"slots must be >= 1, got {v}")
        return v

    @field_validator("lease")
    @classmethod
    def _validate_lease(cls, v: timedelta) -> timedelta:
        if v <= timedelta(0):
            raise ValueError(f"lease must be > 0, got {v!r}")
        return v


class KeyedRateLimitRef(BaseModel):
    """Reference to a per-key token bucket, derived from the payload.

    Mirrors :class:`KeyedReservationRef` but for rate limits: ``base_name``
    namespaces the derived buckets (concrete name is ``f"{base_name}:{key}"``),
    ``key_fn`` derives the key from the actor's validated payload, and
    ``capacity`` / ``refill_per_second`` configure every bucket derived
    from this ref identically (all keys share the same per-key budget).

    A consumer calling an external API with per-tenant rate limits would
    declare ``rate_limits=[KeyedRateLimitRef(base_name="api-per-tenant",
    key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0)]``
    to give each tenant its own independent token budget, with each
    tenant's bucket materializing on first use.

    **Concurrency caps vs. rate limits.** A concurrency limiter (how many
    jobs at once, e.g. :class:`KeyedReservationRef` /
    :class:`~taskq.ratelimit.reservation.ConcurrencyReservation`) and a
    rate limiter (how many per unit time, e.g.
    :class:`~taskq.ratelimit.token_bucket.TokenBucket`) solve different
    problems: N concurrent slots with fast responses can still burst well
    past a per-time-unit budget, so both may be needed together on the
    same actor.

    **Backend selection.** The ``backend`` field (default ``"redis"``)
    controls which storage backend the materialized
    :class:`~taskq.ratelimit.token_bucket.TokenBucket` uses, identical to
    the ``backend`` constructor parameter on a static ``TokenBucket``. In a
    deployment without Redis configured, set ``backend="postgres"`` or
    ``backend="memory"`` to avoid the Redis-required failure mode — a
    keyed bucket with ``backend="redis"`` but no ``redis_client`` raises
    ``RuntimeError`` on acquire (not caught by ``with_pg_fallback``, which
    only handles ``ConnectionError``/``TimeoutError``).

    **PG fallback inheritance.** ``_resolve_rate_limit_name`` constructs a
    plain :class:`~taskq.ratelimit.token_bucket.TokenBucket` (with the
    ``backend`` from this ref, default ``"redis"``) and calls its normal ``.acquire()``.
    The existing ``with_pg_fallback`` path in
    ``token_bucket._acquire_redis_wrapped`` is therefore inherited
    automatically — on Redis ``ConnectionError``/``TimeoutError``, the
    acquire falls back to the PG ``rate_limit_buckets`` table governed by
    ``settings.rate_limit_pg_fallback_enabled``. No second fallback
    mechanism is built or needed.

    **Dual growth bounds.** Per-key Redis memory is self-bounding because
    the token-bucket Lua script sets an ``EXPIRE`` TTL on each bucket's
    Redis hash (computed from ``capacity``/``refill_per_second``). The
    Python-process-local dict/registry growth is bounded separately by
    :meth:`~taskq.ratelimit.registry.RateLimitRegistry.evict_idle_keyed_rate_limits`,
    which evicts idle entries from the in-memory registry. These are two
    independent bounds — Redis TTL bounds Redis memory; registry eviction
    bounds Python memory.

    Concrete per-key :class:`~taskq.ratelimit.token_bucket.TokenBucket`
    instances are registered lazily on first acquisition and are not
    automatically removed — see
    :meth:`~taskq.ratelimit.registry.RateLimitRegistry.evict_idle_keyed_rate_limits`
    for bounding registry growth under high key cardinality.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_name: str
    key_fn: Callable[[dict[str, object]], str]
    capacity: float
    refill_per_second: float
    backend: RateLimitBackend = "redis"

    @field_validator("base_name")
    @classmethod
    def _validate_base_name(cls, v: str) -> str:
        if not v:
            raise ValueError("base_name must not be empty")
        return v

    @field_validator("capacity")
    @classmethod
    def _validate_capacity(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"capacity must be > 0, got {v}")
        return v

    @field_validator("refill_per_second")
    @classmethod
    def _validate_refill_per_second(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"refill_per_second must be >= 0, got {v}")
        return v
