"""Typed references to rate-limit and reservation primitives.

``RateLimitRef`` and ``ReservationRef`` are optional typed helpers for callers
that need structured metadata.  The ``@actor`` decorator stores plain
``list[str]`` name lists; resolution against the registry happens at dispatch
time, not at decoration time.

``KeyedReservationRef`` is the dynamic counterpart: instead of a single
fixed name, it carries a ``key_fn`` that derives a concrete reservation
name per job from the validated payload. This is for session/tenant-scoped
concurrency caps layered on top of a static global cap — e.g. an actor
declares ``reservations=["geocode-global", KeyedReservationRef.typed(MyPayload, base_name="geocode-session", key_fn=lambda p: p.session_id, slots=3, lease=timedelta(minutes=5))]``
to cap total concurrent geocode calls globally *and* per import session,
with each session's cap materializing as its own
:class:`~taskq.ratelimit.reservation.ConcurrencyReservation` on first use.

``KeyedRateLimitRef`` mirrors ``KeyedReservationRef`` for token buckets:
instead of a single fixed rate-limit name, it derives a per-key
:class:`~taskq.ratelimit.token_bucket.TokenBucket` from the payload — e.g.
an actor declares ``rate_limits=[KeyedRateLimitRef.typed(MyPayload, base_name="api-per-tenant", key_fn=lambda p: p.tenant_id, capacity=10, refill_per_second=1.0)]``
to give each tenant its own independent token budget, with each tenant's
bucket materializing on first use.
"""

from collections.abc import Callable
from datetime import timedelta

from pydantic import BaseModel, ConfigDict, field_validator

from taskq.backend._protocol import RateLimitBackend
from taskq.constants import (
    _KEYED_KEY_RE,  # pyright: ignore[reportPrivateUsage]
    _MAX_KEYED_KEY_LEN,  # pyright: ignore[reportPrivateUsage]
    base_name_collides_with_reserved_prefix,
)

__all__ = ["KeyedRateLimitRef", "KeyedReservationRef", "RateLimitRef", "ReservationRef"]


def _validate_keyed_base_name(v: str) -> str:
    """Shared ``base_name`` validation for both keyed ref types.

    The two keyed paths resolve concrete names identically
    (``f"{base_name}:{key}"``) and are deliberately kept on the same
    validation strictness — keep any change to one in the other by
    changing it HERE, once.

    Beyond charset/length, rejects a ``base_name`` whose derived concrete
    names would land inside the reserved queue-cap namespace
    (:data:`~taskq.constants.QUEUE_CONCURRENCY_PREFIX`): the character
    allowlist includes ``":"``, so such a ``base_name`` would pass the
    charset check, sail through DI validation (which skips keyed refs —
    their concrete names only exist at acquire time), and then make EVERY
    job on the actor die with ``ValueError`` from the ``register()``
    prefix guard — forever, since registration never succeeds and the
    failing path is retried per job. Failing here instead surfaces the
    misconfiguration at ref construction (startup/import time), like
    every other invalid ref.
    """
    if not v:
        raise ValueError("base_name must not be empty")
    if len(v) > _MAX_KEYED_KEY_LEN:
        raise ValueError(
            f"base_name must be at most {_MAX_KEYED_KEY_LEN} characters, got length {len(v)}"
        )
    if not _KEYED_KEY_RE.match(v):
        raise ValueError(
            f"base_name {v!r} contains characters outside the allowed set [A-Za-z0-9_\\-:.]"
        )
    if base_name_collides_with_reserved_prefix(v):
        raise ValueError(
            f"base_name {v!r} would derive concrete names inside the reserved "
            f"queue-cap namespace 'taskq:global:queue:' — choose a base_name "
            f"outside that prefix"
        )
    return v


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
    the actor's validated payload (as a :class:`~pydantic.BaseModel`
    instance — the same model stored on the job row after validation) and
    must return a non-empty string — typically a tenant, session, or
    account identifier already present on the payload.

    ``payload_type`` is the :class:`~pydantic.BaseModel` subclass that the
    payload will be validated against. Use :meth:`typed` for type-safe
    construction that binds ``key_fn`` to the same ``payload_type``:
    ``KeyedReservationRef.typed(MyPayload, base_name="geocode-session",
    key_fn=lambda p: p.session_id, slots=3, lease=timedelta(minutes=5))``.

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
    key_fn: Callable[[BaseModel], str]
    payload_type: type[BaseModel]
    slots: int
    lease: timedelta

    @classmethod
    def typed[P: BaseModel](
        cls,
        payload_type: type[P],
        *,
        base_name: str,
        key_fn: Callable[[P], str],
        slots: int,
        lease: timedelta,
    ) -> "KeyedReservationRef":
        """Type-safe constructor that binds ``key_fn`` to ``payload_type``.

        The ``key_fn`` parameter is typed as ``Callable[[P], str]`` where
        ``P`` is the provided ``payload_type`` — at static-analysis time
        the caller's lambda or function is checked against the concrete
        model's attributes (e.g. ``lambda p: p.session_id`` is verified
        against ``payload_type.session_id``).

        At runtime the registry passes the validated payload model to
        ``key_fn``, so the callable always receives an instance of
        ``payload_type`` (verified by ``isinstance`` in the registry
        before the call).
        """
        return cls(
            base_name=base_name,
            key_fn=key_fn,  # type: ignore[arg-type]  # Why: Callable[[P], str] is not assignable to Callable[[BaseModel], str] due to contravariance, but at runtime the registry only passes the validated model P (verified by isinstance check against ref.payload_type).
            payload_type=payload_type,
            slots=slots,
            lease=lease,
        )

    @field_validator("base_name")
    @classmethod
    def _validate_base_name(cls, v: str) -> str:
        return _validate_keyed_base_name(v)

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

    ``payload_type`` is the :class:`~pydantic.BaseModel` subclass that the
    payload will be validated against. Use :meth:`typed` for type-safe
    construction that binds ``key_fn`` to the same ``payload_type``:
    ``KeyedRateLimitRef.typed(MyPayload, base_name="api-per-tenant",
    key_fn=lambda p: p.tenant_id, capacity=10, refill_per_second=1.0)``.

    A consumer calling an external API with per-tenant rate limits would
    declare ``rate_limits=[KeyedRateLimitRef.typed(MyPayload,
    base_name="api-per-tenant", key_fn=lambda p: p.tenant_id, capacity=10,
    refill_per_second=1.0)]`` to give each tenant its own independent
    token budget, with each tenant's bucket materializing on first use.

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
    key_fn: Callable[[BaseModel], str]
    payload_type: type[BaseModel]
    capacity: float
    refill_per_second: float
    backend: RateLimitBackend = "redis"

    @classmethod
    def typed[P: BaseModel](
        cls,
        payload_type: type[P],
        *,
        base_name: str,
        key_fn: Callable[[P], str],
        capacity: float,
        refill_per_second: float,
        backend: RateLimitBackend = "redis",
    ) -> "KeyedRateLimitRef":
        """Type-safe constructor that binds ``key_fn`` to ``payload_type``.

        The ``key_fn`` parameter is typed as ``Callable[[P], str]`` where
        ``P`` is the provided ``payload_type`` — at static-analysis time
        the caller's lambda or function is checked against the concrete
        model's attributes (e.g. ``lambda p: p.tenant_id`` is verified
        against ``payload_type.tenant_id``).

        At runtime the registry passes the validated payload model to
        ``key_fn``, so the callable always receives an instance of
        ``payload_type`` (verified by ``isinstance`` in the registry
        before the call).
        """
        return cls(
            base_name=base_name,
            key_fn=key_fn,  # type: ignore[arg-type]  # Why: Callable[[P], str] is not assignable to Callable[[BaseModel], str] due to contravariance, but at runtime the registry only passes the validated model P (verified by isinstance check against ref.payload_type).
            payload_type=payload_type,
            capacity=capacity,
            refill_per_second=refill_per_second,
            backend=backend,
        )

    @field_validator("base_name")
    @classmethod
    def _validate_base_name(cls, v: str) -> str:
        return _validate_keyed_base_name(v)

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
