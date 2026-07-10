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
"""

from collections.abc import Callable
from datetime import timedelta

from pydantic import BaseModel, ConfigDict, field_validator

__all__ = ["KeyedReservationRef", "RateLimitRef", "ReservationRef"]


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
