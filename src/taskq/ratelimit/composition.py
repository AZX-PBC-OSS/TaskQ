"""Composition types for AND-composition of rate limits and reservations.

These are pure type definitions with no business logic.  They define the
contracts that the registry, consumer, and decorator
will implement against.

``AcquiredResource`` is the runtime-checkable Protocol that both
``ReservationHandle`` and ``RateLimitHandle`` satisfy structurally.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import UUID

from taskq.ratelimit.decision import RateLimitDecision

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.backend.clock import Clock
    from taskq.ratelimit.reservation import ConcurrencyReservation
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.ratelimit.token_bucket import TokenBucket
    from taskq.settings import WorkerSettings

__all__ = [
    "AcquiredResource",
    "RateLimitHandle",
    "ReservationHandle",
]


@runtime_checkable
class AcquiredResource(Protocol):
    """Protocol for a resource handle that can be released."""

    @property
    def name(self) -> str: ...

    async def release(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ReservationHandle:
    """Handle for a successfully acquired reservation slot.

    ``release()`` calls ``ConcurrencyReservation.release(slot_index, worker_id, pool)``
    which sets the slot row's ``job_id`` to ``NULL``.  Idempotent.
    """

    name: str
    reservation: "ConcurrencyReservation"
    slot_index: int
    job_id: UUID
    worker_id: UUID
    pool: "asyncpg.Pool | None"

    async def release(self) -> None:
        await self.reservation.release(self.slot_index, self.worker_id, self.pool)


@dataclass(slots=True)
class RateLimitHandle:
    """Handle for a successfully acquired rate-limit token.

    ``release()`` is a no-op when ``refund_on_release`` is ``False`` (post-actor
    path — token consumption is permanent).  When ``refund_on_release`` is
    ``True`` (rollback path), ``release()`` refunds ``count`` tokens via
    ``primitive.refund()``.
    """

    name: str
    primitive: "TokenBucket | SlidingWindow"
    decision: RateLimitDecision
    redis_client: "redis_async.Redis | None"
    pg_pool: "asyncpg.Pool | None"
    clock: "Clock | None"
    settings: "WorkerSettings | None" = field(default=None)
    count: float = 1.0
    refund_on_release: bool = True

    async def release(self) -> None:
        if not self.refund_on_release:
            return
        await self.primitive.refund(
            self.decision,
            count=self.count,
            redis_client=self.redis_client,
            pg_pool=self.pg_pool,
            clock=self.clock,
            settings=self.settings,
        )
