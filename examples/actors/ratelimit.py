"""Rate-limited and concurrency-reserved actors.

These actors demonstrate TaskQ's rate-limiting and concurrency primitives:
Redis-backed sliding window and token bucket, in-memory sliding window,
and PG-backed concurrency reservation.

Rate-limit primitives are registered on the module-level ``registry``
singleton at import time so the worker picks them up before dispatch
begins.
"""

import asyncio
from datetime import timedelta

from pydantic import BaseModel

from taskq import actor
from taskq.ratelimit import ConcurrencyReservation, SlidingWindow, TokenBucket, registry


class EmptyPayload(BaseModel):
    pass


registry.register(
    SlidingWindow(
        name="example_window",
        limit=3,
        window=timedelta(seconds=15),
        backend="redis",
    )
)

registry.register(
    TokenBucket(
        name="example_token",
        capacity=3,
        refill_per_second=1.0,
        backend="redis",
    )
)

registry.register(
    SlidingWindow(
        name="example_inmemory",
        limit=2,
        window=timedelta(seconds=10),
        backend="memory",
    )
)

registry.register(
    ConcurrencyReservation(
        name="example_concurrency",
        slots=2,
        lease=timedelta(seconds=30),
    )
)


@actor(name="window_rate_limited", queue="examples", rate_limits=["example_window"])
async def window_rate_limited(payload: EmptyPayload) -> None:
    """Sleeps 1s — gated by a Redis-backed sliding window (3 per 15s)."""
    await asyncio.sleep(1)


@actor(name="token_rate_limited", queue="examples", rate_limits=["example_token"])
async def token_rate_limited(payload: EmptyPayload) -> None:
    """Sleeps 1s — gated by a Redis-backed token bucket (cap 3, refill 1/s)."""
    await asyncio.sleep(1)


@actor(name="inmemory_rate_limited", queue="examples", rate_limits=["example_inmemory"])
async def inmemory_rate_limited(payload: EmptyPayload) -> None:
    """Sleeps 1s — gated by an in-memory sliding window (2 per 10s, per worker)."""
    await asyncio.sleep(1)


@actor(name="reserved", queue="examples", reservations=["example_concurrency"])
async def reserved(payload: EmptyPayload) -> None:
    """Sleeps 3s — gated by a PG-backed concurrency reservation (2 slots)."""
    await asyncio.sleep(3)
