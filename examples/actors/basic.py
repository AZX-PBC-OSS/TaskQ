"""Basic actors — simple long-running and deferred jobs.

These actors demonstrate the simplest TaskQ patterns: a cancellable
long-running job (counter) and a deferred/scheduled job (deferred).
"""

import asyncio

from pydantic import BaseModel

from taskq import JobContext, actor


class CounterPayload(BaseModel):
    n: int = 10


class DeferredPayload(BaseModel):
    delay_seconds: int = 30


@actor(name="counter", queue="examples")
async def counter(payload: CounterPayload, ctx: JobContext[CounterPayload]) -> None:
    """Counts from 1 to N, reporting live progress each step."""
    for i in range(1, payload.n + 1):
        ctx.check_cancelled()
        await ctx.progress(
            step=i,
            percent=round(i / payload.n * 100, 1),
            detail=f"step {i} of {payload.n}",
        )
        await asyncio.sleep(1.0)


@actor(name="deferred", queue="examples")
async def deferred(payload: DeferredPayload) -> None:
    """Sleeps 1s then succeeds — scheduled_at is set by the trigger app."""
    await asyncio.sleep(1)
