"""Tag demo — actors that demonstrate job tagging and tag-based filtering.

Tags are applied at enqueue time (``tq.enqueue(..., tags=["alpha", "beta"])``)
and stored on each job row. Callers can list jobs filtered by tag using
``JobFilter(tags=("alpha",))``.
"""

import asyncio
from datetime import timedelta

from pydantic import BaseModel

from taskq import JobContext, actor


class TaggedPayload(BaseModel):
    label: str = "demo"


class TaggedResult(BaseModel):
    label: str
    reversed: str


@actor(name="tagged_lower", queue="examples", result_ttl=timedelta(minutes=1))
async def tagged_lower(payload: TaggedPayload) -> TaggedResult:
    """A tagged actor — enqueue with tags=["alpha", "lower"] to find it later."""
    await asyncio.sleep(0.5)
    return TaggedResult(
        label=payload.label,
        reversed="".join(reversed(payload.label)),
    )


@actor(name="tagged_upper", queue="examples")
async def tagged_upper(payload: TaggedPayload, ctx: JobContext[TaggedPayload]) -> None:
    """A tagged actor — enqueue with tags=["alpha", "upper"] and watch progress."""
    for i, ch in enumerate(payload.label):
        ctx.check_cancelled()
        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / len(payload.label) * 100, 1),
            detail=f"processing char '{ch}'",
        )
        await asyncio.sleep(0.3)
