"""Demo actor for the FastAPI example app — demonstrates ctx.progress() with TaskQ.stream()."""

import asyncio

from pydantic import BaseModel, ConfigDict

from taskq import JobContext, actor


class ItemPayload(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str = "widget"
    steps: int = 3


class ProcessResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    steps_completed: int


@actor(name="process_item", queue="examples")
async def process_item(payload: ItemPayload, ctx: JobContext[ItemPayload]) -> ProcessResult:
    """Processes an item in N steps, reporting progress at each stage."""
    for i in range(1, payload.steps + 1):
        ctx.check_cancelled()
        await ctx.progress(
            step=i,
            percent=round(i / payload.steps * 100, 1),
            detail=f"processing {payload.name}: step {i}/{payload.steps}",
        )
        await asyncio.sleep(0.5)

    return ProcessResult(name=payload.name, steps_completed=payload.steps)
