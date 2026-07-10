"""Chained actors — enqueueing sub-jobs via ctx.jobs.enqueue().

These actors demonstrate TaskQ's actor chaining pattern: one actor enqueues
another as part of its execution using ``ctx.jobs.enqueue()``.  The enqueue
happens within the actor body (transactionally, using the worker's connection
pool), so ``step_two`` only appears in the queue if ``step_one`` succeeds.
"""

import asyncio

from pydantic import BaseModel

from taskq import JobContext, actor
from taskq.batch import EnqueueItem


class PipelinePayload(BaseModel):
    text: str = "hello"


class StepTwoPayload(BaseModel):
    processed: str


@actor(name="step_two", queue="examples")
async def step_two(payload: StepTwoPayload) -> None:
    """Second stage of a two-step pipeline — enqueued by step_one on success."""
    await asyncio.sleep(0.5)


@actor(name="step_one", queue="examples")
async def step_one(payload: PipelinePayload, ctx: JobContext[PipelinePayload]) -> None:
    """First stage — transforms payload and chains step_two via ctx.jobs.enqueue()."""
    processed = payload.text.upper()
    await ctx.jobs.enqueue(step_two, StepTwoPayload(processed=processed))


class FanOutPayload(BaseModel):
    items: list[str] = ["a", "b", "c"]


@actor(name="fan_out", queue="examples")
async def fan_out(payload: FanOutPayload, ctx: JobContext[FanOutPayload]) -> None:
    """Enqueues one step_two job per item — demonstrates ctx.jobs.enqueue_batch()."""
    batch = [
        EnqueueItem(actor_ref=step_two, payload=StepTwoPayload(processed=item.upper()))
        for item in payload.items
    ]
    await ctx.jobs.enqueue_batch(batch)
