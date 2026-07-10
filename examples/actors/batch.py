"""Batch actors — fan-out-then-finalize with enqueue_batch and wait_for_batch.

These actors demonstrate:

- ``batch_counter``: enqueues N child counter jobs as a single batch
  (each carrying a shared ``batch_id`` in metadata), then enqueues a
  ``batch_finalizer`` that waits for all children to complete
  (fan-out-then-finalize pattern).
- ``batch_finalizer``: calls ``wait_for_batch`` which raises
  ``Snooze(snooze_interval)`` while any children are in-flight, then
  returns ``BatchCompletionStatus`` when all are terminal. The snooze
  loop is visible in the admin UI as repeated attempts in the job's
  attempt history.
"""

from uuid import UUID, uuid4

import asyncpg
from pydantic import BaseModel, Field

from examples.actors.basic import CounterPayload, counter
from taskq import JobContext, actor
from taskq.batch import EnqueueItem, wait_for_batch


class BatchCounterPayload(BaseModel):
    n: int = Field(default=5, ge=1, le=1000, description="Number of child jobs")
    steps: int = Field(default=5, ge=1, le=1000, description="Steps per child")


class BatchFinalizerPayload(BaseModel):
    batch_id: UUID
    expected: int


@actor(name="batch_counter", queue="examples")
async def batch_counter(payload: BatchCounterPayload, ctx: JobContext[BatchCounterPayload]) -> None:
    """Enqueues N counter jobs as a single batch, then a finalizer that waits for them."""
    batch_id = uuid4()
    items = [
        EnqueueItem(
            actor_ref=counter,
            payload=CounterPayload(n=payload.steps),
        )
        for _ in range(payload.n)
    ]
    # Pass batch_id explicitly so it matches the id given to batch_finalizer below —
    # otherwise enqueue_batch auto-generates its own id and wait_for_batch would
    # track nothing.
    await ctx.jobs.enqueue_batch(items, batch_id=batch_id)
    await ctx.jobs.enqueue(
        batch_finalizer,
        BatchFinalizerPayload(batch_id=batch_id, expected=payload.n),
        metadata={"batch_id": str(batch_id), "role": "finalizer"},
    )
    ctx.log.info("batch-enqueued", batch_id=str(batch_id), size=payload.n)


@actor(name="batch_finalizer", queue="examples")
async def batch_finalizer(
    payload: BatchFinalizerPayload,
    ctx: JobContext[BatchFinalizerPayload],
    db: asyncpg.Pool,
) -> None:
    """Waits for all children in the batch to complete, then logs the summary."""
    status = await wait_for_batch(db, payload.batch_id)
    ctx.log.info(
        "batch-complete",
        batch_id=str(payload.batch_id),
        succeeded=status.succeeded,
        failed=status.failed,
        total=status.total,
    )
