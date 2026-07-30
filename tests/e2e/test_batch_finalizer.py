"""Batch finalizer e2e — finalizer snoozes until children complete, then runs.

Scenario:
5 chunk jobs + 1 finalizer enqueued in a single ``enqueue_batch`` call with
``finalizer=EnqueueItem(...)``.  The finalizer actor calls ``wait_for_batch``
which snoozes (raises ``Snooze``) until all children reach terminal status,
then records a ``finalized`` effect with the completion counts.

The finalizer is NOT counted as a child — ``wait_for_batch`` excludes the
finalizer job via the batch row's ``finalizer_job_id``.  So ``total == 5``
(children only), not 6 (children + finalizer).

Uses the standard ``e2e_worker`` fixture.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import EnqueueItem

from ._assertions import fetch_effects, wait_for_effects
from .actors import (
    FinalizerPayload,
    ImportContactsChunkPayload,
    batch_finalizer,
    import_contacts_chunk,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_NUM_CHILDREN = 5


async def test_finalizer_snoozes_then_runs(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """5 children + 1 finalizer → finalizer waits for children, records counts."""
    batch_id = uuid4()

    children = [
        EnqueueItem(
            actor_ref=import_contacts_chunk,
            payload=ImportContactsChunkPayload(
                run_id=run_id,
                upload_id=f"u-{run_id[:8]}",
                chunk_id=i,
                start_row=i * 100,
                end_row=(i + 1) * 100,
            ),
            metadata={"run_id": run_id},
        )
        for i in range(_NUM_CHILDREN)
    ]

    finalizer = EnqueueItem(
        actor_ref=batch_finalizer,
        payload=FinalizerPayload(
            run_id=run_id,
            batch_id=str(batch_id),
        ),
        metadata={"run_id": run_id},
    )

    batch = await e2e_client.enqueue_batch(
        children,
        batch_id=batch_id,
        finalizer=finalizer,
    )
    assert batch.size == _NUM_CHILDREN
    assert batch.finalizer_handle is not None

    await asyncio.gather(
        *(h.wait(timeout=60) for h in batch.job_handles),
        return_exceptions=True,
    )

    finalized_effects = await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="finalized",
        min_count=1,
        timeout=90.0,
    )

    assert len(finalized_effects) == 1
    detail = finalized_effects[0]["detail"]
    assert detail["total"] == _NUM_CHILDREN
    assert detail["succeeded"] == _NUM_CHILDREN
    assert detail["failed"] == 0

    chunk_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="chunk_done"
    )
    assert len(chunk_effects) == _NUM_CHILDREN
