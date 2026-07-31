"""Batch discovery e2e — list_batches finds active and completed batches.

Scenario:
- Batch B: 2 long-running jobs (30s each) — stays active for test duration.
- Batch A: 5 fast jobs — completes during test.

Assertions:
- ``list_batches(active=True)`` → batch B present, batch A absent (while A
  is still running or before it completes).
- After batch A's jobs complete, ``list_batches(active=False)`` → batch A
  present.
- ``list_batches(active=True)`` → batch B still present, batch A absent.

Uses the standard ``e2e_worker`` fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import BatchFilter, EnqueueItem
from taskq.batch_policy import AbortBatchAfter

from ._assertions import fetch_effects, poll_until, wait_all_ignoring_failures, wait_for_effects
from .actors import (
    ImportContactsChunkPayload,
    LongRunningPayload,
    import_contacts_chunk,
    long_running_job,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_BATCH_A_SIZE = 5


async def test_list_batches_active_and_completed(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """list_batches finds active batch B and completed batch A."""
    run_id_b = f"{run_id}-b"
    run_id_a = f"{run_id}-a"

    batch_b_id = uuid4()
    batch_a_id = uuid4()

    batch_b_items = [
        EnqueueItem(
            actor_ref=long_running_job,
            payload=LongRunningPayload(run_id=run_id_b),
            metadata={"run_id": run_id_b},
        )
        for _ in range(2)
    ]

    # failure_policy with a high threshold ensures a batches row is
    # created (rows are only created when failure_policy or finalizer is
    # set) without actually aborting the batch.
    batch_b = await e2e_client.enqueue_batch(
        batch_b_items,
        batch_id=batch_b_id,
        failure_policy=AbortBatchAfter(consecutive_failures=999),
    )

    batch_a_items = [
        EnqueueItem(
            actor_ref=import_contacts_chunk,
            payload=ImportContactsChunkPayload(
                run_id=run_id_a,
                upload_id=f"u-{run_id_a[:8]}",
                chunk_id=i,
                start_row=i * 100,
                end_row=(i + 1) * 100,
            ),
            metadata={"run_id": run_id_a},
        )
        for i in range(_BATCH_A_SIZE)
    ]

    batch_a = await e2e_client.enqueue_batch(
        batch_a_items,
        batch_id=batch_a_id,
        failure_policy=AbortBatchAfter(consecutive_failures=999),
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id_a,
        kind="chunk_done",
        min_count=_BATCH_A_SIZE,
        timeout=60.0,
    )

    async def _batch_a_complete() -> bool:
        batches = await e2e_client.list_batches(BatchFilter(batch_id=batch_a_id, active=False))
        return any(b.batch_id == batch_a_id for b in batches)

    await poll_until(
        _batch_a_complete,
        timeout=60.0,
        description=f"batch A {batch_a_id} to appear in list_batches(active=False)",
    )

    active_batches = await e2e_client.list_batches(BatchFilter(queue="e2e", active=True))
    active_ids = {b.batch_id for b in active_batches}
    assert batch_b_id in active_ids, (
        f"batch B {batch_b_id} should be active, got active_ids={active_ids}"
    )
    assert batch_a_id not in active_ids, (
        f"batch A {batch_a_id} should NOT be active, got active_ids={active_ids}"
    )

    completed_batches = await e2e_client.list_batches(BatchFilter(queue="e2e", active=False))
    completed_ids = {b.batch_id for b in completed_batches}
    assert batch_a_id in completed_ids, (
        f"batch A {batch_a_id} should be in completed, got completed_ids={completed_ids}"
    )

    active_batches_after = await e2e_client.list_batches(BatchFilter(queue="e2e", active=True))
    active_ids_after = {b.batch_id for b in active_batches_after}
    assert batch_b_id in active_ids_after, (
        f"batch B {batch_b_id} should still be active, got active_ids_after={active_ids_after}"
    )
    assert batch_a_id not in active_ids_after, (
        f"batch A {batch_a_id} should NOT be active, got active_ids_after={active_ids_after}"
    )

    batch_a_summary = next(b for b in completed_batches if b.batch_id == batch_a_id)
    assert batch_a_summary.completion.succeeded == _BATCH_A_SIZE
    assert batch_a_summary.completion.is_complete

    # Assert batch B's live completion counts — the long-running jobs
    # should still be in-flight (not terminal), so pending > 0 and
    # is_complete is False.
    active_b_summary = next((b for b in active_batches if b.batch_id == batch_b_id), None)
    assert active_b_summary is not None, "batch B should be in active list"
    assert active_b_summary.completion.total == 2
    assert active_b_summary.completion.pending >= 1
    assert active_b_summary.completion.is_complete is False

    await wait_all_ignoring_failures(batch_a.job_handles, timeout=60)

    await wait_all_ignoring_failures(batch_b.job_handles, timeout=60)

    a_effects = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id_a, kind="chunk_done"
    )
    assert len(a_effects) == _BATCH_A_SIZE

    b_started = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id_b, kind="started")
    assert len(b_started) == 2
