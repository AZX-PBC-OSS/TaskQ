"""Batch empty-safety e2e — wait_for_batch raises on empty/wrong batches.

Tests:
1. Wrong batch_id (no jobs, no batches row) → raises ``EmptyBatchError``.
2. ``expect_at_least=5`` with only 3 jobs → raises ``EmptyBatchError``.

Uses ``wait_for_batch`` with ``snooze_via_exception=False`` (outside-actor
blocking mode) so the test process can call it directly.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq import EmptyBatchError, EnqueueItem, wait_for_batch

from ._assertions import fetch_effects, wait_for_effects
from .actors import ImportContactsChunkPayload, import_contacts_chunk

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_wrong_batch_id_raises_empty_error(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """A batch_id with no jobs and no batches row → EmptyBatchError."""
    fake_batch_id = uuid4()

    async with e2e_pg_pool.acquire() as conn:
        with pytest.raises(EmptyBatchError):
            await wait_for_batch(
                conn,
                fake_batch_id,
                schema=e2e_schema.schema_name,
                snooze_via_exception=False,
                snooze_interval=timedelta(seconds=1),
            )


async def test_expect_at_least_with_too_few_jobs(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """3 jobs enqueued, expect_at_least=5 → EmptyBatchError after completion."""
    batch_id = uuid4()

    items = [
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
        for i in range(3)
    ]

    await e2e_client.enqueue_batch(items, batch_id=batch_id)

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="chunk_done",
        min_count=3,
        timeout=60.0,
    )

    async with e2e_pg_pool.acquire() as conn:
        with pytest.raises(EmptyBatchError) as exc_info:
            await wait_for_batch(
                conn,
                batch_id,
                schema=e2e_schema.schema_name,
                snooze_via_exception=False,
                snooze_interval=timedelta(seconds=1),
                expect_at_least=5,
            )

    assert exc_info.value.expected == 5
    assert exc_info.value.actual == 3

    effects = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="chunk_done")
    assert len(effects) == 3
