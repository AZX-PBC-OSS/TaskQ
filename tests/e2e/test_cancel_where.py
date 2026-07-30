"""Bulk cancel by filter e2e — cancel_where against real Postgres + worker.

Scenarios:
- pending/scheduled jobs cancelled directly by tag filter (untagged survive)
- running job cooperative cancel via tag filter (total_affected >= 1)
- empty filter guardrail (EmptyFilterError)
- batch_id filter cancellation
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from taskq.backend._protocol import JobFilter
from taskq.batch import EnqueueItem

from ._assertions import wait_for_handle_status
from .actors import (
    GenerateReportPayload,
    ImportContactsChunkPayload,
    generate_report,
    import_contacts_chunk,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_cancel_where_pending_jobs_by_tag(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where cancels all pending jobs matching a tag filter."""
    tag = f"tenant-{run_id[:8]}"
    future = datetime.now(UTC) + timedelta(seconds=120)

    for i in range(5):
        await e2e_client.enqueue(
            generate_report,
            GenerateReportPayload(run_id=run_id, report_id=f"r-{i}"),
            scheduled_at=future,
            tags=[tag],
        )

    for i in range(2):
        await e2e_client.enqueue(
            generate_report,
            GenerateReportPayload(run_id=f"other-{run_id}", report_id=f"o-{i}"),
            scheduled_at=future,
        )

    result = await e2e_client.cancel_where(
        JobFilter(tags=(tag,)),
        reason="tenant offboarded",
    )

    assert result.cancelled_directly == 5
    assert result.cancel_requested == 0
    assert result.total_affected == 5

    tagged = await e2e_client.list(JobFilter(tags=(tag,), status="cancelled"))
    assert len(tagged.jobs) == 5

    untagged = await e2e_client.list(JobFilter(status="scheduled", queue="e2e"))
    assert len(untagged.jobs) == 2


async def test_cancel_where_running_jobs_cooperative(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where sets cooperative cancel for running jobs."""
    tag = f"run-{run_id[:8]}"

    handle = await e2e_client.enqueue(
        generate_report,
        GenerateReportPayload(
            run_id=run_id,
            report_id=f"r-{run_id[:8]}",
            stages=4,
            stage_latency_ms=2000,
        ),
        tags=[tag],
    )
    await wait_for_handle_status(handle, "running", timeout=30)

    result = await e2e_client.cancel_where(
        JobFilter(tags=(tag,), status="running"),
        reason="abort run",
    )

    assert result.total_affected >= 1, "cancel_where should affect the running job"
    assert result.cancelled_directly == 0

    await wait_for_handle_status(handle, "cancelled", timeout=30)


async def test_cancel_where_empty_filter_raises(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
) -> None:
    """Empty filter is rejected even in e2e."""
    from taskq.exceptions import EmptyFilterError

    with pytest.raises(EmptyFilterError):
        await e2e_client.cancel_where(JobFilter(), reason="oops")


async def test_cancel_where_batch_id_filter(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where works with batch_id filter."""
    future = datetime.now(UTC) + timedelta(seconds=120)
    bid = uuid4()

    items = [
        EnqueueItem(
            actor_ref=import_contacts_chunk,
            payload=ImportContactsChunkPayload(
                run_id=run_id,
                upload_id=str(bid),
                chunk_id=i,
                start_row=i * 100,
                end_row=(i + 1) * 100,
            ),
            scheduled_at=future,
        )
        for i in range(5)
    ]
    await e2e_client.enqueue_batch(items, batch_id=bid)

    result = await e2e_client.cancel_where(
        JobFilter(batch_id=bid),
        reason="batch abort",
    )

    assert result.cancelled_directly == 5
