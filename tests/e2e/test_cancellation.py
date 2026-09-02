"""Cancellation e2e — single-job cancel + bulk cancel_by_filter.

Single-job path:
- ``JobsClient.cancel`` → ``PostgresBackend.write_cancel_request``
  branches on the row's current status:
  - ``pending``/``scheduled`` → straight to ``status='cancelled'``
  - ``running`` → ``cancel_phase=1`` (cooperative), actor sees
    ``ctx.check_cancelled()`` at next stage boundary

Bulk path (``cancel_where``):
- pending/scheduled → terminal ``cancelled`` (set-based, single SQL)
- running → ``cancel_phase=1`` (cooperative, batched NOTIFY)
- empty filter rejected with ``EmptyFilterError``
- batch_id filter cancels all jobs in a batch

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq import JobFailed, JobFilter
from taskq._ids import new_uuid
from taskq.batch import EnqueueItem

from ._assertions import fetch_effects, wait_for_effects, wait_for_handle_status
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


def _long_report_payload(run_id: str) -> GenerateReportPayload:
    """4 stages x 2 s — slow enough to cancel deterministically mid-run."""
    return GenerateReportPayload(
        run_id=run_id,
        report_id=f"r-{run_id[:8]}",
        stages=4,
        stage_latency_ms=2000,
    )


async def test_cancel_long_running_job(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Running report cancelled mid-flight → terminal ``cancelled``, partial effects.

    The cooperative path is phase-1: the row stays ``running`` until the actor
    observes ``cancel_event`` at a stage boundary. The row flips to ``running``
    at claim time — before the actor's first ``check_cancelled`` — and the
    cancel NOTIFY wakes the worker's heartbeat immediately, so a cancel issued
    on ``running`` alone can legitimately land before stage 1 commits. The
    test therefore waits for the stage-1 effect (ground truth) before
    cancelling: at least the stage-1 effect commits by construction, and fewer
    than 4 stages + no ``done`` row prove the pipeline stopped early.
    """
    handle = await e2e_client.enqueue(generate_report, _long_report_payload(run_id))
    await wait_for_handle_status(handle, "running", timeout=30)
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=1,
        timeout=30,
    )

    result = await handle.cancel(reason="e2e")

    assert result.cancellation_initiated is True
    assert result.previous_status == "running"
    assert result.new_status == "running"

    await wait_for_handle_status(handle, "cancelled", timeout=30)
    with pytest.raises(JobFailed) as exc_info:
        await handle.wait(timeout=5)
    assert exc_info.value.row.status == "cancelled"

    stage_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="stage")
    assert 1 <= len(stage_rows) < 4
    done_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="done")
    assert done_rows == []


async def test_cancel_before_dispatch_is_clean(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Pre-dispatch cancel lands straight in ``cancelled`` with zero effects.

    ``scheduled_at`` 60 s out makes "before it starts running" deterministic:
    the worker cannot dispatch the job inside the test window, and the cancel
    hits the ``cancel_pending_scheduled`` path (which covers both ``pending``
    and ``scheduled``) instead of racing the worker's dispatch loop.
    """
    handle = await e2e_client.enqueue(
        generate_report,
        _long_report_payload(run_id),
        scheduled_at=datetime.now(UTC) + timedelta(seconds=60),
    )
    assert await handle.status() == "scheduled"

    result = await handle.cancel(reason="e2e-pre-dispatch")

    assert result.cancellation_initiated is True
    assert result.previous_status == "scheduled"
    assert result.new_status == "cancelled"
    assert await handle.status() == "cancelled"

    with pytest.raises(JobFailed) as exc_info:
        await handle.wait(timeout=5)
    assert exc_info.value.row.status == "cancelled"

    # The job never dispatched, so no actor code ran: no effects of any kind.
    assert await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id) == []


# ── Bulk cancel (cancel_where) ─────────────────────────────────────────


async def test_cancel_where_pending_jobs_by_tag(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where cancels all scheduled jobs matching a tag filter in one
    set-based write. Untagged jobs are unaffected."""
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


async def test_cancel_where_running_cooperative(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """cancel_where on a running job sets cooperative cancel (cancel_phase=1).
    The actor observes the cancel signal at the next stage boundary and
    reaches terminal 'cancelled' via the normal cooperative path."""
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
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="stage",
        min_count=1,
        timeout=30,
    )

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
    """cancel_where with batch_id cancels all jobs in a batch."""
    future = datetime.now(UTC) + timedelta(seconds=120)
    bid = new_uuid()

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
