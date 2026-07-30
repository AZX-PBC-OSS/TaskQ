"""Cancellation e2e — cooperative cancel mid-run + clean pre-dispatch cancel.

Scenario:
long report → ``wait_for_handle_status(handle, "running")`` →
``handle.cancel()`` → terminal ``cancelled``; effects show only early stages.

Real cancel semantics, verified against the library (not guessed):

- ``JobsClient.cancel`` → ``PostgresBackend.write_cancel_request``
  (backend/postgres.py) branches on the row's current status
  (backend/_sql_templates.py):

  - ``pending``/``scheduled`` → ``cancel_pending_scheduled`` UPDATEs the row
    straight to ``status='cancelled'``. The cancel API does NOT reject
    pre-dispatch cancels: ``CancelResult.cancellation_initiated`` is True,
    ``new_status`` is ``'cancelled'``, and the worker never dispatches the
    job — no actor code runs, so no effects exist.
  - ``running`` → ``cancel_running`` sets ``cancel_phase=1``; the worker's
    heartbeat-driven CancelController sets the in-process ``cancel_event``,
    the actor's ``ctx.check_cancelled()`` raises ``asyncio.CancelledError``
    at the next stage boundary, and the consumer writes ``mark_cancelled``.

- ``handle.wait()`` on any non-success terminal status raises ``JobFailed``
  carrying the row (``client/_handle.py._extract_result``).

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq import JobFailed

from ._assertions import fetch_effects, wait_for_effects, wait_for_handle_status
from .actors import GenerateReportPayload, generate_report

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
