"""Result TTL expiry e2e — result cleared after TTL, job status preserved.

Scenario:
actor with ``result_ttl=2 s``; result retrievable immediately after success;
after the TTL elapses and the leader's ``sweep_expired_results`` sweep runs,
``jobs.result`` is NULL while ``jobs.status`` remains ``succeeded``.

Ground truth: ``JobHandle.refresh()`` (``JobRow.result``) and a direct SQL
read of ``jobs.status`` + ``jobs.result``.

Sweep cadence note: ``_sweep_loop`` in ``worker/_leader_sweeps.py`` sleeps
30 s between iterations, so the test polls for ``result IS NULL`` with
``poll_until`` (timeout 60 s) instead of a fixed sleep — the sweep that
clears the result is a background leader task whose exact timing depends on
where in the 30 s cycle the result expired.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ._assertions import fetch_effects, poll_until
from .actors import QuickResultPayload, QuickResultResult, quick_result

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_result_cleared_after_ttl(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """``result_ttl=2 s`` → result present immediately, NULL after sweep.

    (a) Immediate read: after ``handle.wait()`` returns success,
    ``handle.refresh().result`` is non-NULL and validates to
    ``QuickResultResult``.

    (b) Post-TTL read: after the 2 s TTL elapses and the leader's
    ``sweep_expired_results`` sweep runs (polled with a 60 s deadline to
    accommodate the 2 s sweep cycle), ``handle.refresh().result`` is NULL.

    (c) Status preserved: a direct SQL read confirms ``jobs.status`` is still
    ``succeeded`` — the sweep clears only the result columns, not the
    terminal status.
    """
    handle = await e2e_client.enqueue(
        quick_result,
        QuickResultPayload(run_id=run_id, value="hello-ttl"),
    )

    result = await handle.wait(timeout=60)
    assert isinstance(result, QuickResultResult)
    assert result.value == "hello-ttl"

    # (a) Result is present immediately after success.
    row = await handle.refresh()
    assert row.status == "succeeded"
    assert row.result is not None

    effects = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="quick_result")
    assert len(effects) == 1
    assert effects[0]["job_id"] == handle.job_id

    # (b) Poll for the sweep to clear the result.  The sweep runs every 2 s
    # in e2e (TASKQ_SWEEP_INTERVAL=2); the 2 s TTL means the result expired
    # ~2 s after success, so the next sweep tick will NULL it out.
    async def _result_is_none() -> bool:
        refreshed = await handle.refresh()
        return refreshed.result is None

    await poll_until(
        _result_is_none,
        timeout=60.0,
        description=f"result TTL sweep to clear jobs.result for {handle.job_id}",
    )

    row = await handle.refresh()
    assert row.result is None

    # (c) Status is still "succeeded" — the sweep only clears result columns.
    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, result
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "succeeded"
    assert job["result"] is None
