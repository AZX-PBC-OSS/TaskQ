"""Retry exhaustion e2e — job fails all attempts, lands in 'failed'.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
transient ``fail_times=3`` with ``max_attempts=3`` → every attempt fails →
job terminal status is ``failed``; ``handle.wait()`` raises ``JobFailed``;
exactly 3 ``fetch`` effects with attempts [1, 2, 3]; zero ``synced`` effects.

Ground truth: ``jobs.status``, ``jobs.attempt``, ``jobs.error_class``, and
``e2e_effects`` attempt numbers.

Attempt-counter semantics (verified against the library, not guessed):
``mark_retry`` increments the dispatch counter on each re-dispatch
(``backend/_dispatch_sql.py``); ``mark_failed`` leaves ``attempt`` untouched
(``backend/_sql_templates.py``); so a terminal ``failed`` row's ``attempt``
is exactly the number of dispatches the job went through.

Every test requests ``e2e_worker`` explicitly: the worker container fixture is
not autouse, so no worker (and no dispatch) exists unless a test pulls it in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from taskq import JobFailed

from ._assertions import fetch_effects
from .actors import SyncUserProfilePayload, sync_user_profile

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_retry_exhaustion_lands_failed(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Transient RuntimeError on all 3 attempts (max_attempts=3) → ``failed``.

    Each retry is a real re-dispatch across the container boundary
    (``mark_retry`` → ``scheduled`` → leader sweep → dispatch), so the
    effects stream shows exactly three ``fetch`` rows with attempts 1 → 2 → 3.
    No ``synced`` effect is ever written because the actor never reaches the
    post-fail code path.  ``handle.wait()`` raises ``JobFailed`` whose row
    exposes the terminal status and recorded error class.
    """
    handle = await e2e_client.enqueue(
        sync_user_profile,
        SyncUserProfilePayload(
            run_id=run_id,
            user_id="u-1",
            fail_times=3,
            fail_kind="transient",
        ),
    )

    with pytest.raises(JobFailed) as exc_info:
        await handle.wait(timeout=60)
    assert exc_info.value.row.status == "failed"
    assert exc_info.value.row.error_class == "RuntimeError"

    fetch_rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="fetch")
    assert [row["attempt"] for row in fetch_rows] == [1, 2, 3]
    assert {row["job_id"] for row in fetch_rows} == {handle.job_id}

    assert await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="synced") == []

    job = await e2e_pg_pool.fetchrow(
        f"""
        SELECT status, attempt, error_class
        FROM "{e2e_schema.schema_name}".jobs
        WHERE id = $1
        """,
        handle.job_id,
    )
    assert job is not None
    assert job["status"] == "failed"
    assert job["attempt"] == 3
    assert job["error_class"] == "RuntimeError"
