"""E2E: actor deregistration lifecycle with a real worker.

Each test uses a **different actor** to avoid cross-test interference:
``e2e_worker`` is module-scoped and ``sync_actor_config`` runs only at
bootstrap, so once a test deregisters an actor's ``actor_config`` row,
later tests cannot enqueue to that same actor (the dispatch query
inner-joins ``actor_config`` — jobs would never be dispatched).

Actors used (all defined in ``tests/e2e/actors.py``):
- ``quick_result`` — 0.05 s sleep, simple payload/result.
- ``long_running_job`` — 30 s sleep. Used for the refusal-with-active-jobs test.
- ``short_lived_job`` — 0.5 s sleep. Used for the force+purge_queue test.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ._assertions import poll_until, wait_for_handle_status
from .actors import (
    LongRunningPayload,
    QuickResultPayload,
    ShortJobPayload,
    long_running_job,
    quick_result,
    short_lived_job,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def test_deregister_after_jobs_complete(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Deregister an actor after all its jobs are terminal."""
    schema = e2e_schema.schema_name
    actor_name = quick_result.name

    handle = await e2e_client.enqueue(quick_result, QuickResultPayload(run_id=run_id, value="test"))
    await handle.wait(timeout=60)

    ac_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor_name,
    )
    assert ac_count == 1

    result = await e2e_client.actors.deregister(actor_name)

    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == 1

    ac_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor_name,
    )
    assert ac_count == 0

    job_count = await e2e_pg_pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'succeeded'",
        actor_name,
    )
    assert job_count == 1


async def test_deregister_refuses_with_active_jobs(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Deregistration refuses when a job is running."""
    from taskq.exceptions import ActorHasActiveJobsError

    schema = e2e_schema.schema_name
    actor_name = long_running_job.name

    handle = await e2e_client.enqueue(long_running_job, LongRunningPayload(run_id=run_id))

    async def _is_running() -> bool:
        status = await e2e_pg_pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',
            handle.job_id,
        )
        return status == "running"

    await poll_until(_is_running, timeout=30.0, interval=0.5)

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await e2e_client.actors.deregister(actor_name)

    assert exc_info.value.actor == actor_name
    assert exc_info.value.active_count == 1
    assert "running" in exc_info.value.status_counts

    ac_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor_name,
    )
    assert ac_count == 1

    # Cleanup: cancel the job, wait for terminal, then force-deregister.
    # Do NOT use handle.wait() — it raises JobFailed for cancelled status.
    # long_running_job never calls ctx.check_cancelled(), so the cancel
    # lands only after the 30s sleep finishes — budget the full duration.
    await handle.cancel()
    await wait_for_handle_status(handle, "cancelled", timeout=60)

    result = await e2e_client.actors.deregister(actor_name, force=True)
    assert result.actor_config_deleted is True

    ac_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor_name,
    )
    assert ac_count == 0


async def test_deregister_force_with_purge_queue_after_completion(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """force=True + purge_queue=True deregister succeeds when all jobs are terminal.

    Uses short_lived_job (0.5s sleep). Enqueues a job, waits for completion,
    then force-deregisters with purge_queue. The force path cancels 0 jobs
    (none are pending) and the queue is purged if no other actor uses it.
    """
    schema = e2e_schema.schema_name
    actor_name = short_lived_job.name

    handle = await e2e_client.enqueue(
        short_lived_job, ShortJobPayload(run_id=run_id, label="force-test")
    )
    await handle.wait(timeout=60)

    result = await e2e_client.actors.deregister(actor_name, force=True, purge_queue=True)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 0
    assert result.terminal_jobs_remaining == 1
    # All e2e actors share queue="e2e" — purge_queue=True is a safe no-op
    # because the orphan guard correctly refuses to delete a shared queue.
    assert result.queue_purged is False

    ac_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".actor_config WHERE actor = $1',
        actor_name,
    )
    assert ac_count == 0
