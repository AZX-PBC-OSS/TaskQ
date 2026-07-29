"""SIGUSR2 task dump e2e — sending SIGUSR2 to a running worker with
in-flight jobs produces a task-stack dump in the container logs.

The worker's signal handler (``shutdown.py:350-359``) registers a
SIGUSR2 handler that calls ``dump_task_stacks("sigusr2", detector="sigusr2")``
(``_watchdog.py:129-170``). The dump emits one structured log record per
live asyncio task plus a raw stderr marker line:

    === task dump (sigusr2/sigusr2): N live task(s) ===

The test enqueues a ``slow_deliver_webhook`` (3 s sleep), waits for the
``started`` effect (job is in-flight), sends SIGUSR2 via the Docker API,
and verifies the dump marker appears in the container's stdout/stderr
logs. The worker is NOT killed — SIGUSR2 is a diagnostic signal, not a
shutdown signal.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from ._assertions import wait_for_effects
from .actors import SlowDeliverPayload, slow_deliver_webhook
from .conftest import E2EWorker, _container_logs

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(300)]


async def test_sigusr2_task_dump(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """SIGUSR2 to a running worker with in-flight jobs produces a task
    dump in the container logs.

    (a) Enqueue a ``slow_deliver_webhook`` job (3 s sleep) and wait for
    the ``started`` effect — the job is now in-flight and the worker has
    live asyncio tasks.

    (b) Send SIGUSR2 via the Docker API (``container.kill(signal="USR2")``).
    The signal handler calls ``dump_task_stacks`` which logs the dump
    marker and one record per live task to stderr.

    (c) Poll the container logs until the marker
    ``"=== task dump (sigusr2/sigusr2)"`` appears. The worker is NOT
    killed — SIGUSR2 is a diagnostic signal, not a shutdown signal.

    (d) Verify the worker is still running after the dump, and the job
    eventually completes (proving the dump did not interfere with
    execution).
    """
    handle = await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="ep-dump"),
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    wrapped = e2e_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="USR2")

    from ._assertions import poll_until

    async def _dump_visible() -> bool:
        logs = _container_logs(e2e_worker.container)
        return "task dump (sigusr2" in logs

    await poll_until(
        _dump_visible,
        timeout=15.0,
        description="SIGUSR2 task dump marker in worker container logs",
    )

    # The worker should still be running — SIGUSR2 is diagnostic, not fatal.
    await asyncio.to_thread(wrapped.reload)
    assert str(wrapped.status) == "running", (
        "worker should still be running after SIGUSR2 (diagnostic signal, not shutdown)"
    )

    # The in-flight job should complete normally — the dump did not
    # interfere with execution.
    await handle.wait(timeout=60)
