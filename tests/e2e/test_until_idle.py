"""E2E tests for --until-idle worker drain mode.

Verifies that a worker started with TASKQ_UNTIL_IDLE=true:
1. Processes all enqueued jobs and exits 0 when all succeed
2. Exits 3 when some jobs fail permanently
3. Exits 4 when max-runtime is exceeded
4. Waits for and processes scheduled jobs before exiting

Each test enqueues jobs BEFORE starting the worker container, then
polls the container's exit code — the worker must stop on its own
(no SIGTERM needed).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from taskq import JobFailed
from taskq._ids import new_uuid

from ._assertions import poll_until
from .actors import (
    SlowDeliverPayload,
    SyncUserProfilePayload,
    WelcomeEmailPayload,
    send_welcome_email,
    slow_deliver_webhook,
    sync_user_profile,
)
from .conftest import _DELETE_ORDER, _flushdb, _stop_container

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-stopped until-idle workers.

    The until-idle worker exits on its own — the conftest's crash check
    would raise on the next test's setup because the container is no
    longer running.
    """
    if not {"e2e_client", "e2e_pg_pool", "e2e_schema"}.intersection(request.fixturenames):
        yield
        return

    e2e_schema: E2ESchema = request.getfixturevalue("e2e_schema")
    e2e_pg_pool: asyncpg.Pool = request.getfixturevalue("e2e_pg_pool")
    e2e_dragonfly: E2EDragonfly = request.getfixturevalue("e2e_dragonfly")
    schema = e2e_schema.schema_name

    async def _no_running_jobs() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE status = $1',
            "running",
        )
        return count == 0

    with contextlib.suppress(TimeoutError):
        await poll_until(
            _no_running_jobs,
            timeout=30.0,
            description=f"idle gate: zero running jobs in {schema}.jobs",
        )

    async with e2e_pg_pool.acquire() as conn:
        for table in _DELETE_ORDER:
            await conn.execute(f'DELETE FROM "{schema}"."{table}"')

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}")
    yield


async def _start_idle_worker(
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    *,
    extra_env: dict[str, str] | None = None,
) -> DockerContainer:
    """Start a worker container with until-idle mode enabled."""
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_network(e2e_network).with_network_aliases(
        f"worker-idle-{e2e_schema.schema_name}-{new_uuid().hex[:6]}"
    )
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)
    container.with_env("TASKQ_UNTIL_IDLE", "true")
    container.with_env("TASKQ_IDLE_SETTLE_WINDOW", "1.0")
    container.with_env("TASKQ_IDLE_POLL_INTERVAL", "0.5")
    for key, value in (extra_env or {}).items():
        container.with_env(key, value)
    await asyncio.to_thread(container.start)
    return container


async def _wait_for_container_exit(
    container: DockerContainer,
    timeout: float = 60.0,  # noqa: ASYNC109  # Why: polling deadline, not asyncio.timeout wrap
) -> int:
    """Poll container status until it exits, then return the exit code.

    Calls wrapped.reload() before reading status/attrs — Docker attrs
    are cached at fetch time and must be explicitly refreshed.
    """

    async def _container_exited() -> bool:
        wrapped = container.get_wrapped_container()
        wrapped.reload()
        return wrapped.status == "exited"

    await poll_until(
        _container_exited,
        timeout=timeout,
        description="worker container exits after drain",
    )

    wrapped = container.get_wrapped_container()
    wrapped.reload()
    return int(wrapped.attrs["State"]["ExitCode"])


async def test_until_idle_drains_and_exits_zero(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    e2e_pg_pool: asyncpg.Pool,
    run_id: str,
) -> None:
    """Worker with --until-idle processes all jobs and exits 0."""
    handles = []
    for i in range(3):
        handle = await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(run_id=run_id, user_id=f"u-{i}", email=f"u{i}@example.com"),
        )
        handles.append(handle)

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        for handle in handles:
            await handle.wait(timeout=60)

        exit_code = await _wait_for_container_exit(container, timeout=60.0)
        assert exit_code == 0, f"expected exit 0, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_exits_nonzero_on_failures(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    e2e_pg_pool: asyncpg.Pool,
    run_id: str,
) -> None:
    """Worker with --until-idle exits 3 when a job reaches 'failed'.

    Uses sync_user_profile with fail_kind='permanent': PermanentSyncError
    is in that actor's non_retryable_exceptions, so the first attempt
    moves the job straight to terminal 'failed' — dispatch_one_job
    returns 'failed', drain_failures increments, exit code becomes 3.
    """
    good = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="ok", email="ok@example.com"),
    )
    bad = await e2e_client.enqueue(
        sync_user_profile,
        SyncUserProfilePayload(run_id=run_id, user_id="bad", fail_times=1, fail_kind="permanent"),
    )

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        await good.wait(timeout=60)
        with pytest.raises(JobFailed):
            await bad.wait(timeout=60)
        exit_code = await _wait_for_container_exit(container, timeout=60.0)
        assert exit_code == 3, f"expected exit 3, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_timeout_exit_4(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    e2e_pg_pool: asyncpg.Pool,
    run_id: str,
) -> None:
    """Worker with --until-idle --idle-max-runtime exits 4 when timeout hits.

    Enqueues a long-running job (slow_deliver_webhook, sleeps 3s), starts
    a worker with TASKQ_IDLE_MAX_RUNTIME=2. The queue never reads as idle
    within the cap because the job is still active (pending or running —
    the specific state doesn't matter, both prevent idle). Exit code 4.
    """
    await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="slow"),
    )

    container = await _start_idle_worker(
        e2e_network,
        e2e_schema,
        e2e_worker_image,
        extra_env={"TASKQ_IDLE_MAX_RUNTIME": "2"},
    )
    try:
        exit_code = await _wait_for_container_exit(container, timeout=60.0)
        assert exit_code == 4, f"expected exit 4, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)


async def test_until_idle_scheduled_jobs_drain(
    e2e_client: TaskQ,
    e2e_schema: E2ESchema,
    e2e_worker_image: BuiltImage,
    e2e_network: Network,
    e2e_pg_pool: asyncpg.Pool,
    run_id: str,
) -> None:
    """Worker with --until-idle waits for future-scheduled jobs.

    Enqueues a job with scheduled_at 3s in the future, starts a worker
    with TASKQ_UNTIL_IDLE=true, and verifies:
    1. The worker waits (does not exit immediately — 'scheduled' counts
       as active under count_active_jobs)
    2. The scheduled job becomes due, is dispatched, and succeeds
    3. The worker then exits with code 0
    """
    scheduled_at = datetime.now(UTC) + timedelta(seconds=3)
    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="sched", email="sched@example.com"),
        scheduled_at=scheduled_at,
    )

    container = await _start_idle_worker(e2e_network, e2e_schema, e2e_worker_image)
    try:
        await handle.wait(timeout=60)
        exit_code = await _wait_for_container_exit(container, timeout=60.0)
        assert exit_code == 0, f"expected exit 0, got {exit_code}"
    finally:
        await asyncio.to_thread(_stop_container, container)
