"""Loop-lag watchdog e2e — a sync-blocking actor trips detector 4.

The ``loop_blocker_job`` actor calls ``time.sleep`` inside an async body,
blocking the worker's entire event loop. Heartbeats stop (stale only
past the 10s floor), so the loop-lag watchdog (budget 5s, startup grace
2s in this module's env) is the first detector to trip: the container
must exit with the watchdog's non-zero code AND the dump marker in its
log — pinning the detector, not any crash.

A dedicated worker container carries the tightened watchdog knobs so the
trip lands in seconds; no other module enqueues the blocker actor.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from taskq.testing._shared_containers import creator_labels
from taskq.worker._watchdog import EXIT_WATCHDOG

from ._assertions import wait_for_worker_ready
from .conftest import E2EWorker, _container_logs, _stop_container

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(300)]


# ── Module-local clean_e2e_state override ─────────────────────────────────
# The first test intentionally orphans a 'running' job by killing its
# worker mid-block; the shared idle gate cannot see it drain before the
# replacement worker starts. Tolerate the leftover row; the DELETE pass
# below resets state regardless.


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    if not {"e2e_client", "e2e_pg_pool", "e2e_schema"}.intersection(request.fixturenames):
        yield
        return

    import contextlib as _cl

    from .conftest import _DELETE_ORDER, _flushdb

    e2e_schema: E2ESchema = request.getfixturevalue("e2e_schema")
    e2e_pg_pool: asyncpg.Pool = request.getfixturevalue("e2e_pg_pool")
    e2e_dragonfly = request.getfixturevalue("e2e_dragonfly")
    schema = e2e_schema.schema_name

    from ._assertions import poll_until

    async def _no_running_jobs() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE status = $1',
            "running",
        )
        return count == 0

    with _cl.suppress(TimeoutError):
        await poll_until(
            _no_running_jobs,
            timeout=10.0,
            description=f"idle gate: zero running jobs in {schema}.jobs",
        )

    async with e2e_pg_pool.acquire() as conn:
        for table in _DELETE_ORDER:
            await conn.execute(f'DELETE FROM "{schema}"."{table}"')

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}")
    yield


@pytest_asyncio.fixture
async def blocker_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
) -> AsyncIterator[E2EWorker]:
    """Dedicated worker container with a fast loop-lag watchdog."""
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    container.with_network(e2e_network).with_network_aliases(
        f"worker-blocker-{e2e_schema.schema_name}"
    )
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)
    # Fast trip: detector 4 must win before the 10s staleness floor.
    # Re-enable the watchdog (the conftest fleet runs with it off) and size
    # the lease to hold the fast budget: 5.0 + 0.5 < 8.0 satisfies the
    # lag-lease invariant, and 5.0 > the 1.0s default check interval keeps
    # the detector clear of its own sampling cadence.
    container.with_env("TASKQ_WATCHDOG_ENABLED", "true")
    container.with_env("TASKQ_LOCK_LEASE", "8.0")
    container.with_env("TASKQ_WATCHDOG_LOOP_LAG_BUDGET", "5.0")
    container.with_env("TASKQ_WATCHDOG_LOOP_LAG_STARTUP_GRACE", "2.0")

    await asyncio.to_thread(container.start)
    try:
        try:
            await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)
        except TimeoutError:
            logs = _container_logs(container)
            msg = f"loop-blocker e2e worker failed readiness gate\n{logs}"
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


async def test_sync_blocking_actor_trips_loop_lag_watchdog(
    e2e_client: TaskQ,
    blocker_worker: E2EWorker,
    run_id: str,
) -> None:
    """A sync-blocking async actor blocks the loop; the watchdog trips with
    its own exit code and leaves the dump marker in the container log."""
    from .actors import LoopBlockerPayload, loop_blocker_job

    await e2e_client.enqueue(loop_blocker_job, LoopBlockerPayload(run_id=run_id))

    wrapped = blocker_worker.container.get_wrapped_container()

    async def _exited() -> bool:
        await asyncio.to_thread(wrapped.reload)
        return str(wrapped.status) == "exited"

    # Beat lands ~2s (grace) + lag budget 5s + poll cadence; generous bound
    # for Docker starvation.
    from ._assertions import poll_until

    await poll_until(
        _exited,
        timeout=90.0,
        description="blocker worker to exit via the loop-lag watchdog",
    )

    await asyncio.to_thread(wrapped.reload)
    exit_code = wrapped.attrs["State"]["ExitCode"]
    logs = _container_logs(blocker_worker.container)
    assert exit_code == EXIT_WATCHDOG, (
        f"expected watchdog exit code {EXIT_WATCHDOG}, got {exit_code}\n{logs}"
    )
    assert "watchdog trip: event loop blocked" in logs, (
        f"loop-lag dump marker missing from worker log\n{logs}"
    )


async def test_watchdog_kill_orphan_is_reclaimed_and_fleet_recovers(
    e2e_client: TaskQ,
    blocker_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    run_id: str,
) -> None:
    """The restart half of the watchdog contract: after a trip, the
    orphaned job is reclaimed by the leader sweep on lock-lease expiry
    (not left 'running' forever), and a replacement worker keeps the fleet
    functional. The poison job itself is cancelled before it can re-block
    a worker — the assertion is the reclaim, not its completion."""
    from testcontainers.core.container import DockerContainer

    from ._assertions import fetch_effects, poll_until
    from .actors import (
        LoopBlockerPayload,
        WelcomeEmailPayload,
        loop_blocker_job,
        send_welcome_email,
    )

    handle = await e2e_client.enqueue(
        loop_blocker_job, LoopBlockerPayload(run_id=run_id, block_seconds=600.0)
    )

    wrapped = blocker_worker.container.get_wrapped_container()

    async def _exited() -> bool:
        await asyncio.to_thread(wrapped.reload)
        return str(wrapped.status) == "exited"

    from ._assertions import poll_until as _poll

    await _poll(
        _exited,
        timeout=90.0,
        description="blocker worker to exit via the loop-lag watchdog",
    )

    # The row is orphaned 'running'; the lease (8s) expires. A replacement
    # worker's leader sweep must reclaim it within the recovery window.
    replacement = DockerContainer(image=e2e_worker_image.tag)
    replacement.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    replacement.with_network(e2e_network).with_network_aliases(
        f"worker-repl-blocker-{e2e_schema.schema_name}"
    )
    for key, value in e2e_schema.worker_env.items():
        replacement.with_env(key, value)
    # The replacement is the only live worker, and this test's tail needs
    # the fleet to self-heal: a redispatched poison job re-blocks it, and
    # without the lag watchdog that block is permanent — the reclaim flips
    # the row to pending for less than a poll interval and the fresh-work
    # assertion then starves. Run it under the same coherent watchdog knobs
    # as the blocker (re-enabled; the conftest fleet runs with it off), so
    # a re-blocked replacement dies, the job is re-orphaned and reclaimed
    # again, and the cycle continues until the cancel lands.
    replacement.with_env("TASKQ_WATCHDOG_ENABLED", "true")
    replacement.with_env("TASKQ_LOCK_LEASE", "8.0")
    replacement.with_env("TASKQ_WATCHDOG_LOOP_LAG_BUDGET", "5.0")
    await asyncio.to_thread(replacement.start)
    try:
        await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)

        schema = e2e_schema.schema_name

        async def _reclaimed() -> bool:
            status = await e2e_pg_pool.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',
                handle.job_id,
            )
            return status != "running"

        await poll_until(
            _reclaimed,
            timeout=60.0,
            description="orphaned job reclaimed after watchdog kill (lease expiry sweep)",
        )

        # Terminal-ize the poison job before it can re-block a worker, then
        # prove the fleet still completes real work.
        cancel_result = await handle.cancel()
        assert cancel_result.cancellation_initiated

        fresh = await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(run_id=run_id, user_id="u-rec", email="r@example.com"),
        )
        await fresh.wait(timeout=60)
        effects = await fetch_effects(e2e_pg_pool, schema, run_id, kind="send")
        assert len(effects) == 1, (
            f"replacement fleet should process new work after the trip: {effects}"
        )
    finally:
        await asyncio.to_thread(_stop_container, replacement)
