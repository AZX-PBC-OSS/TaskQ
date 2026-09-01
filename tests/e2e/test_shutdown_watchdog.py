"""Shutdown hard-deadline watchdog e2e — a shutdown that exceeds
``termination_grace_period`` trips detector 1 (``ShutdownWatchdog``).

The ``ShutdownWatchdog`` (``_watchdog.py:269-337``) parks on
``shutdown_event``, then counts down ``termination_grace_period`` (anchored
on ``shutdown_started_at``). If the shutdown is still incomplete when the
deadline elapses, it calls ``trip("shutdown-deadline", ...)`` which
force-exits the container with ``EXIT_WATCHDOG`` (code 2).

Triggering the watchdog requires the shutdown orchestration to exceed
``termination_grace_period``. Under normal conditions this is impossible:
the settings validator enforces
``cancellation_grace + cleanup_grace < termination_grace - 5.0``, and the
``close_conn_bounded`` in the orchestration is bounded at 5.0 s
(``CLOSE_TIMEOUT_SECS``), so the total is always under
``termination_grace``.

The watchdog is a safety net for *when things go beyond bounds*. This test
triggers it by stopping Postgres mid-run: with PG down,
``close_conn_bounded`` hangs for the full 5.0 s timeout. The orchestration
sets ``shutdown_event`` BEFORE the close (``shutdown.py:254``), so the
watchdog starts counting during the hang. With
``termination_grace = 5.01`` and zero grace periods, the watchdog trips
just after the 5.0 s close timeout.

This module follows the ``test_pg_restart_chaos.py`` pattern: a dedicated
PG container, schema, pool, client, and worker — none of the shared module
fixtures are requested, so the autouse ``clean_e2e_state`` guard
early-yields.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, NamedTuple
from uuid import uuid4

import pytest
import pytest_asyncio

from taskq.worker._watchdog import EXIT_WATCHDOG
from tests.conftest import free_host_port

from ._assertions import poll_until, wait_for_effects, wait_for_worker_ready
from .actors import LongRunningPayload, long_running_job
from .conftest import (
    _E2E_EFFECTS_DDL,
    _PG_DB,
    _PG_IMAGE,
    _PG_PASSWORD,
    _PG_USER,
    _SCHEMA_NAME_RE,
    E2EWorker,
    _container_logs,
    _flushdb,
    _next_redis_db,
    _probe_pg,
    _stop_container,
)

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2EDragonfly

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(300)]


class ChaosPg(NamedTuple):
    """Chaos PG endpoints and container."""

    container: DockerContainer
    host_dsn: str
    network_dsn: str


class ChaosSchema(NamedTuple):
    """Chaos isolation unit: PG schema + Dragonfly logical DB."""

    schema_name: str
    host_dsn: str
    redis_db: int


@pytest.fixture
def chaos_pg(e2e_network: Network) -> Iterator[ChaosPg]:
    """Function-scoped chaos PG container."""
    from testcontainers.community.postgres import PostgresContainer

    alias = f"pg-sdw-{uuid4().hex[:8]}"
    container = PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        command="-c max_connections=1000",
    )
    container.with_network(e2e_network).with_network_aliases(alias)
    container.with_bind_ports(5432, free_host_port())
    with container:
        host_dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        yield ChaosPg(
            container=container,
            host_dsn=host_dsn,
            network_dsn=f"postgresql://{_PG_USER}:{_PG_PASSWORD}@{alias}:5432/{_PG_DB}",
        )


@pytest_asyncio.fixture
async def chaos_schema(
    chaos_pg: ChaosPg,
    e2e_dragonfly: E2EDragonfly,
) -> AsyncIterator[ChaosSchema]:
    """Function-scoped PG schema + Dragonfly logical DB."""
    import asyncpg

    from taskq.migrate import apply_pending_locked

    schema = f"tsw_{uuid4().hex[:10]}"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        msg = f"derived chaos schema name {schema!r} is not a valid PG identifier"
        raise RuntimeError(msg)

    await _probe_pg(chaos_pg.host_dsn)

    conn = await asyncpg.connect(chaos_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    await apply_pending_locked(chaos_pg.host_dsn, schema=schema)

    conn = await asyncpg.connect(chaos_pg.host_dsn)
    try:
        await conn.execute(_E2E_EFFECTS_DDL.format(schema=schema))
    finally:
        await conn.close()

    redis_db = _next_redis_db()
    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{redis_db}")

    yield ChaosSchema(schema_name=schema, host_dsn=chaos_pg.host_dsn, redis_db=redis_db)

    conn = await asyncpg.connect(chaos_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def chaos_pool(chaos_schema: ChaosSchema) -> AsyncIterator[asyncpg.Pool]:
    """Function-scoped asyncpg pool on the chaos host DSN."""
    import asyncpg

    pool = await asyncpg.create_pool(chaos_schema.host_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


def _worker_env(
    chaos_pg: ChaosPg, e2e_dragonfly: E2EDragonfly, chaos_schema: ChaosSchema
) -> dict[str, str]:
    """Worker env with minimal grace periods and a tight watchdog deadline."""
    return {
        "TASKQ_PG_DSN": chaos_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{e2e_dragonfly.network_url}/{chaos_schema.redis_db}",
        "TASKQ_SCHEMA_NAME": chaos_schema.schema_name,
        "TASKQ_QUEUES": "e2e",
        "TASKQ_MIGRATE_ON_START": "false",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "5.01",
        "TASKQ_WATCHDOG_DUMP_INTERVAL": "0.5",
        "TASKQ_SWEEP_INTERVAL": "2.0",
        "TASKQ_QUEUE_DEPTH_INTERVAL": "2.0",
        "TASKQ_RESERVATION_SLOTS_INTERVAL": "2.0",
        "TASKQ_STRANDED_JOBS_INTERVAL": "2.0",
    }


async def _start_gated_worker(
    *,
    image_tag: str,
    network: Network,
    worker_env: dict[str, str],
    alias: str,
    pool: asyncpg.Pool,
    schema: str,
    label: str,
) -> DockerContainer:
    """Start a worker container and gate on a fresh heartbeat."""
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=image_tag)
    container.with_network(network).with_network_aliases(alias)
    for key, value in worker_env.items():
        container.with_env(key, value)

    await asyncio.to_thread(container.start)
    try:
        await wait_for_worker_ready(pool, schema, timeout=30.0)
    except TimeoutError:
        logs = _container_logs(container)
        await asyncio.to_thread(_stop_container, container)
        msg = f"{label} failed readiness gate\n{logs}"
        raise RuntimeError(msg) from None
    return container


@pytest_asyncio.fixture
async def chaos_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    e2e_dragonfly: E2EDragonfly,
    chaos_pg: ChaosPg,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
) -> AsyncIterator[E2EWorker]:
    """Worker container with tight watchdog timing."""
    container = await _start_gated_worker(
        image_tag=e2e_worker_image.tag,
        network=e2e_network,
        worker_env=_worker_env(chaos_pg, e2e_dragonfly, chaos_schema),
        alias=f"worker-sdw-{chaos_schema.schema_name}",
        pool=chaos_pool,
        schema=chaos_schema.schema_name,
        label="shutdown-watchdog e2e worker",
    )
    try:
        yield E2EWorker(container=container, schema=chaos_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


@pytest_asyncio.fixture
async def chaos_client(chaos_schema: ChaosSchema) -> AsyncIterator[TaskQ]:
    """Function-scoped TaskQ client."""
    from taskq import TaskQ

    async with TaskQ(dsn=chaos_schema.host_dsn, schema=chaos_schema.schema_name) as client:
        yield client


# ── Test ──────────────────────────────────────────────────────────────────


async def test_shutdown_hard_deadline_watchdog(
    chaos_client: TaskQ,
    chaos_worker: E2EWorker,
    chaos_pg: ChaosPg,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
    run_id: str,
) -> None:
    """Stop PG, SIGTERM the worker: the ShutdownWatchdog trips when the
    ``close_conn_bounded`` hang pushes the shutdown past
    ``termination_grace_period``.

    With ``termination_grace = 5.01`` and zero grace periods, the
    orchestration completes in < 0.1 s. The ``shutdown_event.set()`` at
    line 254 wakes the watchdog. Then ``close_conn_bounded`` hangs for
    5.0 s (PG is down, ``CLOSE_TIMEOUT_SECS``). The watchdog checks every
    0.5 s; at ``elapsed ≈ 5.0 s >= 5.01``, it trips with
    ``detector="shutdown-deadline"`` and exits the container with
    ``EXIT_WATCHDOG`` (code 2).
    """
    schema = chaos_schema.schema_name

    await chaos_client.enqueue(
        long_running_job,
        LongRunningPayload(run_id=run_id),
    )

    await wait_for_effects(
        chaos_pool,
        schema,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    # Stop PG so close_conn_bounded hangs for the full CLOSE_TIMEOUT_SECS.
    wrapped_pg = chaos_pg.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_pg.stop, timeout=2)

    # Send SIGTERM immediately (before isolate_self fires).
    wrapped_worker = chaos_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_worker.kill, signal="TERM")

    try:

        async def _exited() -> bool:
            await asyncio.to_thread(wrapped_worker.reload)
            return str(wrapped_worker.status) == "exited"

        await poll_until(
            _exited,
            timeout=60.0,
            description="watchdog worker to exit via the shutdown-deadline watchdog",
        )

        await asyncio.to_thread(wrapped_worker.reload)
        exit_code = wrapped_worker.attrs["State"]["ExitCode"]
        logs = _container_logs(chaos_worker.container)
        assert exit_code == EXIT_WATCHDOG, (
            f"expected watchdog exit code {EXIT_WATCHDOG}, got {exit_code}\n{logs}"
        )
        assert "shutdown-deadline" in logs, (
            f"shutdown-deadline detector marker missing from worker log\n{logs}"
        )
    finally:
        # Restart PG so the chaos_schema fixture teardown can connect.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(wrapped_pg.start)
            await _probe_pg(chaos_pg.host_dsn, attempts=60, interval=1.0)
