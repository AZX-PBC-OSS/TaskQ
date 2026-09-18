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
triggers it by pausing Postgres's container mid-run. Paused, not stopped: a
stopped container loses its Docker DNS record, and on a runner whose resolver
honours that immediately every pool-path PG write fails FAST (gaierror name
resolution, no retries worth the name) — the whole orchestration then
completes inside the 5.01 s budget, the clean path disarms the watchdog
(it is cancelled only after the TaskGroup's exit, so a completed shutdown
exits 0, which is the correct behavior for a shutdown that finished before
its deadline), and the test's exit-2 assertion becomes a coin flip on the
runner's DNS behavior. CI rolled exactly that once: the worker log showed
``terminal-write-failed ... gaierror`` and the container exited 0. A paused
container is the deterministic shape this test needs: the DNS record stays,
the established TCP connections blackhole (the peer's network stack is
frozen — no RST, no reply), so every in-flight and next PG write hangs
past the deadline and the trip is the only exit left. The orchestration
sets ``shutdown_event`` BEFORE the bounded close, so the watchdog starts
counting during the hang. With ``termination_grace = 5.01`` and zero grace
periods, the watchdog trips just after the deadline.

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

import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.worker._watchdog import EXIT_WATCHDOG
from tests.conftest import free_host_port

from ._assertions import poll_until, wait_for_effects
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
    running_worker,
)

if TYPE_CHECKING:
    import asyncpg
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from ._types import BuiltImage
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
    """Function-scoped chaos PG container. Skips with a reason (never
    errors) when the Docker daemon is unreachable."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    alias = f"pg-sdw-{new_uuid().hex[:8]}"
    container = PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        command="-c max_connections=1000",
    )
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
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

    schema = f"tsw_{new_uuid().hex[:10]}"
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
        # Lease and lag budget sized so the shutdown-deadline detector (the
        # subject of this test, tripping at termination_grace 5.01s) wins
        # any race with the lag detector: 6.0 + 0.5 < 8.0 keeps the
        # lag-lease invariant, and 6.0 > 5.01 keeps a loop blocked by the
        # close hang under detector 1's deadline, not detector 4's.
        "TASKQ_LOCK_LEASE": "8.0",
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "6.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "5.01",
        "TASKQ_WATCHDOG_DUMP_INTERVAL": "0.5",
        "TASKQ_SWEEP_INTERVAL": "2.0",
        "TASKQ_QUEUE_DEPTH_INTERVAL": "2.0",
        "TASKQ_RESERVATION_SLOTS_INTERVAL": "2.0",
        "TASKQ_STRANDED_JOBS_INTERVAL": "2.0",
    }


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
    async with running_worker(
        request,
        network=e2e_network,
        schema=chaos_schema,
        pg_pool=chaos_pool,
        image=e2e_worker_image,
        alias=f"worker-sdw-{chaos_schema.schema_name}",
        env=_worker_env(chaos_pg, e2e_dragonfly, chaos_schema),
        label="shutdown-watchdog e2e worker",
    ) as worker:
        yield worker


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
    """Pause PG, SIGTERM the worker: the ShutdownWatchdog trips when the
    shutdown can no longer make progress past
    ``termination_grace_period``.

    With ``termination_grace = 5.01`` and zero grace periods, every
    pool-path PG write hangs once PG's container is paused (the peer's
    TCP stack is frozen: no RST, no reply, no DNS change — see the module
    docstring for why the container is paused rather than stopped). The
    watchdog checks every 0.5 s; at ``elapsed ≈ 5.0 s >= 5.01``, it trips
    with ``detector="shutdown-deadline"`` and exits the container with
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

    # Pause PG so every pool-path write hangs past the deadline (paused =
    # frozen network stack: established connections blackhole instead of
    # failing fast, so the shutdown provably cannot complete before the
    # watchdog's deadline). Stopping instead would let a runner whose
    # resolver drops the container's DNS record fail every write fast,
    # finish the orchestration inside the budget, and exit 0 — correct
    # behavior for a completed shutdown, and a coin flip this test must
    # not depend on.
    wrapped_pg = chaos_pg.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_pg.pause)

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
        # Pin the DETECTOR-1 trip record, not just any force-exit: a paused
        # PG hangs the leader loops too, so detector 2 (stale sibling loop,
        # stale floor 10s) can also force-exit with EXIT_WATCHDOG, and the
        # straggler dumps carry the "shutdown-deadline" label without any
        # trip having fired. The trip record with the stall reason is the
        # one line only detector 1 writes (this actor is async, so no
        # tracked handle can substitute its reason).
        assert "worker-watchdog-trip" in logs and "shutdown still incomplete" in logs, (
            "the shutdown-deadline trip record is missing from the worker log: "
            "the exit must be detector 1's deadline trip, not a sibling "
            f"watchdog's force-exit\n{logs}"
        )
    finally:
        # Unpause (and, defensively, start) PG so the chaos_schema fixture
        # teardown can connect.
        with contextlib.suppress(Exception):
            await asyncio.to_thread(wrapped_pg.unpause)
            await asyncio.to_thread(wrapped_pg.start)
            await _probe_pg(chaos_pg.host_dsn, attempts=60, interval=1.0)


# ── The tracked-actor exit gate (#232's F1 closure) ────────────────────


def _reap_gate_worker_env(
    chaos_pg: ChaosPg, e2e_dragonfly: E2EDragonfly, chaos_schema: ChaosSchema
) -> dict[str, str]:
    """Worker env for the exit-gate test: a park-able budget and a live trip.

    Zero graces so the whole phase sequence completes in well under a
    second and everything after is the parked consumer + the exit gate;
    ``termination_grace 8.01`` gives the park a real window (the lease cap
    8 - 0.5 - 5 = 2.5s binds first) and the deadline trip a close bound;
    the 60s sync actor body outlives all of it. Lease/lag pair keeps the
    lag-lease invariant (6.0 + 0.5 < 8.0) and the lease above 4 x
    heartbeat.
    """
    return {
        "TASKQ_PG_DSN": chaos_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{e2e_dragonfly.network_url}/{chaos_schema.redis_db}",
        "TASKQ_SCHEMA_NAME": chaos_schema.schema_name,
        "TASKQ_QUEUES": "e2e",
        "TASKQ_MIGRATE_ON_START": "false",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "8.0",
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "6.0",
        "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "1.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "8.01",
        "TASKQ_WATCHDOG_DUMP_INTERVAL": "0.5",
        "TASKQ_SWEEP_INTERVAL": "2.0",
        "TASKQ_QUEUE_DEPTH_INTERVAL": "2.0",
        "TASKQ_RESERVATION_SLOTS_INTERVAL": "2.0",
        "TASKQ_STRANDED_JOBS_INTERVAL": "2.0",
    }


@pytest_asyncio.fixture
async def reap_gate_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    e2e_dragonfly: E2EDragonfly,
    chaos_pg: ChaosPg,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
) -> AsyncIterator[E2EWorker]:
    """Worker container with a live watchdog and a park-able budget."""
    async with running_worker(
        request,
        network=e2e_network,
        schema=chaos_schema,
        pg_pool=chaos_pool,
        image=e2e_worker_image,
        alias=f"worker-reap-{chaos_schema.schema_name}",
        env=_reap_gate_worker_env(chaos_pg, e2e_dragonfly, chaos_schema),
        label="tracked-actor exit-gate e2e worker",
    ) as worker:
        yield worker


async def test_shutdown_exit_gate_trips_when_a_sync_actor_outlives_teardown(
    chaos_client: TaskQ,
    reap_gate_worker: E2EWorker,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
) -> None:
    """THE F1 closure, at process level: a sync actor whose thread outlives
    the TaskGroup makes the process exit at the deadline trip — not park in
    the default executor's join past the hold it modeled.

    Pre-closure this was the constructible double-run: the TaskGroup exited
    cleanly (~0.1s), the watchdog was disarmed, and ``asyncio.Runner.close``
    then joined the detached 60s thread (THREAD_JOIN_TIMEOUT, 300s) — the
    container stayed alive well past the released row's ``scheduled_at``
    (deadline + exit tail), the sweep promoted it, and a second worker
    claimed it while the thread still executed. With the exit gate the
    watchdog stays armed until the tracked handle is reaped, the deadline
    trip is the process exit the hold always modeled, and the trip's reason
    says exactly what is still alive.
    """
    from datetime import UTC, datetime, timedelta

    from .actors import SyncOutliveShutdownPayload, sync_outlive_shutdown

    schema = chaos_schema.schema_name

    job = await chaos_client.enqueue(
        sync_outlive_shutdown,
        SyncOutliveShutdownPayload(run_id="reap-gate"),
    )
    enqueued = job.row
    assert enqueued.status == "pending"

    async def _row() -> dict[str, object]:
        async with chaos_pool.acquire() as conn:
            return dict(
                await conn.fetchrow(  # type: ignore[union-attr]  # Why: fetchrow returns a Record here; the dict() wrap is for attribute-free access below.
                    f"SELECT status, attempt, interrupt_count, scheduled_at "  # Why: schema is a fixture-owned identifier; the job id is $-bound.
                    f'FROM "{schema}".jobs WHERE id = $1',
                    job.job_id,
                )
            )

    async def _running() -> bool:
        return (await _row())["status"] == "running"

    await poll_until(_running, timeout=30.0, description="the sync actor's row to go running")

    sigterm_wall = datetime.now(UTC)
    wrapped_worker = reap_gate_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_worker.kill, signal="TERM")

    async def _exited() -> bool:
        await asyncio.to_thread(wrapped_worker.reload)
        return str(wrapped_worker.status) == "exited"

    # 30s, not 60: the pre-closure shape parked in the executor join until
    # the 60s body finished — this poll times out on that shape. The
    # closure trips at ~termination_grace + render/flush, ~10s.
    await poll_until(
        _exited,
        timeout=30.0,
        description="the worker to exit via the deadline trip despite the live actor thread",
    )

    await asyncio.to_thread(wrapped_worker.reload)
    exit_code = wrapped_worker.attrs["State"]["ExitCode"]
    logs = _container_logs(reap_gate_worker.container)
    assert exit_code == EXIT_WATCHDOG, (
        f"expected the watchdog trip's exit code {EXIT_WATCHDOG}, got "
        f"{exit_code} — the process must not outlive its own deadline just "
        f"because an actor thread does\n{logs}"
    )
    assert "tracked-actor-outlived-teardown" in logs, (
        "the trip must name the live tracked actor handle(s) — the distinct "
        f"reason is what keeps an operator from misreading it as a loop stall\n{logs}"
    )

    row = await _row()
    assert row["status"] == "scheduled", (
        "the interrupted sync actor's row must be released HELD (scheduled "
        f"behind the exit window); got {row['status']!r}"
    )
    assert row["attempt"] == enqueued.attempt, (
        "the interruption refunds the claim's attempt increment — the re-run "
        "must not spend a second attempt on one interruption"
    )
    assert row["interrupt_count"] == 1
    scheduled_at = row["scheduled_at"]
    assert isinstance(scheduled_at, datetime)
    exit_tail = 0.5 + 2.0 + 1.0  # dump interval + bounded flush + slack
    assert scheduled_at >= sigterm_wall + timedelta(seconds=8.01 + exit_tail - 1.5), (
        "the hold must keep the row unclaimable until the process is provably "
        "gone — the deadline trip plus the exit tail, which is now the true "
        f"exit by construction; scheduled_at={scheduled_at}"
    )
