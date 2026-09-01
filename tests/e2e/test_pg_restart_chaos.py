"""PG-restart chaos e2e - stop Postgres mid-job; the worker isolates, a replacement reclaims.

Scenario: a worker running ``long_running_job`` loses its Postgres
container mid-run. After TASKQ_MAX_HEARTBEAT_FAILURES consecutive
heartbeat failures the worker isolates (``isolate_self``) and its
container exits. When Postgres comes back, a replacement worker's
leader sweep reclaims the expired lock and re-dispatches the job, which
then completes exactly once overall.

This module runs its own function-scoped PostgresContainer
(``chaos_pg``): the shared session PG (``e2e_pg``) must never be
stopped, so the outage is confined to infrastructure this module owns.

The autouse ``clean_e2e_state`` fixture needs NO module-local override:
the test requests none of ``e2e_client`` / ``e2e_pg_pool`` /
``e2e_schema`` / ``e2e_worker``, so the conftest guard early-yields
without touching shared module state.

Why the design is deterministic: the test waits for the isolated
worker's container to reach ``exited`` BEFORE restarting Postgres, so
the old worker can never heartbeat or sweep again - there is no
sweep-vs-heartbeat reclaim race. The job's 3 s lock lease expires
during the outage, but no consumer can observe that until PG returns;
the replacement worker is then the only possible reclaimer. During the
outage the test-side pool and TaskQ client are equally blind, so
outage-phase assertions are limited to Docker container state.

Timing budget (worst case):
  ~2 s (4 heartbeat failures at the 0.5 s e2e interval; isolate fires on
      failures > TASKQ_MAX_HEARTBEAT_FAILURES=3)
  + ~5 s (isolate_self's bounded PG reconnect attempt fails)
  + the sibling-drain tail: isolate_self sets shutdown_event, every loop
      observes it and returns, the in-flight actor's terminal write to the
      dead PG fails fast, and the bounded teardown closes (~5 s each) run
      last. NOT bounded by TASKQ_TERMINATION_GRACE_PERIOD: that setting is
      validated in settings.py but no runtime path enforces it, so the
      drain is only as prompt as the slowest loop.
  ~= 20 s to container exit in practice (120 s poll budget).
After PG returns: leadership + 2 s sweep + 5 s retry backoff + 30 s actor
~= 40 s; the 180 s ``handle.wait`` budget covers this with margin.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, NamedTuple
from uuid import uuid4

import pytest
import pytest_asyncio

from taskq.testing._shared_containers import creator_labels
from tests.conftest import free_host_port

from ._assertions import fetch_effects, poll_until, wait_for_effects, wait_for_worker_ready
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
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2EDragonfly

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

# isolate (~2 s) + bounded reconnect failure (~5 s) + sibling drain
# (~15 s) ~= 22 s to container exit; leadership + sweep (2 s) + retry
# backoff (5 s) + actor sleep (30 s) ~= 40 s after PG returns. The 180 s
# handle.wait budget allows generous margin for Docker starvation.
_RECOVERY_TIMEOUT = 180.0

# Worker exit poll after PG loss: ~22 s worst case, generously padded for
# Docker starvation on constrained hosts.
# Isolate at ~2s (4 failed 0.5s heartbeat ticks) sets shutdown_event; the
# in-flight actor is cancelled, its terminal write to the dead PG fails
# fast, every sibling loop observes shutdown_event and returns, and the
# bounded teardown closes (~5s each) run last. No runtime deadline bounds
# that drain today (TASKQ_TERMINATION_GRACE_PERIOD is validation-only), so
# the budget is empirical: 45s shaved the teardown tail too closely.
_WORKER_EXIT_TIMEOUT = 120.0

# PG restart + readiness probe: the host port is pinned explicitly and the
# network alias is preserved by Docker, so both DSNs stay valid across the
# restart (see chaos_pg); 60 s covers slow restarts.
_PG_RESTART_PROBE_TIMEOUT = 60.0


class ChaosPg(NamedTuple):
    """Chaos PG endpoints and container: host-mapped (test process) and
    in-network (workers) DSNs, plus the container for stop/start control."""

    container: PostgresContainer
    host_dsn: str
    network_dsn: str


class ChaosSchema(NamedTuple):
    """Chaos isolation unit: PG schema on the chaos PG + Dragonfly logical DB."""

    schema_name: str
    host_dsn: str
    redis_db: int


# -- Module-local fixtures ---------------------------------------------------


@pytest.fixture
def chaos_pg(e2e_network: Network) -> Iterator[ChaosPg]:
    """Function-scoped chaos PG container on the shared e2e network.

    Own PostgresContainer so the test can stop/start it without touching
    the shared session ``e2e_pg`` container. The network alias is unique
    per test and Docker preserves the container's network config across
    stop/start, so the in-network DSN survives the outage untouched.

    The HOST port is pinned explicitly with ``with_bind_ports``. Docker
    does NOT preserve a *Docker-assigned* ephemeral host port across
    stop/start — it allocates a fresh one on start (observed: 34252 →
    34253) — which would silently invalidate ``host_dsn`` mid-test and
    strand the test-side pool, TaskQ client, and schema teardown on a
    port nothing listens on. An explicitly published port is part of the
    container's declared config and is restored verbatim on start.
    """
    from testcontainers.community.postgres import PostgresContainer

    alias = f"pg-restart-{uuid4().hex[:8]}"
    container = PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        # Same headroom rationale as the conftest's e2e_pg: worker pools
        # plus test-side asyncpg pools would otherwise approach PG's
        # default of 100 connections.
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
    """Function-scoped PG schema + Dragonfly logical DB on the chaos PG.

    Mirrors the conftest's e2e_schema setup: DROP SCHEMA IF EXISTS ...
    CASCADE before migrating (crash-safe), TaskQ migrations applied, the
    e2e_effects scratch table created, and the allocated Dragonfly DB
    flushed. Teardown drops the schema CASCADE; it assumes the test
    restarted PG (a test that fails mid-outage reports a teardown
    connect error on top of the primary failure).
    """
    import asyncpg

    from taskq.migrate import apply_pending_locked

    schema = f"tpr_{uuid4().hex[:10]}"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        msg = f"derived chaos schema name {schema!r} is not a valid PG identifier"
        raise RuntimeError(msg)

    # Belt-and-braces readiness probe on top of the testcontainers wait
    # strategy (mirrors the conftest's e2e_pg probe).
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
    """Function-scoped asyncpg pool on the chaos host DSN (mirrors
    e2e_pg_pool). All direct-SQL assertions go through this pool.

    Connections cached before the outage are marked closed when Docker
    tears down the proxied connection at container stop, so post-restart
    acquires transparently open fresh connections.
    """
    import asyncpg

    pool = await asyncpg.create_pool(
        chaos_schema.host_dsn,
        min_size=1,
        max_size=4,
    )
    assert pool is not None  # asyncpg returns None only for record_class paths
    try:
        yield pool
    finally:
        await pool.close()


def _worker_env(
    chaos_pg: ChaosPg,
    dragonfly: E2EDragonfly,
    chaos_schema: ChaosSchema,
) -> dict[str, str]:
    """Standard e2e worker env (mirrors the conftest's e2e_schema knobs)
    pointed at the chaos PG, schema, and Dragonfly logical DB.

    TASKQ_MAX_HEARTBEAT_FAILURES is NOT elevated: the default of 3 with
    the 0.5 s heartbeat interval drives isolate_self ~2 s after PG loss.
    """
    return {
        "TASKQ_PG_DSN": chaos_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{dragonfly.network_url}/{chaos_schema.redis_db}",
        "TASKQ_SCHEMA_NAME": chaos_schema.schema_name,
        "TASKQ_QUEUES": "e2e",
        # worker_main never migrates - only the CLI consumes
        # TASKQ_MIGRATE_ON_START, so it is inert on this entry path. The
        # chaos_schema fixture migrates before container start; the env
        # var is set only defensively, as in the conftest.
        "TASKQ_MIGRATE_ON_START": "false",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
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
    """Start a worker container and gate on a fresh post-register
    heartbeat row in ``{schema}.workers`` (mirrors the conftest's
    e2e_worker readiness gate).

    On readiness timeout the container logs are dumped into the failure
    message and the container is stopped before raising.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=image_tag)
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
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
    """Worker container bound to the chaos PG/schema (mirrors e2e_worker).

    Heartbeat failure knobs keep their defaults: with the 0.5 s e2e
    heartbeat interval and TASKQ_MAX_HEARTBEAT_FAILURES=3, PG loss drives
    isolate_self in ~2 s.
    """
    container = await _start_gated_worker(
        image_tag=e2e_worker_image.tag,
        network=e2e_network,
        worker_env=_worker_env(chaos_pg, e2e_dragonfly, chaos_schema),
        alias=f"worker-pgr-{chaos_schema.schema_name}",
        pool=chaos_pool,
        schema=chaos_schema.schema_name,
        label="chaos e2e worker",
    )
    try:
        yield E2EWorker(container=container, schema=chaos_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


@pytest_asyncio.fixture
async def chaos_client(chaos_schema: ChaosSchema) -> AsyncIterator[TaskQ]:
    """Function-scoped open ``TaskQ`` client on the chaos host DSN + schema
    (mirrors e2e_client)."""
    from taskq import TaskQ

    async with TaskQ(dsn=chaos_schema.host_dsn, schema=chaos_schema.schema_name) as client:
        yield client


# -- Test --------------------------------------------------------------------


async def test_pg_restart_isolates_worker_and_recovers_on_replacement(
    request: pytest.FixtureRequest,
    chaos_client: TaskQ,
    chaos_worker: E2EWorker,
    chaos_pg: ChaosPg,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    e2e_dragonfly: E2EDragonfly,
    run_id: str,
) -> None:
    """Stop PG mid-job: the worker isolates and exits; a replacement completes it.

    (a) After the ``started`` effect lands, the test stops the chaos PG
    container. The worker's heartbeat fails through
    TASKQ_MAX_HEARTBEAT_FAILURES, isolate_self runs (its own bounded
    reconnect fails because PG is down), and the container exits after
    the termination grace. The only outage-phase assertion is the Docker
    container state: every PG reader on the test side is blind.

    (b) With PG restarted, a replacement worker (identical env) starts.
    Its leader sweep records attempt 1 as crashed and re-pends the job
    with the 5 s retry backoff, then dispatches and completes it.

    (c) Two ``started`` effects prove the job ran once per attempt
    (isolated worker, then replacement); one ``finished`` effect proves
    exactly-once completion overall.
    """
    schema = chaos_schema.schema_name

    # -- Enqueue and wait for the job to start ------------------------------
    handle = await chaos_client.enqueue(
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

    # -- Stop PG: the worker loses its heartbeat ----------------------------
    wrapped_pg = chaos_pg.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_pg.stop, timeout=2)

    # -- Wait for the worker to isolate and exit ----------------------------
    # 4 heartbeat failures at the 0.5 s interval (~2 s) trigger
    # isolate_self; its bounded reconnect (5 s) fails against the dead PG,
    # and the worker shuts down through the 15 s termination grace. PG is
    # down, so this is a Docker-only assertion: no test-side SQL here.
    wrapped_worker = chaos_worker.container.get_wrapped_container()

    async def _worker_exited() -> bool:
        await asyncio.to_thread(wrapped_worker.reload)
        return str(wrapped_worker.status) == "exited"

    try:
        await poll_until(
            _worker_exited,
            timeout=_WORKER_EXIT_TIMEOUT,
            description="chaos worker container to exit after PG-loss isolation",
        )
    except TimeoutError:
        # The timeout message alone cannot show WHERE the shutdown chain
        # stalled (heartbeat isolate, phase machine, scope close, bounded
        # pool teardown) - attach the worker's own tail so the failure is
        # diagnosable without a manual container reproduction.
        msg = f"chaos worker did not exit within {_WORKER_EXIT_TIMEOUT}s\n{_container_logs(chaos_worker.container)}"
        raise RuntimeError(msg) from None

    # -- Restart PG and probe until it accepts connections ------------------
    await asyncio.to_thread(wrapped_pg.start)

    async def _pg_accepts_connections() -> bool:
        import asyncpg

        try:
            conn = await asyncpg.connect(chaos_pg.host_dsn)
        except (OSError, asyncpg.PostgresError):
            return False
        try:
            await conn.fetchval("SELECT 1")
        finally:
            await conn.close()
        return True

    await poll_until(
        _pg_accepts_connections,
        timeout=_PG_RESTART_PROBE_TIMEOUT,
        description="chaos PG to accept connections after restart",
    )

    # -- Start the replacement worker ---------------------------------------
    # Identical env to the first worker. The workers table still holds the
    # dead worker's row, but the readiness gate requires a FRESH
    # post-register heartbeat (within 10 s, after started_at); the first
    # worker's last heartbeat predates the outage, so only the replacement
    # can satisfy it.
    replacement = await _start_gated_worker(
        image_tag=e2e_worker_image.tag,
        network=e2e_network,
        worker_env=_worker_env(chaos_pg, e2e_dragonfly, chaos_schema),
        alias=f"worker-pgr2-{schema}",
        pool=chaos_pool,
        schema=schema,
        label="replacement chaos e2e worker",
    )
    try:
        # The replacement's leader sweep (2 s interval) reclaims the
        # expired lock, records attempt 1 as crashed, and re-pends with
        # the 5 s retry backoff; the job re-dispatches and runs its 30 s
        # actor to success. The pre-outage client pool reconnects via
        # fresh acquires now that PG is back.
        await handle.wait(timeout=_RECOVERY_TIMEOUT)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(replacement))
        await asyncio.to_thread(_stop_container, replacement)

    # -- Assertions ----------------------------------------------------------
    started = await fetch_effects(chaos_pool, schema, run_id, kind="started")
    assert len(started) == 2, (
        f"expected 2 'started' effects (one from the isolated worker, "
        f"one from the replacement), got {len(started)}"
    )

    finished = await fetch_effects(chaos_pool, schema, run_id, kind="finished")
    assert len(finished) == 1, (
        f"expected 1 'finished' effect (replacement worker completed the job), got {len(finished)}"
    )

    status = await chaos_pool.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',
        handle.job_id,
    )
    assert status == "succeeded"
