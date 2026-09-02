"""Redis outage chaos e2e — stop Dragonfly mid-run, verify graceful degradation.

Scenario: a worker running rate-limited jobs loses its Dragonfly (Redis)
container mid-run. The worker must NOT crash catastrophically: rate-limited
jobs should snooze/requeue (the token-bucket acquisition fails, the
consumer routes to ``mark_snoozed`` which re-schedules without consuming
retry budget). When Dragonfly returns, normal operation resumes — a fresh
job completes successfully.

This module runs its own function-scoped Dragonfly container (``chaos_df``)
so the shared session ``e2e_dragonfly`` is never stopped. The worker is
pointed at the chaos Dragonfly via its network alias. The module owns its
own schema, pool, client, and worker container — the shared ``e2e_pg``
session fixture provides the PG instance, but none of the shared module
fixtures (``e2e_schema``, ``e2e_worker``, ``e2e_client``, ``e2e_pg_pool``)
are requested, so the autouse ``clean_e2e_state`` guard early-yields.

Unlike PG loss (which drives ``isolate_self`` via heartbeat failures),
Redis loss does not affect heartbeats (PG is still up). The worker stays
alive and the producer keeps dispatching; rate-limited jobs that cannot
acquire a token are snoozed with ``ReservationUnavailable`` and re-scheduled
for later. Non-rate-limited jobs continue to run normally.

Timing budget:
  Stop Dragonfly → rate-limited jobs snooze immediately (Redis acquisition
  fails fast on connection refused). Restart Dragonfly → snoozed jobs
  re-dispatch on the next scheduled-wake tick (~1 s) and acquire tokens
  from the fresh bucket (capacity 5, refill 5/s).
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

from ._assertions import (
    fetch_effects,
    poll_until,
    wait_all,
    wait_for_effects,
    wait_for_worker_ready,
)
from .actors import (
    DeliverWebhookPayload,
    WelcomeEmailPayload,
    deliver_webhook,
    send_welcome_email,
)
from .conftest import (
    _DRAGONFLY_IMAGE,
    _E2E_EFFECTS_DDL,
    _SCHEMA_NAME_RE,
    E2EPg,
    E2EWorker,
    _container_logs,
    _flushdb,
    _next_redis_db,
    _stop_container,
)

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    from taskq import TaskQ

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_RESTART_PROBE_TIMEOUT = 60.0
_RECOVERY_TIMEOUT = 120.0


class ChaosDf(NamedTuple):
    """Chaos Dragonfly endpoints and container: host-mapped (test process)
    and in-network (workers) URLs, plus the container for stop/start control."""

    container: DockerContainer
    host_url: str
    network_url: str


class ChaosSchema(NamedTuple):
    """Chaos isolation unit: PG schema + Dragonfly logical DB on the chaos DF."""

    schema_name: str
    host_dsn: str
    redis_db: int
    dragonfly_network_url: str
    dragonfly_host_url: str


# ── Module-local fixtures ─────────────────────────────────────────────────


@pytest.fixture
def chaos_df(e2e_network: Network) -> Iterator[ChaosDf]:
    """Function-scoped chaos Dragonfly container on the shared e2e network.

    Own container so the test can stop/start it without touching the shared
    session ``e2e_dragonfly``. The network alias is unique per test and
    Docker preserves the container's network config across stop/start.
    """
    from testcontainers.community.redis import RedisContainer

    alias = f"df-outage-{uuid4().hex[:8]}"
    container = RedisContainer(image=_DRAGONFLY_IMAGE).with_command("--dbnum 128")
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    container.with_network(e2e_network).with_network_aliases(alias)
    container.with_bind_ports(6379, free_host_port())
    with container:
        client = container.get_client()
        try:
            assert client.ping()
        finally:
            client.close()
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield ChaosDf(
            container=container,
            host_url=f"redis://{host}:{port}",
            network_url=f"redis://{alias}:6379",
        )


@pytest_asyncio.fixture
async def chaos_schema(
    e2e_pg: E2EPg,
    chaos_df: ChaosDf,
) -> AsyncIterator[ChaosSchema]:
    """Function-scoped PG schema + chaos Dragonfly logical DB.

    Mirrors the conftest's e2e_schema setup: DROP SCHEMA IF EXISTS ...
    CASCADE before migrating (crash-safe), TaskQ migrations applied, the
    e2e_effects scratch table created, and the allocated Dragonfly DB
    flushed. Teardown drops the schema CASCADE.
    """
    import asyncpg

    from taskq.migrate import apply_pending_locked

    schema = f"tro_{uuid4().hex[:10]}"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        msg = f"derived chaos schema name {schema!r} is not a valid PG identifier"
        raise RuntimeError(msg)

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    await apply_pending_locked(e2e_pg.host_dsn, schema=schema)

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(_E2E_EFFECTS_DDL.format(schema=schema))
    finally:
        await conn.close()

    redis_db = _next_redis_db()
    await asyncio.to_thread(_flushdb, f"{chaos_df.host_url}/{redis_db}")

    yield ChaosSchema(
        schema_name=schema,
        host_dsn=e2e_pg.host_dsn,
        redis_db=redis_db,
        dragonfly_network_url=chaos_df.network_url,
        dragonfly_host_url=chaos_df.host_url,
    )

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def chaos_pool(chaos_schema: ChaosSchema) -> AsyncIterator[asyncpg.Pool]:
    """Function-scoped asyncpg pool on the host DSN."""
    import asyncpg

    pool = await asyncpg.create_pool(chaos_schema.host_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


def _worker_env(
    e2e_pg: E2EPg,
    chaos_schema: ChaosSchema,
) -> dict[str, str]:
    """Standard e2e worker env pointed at the chaos PG + Dragonfly."""
    return {
        "TASKQ_PG_DSN": e2e_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{chaos_schema.dragonfly_network_url}/{chaos_schema.redis_db}",
        "TASKQ_SCHEMA_NAME": chaos_schema.schema_name,
        "TASKQ_QUEUES": "e2e",
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
    """Start a worker container and gate on a fresh heartbeat."""
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
    e2e_pg: E2EPg,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
) -> AsyncIterator[E2EWorker]:
    """Worker container bound to the chaos PG/schema + Dragonfly."""
    container = await _start_gated_worker(
        image_tag=e2e_worker_image.tag,
        network=e2e_network,
        worker_env=_worker_env(e2e_pg, chaos_schema),
        alias=f"worker-redis-outage-{chaos_schema.schema_name}",
        pool=chaos_pool,
        schema=chaos_schema.schema_name,
        label="redis-outage e2e worker",
    )
    try:
        yield E2EWorker(container=container, schema=chaos_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


@pytest_asyncio.fixture
async def chaos_client(chaos_schema: ChaosSchema) -> AsyncIterator[TaskQ]:
    """Function-scoped TaskQ client on the chaos host DSN + schema."""
    from taskq import TaskQ

    async with TaskQ(dsn=chaos_schema.host_dsn, schema=chaos_schema.schema_name) as client:
        yield client


# ── Test ──────────────────────────────────────────────────────────────────


async def test_redis_outage_degrades_gracefully(
    request: pytest.FixtureRequest,
    chaos_client: TaskQ,
    chaos_worker: E2EWorker,
    chaos_df: ChaosDf,
    chaos_schema: ChaosSchema,
    chaos_pool: asyncpg.Pool,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    e2e_pg: E2EPg,
    run_id: str,
) -> None:
    """Stop Dragonfly mid-run: rate-limited jobs snooze; worker survives;
    restart Dragonfly and verify normal operation resumes.

    (a) Enqueue 5 rate-limited ``deliver_webhook`` jobs (token-bucket cap 5,
    refill 5/s). Wait for the first ``delivered`` effect — proving the
    worker is dispatching and Redis is healthy.

    (b) Stop the chaos Dragonfly container. Enqueue 3 more rate-limited
    jobs. These jobs cannot acquire a token (Redis connection refused), so
    the consumer routes them to ``mark_snoozed`` — they return to
    ``scheduled`` without consuming retry budget. The worker must NOT
    crash: verify the container is still running.

    (c) Restart Dragonfly. The snoozed jobs re-dispatch on the next
    scheduled-wake tick, acquire tokens from the fresh bucket, and
    complete. A fresh non-rate-limited ``send_welcome_email`` job also
    completes, proving the system is fully functional.
    """
    schema = chaos_schema.schema_name

    # ── (a) Baseline: 5 rate-limited jobs, verify Redis works ──────────
    burst_handles = [
        await chaos_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=run_id, endpoint_id=f"ep-{i:02d}"),
        )
        for i in range(5)
    ]

    await wait_for_effects(
        chaos_pool,
        schema,
        run_id,
        kind="delivered",
        min_count=1,
        timeout=30.0,
    )

    # ── (b) Stop Dragonfly, enqueue more rate-limited jobs ─────────────
    wrapped_df = chaos_df.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_df.stop, timeout=2)

    outage_handles = [
        await chaos_client.enqueue(
            deliver_webhook,
            DeliverWebhookPayload(run_id=f"{run_id}-outage", endpoint_id=f"out-{i}"),
        )
        for i in range(3)
    ]

    # The worker must NOT crash — verify it's still running after a short
    # settling period.
    await asyncio.sleep(2.0)
    wrapped_worker = chaos_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped_worker.reload)
    assert str(wrapped_worker.status) == "running", (
        "worker container should survive Redis loss without crashing"
    )

    # ── (c) Restart Dragonfly, verify recovery ─────────────────────────
    await asyncio.to_thread(wrapped_df.start)

    async def _df_accepts_connections() -> bool:
        import redis as redis_sync

        try:
            with redis_sync.from_url(f"{chaos_df.host_url}/{chaos_schema.redis_db}") as client:
                return bool(client.ping())
        except Exception:
            return False

    await poll_until(
        _df_accepts_connections,
        timeout=_RESTART_PROBE_TIMEOUT,
        description="chaos Dragonfly to accept connections after restart",
    )

    # Wait for the original burst to complete (some may have been snoozed
    # during the outage if they hadn't acquired tokens yet).
    await wait_all(burst_handles, timeout=_RECOVERY_TIMEOUT)

    # The outage jobs should complete after Dragonfly is back.
    await wait_all(outage_handles, timeout=_RECOVERY_TIMEOUT)

    # Verify all delivered effects landed.
    burst_effects = await fetch_effects(chaos_pool, schema, run_id, kind="delivered")
    assert len(burst_effects) == 5

    outage_effects = await fetch_effects(chaos_pool, schema, f"{run_id}-outage", kind="delivered")
    assert len(outage_effects) == 3

    # Fresh non-rate-limited job: proves the system is fully functional.
    fresh_handle = await chaos_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(
            run_id=f"{run_id}-recovery",
            user_id="u-recovery",
            email="recovery@example.com",
        ),
    )
    await fresh_handle.wait(timeout=60)

    recovery_effects = await fetch_effects(chaos_pool, schema, f"{run_id}-recovery", kind="send")
    assert len(recovery_effects) == 1
