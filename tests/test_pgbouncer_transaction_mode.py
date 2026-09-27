# ruff: noqa: S608  # Why: schema is a fixture-generated identifier, never user input; every value is $-bound.
"""PgBouncer transaction mode: the documented split topology, proven end-to-end.

The docs' prescribed deployment shape (configuration.md, "PgBouncer
Configuration Pattern" + worker/budget.py's ``pgbouncer_recommended``
threshold) splits the DSNs by connection role:

* ``TASKQ_PG_DSN_DIRECT`` → dispatcher_pool, heartbeat_pool, notify_conn,
  leader_conn: LISTEN/NOTIFY and advisory locks need a SESSION, so these
  roles must bypass the pooler entirely.
* ``TASKQ_PG_DSN_POOLED`` → worker_pool only: the worker pool uses no
  session-level features, so it may ride a transaction-mode PgBouncer —
  where asyncpg's prepared-statement cache is the classic dealbreaker
  (the pooler remaps server connections between statements, splitting a
  prepared statement's Parse from its Bind).

This module stands the topology up in containers and proves all four
mission legs:

* the claim/dispatch-adjacent hot path (enqueue + terminal writes run on
  ``worker_pool``) through the pooler — first NAIVELY (statement cache
  on, pooler without ``max_prepared_statements``), which must LOUDLY
  break with the pooler-remap signature (SQLSTATE ``26000``/``42P05``,
  "prepared statement does not exist"), then with TaskQ's knob
  (``TASKQ_PG_IS_POOLED=true`` → ``statement_cache_size=0``/
  ``max_cached_statement_lifetime=0`` on every TaskQ-built pool), which
  must run clean; and with a pooler that DOES track prepared statements
  (``max_prepared_statements`` set), where the default cache is safe;
* the heartbeat renewal path (direct pool by design) renewing leases of
  jobs claimed while the worker pool churns behind the pooler;
* a full worker subprocess boot + job completion through the split
  topology (the system_e2e spawn shape);
* the admin/progress paths staying DIRECT and working simultaneously
  with the pooled worker traffic, including a live LISTEN round trip on
  the notify session and ``pg_stat_activity`` proof that the pooled
  conns traverse PgBouncer while the session conns do not.

Container shapes, matching the timescale module's idiom: one module
container per pooler config, labeled with ``creator_labels()`` so a
crashed run's leftovers are sweepable, ``skip_test_without_docker`` so a
Docker-less machine sees skips with reasons.
"""

from __future__ import annotations

import asyncio
import contextlib
import statistics
import time
from collections.abc import AsyncGenerator, Iterator
from datetime import timedelta
from typing import Any, NamedTuple

import asyncpg
import pytest

from taskq import TaskQ
from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from taskq.testing.settings import make_integration_settings_dict
from taskq.worker.deps import WorkerDeps, open_worker_deps
from tests.system_e2e._harness import graceful_stop, reap, spawn_worker, wait_worker_ready
from tests.system_e2e.actors import SysPayload, sys_fast, sys_progress

pytestmark = pytest.mark.integration

_PG_IMAGE = "postgres:18-alpine"
_PGBOUNCER_IMAGE = "edoburu/pgbouncer:latest"
_PGBOUNCER_PORT = 5432

#: The SQLSTATEs a transaction-mode pooler's server-conn remap produces
#: when a client-side cached prepared statement is replayed against a
#: server connection that never saw its Parse: 26000
#: (invalid_sql_statement_name, "prepared statement ... does not exist")
#: and 42P05 (duplicate_prepared_statement, "... already exists").
_POOLER_REMAP_SQLSTATES = frozenset({"26000", "42P05"})

#: Queue the in-process hot-path legs enqueue to. The probe actor's
#: registration row (``actor_config``) is seeded in :func:`_migrated_conn`:
#: the claim CTE reads actor_config by primary key for every candidate
#: actor, an unregistered actor's rows are invisible to dispatch.
_QUEUE = "default"


class _PgBouncerStack(NamedTuple):
    """The module's container topology: one Postgres, two PgBouncers."""

    direct_dsn: str
    """Direct (session-mode) DSN at the Postgres backend."""

    naive_pooled_dsn: str
    """Through PgBouncer in transaction mode with ``max_prepared_statements=0``
    (the pre-1.21 / not-configured shape: NO prepared-statement tracking)."""

    full_pooled_dsn: str
    """Through PgBouncer in transaction mode with ``max_prepared_statements=200``
    (the pooler tracks prepared statements across server-conn remaps)."""

    naive_ip: str
    """The naive pooler container's IP on the shared docker network: the
    ``client_addr`` Postgres sees on every connection IT opens."""

    full_ip: str
    """Same, for the prepared-statement-tracking pooler."""


def _pooled_url(container: Any) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(_PGBOUNCER_PORT)
    return f"postgresql://taskq:taskq@{host}:{port}/taskq"


def _container_ip(container: Any) -> str:
    """The container's IP on its (non-default) docker network.

    ``ContainerInspectInfo`` is the testcontainers inspect model: PascalCase
    attributes, ``NetworkSettings.Networks`` a mapping of network name →
    network info (``IPAddress`` empty on networks the container has not
    joined yet).
    """
    networks = container.get_container_info().NetworkSettings.Networks
    for net in networks.values():
        ip = net.IPAddress
        if ip:
            return ip
    raise AssertionError(f"no docker-network IP on {container.get_container_id()}: {networks!r}")


@pytest.fixture(scope="module")
def pgbouncer_stack() -> Iterator[_PgBouncerStack]:
    """One Postgres + two transaction-mode PgBouncers (naive / full), one network.

    Both poolers point at the same Postgres backend; they differ in exactly
    one setting — ``max_prepared_statements`` — the knob that decides
    whether asyncpg's prepared-statement cache can survive transaction
    pooling.
    """
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    labels = creator_labels()

    def _pooler(max_prepared_statements: int) -> DockerContainer:
        return (
            DockerContainer(image=_PGBOUNCER_IMAGE)
            .with_exposed_ports(_PGBOUNCER_PORT)
            .with_network(network)
            .with_env("DB_HOST", "postgres")
            .with_env("DB_PORT", "5432")
            .with_env("DB_USER", "taskq")
            .with_env("DB_PASSWORD", "taskq")
            .with_env("DB_NAME", "taskq")
            .with_env("LISTEN_PORT", str(_PGBOUNCER_PORT))
            .with_env("POOL_MODE", "transaction")
            .with_env("AUTH_TYPE", "scram-sha-256")
            .with_env("MAX_PREPARED_STATEMENTS", str(max_prepared_statements))
            # The saturation a transaction pooler exists for: many client
            # conns, few server conns. With 2 server conns the client→server
            # remapping is structural (32 hammer clients cannot each own a
            # server conn), which is exactly the regime where an untracked
            # pooler breaks cached prepared statements and a tracked one
            # must carry them.
            .with_env("DEFAULT_POOL_SIZE", "2")
            .with_kwargs(labels=labels)
        )

    with (
        Network() as network,
        PostgresContainer(image=_PG_IMAGE, username="taskq", password="taskq", dbname="taskq")
        .with_network(network)
        .with_network_aliases("postgres")
        .with_kwargs(labels=labels) as pg,
    ):
        direct_dsn = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        with _pooler(0) as naive, _pooler(200) as full:
            stack = _PgBouncerStack(
                direct_dsn=direct_dsn,
                naive_pooled_dsn=_pooled_url(naive),
                full_pooled_dsn=_pooled_url(full),
                naive_ip=_container_ip(naive),
                full_ip=_container_ip(full),
            )
            _await_ready(stack.naive_pooled_dsn)
            _await_ready(stack.full_pooled_dsn)
            yield stack


@pytest.fixture
def probe_schema() -> str:
    """Per-test unique schema name (dropped by each test's cleanup)."""
    return "pgb_" + new_uuid().hex[:12]


# ── Helpers ───────────────────────────────────────────────────────────────


def _await_ready(pooled_dsn: str, timeout: float = 60.0) -> None:
    """Block until a connection through the pooler answers (the pooler
    container boots in well under a second; the Postgres backend it points
    at is the slow one)."""
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:

        async def _probe() -> None:
            conn = await asyncpg.connect(pooled_dsn)
            try:
                await conn.fetchval("SELECT 1")
            finally:
                await conn.close()

        try:
            asyncio.run(_probe())
            return
        except Exception as exc:  # Why: any boot-time refusal just means "not ready yet".
            last = exc
            time.sleep(0.25)
    raise TimeoutError(f"pooler at {pooled_dsn} never answered: {last!r}")


async def _migrated_conn(direct_dsn: str, schema: str) -> asyncpg.Connection:
    """A direct connection on a freshly migrated schema (+ the system tier's
    effects ledger table, which the spawned worker's actors append to, and
    the probe actor's registration: the claim CTE reads actor_config by
    primary key for every candidate actor)."""
    from taskq.testing.pg import seed_actors

    conn = await asyncpg.connect(direct_dsn)
    try:
        await apply_pending(conn, schema=schema)
        await seed_actors(conn, schema, actors=["pgbouncer_probe"])
        await conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".sys_effects ('
            "job_id UUID NOT NULL, attempt INT NOT NULL, actor TEXT NOT NULL, "
            "kind TEXT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"
        )
    except BaseException:
        await conn.close()
        raise
    return conn


async def _drop_schema(direct_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(direct_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


@contextlib.asynccontextmanager
async def _open_split(
    stack: _PgBouncerStack,
    schema: str,
    *,
    pooled_dsn: str,
    pg_is_pooled: bool,
    max_concurrency: str = "8",
) -> AsyncGenerator[tuple[WorkerDeps, PostgresBackend]]:
    """Open the documented topology through TaskQ's own factories.

    ``pg_dsn_direct`` → the Postgres backend (dispatcher, heartbeat, notify,
    leader); ``pg_dsn_pooled`` → the given pooler URL (worker_pool only);
    ``pg_is_pooled`` is TaskQ's declaration knob. Everything is built by
    ``open_worker_deps`` exactly the way a real pod builds it.
    ``max_concurrency`` sizes the worker pool (``int(max_concurrency * 1.5)``
    client conns) — the hot-path legs widen it so the hammer can fan wider
    than the pooler's server pool and force the connection rotation.
    """
    raw = make_integration_settings_dict(
        stack.direct_dsn,
        TASKQ_SCHEMA_NAME=schema,
        TASKQ_PG_DSN_DIRECT=stack.direct_dsn,
        TASKQ_PG_DSN_POOLED=pooled_dsn,
        TASKQ_PG_IS_POOLED="true" if pg_is_pooled else "false",
        TASKQ_MAX_CONCURRENCY=max_concurrency,
    )
    settings = WorkerSettings.load_from_dict(raw)
    async with open_worker_deps(settings) as deps:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=settings.cancellation_grace_period),
            cleanup_grace_period=timedelta(seconds=settings.cleanup_grace_period),
        )
        yield deps, backend


def _job_args(i: int) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_uuid(),
        actor="pgbouncer_probe",
        queue=_QUEUE,
        payload={"i": i},
        max_attempts=1,
        retry_kind="transient",
        scheduled_at=None,
    )


def _is_pooler_remap_error(exc: BaseException) -> bool:
    """The prepared-statement remap signature, by SQLSTATE or message."""
    sqlstate = getattr(exc, "sqlstate", None) or getattr(exc, "sqlcode", None)
    if sqlstate in _POOLER_REMAP_SQLSTATES:
        return True
    return "prepared statement" in str(exc).lower()


async def _remap_dance(pooled_dsn: str, rounds: int = 10) -> list[BaseException]:
    """The remap probe: four single-connection pools, opened with TaskQ's
    shipped cache tuning, round-robin the same cached statement.

    The pooler runs ``default_pool_size=2`` (the saturation a transaction
    pooler exists for: many clients, few server conns), so four client
    conns cannot each own a server conn — by pigeonhole at least two share
    one, and an untracked pooler (``max_prepared_statements=0``) neither
    re-pairs a client with the conn holding its prepared statements nor
    DISCARDs what the previous pairing left there. Every client names its
    first cached statement ``__asyncpg_stmt_0__``, so the second client to
    prepare on a shared server conn takes the remap error (26000 "does
    not exist" or 42P05 "already exists") within the first rounds — by
    construction, no scheduling race.
    """
    from taskq.connections import (
        DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
        DEFAULT_STATEMENT_CACHE_SIZE,
    )

    pool_kwargs: dict[str, Any] = {
        "min_size": 1,
        "max_size": 1,
        "statement_cache_size": DEFAULT_STATEMENT_CACHE_SIZE,
        "max_cached_statement_lifetime": DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    }
    pools = [await asyncpg.create_pool(dsn=pooled_dsn, **pool_kwargs) for _ in range(4)]
    errors: list[BaseException] = []
    try:
        for _round in range(rounds):
            for pool in pools:
                try:
                    async with pool.acquire() as conn:
                        await conn.fetchval("SELECT $1::int", _round)
                except asyncpg.PostgresError as exc:
                    errors.append(exc)
    finally:
        for pool in pools:
            await pool.close()
    return errors


async def _client_addr_of(pool_or_conn: Any) -> str | None:
    """The server-visible ``client_addr`` of one of the pool's (or the
    dedicated connection's) sessions.

    Through PgBouncer this is the pooler's container IP; on a direct
    connection it is the client's own address. ``None`` only for a unix
    socket (never the case for these TCP containers).
    """
    if hasattr(pool_or_conn, "acquire"):
        async with pool_or_conn.acquire() as conn:
            return await conn.fetchval(
                "SELECT host(client_addr) FROM pg_stat_activity "
                "WHERE pid = pg_backend_pid() AND datname = current_database()"
            )
    return await pool_or_conn.fetchval(
        "SELECT host(client_addr) FROM pg_stat_activity "
        "WHERE pid = pg_backend_pid() AND datname = current_database()"
    )


async def _hammer_enqueue(
    backend: PostgresBackend, tasks: int = 16, iters: int = 15
) -> list[BaseException]:
    """Concurrent enqueue rounds through ``worker_pool`` — the pooler hot path.

    Each task drives sequential single-job enqueues (each call acquires,
    runs its admission reads + insert, and releases) with a phase-jittered
    micro-yield between rounds, and the tasks run gathered: the
    desynchronized interleaving is what forces the remap — a synchronized
    hot loop tends to be handed back the same server connection (LIFO
    reuse), which hides the rotation the shape is named for.
    """

    async def _one(task_i: int) -> int:
        for k in range(iters):
            await backend.enqueue_batch([_job_args(task_i * 1000 + k)])
            await asyncio.sleep(0.001 * ((task_i + k) % 3))
        return 0

    results = await asyncio.gather(*[_one(t) for t in range(tasks)], return_exceptions=True)
    return [r for r in results if isinstance(r, BaseException)]


# ── Leg a: the hot path through the pooler — naive breaks, the knob fixes ─


@pytest.mark.timeout(240)
async def test_naive_pooler_breaks_and_taskq_knob_fixes(
    pgbouncer_stack: _PgBouncerStack, probe_schema: str
) -> None:
    """Transaction pooling + asyncpg's statement cache, the classic dealbreaker.

    Red leg: the naive deployment (no ``TASKQ_PG_IS_POOLED``, so TaskQ's
    pools keep their 512-entry prepared-statement cache) against a pooler
    without ``max_prepared_statements`` must LOUDLY break with the remap
    signature — never hang, never silently misbehave.

    Green leg: the same pooler with ``TASKQ_PG_IS_POOLED=true`` — TaskQ's
    pools disable the cache (``statement_cache_size=0``,
    ``max_cached_statement_lifetime=0``, overriding any operator tuning) —
    must run the same hot path clean, terminal rows included.
    """
    stack = pgbouncer_stack
    ledger = await _migrated_conn(stack.direct_dsn, probe_schema)
    try:
        # ── Red: the naive config breaks loudly ──────────────────────
        remap_errors: list[BaseException] = []
        async with _open_split(
            stack, probe_schema, pooled_dsn=stack.naive_pooled_dsn, pg_is_pooled=False
        ) as (_deps, backend):
            hot_path = await _hammer_enqueue(backend, tasks=32)
            remap_errors = [e for e in hot_path if _is_pooler_remap_error(e)]
        # The deterministic dance decides; the hot-path hammer is the
        # under-contention corroboration (the same remap signature, on
        # TaskQ's own statements).
        remap_errors += [
            e for e in await _remap_dance(stack.naive_pooled_dsn) if _is_pooler_remap_error(e)
        ]
        assert remap_errors, (
            "the naive config (statement cache on, pooler without "
            "max_prepared_statements) did NOT break — asyncpg/pgbouncer "
            "behavior changed; this red leg is the regression guard's teeth"
        )

        await ledger.execute(f'DELETE FROM "{probe_schema}".jobs')

        # ── Green: the same pooler, the same hammer, TaskQ's knob on ─
        async with _open_split(
            stack, probe_schema, pooled_dsn=stack.naive_pooled_dsn, pg_is_pooled=True
        ) as (deps, backend):
            errors = await _hammer_enqueue(backend, tasks=32)
            assert not errors, f"the pooled-knob hot path raised: {errors!r}"
            assert not await _remap_dance(stack.naive_pooled_dsn), (
                "the remap dance fired with the statement cache disabled"
            )

            # The terminal write also rides worker_pool: complete one
            # claimed job through the pooler and read it back direct.
            worker_id = new_uuid()
            lease = timedelta(seconds=deps.settings.lock_lease)
            claimed = await backend.dispatch_batch(worker_id, [_QUEUE], 10, lease)
            assert claimed, "nothing claimed for the terminal leg"
            for job in claimed:
                ok = await backend.mark_succeeded(
                    job.id,
                    worker_id,
                    result={"done": True},
                    attempt=job.attempt,
                    claim_epoch=job.claim_epoch,
                )
                assert ok, f"mark_succeeded fenced off for {job.id}"
            rows = await ledger.fetch(
                f'SELECT status, result FROM "{probe_schema}".jobs WHERE id = ANY($1::uuid[])',
                [str(j.id) for j in claimed],
            )
            assert rows and all(r["status"] == "succeeded" for r in rows), rows
    finally:
        await ledger.close()
        await _drop_schema(stack.direct_dsn, probe_schema)


@pytest.mark.timeout(240)
async def test_pooler_with_prepared_statement_tracking_keeps_the_cache(
    pgbouncer_stack: _PgBouncerStack, probe_schema: str
) -> None:
    """The operator alternative: pooler-side ``max_prepared_statements``.

    With the pooler itself tracking prepared statements across server-conn
    remaps (PgBouncer ≥ 1.21), asyncpg's default cache is safe and TaskQ
    needs no declaration — the same hot path runs clean with the shipped
    cache tuning untouched.
    """
    stack = pgbouncer_stack
    ledger = await _migrated_conn(stack.direct_dsn, probe_schema)
    try:
        async with _open_split(
            stack, probe_schema, pooled_dsn=stack.full_pooled_dsn, pg_is_pooled=False
        ) as (deps, backend):
            errors = await _hammer_enqueue(backend, tasks=32)
            assert not errors, f"the tracked-pooler hot path raised with the cache on: {errors!r}"
            # The deterministic probe stays clean too: the pooler's own
            # statement tracking carries every cached name across remaps.
            assert not await _remap_dance(stack.full_pooled_dsn), (
                "the remap dance fired with pooler-side max_prepared_statements tracking on"
            )
            # And the traversal proof: the worker pool's server-side conns
            # are the pooler's (client_addr = the pgbouncer container IP).
            addr = await _client_addr_of(deps.worker_pool)
            assert addr == stack.full_ip, f"worker_pool is not traversing the pooler: {addr}"
    finally:
        await ledger.close()
        await _drop_schema(stack.direct_dsn, probe_schema)


# ── Legs b + d: heartbeat renews, admin/progress stay direct, together ────


@pytest.mark.timeout(240)
async def test_documented_topology_heartbeat_admin_and_listen_stay_direct(
    pgbouncer_stack: _PgBouncerStack, probe_schema: str
) -> None:
    """The full role split, live at once, on the naive pooler + the knob.

    One ``open_worker_deps`` under the documented env split: jobs enqueued
    and completed through the pooler, claims + heartbeat renewals through
    the direct pools, admin reads querying the same rows concurrently, and
    the notify session proving a live LISTEN round trip — all while
    ``pg_stat_activity`` shows the worker pool's conns behind the pooler
    and every session-role conn direct.

    Latency is also recorded here: enqueue through the pooler vs enqueue
    with every role direct (the fallback topology), same schema, same
    round count — surfaced in the run output for the deployment guide.
    """
    stack = pgbouncer_stack
    ledger = await _migrated_conn(stack.direct_dsn, probe_schema)
    # The admin lane gets its OWN direct connection: the concurrency this
    # leg proves is the admin reads running simultaneously with the pooled/
    # direct job traffic, not two tasks sharing one conn — asyncpg forbids
    # concurrent operations on a single connection (the main lane issues
    # pg_notify on ``ledger`` while the admin loop is mid-fetch), and the
    # product never shares a conn across tasks either. Closed in the outer
    # finally, before ``ledger``.
    admin_conn = await asyncpg.connect(stack.direct_dsn)
    admin_reads = 0
    try:
        async with _open_split(
            stack, probe_schema, pooled_dsn=stack.naive_pooled_dsn, pg_is_pooled=True
        ) as (deps, backend):
            # ── Topology pins: who is behind the pooler, who is not ──
            worker_addr = await _client_addr_of(deps.worker_pool)
            dispatcher_addr = await _client_addr_of(deps.dispatcher_pool)
            heartbeat_addr = await _client_addr_of(deps.heartbeat_pool)
            assert worker_addr == stack.naive_ip, (
                f"worker_pool must traverse the pooler, saw client_addr={worker_addr}"
            )
            assert dispatcher_addr != stack.naive_ip, (
                f"dispatcher_pool must be DIRECT, saw client_addr={dispatcher_addr}"
            )
            assert heartbeat_addr != stack.naive_ip, (
                f"heartbeat_pool must be DIRECT, saw client_addr={heartbeat_addr}"
            )
            notify_conn = deps.notify_conn
            assert notify_conn is not None, "the split topology must open the notify session"
            notify_addr = await _client_addr_of(notify_conn)
            assert notify_addr != stack.naive_ip, (
                f"notify_conn must be DIRECT (LISTEN needs a session), saw {notify_addr}"
            )

            # ── Hot path: enqueue (pooler) → claim (direct) → heartbeat
            #    (direct) → terminal (pooler), with admin reads concurrent
            n_jobs = 24
            worker_id = new_uuid()
            lease = timedelta(seconds=deps.settings.lock_lease)

            enqueue_errors = await _hammer_enqueue(backend, tasks=8, iters=3)
            assert not enqueue_errors, f"pooled enqueue raised: {enqueue_errors!r}"

            async def _admin_reads() -> None:
                """The admin path's direct read shape, running while pooled
                traffic churns: never blocked by the pooler, never routed
                through it. Rides ``admin_conn`` — a dedicated direct conn,
                since the main lane drives ``ledger`` concurrently."""
                nonlocal admin_reads
                for _ in range(20):
                    rows = await admin_conn.fetch(f'SELECT count(*) FROM "{probe_schema}".jobs')
                    admin_reads += int(rows[0][0])
                    await asyncio.sleep(0.005)

            admin_task = asyncio.create_task(_admin_reads())
            try:
                claimed = await backend.dispatch_batch(worker_id, [_QUEUE], n_jobs, lease)
                assert len(claimed) == n_jobs, f"claimed {len(claimed)} of {n_jobs}"

                # ── Leg b: the heartbeat renewal path (direct pool) renews
                #    every lease the claim took.
                renewed = await backend.heartbeat_jobs(worker_id, lease)
                assert renewed == n_jobs, f"heartbeat renewed {renewed} of {n_jobs} leases"

                # ── Leg d: LISTEN round trip on the (direct) notify session
                #    — the session feature transaction pooling cannot carry.
                got: list[str] = []
                done = asyncio.Event()

                def _on_notify(_conn: object, _pid: int, _chan: str, payload: str) -> None:
                    got.append(payload)
                    done.set()

                await notify_conn.add_listener("taskq_pgbouncer_probe", _on_notify)
                await ledger.execute("SELECT pg_notify('taskq_pgbouncer_probe', 'direct-session')")
                await asyncio.wait_for(done.wait(), timeout=10)
                await notify_conn.remove_listener("taskq_pgbouncer_probe", _on_notify)
                assert got == ["direct-session"]

                for job in claimed:
                    ok = await backend.mark_succeeded(
                        job.id,
                        worker_id,
                        result={"done": True},
                        attempt=job.attempt,
                        claim_epoch=job.claim_epoch,
                    )
                    assert ok, f"terminal write fenced off for {job.id}"
            finally:
                # The ledger conn closes in the outer finally under every
                # outcome; the admin task must never outlive it.
                if not admin_task.done():
                    admin_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await admin_task
            assert admin_reads >= n_jobs, "admin reads never saw the enqueued rows"

            rows = await ledger.fetch(
                f'SELECT status FROM "{probe_schema}".jobs WHERE id = ANY($1::uuid[])',
                [str(j.id) for j in claimed],
            )
            assert rows and all(r["status"] == "succeeded" for r in rows)

            # ── Latency: the same enqueue hot path through the pooler vs
            #    all-direct (the fallback topology), recorded for the guide.
            pooled_rounds = await _hammer_enqueue(backend)
            assert not pooled_rounds
            async with _open_split(
                stack, probe_schema, pooled_dsn=stack.direct_dsn, pg_is_pooled=False
            ) as (_direct_deps, direct_backend):
                t_direct: list[float] = []
                t_pooled: list[float] = []
                for _ in range(30):
                    t0 = time.monotonic()
                    await direct_backend.enqueue_batch([_job_args(0)])
                    t_direct.append(time.monotonic() - t0)
                    t0 = time.monotonic()
                    await backend.enqueue_batch([_job_args(0)])
                    t_pooled.append(time.monotonic() - t0)
            print(  # Why: the latency comparison is the deliverable; surfaced with -rP.
                f"pgbouncer hot-path enqueue ms (pooler p50={statistics.median(t_pooled) * 1000:.2f} mean={statistics.mean(t_pooled) * 1000:.2f}) "
                f"(direct p50={statistics.median(t_direct) * 1000:.2f} mean={statistics.mean(t_direct) * 1000:.2f})"
            )
    finally:
        await admin_conn.close()
        await ledger.close()
        await _drop_schema(stack.direct_dsn, probe_schema)


# ── Leg c: full worker boot + completion through the pooler ──────────────


@pytest.mark.timeout(300)
async def test_worker_boot_and_job_completion_through_pooler(
    pgbouncer_stack: _PgBouncerStack, probe_schema: str
) -> None:
    """A real worker pod on the documented env split: boot, dispatch,
    complete, drain — the worker_pool behind PgBouncer the whole time.

    The spawn is the system tier's production shape (``tests.system_e2e``
    harness: a real OS process running the real bootstrap, env-configured
    like a pod). The client rides the direct DSN (the operator's default:
    ``TASKQ_PG_DSN`` stays direct unless overridden), the worker gets the
    full split — DIRECT for dispatcher/heartbeat/notify/leader, the naive
    pooler + ``TASKQ_PG_IS_POOLED=true`` for worker_pool — and completes
    fast + progress jobs end-to-end. The admin path is proven concurrent:
    while the worker runs, a direct connection reads the jobs rows (the
    admin read shape) and the pooler's server-side conns appear in
    ``pg_stat_activity``.
    """
    stack = pgbouncer_stack
    ledger = await _migrated_conn(stack.direct_dsn, probe_schema)
    worker = spawn_worker(
        stack.direct_dsn,
        probe_schema,
        tag="pgb",
        extra_env={
            "TASKQ_PG_DSN_DIRECT": stack.direct_dsn,
            "TASKQ_PG_DSN_POOLED": stack.naive_pooled_dsn,
            "TASKQ_PG_IS_POOLED": "true",
        },
    )
    try:
        wait_worker_ready(worker)

        # Pooler traversal, concurrent with a live worker: at least one
        # server-side conn from the pooler's IP while the pod is up.
        async def _wait_for_pooler_conn() -> None:
            for _ in range(40):
                n = await ledger.fetchval(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE host(client_addr) = $1 AND datname = current_database()",
                    stack.naive_ip,
                )
                if n:
                    return
                await asyncio.sleep(0.25)
            raise AssertionError(
                "no pooler-IP backends appeared in the activity view while the pod ran"
            )

        async with TaskQ(dsn=stack.direct_dsn, schema=probe_schema) as client:
            fast = await client.enqueue(sys_fast, SysPayload())
            got = await fast.wait(timeout=60)
            assert got == {"beats": 0}

            progress = await client.enqueue(sys_progress, SysPayload(beats=3))
            events = [ev async for ev in progress.progress_stream()]
            # The stream must terminate on the job's terminal event (the PG
            # poll fallback may coalesce the intermediate beats into the
            # final snapshot — the contract is the terminal event, not the
            # event count).
            assert events and events[-1].terminal, f"stream did not terminate: {events!r}"
            assert events[-1].status == "succeeded"
            assert events[-1].step == 2, f"the actor's progress beats were lost: {events!r}"

            await _wait_for_pooler_conn()
            # The admin read shape (direct, concurrent with the pooled pod):
            rows = await ledger.fetch(
                f'SELECT status, count(*) FROM "{probe_schema}".jobs GROUP BY status'
            )
            statuses = {r["status"]: r["count"] for r in rows}
            assert statuses.get("succeeded", 0) >= 2

        rc = graceful_stop(worker)
        assert rc == 0, f"worker did not drain cleanly through the pooler: rc={rc}"
    finally:
        reap(worker)
        await ledger.close()
        await _drop_schema(stack.direct_dsn, probe_schema)
