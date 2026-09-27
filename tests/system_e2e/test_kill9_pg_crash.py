"""The kill9 SIGKILL campaign, part 3: Postgres dies by ``docker kill``.

The repo's PG-restart coverage (tests/e2e/test_pg_restart_chaos.py)
STOPS Postgres (SIGTERM: a clean shutdown, a checkpointed exit). This
campaign kills pid 1 with SIGKILL mid-transaction: WAL crash recovery on
restart, all in-flight transactions rolled back by the redo pass, every
live backend's work discarded the hard way.

The proof, in order:

1. AT THE KILL, a worker is mid-job (real executions in flight, observed
   via the effects ledger) and the TEST holds an uncommitted transaction
   that has written a row - the mid-transaction shape;
2. ``docker kill`` (no checkpoint, no graceful shutdown), then
   ``docker start`` on the SAME container (its declared config - the
   pinned host port included - is restored verbatim);
3. crash recovery CONVERGES: PG answers, the migration ledger is whole
   (a re-run of ``apply_pending_locked`` applies NOTHING - no half-applied
   migration state, no checksum drift), the uncommitted transaction's row
   is gone (the rollback happened), the tagged population's conservation
   counter balances across jobs + events + attempts + archive;
4. the fleet recovers: the killed-PG worker isolates and exits (the
   established heartbeat-failure contract), a replacement reclaims the
   in-flight rows, and the queue drains (fresh enqueues run to success).
"""

# ruff: noqa: S608  # Why: schema identifiers are fixture-side validated names; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING, NamedTuple

import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from taskq.migrate import apply_pending, apply_pending_locked
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker
from tests.conftest import free_host_port
from tests.system_e2e._harness import WorkerProc, reap, wait_worker_ready
from tests.system_e2e._invariants import (
    assert_effects_balance,
    conservation_violations,
)
from tests.system_e2e._kill_actors import Kill9Payload, kill9_slow, kill9_starting

if TYPE_CHECKING:
    import asyncpg
    from testcontainers.community.postgres import PostgresContainer

    from taskq import TaskQ

pytestmark = [
    pytest.mark.integration,
    pytest.mark.system,
    pytest.mark.timeout(600),
]

_TAG = "kill9-pg"

_PG_IMAGE = "postgres:18-alpine"
_PG_USER = "taskq"
_PG_PASSWORD = "taskq"
_PG_DB = "taskq"

#: Worker isolation after the PG kill: 3 failed heartbeats at the 0.5s
#: interval trigger isolate_self, its bounded reconnect fails against the
#: dead PG, and the shutdown ladder runs the 15s termination grace. The
#: cap is generous because it bounds the LADDER, not one phase.
_WORKER_EXIT_CAP_S = 90.0

#: PG restart + readiness probe: 60s covers slow crash recovery (WAL
#: replay) on a co-tenanted daemon.
_PG_RESTART_PROBE_CAP_S = 60.0

_SETTLE_CAP_S = 90.0


class Kill9ChaosPg(NamedTuple):
    """The chaos PG's endpoints and container control."""

    container: PostgresContainer
    container_id: str
    host_dsn: str


@pytest.fixture
def kill9_chaos_pg() -> Iterator[Kill9ChaosPg]:
    """Function-scoped chaos PG: its OWN container (the session-shared one
    must never be touched), pinned host port (an explicitly published port
    is part of the container's declared config and survives kill/start),
    ownership labels for sweepability under disabled Ryuk. Skips with a
    reason when the Docker daemon is unreachable."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    container = PostgresContainer(
        image=_PG_IMAGE,
        username=_PG_USER,
        password=_PG_PASSWORD,
        dbname=_PG_DB,
        command="-c max_connections=200",
    )
    container.with_kwargs(labels=creator_labels())
    container.with_bind_ports(5432, free_host_port())
    with container as c:
        yield Kill9ChaosPg(
            container=c,
            container_id=c.get_container_id(),
            host_dsn=c.get_connection_url().replace("postgresql+psycopg2://", "postgresql://"),
        )


@pytest_asyncio.fixture
async def kill9_schema(kill9_chaos_pg: Kill9ChaosPg) -> AsyncIterator[str]:
    """The chaos schema: unique per test, migrations applied ONCE at setup
    (the crash mid-test must recover a FULLY-migrated ledger, not a
    half-applied one), torn down after the container is back up."""
    import asyncpg

    schema = f"kill9pg_{new_uuid().hex[:10]}"
    conn = await asyncpg.connect(kill9_chaos_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'CREATE TABLE "{schema}".sys_effects ('
            "job_id UUID NOT NULL, attempt INT NOT NULL, actor TEXT NOT NULL, "
            "kind TEXT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())"
        )
        yield schema
    finally:
        with contextlib.suppress(Exception):
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


def _docker_path() -> str:
    import shutil

    docker = shutil.which("docker")
    assert docker is not None, "docker CLI not on PATH - the kill idiom requires it"
    return docker


def _docker_kill(container_id: str) -> None:
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell, the id is the fixture's own container.
        [_docker_path(), "kill", container_id], check=True, capture_output=True, timeout=30
    )


def _docker_start(container_id: str) -> None:
    subprocess.run(  # noqa: S603  # Why: fixed argv, no shell, the id is the fixture's own container.
        [_docker_path(), "start", container_id], check=True, capture_output=True, timeout=30
    )


async def _wait_pg_answers(dsn: str, cap_s: float) -> None:
    """Poll PG back to answering queries (crash recovery's readiness
    signal - the restart is done when the redo pass is)."""
    import asyncpg

    deadline = time.monotonic() + cap_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = await asyncpg.connect(dsn)
            try:
                await conn.fetchval("SELECT 1")
            finally:
                await conn.close()
            return
        except (OSError, asyncpg.PostgresError) as exc:
            last = exc
            await asyncio.sleep(0.5)
    raise AssertionError(f"PG never accepted connections within {cap_s}s: {last!r}")


async def test_docker_kill_mid_transaction_recovers_and_conserves(
    kill9_chaos_pg: Kill9ChaosPg,
    kill9_schema: str,
) -> None:
    """SIGKILL Postgres mid-transaction: crash recovery converges (the
    ledger reconciles, migrations are whole, the uncommitted row is gone)
    and the fleet drains after the replacement wins leadership."""
    dsn = kill9_chaos_pg.host_dsn
    schema = kill9_schema
    worker: WorkerProc | None = None
    client: TaskQ | None = None
    tx_conn: asyncpg.Connection | None = None
    docker_killed = False
    try:
        import asyncpg

        from taskq import TaskQ

        client = TaskQ(dsn=dsn, schema=schema)
        await client.open()
        worker = _spawn_worker(dsn, schema)
        wait_worker_ready(worker)

        # The in-flight population: enough concurrent work that the kill
        # lands mid-execution (observed via the effects ledger, not guessed).
        handles = [
            await client.enqueue(kill9_starting, Kill9Payload(sleep=20.0), tags=[_TAG])
            for _ in range(4)
        ]
        await _wait_started(dsn, schema, handles[0].job_id)

        # The mid-transaction shape: an open transaction with a durable
        # write the kill must ROLL BACK.
        tx_conn = await asyncpg.connect(dsn)
        tx = tx_conn.transaction()
        await tx.start()
        await tx_conn.execute(
            f'INSERT INTO "{schema}".sys_effects (job_id, attempt, actor, kind) '
            "VALUES ($1, 99, 'kill9-test', 'uncommitted')",
            handles[0].job_id,
        )

        # The dirty death.
        await asyncio.to_thread(_docker_kill, kill9_chaos_pg.container_id)
        docker_killed = True

        # The worker isolates and exits (the heartbeat-failure contract):
        # 3 failed beats trigger isolate_self, its bounded reconnect fails
        # against the dead PG, the ladder runs.
        deadline = time.monotonic() + _WORKER_EXIT_CAP_S
        while time.monotonic() < deadline:
            if worker.poll() is not None:
                break
            await asyncio.sleep(0.25)
        else:
            raise AssertionError(
                f"the worker did not isolate and exit within {_WORKER_EXIT_CAP_S}s "
                "of the PG kill - a worker that outlives its database"
            )

        # Restart on the same container: crash recovery (WAL redo) runs.
        await asyncio.to_thread(_docker_start, kill9_chaos_pg.container_id)
        docker_killed = False  # the container is up again; teardown can stop it
        await _wait_pg_answers(dsn, _PG_RESTART_PROBE_CAP_S)

        # Convergence 1 - NO HALF-APPLIED MIGRATION STATE: a re-run of the
        # migration runner applies NOTHING (every shipped file is in the
        # ledger with its checksum intact; a drifted or half-applied
        # ledger raises SystemExit here).
        pending = await apply_pending_locked(dsn, schema=schema)
        assert pending == [], f"the crash left {len(pending)} migration(s) unapplied: {pending}"

        # Convergence 2 - the uncommitted transaction is GONE.
        with contextlib.suppress(Exception):
            await tx.rollback()  # the connection died with the kill; the rollback is PG's own
        ghost = await _count_effects(dsn, schema, kind="uncommitted")
        assert ghost == 0, f"{ghost} row(s) from the killed transaction SURVIVED crash recovery"

        # Convergence 3 - the fleet drains: a replacement reclaims the
        # in-flight rows and fresh enqueues run to success.
        replacement = _spawn_worker(dsn, schema)
        try:
            wait_worker_ready(replacement)
            trailing = [
                await client.enqueue(kill9_slow, Kill9Payload(sleep=0.2), tags=[_TAG])
                for _ in range(3)
            ]
            _ = trailing
            deadline = time.monotonic() + _SETTLE_CAP_S
            remaining = -1
            while time.monotonic() < deadline:
                remaining = await _count_non_terminal(dsn, schema)
                if remaining == 0:
                    break
                await asyncio.sleep(0.5)
            assert remaining == 0, (
                f"{remaining} tagged job(s) never reached terminal after the "
                "crash recovery - the queue did not drain"
            )
            violations = await conservation_violations_conn(dsn, schema)
            assert not violations, (
                "the conservation counter does not balance across jobs + events "
                "+ attempts + archive after the crash recovery:\n" + "\n".join(violations)
            )
            conn = await asyncpg.connect(dsn)
            try:
                await assert_effects_balance(conn, schema, _TAG)
            finally:
                await conn.close()
        finally:
            reap(replacement)
    finally:
        if worker is not None:
            reap(worker)
        if tx_conn is not None:
            with contextlib.suppress(Exception):
                await tx_conn.close()
        if client is not None:
            with contextlib.suppress(Exception):
                await client.close()
        if docker_killed:
            # The fixture's teardown (and the schema drop) need PG up;
            # a test that failed mid-outage still unwinds cleanly.
            with contextlib.suppress(Exception):
                await asyncio.to_thread(_docker_start, kill9_chaos_pg.container_id)


def _spawn_worker(pg_dsn: str, schema: str) -> WorkerProc:
    """The tier's standard worker, env-configured for the chaos PG (the
    campaign's shortened leader lease so the replacement's leadership
    handover is bounded)."""
    import os
    import subprocess
    import sys

    from taskq.testing.health import unique_health_sock_path
    from tests.system_e2e._harness import (
        _BASE_ENV,  # pyright: ignore[reportPrivateUsage]  # Why: the fleet env is the tier's shared constant.
    )

    sock_path = unique_health_sock_path("syse2e-kill9pg")
    env = {**os.environ, **_BASE_ENV}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEALTH_SOCKET_PATH": sock_path,
            "TASKQ_LEADER_LEASE": "6.0",
        }
    )
    proc: subprocess.Popen[bytes] = (
        subprocess.Popen(  # Why: fixed argv, no shell, this interpreter, project-owned module.
            [sys.executable, "-m", "tests.system_e2e._worker_entry"],
            env=env,
            cwd=os.environ.get("TASKQ_REPO_ROOT", os.getcwd()),
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
    )
    return WorkerProc(proc=proc, sock_path=sock_path)


async def _wait_started(dsn: str, schema: str, job_id: object) -> None:
    """Block until a body's ``start`` effect is observable - the readiness
    signal proving executions are in flight at the kill."""
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        n = await _count_effects(dsn, schema, job_id=job_id, kind="start")
        if n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"no body was observed in flight before the kill (job {job_id})")


async def _count_effects(dsn: str, schema: str, *, kind: str, job_id: object | None = None) -> int:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        if job_id is None:
            n = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".sys_effects WHERE kind = $1', kind
            )
        else:
            n = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".sys_effects WHERE kind = $1 AND job_id = $2',
                kind,
                job_id,
            )
        return int(n or 0)
    finally:
        await conn.close()


async def _count_non_terminal(dsn: str, schema: str) -> int:
    import asyncpg

    from taskq.backend.statemachine import TERMINAL_STATUSES

    conn = await asyncpg.connect(dsn)
    try:
        n = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status::text != ALL($2::text[])",
            _TAG,
            list(TERMINAL_STATUSES),
        )
        return int(n or 0)
    finally:
        await conn.close()


async def conservation_violations_conn(dsn: str, schema: str) -> list[str]:
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        return await conservation_violations(conn, schema, _TAG)
    finally:
        await conn.close()
