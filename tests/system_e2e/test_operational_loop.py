"""The operational loop, end to end: DEPLOY → OBSERVE → ACT → RECOVER.

This is the operator's shift, asserted. The family proves the loop works
as a WHOLE on both storage modes - the deployment surfaces, the
observability surfaces, the operator actions, the crash recovery - and
that every surface TELLS THE TRUTH about a fleet the test seeded itself.
Nothing here is a vacuous 200-check: every read is reconciled against the
seeded population (depth, wait percentiles, the cron ledger's fires, the
Prometheus exposition's gauges), and every action is verified in the
ledger (``admin_audit``, ``job_events``, the job row) AND rendered back
on the surface it claims to serve.

The four phases, per test:

1. **DEPLOY** - the schema migrated through the REAL deploy step (the
   ``taskq migrate status`` CLI; on the timescale mode the conversion
   lands with ``taskq migrate up`` and the flag on), two workers spawned
   through the real bootstrap, ``taskq health live/ready`` answering as
   exec probes, the leader elected, the standalone admin UI (``taskq ui
   serve``) showing the fleet on ``/admin/workers`` and the elected
   leader on ``/admin/leader``.
2. **OBSERVE** - a realistic traffic mix (fast jobs, a per-tenant
   rate-limited actor, a running pair the scenario OWNS (the
   ``sys_hang`` deadline workload, running until the operator acts - a
   durable premise no read sweep can outrun), an every-minute cron
   schedule), then the operator's read sweep: the admin pages' rendered
   values, the ``taskq.insights`` SQL layer, and the workers' Prometheus
   exposition (``taskq health metrics``) must all agree with the seed.
3. **ACT** - cancel a RUNNING job through the embedded admin router
   (``create_router`` + a real ``PostgresBackend``: the admin-ui.md
   embedding contract) and its pair through the CLI (``taskq job
   cancel`` - the runbook's surface), retry a failed job (``taskq job
   retry``), move an actor's queue (``taskq actor-config move-queue``),
   drain an actor (``taskq actor-config set --max-concurrent 0``, the
   documented drain mode). Each action lands in the ledger and renders
   back.
4. **RECOVER** - ``kill -9`` a worker mid-job: the surviving fleet
   reclaims and re-runs the work, the stale worker row is cleaned, no
   lease is left stuck, and the event trail shows the reclaim's second
   attempt.

The loop runs TWICE - plain Postgres and the TimescaleDB hypertable mode
(``TASKQ_TIMESCALEDB_HYPERTABLES=true``, the admin-on-hypertables
precedent) - from the SAME body, so no phase can silently depend on a
vanilla-only shape.

One deployment gap is PINNED where the loop hits it: ``taskq ui serve``
never configures a Backend (``_ui_serve`` passes none, and
``create_router(backend=None)`` keeps None), so the admin mutation routes
answer 503 from the standalone UI process even with
``TASKQ_ADMIN_ACTIONS_ENABLED=true`` - though admin-ui.md documents the
no-backend case as "the router builds its own ``PostgresBackend`` from
``pg_pool``/``schema``". The loop pins that 503 with the gap named, and
drives the mutations through the surfaces that do work (the CLI and the
embedded router).
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from a fixture the settings boundary validated, and every value is $-bound.

from __future__ import annotations

import asyncio
import json
import math
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, NamedTuple

import httpx
import pytest
import pytest_asyncio

pytest.importorskip("fastapi", reason="requires taskq[fastapi]: the loop drives the admin router")

import asyncpg  # Why: importorskip guards the optional extra first.
from fastapi import FastAPI

from taskq import TaskQ
from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.insights import (
    fetch_actor_backlog,
    fetch_cron_ledger,
    fetch_drain_estimates,
    fetch_queue_imbalance,
    fetch_wait_distribution,
)
from taskq.testing.assertions import plain_cli_output
from taskq.web.admin import create_router, setup_admin_state
from tests.system_e2e._harness import (
    BOOT_READY_BOUND_S,
    DEPLOYMENT_CANCELLATION_GRACE_S,
    DEPLOYMENT_CLEANUP_GRACE_S,
    DEPLOYMENT_HEARTBEAT_INTERVAL_S,
    DEPLOYMENT_LOCK_LEASE_S,
    DEPLOYMENT_TERMINATION_GRACE_S,
    SWEEP_INTERVAL_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
    spawn_joined_worker,
)
from tests.system_e2e._invariants import (
    assert_balanced,
    assert_effects_balance,
    delete_tagged,
)
from tests.system_e2e.actors import (
    RatedPayload,
    SysPayload,
    sys_cron,
    sys_drain,
    sys_fast,
    sys_hang,
    sys_mover,
    sys_rated,
    sys_retry_me,
    sys_slow,
)

if TYPE_CHECKING:
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "ops-loop"
#: The queues the loop's workers subscribe: the tier's own queue plus the
#: move-queue target and the drain actor's queue.
_LOOP_QUEUES = "system_e2e,ops_moved,ops_drain"
_MOVE_TARGET_QUEUE = "ops_moved"
_CRON_EXPR = "* * * * *"
_CRON_NAME = "ops-loop-cron"
_CANCEL_REASON = "ops-loop cancel: the operator stopped this one"
#: The harness's grace knobs for this fleet (the deployment-shaped pair
#: from _harness - the reaping window and the reclaim's cancel carve-out
#: read them, so the limbo contract here reads the same numbers).
_CANCELLATION_GRACE_S = DEPLOYMENT_CANCELLATION_GRACE_S
_CLEANUP_GRACE_S = DEPLOYMENT_CLEANUP_GRACE_S
#: The recovery workload's simulated runtime: long enough that the
#: kill -9 lands mid-run (the RECOVER phase's reclaim has a body to
#: catch), short enough to keep the loop under its timeout.
_SLOW_SLEEP = 40.0

# ── The derived bounds (#651's doctrine, in-code) ────────────────────────
# Every deadline below derives from the fleet's own knobs - the tier's
# dispatch cadence and the deployment-shaped pair this file's workers boot
# with - multiplied by the tier's load stretch. A bare wall-clock number
# is a bet against the runner; a derived bound moves with the fleet and
# the runner instead of reding a healthy loop on a starved box.

#: The dispatch cadence the loop's workers run (the harness's sweep tick
#: plus the poll floor every wait budgets).
_POLL_FLOOR_S = 1.0
_CLAIM_CYCLE_S = SWEEP_INTERVAL_S + _POLL_FLOOR_S

#: One job's claim → terminal chain on this fleet: a boot-readiness
#: ceiling (the slowest legal first beat), one claim cycle, and the lock
#: lease the claim rides, stretched by the tier's load factor. The
#: fast-mix, rated-mix and neighbor settles all wait on THIS bound; a
#: fleet that never serves still reds inside it.
_SETTLE_BOUND_S = (
    BOOT_READY_BOUND_S + _CLAIM_CYCLE_S + DEPLOYMENT_LOCK_LEASE_S
) * TIER_LOAD_STRETCH

#: The operator-cancel bound: the cancel ladder's full arithmetic - the
#: worker observes the phase-1 flag on its heartbeat poll (three beats of
#: slack), the cooperative grace and the cleanup grace lapse, the force
#: cancel interrupts the body, the terminal write lands - all stretched.
#: A cancel that never wins reds here (the escalation ladder's abandoned,
#: not cancelled, names the broken mechanism in the failure).
_CANCEL_LAND_BOUND_S = (
    DEPLOYMENT_HEARTBEAT_INTERVAL_S * 3
    + DEPLOYMENT_CANCELLATION_GRACE_S
    + DEPLOYMENT_CLEANUP_GRACE_S
) * TIER_LOAD_STRETCH

#: The every-minute cron's first-fire bound: the next minute boundary
#: (60s) plus the settle bound above.
_CRON_FIRE_BOUND_S = 60.0 + _SETTLE_BOUND_S

#: The RECOVER phase's bounds: the killed worker's lease must lapse (the
#: lock lease the claim rode) before the leader's sweep requeues, one or
#: two dispatch cycles move the work, and the survivor's re-run of the
#: body the kill caught adds the recovery mix's own runtime - all
#: stretched by the tier's load factor. Measured under the contended
#: shape (four hogs, -n 2 co-tenancy), the kill-to-reclaimed settle runs
#: ~92s against the ~70s nominal arithmetic; the bare 30/60/120s these
#: replace were 1.3-2x bets on the runner, not the tier's 2x stretch.
_RECLAIM_LAND_S = (DEPLOYMENT_LOCK_LEASE_S + 2 * _CLAIM_CYCLE_S) * TIER_LOAD_STRETCH
_RECLAIM_SETTLE_S = (DEPLOYMENT_LOCK_LEASE_S + 2 * _CLAIM_CYCLE_S + _SLOW_SLEEP) * TIER_LOAD_STRETCH

#: The exec probes' retry budget (the health live/ready loop and the
#: Prometheus probe). The ATTEMPTS are the test-side lever: the CLI's
#: per-request bound is the product's hardcoded 2.0s (``Final`` — not
#: ours to move), so under box load the probe's tolerance is the ATTEMPT
#: COUNT. Four attempts ≈ 12s of tolerance lost the DEPLOY-phase probes
#: on the cross-contended runner (the review's contended runs: F, F, P
#: with the worker logs clean — the box, not the fleet; the same
#: 1.3-2x runner-bet the recover-phase bounds cured, left un-cured
#: here). The attempts price off the tier doctrine: the base 4 stretched
#: by the tier's load factor, ceil'd — 8 attempts ≈ 24s of tolerance,
#: the probe's window moving with the fleet's own weather knob.
_PROBE_ATTEMPTS = 4 * math.ceil(TIER_LOAD_STRETCH)

#: The drain-cap's exposure window: how long the held backlog is given
#: the chance to (wrongly) run. A broken ``max_concurrent = 0`` leaks a
#: claim within one dispatch cycle; the window is two, stretched - and
#: the assertion after it is STATE (pending rows, zero running), not a
#: clock, so the exposure's length carries no tooth of its own.
_DRAIN_EXPOSURE_S = 2 * _CLAIM_CYCLE_S * TIER_LOAD_STRETCH
_TIMESCALE_IMAGE = (
    os.environ.get("TASKQ_TEST_TIMESCALEDB_IMAGE") or "timescale/timescaledb:2.30.1-pg18"
)

_EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".sys_effects (
    job_id  UUID NOT NULL,
    attempt INT NOT NULL,
    actor   TEXT NOT NULL,
    kind    TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""

# The fleet env: the harness's defaults with the loop's cadence - the
# DEPLOYMENT-shaped pair from the tier's own constants (the rationale
# lives on the constants; the reaping window at this beat is 12s, the
# kill9 reclaim still lands well inside the recovery poll, and the
# fleet-wide surfaces (the 30s liveness window) see the same healthy
# fleet).
_WORKER_ENV = {
    "TASKQ_QUEUES": _LOOP_QUEUES,
    "TASKQ_HEARTBEAT_INTERVAL": str(DEPLOYMENT_HEARTBEAT_INTERVAL_S),
    "TASKQ_LOCK_LEASE": str(DEPLOYMENT_LOCK_LEASE_S),
    "TASKQ_CANCELLATION_GRACE_PERIOD": str(DEPLOYMENT_CANCELLATION_GRACE_S),
    "TASKQ_CLEANUP_GRACE_PERIOD": str(DEPLOYMENT_CLEANUP_GRACE_S),
    # The shutdown budget the graces imply (grace + cleanup + 5 <= 25).
    "TASKQ_TERMINATION_GRACE_PERIOD": str(DEPLOYMENT_TERMINATION_GRACE_S),
}
# The standalone admin deployment: actions enabled (the operator's choice
# the checklist documents), http-dev so the CSRF cookie works over plain
# HTTP, dev environment so the unauthenticated UI serves.
_UI_ENV = {
    "TASKQ_ADMIN_HOST": "127.0.0.1",
    "TASKQ_ADMIN_UI_SECURE_COOKIES": "false",
    "TASKQ_ADMIN_ACTIONS_ENABLED": "true",
    "TASKQ_ENVIRONMENT": "dev",
}


# ── Small process/CLI/HTTP helpers ───────────────────────────────────────


def _repo_root() -> str:
    return os.environ.get("TASKQ_REPO_ROOT", os.getcwd())


def run_cli(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[bytes]:
    """Run the REAL ``taskq`` CLI the way an operator's script would."""
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell, this interpreter, project-owned module.
        [sys.executable, "-m", "taskq", *args],
        env={**os.environ, **env},
        cwd=_repo_root(),
        capture_output=True,
        timeout=90,
        check=False,
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _poll(
    predicate: Any,
    timeout: float,  # noqa: ASYNC109  # Why: the wait budget, not a missing asyncio.timeout pattern - the predicate, not the clock, decides.
    desc: str,
    interval: float = 0.25,
) -> None:
    """Wait until *predicate* holds, or fail naming what never came true."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {desc}")


def _parse_metrics(text: str) -> dict[str, float]:
    """A Prometheus text exposition into {name: value} (last sample wins)."""
    values: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, raw = line.rpartition(" ")
        try:
            values[name] = float(raw)
        except ValueError:
            continue
    return values


async def _job_succeeded(conn: asyncpg.Connection, schema: str, job_id: Any) -> bool:
    """The durable receipt of one job's terminal success (the ledger's row)."""
    row = await conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE id = $1 AND status = 'succeeded'",
        job_id,
    )
    return int(row) == 1


async def _worker_metrics(sock_path: str) -> dict[str, float]:
    """One worker's /metrics exposition through the operator's exec probe.

    ``taskq health metrics`` is the deployment.md probe surface: it dials
    the worker's health socket and prints the exposition body. Its exit
    code IS the probe's verdict; its stdout is the truth asserted here.
    Retried: the CLI's own request bound is 2.0s, and a probe against a
    just-booted worker on a loaded box can lose one request to the
    bootstrap's work - the way a Kubernetes exec probe's
    failureThreshold absorbs a slow first tick. The attempt budget is
    _PROBE_ATTEMPTS (the tier doctrine's stretch of the base 4): the
    cross-contended runner's loss (the review's F, F, P record, worker
    logs clean) ate the base budget's ~12s; the stretched budget's ~24s
    is the tolerance that survives it.
    """
    proc: subprocess.CompletedProcess[bytes] | None = None
    for _attempt in range(_PROBE_ATTEMPTS):
        proc = await asyncio.to_thread(
            run_cli,
            ["health", "metrics"],
            {"TASKQ_HEALTH_SOCKET_PATH": sock_path},
        )
        if proc.returncode == 0:
            break
        await asyncio.sleep(1.0)
    assert proc is not None and proc.returncode == 0, (
        f"the Prometheus probe failed against {sock_path}: "
        f"{proc.stderr.decode(errors='replace') if proc else 'no attempt'}"
    )
    return _parse_metrics(proc.stdout.decode(errors="replace"))


# ── The standalone admin deployment (``taskq ui serve``) ─────────────────


class AdminUI:
    """The REAL ``taskq ui serve`` deployment, driven over HTTP.

    A subprocess running the production admin entrypoint (not a test
    double) plus an httpx client pointed at it: the standalone admin
    sidecar of a compose stack.
    """

    def __init__(self, client: httpx.AsyncClient, proc: subprocess.Popen[bytes]) -> None:
        self._client = client
        self.proc = proc

    async def get(self, path: str, **params: str) -> httpx.Response:
        return await self._client.get(path, params=params or None)

    async def post(self, path: str, fields: dict[str, str]) -> httpx.Response:
        """The synchronizer-token CSRF handshake: a GET arms the cookie,
        the POST carries the token (the web_admin tier's idiom)."""
        token = self._client.cookies.get("taskq_csrf_token", "")
        if not token:
            raise AssertionError(f"no taskq_csrf_token cookie before POST {path}")
        return await self._client.post(
            path,
            data={**fields, "csrf_token": token},
            follow_redirects=False,
        )


def _spawn_admin_process(port: int, dsn: str, schema: str) -> subprocess.Popen[bytes]:
    """The blocking Popen the async spawn hands to a worker thread."""
    env = {
        **os.environ,
        **_UI_ENV,
        "TASKQ_PG_DSN": dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_ADMIN_PORT": str(port),
    }
    return subprocess.Popen(  # noqa: S603  # Why: fixed argv, no shell, this interpreter, project-owned module.
        [sys.executable, "-m", "taskq", "ui", "serve", "--host", "127.0.0.1", "--port", str(port)],
        env=env,
        cwd=_repo_root(),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )


async def spawn_admin_ui(dsn: str, schema: str) -> tuple[AdminUI, httpx.AsyncClient]:
    """Stand the standalone admin deployment: the real ``taskq ui serve``.

    Returns the driver and the raw client (the client owns the cookie
    jar and the teardown).
    """
    port = _free_port()
    proc = await asyncio.to_thread(_spawn_admin_process, port, dsn, schema)
    client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=10.0)

    async def _ready() -> bool:
        if proc.poll() is not None:
            _stdout, stderr = proc.communicate(timeout=5)
            raise RuntimeError(
                f"taskq ui serve exited rc={proc.returncode} during startup: "
                f"{stderr.decode(errors='replace')}"
            )
        try:
            resp = await client.get("/admin/workers")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200

    await _poll(_ready, BOOT_READY_BOUND_S, "taskq ui serve readiness")
    return AdminUI(client, proc), client


async def stop_admin_ui(ui: AdminUI, client: httpx.AsyncClient) -> None:
    await client.aclose()
    if ui.proc.poll() is None:
        ui.proc.terminate()
    try:
        ui.proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        ui.proc.kill()
        ui.proc.wait(timeout=5)


# ── The embedded admin router (the admin-ui.md in-app deployment) ────────

# The mutation routes need a Backend on app.state; the admin-ui.md embed
# contract passes one explicitly ("pass an existing Backend"). The deps
# double is the web_admin tier's idiom (test_admin_on_hypertables.py):
# the protocol declares every field the routes read.


class _BackendSettings:
    schema_name: str

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _BackendDeps:
    settings: _BackendSettings
    worker_pool: Any
    heartbeat_pool: Any
    dispatcher_pool: Any = None

    def __init__(self, schema: str, pool: Any) -> None:
        self.settings = _BackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


class EmbeddedAdmin:
    """The admin router mounted in a host app WITH a real backend - the
    embedding deployment admin-ui.md documents. Served in-process over
    httpx's ASGI transport (the tests/web_admin idiom): the routes, the
    CSRF handshake, the audit writes and the page renders are all real."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def get(self, path: str) -> httpx.Response:
        return await self._client.get(path, follow_redirects=False)

    async def post(self, path: str, fields: dict[str, str]) -> httpx.Response:
        token = self._client.cookies.get("taskq_csrf_token", "")
        if not token:
            raise AssertionError(f"no taskq_csrf_token cookie before POST {path}")
        return await self._client.post(
            path,
            data={**fields, "csrf_token": token},
            follow_redirects=False,
        )


async def open_embedded_admin(dsn: str, schema: str) -> tuple[EmbeddedAdmin, asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    assert pool is not None
    backend = PostgresBackend(
        _BackendDeps(schema, pool),
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )
    app = FastAPI()
    bundle = create_router(pool, schema=schema, backend=backend, base_path="/embed")
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/embed")
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )
    return EmbeddedAdmin(client), pool


# ── Storage-mode environments ────────────────────────────────────────────


class LoopWorld(NamedTuple):
    """One storage mode's deployment: DSN + migrated schema + ledger conn."""

    mode: str
    dsn: str
    schema: str
    conn: asyncpg.Connection


async def _open_world(mode: str, dsn: str, schema: str) -> LoopWorld:
    conn = await asyncpg.connect(dsn)
    await conn.execute(_EFFECTS_DDL.format(schema=schema))
    return LoopWorld(mode=mode, dsn=dsn, schema=schema, conn=conn)


@pytest_asyncio.fixture(params=["plain", "timescale"], ids=["plain-pg", "timescale"])
async def loop_env(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
) -> Any:
    """The loop's two storage modes.

    plain: the system tier's own module schema on the shared PG container
    (migrated by the tier fixture).

    timescale: a dedicated TimescaleDB container whose schema is migrated
    through the REAL deploy step - the ``taskq migrate up`` CLI with
    ``TASKQ_TIMESCALEDB_HYPERTABLES=true`` - so the conversion lands the
    way deployment.md's checklist demands (three hypertables, three
    retention policies).
    """
    if request.param == "plain":
        world = await _open_world("plain", pg_dsn, module_pg_schema.schema_name)
        yield world
        await world.conn.close()
        return

    from testcontainers.community.postgres import PostgresContainer

    from taskq.testing._shared_containers import creator_labels

    with PostgresContainer(
        image=_TIMESCALE_IMAGE,
        username="taskq",
        password="taskq",
        dbname="taskq",
    ).with_kwargs(labels=creator_labels()) as container:
        ts_dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        ts_schema = "opsloop_" + new_uuid().hex[:12]
        up = await asyncio.to_thread(
            run_cli,
            ["migrate", "up"],
            {
                "TASKQ_PG_DSN": ts_dsn,
                "TASKQ_SCHEMA_NAME": ts_schema,
                "TASKQ_TIMESCALEDB_HYPERTABLES": "true",
            },
        )
        assert up.returncode == 0, (
            f"the deploy step failed on the timescale mode: "
            f"{up.stdout.decode(errors='replace')} {up.stderr.decode(errors='replace')}"
        )
        # The checklist's storage-engine row, second half: "at least one
        # chunk exists once events have landed - SELECT
        # show_chunks('<schema>'.job_events)". The loop lands the FIRST
        # event at deploy time (a probe row that is removed in the same
        # transaction, so only the chunk it forces stays): the first
        # chunk-creation DDL then happens BEFORE the fleet boots, not as
        # a surprise under the first event burst.
        probe = await asyncpg.connect(ts_dsn)
        try:
            probe_job_id = new_uuid()
            async with probe.transaction():
                await probe.execute(
                    f"""
                    INSERT INTO "{ts_schema}".jobs
                        (id, actor, queue, payload, max_attempts, retry_kind, status, finished_at)
                    VALUES ($1, 'deploy_chunk_probe', 'deploy_probe', '{{}}'::jsonb,
                            1, 'transient', 'crashed', clock_timestamp())
                    """,
                    probe_job_id,
                )
                await probe.execute(
                    f"""
                    INSERT INTO "{ts_schema}".job_events (job_id, kind, detail)
                    VALUES ($1, 'state_change', '{{}}'::jsonb)
                    """,
                    probe_job_id,
                )
                await probe.execute(
                    f'DELETE FROM "{ts_schema}".job_events WHERE job_id = $1', probe_job_id
                )
                await probe.execute(f'DELETE FROM "{ts_schema}".jobs WHERE id = $1', probe_job_id)
            chunks = await probe.fetchval(
                f"SELECT count(*) FROM show_chunks('\"{ts_schema}\".job_events')"
            )
        finally:
            await probe.close()
        assert chunks >= 1, f"the job_events hypertable has no chunk after the deploy: {chunks}"
        world = await _open_world("timescale", ts_dsn, ts_schema)
        yield world
        await world.conn.close()


# ── The loop ─────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _loop_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The test process's own admin wiring runs as the dev deployment the
    loop describes: the embedded router's ``create_router`` reads THESE
    env vars (the fail-closed auth check and the CSRF cookie policy), and
    spawned CLI/admin subprocesses inherit them."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")


async def _spawn_fleet_worker(
    dsn: str, schema: str, conn: asyncpg.Connection, tag: str
) -> WorkerProc:
    """One fleet replica, held to the operator's own deployment standard.

    The tier's shared spawn standard (``_harness.spawn_joined_worker``):
    a registered row whose heartbeat advances, else reap and respawn —
    the ghost-replica accommodation every multi-replica scenario shares.
    """
    return await spawn_joined_worker(conn, dsn, schema, tag, extra_env=_WORKER_ENV)


@pytest.mark.timeout(900)
async def test_operational_loop_deploy_observe_act_recover(loop_env: Any) -> None:
    """The whole loop on one storage mode: the operator's shift, asserted."""
    world: LoopWorld = loop_env
    conn = world.conn
    dsn = world.dsn
    schema = world.schema
    mode = world.mode
    cli_env = {"TASKQ_PG_DSN": dsn, "TASKQ_SCHEMA_NAME": schema}

    async with TaskQ(dsn=dsn, schema=schema) as client:
        # ══ 1. DEPLOY ══════════════════════════════════════════════════
        # The operator's post-deploy checklist (deployment.md), executed
        # as assertions.

        # Schemas migrated: the REAL migrate status answers, and every
        # migration line carries the applied marker.
        status = await asyncio.to_thread(run_cli, ["migrate", "status"], cli_env)
        assert status.returncode == 0, (
            f"taskq migrate status failed: {status.stderr.decode(errors='replace')}"
        )
        status_out = status.stdout.decode(errors="replace")
        # The subprocess inherits the runner env (GITHUB_ACTIONS et al.
        # colorize typer's rich-rendered surfaces); match the plain bytes.
        plain_status = plain_cli_output(status_out)
        assert f"schema: {schema}" in plain_status, status_out
        assert "applied:" in plain_status, status_out
        assert "[ ]" not in plain_status, (
            f"the schema has unapplied migrations at deploy time:\n{status_out}"
        )

        if mode == "timescale":
            # The checklist's storage-engine row: three hypertables, three
            # retention policies, landed by the enabling deploy.
            hypertables = await conn.fetch(
                "SELECT table_name FROM _timescaledb_catalog.hypertable WHERE schema_name = $1",
                schema,
            )
            assert {r["table_name"] for r in hypertables} == {
                "jobs_archive",
                "job_attempts_archive",
                "job_events",
            }, f"the hypertable conversion did not land: {[dict(r) for r in hypertables]}"
            policies = await conn.fetchval(
                "SELECT count(*) FROM timescaledb_information.jobs "
                "WHERE hypertable_schema = $1 AND proc_name = 'policy_retention'",
                schema,
            )
            assert policies == 3, f"expected three retention policies, found {policies}"

        # The fleet: two workers through the real bootstrap, each held to
        # the joined-fleet standard (registered row, heartbeating).
        worker_a = await _spawn_fleet_worker(dsn, schema, conn, "loop-a")
        worker_b = await _spawn_fleet_worker(dsn, schema, conn, "loop-b")
        ui: AdminUI | None = None
        ui_client: httpx.AsyncClient | None = None
        embedded_pool: asyncpg.Pool | None = None
        try:
            # Health probes: the exec probes the manifest wires, live AND
            # ready, per replica. Retried, the way an orchestrator's
            # failureThreshold absorbs a probe that loses its request to
            # a busy bootstrap tick (the CLI's own request bound is 2s).
            # The attempt budget is _PROBE_ATTEMPTS (the tier doctrine's
            # stretch of the base 4, the test-side lever - the 2s request
            # bound is the product's): the cross-contended runner's loss
            # (the review's F, F, P record, worker logs clean) ate the
            # base budget's ~12s; the stretched budget's ~24s survives it.
            for worker in (worker_a, worker_b):
                for probe in ("live", "ready"):
                    health: subprocess.CompletedProcess[bytes] | None = None
                    for _attempt in range(_PROBE_ATTEMPTS):
                        health = await asyncio.to_thread(
                            run_cli,
                            ["health", probe],
                            {"TASKQ_HEALTH_SOCKET_PATH": worker.sock_path},
                        )
                        if health.returncode == 0:
                            break
                        await asyncio.sleep(1.0)
                    assert health is not None and health.returncode == 0, (
                        f"taskq health {probe} failed for worker {worker.proc.pid}: "
                        f"{health.stderr.decode(errors='replace') if health else 'no attempt'}"
                    )

            # Worker registration: both replicas heartbeating. The wait's
            # bound is the harness's own boot-readiness ceiling stretched
            # by the tier's load factor - registration is the boot's last
            # DB step, so the bound that gates readiness gates the row.
            pids = {worker_a.proc.pid, worker_b.proc.pid}

            async def _fleet_registered() -> bool:
                rows = await conn.fetch(f'SELECT pid FROM "{schema}".workers')
                return pids <= {r["pid"] for r in rows}

            await _poll(
                _fleet_registered,
                BOOT_READY_BOUND_S * TIER_LOAD_STRETCH,
                "both workers registered in the fleet table",
            )

            # The leader elected, and it is one of OUR workers.
            leader_pid_box: list[int] = []

            async def _leader_elected() -> bool:
                row = await conn.fetchrow(
                    f"""
                    SELECT w.pid FROM "{schema}".maintenance_leader ml
                    JOIN "{schema}".workers w ON w.id = ml.worker_id
                    """
                )
                if row is None:
                    return False
                leader_pid_box.clear()
                leader_pid_box.append(int(row["pid"]))
                return leader_pid_box[0] in pids

            await _poll(
                _leader_elected,
                BOOT_READY_BOUND_S * TIER_LOAD_STRETCH,
                "a leader elected from the deployed fleet",
            )
            leader_pid = leader_pid_box[0]

            # The admin UI serves the fleet: the workers page shows BOTH
            # replicas, and the leader page names the elected one.
            ui, ui_client = await spawn_admin_ui(dsn, schema)
            workers_page = await ui.get("/admin/workers")
            assert workers_page.status_code == 200
            for pid in pids:
                assert str(pid) in workers_page.text, (
                    f"the workers page does not show the deployed replica pid {pid}"
                )
            leader_page = await ui.get("/admin/leader")
            assert leader_page.status_code == 200
            assert str(leader_pid) in leader_page.text, (
                "the leader page does not name the elected leader's pid"
            )

            # ══ 2. OBSERVE ═════════════════════════════════════════════
            # Seed the traffic mix, then read every operational surface
            # and reconcile it against this seeded truth.

            # The cron schedule goes in FIRST: its every-minute fire
            # chain starts on the next minute boundary while the rest of
            # the mix runs.
            schedule = await client.create_schedule(
                sys_cron, _CRON_EXPR, static_payload={"sleep": 0.0}, name=_CRON_NAME
            )

            fast_jobs = [
                await client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
                for _ in range(6)
            ]
            rated_jobs = [
                await client.enqueue(sys_rated, RatedPayload(tenant="t0"), tags=[_TAG])
                for _ in range(3)
            ]
            # The running pair the OBSERVE surfaces must read: the hold
            # actor runs UNTIL the operator's cancel lands, so "two rows
            # running" is a state the scenario owns for as long as the
            # read sweep needs - not a 40s sleep the sweep can outrun on a
            # loaded runner (the timing premise the #651 doctrine
            # retires: the seeded truth is durable, the clock is not a
            # participant).
            hold_jobs = [
                await client.enqueue(sys_hang, SysPayload(), tags=[_TAG]) for _ in range(2)
            ]
            retry_me = await client.enqueue(sys_retry_me, SysPayload(), tags=[_TAG])
            mover_jobs = [
                await client.enqueue(sys_mover, SysPayload(sleep=0.2), tags=[_TAG])
                for _ in range(3)
            ]
            drain_first_wave = [
                await client.enqueue(sys_drain, SysPayload(sleep=0.3), tags=[_TAG])
                for _ in range(4)
            ]

            async def _actor_succeeded(actor: str, n: int) -> bool:
                row = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM "{schema}".jobs
                    WHERE tags @> ARRAY[$1::text] AND actor = $2 AND status = 'succeeded'
                    """,
                    _TAG,
                    actor,
                )
                return int(row) >= n

            async def _actor_running(actor: str, n: int) -> bool:
                row = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM "{schema}".jobs
                    WHERE tags @> ARRAY[$1::text] AND actor = $2 AND status = 'running'
                    """,
                    _TAG,
                    actor,
                )
                return int(row) >= n

            # The fast mix completes; the hold pair runs on.
            try:
                await _poll(
                    lambda: _actor_succeeded("sys_fast", len(fast_jobs)),
                    _SETTLE_BOUND_S,
                    "all seeded fast jobs reached succeeded",
                )
            except AssertionError:
                diag_jobs = await conn.fetch(
                    f"""
                    SELECT actor, status::text AS status, count(*)::int AS n
                    FROM "{schema}".jobs GROUP BY actor, status::text ORDER BY actor
                    """
                )
                diag_workers = await conn.fetch(
                    f"""
                    SELECT pid, last_seen_at,
                        EXTRACT(EPOCH FROM (clock_timestamp() - last_seen_at))::int AS age_s
                    FROM "{schema}".workers
                    """
                )
                raise AssertionError(
                    f"the fast mix never completed. job states: {[dict(r) for r in diag_jobs]} "
                    f"workers: {[dict(r) for r in diag_workers]}"
                ) from None

            # The rate-limited actor: with a 1-token bucket refilling at
            # half a token per second, the SECOND and THIRD concurrent
            # enqueue are denied and re-scheduled - the deferral the
            # insights layer counts.
            await _poll(
                lambda: _actor_succeeded("sys_rated", len(rated_jobs)),
                _SETTLE_BOUND_S,
                "the rate-limited mix drained through the 1-token bucket",
            )
            blocked = await conn.fetchval(
                f"""
                SELECT count(*) FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text] AND actor = 'sys_rated'
                  AND rate_limit_blocked_count > 0
                """,
                _TAG,
            )
            assert int(blocked) >= 1, (
                "no sys_rated job was ever rate-limit denied: the limiter's "
                "deferral never reached the ledger"
            )

            # The running pair is on the surfaces while it runs: the
            # admin count endpoint, the depth CLI, the Prometheus
            # exposition, the insights layer. The premise itself is a
            # DURABLE receipt first (the hold pair is running - the state
            # the scenario owns, waited out on the ledger), then every
            # surface is reconciled against the ledger's count - a
            # surface that lies reds even when the premise holds.
            try:
                await _poll(
                    lambda: _actor_running("sys_hang", len(hold_jobs)),
                    _SETTLE_BOUND_S,
                    "the hold pair reached running",
                )
            except AssertionError:
                diag = await conn.fetch(
                    f"""
                    SELECT actor, status::text AS status, count(*)::int AS n
                    FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]
                    GROUP BY actor, status::text ORDER BY actor
                    """,
                    _TAG,
                )
                raise AssertionError(f"the hold pair never started: {diag}") from None

            running_now = await conn.fetchval(
                f"""
                SELECT count(*) FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text] AND actor = 'sys_hang' AND status = 'running'
                """,
                _TAG,
            )
            running_slow = await ui.get("/admin/jobs/count", actor="sys_hang", status="running")
            assert running_slow.status_code == 200
            assert running_slow.json() == {"count": int(running_now)}, (
                f"the admin count endpoint does not tell the hold pair's truth "
                f"(ledger: {running_now}): {running_slow.json()}"
            )
            assert int(running_now) >= 2, (
                f"the hold pair's running receipt does not hold: {running_now}"
            )

            depth = await asyncio.to_thread(run_cli, ["queues", "depth"], cli_env)
            assert depth.returncode == 0
            assert "system_e2e" in plain_cli_output(depth.stdout.decode(errors="replace")), (
                "the queues depth command does not list the fleet's own queue"
            )

            # Prometheus: the fleet's exposition sums to the seeded truth,
            # re-read together with the DB count (a cron fire or a straggler
            # between the two reads is a skew, not a lie - retry the pair).
            metrics_a: dict[str, float] = {}
            metrics_b: dict[str, float] = {}
            active_sum = -1
            leader_gauge_sum = -1
            running_now = -1
            for _attempt in range(5):
                metrics_a = await _worker_metrics(worker_a.sock_path)
                metrics_b = await _worker_metrics(worker_b.sock_path)
                running_now = await conn.fetchval(
                    f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'running'"
                )
                active_sum = metrics_a.get("taskq_active_jobs", -1) + metrics_b.get(
                    "taskq_active_jobs", -1
                )
                leader_gauge_sum = metrics_a.get("taskq_is_leader", -1) + metrics_b.get(
                    "taskq_is_leader", -1
                )
                if active_sum == int(running_now) and leader_gauge_sum == 1:
                    break
                await asyncio.sleep(1.0)
            assert active_sum == int(running_now), (
                f"the Prometheus gauges do not tell the running truth: "
                f"sum(taskq_active_jobs)={active_sum} vs {running_now} running rows "
                f"({metrics_a}, {metrics_b})"
            )
            assert leader_gauge_sum == 1, (
                f"exactly one worker must hold the maintenance leader gauge, got "
                f"{leader_gauge_sum}: {metrics_a}, {metrics_b}"
            )
            assert metrics_a.get("taskq_shutdown_phase") == 0.0
            assert metrics_b.get("taskq_shutdown_phase") == 0.0

            # Insights: the SQL layer's reads agree with the seed.
            backlog = await fetch_actor_backlog(conn, schema=schema)
            by_actor = {r["actor"]: r for r in backlog}
            hold_row = by_actor.get("sys_hang")
            assert hold_row is not None and hold_row["running"] == int(running_now), (
                f"the actor backlog does not tell the hold pair's truth "
                f"(ledger: {running_now}): {hold_row}"
            )

            imbalance = await fetch_queue_imbalance(conn, schema=schema)
            e2e_row = next((r for r in imbalance if r["queue"] == "system_e2e"), None)

            async def _fleet_visible() -> bool:
                imbalance_now = await fetch_queue_imbalance(conn, schema=schema)
                row_now = next((r for r in imbalance_now if r["queue"] == "system_e2e"), None)
                return row_now is not None and row_now["live_workers"] == 2

            try:
                await _poll(
                    _fleet_visible,
                    BOOT_READY_BOUND_S * TIER_LOAD_STRETCH,
                    "the imbalance view seeing the deployed fleet",
                )
            except AssertionError:
                diag_workers = await conn.fetch(
                    f"""
                    SELECT pid, last_seen_at,
                        EXTRACT(EPOCH FROM (clock_timestamp() - last_seen_at))::int AS age_s
                    FROM "{schema}".workers
                    """
                )
                diag_activity = await conn.fetch(
                    """
                    SELECT pid, state, wait_event_type, wait_event, query_start,
                        left(query, 60) AS query
                    FROM pg_stat_activity
                    WHERE application_name = $1
                        AND datname = current_database()
                    """,
                    schema,
                )
                raise AssertionError(
                    f"the imbalance view does not see the deployed fleet (last row: "
                    f"{e2e_row}) worker rows: {[dict(r) for r in diag_workers]} "
                    f"proc_a={worker_a.proc.poll()} proc_b={worker_b.proc.poll()} "
                    f"activity: {[dict(r) for r in diag_activity]}"
                ) from None
            imbalance = await fetch_queue_imbalance(conn, schema=schema)
            e2e_row = next((r for r in imbalance if r["queue"] == "system_e2e"), None)
            assert e2e_row is not None and e2e_row["live_workers"] == 2

            waits = await fetch_wait_distribution(conn, schema=schema, window=timedelta(hours=1))
            clean = next((r for r in waits if r["segment"] == "clean"), None)
            assert clean is not None and clean["count"] >= len(fast_jobs), (
                f"the wait distribution's clean segment missed the delivered mix: {waits}"
            )
            assert clean["p50_wait_s"] >= 0, f"an implausible negative p50 wait: {clean}"
            deferred = next((r for r in waits if r["segment"] == "deferred"), None)
            assert deferred is not None and deferred["count"] >= 1, (
                f"the rate-limited deferrals never surfaced as the deferred segment "
                f"the limiter's SLO is read against: {waits}"
            )

            drains = await fetch_drain_estimates(conn, schema=schema, window=timedelta(hours=1))
            e2e_drain = next((r for r in drains if r["queue"] == "system_e2e"), None)
            assert e2e_drain is not None and e2e_drain["has_traffic"] is True, (
                f"the drain estimate does not see the fleet's own traffic: {e2e_drain}"
            )

            # ══ 3. ACT ═════════════════════════════════════════════════
            # Through the real surfaces, each action verified in the
            # ledger AND rendered back. The running-job cancel goes
            # FIRST: it acts on the hold pair, which runs until the
            # operator acts (the premise is the scenario's own durable
            # state, not a sleep the read sweep could have outrun); the
            # cron wait trails the actions (its minute boundary is
            # already ticking while the operator works).

            embedded, embedded_pool = await open_embedded_admin(dsn, schema)

            # (a) Cancel a RUNNING job through the admin route.
            cancel_target = hold_jobs[0]
            still_running = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',
                cancel_target.job_id,
            )
            assert still_running == "running", (
                f"the cancel's target left its run before the operator could act: {still_running}"
            )
            arm = await embedded.get(f"/embed/jobs/{cancel_target.job_id}")
            assert arm.status_code == 200, (
                f"the embedded job page did not render: {arm.status_code}"
            )
            cancel_resp = await embedded.post(
                f"/embed/jobs/{cancel_target.job_id}/cancel", {"reason": _CANCEL_REASON}
            )
            assert cancel_resp.status_code == 303, (
                f"the admin cancel did not land: {cancel_resp.status_code} {cancel_resp.text[:300]}"
            )

            async def _cancel_landed() -> bool:
                row = await conn.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',
                    cancel_target.job_id,
                )
                return row == "cancelled"

            try:
                await _poll(
                    _cancel_landed,
                    _CANCEL_LAND_BOUND_S,
                    "the running job reached cancelled",
                )
            except AssertionError:
                diag = await conn.fetch(
                    f"""
                    SELECT id, actor, status::text AS status, cancel_phase, locked_by_worker
                    FROM "{schema}".jobs WHERE status = 'running'
                    """
                )
                diag_events = await conn.fetch(
                    f"""
                    SELECT job_id, kind, detail FROM "{schema}".job_events
                    WHERE job_id = $1 ORDER BY occurred_at
                    """,
                    cancel_target.job_id,
                )
                raise AssertionError(
                    f"the running job never reached cancelled. running rows: "
                    f"{[dict(r) for r in diag]} events: {[dict(r) for r in diag_events]}"
                ) from None

            # The ledger tells who did what: the cancel_request event
            # carries the operator's reason; the audit trail names the
            # action and the target.
            event = await conn.fetchrow(
                f"""
                SELECT detail FROM "{schema}".job_events
                WHERE job_id = $1 AND kind = 'cancel_request'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                cancel_target.job_id,
            )
            assert event is not None, "the cancel wrote no cancel_request event"
            event_detail = event["detail"]
            if isinstance(event_detail, str):  # Why: asyncpg returns jsonb as text uncoded.
                event_detail = json.loads(event_detail)
            assert event_detail.get("reason") == _CANCEL_REASON, (
                f"the cancel_request event does not carry the operator's reason: {event_detail}"
            )
            audit = await conn.fetchrow(
                f"""
                SELECT reason FROM "{schema}".admin_audit
                WHERE target_id = $1 AND action = 'job.cancel'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                str(cancel_target.job_id),
            )
            assert audit is not None, "the admin cancel left no audit trail row"
            assert audit["reason"] == _CANCEL_REASON

            # ...and the action renders back: the embedded job page and
            # the standalone UI's count endpoint both say cancelled.
            detail_after = await embedded.get(f"/embed/jobs/{cancel_target.job_id}")
            assert detail_after.status_code == 200
            assert "cancelled" in detail_after.text, (
                "the job page does not render the operator's cancel"
            )
            ui_cancelled = await ui.get("/admin/jobs/count", actor="sys_hang", status="cancelled")
            assert ui_cancelled.json() == {"count": 1}, (
                f"the standalone admin UI does not reflect the operator's cancel: "
                f"{ui_cancelled.json()}"
            )

            # The standalone ui serve's OWN mutation route: the pinned gap.
            # _ui_serve never configures a Backend and create_router
            # (backend=None) does not build one, so the mutation routes
            # answer 503 from the standalone deployment even with admin
            # actions enabled - though admin-ui.md documents the no-backend
            # case as "the router builds its own PostgresBackend".
            ui_pin = await ui.post(f"/admin/jobs/{cancel_target.job_id}/cancel", {"reason": "pin"})
            assert ui_pin.status_code == 503, (
                "the standalone taskq ui serve mutation route changed behavior (got "
                f"{ui_pin.status_code}): if the missing-Backend gap was fixed, drive the "
                "ACT phase's cancels through this surface and drop this pin"
            )

            # (a2) The pair's second hold through the CLI - the operator's
            # other surface (the same `taskq job cancel` a runbook calls).
            # The loop's population must terminalise through operator
            # action, and the hold actor gives the cancel a target that is
            # running BY CONSTRUCTION. Same receipt: the row terminalises
            # 'cancelled' inside the derived bound, the event carries the
            # reason, and the cooperative cancel - not the escalation
            # ladder - is what landed it.
            cli_cancel_target = hold_jobs[1]
            cli_cancel = await asyncio.to_thread(
                run_cli,
                ["job", "cancel", str(cli_cancel_target.job_id), "--reason", _CANCEL_REASON],
                cli_env,
            )
            assert cli_cancel.returncode == 0, (
                f"taskq job cancel failed: {cli_cancel.stderr.decode(errors='replace')}"
            )

            async def _cli_cancel_landed() -> bool:
                row = await conn.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',
                    cli_cancel_target.job_id,
                )
                return row == "cancelled"

            await _poll(
                _cli_cancel_landed,
                _CANCEL_LAND_BOUND_S,
                "the CLI cancel terminalised the second hold",
            )
            cli_event = await conn.fetchrow(
                f"""
                SELECT detail FROM "{schema}".job_events
                WHERE job_id = $1 AND kind = 'cancel_request'
                ORDER BY occurred_at DESC LIMIT 1
                """,
                cli_cancel_target.job_id,
            )
            assert cli_event is not None, "the CLI cancel wrote no cancel_request event"
            cli_detail = cli_event["detail"]
            if isinstance(cli_detail, str):  # Why: asyncpg returns jsonb as text uncoded.
                cli_detail = json.loads(cli_detail)
            assert cli_detail.get("reason") == _CANCEL_REASON, (
                f"the CLI cancel's cancel_request event does not carry the operator's "
                f"reason: {cli_detail}"
            )

            # (b) Retry the failed job through the CLI.
            failed_row = await conn.fetchrow(
                f'SELECT status::text AS status, attempt FROM "{schema}".jobs WHERE id = $1',
                retry_me.job_id,
            )
            assert failed_row is not None and failed_row["status"] == "failed", (
                f"the always-failing actor did not terminalise failed: {failed_row}"
            )
            retry_cli = await asyncio.to_thread(
                run_cli, ["job", "retry", str(retry_me.job_id)], cli_env
            )
            assert retry_cli.returncode == 0, (
                f"taskq job retry failed: {retry_cli.stderr.decode(errors='replace')}"
            )

            async def _retried_ran() -> bool:
                row = await conn.fetchrow(
                    f'SELECT status::text AS status, attempt FROM "{schema}".jobs WHERE id = $1',
                    retry_me.job_id,
                )
                return row is not None and row["status"] == "failed" and int(row["attempt"]) >= 2

            await _poll(
                _retried_ran,
                _SETTLE_BOUND_S,
                "the operator retry re-ran the failed job",
            )
            failed_transitions = await conn.fetchval(
                f"""
                SELECT count(*) FROM "{schema}".job_events
                WHERE job_id = $1 AND kind = 'state_change'
                  AND detail->>'to_state' = 'failed'
                """,
                retry_me.job_id,
            )
            assert int(failed_transitions) >= 2, (
                f"the job's event log does not show both failure transitions after the "
                f"retry: {failed_transitions} failed transitions"
            )

            # (c) Move the actor's queue through the CLI.
            move = await asyncio.to_thread(
                run_cli,
                ["actor-config", "move-queue", "sys_mover", _MOVE_TARGET_QUEUE],
                cli_env,
            )
            assert move.returncode == 0, (
                f"actor-config move-queue failed: {move.stderr.decode(errors='replace')}"
            )
            # The enqueue side keeps the actor literal's queue label (the
            # row's audit trail); what the move changes is the ROUTING.
            # The loop therefore does not wait for the queue column to
            # turn over - it asserts the assignment, the served work and
            # the reads below.
            post_move: list[Any] = [
                await client.enqueue(sys_mover, SysPayload(sleep=0.1), tags=[_TAG])
                for _ in range(2)
            ]

            stored_queue = await conn.fetchval(
                f'SELECT queue FROM "{schema}".actor_config WHERE actor = $1', "sys_mover"
            )
            assert stored_queue == _MOVE_TARGET_QUEUE, (
                f"the stored assignment did not move: {stored_queue}"
            )
            # The move's dispatch-side effect: jobs enqueued after it are
            # served through the target's consumers - nothing strands on
            # the retired queue (ops.md's no-strays contract). The
            # settle is the ledger's receipt (the durable row), not a
            # client-side wait on a starved process's clock.
            for handle in post_move:
                await _poll(
                    lambda h=handle: _job_succeeded(conn, schema, h.job_id),
                    _SETTLE_BOUND_S,
                    "the post-move job completed through the moved queue",
                )
            backlog_after_move = await fetch_actor_backlog(conn, schema=schema)
            mover_row = next((r for r in backlog_after_move if r["actor"] == "sys_mover"), None)
            assert mover_row is not None and mover_row["queue"] == _MOVE_TARGET_QUEUE, (
                f"the insights backlog still routes sys_mover to the old queue: {mover_row}"
            )
            actors_page = await ui.get("/admin/actors")
            assert actors_page.status_code == 200
            assert "ops_moved" in actors_page.text, (
                "the actors overview does not render the moved-to assignment"
            )

            # (d) Drain: the documented path is the per-actor cap at 0
            # (ops.md: "an emergency drain to 0 belongs to actor-config
            # set --max-concurrent 0").
            drain_cli = await asyncio.to_thread(
                run_cli,
                ["actor-config", "set", "sys_drain", "--max-concurrent", "0"],
                cli_env,
            )
            assert drain_cli.returncode == 0, (
                f"actor-config set --max-concurrent 0 failed: "
                f"{drain_cli.stderr.decode(errors='replace')}"
            )
            held_back = [
                await client.enqueue(sys_drain, SysPayload(sleep=0.2), tags=[_TAG])
                for _ in range(3)
            ]
            held_ids = [str(h.job_id) for h in held_back]

            async def _drain_holds() -> bool:
                row = await conn.fetchrow(
                    f"""
                    SELECT
                        count(*) FILTER (WHERE status = 'pending') AS pending,
                        count(*) FILTER (WHERE status = 'running') AS running
                    FROM "{schema}".jobs
                    WHERE tags @> ARRAY[$1::text] AND actor = 'sys_drain'
                      AND id = ANY($2::uuid[])
                    """,
                    _TAG,
                    held_ids,
                )
                assert row is not None
                return int(row["pending"]) == len(held_ids) and int(row["running"]) == 0

            # Two exposures, each a derived window (a broken cap leaks a
            # claim within one dispatch cycle; the window gives it two,
            # stretched), each followed by the SAME state assertion - the
            # teeth are the pending rows and the zero running count, not
            # the clock.
            await asyncio.sleep(_DRAIN_EXPOSURE_S)
            await _poll(
                _drain_holds,
                _CLAIM_CYCLE_S * TIER_LOAD_STRETCH,
                "the drained actor's backlog held pending",
            )
            await asyncio.sleep(_DRAIN_EXPOSURE_S)
            await _poll(
                _drain_holds,
                _CLAIM_CYCLE_S * TIER_LOAD_STRETCH,
                "the drained actor's backlog STILL held pending",
            )

            # The rest of the fleet is alive through the drain: the
            # neighbor's completion is read off the LEDGER (the durable
            # row), on the settle bound - not a client-side wait whose
            # clock a starved runner owns.
            neighbor = await client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
            await _poll(
                lambda: _job_succeeded(conn, schema, neighbor.job_id),
                _SETTLE_BOUND_S,
                "the fleet's neighbor job survived the drain and completed",
            )
            ui_pending = await ui.get("/admin/jobs/count", actor="sys_drain", status="pending")
            assert ui_pending.json() == {"count": len(held_ids)}, (
                f"the admin UI does not render the drain's held backlog: {ui_pending.json()}"
            )
            backlog_drained = await fetch_actor_backlog(conn, schema=schema)
            drain_row = next((r for r in backlog_drained if r["actor"] == "sys_drain"), None)
            assert drain_row is not None and drain_row["backlog"] == len(held_ids), (
                f"the insights backlog does not see the drain's held work: {drain_row}"
            )

            undrain = await asyncio.to_thread(
                run_cli,
                ["actor-config", "set", "sys_drain", "--clear-max-concurrent"],
                cli_env,
            )
            assert undrain.returncode == 0, (
                f"clearing the drain cap failed: {undrain.stderr.decode(errors='replace')}"
            )

            async def _drain_flushed() -> bool:
                row = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM "{schema}".jobs
                    WHERE tags @> ARRAY[$1::text] AND actor = 'sys_drain'
                      AND status = 'succeeded'
                    """,
                    _TAG,
                )
                return int(row) == len(drain_first_wave) + len(held_ids)

            await _poll(
                _drain_flushed,
                _SETTLE_BOUND_S,
                "the undrained backlog completed after the cap cleared",
            )

            # The cron ledger counts the schedule's own fire: the wait
            # trails the operator's actions, so the minute boundary the
            # schedule was seeded on has long passed by the time the
            # drain flushed.
            async def _cron_fired() -> bool:
                row = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM "{schema}".jobs
                    WHERE metadata->>'cron_schedule_id' = $1 AND status = 'succeeded'
                    """,
                    str(schedule.schedule_id),
                )
                return int(row) >= 1

            await _poll(
                _cron_fired,
                _CRON_FIRE_BOUND_S,
                "the every-minute cron schedule's first fire landed",
            )
            ledger = await fetch_cron_ledger(conn, schema=schema, window=timedelta(hours=1))
            ledger_row = next(
                (r for r in ledger if str(r.get("schedule_id")) == str(schedule.schedule_id)),
                None,
            )
            assert ledger_row is not None and ledger_row["fires_window"] >= 1, (
                f"the cron ledger does not count the schedule's own fire: {ledger}"
            )

            # ══ 4. RECOVER ═════════════════════════════════════════════
            # kill -9 a worker mid-loop: the fleet self-heals, and the
            # event trail answers what happened.

            recovery_jobs = [
                await client.enqueue(sys_slow, SysPayload(sleep=_SLOW_SLEEP), tags=[_TAG])
                for _ in range(3)
            ]
            recovery_ids = [str(h.job_id) for h in recovery_jobs]

            # The victim: whichever replica is running the recovery work.
            victim_box: list[WorkerProc] = []
            survivor_box: list[WorkerProc] = []

            async def _victim_running() -> bool:
                rows = await conn.fetch(
                    f"""
                    SELECT DISTINCT w.pid FROM "{schema}".jobs j
                    JOIN "{schema}".workers w ON w.id = j.locked_by_worker
                    WHERE tags @> ARRAY[$1::text] AND j.status = 'running'
                      AND j.actor = 'sys_slow'
                      AND j.id = ANY($2::uuid[])
                    """,
                    _TAG,
                    recovery_ids,
                )
                for row in rows:
                    pid = int(row["pid"])
                    if pid == worker_a.proc.pid:
                        victim_box[:] = [worker_a]
                        survivor_box[:] = [worker_b]
                        return True
                    if pid == worker_b.proc.pid:
                        victim_box[:] = [worker_b]
                        survivor_box[:] = [worker_a]
                        return True
                return False

            await _poll(
                _victim_running,
                _RECLAIM_LAND_S,
                "a recovery job claimed by a killable replica",
            )
            victim = victim_box[0]
            survivor = survivor_box[0]

            victim_pid = victim.proc.pid
            # Map worker ids to pids NOW (the stale-worker cleanup erases
            # the dead replica's row later): the limbo forensic below
            # needs to name WHOSE row lapsed.
            worker_id_by_pid = {
                int(r["pid"]): str(r["id"])
                for r in await conn.fetch(f'SELECT id, pid FROM "{schema}".workers')
            }
            pre_kill_rows = await conn.fetch(
                f"""
                SELECT id, actor, status::text AS status, attempt, cancel_phase,
                       lock_expires_at, locked_by_worker
                FROM "{schema}".jobs WHERE actor = 'sys_slow'
                """
            )
            print(
                f"LOOP-FORENSICS kill: victim_pid={victim_pid} "
                f"victim_worker_id={worker_id_by_pid.get(victim_pid)} "
                f"map={worker_id_by_pid} rows={[dict(r) for r in pre_kill_rows]}"
            )
            os.kill(victim_pid, signal.SIGKILL)
            assert await asyncio.to_thread(victim.proc.wait, 30) == -int(signal.SIGKILL), (
                "the SIGKILLed worker did not die by the signal"
            )

            # Self-heal: the survivor reclaims the orphaned lease and
            # re-runs the work to succeeded (a fresh attempt under the
            # retry budget).
            async def _recovered() -> bool:
                row = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM "{schema}".jobs
                    WHERE tags @> ARRAY[$1::text] AND actor = 'sys_slow'
                      AND id = ANY($2::uuid[]) AND status = 'succeeded'
                    """,
                    _TAG,
                    recovery_ids,
                )
                return int(row) == len(recovery_jobs)

            await _poll(
                _recovered,
                _RECLAIM_SETTLE_S,
                "the SIGKILLed worker's jobs reclaimed and re-run",
            )

            # No stuck leases - the sweep's OWN eligibility contract, not
            # a looser glance: a running row whose lease lapsed must have
            # been reclaimed, EXCEPT a row carrying an in-flight cancel
            # request, which the sweep deliberately leaves with its
            # (possibly live) holder until the lapse outlasts both graces
            # plus a 60s honesty window before it terminalises the
            # operator's request itself (_SWEEP_1_SQL's lease arm). A row
            # inside that window is the contract's designed shape, not a
            # stuck lease; anything else lapsed-and-running is.
            # POLLED, not single-shot: the reclaim is the sweep's own
            # eventual act - its tick must land AND the killed holder's
            # worker row must expire before the lease arm is eligible -
            # and neither synchronises with the moment the recovery poll
            # above returns. A single glance raced that arithmetic and
            # flaked; the poll fails only if the sweep NEVER reclaims.
            limbo: list[Any] = []

            async def _sweep_reclaimed_all_lapsed() -> bool:
                nonlocal limbo
                limbo = await conn.fetch(
                    f"""
                    SELECT id, actor, status::text AS status, attempt, cancel_phase,
                           lock_expires_at, locked_by_worker,
                           id = ANY($1::uuid[]) AS is_recovery_job
                    FROM "{schema}".jobs
                    WHERE status = 'running'
                      AND lock_expires_at < clock_timestamp()
                      AND (cancel_phase = 0
                           OR lock_expires_at < clock_timestamp()
                              - make_interval(secs => $2::double precision)
                              - make_interval(secs => $3::double precision)
                              - interval '60 seconds')
                    """,
                    recovery_ids,
                    _CANCELLATION_GRACE_S,
                    _CLEANUP_GRACE_S,
                )
                return not limbo

            try:
                await _poll(
                    _sweep_reclaimed_all_lapsed,
                    _RECLAIM_LAND_S,
                    "the sweep to reclaim every lapsed-lease running row",
                )
            except AssertionError:
                assert not limbo, (
                    f"{len(limbo)} running row(s) hold a lapsed lease the sweep should have "
                    f"reclaimed: {[dict(r) for r in limbo]}; worker map: {worker_id_by_pid}"
                )
                raise

            # The reclaim is visible in the ledger: at least one recovery
            # job re-ran at a NEW attempt. POLLED, not single-shot: the
            # reclaim's crashed-attempt row is the batched insert the sweep
            # commits after the lease transfer, and the re-run's own row
            # lands at its terminal write - both eventual, and neither
            # synchronises with the limbo poll above returning. The poll
            # fails only if the ledger NEVER shows the second attempt.
            attempts: list[Any] = []

            async def _reclaim_visible_in_ledger() -> bool:
                nonlocal attempts
                attempts = await conn.fetch(
                    f"""
                    SELECT j.id, count(a.attempt)::int AS attempts
                    FROM "{schema}".jobs j
                    LEFT JOIN "{schema}".job_attempts a ON a.job_id = j.id
                    WHERE j.tags @> ARRAY[$1::text] AND j.actor = 'sys_slow'
                      AND j.id = ANY($2::uuid[])
                    GROUP BY j.id
                    """,
                    _TAG,
                    recovery_ids,
                )
                return any(int(r["attempts"]) >= 2 for r in attempts)

            try:
                await _poll(
                    _reclaim_visible_in_ledger,
                    _RECLAIM_LAND_S,
                    "the reclaim's second attempt to land in the ledger",
                )
            except AssertionError:
                assert any(int(r["attempts"]) >= 2 for r in attempts), (
                    f"no recovery job shows the reclaim's second attempt: {[dict(r) for r in attempts]}"
                )
                raise

            # Liveness: the stale worker row is cleaned and the pages stop
            # showing the dead replica.
            async def _victim_forgotten() -> bool:
                row = await conn.fetchval(
                    f'SELECT count(*) FROM "{schema}".workers WHERE pid = $1', victim_pid
                )
                if int(row) != 0:
                    return False
                page = await ui.get("/admin/workers")
                return str(victim_pid) not in page.text

            await _poll(
                _victim_forgotten, _RECLAIM_LAND_S, "the dead worker row cleaned and off the pages"
            )

            # The leader's trail still names a live holder (a killed
            # leader's takeover re-fences the row; a killed follower's row
            # never moved).
            async def _leader_is_live() -> bool:
                row = await conn.fetchrow(
                    f"""
                    SELECT w.pid FROM "{schema}".maintenance_leader ml
                    JOIN "{schema}".workers w ON w.id = ml.worker_id
                    """
                )
                return row is not None and int(row["pid"]) == survivor.proc.pid

            await _poll(
                _leader_is_live, _RECLAIM_LAND_S, "the leader row naming the surviving replica"
            )

            # ══ The balance ════════════════════════════════════════════
            counts = await assert_balanced(conn, schema, _TAG)
            await assert_effects_balance(conn, schema, _TAG)
            assert counts.get("succeeded", 0) >= (
                len(fast_jobs)
                + len(rated_jobs)
                + len(mover_jobs)
                + len(post_move)
                + len(drain_first_wave)
                + len(held_ids)
                + 1  # the cron fire
                + len(recovery_jobs)
            ), f"the loop's delivered work is missing from the balance: {counts}"
            assert counts.get("cancelled", 0) == 2, (
                f"the loop performed exactly two operator cancels (the embedded router's "
                f"and the CLI's); the ledger names a different count: {counts}"
            )
            assert counts.get("failed", 0) >= 1, counts

            # Teardown inside the client scope: the schedule must stop
            # firing before the tagged population is judged.
            await client.delete_schedule(schedule.schedule_id)
            await conn.execute(
                f"DELETE FROM \"{schema}\".jobs WHERE metadata->>'cron_schedule_id' = $1",
                str(schedule.schedule_id),
            )
        finally:
            if embedded_pool is not None:
                await embedded_pool.close()
            if ui is not None and ui_client is not None:
                await stop_admin_ui(ui, ui_client)
            reap(worker_a)
            reap(worker_b)
            await delete_tagged(conn, schema, _TAG)
