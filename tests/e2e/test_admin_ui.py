"""Admin UI e2e - a real uvicorn subprocess serving real job data from the e2e PG.

The admin FastAPI app runs as a host SUBPROCESS (``tests/e2e/admin_entry.py``,
spawned by the ``admin_server`` fixture) wired to the module's e2e PG schema
via ``e2e_schema.host_dsn`` in bearer-token auth mode. No ASGI transport and
no mocks: assertions go over real HTTP through a real uvicorn against the
same migrated schema the containerized worker writes.

Endpoints asserted (verified in ``src/taskq/web/admin/``):

- ``GET /admin/jobs/{job_id}`` (jobs.py:470-527): job detail HTML page. Reads
  ``{schema}.jobs`` and falls back to ``{schema}.jobs_archive``, so it
  resolves whether or not the prune sweep archived the row. The template
  (templates/job_detail.html) renders the full job UUID and the actor name.
- ``GET /admin/jobs/count?actor=...`` (jobs.py:412-435): JSON ``{"count": N}``
  over the live ``jobs`` table; the default live tab spans all statuses
  including ``succeeded`` (``_constants.py`` ``_ALL_STATUSES``). A completed
  job is NOT archived mid-test: ``prune_terminal_jobs`` only moves rows with
  ``finished_at < now() - retention`` (worker/_leader_shared.py:317-324) and
  the e2e worker env keeps the 30-day ``prune_retention_succeeded`` default.
- The same route without the bearer token returns 401: ``token_auth`` is a
  router-level dependency (auth/token.py, wired at _factory.py:334-336), so
  the subprocess provably enforces auth on every admin route.

The queue overview page (``queues.py:79-102``) is asserted via
``test_admin_queue_overview``: the SQL counts only
pending/scheduled/running/failed rows, so the test enqueues a
long-running job and hits the endpoint while it is running.

The admin SSE endpoint (``sse.py:81-104``) is asserted via
``test_admin_sse_realtime_events``: PG LISTEN/NOTIFY streams
``state_change`` events as jobs transition through statuses.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import socket
import subprocess
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest

from ._assertions import poll_until, wait_for_effects
from .actors import (
    SlowDeliverPayload,
    WelcomeEmailPayload,
    send_welcome_email,
    slow_deliver_webhook,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ADMIN_ENTRY = _REPO_ROOT / "tests" / "e2e" / "admin_entry.py"
_READY_TIMEOUT = 30.0
_SHUTDOWN_TIMEOUT = 10.0
# Bind-and-release port selection loses a TOCTOU race now and then under
# parallel load; a child that exits early with "address already in use" gets
# this many total attempts, each on a fresh port.
_MAX_BIND_ATTEMPTS = 3


def _free_port() -> int:
    """Ephemeral host port: bind 0, read back, close.

    The bind-and-release TOCTOU race is mitigated, not eliminated: the
    ``admin_server`` fixture detects a child that lost the race (early exit
    with "address already in use") and retries on a fresh port; any other
    startup failure still surfaces as a readiness timeout with the
    subprocess logs attached, never as a silent pass.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _spawn_admin(env: dict[str, str]) -> subprocess.Popen[str]:
    """Fork/exec the admin entry script. Non-blocking: the child runs uvicorn
    and the parent returns as soon as the process exists."""
    # S603 waived on the Popen call below: static argv (sys.executable +
    # repo-local entry script), no shell; all env values are test-controlled.
    return subprocess.Popen(  # noqa: S603
        [sys.executable, str(_ADMIN_ENTRY)],
        cwd=str(_REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _shutdown_and_collect_logs(proc: subprocess.Popen[str]) -> str:
    """SIGTERM, escalate to SIGKILL on timeout, then drain captured output.

    ``communicate`` waits for process exit and returns everything buffered in
    the stdout pipe (stderr is merged into it at spawn). Repeated calls after
    process death are safe: CPython caches the drained buffers.
    """
    if proc.poll() is None:
        proc.terminate()
    try:
        out, _ = proc.communicate(timeout=_SHUTDOWN_TIMEOUT)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out.strip()


def _admin_readiness(
    proc: subprocess.Popen[str], client: httpx.AsyncClient
) -> Callable[[], Awaitable[bool]]:
    """Readiness predicate for one spawned admin server.

    Fail-fast with captured logs when the child exits during startup (the
    caller classifies "address already in use" for a fresh-port retry);
    otherwise probe ``/admin/jobs/count`` with the bearer token.
    """

    async def _ready() -> bool:
        if proc.poll() is not None:
            logs = await asyncio.to_thread(_shutdown_and_collect_logs, proc)
            msg = f"admin server exited during startup (rc={proc.returncode})\n{logs}"
            raise RuntimeError(msg)
        try:
            resp = await client.get("/admin/jobs/count")
        except httpx.HTTPError:
            return False
        return resp.status_code == 200

    return _ready


@pytest.fixture
async def admin_server(e2e_schema: E2ESchema) -> AsyncIterator[httpx.AsyncClient]:
    """Real admin UI uvicorn subprocess plus an authenticated httpx client.

    Function-scoped: one server per test, always torn down. The readiness
    probe hits ``/admin/jobs/count`` WITH the bearer token, so a 200 proves
    the HTTP stack, the auth dependency, and the PG pool are all up. A
    subprocess that crashes during startup fails fast with its logs; a
    readiness timeout also fails with its logs.

    The port is chosen bind-and-release (TOCTOU window: another process can
    win it before the child binds), so a child that exits early with
    "address already in use" is retried on a fresh port, up to
    ``_MAX_BIND_ATTEMPTS`` attempts.
    """
    token = secrets.token_hex(16)
    python_path = os.environ.get("PYTHONPATH")

    proc: subprocess.Popen[str] | None = None
    client: httpx.AsyncClient | None = None
    try:
        for attempt in range(1, _MAX_BIND_ATTEMPTS + 1):
            port = _free_port()
            env = {
                **os.environ,
                "TASKQ_PG_DSN": e2e_schema.host_dsn,
                "TASKQ_SCHEMA_NAME": e2e_schema.schema_name,
                "TASKQ_E2E_ADMIN_TOKEN": token,
                "TASKQ_E2E_ADMIN_PORT": str(port),
                "PYTHONPATH": (
                    str(_REPO_ROOT) if not python_path else f"{_REPO_ROOT}{os.pathsep}{python_path}"
                ),
            }
            proc = await asyncio.to_thread(_spawn_admin, env)
            client = httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                headers={"Authorization": f"Bearer {token}"},
            )

            try:
                try:
                    await poll_until(
                        _admin_readiness(proc, client),
                        timeout=_READY_TIMEOUT,
                        description=f"admin server readiness at {client.base_url}",
                    )
                except TimeoutError:
                    logs = await asyncio.to_thread(_shutdown_and_collect_logs, proc)
                    msg = f"admin server not ready within {_READY_TIMEOUT}s\n{logs}"
                    raise RuntimeError(msg) from None
            except RuntimeError as exc:
                await client.aclose()
                if "address already in use" in str(exc).lower() and attempt < _MAX_BIND_ATTEMPTS:
                    continue  # lost the bind race — fresh port, fresh child
                raise
            break

        assert proc is not None and client is not None
        yield client
    finally:
        if client is not None:
            await client.aclose()
        if proc is not None:
            await asyncio.to_thread(_shutdown_and_collect_logs, proc)


async def test_admin_ui_serves_real_job_data(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    admin_server: httpx.AsyncClient,
    run_id: str,
) -> None:
    """Enqueue -> container worker completes -> admin HTTP endpoints serve the row.

    ``handle.wait()`` proves real cross-container completion first; the HTTP
    assertions then prove the admin server reads the same live schema. The
    worker fixture is requested explicitly so the job actually dispatches
    (mirrors test_core_lifecycle.py's contract).
    """
    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="u-1", email="a@example.com"),
    )
    result = await handle.wait(timeout=60)
    assert result.sent is True

    job_id = str(handle.job_id)

    detail = await admin_server.get(f"/admin/jobs/{job_id}")
    assert detail.status_code == 200
    assert job_id in detail.text
    assert "send_welcome_email" in detail.text

    count = await admin_server.get("/admin/jobs/count", params={"actor": "send_welcome_email"})
    assert count.status_code == 200
    assert count.json() == {"count": 1}

    async with httpx.AsyncClient(base_url=str(admin_server.base_url)) as anonymous:
        denied = await anonymous.get("/admin/jobs/count")
    assert denied.status_code == 401


# ── Admin SSE real-time events ────────────────────────────────────────────


async def test_admin_sse_realtime_events(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    admin_server: httpx.AsyncClient,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Open the admin SSE endpoint for real-time job events and verify
    ``state_change`` events stream as a job transitions through statuses.

    The admin SSE endpoint ``GET /admin/sse/jobs`` (``web/admin/sse.py``)
    uses PG LISTEN/NOTIFY via ``listen_with_reconnect`` (not Redis
    pub/sub) to stream job state-change events. The endpoint emits an
    initial ``event: status`` ack, then ``event: state_change`` lines as
    NOTIFY payloads arrive.

    The test opens an SSE stream to ``/admin/sse/jobs``, enqueues a
    ``slow_deliver_webhook`` job (3 s sleep), waits for it to start
    running, then cancels it. The cancel request triggers a
    ``pg_notify`` on the ``events_channel`` (``postgres.py:593-602``),
    which the SSE endpoint receives and emits as an
    ``event: state_change`` line.
    """
    from .actors import SlowDeliverPayload, slow_deliver_webhook

    events: list[str] = []
    state_change_seen = False

    # Use a dedicated client with no read timeout for SSE streaming.
    sse_client = httpx.AsyncClient(
        base_url=str(admin_server.base_url).rstrip("/"),
        headers=admin_server.headers,
        timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0),
    )
    try:
        async with asyncio.timeout(60):
            async with sse_client.stream("GET", "/admin/sse/jobs") as resp:
                assert resp.status_code == 200, f"SSE endpoint returned {resp.status_code}"

                # Read the initial status ack first, then wait for the
                # PG LISTEN to register before enqueuing the job.
                aiter = resp.aiter_lines().__aiter__()
                first_line = await anext(aiter)
                if first_line.startswith("event: "):
                    events.append(first_line[len("event: ") :])

                # Give the LISTEN connection time to register.
                await asyncio.sleep(1.0)

                handle = await e2e_client.enqueue(
                    slow_deliver_webhook,
                    SlowDeliverPayload(run_id=run_id, endpoint_id="ep-sse"),
                )

                # Wait for the job to start running, then cancel it.
                # The cancel triggers pg_notify on the events_channel.
                from ._assertions import wait_for_effects

                await wait_for_effects(
                    e2e_pg_pool,
                    e2e_schema.schema_name,
                    run_id,
                    kind="started",
                    min_count=1,
                    timeout=30.0,
                )
                await handle.cancel()

                async for line in aiter:
                    if line.startswith("event: "):
                        events.append(line[len("event: ") :])
                    if "state_change" in events:
                        state_change_seen = True
                        break
    finally:
        await sse_client.aclose()

    assert state_change_seen, (
        f"expected at least one 'state_change' SSE event; events seen: {events}"
    )


# ── Admin queue overview ──────────────────────────────────────────────────


async def test_admin_queue_overview(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    admin_server: httpx.AsyncClient,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """Hit the queue overview endpoint with jobs on the ``e2e`` queue and
    verify the HTML response contains the queue name and correct counts.

    The queue overview endpoint ``GET /admin/queues``
    (``web/admin/queues.py:79-102``) queries
    ``{schema}.jobs`` for counts grouped by queue, filtering only
    ``pending``, ``scheduled``, ``running``, and ``failed`` statuses.
    The HTML template renders one row per queue with its counts.

    The test enqueues a ``slow_deliver_webhook`` job (3 s sleep) and,
    while it is running, hits ``/admin/queues``. The response HTML must
    contain the ``e2e`` queue name and a ``running_count`` of at least 1.
    A second queue (``e2e_other``) is seeded via a direct SQL INSERT to
    verify multi-queue rendering.
    """
    await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="ep-overview"),
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    # Seed a second queue with a pending job via direct SQL so the
    # overview shows multiple queues. The jobs table requires several
    # NOT NULL columns; this mirrors the minimal column set the
    # enqueue path writes.
    import uuid as uuid_mod

    other_job_id = uuid_mod.uuid4()
    async with e2e_pg_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{e2e_schema.schema_name}".jobs '
            "(id, actor, queue, payload, retry_kind, status, attempt, max_attempts, priority, created_at, scheduled_at) "
            "VALUES ($1, 'test_actor', 'e2e_other', '{}'::jsonb, 'transient', 'pending', 0, 3, 0, now(), now())",
            other_job_id,
        )

    try:
        resp = await admin_server.get("/admin/queues")
        assert resp.status_code == 200
        html = resp.text
        assert "e2e" in html, "queue overview HTML should contain the 'e2e' queue name"
        assert "e2e_other" in html, "queue overview HTML should contain the 'e2e_other' queue name"
    finally:
        async with e2e_pg_pool.acquire() as conn:
            await conn.execute(
                f'DELETE FROM "{e2e_schema.schema_name}".jobs WHERE id = $1',
                other_job_id,
            )
