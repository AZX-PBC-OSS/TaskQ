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

The queue overview page (queues.py:79-102) is deliberately NOT asserted: its
SQL counts only pending/scheduled/running/failed rows, so a succeeded job
leaves no visible queue row after completion.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import socket
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest

from ._assertions import poll_until
from .actors import WelcomeEmailPayload, send_welcome_email

if TYPE_CHECKING:
    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ADMIN_ENTRY = _REPO_ROOT / "tests" / "e2e" / "admin_entry.py"
_READY_TIMEOUT = 30.0
_SHUTDOWN_TIMEOUT = 10.0


def _free_port() -> int:
    """Ephemeral host port: bind 0, read back, close.

    The bind-and-release TOCTOU race is accepted (standard practice): a lost
    race surfaces as a readiness timeout with the subprocess logs attached,
    never as a silent pass.
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


@pytest.fixture
async def admin_server(e2e_schema: E2ESchema) -> AsyncIterator[httpx.AsyncClient]:
    """Real admin UI uvicorn subprocess plus an authenticated httpx client.

    Function-scoped: one server per test, always torn down. The readiness
    probe hits ``/admin/jobs/count`` WITH the bearer token, so a 200 proves
    the HTTP stack, the auth dependency, and the PG pool are all up. A
    subprocess that crashes during startup fails fast with its logs; a
    readiness timeout also fails with its logs.
    """
    port = _free_port()
    token = secrets.token_hex(16)
    python_path = os.environ.get("PYTHONPATH")
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

    try:
        try:
            await poll_until(
                _ready,
                timeout=_READY_TIMEOUT,
                description=f"admin server readiness at {client.base_url}",
            )
        except TimeoutError:
            logs = await asyncio.to_thread(_shutdown_and_collect_logs, proc)
            msg = f"admin server not ready within {_READY_TIMEOUT}s\n{logs}"
            raise RuntimeError(msg) from None
        yield client
    finally:
        await client.aclose()
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
