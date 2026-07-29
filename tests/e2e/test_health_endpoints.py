"""Health endpoint e2e — /live and /ready on the worker's Unix-socket health server.

The worker container runs a :class:`HealthServer` on a Unix domain socket
at ``/tmp/taskq_health.sock`` (default ``health_socket_path``). The server
is a bare ``asyncio.start_unix_server`` HTTP responder (not FastAPI) with
two endpoints:

- ``GET /live`` — event-loop responsiveness probe (``{"status":"ok"}``).
- ``GET /ready`` — readiness probe with watchdog observability fields:
  ``stale_loops``, ``loop_tick_ages``, ``shutdown_elapsed_seconds``,
  ``shutdown_phase``, ``active_jobs``, ``is_leader``, ``redis_configured``,
  ``pg_ping_ok``, ``live``, ``ready``, ``reasons``.

Both endpoints are accessed from the test process via ``docker exec``:
the Unix socket is inside the container, so a Python one-liner is
``exec_run``'d in the container to issue a raw HTTP GET over the socket
and return the response body.

The ``/ready`` shutdown-state assertion SIGTERMs the worker and polls
``/ready`` until it reports a non-null ``shutdown_phase`` (the
orchestration sets ``deps.shutdown_phase`` at the start of DRAINING).
The module-local ``clean_e2e_state`` override tolerates the intentionally
killed worker.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from ._assertions import poll_until
from .conftest import E2EWorker

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(300)]

_HEALTH_SOCKET = "/tmp/taskq_health.sock"  # noqa: S108  # Why: matches the worker's default health_socket_path setting; this is the path inside the container, not a test-created temp file.

_QUERY_SCRIPT = (
    "import socket,sys,json\n"
    f"sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)\n"
    f"sock.connect({_HEALTH_SOCKET!r})\n"
    "sock.sendall(b'GET ' + sys.argv[1].encode() + b' HTTP/1.0\\r\\n\\r\\n')\n"
    "data=b''\n"
    "while True:\n"
    "    chunk=sock.recv(4096)\n"
    "    if not chunk: break\n"
    "    data+=chunk\n"
    "sock.close()\n"
    "header,_,body=data.partition(b'\\r\\n\\r\\n')\n"
    "sys.stdout.write(body.decode())\n"
)


def _exec_health_get(container: object, path: str) -> str:
    """Run a Python HTTP-over-Unix-socket GET inside the container.

    Returns the response body as a string. Raises ``RuntimeError`` if the
    exec fails or the socket is not connectable (the health server has not
    started yet, or the container is dead).
    """
    wrapped = container.get_wrapped_container()  # type: ignore[attr-defined]
    result = wrapped.exec_run(
        ["python", "-c", _QUERY_SCRIPT, path],
        workdir="/app",
    )
    exit_code, output = result
    if exit_code != 0:
        msg = f"docker exec health GET {path} failed (exit {exit_code}): {output!r}"
        raise RuntimeError(msg)
    if isinstance(output, bytes):
        output = output.decode("utf-8", errors="replace")
    return output


# ── Module-local clean_e2e_state override ─────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-killed workers.

    The ``/ready`` shutdown-state test SIGTERMs the worker mid-test.
    Skips ``_raise_if_worker_crashed`` and tolerates idle-gate timeout.
    """
    if not {"e2e_client", "e2e_pg_pool", "e2e_worker", "e2e_schema"}.intersection(
        request.fixturenames
    ):
        yield
        return

    from .conftest import _DELETE_ORDER, _flushdb

    e2e_schema: E2ESchema = request.getfixturevalue("e2e_schema")
    e2e_pg_pool: asyncpg.Pool = request.getfixturevalue("e2e_pg_pool")
    e2e_dragonfly: E2EDragonfly = request.getfixturevalue("e2e_dragonfly")
    schema = e2e_schema.schema_name

    async def _no_running_jobs() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE status = $1',
            "running",
        )
        return count == 0

    with contextlib.suppress(TimeoutError):
        await poll_until(
            _no_running_jobs,
            timeout=30.0,
            description=f"idle gate: zero running jobs in {schema}.jobs",
        )

    async with e2e_pg_pool.acquire() as conn:
        for table in _DELETE_ORDER:
            await conn.execute(f'DELETE FROM "{schema}"."{table}"')

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}")
    yield


# ── Tests ─────────────────────────────────────────────────────────────────


async def test_health_live_endpoint(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    run_id: str,
) -> None:
    """``GET /live`` on a running worker returns 200 + ``{"status":"ok"}``.

    The health server is a Unix-socket HTTP responder started by the
    worker's bootstrap (``_bootstrap.py:572-575``). The test execs a
    Python one-liner inside the container that connects to the socket
    and issues a raw HTTP GET. The event-loop liveness probe schedules
    a ``call_later(0.01)`` callback and waits up to 1 s for it to fire
    (``worker/health.py:_check_live``); a healthy worker returns
    ``{"status":"ok"}`` with HTTP 200.
    """
    from .actors import WelcomeEmailPayload, send_welcome_email

    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="u-live", email="live@example.com"),
    )
    await handle.wait(timeout=60)

    body = await asyncio.to_thread(_exec_health_get, e2e_worker.container, "/live")
    data = json.loads(body)
    assert data["status"] == "ok"


async def test_health_ready_endpoint(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    run_id: str,
) -> None:
    """``GET /ready`` reports health fields while running, then reflects
    shutdown state after SIGTERM.

    (a) While the worker is running normally, ``/ready`` returns 200 with
    fields: ``stale_loops`` (not in body — the readiness body includes
    ``loop_tick_ages``, ``shutdown_elapsed_seconds``, ``shutdown_phase``,
    ``active_jobs``, ``is_leader``, ``redis_configured``, ``pg_ping_ok``,
    ``live``, ``ready``, ``reasons``). The ``ready`` field is ``true`` and
    ``shutdown_phase`` is ``null`` (``ShutdownPhase.NONE`` is serialised
    as ``null`` in ``build_ready_body``).

    (b) After SIGTERM, the shutdown orchestration sets
    ``deps.shutdown_phase`` to ``DRAINING`` (value 1) at the start of
    phase 1 (``shutdown.py:135``). Polling ``/ready`` shows
    ``shutdown_phase`` is no longer null and ``ready`` is ``false``.
    """
    from .actors import WelcomeEmailPayload, send_welcome_email

    handle = await e2e_client.enqueue(
        send_welcome_email,
        WelcomeEmailPayload(run_id=run_id, user_id="u-ready", email="ready@example.com"),
    )
    await handle.wait(timeout=60)

    body = await asyncio.to_thread(_exec_health_get, e2e_worker.container, "/ready")
    data = json.loads(body)
    assert data["ready"] is True
    assert data["live"] is True
    assert data["shutdown_phase"] is None
    assert "loop_tick_ages" in data
    assert "shutdown_elapsed_seconds" in data

    wrapped = e2e_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    async def _shutdown_visible() -> bool:
        try:
            body = await asyncio.to_thread(_exec_health_get, e2e_worker.container, "/ready")
        except RuntimeError:
            return False
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return False
        return data.get("shutdown_phase") is not None and data.get("ready") is False

    await poll_until(
        _shutdown_visible,
        timeout=15.0,
        description="/ready reflecting shutdown state (non-null shutdown_phase, ready=false)",
    )
