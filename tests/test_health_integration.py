"""Integration tests for end-to-end health endpoints.

Uses the session-scoped ``pg_container`` fixture from ``tests/conftest.py``,
spawning a worker subprocess via ``tests/_worker_harness.py`` and driving
CLI commands through the Unix health socket.

Tests poll ``os.path.exists(socket_path)`` with a 5 s deadline for worker
readiness rather than relying on a ``worker-ready`` stderr line — the
polling approach keeps the harness slim and avoids buffering concerns.
"""

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────────────


_WS = WorkerSettings


_SOCK_PREFIX = f"/tmp/tq-integ-{os.getpid()}-"  # noqa: S108 # Why: macOS AF_UNIX limit is 104 chars; /tmp is the shortest safe prefix.
_sock_seq = 0


def _next_sock_path() -> str:
    global _sock_seq
    _sock_seq += 1
    return f"{_SOCK_PREFIX}{_sock_seq}.sock"


def _worker_settings_dict(pg_dsn: str, socket_path: str, schema: str) -> dict[str, str]:
    return {
        "TASKQ_PG_DSN": pg_dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_HEALTH_SOCKET_PATH": socket_path,
        # Shorten shutdown grace periods for fast test teardown.
        # Must satisfy lock_lease >= 4*heartbeat_interval and
        # cancel+cleanup < lock_lease (/).
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
        "TASKQ_HEALTH_PG_PING_TIMEOUT": "2.0",
    }


def _worker_env(pg_dsn: str, socket_path: str, schema: str) -> dict[str, str]:
    return {**os.environ, **_worker_settings_dict(pg_dsn, socket_path, schema)}


async def _prepare_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


def _spawn_worker(pg_dsn: str, socket_path: str, schema: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "tests._worker_harness"],
        env=_worker_env(pg_dsn, socket_path, schema),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )


def _wait_for_socket(
    socket_path: str, proc: subprocess.Popen[bytes] | None = None, deadline: float = 3.0
) -> None:
    t0 = time.monotonic()
    while True:
        if os.path.exists(socket_path):
            try:
                with socket.socket(socket.AF_UNIX) as sock:
                    sock.settimeout(0.1)
                    sock.connect(socket_path)
                break
            except (ConnectionRefusedError, FileNotFoundError, OSError):
                pass
        if time.monotonic() - t0 > deadline:
            context = ""
            if proc is not None and proc.returncode is not None:
                _stdout, stderr = proc.communicate(timeout=1)
                context = f" worker exited rc={proc.returncode} stderr={stderr.decode()!r}"
            raise TimeoutError(
                f"socket {socket_path!r} did not appear within {deadline}s.{context}"
            )
        if proc is not None and proc.poll() is not None:
            _stdout, stderr = proc.communicate(timeout=1)
            raise RuntimeError(f"worker exited rc={proc.returncode} stderr={stderr.decode()!r}")
        time.sleep(0.05)


def _run_cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 # Why: test harness invokes the project's own CLI via sys.executable with fixed args; no untrusted input in argv.
        [sys.executable, "-m", "taskq.cli", *args],
        capture_output=True,
        text=True,
        timeout=10,
        env=env if env is not None else os.environ,
    )


def _cleanup_worker(
    proc: subprocess.Popen[bytes] | None,
    socket_path: str | None,
) -> None:
    if proc is not None and proc.returncode is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    if socket_path is not None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(socket_path)


# ── Fixtures ───────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def health_schema_name(pg_dsn: str) -> str:
    """Prepare the health-integration schema and return its name."""
    schema = f"tq_h_{new_base62()}".lower()
    await _prepare_schema(pg_dsn, schema)
    return schema


# ── End-to-end happy path ───────────────────────────────────


def test_ti1_live_and_ready_happy(pg_dsn: str, health_schema_name: str) -> None:
    socket_path = _next_sock_path()
    proc: subprocess.Popen[bytes] | None = None
    try:
        proc = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        _wait_for_socket(socket_path, proc)

        live_result = _run_cli(
            "health",
            "live",
            env=_worker_env(pg_dsn, socket_path, health_schema_name),
        )
        assert live_result.returncode == 0, f"stderr: {live_result.stderr}"
        assert json.loads(live_result.stdout.strip()) == {"status": "ok"}

        ready_result = _run_cli(
            "health",
            "ready",
            env=_worker_env(pg_dsn, socket_path, health_schema_name),
        )
        assert ready_result.returncode == 0, f"stderr: {ready_result.stderr}"
        body = json.loads(ready_result.stdout.strip())
        assert body["ready"] is True
        assert body["redis_configured"] is False
        assert body["shutdown_phase"] is None
    finally:
        _cleanup_worker(proc, socket_path)


# ── Readiness 503 when PG paused ────────────────────────────


@pytest.mark.xdist_group(name="chaos")
def test_ti2_ready_fails_when_pg_stopped(
    pg_dsn: str,
    pg_container: object,  # PostgresContainer — dynamic inspect for stop/start
    health_schema_name: str,
) -> None:
    socket_path = _next_sock_path()
    proc: subprocess.Popen[bytes] | None = None
    try:
        proc = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        _wait_for_socket(socket_path, proc)

        worker_env = _worker_env(pg_dsn, socket_path, health_schema_name)

        pre = _run_cli("health", "ready", env=worker_env)
        assert pre.returncode == 0
        assert json.loads(pre.stdout.strip())["ready"] is True

        docker_container = pg_container._container  # type: ignore[attr-defined] # Why: testcontainers' _container is not part of the public PostgresContainer API; accessing the Docker SDK object to pause/unpause is unavoidable here.
        docker_container.pause()
        try:
            post_stop = _run_cli("health", "ready", env=worker_env)
            assert post_stop.returncode == 1, (
                f"expected exit 1 after PG pause, got {post_stop.returncode}; "
                f"stdout={post_stop.stdout!r} stderr={post_stop.stderr!r}"
            )
            # When the PG ping times out the worker may take longer than the CLI's
            # 2 s request timeout to respond — in that case stdout is empty and the
            # exit-1 alone is sufficient evidence. When a body is present, verify
            # the schema matches the acceptance definition.
            if post_stop.stdout.strip():
                post_stop_body = json.loads(post_stop.stdout.strip())
                assert post_stop_body["ready"] is False
                assert post_stop_body["shutdown_phase"] is None
        finally:
            docker_container.unpause()

        post_start = _run_cli("health", "ready", env=worker_env)
        assert post_start.returncode == 0, (
            f"stdout: {post_start.stdout}, stderr: {post_start.stderr}"
        )
        assert json.loads(post_start.stdout.strip())["ready"] is True
    finally:
        _cleanup_worker(proc, socket_path)


# ── FastAPI router 503 when PG stopped ──────────────────────


@pytest.mark.asyncio
async def test_ti3_fastapi_router_503_on_pg_stop(
    pg_dsn: str,
    health_schema_name: str,
) -> None:
    pytest.importorskip("fastapi")

    from contextlib import AsyncExitStack

    import asyncpg
    import httpx
    from fastapi import FastAPI

    from taskq.web.health import create_health_router
    from taskq.worker.deps import open_worker_deps

    socket_path = _next_sock_path()
    settings = _WS.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": health_schema_name,
            "health_socket_path": socket_path,
            "health_pg_ping_timeout": "2.0",
        }
    )

    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        app = FastAPI()
        router = create_health_router(deps)
        app.include_router(router)

        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type] # Why: httpx.ASGITransport accepts FastAPI app; pyright sees ASGIApp protocol mismatch from FastAPI.
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            pre: httpx.Response | None = None
            for _attempt in range(10):
                pre = await client.get("/jobs/health/ready")
                if pre.status_code == 200:
                    break
                await asyncio.sleep(0.1)
            assert pre is not None, "pre not assigned in retry loop"
            assert pre.status_code == 200, f"pre-pause ready not 200; body={pre.text}"
            pre_body = pre.json()
            assert pre_body["ready"] is True
            assert pre_body["shutdown_phase"] is None

            # Simulate PG unavailability by replacing the dispatcher pool
            # with a stub that raises on acquire. Avoids pausing the shared
            # container (which would affect ALL tests sharing it).
            class _UnavailablePool:
                def acquire(self, *, timeout: float | None = None) -> None:
                    raise asyncpg.PostgresConnectionError("simulated PG unavailability")

            original_pool = deps.dispatcher_pool
            deps.dispatcher_pool = _UnavailablePool()  # type: ignore[assignment]
            try:
                post_stop = await client.get("/jobs/health/ready")
                assert post_stop.status_code == 503, f"body={post_stop.text}"
                post_body = post_stop.json()
                assert post_body["ready"] is False
                assert post_body["shutdown_phase"] is None
            finally:
                deps.dispatcher_pool = original_pool  # type: ignore[assignment]

            post_start: httpx.Response | None = None
            for _attempt in range(10):
                post_start = await client.get("/jobs/health/ready")
                if post_start.status_code == 200:
                    break
                await asyncio.sleep(0.1)
            assert post_start is not None, "post_start not assigned in retry loop"
            assert post_start.status_code == 200, f"post-start body={post_start.text}"
            assert post_start.json()["ready"] is True
    finally:
        await stack.aclose()


# ── Readiness 503 after SIGTERM ─────────────────────────────


def test_ti4_shutdown_phase_after_sigterm(
    pg_dsn: str,
    health_schema_name: str,
) -> None:
    socket_path = _next_sock_path()
    worker_env = _worker_env(pg_dsn, socket_path, health_schema_name)

    last_err: str | None = None
    for iteration in range(5):
        proc = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        try:
            _wait_for_socket(socket_path, proc)

            pre = _run_cli("health", "ready", env=worker_env)
            assert pre.returncode == 0, f"pre-signal ready failed; iteration {iteration}"

            os.kill(proc.pid, signal.SIGTERM)
            time.sleep(0.5)

            post = _run_cli("health", "ready", env=worker_env)
            body = json.loads(post.stdout.strip())
            shutdown_phase = body.get("shutdown_phase")
            if (
                post.returncode == 1
                and shutdown_phase is not None
                and isinstance(shutdown_phase, int)
                and shutdown_phase >= 1
            ):
                return
            last_err = (
                f"iteration {iteration}: got returncode={post.returncode}, "
                f"shutdown_phase={shutdown_phase!r}, body={body}"
            )
        except AssertionError as e:
            last_err = str(e)
        finally:
            _cleanup_worker(proc, socket_path)

    pytest.fail(last_err or "no success after 5 iterations")


# ── Metrics endpoint integration ────────────────────────────


def test_ti5_metrics_endpoint(pg_dsn: str, health_schema_name: str) -> None:
    socket_path = _next_sock_path()
    proc: subprocess.Popen[bytes] | None = None
    try:
        proc = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        _wait_for_socket(socket_path, proc)

        worker_env = _worker_env(pg_dsn, socket_path, health_schema_name)

        result = _run_cli("health", "metrics", env=worker_env)
        assert result.returncode == 0, f"stderr: {result.stderr}"
        body = result.stdout.strip()
        assert "taskq_active_jobs" in body
        assert "taskq_is_leader" in body
        assert "taskq_shutdown_phase" in body
    finally:
        _cleanup_worker(proc, socket_path)


# ── Stale socket survives SIGKILL ───────────────────────────


def test_ti6_stale_socket_sigkill(pg_dsn: str, health_schema_name: str) -> None:
    socket_path = _next_sock_path()

    for iteration in range(5):
        proc_a = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        try:
            _wait_for_socket(socket_path, proc_a)
            assert os.path.exists(socket_path), (
                f"iteration {iteration}: socket not created by worker A"
            )

            os.kill(proc_a.pid, signal.SIGKILL)
            proc_a.wait(timeout=10)
            assert os.path.exists(socket_path), (
                f"iteration {iteration}: stale socket removed after SIGKILL (expected to persist)"
            )
        finally:
            if proc_a.returncode is None:
                proc_a.kill()
                proc_a.wait(timeout=5)

        proc_b = _spawn_worker(pg_dsn, socket_path, health_schema_name)
        try:
            _wait_for_socket(socket_path, proc_b)
            worker_env = _worker_env(pg_dsn, socket_path, health_schema_name)
            live = _run_cli("health", "live", env=worker_env)
            if live.returncode != 0:
                raise AssertionError(
                    f"iteration {iteration}: worker B liveness failed — "
                    f"stdout={live.stdout!r}, stderr={live.stderr!r}"
                )
            break
        finally:
            _cleanup_worker(proc_b, socket_path)
            # socket unlinked by cleanup — ready for next iteration
