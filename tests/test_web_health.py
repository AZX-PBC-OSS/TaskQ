"""Tests for taskq.web.health: create_health_router FastAPI router."""

import inspect
import re
from types import SimpleNamespace

import asyncpg
import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.health import create_health_router
from taskq.worker.health import build_ready_body, compute_health
from taskq.worker.shutdown import ShutdownPhase

pytestmark = pytest.mark.asyncio


class _FakeConn:
    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _AcquireCtx:
    def __init__(self, conn: _FakeConn | None = None, error: BaseException | None = None) -> None:
        self._conn = conn
        self._error = error

    async def __aenter__(self) -> _FakeConn:
        if self._error is not None:
            raise self._error
        assert self._conn is not None
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.acquire_calls = 0

    def acquire(self, timeout: float = 30.0) -> _AcquireCtx:
        self.acquire_calls += 1
        return _AcquireCtx(
            conn=_FakeConn() if self._error is None else None,
            error=self._error,
        )


def _make_deps(**overrides: object) -> SimpleNamespace:
    return SimpleNamespace(
        **{
            "shutdown_phase": ShutdownPhase.NONE,
            "dispatcher_pool": _StubPool(),
            "heartbeat_pool": _StubPool(),
            "settings": SimpleNamespace(
                health_pg_ping_timeout=0.2,
                max_heartbeat_failures=3,
                redis_url=None,
                health_socket_path="/tmp/taskq_health.sock",  # noqa: S108 # Why: test-only stub; no real file operations touch this path.
            ),
            "is_leader": SimpleNamespace(is_set=lambda: False),
            "active_jobs": SimpleNamespace(count=lambda: 2),
            "heartbeat_failures": 0,
            # WorkerDeps.redis_client (default None) — health reads it for redis_configured.
            "redis_client": None,
            **overrides,
        }
    )


# ── part 1: /live returns 200 ──────────────────────────────────


async def test_live_returns_200() -> None:
    """part 1. GET /jobs/health/live returns 200 and {"status":"ok"}."""
    deps = _make_deps()
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/live")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ── part 2: /ready returns 200 with fields ───────────────


async def test_ready_returns_200_with_fr4_fields() -> None:
    """part 2. GET /jobs/health/ready: 200; JSON; five fields;
    shutdown_phase is None when deps phase is NONE."""
    deps = _make_deps()
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/ready")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 200
    body = response.json()  # pyright: ignore[reportUnknownVariableType] # Why: response.json() return type is Any; pyright reports unknown.
    assert len(body) == 5
    assert "ready" in body
    assert "redis_configured" in body
    assert "active_jobs" in body
    assert "is_leader" in body
    assert "shutdown_phase" in body
    assert body["shutdown_phase"] is None
    assert body["redis_configured"] is False
    assert body["active_jobs"] == 2
    assert body["is_leader"] is False
    assert body["ready"] is True


# ── Readiness 503 when shutdown_phase set ─────────────────────────────


async def test_ready_503_when_draining() -> None:
    """Readiness 503 when shutdown_phase=DRAINING. Body has shutdown_phase=1."""
    deps = _make_deps(shutdown_phase=ShutdownPhase.DRAINING)
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/ready")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 503
    body = response.json()  # pyright: ignore[reportUnknownVariableType] # Why: response.json() return type is Any; pyright reports unknown.
    assert body["shutdown_phase"] == 1
    assert body["ready"] is False


# ── Readiness 503 when PG ping fails ──────────────────────────────────


async def test_ready_503_when_pg_ping_fails() -> None:
    """Readiness 503 when PG ping fails. Body has ready=false; pg_ping_ok is NOT on wire."""
    deps = _make_deps(
        dispatcher_pool=_StubPool(error=asyncpg.PostgresConnectionError("connection refused")),
    )
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/ready")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.

    assert response.status_code == 503
    body = response.json()  # pyright: ignore[reportUnknownVariableType] # Why: response.json() return type is Any; pyright reports unknown.
    assert body["ready"] is False
    # pg_ping_ok is internal — must NOT appear on the wire
    assert "pg_ping_ok" not in body
    # fields must all be present
    assert "ready" in body
    assert "redis_configured" in body
    assert "active_jobs" in body
    assert "is_leader" in body
    assert "shutdown_phase" in body
    assert len(body) == 5


# ── Import discipline ─────────────────────────────────────────────────


async def test_no_lazy_fastapi_import() -> None:
    """Import discipline. Verify the module source does not contain a try/except import pattern."""
    source = inspect.getsource(create_health_router)
    assert not re.search(r"try\s*:\s*import\s+fastapi", source)


# ── Helper-parity behavioural test ────────────────────────────────────


async def test_ready_body_matches_helper_none_healthy() -> None:
    """DoD Router /ready body matches build_ready_body + compute_health (NONE+healthy)."""
    deps = _make_deps()
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/ready")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.
    router_bytes = response.content  # pyright: ignore[reportUnknownVariableType] # Why: response.content type is bytes; pyright sees it as unknown due to upstream Any.

    report = await compute_health(deps)  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    helper_bytes = build_ready_body(report, deps)  # pyright: ignore[reportArgumentType] # Why: test duck-type; deps is SimpleNamespace; same pattern.

    assert router_bytes == helper_bytes


async def test_ready_body_matches_helper_draining_healthy() -> None:
    """DoD Router /ready body matches build_ready_body + compute_health (DRAINING+healthy)."""
    deps = _make_deps(shutdown_phase=ShutdownPhase.DRAINING)
    app = FastAPI()
    app.include_router(create_health_router(deps))  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    client = TestClient(app)

    response = client.get("/jobs/health/ready")  # pyright: ignore[reportUnknownVariableType] # Why: TestClient.get return type is Any; pyright reports unknown.
    router_bytes = response.content  # pyright: ignore[reportUnknownVariableType] # Why: response.content type is bytes; pyright sees it as unknown due to upstream Any.

    report = await compute_health(deps)  # pyright: ignore[reportArgumentType] # Why: test duck-type; same suppression pattern as test_health.py:53.
    helper_bytes = build_ready_body(report, deps)  # pyright: ignore[reportArgumentType] # Why: test duck-type; deps is SimpleNamespace; same pattern.

    assert router_bytes == helper_bytes
