"""The /history admin panel is registered on the router (#110).

The history module shipped as ``_history.py`` — a leading underscore that
``_factory._discover_and_register`` skips — so the fully-implemented panel
was never attached to any admin router. The rename to ``history.py`` puts it
on the auto-discovery path; these tests pin that ``create_router`` actually
registers its routes, so a future rename back to an underscore-prefixed
module fails here instead of silently unregistering the panel.
"""

from datetime import UTC, datetime
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state


class _StubRecord(dict[str, object]):
    """Minimal asyncpg.Record duck type for testing."""


class _StubConn:
    """Minimal asyncpg.Connection duck type that returns empty results."""

    async def fetch(self, query: str, *args: object) -> list[_StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> _StubRecord | None:
        return None

    async def fetchval(self, query: str, *args: object) -> object:
        # The router-level clock-offset dependency probes the database clock;
        # answer with a timestamp the way the real database does.
        if "clock_timestamp()" in query:
            return datetime.now(UTC)
        return False

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _AcquireCtx:
    def __init__(self, conn: _StubConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _StubConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    """Minimal asyncpg.Pool duck type yielding one stub connection."""

    def acquire(self, *args: Any, **kwargs: Any) -> _AcquireCtx:
        return _AcquireCtx(_StubConn())


def _make_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Build a TestClient around a full admin router (history auto-discovered)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool; asyncpg.Pool is a protocol this stub satisfies at runtime.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def test_history_routes_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_router auto-discovers history.py: /history and /api/history/stats exist."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(_StubPool())  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]
    assert "/history" in route_paths
    assert "/api/history/stats" in route_paths


def test_history_page_returns_200(monkeypatch: pytest.MonkeyPatch) -> None:
    """GET /history over the test client returns 200 HTML."""
    client = _make_client(monkeypatch)
    response = client.get("/history")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
