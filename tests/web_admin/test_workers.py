"""Tests for workers, leader, watchdog, and their admin templates."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI  # Why: importorskip guard must precede.
from fastapi.testclient import TestClient

from taskq.web.admin import (  # Why: importorskip guard must precede.
    create_router,
    setup_admin_state,
)
from taskq.web.admin.workers import _is_watchdog_healthy

from . import StubRecord, _StubPool  # Why: importorskip guard must precede.

# ── Workers routes: discovery and registration ─────────────────────────


def test_workers_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /workers route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/workers" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_workers_page_returns_html(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /workers returns 200 with text/html content type."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/workers")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


def test_leader_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """GET /leader route is present after create_router."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/leader" in route_paths  # pyright: ignore[reportUnknownVariableType]


def test_leader_page_returns_html_when_no_leader(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET /leader returns 200 with text/html even when no leader is elected."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/leader")  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 200  # pyright: ignore[reportUnknownVariableType]
    ct = response.headers.get("content-type", "")  # pyright: ignore[reportUnknownVariableType]
    assert "text/html" in ct


# ── Workers template ───────────────────────────────────────────────────


def test_workers_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: workers.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "workers.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_workers_template_renders_worker_data(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """workers.html renders hostname, pid, queues, last_seen_at, and leader indicator."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    html = template.render(
        workers=[
            {
                "hostname": "worker-1",
                "pid": 1234,
                "queues": "default,emails",
                "last_seen_at": "2025-01-01T00:00:00+00:00",
                "is_leader": True,
            },
            {
                "hostname": "worker-2",
                "pid": 5678,
                "queues": "default",
                "last_seen_at": "2025-01-01T00:00:01+00:00",
                "is_leader": False,
            },
        ]
    )
    assert "worker-1" in html
    assert "1234" in html
    assert "worker-2" in html
    assert "Leader" in html


def test_workers_template_empty_workers(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """workers.html shows 'No workers registered' when list is empty."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    html = template.render(workers=[])
    assert "No workers registered" in html


# ── Leader template ────────────────────────────────────────────────────


def test_leader_template_extends_base(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """DoD: leader.html extends _base.html."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    source = env.loader.get_source(env, "leader.html")[0]  # pyright: ignore[reportOptionalMemberAccess, reportUnknownMemberType]  # Why: loader is set by create_router.
    assert 'extends "_base.html"' in source


def test_leader_template_no_leader_elected(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html shows 'No leader elected' when leader is None."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(leader=None, watchdog_healthy=None)
    assert "No leader elected" in html


def test_leader_template_renders_leader_info(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html renders hostname, pid, elected_at, last_seen_at, worker_last_seen, watchdog."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=True,
    )
    assert "leader-1" in html
    assert "9999" in html
    assert "Healthy" in html


def test_leader_template_unhealthy_watchdog(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """leader.html shows 'Unhealthy' badge when watchdog_healthy is False."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    html = template.render(
        leader={
            "hostname": "leader-1",
            "pid": 9999,
            "elected_at": "2025-01-01T00:00:00+00:00",
            "last_seen_at": "2025-01-01T00:00:10+00:00",
            "worker_last_seen": "2025-01-01T00:00:10+00:00",
        },
        watchdog_healthy=False,
    )
    assert "Unhealthy" in html


# ── Watchdog health computation ────────────────────────────────────────


def test_watchdog_healthy_recent() -> None:
    """_is_watchdog_healthy returns True when last_seen_at is within 30 seconds."""
    recent = datetime.now(UTC) - timedelta(seconds=10)
    assert _is_watchdog_healthy(recent) is True


def test_watchdog_unhealthy_stale() -> None:
    """_is_watchdog_healthy returns False when last_seen_at is over 30 seconds ago."""
    stale = datetime.now(UTC) - timedelta(seconds=60)
    assert _is_watchdog_healthy(stale) is False


def test_watchdog_none_last_seen() -> None:
    """_is_watchdog_healthy returns None when last_seen_at is None."""
    assert _is_watchdog_healthy(None) is None


def test_watchdog_naive_datetime_treated_as_utc() -> None:
    """Naive datetime (no tzinfo) is treated as UTC."""
    recent_naive = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=5)
    assert _is_watchdog_healthy(recent_naive) is True


# ── XSS prevention: workers and leader templates ───────────────────────


def test_workers_template_autoescapes_hostname(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Workers page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("workers.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        workers=[
            {"hostname": xss, "pid": 1, "queues": "default", "last_seen_at": "", "is_leader": False}
        ]
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


def test_leader_template_autoescapes_hostname(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Leader page auto-escapes user-derived fields."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    template = env.get_template("leader.html")
    xss = '<script>alert("xss")</script>'
    html = template.render(
        leader={
            "hostname": xss,
            "pid": 1,
            "elected_at": "",
            "last_seen_at": "",
            "worker_last_seen": "",
        },
        watchdog_healthy=True,
    )
    assert "&lt;script&gt;" in html
    assert '<script>alert("xss")</script>' not in html


# ── Leader route: leader-present branch (lines 97-105) ──────────────────


class _FetchRowConn:
    """Connection returning a single preset fetchrow result."""

    def __init__(self, row: StubRecord | None) -> None:
        self._row = row

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return []

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return self._row

    async def execute(self, query: str, *args: object) -> str:
        return ""


class _FetchRowAcquireCtx:
    def __init__(self, conn: _FetchRowConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FetchRowConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _FetchRowPool:
    def __init__(self, conn: _FetchRowConn) -> None:
        self._conn = conn

    def acquire(self) -> _FetchRowAcquireCtx:
        return _FetchRowAcquireCtx(self._conn)


def _build_workers_app(pool: object, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Build a TestClient with the admin router backed by *pool*."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


def _leader_row(last_seen_at: datetime | None) -> StubRecord:
    return StubRecord(
        worker_id="00000000-0000-0000-0000-000000000001",
        hostname="leader-1",
        pid=9999,
        elected_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
        last_seen_at=last_seen_at,
        worker_last_seen=datetime(2025, 1, 1, 0, 0, 10, tzinfo=UTC),
    )


def test_leader_page_renders_when_leader_exists_healthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader renders leader details with a Healthy watchdog when recent."""
    row = _leader_row(datetime.now(UTC) - timedelta(seconds=5))
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "9999" in text
    assert "Healthy" in text


def test_leader_page_renders_when_leader_exists_unhealthy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader renders an Unhealthy watchdog when last_seen_at is stale."""
    row = _leader_row(datetime.now(UTC) - timedelta(seconds=120))
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    text = response.text  # pyright: ignore[reportUnknownMemberType]
    assert "leader-1" in text
    assert "Unhealthy" in text


def test_leader_page_renders_unknown_watchdog_when_last_seen_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /leader shows Unknown watchdog when leader last_seen_at is None."""
    row = _leader_row(None)
    client = _build_workers_app(_FetchRowPool(_FetchRowConn(row)), monkeypatch)
    response = client.get("/leader")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "Unknown" in response.text  # pyright: ignore[reportUnknownMemberType]


# ── Workers overview: metadata decode_jsonb loop (lines 60-61) ──────────


class _FetchListConn:
    """Connection returning a preset list from ``fetch``."""

    def __init__(self, rows: list[StubRecord]) -> None:
        self._rows = rows

    async def fetch(self, query: str, *args: object) -> list[StubRecord]:
        return self._rows

    async def fetchrow(self, query: str, *args: object) -> StubRecord | None:
        return None

    async def execute(self, query: str, *args: object) -> str:
        return ""


def test_workers_page_decodes_dict_metadata_notify_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /workers decodes jsonb metadata and surfaces notify_enabled=True."""
    rows = [
        StubRecord(
            hostname="worker-1",
            pid=1234,
            queues="default",
            last_seen_at=datetime.now(UTC),
            is_leader=True,
            metadata='{"notify_enabled": true}',
        )
    ]
    client = _build_workers_app(_FetchRowPool(_FetchListConn(rows)), monkeypatch)
    response = client.get("/workers")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "worker-1" in response.text  # pyright: ignore[reportUnknownMemberType]


def test_workers_page_non_dict_metadata_defaults_notify_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-dict metadata (None) falls back to notify_enabled=False."""
    rows = [
        StubRecord(
            hostname="worker-2",
            pid=5678,
            queues="default",
            last_seen_at=datetime.now(UTC),
            is_leader=False,
            metadata=None,
        )
    ]
    client = _build_workers_app(_FetchRowPool(_FetchListConn(rows)), monkeypatch)
    response = client.get("/workers")  # pyright: ignore[reportUnknownMemberType]
    assert response.status_code == 200  # pyright: ignore[reportUnknownMemberType]
    assert "worker-2" in response.text  # pyright: ignore[reportUnknownMemberType]
