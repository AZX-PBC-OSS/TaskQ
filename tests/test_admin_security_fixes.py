"""Tests for admin UI security fixes (S2, F5, F6).

Covers:
- schedule_run_now rejects disabled schedules
- schedule_run_now rate-limits rapid re-trigger (cooldown)
- schedule_run_now rejects when admin_actions_enabled is False
- job_cancel rejects when admin_actions_enabled is False
- create_router raises RuntimeError when no auth in non-dev with require_auth=True
- create_router succeeds when no auth in dev environment
- payload factory error redirect uses generic error code, not exception text
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state

_FAKE_UUID = UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture(autouse=True)
def _dev_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed by test runner via parameter injection.
    """Set TASKQ_ENVIRONMENT=dev for all tests so create_router's fail-closed
    auth check does not raise. Tests that need a non-dev environment override
    this with their own monkeypatch.setenv."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")


# ── Fake asyncpg pool / connection ─────────────────────────────────────────


class _ScheduleRunConn:
    """Fake asyncpg connection that returns configurable rows by query keyword."""

    def __init__(
        self,
        *,
        schedule_row: dict[str, Any] | None = None,
        actor_config_row: dict[str, Any] | None = None,
    ) -> None:
        self._schedule_row = schedule_row
        self._actor_config_row = actor_config_row

    async def fetchrow(self, query: str, *args: object) -> dict[str, Any] | None:
        if "cron_schedules" in query:
            return self._schedule_row
        if "actor_config" in query:
            return self._actor_config_row
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        return []

    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _AcquireCtx:
    def __init__(self, conn: _ScheduleRunConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ScheduleRunConn:
        return self._conn

    async def __aexit__(self, *args: object) -> None:
        pass


class _StubPool:
    def __init__(self, conn: _ScheduleRunConn) -> None:
        self._conn = conn

    def acquire(self, *, timeout: float | None = None) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


def _make_backend() -> Any:
    backend = MagicMock()
    backend.enqueue = AsyncMock()
    return backend


def _mount_router(
    pool: _StubPool,
    *,
    backend: Any | None = None,
    base_path: str = "/admin",
) -> tuple[FastAPI, TestClient]:
    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type; _StubPool satisfies asyncpg.Pool contract at runtime.
        base_path=base_path,
        backend=backend,
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app)
    return app, client


def _csrf_post(
    client: TestClient,
    path: str,
    *,
    follow_redirects: bool = False,
) -> Any:
    token = "test-csrf-token"
    client.cookies.set("taskq_csrf_token", token)
    return client.post(path, data={"csrf_token": token}, follow_redirects=follow_redirects)


# ── 1. schedule_run_now rejects disabled schedule ──────────────────────────


def test_schedule_run_now_rejects_disabled_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /admin/schedules/{id}/run on a disabled schedule redirects with
    ?error=schedule+is+disabled instead of enqueuing."""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    import taskq.web.admin.ops as ops_mod

    monkeypatch.setattr(ops_mod, "_last_schedule_run", {})

    conn = _ScheduleRunConn(
        schedule_row={
            "actor": "test_actor",
            "payload_factory": None,
            "enabled": False,
            "metadata": None,
        },
    )
    pool = _StubPool(conn)
    _app, client = _mount_router(pool, backend=_make_backend())

    resp = _csrf_post(client, f"/admin/schedules/{_FAKE_UUID}/run")

    assert resp.status_code == 303
    assert "error=schedule+is+disabled" in resp.headers["location"]


# ── 2. schedule_run_now rejects rapid re-trigger (cooldown) ─────────────────


def test_schedule_run_now_rejects_rapid_retrigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two rapid POSTs to /admin/schedules/{id}/run: the first enqueues, the
    second is rate-limited with ?error=schedule+run+on+cooldown."""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    import taskq.web.admin.ops as ops_mod

    monkeypatch.setattr(ops_mod, "_last_schedule_run", {})

    conn = _ScheduleRunConn(
        schedule_row={
            "actor": "test_actor",
            "payload_factory": None,
            "enabled": True,
            "metadata": None,
        },
        actor_config_row={
            "queue": "default",
            "max_attempts": 3,
            "retry_kind": "transient",
        },
    )
    pool = _StubPool(conn)
    backend = _make_backend()
    _app, client = _mount_router(pool, backend=backend)

    first = _csrf_post(client, f"/admin/schedules/{_FAKE_UUID}/run")
    assert first.status_code == 303
    assert "error=" not in first.headers["location"]

    second = _csrf_post(client, f"/admin/schedules/{_FAKE_UUID}/run")
    assert second.status_code == 303
    assert "error=schedule+run+on+cooldown" in second.headers["location"]

    backend.enqueue.assert_awaited_once()


# ── 3. schedule_run_now rejects when admin_actions_enabled is False ─────────


def test_schedule_run_now_rejects_when_actions_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /admin/schedules/{id}/run returns 403 when
    TASKQ_ADMIN_ACTIONS_ENABLED is not set (defaults to False)."""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")
    import taskq.web.admin.ops as ops_mod

    monkeypatch.setattr(ops_mod, "_last_schedule_run", {})

    conn = _ScheduleRunConn(
        schedule_row={
            "actor": "test_actor",
            "payload_factory": None,
            "enabled": True,
            "metadata": None,
        },
    )
    pool = _StubPool(conn)
    _app, client = _mount_router(pool, backend=_make_backend())

    resp = _csrf_post(client, f"/admin/schedules/{_FAKE_UUID}/run")

    assert resp.status_code == 403


# ── 4. create_router raises RuntimeError when no auth in non-dev ────────────


def test_create_router_raises_runtime_error_no_auth_non_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_router with auth_dependency=None in a non-dev environment raises
    RuntimeError when admin_ui_require_auth is True (the default)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.delenv("TASKQ_ADMIN_UI_REQUIRE_AUTH", raising=False)

    conn = _ScheduleRunConn()
    pool = _StubPool(conn)

    with pytest.raises(RuntimeError, match="auth_dependency"):
        create_router(
            pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
            auth_dependency=None,
        )


# ── 5. create_router succeeds when no auth in dev environment ───────────────


def test_create_router_succeeds_no_auth_dev_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_router with auth_dependency=None in a dev environment succeeds
    without raising."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    conn = _ScheduleRunConn()
    pool = _StubPool(conn)

    bundle = create_router(
        pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type.
        auth_dependency=None,
    )
    assert bundle.router is not None


# ── 6. payload factory error redirect uses generic error code ───────────────


def test_payload_factory_error_redirect_uses_generic_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /admin/schedules/{id}/run with an unresolvable payload_factory
    redirects with ?error=payload+factory+error — the exception text must not
    appear in the redirect URL."""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    import taskq.web.admin.ops as ops_mod

    monkeypatch.setattr(ops_mod, "_last_schedule_run", {})

    conn = _ScheduleRunConn(
        schedule_row={
            "actor": "test_actor",
            "payload_factory": "nonexistent.module.factory",
            "enabled": True,
            "metadata": None,
        },
    )
    pool = _StubPool(conn)
    _app, client = _mount_router(pool, backend=_make_backend())

    resp = _csrf_post(client, f"/admin/schedules/{_FAKE_UUID}/run")

    assert resp.status_code == 303
    location = resp.headers["location"]
    assert "error=payload+factory+error" in location
    assert "nonexistent" not in location
    assert "No+module+named" not in location


# ── 7. job_cancel rejects when admin_actions_enabled is False ──────────────


def test_job_cancel_rejects_when_actions_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /admin/jobs/{id}/cancel returns 403 when
    TASKQ_ADMIN_ACTIONS_ENABLED is not set (defaults to False)."""
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "false")

    backend = MagicMock()
    backend.get = AsyncMock(return_value=MagicMock(status="running"))
    backend.write_cancel_request = AsyncMock()

    conn = _ScheduleRunConn()
    pool = _StubPool(conn)
    _app, client = _mount_router(pool, backend=backend)

    resp = _csrf_post(client, f"/admin/jobs/{_FAKE_UUID}/cancel")

    assert resp.status_code == 403
    backend.write_cancel_request.assert_not_called()
