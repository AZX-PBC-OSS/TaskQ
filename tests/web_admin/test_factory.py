"""Tests for taskq.web.admin: create_router factory function and related invariants."""

import inspect
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from fastapi import FastAPI
from fastapi.testclient import TestClient

import taskq.web.admin._factory as _factory_mod
import taskq.web.admin._static as _static_mod
import taskq.web.admin.jobs as _jobs_mod
import taskq.web.admin.ops as _ops_mod
import taskq.web.admin.queues as _queues_mod
import taskq.web.admin.sse as _sse_mod
import taskq.web.admin.workers as _workers_mod
from taskq.web.admin import AdminBundle, create_router, setup_admin_state

from . import _StubPool

# ── create_router returns APIRouter ────────────────────────────────────


def test_create_router_returns_api_router(stub_pool: _StubPool) -> None:
    """create_router returns an AdminBundle whose .router is a FastAPI APIRouter with a GET / route."""
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    assert hasattr(bundle.router, "routes")
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed; test inspects route shape.
    assert "/" in route_paths


# ── Schema validation ──────────────────────────────────────────────────


def test_invalid_schema_raises_value_error(stub_pool: _StubPool) -> None:
    """Schema parameter validated against _IDENT_RE; rejects SQL-unsafe names."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        create_router(stub_pool, schema="; DROP TABLE jobs--")  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.


# ── : GET / redirects to /queues with 302 ─────────────────────────


def test_root_redirects_to_queues(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """GET / returns 302 redirect to queues (relative URL for prefix-safe routing)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/", follow_redirects=False)  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 302  # pyright: ignore[reportUnknownVariableType]
    assert response.headers.get("location", "") == "queues"  # pyright: ignore[reportUnknownVariableType]


def test_root_redirect_is_prefix_safe(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Regression: relative redirect resolves correctly when router is mounted at a prefix.

    The standard deployment mounts the router at /admin. An absolute
    /queues redirect would send the browser to /queues (404), not /admin/queues.
    A relative ``queues`` redirect resolves against the current path.
    """
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app)
    response = client.get("/admin/", follow_redirects=False)  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 302  # pyright: ignore[reportUnknownVariableType]
    location = response.headers.get("location", "")  # pyright: ignore[reportUnknownVariableType]
    assert location == "queues"  # pyright: ignore[reportUnknownVariableType]


# ── : Jinja2 Environment with autoescape and PackageLoader ──────


def test_jinja2_env_autoescape_and_package_loader(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """Jinja2 env has autoescape=True and PackageLoader."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    env = bundle.templates
    assert env.autoescape is True
    from jinja2 import PackageLoader

    assert isinstance(env.loader, PackageLoader)


# ── : poll_interval_ms injected as global ──────────────────────────


def test_poll_interval_ms_global_injected(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """env.globals['poll_interval_ms'] equals 2000 with default polling interval of 2.0s."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    assert bundle.templates.globals["poll_interval_ms"] == 2000


def test_poll_interval_ms_custom_value(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """env.globals['poll_interval_ms'] reflects TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS", "5.0")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    assert bundle.templates.globals["poll_interval_ms"] == 5000


# ── : auth_dependency applied as router-level Depends ──────────────


def test_auth_dependency_applied_to_all_routes(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """When auth_dependency is provided, routes return 401 when it denies."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    from fastapi import HTTPException

    def deny_auth() -> None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    client = make_app(auth_dependency=deny_auth)
    response = client.get("/", follow_redirects=False)  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 401  # pyright: ignore[reportUnknownVariableType]


def test_no_auth_dependency_routes_are_accessible(
    monkeypatch: pytest.MonkeyPatch, make_app: Callable[..., Any]
) -> None:
    """When auth_dependency is None, routes are accessible without auth."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = make_app()
    response = client.get("/", follow_redirects=False)  # pyright: ignore[reportUnknownVariableType]  # Why: TestClient.get return type is Any.
    assert response.status_code == 302  # pyright: ignore[reportUnknownVariableType]


# ── : WARNING when no auth in non-dev environment ──────────────────


def test_warning_logged_when_no_auth_non_dev(
    monkeypatch: pytest.MonkeyPatch,
    stub_pool: _StubPool,
    structlog_capture: list[dict[str, Any]],
) -> None:
    """WARNING with event name admin-ui-no-auth when no auth in production
    with admin_ui_require_auth explicitly set to false (opt-out path)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKQ_ADMIN_UI_REQUIRE_AUTH", "false")
    create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    warning_events = [
        e
        for e in structlog_capture
        if e.get("event") == "admin-ui-no-auth" and e.get("log_level") == "warning"
    ]
    assert len(warning_events) >= 1


def test_no_warning_when_no_auth_in_dev(
    monkeypatch: pytest.MonkeyPatch,
    stub_pool: _StubPool,
    structlog_capture: list[dict[str, Any]],
) -> None:
    """No WARNING about unauthenticated when env is dev."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    admin_warnings = [e for e in structlog_capture if e.get("event") == "admin-ui-no-auth"]
    assert len(admin_warnings) == 0


def test_no_warning_when_no_auth_in_development(
    monkeypatch: pytest.MonkeyPatch,
    stub_pool: _StubPool,
    structlog_capture: list[dict[str, Any]],
) -> None:
    """No WARNING about unauthenticated when env is development."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "development")
    create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    admin_warnings = [e for e in structlog_capture if e.get("event") == "admin-ui-no-auth"]
    assert len(admin_warnings) == 0


# ── No import from taskq.worker ──────────────────────────────────────


def test_no_worker_import() -> None:
    """NThe package does not import from taskq.worker at module level.

    Lazy imports inside function bodies (e.g. ``rate_limits_page`` importing
    ``WorkerSettings`` on-demand) are allowed — they avoid module-level coupling
    and are only resolved when the handler is actually invoked.  The
    ``inspect.getsource`` check cannot distinguish module-level from function-body
    imports, so ``_ops_mod`` is excluded here.
    """
    for mod in (
        _factory_mod,
        _static_mod,
        _queues_mod,
        _jobs_mod,
        _workers_mod,
        # _ops_mod: contains a lazy import from taskq.worker.deps inside the
        # rate_limits_page handler body, not at module level.  Excluded from
        # this module-level import check — see docstring above.
        _sse_mod,
    ):
        source = inspect.getsource(mod)
        assert "taskq.worker" not in source


# ── No from __future__ import annotations ──────────────────────────────


def test_no_future_annotations() -> None:
    """No from __future__ import annotations."""
    for mod in (
        _factory_mod,
        _static_mod,
        _queues_mod,
        _jobs_mod,
        _workers_mod,
        _ops_mod,
        _sse_mod,
    ):
        source = inspect.getsource(mod)
        assert "from __future__ import annotations" not in source


# ── Public surface preserved ──────────────────────────────────────────


def test_public_surface_create_router() -> None:
    """from taskq.web.admin import create_router works after package refactor."""
    assert callable(create_router)


def test_public_surface_admin_bundle() -> None:
    """from taskq.web.admin import AdminBundle works after package refactor."""
    assert AdminBundle is not None


def test_public_surface_setup_admin_state() -> None:
    """from taskq.web.admin import setup_admin_state works after package refactor."""
    assert callable(setup_admin_state)


# ── Auto-discovery mechanism ───────────────────────────────────────────


def test_static_route_registered_via_discovery(
    monkeypatch: pytest.MonkeyPatch, stub_pool: _StubPool
) -> None:
    """The static route is registered via _static.register, not hardcoded in _factory."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    bundle = create_router(stub_pool)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    route_paths = [getattr(r, "path", None) for r in bundle.router.routes]  # pyright: ignore[reportUnknownVariableType]  # Why: APIRouter.routes is not fully typed.
    assert "/static/{path:path}" in route_paths


# ── _time_ago filter ────────────────────────────────────────────────────


def test_time_ago_none_returns_dash() -> None:
    """_time_ago returns '—' for None."""
    from taskq.web.admin._factory import _time_ago

    assert _time_ago(None) == "—"


def test_time_ago_empty_string_returns_dash() -> None:
    """_time_ago returns '—' for empty string."""
    from taskq.web.admin._factory import _time_ago

    assert _time_ago("") == "—"


def test_time_ago_iso_string_returns_humanized() -> None:
    """_time_ago returns a humanized string for an ISO datetime string."""
    from datetime import UTC, datetime

    from taskq.web.admin._factory import _time_ago

    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    result = _time_ago(past)
    assert isinstance(result, str)
    assert result != "—"


def test_time_ago_datetime_returns_humanized() -> None:
    """_time_ago returns a humanized string for a datetime object."""
    from datetime import UTC, datetime

    from taskq.web.admin._factory import _time_ago

    past = datetime.now(UTC) - timedelta(hours=2)
    result = _time_ago(past)
    assert isinstance(result, str)
    assert result != "—"


def test_time_ago_naive_datetime_adds_utc() -> None:
    """_time_ago handles naive datetimes by adding UTC."""
    from datetime import datetime

    from taskq.web.admin._factory import _time_ago

    past = datetime(2020, 1, 1)
    result = _time_ago(past)
    assert isinstance(result, str)
    assert result != "—"


def test_time_ago_int_returns_str() -> None:
    """_time_ago returns str(int) for non-datetime, non-string types."""
    from taskq.web.admin._factory import _time_ago

    assert _time_ago(42) == "42"


def test_time_ago_invalid_string_falls_back() -> None:
    """_time_ago returns the raw string on parse failure."""
    from taskq.web.admin._factory import _time_ago

    assert _time_ago("not-a-date") == "not-a-date"


# ── _iso_attr filter ────────────────────────────────────────────────────


def test_iso_attr_none_returns_empty() -> None:
    """_iso_attr returns '' for None."""
    from taskq.web.admin._factory import _iso_attr

    assert _iso_attr(None) == ""


def test_iso_attr_datetime_returns_isoformat() -> None:
    """_iso_attr returns isoformat for a datetime."""
    from datetime import UTC, datetime

    from taskq.web.admin._factory import _iso_attr

    dt = datetime(2025, 1, 1, tzinfo=UTC)
    assert _iso_attr(dt) == dt.isoformat()


def test_iso_attr_string_returns_string() -> None:
    """_iso_attr returns the string as-is."""
    from taskq.web.admin._factory import _iso_attr

    assert _iso_attr("2025-01-01T00:00:00+00:00") == "2025-01-01T00:00:00+00:00"


def test_iso_attr_int_returns_str() -> None:
    """_iso_attr returns str(value) for other types."""
    from taskq.web.admin._factory import _iso_attr

    assert _iso_attr(42) == "42"


# ── GZipStaticOnly middleware ───────────────────────────────────────────


async def test_gzip_static_only_bypasses_non_static() -> None:
    """GZipStaticOnly calls the wrapped app directly for non-static paths."""
    from taskq.web.admin._factory import GZipStaticOnly

    called_directly: list[str] = []

    class _FakeApp:
        async def __call__(self, scope: dict[str, object], receive: object, send: object) -> None:
            called_directly.append(str(scope.get("path", "")))

    middleware = GZipStaticOnly(_FakeApp())  # pyright: ignore[reportArgumentType]
    await middleware({"type": "http", "path": "/jobs"}, None, None)
    assert called_directly == ["/jobs"]
