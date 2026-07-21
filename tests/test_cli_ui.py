"""Tests for taskq.cli ui sub-app: serve command and settings resolution."""

import pytest
from typer.testing import CliRunner

from taskq.cli import app
from taskq.settings import TaskQSettings
from taskq.testing.assertions import plain_cli_output

runner = CliRunner()


def _make_fake_serve(calls: dict[str, object]) -> object:
    def _fake_serve(
        pg_dsn: str,
        schema: str,
        redis_url: str | None,
        host: str,
        port: int,
        run_migrate: bool = False,
        settings: object = None,
    ) -> None:
        calls["pg_dsn"] = pg_dsn
        calls["schema"] = schema
        calls["redis_url"] = redis_url
        calls["host"] = host
        calls["port"] = port
        calls["run_migrate"] = run_migrate

    return _fake_serve


def _invoke_serve(
    monkeypatch: object, extra_args: list[str] | None = None, env: dict[str, str] | None = None
) -> dict[str, object]:
    import taskq.cli as cli_mod

    calls: dict[str, object] = {}
    monkeypatch.setattr(cli_mod, "_ui_serve", _make_fake_serve(calls))  # type: ignore[arg-type] # Why: monkeypatch stub; runtime duck-type is compatible.
    args = ["ui", "serve", *(extra_args or [])]
    runner.invoke(app, args, env=env)
    return calls


def _dev_settings(monkeypatch: pytest.MonkeyPatch) -> TaskQSettings:
    """Load TaskQSettings with TASKQ_ENVIRONMENT=dev so create_router's
    fail-closed auth check doesn't raise."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    return TaskQSettings.load()


# ── ui_app is wired into root Typer app ──────────────────────────────────


def test_ui_sub_app_registered() -> None:
    """ui_app is wired into root Typer app — 'taskq ui --help' succeeds."""
    result = runner.invoke(app, ["ui", "--help"])
    assert result.exit_code == 0, result.stderr
    assert "serve" in plain_cli_output(result.output).lower()


def test_ui_serve_help_displays_options() -> None:
    """taskq ui serve --help shows all CLI options."""
    result = runner.invoke(app, ["ui", "serve", "--help"])
    assert result.exit_code == 0, result.stderr
    plain = plain_cli_output(result.output)
    for opt in ("--pg-dsn", "--schema", "--redis-url", "--host", "--port"):
        assert opt in plain


# ── Settings resolution: dotenvmodel defaults ────────────────────────────


def test_ui_serve_resolves_dsn_from_settings(monkeypatch: object) -> None:
    """When --pg-dsn is not passed, DSN falls back to TaskQSettings.pg_dsn."""
    calls = _invoke_serve(
        monkeypatch,
        env={"TASKQ_PG_DSN": "postgresql://u:p@h:5432/db"},
    )
    assert calls["pg_dsn"] == "postgresql://u:p@h:5432/db"
    assert calls["schema"] == "taskq"
    assert calls["host"] == "0.0.0.0"  # noqa: S104 # Why: verifying the default bind address, not a real bind.
    assert calls["port"] == 8080
    assert calls["redis_url"] is None


def test_ui_serve_cli_overrides_settings(monkeypatch: object) -> None:
    """CLI flags override dotenvmodel settings values."""
    calls = _invoke_serve(
        monkeypatch,
        extra_args=[
            "--pg-dsn",
            "postgresql://cli:host@db:5432/mydb",
            "--schema",
            "custom",
            "--redis-url",
            "redis://clihost:6379/0",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
        ],
    )
    assert calls["pg_dsn"] == "postgresql://cli:host@db:5432/mydb"
    assert calls["schema"] == "custom"
    assert calls["redis_url"] == "redis://clihost:6379/0"
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 9090


def test_ui_serve_host_port_from_env(monkeypatch: object) -> None:
    """TASKQ_ADMIN_HOST and TASKQ_ADMIN_PORT env vars flow through settings."""
    calls = _invoke_serve(
        monkeypatch,
        env={
            "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
            "TASKQ_ADMIN_HOST": "192.168.1.1",
            "TASKQ_ADMIN_PORT": "3000",
        },
    )
    assert calls["host"] == "192.168.1.1"
    assert calls["port"] == 3000


def test_ui_serve_redis_url_from_env(monkeypatch: object) -> None:
    """TASKQ_REDIS_URL env var (shared with worker) flows through to redis_url."""
    calls = _invoke_serve(
        monkeypatch,
        env={
            "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
            "TASKQ_REDIS_URL": "redis://redis-host:6379/1",
        },
    )
    assert calls["redis_url"] == "redis://redis-host:6379/1"


def test_ui_serve_schema_from_env(monkeypatch: object) -> None:
    """TASKQ_SCHEMA_NAME env var (shared with worker) flows through to schema."""
    calls = _invoke_serve(
        monkeypatch,
        env={
            "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
            "TASKQ_SCHEMA_NAME": "myschema",
        },
    )
    assert calls["schema"] == "myschema"


# ── _ui_serve wires FastAPI + router correctly ──────────────────────────


def test_ui_serve_lifespan_mounts_admin_router(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_ui_serve lifespan creates pool, mounts router, sets app.state, and closes pool on shutdown."""
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager

    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    pool_closed = False

    class _FakeConn:
        async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
            return []

        async def execute(self, query: str, *args: object) -> str:
            return "SELECT 1"

    class _FakeAcquire:
        async def __aenter__(self) -> _FakeConn:
            return _FakeConn()

        async def __aexit__(self, *a: object) -> None:
            pass

    class _CloseablePool:
        def acquire(self) -> _FakeAcquire:
            return _FakeAcquire()

        async def close(self) -> None:
            nonlocal pool_closed
            pool_closed = True

    pool = _CloseablePool()

    @asynccontextmanager
    async def lifespan(fa_app: FastAPI) -> AsyncGenerator[None]:
        bundle = create_router(pool, schema="taskq", redis_client=None)

        setup_admin_state(fa_app, bundle)
        fa_app.include_router(bundle.router, prefix="/admin")

        yield

        await pool.close()

    fa_app = FastAPI(lifespan=lifespan)

    from fastapi.testclient import TestClient

    with TestClient(fa_app) as client:
        response = client.get("/admin/queues")
        assert response.status_code == 200

    assert pool_closed, "lifespan shutdown did not close the pool"


# ── Regression: _ui_serve calls uvicorn.run directly (no nested asyncio.run) ──


def test_ui_serve_calls_uvicorn_run_with_correct_args(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real _ui_serve invokes uvicorn.run(app, host, port) — no asyncio.Runner wrapping.

    Regression for nested-asyncio.run RuntimeError: _ui_serve must be
    synchronous so uvicorn.run() can create its own event loop.
    """
    import uvicorn

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app
        captured["kwargs"] = kwargs

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    uvicorn_kwargs = captured.get("kwargs")
    assert isinstance(uvicorn_kwargs, dict)
    assert uvicorn_kwargs.get("host") == "127.0.0.1"
    assert uvicorn_kwargs.get("port") == 9999
    assert captured["app"] is not None

    from fastapi import FastAPI

    assert isinstance(captured["app"], FastAPI)


# ── _ui_serve lifespan body: pool creation, redis, migrate, root redirect ──


class _FakePool:
    """Minimal asyncpg.Pool stand-in accepted by create_router."""


class _FakeAsyncCM:
    """Generic async context manager yielding a fixed value."""

    def __init__(self, value: object) -> None:
        self._value = value
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> object:
        self.entered = True
        return self._value

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


def test_ui_serve_lifespan_creates_pool_and_redirects_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_ui_serve's lifespan creates a pg pool via asyncpg.create_pool and mounts /admin.

    Also covers the root-path redirect route (GET / -> 307 to /admin/).
    """
    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    pool_cm = _FakeAsyncCM(_FakePool())
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool_cm)

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)

    with TestClient(app) as client:
        response = client.get("/", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/admin/"

    assert pool_cm.entered is True
    assert pool_cm.exited is True


def test_ui_serve_lifespan_runs_migration_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_ui_serve's lifespan calls migrate_mod.apply_pending_locked when run_migrate=True."""
    from unittest.mock import AsyncMock

    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    pool_cm = _FakeAsyncCM(_FakePool())
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool_cm)
    apply_pending_locked_mock = AsyncMock(return_value=[])
    monkeypatch.setattr(cli_mod.migrate_mod, "apply_pending_locked", apply_pending_locked_mock)

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="custom_schema",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=True,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)

    with TestClient(app):
        pass

    apply_pending_locked_mock.assert_awaited_once_with(
        "postgresql://u:p@h:5432/db", schema="custom_schema"
    )


def test_ui_serve_lifespan_creates_redis_client_when_redis_url_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_ui_serve's lifespan enters the redis async context manager when redis_url is set."""
    import redis.asyncio as aioredis
    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    pool_cm = _FakeAsyncCM(_FakePool())
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool_cm)
    redis_cm = _FakeAsyncCM(object())
    monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: redis_cm)

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url="redis://localhost:6379/0",
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)

    with TestClient(app):
        pass

    assert redis_cm.entered is True
    assert redis_cm.exited is True


def test_ui_serve_lifespan_redis_import_error_wrapped_with_install_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When redis_url is set but the [redis] extra isn't importable, raise a helpful ImportError.

    Simulated by forcing `import redis.asyncio` to fail via sys.modules poisoning
    (setting a module to None makes CPython raise ImportError on import), since
    the redis package is actually installed in this dev environment.
    """
    import sys

    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)

    pool_cm = _FakeAsyncCM(_FakePool())
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool_cm)
    monkeypatch.setitem(sys.modules, "redis.asyncio", None)

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url="redis://localhost:6379/0",
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)

    with pytest.raises(ImportError, match="taskq\\[redis\\]"), TestClient(app):
        pass


# ── Health and metrics endpoints wired into _ui_serve ─────────────────────


class _HealthFakeConn:
    async def execute(self, query: str, *args: object) -> str:
        return "SELECT 1"


class _HealthFakeAcquire:
    async def __aenter__(self) -> _HealthFakeConn:
        return _HealthFakeConn()

    async def __aexit__(self, *a: object) -> None:
        pass


class _HealthFakePool:
    def acquire(self) -> _HealthFakeAcquire:
        return _HealthFakeAcquire()


def _capture_app(monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, object], pytest.MonkeyPatch]:
    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)
    pool_cm = _FakeAsyncCM(_HealthFakePool())
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool_cm)
    return captured, monkeypatch


def test_ui_serve_health_live_returns_200(monkeypatch: pytest.MonkeyPatch) -> None:
    """_ui_serve mounts /jobs/health/live and it returns 200."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/jobs/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


def test_ui_serve_health_ready_returns_200(monkeypatch: pytest.MonkeyPatch) -> None:
    """_ui_serve mounts /jobs/health/ready and it returns 200 with PG ping."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/jobs/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ready"] is True
        assert body["pg_ping_ok"] is True


def test_ui_serve_health_token_protects_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    """When TASKQ_HEALTH_TOKEN is set, health endpoints require a bearer token."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    settings = _dev_settings(monkeypatch)
    settings.health_token = "secret-health-token"

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        # Missing token → 401
        resp = client.get("/jobs/health/live")
        assert resp.status_code == 401
        # Wrong token → 401
        resp = client.get(
            "/jobs/health/live",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401
        # Valid token → 200
        resp = client.get(
            "/jobs/health/live",
            headers={"Authorization": "Bearer secret-health-token"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # Same token requirement applies to /ready, not just /live.
        resp = client.get("/jobs/health/ready")
        assert resp.status_code == 401
        resp = client.get(
            "/jobs/health/ready",
            headers={"Authorization": "Bearer secret-health-token"},
        )
        assert resp.status_code == 200

        # ...and to /metrics, when taskq[prometheus] is installed.
        if _prometheus_available():
            resp = client.get("/jobs/health/metrics")
            assert resp.status_code == 401
            resp = client.get(
                "/jobs/health/metrics",
                headers={"Authorization": "Bearer secret-health-token"},
            )
            assert resp.status_code == 200


def _prometheus_available() -> bool:
    import importlib.util

    try:
        return importlib.util.find_spec("opentelemetry.exporter.prometheus") is not None
    except ModuleNotFoundError:
        return False


def test_ui_serve_metrics_endpoint_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """_ui_serve mounts /jobs/health/metrics when taskq[prometheus] is installed."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/jobs/health/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers.get("content-type", "")


# ── health_require_token fail-closed default (mirrors admin_ui_require_auth) ──


def test_ui_serve_raises_runtime_error_no_health_token_non_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_ui_serve with health_token empty in a non-dev environment raises
    RuntimeError when health_require_token is True (the default)."""
    from taskq.cli import _ui_serve

    _capture_app(monkeypatch)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.delenv("TASKQ_HEALTH_REQUIRE_TOKEN", raising=False)
    settings = TaskQSettings.load()

    with pytest.raises(RuntimeError, match="TASKQ_HEALTH_TOKEN"):
        _ui_serve(
            pg_dsn="postgresql://u:p@h:5432/db",
            schema="taskq",
            redis_url=None,
            host="127.0.0.1",
            port=9999,
            run_migrate=False,
            settings=settings,
        )


def test_ui_serve_succeeds_no_health_token_when_require_token_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setting TASKQ_HEALTH_REQUIRE_TOKEN=false suppresses the RuntimeError and
    allows unauthenticated health/metrics endpoints in a non-dev environment."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKQ_HEALTH_REQUIRE_TOKEN", "false")
    # Isolate the health_require_token check from admin_ui_require_auth, which
    # also fails closed in non-dev when no auth_dependency is configured.
    monkeypatch.setenv("TASKQ_ADMIN_UI_REQUIRE_AUTH", "false")
    settings = TaskQSettings.load()

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/jobs/health/live")
        assert resp.status_code == 200
        resp = client.get("/jobs/health/ready")
        assert resp.status_code == 200
        if _prometheus_available():
            resp = client.get("/jobs/health/metrics")
            assert resp.status_code == 200


def test_ui_serve_succeeds_no_health_token_in_dev_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dev environment allows an empty health_token without raising, even
    though health_require_token defaults to True."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    settings = _dev_settings(monkeypatch)

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/jobs/health/live")
        assert resp.status_code == 200


def test_ui_serve_fully_opted_out_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit, simultaneous opt-out of both fail-closed defaults
    (TASKQ_ADMIN_UI_REQUIRE_AUTH=false and TASKQ_HEALTH_REQUIRE_TOKEN=false) in a
    non-dev environment starts cleanly and leaves the whole surface open —
    admin UI, health, and metrics all reachable without any auth_dependency or
    health_token configured. This is the deliberate "fully unauthenticated,
    BYO-auth via reverse proxy" deployment shape, not an accidental one."""
    from taskq.cli import _ui_serve

    captured, _ = _capture_app(monkeypatch)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "production")
    monkeypatch.setenv("TASKQ_ADMIN_UI_REQUIRE_AUTH", "false")
    monkeypatch.setenv("TASKQ_HEALTH_REQUIRE_TOKEN", "false")
    settings = TaskQSettings.load()
    assert settings.health_token == ""
    assert settings.sso_backend == "none"

    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=None,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = captured["app"]
    assert isinstance(app, FastAPI)
    with TestClient(app) as client:
        resp = client.get("/admin/", follow_redirects=False)
        assert resp.status_code == 302
        resp = client.get("/jobs/health/live")
        assert resp.status_code == 200
        resp = client.get("/jobs/health/ready")
        assert resp.status_code == 200
        if _prometheus_available():
            resp = client.get("/jobs/health/metrics")
            assert resp.status_code == 200
