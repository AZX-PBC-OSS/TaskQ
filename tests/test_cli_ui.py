"""Tests for taskq.cli ui sub-app: serve command and settings resolution."""

import asyncio
from collections.abc import Generator

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
        **_kwargs: object,
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
    """Fake asyncpg.Pool stand-in accepted by create_router.

    Awaitable and an async context manager (mirroring the real Pool, whose
    ``__aexit__`` calls ``close()``), with close()/terminate() tracking and
    a hang gate for bounded-close tests: clear close_wait to make close()
    block forever (dead PG). Mirrors the _FakePool conventions in
    tests/test_worker_deps_teardown.py.
    """

    def __init__(self) -> None:
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.closed = False
        self.terminated = False

    def __await__(self) -> Generator[object, None, "_FakePool"]:
        async def _self() -> "_FakePool":
            return self

        return _self().__await__()

    async def __aenter__(self) -> "_FakePool":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()  # mirrors real Pool.__aexit__

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()


class _FakeAsyncCM:
    """Generic async context manager yielding a fixed value.

    Also awaitable (returns the value), mirroring asyncpg.create_pool's
    return — the lifespan code can either enter it as a CM or await it.
    """

    def __init__(self, value: object) -> None:
        self._value = value
        self.entered = False
        self.exited = False

    def __await__(self) -> Generator[object, None, object]:
        async def _value() -> object:
            return self._value

        return _value().__await__()

    async def __aenter__(self) -> object:
        self.entered = True
        return self._value

    async def __aexit__(self, *exc_info: object) -> None:
        self.exited = True


class _FakeRedis:
    """Fake redis.asyncio.Redis for UI-lifespan bounded-close tests.

    Supports both lifecycle styles: async-CM (pre-fix enter_async_context
    path) and explicit initialize() + pushed bounded-aclose callback
    (post-fix path). aclose() blocks while aclose_wait is cleared (hung
    broker). Mirrors the _FakeRedisClient conventions in
    tests/test_jobs_client.py.
    """

    def __init__(self) -> None:
        self.initialize_calls = 0
        self.aclose_calls = 0
        self.aclose_wait = asyncio.Event()
        self.aclose_wait.set()  # aclose() completes instantly by default

    async def initialize(self) -> "_FakeRedis":
        self.initialize_calls += 1
        return self

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await self.aclose_wait.wait()

    async def __aenter__(self) -> "_FakeRedis":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class _FakeRedisInitializeRaises(_FakeRedis):
    """_FakeRedis whose initialize() fails (broker down at startup).

    from_url() has already allocated the connection pool by the time
    initialize() runs, so a raising eager setup must still be followed by
    aclose() during unwind.
    """

    async def initialize(self) -> "_FakeRedis":
        self.initialize_calls += 1
        raise ConnectionError("broker down")


class _FakeRedisAcloseRaises(_FakeRedis):
    """_FakeRedis whose aclose() fails (broker error at teardown)."""

    async def aclose(self) -> None:
        self.aclose_calls += 1
        raise ConnectionError("broker gone")


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

    pool = _FakePool()
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool)

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

    assert pool.closed is True
    assert pool.terminated is False


# ── Bounded pool close at lifespan exit (#38) ───────────────────────────
#
# The lifespan entered the pool on the AsyncExitStack (``Pool.__aexit__``
# → unbounded ``close()``) — a dead PG could wedge UI shutdown. These
# tests pin the bounded-close discipline (asyncio.wait_for + terminate on
# timeout) applied via ``close_pool_bounded``; the shrink seam is the
# same module-global monkeypatch convention as
# tests/test_worker_deps_teardown.py. The lifespan is driven directly
# (not via TestClient) so ``asyncio.timeout`` can bound the RED state.


def _capture_app_for_lifespan(
    monkeypatch: pytest.MonkeyPatch,
    pool: _FakePool,
    redis_client: _FakeRedis | None = None,
) -> object:
    """Run _ui_serve with a stubbed uvicorn.run and return the FastAPI app.

    When redis_client is given, redis.asyncio.from_url is patched to return
    it and _ui_serve is invoked with a redis_url set.
    """
    import uvicorn

    import taskq.cli as cli_mod

    captured: dict[str, object] = {}

    def _fake_uvicorn_run(app: object, **kwargs: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(uvicorn, "run", _fake_uvicorn_run)
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool)

    redis_url: str | None = None
    if redis_client is not None:
        import redis.asyncio as aioredis

        monkeypatch.setattr(aioredis, "from_url", lambda *a, **kw: redis_client)
        redis_url = "redis://localhost:6379/0"

    from taskq.cli import _ui_serve

    settings = _dev_settings(monkeypatch)
    _ui_serve(
        pg_dsn="postgresql://u:p@h:5432/db",
        schema="taskq",
        redis_url=redis_url,
        host="127.0.0.1",
        port=9999,
        run_migrate=False,
        settings=settings,
    )
    return captured["app"]


async def test_ui_serve_lifespan_terminates_hung_pool_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung pool close at lifespan exit (dead PG) is terminated after the
    bounded timeout and lifespan shutdown completes."""
    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    pool = _FakePool()
    pool.close_wait.clear()  # close() blocks forever from now on
    app = _capture_app_for_lifespan(monkeypatch, pool)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    # Why the outer timeout: pre-fix lifespan exit awaited Pool.__aexit__
    # (unbounded close), so the RED state would hang forever instead of
    # failing fast.
    async with asyncio.timeout(5):
        async with app.router.lifespan_context(app):
            pass

    assert pool.terminated is True
    assert pool.close_calls == 1


async def test_ui_serve_lifespan_fast_pool_close_not_terminated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Healthy pool close at lifespan exit: closed once, never terminated.
    Pins the no-regression behaviour (passes pre- and post-fix)."""
    pool = _FakePool()
    app = _capture_app_for_lifespan(monkeypatch, pool)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    async with asyncio.timeout(5):
        async with app.router.lifespan_context(app):
            pass

    assert pool.closed is True
    assert pool.close_calls == 1
    assert pool.terminated is False


# ── Bounded redis close at lifespan exit (#38 follow-up) ────────────────
#
# The lifespan entered the redis client on the AsyncExitStack
# (``Redis.__aexit__`` → shielded, unbounded ``aclose()``) — a hung broker
# could wedge UI shutdown. These tests pin the bounded-close discipline
# (asyncio.wait_for, log-and-continue — Redis has no terminate()) applied
# via ``close_redis_bounded``, and the preserved eager-initialize
# semantics of ``Redis.__aenter__``. The shrink seam is the same
# module-global monkeypatch convention as the pool tests above; the
# lifespan is driven directly so ``asyncio.timeout`` can bound the RED
# state.


async def test_ui_serve_lifespan_bounds_hung_redis_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung redis aclose at lifespan exit (hung broker) is bounded: the
    lifespan shutdown logs and continues instead of hanging (no terminate
    on redis — it has none)."""
    import structlog

    import taskq.cli as cli_mod

    monkeypatch.setattr(cli_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    redis_client = _FakeRedis()
    redis_client.aclose_wait.clear()  # aclose() blocks forever from now on
    app = _capture_app_for_lifespan(monkeypatch, _FakePool(), redis_client)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    # Why the outer timeout: pre-fix lifespan exit awaited Redis.__aexit__
    # (shielded, unbounded aclose), so the RED state would hang forever
    # instead of failing fast.
    with structlog.testing.capture_logs() as captured:
        async with asyncio.timeout(5):
            async with app.router.lifespan_context(app):
                pass

    assert redis_client.initialize_calls == 1
    assert redis_client.aclose_calls == 1
    # Why the log assertion: redis has no terminate(), so the
    # redis-teardown-close-timeout event is the only positive signal that
    # the TimeoutError branch fired (as opposed to the generic
    # except-Exception branch, which logs redis-teardown-close-error).
    timeout_events = [e for e in captured if e.get("event") == "redis-teardown-close-timeout"]
    assert len(timeout_events) == 1, (
        f"expected 1 redis-teardown-close-timeout log, got {captured!r}"
    )
    # label= identifies WHICH client hung (review N7) — the UI admin client,
    # matching the ui-admin pool label.
    assert timeout_events[0].get("label") == "ui-admin", (
        f"expected label=ui-admin on the timeout event, got {timeout_events[0]!r}"
    )


async def test_ui_serve_lifespan_fast_redis_close_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Healthy redis close at lifespan exit: aclose happens exactly once.
    Pins the no-regression behaviour (passes pre- and post-fix)."""
    redis_client = _FakeRedis()
    app = _capture_app_for_lifespan(monkeypatch, _FakePool(), redis_client)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    async with asyncio.timeout(5):
        async with app.router.lifespan_context(app):
            pass

    assert redis_client.aclose_calls == 1


async def test_ui_serve_lifespan_redis_initialize_failure_still_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redis client whose initialize() fails (broker down at startup) is
    still closed during lifespan unwind: from_url() has already allocated
    the connection pool, so failed eager setup must not leak it. Pins that
    the bounded-close callback is pushed BEFORE initialize() is awaited."""
    redis_client = _FakeRedisInitializeRaises()
    app = _capture_app_for_lifespan(monkeypatch, _FakePool(), redis_client)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    with pytest.raises(ConnectionError, match="broker down"):
        async with asyncio.timeout(5):
            async with app.router.lifespan_context(app):
                pass

    assert redis_client.initialize_calls == 1
    assert redis_client.aclose_calls == 1


async def test_ui_serve_lifespan_redis_aclose_error_does_not_abort_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A redis aclose() that raises at lifespan exit does not abort the
    remaining teardown: the bounded close swallows the error and the pool
    close callback (pushed earlier, so unwound after redis — LIFO) still
    executes."""
    pool = _FakePool()
    redis_client = _FakeRedisAcloseRaises()
    app = _capture_app_for_lifespan(monkeypatch, pool, redis_client)

    from fastapi import FastAPI

    assert isinstance(app, FastAPI)
    async with asyncio.timeout(5):
        async with app.router.lifespan_context(app):
            pass

    assert redis_client.aclose_calls == 1
    assert pool.close_calls == 1


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

    pool = _FakePool()
    monkeypatch.setattr(cli_mod.asyncpg, "create_pool", lambda *a, **kw: pool)
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
    """_ui_serve's lifespan initializes the redis client eagerly when redis_url is set
    and closes it on shutdown.

    Pins the post-#38 wiring: explicit ``initialize()`` (preserving
    ``Redis.__aenter__``'s eager-setup semantics) plus a pushed
    bounded-aclose callback, instead of entering the client as an async
    context manager (whose ``__aexit__`` closes unbounded).
    """
    redis_client = _FakeRedis()
    app = _capture_app_for_lifespan(monkeypatch, _FakePool(), redis_client)

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    assert isinstance(app, FastAPI)

    with TestClient(app):
        pass

    assert redis_client.initialize_calls == 1
    assert redis_client.aclose_calls == 1


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

    async def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass


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
