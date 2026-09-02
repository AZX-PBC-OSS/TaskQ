"""Credential providers resolved from the `taskq worker` CLI.

The credential machinery (``taskq.auth`` factories, ``WorkerConnections``,
``reload_credentials``) was only reachable from a custom entrypoint: the
console script never passed ``connections=``, so ``TASKQ_RELOAD_INTERVAL``
and SIGHUP rotated nothing on a stock ``taskq worker`` (and therefore on
every workgroup-supervised child, which are ``taskq worker`` subprocesses).

These tests drive the CLI and then exercise the resource the CLI actually
handed the worker — a rotation that reaches a real provider, not a
source-text check.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import asyncpg
import pytest
from typer.testing import CliRunner

from taskq.auth import PgCredential, RedisCredential
from taskq.cli import app
from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps, reload_credentials

runner = CliRunner()

_REPO_ROOT = Path(__file__).resolve().parents[1]

_NO_ACTORS: Any = MappingProxyType({})
_NO_ACTORS_PATH = "tests.test_cli_credential_provider:_NO_ACTORS"

_MODULE = "tests.test_cli_credential_provider"


# ── Fakes ──────────────────────────────────────────────────────────────


class _CountingProvider:
    """Implements both credential Protocols; issues a fresh token per call."""

    def __init__(self) -> None:
        self.pg_calls = 0
        self.redis_calls = 0

    async def get_pg_credential(self) -> PgCredential:
        self.pg_calls += 1
        return PgCredential(password=f"pg-token-{self.pg_calls}")

    async def get_redis_credential(self) -> RedisCredential:
        self.redis_calls += 1
        return RedisCredential(username="mi-object-id", password=f"redis-token-{self.redis_calls}")


#: Module-level refs the CLI resolves by ``module:attr``.
PROVIDER = _CountingProvider()


def make_provider() -> _CountingProvider:
    """Zero-arg factory shape (``myapp.auth:make_provider``)."""
    return PROVIDER


NOT_A_PROVIDER = 5


class _FakePool:
    """Minimal asyncpg.Pool stand-in that records its connect kwargs."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed


class _FakeConn:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.closed = False

    async def execute(self, sql: str, *_args: object) -> str:
        return "OK"

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


@pytest.fixture
def fake_pg(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Replace asyncpg.create_pool / connect with recording fakes."""
    built: list[Any] = []

    async def _create_pool(**kwargs: Any) -> Any:
        pool = _FakePool(**kwargs)
        built.append(pool)
        return pool

    async def _connect(*args: Any, **kwargs: Any) -> Any:
        # Positional form: the DSN-built dedicated conns (asyncpg.connect(dsn, ...)).
        conn = _FakeConn(**kwargs)
        built.append(conn)
        return conn

    monkeypatch.setattr(asyncpg, "create_pool", _create_pool)
    monkeypatch.setattr(asyncpg, "connect", _connect)
    return built


@pytest.fixture
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Replace redis.asyncio.Redis.from_url with a recording fake."""
    import redis.asyncio as redis_async

    built: list[Any] = []

    def _from_url(url: str, **kwargs: Any) -> Any:
        client = MagicMock()
        client.url = url
        client.credential_provider = kwargs.get("credential_provider")
        client.aclose = AsyncMock()
        built.append(client)
        return client

    monkeypatch.setattr(redis_async.Redis, "from_url", _from_url)
    return built


@pytest.fixture(autouse=True)
def _worker_env(monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Deterministic settings for the CLI-invoked WorkerSettings.load()."""
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://app@db.example:5432/taskq")
    monkeypatch.setenv("TASKQ_PG_DSN_DIRECT", "postgresql://app@db.example:5432/taskq")
    monkeypatch.setenv("TASKQ_PG_DSN_POOLED", "postgresql://app@pgbouncer.example:6432/taskq")
    monkeypatch.setenv("TASKQ_REDIS_URL", "rediss://cache.example:6380/0")
    monkeypatch.setenv("TASKQ_NOTIFY_ENABLED", "false")
    monkeypatch.setenv("TASKQ_HEALTH_ENABLED", "false")
    PROVIDER.pg_calls = 0
    PROVIDER.redis_calls = 0


def _invoke_worker(monkeypatch: pytest.MonkeyPatch, *args: str) -> tuple[Any, Any, Any]:
    """Run `taskq worker` with a stubbed worker_main; return (result, settings, connections)."""
    captured: dict[str, Any] = {}

    def fake_worker_main(settings: Any, *, connections: Any = None, **_kw: Any) -> int:
        captured["settings"] = settings
        captured["connections"] = connections
        return 0

    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = runner.invoke(app, ["worker", "--actors", _NO_ACTORS_PATH, *args])
    return result, captured.get("settings"), captured.get("connections")


# ── Headline: the CLI path actually rotates a credential ───────────────


async def test_cli_pg_provider_rotates_credentials_on_reload(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """A provider named on the CLI is rotated by reload_credentials.

    Before the entrypoint hook existed, the CLI passed no ``connections=``
    and this reload rotated nothing (``resources=[]``).
    """
    result, settings, connections = _invoke_worker(
        monkeypatch, "--pg-credential-provider", f"{_MODULE}:PROVIDER"
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connections is not None, "CLI passed no connections= to worker_main"

    assert isinstance(settings, WorkerSettings)
    async with open_worker_deps(settings, connections=connections) as deps:
        calls_after_open = PROVIDER.pg_calls
        assert calls_after_open > 0, "provider was never consulted at startup"
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.1)
        await asyncio.sleep(0.05)

    assert failed == []
    assert set(reloaded) == {
        "dispatcher",
        "heartbeat",
        "worker",
        "notify_conn",
        "leader_conn",
    }
    assert PROVIDER.pg_calls > calls_after_open, "reload did not fetch a fresh credential"

    # The rebuilt pool authenticates per physical connection through the
    # provider — awaiting asyncpg's password callable yields a NEW token.
    newest_pool = next(p for p in reversed(fake_pg) if isinstance(p, _FakePool))
    password = newest_pool.kwargs["password"]
    token = await password()
    assert token == f"pg-token-{PROVIDER.pg_calls}"


async def test_cli_redis_provider_rotates_client_on_reload(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any], fake_redis: list[Any]
) -> None:
    """--redis-credential-provider yields a rotating Redis client."""
    result, settings, connections = _invoke_worker(
        monkeypatch,
        "--pg-credential-provider",
        f"{_MODULE}:PROVIDER",
        "--redis-credential-provider",
        f"{_MODULE}:PROVIDER",
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connections is not None, "CLI passed no connections= to worker_main"

    async with open_worker_deps(settings, connections=connections) as deps:
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.1)
        await asyncio.sleep(0.05)

    assert "redis_client" in reloaded
    assert failed == []
    assert len(fake_redis) >= 2, "reload did not rebuild the Redis client"

    # redis-py re-fetches through the adapter on every (re)connect.
    adapter = fake_redis[-1].credential_provider
    username, password = await adapter.get_credentials_async()
    assert username == "mi-object-id"
    assert password == f"redis-token-{PROVIDER.redis_calls}"


async def test_cli_provider_accepts_zero_arg_factory(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """``module:make_provider`` (a callable returning a provider) is accepted."""
    _result, settings, connections = _invoke_worker(
        monkeypatch, "--pg-credential-provider", f"{_MODULE}:make_provider"
    )
    assert connections is not None
    async with open_worker_deps(settings, connections=connections) as deps:
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.1)
        await asyncio.sleep(0.05)
    assert failed == []
    assert "dispatcher" in reloaded


# ── Workgroup: children are `taskq worker` subprocesses ────────────────


async def test_provider_configurable_by_env_var_alone(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """TASKQ_PG_CREDENTIAL_PROVIDER alone configures the worker.

    This is what makes the workgroup supervisor work: it spawns
    ``taskq worker`` subprocesses that inherit the environment.
    """
    monkeypatch.setenv("TASKQ_PG_CREDENTIAL_PROVIDER", f"{_MODULE}:PROVIDER")
    _result, settings, connections = _invoke_worker(monkeypatch)
    assert connections is not None, "env var did not reach the worker command"
    async with open_worker_deps(settings, connections=connections) as deps:
        reloaded, _failed = await reload_credentials(deps, drain_timeout=0.1)
        await asyncio.sleep(0.05)
    assert "dispatcher" in reloaded


def test_workgroup_children_inherit_environment() -> None:
    """_spawn_child passes no env= override, so children inherit the provider env."""
    from taskq.worker.workgroup import WorkerSpec, _ChildState, _spawn_child

    child = _ChildState(spec=WorkerSpec(name="w1", queues=["default"]))
    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MagicMock(stdout=None, stderr=None)
        asyncio.run(_spawn_child(child, "myapp.actors:registry", UUID(int=1)))
    assert mock_exec.call_args is not None
    assert "env" not in mock_exec.call_args.kwargs


def test_worker_subprocess_fails_loudly_on_bad_provider_env() -> None:
    """A `taskq worker` subprocess (as the supervisor spawns) exits 1 on a bad provider."""
    env = dict(os.environ)
    env["TASKQ_PG_CREDENTIAL_PROVIDER"] = "no.such.module:make_provider"
    env["TASKQ_PG_DSN"] = "postgresql://app@db.example:5432/taskq"
    proc = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell — this is the supervisor's own spawn shape.
        [sys.executable, "-m", "taskq", "worker", "--actors", _NO_ACTORS_PATH],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 1, f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    assert "module not found" in (proc.stdout + proc.stderr).lower()


# ── Import failures are loud and fatal (never a silent DSN fallback) ───


@pytest.mark.parametrize(
    ("ref", "needle"),
    [
        ("no.such.module:make_provider", "module not found"),
        (f"{_MODULE}:nope", "not found in module"),
        ("nocolon", "module:attr"),
        (f"{_MODULE}:NOT_A_PROVIDER", "get_pg_credential"),
    ],
)
def test_bad_pg_provider_is_fatal(monkeypatch: pytest.MonkeyPatch, ref: str, needle: str) -> None:
    result, _settings, connections = _invoke_worker(monkeypatch, "--pg-credential-provider", ref)
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert needle in result.stderr
    assert connections is None, "worker started despite an unusable credential provider"


def test_bad_redis_provider_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _settings, connections = _invoke_worker(
        monkeypatch, "--redis-credential-provider", f"{_MODULE}:NOT_A_PROVIDER"
    )
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "get_redis_credential" in result.stderr
    assert connections is None


def test_redis_provider_without_redis_url_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Redis provider with no TASKQ_REDIS_URL fails at startup, not silently."""
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)
    result, _settings, connections = _invoke_worker(
        monkeypatch, "--redis-credential-provider", f"{_MODULE}:PROVIDER"
    )
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "TASKQ_REDIS_URL" in result.stderr
    assert connections is None


def test_no_provider_leaves_connections_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a provider the worker keeps the DSN path (purely additive)."""
    result, _settings, connections = _invoke_worker(monkeypatch)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connections is None or not connections.has_any()


# ── migrate / ui ──────────────────────────────────────────────────────


def test_migrate_up_uses_credential_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """`taskq migrate up --pg-credential-provider` connects through the provider."""
    connected: list[dict[str, Any]] = []

    async def _connect(**kwargs: Any) -> Any:
        connected.append(kwargs)
        conn = MagicMock()
        conn.close = AsyncMock()
        return conn

    monkeypatch.setattr(asyncpg, "connect", _connect)
    monkeypatch.setattr("taskq.migrate.apply_pending", AsyncMock(return_value=[]))

    class _NullLock:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_: object) -> None:
            return None

    monkeypatch.setattr("taskq.migrate.migration_advisory_lock", lambda _conn: _NullLock())

    result = runner.invoke(
        app, ["migrate", "up", "--pg-credential-provider", f"{_MODULE}:PROVIDER"]
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connected, "migrate never opened a connection"
    assert PROVIDER.pg_calls > 0, "migrate did not consult the credential provider"
    token = await_sync(connected[-1]["password"])
    assert token.startswith("pg-token-")


def await_sync(coro_fn: Any) -> Any:
    """Run a zero-arg async callable to completion from sync test code."""
    return asyncio.run(coro_fn())


async def test_ui_serve_uses_credential_provider(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """`taskq ui serve --pg-credential-provider` builds its admin pool through the provider."""
    captured: dict[str, Any] = {}

    def fake_ui_serve(*args: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr("taskq.cli._ui_serve", fake_ui_serve)
    result = runner.invoke(app, ["ui", "serve", "--pg-credential-provider", f"{_MODULE}:PROVIDER"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"

    pool_factory = captured.get("pool_factory")
    assert pool_factory is not None, "ui serve ignored the credential provider"
    pool = await pool_factory()
    assert PROVIDER.pg_calls > 0
    token = await pool.kwargs["password"]()
    assert token == f"pg-token-{PROVIDER.pg_calls}"


def test_ui_serve_redis_provider_without_url_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TASKQ_REDIS_URL", raising=False)
    monkeypatch.setattr("taskq.cli._ui_serve", lambda *a, **kw: None)
    result = runner.invoke(
        app, ["ui", "serve", "--redis-credential-provider", f"{_MODULE}:PROVIDER"]
    )
    assert result.exit_code == 1, f"stderr: {result.stderr}"
    assert "TASKQ_REDIS_URL" in result.stderr


async def test_reload_interval_without_provider_warns_at_startup(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """A DSN-only worker with TASKQ_RELOAD_INTERVAL set says so at startup.

    That configuration is the silent no-op this wiring exists to remove:
    the timer fires and rebuilds connections on the same static password.
    """
    import structlog

    monkeypatch.setenv("TASKQ_RELOAD_INTERVAL", "720")
    _result, settings, connections = _invoke_worker(monkeypatch)
    assert connections is None

    with structlog.testing.capture_logs() as captured:
        async with open_worker_deps(settings, connections=None) as deps:
            reloaded, _failed = await reload_credentials(deps, drain_timeout=0.1)
            await asyncio.sleep(0.05)

    events = [entry.get("event") for entry in captured]
    assert "reload-interval-set-without-credential-provider" in events
    # …and the reload genuinely rotates no credential: only the two dedicated
    # connections reconnect, on the same static DSN.
    assert set(reloaded) == {"notify_conn", "leader_conn"}


async def test_reload_interval_with_provider_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    import structlog

    monkeypatch.setenv("TASKQ_RELOAD_INTERVAL", "720")
    _result, settings, connections = _invoke_worker(
        monkeypatch, "--pg-credential-provider", f"{_MODULE}:PROVIDER"
    )
    with structlog.testing.capture_logs() as captured:
        async with open_worker_deps(settings, connections=connections):
            pass

    events = [entry.get("event") for entry in captured]
    assert "reload-interval-set-without-credential-provider" not in events


# ── Provider refs are settings, so the .env cascade reaches them ───────


async def test_provider_configurable_by_dotenv_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_pg: list[Any]
) -> None:
    """A provider named only in `.env` configures the worker.

    The refs used to be read by Typer's ``envvar=``, which sees
    ``os.environ`` and nothing else; every other TaskQ setting honours
    dotenvmodel's ``.env`` cascade.
    """
    (tmp_path / ".env").write_text(f"TASKQ_PG_CREDENTIAL_PROVIDER={_MODULE}:PROVIDER\n")
    # DOTENV_DIR, not chdir: the session-wide _no_developer_dotfiles
    # fixture pins dotenvmodel at an empty directory.
    monkeypatch.setenv("DOTENV_DIR", str(tmp_path))

    _result, settings, connections = _invoke_worker(monkeypatch)
    assert connections is not None, ".env did not reach the worker command"
    async with open_worker_deps(settings, connections=connections) as deps:
        reloaded, _failed = await reload_credentials(deps, drain_timeout=0.1)
        await asyncio.sleep(0.05)
    assert "dispatcher" in reloaded


def test_explicit_flag_beats_dotenv_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_pg: list[Any]
) -> None:
    """An explicit --pg-credential-provider still wins over the loaded value."""
    (tmp_path / ".env").write_text("TASKQ_PG_CREDENTIAL_PROVIDER=no.such.module:make_provider\n")
    # DOTENV_DIR, not chdir: the session-wide _no_developer_dotfiles
    # fixture pins dotenvmodel at an empty directory.
    monkeypatch.setenv("DOTENV_DIR", str(tmp_path))

    result, _settings, connections = _invoke_worker(
        monkeypatch, "--pg-credential-provider", f"{_MODULE}:PROVIDER"
    )
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connections is not None


def test_empty_provider_env_is_treated_as_unset(
    monkeypatch: pytest.MonkeyPatch, fake_pg: list[Any]
) -> None:
    """``TASKQ_PG_CREDENTIAL_PROVIDER=`` means "no provider", not a bad ref."""
    monkeypatch.setenv("TASKQ_PG_CREDENTIAL_PROVIDER", "")
    result, _settings, connections = _invoke_worker(monkeypatch)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert connections is None


def test_migrate_status_uses_dotenv_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_pg: list[Any]
) -> None:
    """`taskq migrate status` opens its connection through a .env-named provider."""
    (tmp_path / ".env").write_text(f"TASKQ_PG_CREDENTIAL_PROVIDER={_MODULE}:PROVIDER\n")
    # DOTENV_DIR, not chdir: the session-wide _no_developer_dotfiles
    # fixture pins dotenvmodel at an empty directory.
    monkeypatch.setenv("DOTENV_DIR", str(tmp_path))

    captured: dict[str, Any] = {}

    async def fake_status(settings: Any, *, conn_factory: Any = None, **_kw: Any) -> None:
        captured["conn_factory"] = conn_factory

    monkeypatch.setattr("taskq.cli._status", fake_status)
    result = runner.invoke(app, ["migrate", "status"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["conn_factory"] is not None, ".env did not reach `migrate status`"
