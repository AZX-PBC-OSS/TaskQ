"""The heartbeat pool's per-query timeout is a setting, not a literal.

Both construction paths — the DSN fallback in ``taskq.worker.deps`` and the
credential-provider builder in ``taskq.auth`` — hardcoded ``command_timeout=2``.
An operator on a loaded or cross-region Postgres who raised
``dispatcher_command_timeout`` got no relief on the heartbeat path: beats kept
timing out at 2 s, failures accumulated to ``max_heartbeat_failures`` and the
worker self-terminated for a reason not visible in any setting.
"""

from typing import Any, Self

import pytest

from taskq.testing.settings import make_integration_settings
from taskq.worker import deps as deps_mod
from taskq.worker.deps import open_worker_deps

_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"
_RAISED = 9.0


class _FakeConn:
    async def execute(self, *args: object, **kwargs: object) -> str:
        return "LISTEN"

    async def close(self) -> None:
        return None

    def is_closed(self) -> bool:
        return False


class _FakeAcquireCtx:
    async def __aenter__(self) -> _FakeConn:
        return _FakeConn()

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePool:
    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False

    def acquire(self, timeout: float | None = None) -> _FakeAcquireCtx:
        return _FakeAcquireCtx()


async def test_deps_heartbeat_pool_uses_the_configured_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def _fake_create_pool(*args: object, **kwargs: Any) -> _FakePool:
        calls.append(kwargs)
        return _FakePool()

    async def _fake_open_dedicated_conn(dsn: str, **kwargs: object) -> _FakeConn:
        return _FakeConn()

    monkeypatch.setattr("asyncpg.create_pool", _fake_create_pool)
    monkeypatch.setattr(deps_mod, "open_dedicated_conn", _fake_open_dedicated_conn)

    settings = make_integration_settings(
        _DSN,
        HEARTBEAT_COMMAND_TIMEOUT=str(_RAISED),
        HEARTBEAT_POOL_SIZE="3",
    )
    async with open_worker_deps(settings):
        pass

    heartbeat_calls = [c for c in calls if c.get("max_size") == 3]
    assert heartbeat_calls, f"heartbeat pool not built: {calls!r}"
    assert heartbeat_calls[0]["command_timeout"] == _RAISED


def test_provider_heartbeat_factory_uses_the_configured_command_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``build_worker_connections`` promises the provider path is timed out
    exactly as the DSN path — so it must read the same setting."""
    from taskq import auth as auth_mod

    calls: list[dict[str, Any]] = []

    def _fake_factory(*args: object, **kwargs: Any) -> object:
        calls.append(kwargs)
        return lambda: None

    monkeypatch.setattr(auth_mod, "make_pg_pool_factory", _fake_factory)

    settings = make_integration_settings(
        _DSN,
        HEARTBEAT_COMMAND_TIMEOUT=str(_RAISED),
        HEARTBEAT_POOL_SIZE="3",
    )

    class _Provider:
        async def get_password(self) -> str:  # pragma: no cover - never called
            return "pw"

    auth_mod.build_worker_connections(settings, pg_provider=_Provider())  # type: ignore[arg-type]

    heartbeat_calls = [c for c in calls if c.get("max_size") == 3]
    assert heartbeat_calls, f"heartbeat factory not built: {calls!r}"
    assert heartbeat_calls[0]["command_timeout"] == _RAISED
