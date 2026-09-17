"""`taskq ui serve` rotates a provider-backed admin pool.

A provider that issues a username-bearing pair (Vault dynamic credentials)
pins the admin pool to that pair for its life, so without a rebuild every
connection recycled after the lease expires fails authentication. The UI
lifespan therefore runs the same rebuild loop the worker's coordinator
does: on SIGHUP, and on the cadence derived from the granted lease (or
TASKQ_RELOAD_INTERVAL). The lifespan is driven directly (not through a
test client) so the signal handler lands on this loop and the swap is
observed on ``app.state``.
"""

from __future__ import annotations

import asyncio
import os
import signal
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import structlog.testing

pytest.importorskip("fastapi")

from taskq.auth import PgCredential, ReloadSchedule, make_pg_pool_factory
from taskq.settings import TaskQSettings
from taskq.testing.assertions import wait_for


class _FakePool:
    def __init__(self, name: str) -> None:
        self.name = name
        self.closed = asyncio.Event()

    async def close(self) -> None:
        self.closed.set()

    def terminate(self) -> None:
        self.closed.set()


class _LeaseProvider:
    def __init__(self, lease_duration: float | None) -> None:
        self.calls = 0
        self.lease_duration = lease_duration

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(
            password=f"pw-{self.calls}",
            username=f"v-{self.calls}",
            lease_duration=self.lease_duration,
        )


def _capture_ui_app(monkeypatch: pytest.MonkeyPatch, pool_factory: Any) -> Any:
    import uvicorn

    captured: dict[str, Any] = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: captured.setdefault("app", app))
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    from taskq.cli import _ui_serve

    _ui_serve(
        "postgresql://u:p@h:5432/db",
        "taskq",
        None,
        "127.0.0.1",
        9999,
        False,
        TaskQSettings.load(),
        pool_factory=pool_factory,
    )
    return captured["app"]


def _lease_factory(provider: _LeaseProvider, *, configured: float | None = None) -> Any:
    return make_pg_pool_factory(
        "postgresql://u:p@h:5432/db",
        provider,
        reload_schedule=ReloadSchedule(configured=configured),
    )


def _pools() -> tuple[list[_FakePool], AsyncMock]:
    built: list[_FakePool] = []

    async def _create_pool(**_: Any) -> _FakePool:
        pool = _FakePool(f"pool-{len(built) + 1}")
        built.append(pool)
        return pool

    return built, AsyncMock(side_effect=_create_pool)


async def test_sighup_rebuilds_the_admin_pool_on_a_fresh_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SIGHUP takes a fresh credential, swaps the new pool into app.state
    (where every admin route resolves it) and closes the old one; the
    provider is asked once at startup and once per rotation."""
    provider = _LeaseProvider(lease_duration=None)
    factory = _lease_factory(provider, configured=3600.0)
    built, create_pool = _pools()
    app = _capture_ui_app(monkeypatch, factory)

    with patch("asyncpg.create_pool", new=create_pool):
        async with asyncio.timeout(10):
            async with app.router.lifespan_context(app):
                assert provider.calls == 1
                assert app.state.pg_pool is built[0]

                os.kill(os.getpid(), signal.SIGHUP)
                await wait_for(built[0].closed, timeout=5.0)
                assert provider.calls == 2
                assert app.state.pg_pool is built[1]
                assert built[1].closed.is_set() is False
            # Shutdown closes the pool that was live at exit, not the one
            # the lifespan started with.
            assert built[1].closed.is_set()


async def test_rotation_cadence_is_derived_from_the_granted_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no TASKQ_RELOAD_INTERVAL the admin pool is rebuilt at half the
    lease TTL the provider granted, with no signal at all."""
    provider = _LeaseProvider(lease_duration=0.2)
    factory = _lease_factory(provider)
    built, create_pool = _pools()
    app = _capture_ui_app(monkeypatch, factory)

    with patch("asyncpg.create_pool", new=create_pool), structlog.testing.capture_logs() as logs:
        async with asyncio.timeout(10):
            async with app.router.lifespan_context(app):
                await wait_for(built[0].closed, timeout=5.0)
                assert provider.calls >= 2
                assert app.state.pg_pool is not built[0]

    armed = [e for e in logs if e["event"] == "ui-credential-rotation-armed"]
    assert len(armed) == 1
    assert armed[0]["reload_interval"] == pytest.approx(0.1)
    assert armed[0]["derived_from_lease"] is True
    assert armed[0]["lease_duration"] == pytest.approx(0.2)


async def test_an_explicit_interval_overrides_the_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TASKQ_RELOAD_INTERVAL is the operator's cadence and wins over the
    lease-derived one."""
    monkeypatch.setenv("TASKQ_RELOAD_INTERVAL", "0.05")
    provider = _LeaseProvider(lease_duration=3600)
    factory = _lease_factory(provider)
    built, create_pool = _pools()
    app = _capture_ui_app(monkeypatch, factory)

    with patch("asyncpg.create_pool", new=create_pool), structlog.testing.capture_logs() as logs:
        async with asyncio.timeout(10):
            async with app.router.lifespan_context(app):
                await wait_for(built[0].closed, timeout=5.0)
    armed = [e for e in logs if e["event"] == "ui-credential-rotation-armed"]
    assert armed[0]["reload_interval"] == 0.05
    assert armed[0]["derived_from_lease"] is False
    assert armed[0]["sighup"] is True


async def test_a_failed_rebuild_leaves_the_live_pool_serving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A factory that fails on rotation is reported and the pool that was
    serving keeps serving: the lifespan never swaps in a pool it did not
    get."""
    provider = _LeaseProvider(lease_duration=None)
    factory = _lease_factory(provider, configured=3600.0)
    built, create_pool = _pools()
    app = _capture_ui_app(monkeypatch, factory)

    with patch("asyncpg.create_pool", new=create_pool), structlog.testing.capture_logs() as logs:
        async with asyncio.timeout(10):
            async with app.router.lifespan_context(app):
                attempted = asyncio.Event()

                async def _fail(**_: Any) -> _FakePool:
                    attempted.set()
                    raise RuntimeError("vault sealed")

                create_pool.side_effect = _fail
                os.kill(os.getpid(), signal.SIGHUP)
                await wait_for(attempted, timeout=5.0)
                # The failure is handled on the rotation task; yield until
                # it has logged and gone back to waiting.
                for _ in range(20):
                    await asyncio.sleep(0)
                assert app.state.pg_pool is built[0]
                assert built[0].closed.is_set() is False
    entry = next(e for e in logs if e["event"] == "credentials-reload-failed")
    assert entry["error_type"] == "RuntimeError"
    assert entry["cause"] == "trigger"
