"""Tests for the SIGHUP reload coordinator loop (taskq.worker._bootstrap).

The coordinator is the only production caller of reload_credentials — it
watches deps.reload_event (set by the SIGHUP handler) and an optional
interval timer, invokes the reload, refreshes the DI LOOP-scope pool,
and handles failure/shutdown semantics. Uses fakes — no real
Postgres/Redis required.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock

import asyncpg
import pytest

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import _reload_coordinator_loop
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.shutdown import ShutdownPhase

# ── Test helpers ───────────────────────────────────────────────────────


def _make_settings(**overrides: str) -> WorkerSettings:
    """Build WorkerSettings from a dict, bypassing .env discovery."""
    base: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_HEALTH_ENABLED": "false",
        "TASKQ_NOTIFY_ENABLED": "false",
    }
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


class _FakePool:
    """Fake asyncpg.Pool that tracks close() calls."""

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> _FakePool:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class _FakeConn:
    """Fake asyncpg.Connection tracking close() and execute() calls."""

    def __init__(self) -> None:
        self.closed = False

    async def execute(self, sql: str, *_args: object) -> str:
        return "OK"

    async def close(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


def _make_pool_factory(fakes: list[_FakePool]) -> Any:
    """Build a factory that returns successive _FakePool instances."""
    idx = 0

    async def factory() -> asyncpg.Pool:
        nonlocal idx
        pool = fakes[idx]
        idx += 1
        return pool  # type: ignore[return-value]

    return factory


def _basic_conns(**kw: Any) -> WorkerConnections:
    base: dict[str, Any] = {
        "dispatcher_pool": _FakePool("dp"),
        "heartbeat_pool": _FakePool("hb"),
        "worker_pool": _FakePool("wk"),
        "notify_conn": _FakeConn(),
        "leader_conn": _FakeConn(),
    }
    base.update(kw)
    # WorkerConnections rejects concrete + factory for the same role.
    for role in ("dispatcher_pool", "heartbeat_pool", "worker_pool", "notify_conn", "leader_conn"):
        if f"{role}_factory" in kw:
            base[role] = None
    return WorkerConnections(**base)


async def _run_coordinator(
    deps: WorkerDeps,
    shutdown: asyncio.Event,
    **kw: Any,
) -> asyncio.Task[None]:
    """Start the coordinator as a task."""
    return asyncio.create_task(_reload_coordinator_loop(deps, shutdown, **kw))


async def _stop(task: asyncio.Task[None], shutdown: asyncio.Event) -> None:
    shutdown.set()
    await asyncio.wait_for(task, timeout=2.0)


# ── Event-driven reload ────────────────────────────────────────────────


async def test_coordinator_reloads_on_event_and_clears_it() -> None:
    """Setting reload_event triggers a real reload and clears the event."""
    settings = _make_settings()
    old_worker = _FakePool("old-worker")
    new_worker = _FakePool("new-worker")
    conns = _basic_conns(worker_pool_factory=_make_pool_factory([old_worker, new_worker]))
    async with open_worker_deps(settings, connections=conns) as deps:
        shutdown = asyncio.Event()
        task = await _run_coordinator(deps, shutdown)

        deps.reload_event.set()
        for _ in range(100):
            if (
                deps.worker_pool is cast(object, new_worker)
            ):  # Why: pool typed asyncpg.Pool; _FakePool has no type overlap, so plain `is` trips pyright's no-overlap check.
                break
            await asyncio.sleep(0.01)
        assert deps.worker_pool is cast(object, new_worker)
        assert not deps.reload_event.is_set()

        await _stop(task, shutdown)


async def test_coordinator_exits_on_shutdown() -> None:
    """The coordinator returns promptly when shutdown is set."""
    settings = _make_settings()
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        shutdown = asyncio.Event()
        task = await _run_coordinator(deps, shutdown)
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


async def test_coordinator_reloads_exactly_once_per_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One trigger → exactly one reload_credentials call. (Regression: a
    leftover second call reloaded twice per event, re-fetching credentials
    and double-draining pools on every SIGHUP.)"""
    settings = _make_settings()
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        shutdown = asyncio.Event()
        mock_reload = AsyncMock(return_value=([], []))
        monkeypatch.setattr("taskq.worker.deps.reload_credentials", mock_reload)

        task = await _run_coordinator(deps, shutdown)
        deps.reload_event.set()
        await asyncio.sleep(0.1)
        assert mock_reload.await_count == 1

        deps.request_reload()
        await asyncio.sleep(0.1)
        assert mock_reload.await_count == 2

        await _stop(task, shutdown)


async def test_coordinator_honors_sighup_arriving_during_failed_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SIGHUP arriving while a reload is FAILING must not be discarded —
    the operator's retry signal is the only recovery path when credentials
    are expiring."""
    settings = _make_settings()
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        shutdown = asyncio.Event()
        gate = asyncio.Event()
        calls = 0

        async def flaky_reload(_deps: WorkerDeps, **_kw: object) -> tuple[list[str], list[str]]:
            nonlocal calls
            calls += 1
            if calls == 1:
                # Simulate a slow, failing reload; SIGHUP arrives mid-flight.
                gate.set()
                await asyncio.sleep(0.05)
                raise RuntimeError("simulated reload failure")
            return ([], [])

        monkeypatch.setattr("taskq.worker.deps.reload_credentials", flaky_reload)

        task = await _run_coordinator(deps, shutdown)
        deps.reload_event.set()
        await gate.wait()  # first reload in flight
        deps.reload_event.set()  # operator retry during the failure

        for _ in range(100):
            if calls >= 2:
                break
            await asyncio.sleep(0.01)
        assert calls >= 2

        await _stop(task, shutdown)


async def test_coordinator_skips_reload_during_shutdown_orchestration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SIGHUP arriving while the worker is draining must be a no-op —
    reloading would churn pools and let the leader watchdog re-acquire
    leadership mid-shutdown."""
    settings = _make_settings()
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        shutdown = asyncio.Event()
        mock_reload = AsyncMock(return_value=([], []))
        monkeypatch.setattr("taskq.worker.deps.reload_credentials", mock_reload)

        deps.shutdown_phase = ShutdownPhase.DRAINING
        task = await _run_coordinator(deps, shutdown)
        deps.reload_event.set()
        await asyncio.sleep(0.1)

        mock_reload.assert_not_awaited()
        assert not deps.reload_event.is_set()  # consumed, not retried forever

        await _stop(task, shutdown)


# ── Interval-driven reload (signal-less platforms / scheduled rotation) ─


async def test_coordinator_interval_triggers_reload_without_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With reload_interval set, reloads fire on the timer alone — the
    rotation path for Windows and for hands-off scheduled rotation."""
    settings = _make_settings(TASKQ_RELOAD_INTERVAL="0.05")
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        shutdown = asyncio.Event()
        mock_reload = AsyncMock(return_value=([], []))
        monkeypatch.setattr("taskq.worker.deps.reload_credentials", mock_reload)

        task = await _run_coordinator(deps, shutdown)
        for _ in range(100):
            if mock_reload.await_count >= 1:
                break
            await asyncio.sleep(0.01)
        mock_reload.assert_awaited()

        await _stop(task, shutdown)


async def test_request_reload_sets_the_event() -> None:
    """deps.request_reload() is the programmatic trigger for embedders."""
    settings = _make_settings()
    async with open_worker_deps(settings, connections=_basic_conns()) as deps:
        assert not deps.reload_event.is_set()
        deps.request_reload()
        assert deps.reload_event.is_set()


# ── DI LOOP-scope refresh ──────────────────────────────────────────────


async def _bootstrap_loop_scope_with_pool(pool: object) -> LoopScope:
    """Register asyncpg.Pool at LOOP scope and bootstrap a LoopScope."""

    async def _stub_resolver(func: object, **kw: object) -> dict[str, object]:
        return {}

    registry = ProviderRegistry()
    registry.register_value(asyncpg.Pool, Scope.LOOP, pool)
    loop_scope = LoopScope(resolver=_stub_resolver)  # type: ignore[arg-type]
    await loop_scope.bootstrap(registry, ProcessScope(resolver=_stub_resolver))  # type: ignore[arg-type]
    return loop_scope


async def test_coordinator_refreshes_di_pool_after_worker_reload() -> None:
    """After a successful worker-pool reload, DI consumers must resolve
    the NEW pool — otherwise actors injected with db: asyncpg.Pool hold a
    closed pool 5s after SIGHUP."""
    settings = _make_settings()
    old_worker = _FakePool("old-worker")
    new_worker = _FakePool("new-worker")
    conns = _basic_conns(worker_pool_factory=_make_pool_factory([old_worker, new_worker]))
    async with open_worker_deps(settings, connections=conns) as deps:
        loop_scope = await _bootstrap_loop_scope_with_pool(deps.worker_pool)
        shutdown = asyncio.Event()
        task = await _run_coordinator(
            deps, shutdown, loop_scope=loop_scope, refresh_worker_pool_di=True
        )

        deps.reload_event.set()
        for _ in range(100):
            if loop_scope.get(asyncpg.Pool) is cast(object, new_worker):
                break
            await asyncio.sleep(0.01)
        assert loop_scope.get(asyncpg.Pool) is cast(object, new_worker)

        await _stop(task, shutdown)
        await loop_scope.shutdown()


async def test_coordinator_does_not_refresh_di_when_flag_off() -> None:
    """When the user registered their own asyncpg.Pool provider, the worker
    must not overwrite it after a reload."""
    settings = _make_settings()
    old_worker = _FakePool("old-worker")
    new_worker = _FakePool("new-worker")
    user_pool = _FakePool("user-pool")
    conns = _basic_conns(worker_pool_factory=_make_pool_factory([old_worker, new_worker]))
    async with open_worker_deps(settings, connections=conns) as deps:
        loop_scope = await _bootstrap_loop_scope_with_pool(user_pool)
        shutdown = asyncio.Event()
        task = await _run_coordinator(
            deps, shutdown, loop_scope=loop_scope, refresh_worker_pool_di=False
        )

        deps.reload_event.set()
        for _ in range(100):
            if (
                deps.worker_pool is cast(object, new_worker)
            ):  # Why: pool typed asyncpg.Pool; _FakePool has no type overlap, so plain `is` trips pyright's no-overlap check.
                break
            await asyncio.sleep(0.01)
        assert deps.worker_pool is cast(object, new_worker)
        assert loop_scope.get(asyncpg.Pool) is user_pool  # untouched

        await _stop(task, shutdown)
        await loop_scope.shutdown()
