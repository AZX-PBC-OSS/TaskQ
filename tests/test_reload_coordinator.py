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
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import structlog.testing

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope
from taskq.auth import PgCredential
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for
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
        # Why an event alongside the flag: the flag is the assertion
        # surface; the event is the WAIT surface — the coordinator's
        # reload closes the OLD pool on a background drain task spawned
        # strictly AFTER the swap, so awaiting this event is a bounded
        # wait that cannot observe a half-applied reload (and never a
        # fixed sleep racing the drain under load).
        self.closed_event = asyncio.Event()

    async def close(self) -> None:
        self.closed = True
        self.closed_event.set()

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
    """Build a factory that returns successive _FakePool instances.

    Accepts and ignores keyword arguments so it also stands in for
    ``asyncpg.create_pool`` under a provider-backed factory.
    """
    idx = 0

    async def factory(**_kwargs: Any) -> asyncpg.Pool:
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
        # The reload swaps deps.worker_pool and only THEN spawns the old
        # pool's background drain, so the old pool's closed event is a
        # bounded wait that cannot resume before the swap has landed.
        await wait_for(old_worker.closed_event, timeout=5.0)
        assert (
            deps.worker_pool is cast(object, new_worker)
        )  # Why: pool typed asyncpg.Pool; _FakePool has no type overlap, so plain `is` trips pyright's no-overlap check.
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
        first_reload_started = asyncio.Event()
        second_reload_started = asyncio.Event()
        reload_calls = 0

        def _on_reload_call(*_args: object, **_kwargs: object) -> tuple[list[str], list[str]]:
            # Why a side_effect: the observable is "the coordinator made
            # its Nth reload_credentials call" — the event fires at the
            # exact point that flips, so the waits below never race the
            # coordinator's loop under load. Accepts (and ignores) the
            # call args mock passes through, and returns the mock's
            # contract value (reloaded, failed) for the coordinator.
            nonlocal reload_calls
            reload_calls += 1
            if reload_calls == 1:
                first_reload_started.set()
            else:
                second_reload_started.set()
            return ([], [])

        mock_reload = AsyncMock(return_value=([], []), side_effect=_on_reload_call)
        monkeypatch.setattr("taskq.worker.deps.reload_credentials", mock_reload)

        task = await _run_coordinator(deps, shutdown)
        deps.reload_event.set()
        await wait_for(first_reload_started, timeout=5.0)
        # Exact-once is safe to assert here without further settling: the
        # event fires inside the coordinator's own step, and a buggy
        # back-to-back second call would run in that same continuation
        # (before the coordinator suspends) — i.e. before this test task
        # can resume.
        assert mock_reload.await_count == 1

        deps.request_reload()
        await wait_for(second_reload_started, timeout=5.0)
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
        second_call_seen = asyncio.Event()
        calls = 0

        async def flaky_reload(_deps: WorkerDeps, **_kw: object) -> tuple[list[str], list[str]]:
            nonlocal calls
            calls += 1
            if calls == 1:
                # Simulate a slow, failing reload; SIGHUP arrives mid-flight.
                gate.set()
                await asyncio.sleep(0.05)
                raise RuntimeError("simulated reload failure")
            # Why an event at the point reached: "the coordinator honored
            # the mid-failure SIGHUP with a follow-up reload" is exactly
            # "the second call started" — awaiting it is bounded and never
            # a fixed sleep racing the coordinator under load.
            second_call_seen.set()
            return ([], [])

        monkeypatch.setattr("taskq.worker.deps.reload_credentials", flaky_reload)

        task = await _run_coordinator(deps, shutdown)
        deps.reload_event.set()
        await gate.wait()  # first reload in flight
        deps.reload_event.set()  # operator retry during the failure

        await wait_for(second_call_seen, timeout=5.0)
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
        interval_reload_fired = asyncio.Event()

        def _on_reload_call(*_args: object, **_kwargs: object) -> tuple[list[str], list[str]]:
            # Why a side_effect: "the interval timer fired a reload" is
            # exactly "reload_credentials was called" — the event fires at
            # that point, so the wait below never races the timer or the
            # coordinator's loop under load. Accepts (and ignores) the
            # call args mock passes through, and returns the mock's
            # contract value (reloaded, failed) for the coordinator.
            interval_reload_fired.set()
            return ([], [])

        mock_reload = AsyncMock(return_value=([], []), side_effect=_on_reload_call)
        monkeypatch.setattr("taskq.worker.deps.reload_credentials", mock_reload)

        task = await _run_coordinator(deps, shutdown)
        await wait_for(interval_reload_fired, timeout=5.0)
        mock_reload.assert_awaited()

        await _stop(task, shutdown)


class _LeaseProvider:
    """A provider issuing a fresh username-bearing pair per call, with the
    lease TTL a Vault dynamic role would report."""

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


async def test_coordinator_derives_its_timer_from_the_granted_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no TASKQ_RELOAD_INTERVAL, a worker pool built through a
    provider that reports a lease is rebuilt at half that lease's TTL: the
    provider is asked once at bootstrap and again on the derived tick, with
    no signal sent."""
    from taskq.auth import ReloadSchedule, make_pg_pool_factory

    settings = _make_settings()
    provider = _LeaseProvider(lease_duration=0.2)
    fakes = [_FakePool("wk-1"), _FakePool("wk-2"), _FakePool("wk-3")]
    monkeypatch.setattr(asyncpg, "create_pool", _make_pool_factory(fakes))
    factory = make_pg_pool_factory(
        "postgresql://fake:fake@fake:5432/fake",
        provider,
        reload_schedule=ReloadSchedule(configured=settings.reload_interval),
    )
    conns = _basic_conns(worker_pool_factory=factory)
    with structlog.testing.capture_logs() as logs:
        async with open_worker_deps(settings, connections=conns) as deps:
            assert provider.calls == 1
            shutdown = asyncio.Event()
            task = await _run_coordinator(deps, shutdown)
            await wait_for(fakes[0].closed_event, timeout=5.0)
            assert provider.calls >= 2
            assert deps.worker_pool is not cast(object, fakes[0])
            await _stop(task, shutdown)
    derived = [e for e in logs if e["event"] == "reload-interval-derived-from-lease"]
    assert len(derived) == 1
    assert derived[0]["reload_interval"] == pytest.approx(0.1)
    assert derived[0]["lease_duration"] == pytest.approx(0.2)


async def test_coordinator_has_no_timer_when_no_lease_and_no_interval() -> None:
    """A username-bearing provider that reports no lease, with no
    TASKQ_RELOAD_INTERVAL, leaves the coordinator waiting on SIGHUP alone:
    the pool build warned, and nothing rebuilds by itself."""
    from taskq.auth import ReloadSchedule, make_pg_pool_factory

    settings = _make_settings()
    provider = _LeaseProvider(lease_duration=None)
    fakes = [_FakePool("wk-1"), _FakePool("wk-2")]
    with patch.object(asyncpg, "create_pool", _make_pool_factory(fakes)):
        factory = make_pg_pool_factory(
            "postgresql://fake:fake@fake:5432/fake",
            provider,
            reload_schedule=ReloadSchedule(configured=settings.reload_interval),
        )
        with structlog.testing.capture_logs() as logs:
            async with open_worker_deps(
                settings, connections=_basic_conns(worker_pool_factory=factory)
            ) as deps:
                shutdown = asyncio.Event()
                task = await _run_coordinator(deps, shutdown)
                await asyncio.sleep(0.1)
                assert provider.calls == 1
                assert not fakes[0].closed
                await _stop(task, shutdown)
    assert any(e["event"] == "pg-lease-pair-pinned-without-reload" for e in logs)
    assert not any(e["event"] == "reload-interval-derived-from-lease" for e in logs)


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
        # The old pool's background drain is spawned strictly AFTER the
        # worker-pool swap, and the DI cache refresh happens before the
        # coordinator suspends again — so the old pool's closed event is a
        # bounded wait that cannot resume before both have landed.
        await wait_for(old_worker.closed_event, timeout=5.0)
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
        # Same witness as the DI-refresh test: the old pool's drain runs
        # only after the swap, so its closed event is a bounded wait that
        # cannot resume before the swap has landed.
        await wait_for(old_worker.closed_event, timeout=5.0)
        assert (
            deps.worker_pool is cast(object, new_worker)
        )  # Why: pool typed asyncpg.Pool; _FakePool has no type overlap, so plain `is` trips pyright's no-overlap check.
        assert loop_scope.get(asyncpg.Pool) is user_pool  # untouched

        await _stop(task, shutdown)
        await loop_scope.shutdown()
