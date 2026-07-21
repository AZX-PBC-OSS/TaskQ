"""Tests for SIGHUP credential hot-reload (taskq.worker.deps.reload_credentials).

Verifies that reload_credentials hot-swaps every factory-backed resource
on WorkerDeps with freshly-built replacements, that old resources are
drained in the background, and that caller-owned resources are skipped.
Uses fakes — no real Postgres/Redis required.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps, open_worker_deps, reload_credentials

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
    """Fake asyncpg.Pool that tracks close()/terminate() calls."""

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default

    async def acquire(self, **_kw: object) -> object:
        return MagicMock()

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return  # close-after-terminate is a no-op on a real pool
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()  # aborts any in-flight close() wait

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> _FakePool:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class _FakeConn:
    """Fake asyncpg.Connection tracking close() and execute() calls."""

    def __init__(self) -> None:
        self.closed = False
        self.terminated = False
        self.executed: list[str] = []

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def add_listener(self, channel: str, callback: object) -> None:
        pass

    async def remove_listener(self, channel: str, callback: object) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
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


def _make_conn_factory(fakes: list[_FakeConn]) -> Any:
    """Build a factory that returns successive _FakeConn instances."""
    idx = 0

    async def factory() -> asyncpg.Connection:
        nonlocal idx
        conn = fakes[idx]
        idx += 1
        return conn  # type: ignore[return-value]

    return factory


# ── reload_credentials: pools ──────────────────────────────────────────


async def test_reload_swaps_factory_backed_pools() -> None:
    """reload_credentials replaces factory-backed pools with fresh ones."""
    settings = _make_settings()
    old_dispatcher = _FakePool("old-dispatcher")
    old_heartbeat = _FakePool("old-heartbeat")
    old_worker = _FakePool("old-worker")
    new_dispatcher = _FakePool("new-dispatcher")
    new_heartbeat = _FakePool("new-heartbeat")
    new_worker = _FakePool("new-worker")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([old_dispatcher, new_dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([old_heartbeat, new_heartbeat]),
        worker_pool_factory=_make_pool_factory([old_worker, new_worker]),
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.dispatcher_pool is old_dispatcher
        await reload_credentials(deps, drain_timeout=0.5)
        # New pools are in place
        assert deps.dispatcher_pool is new_dispatcher
        assert deps.heartbeat_pool is new_heartbeat
        assert deps.worker_pool is new_worker
        # Wait for the background drain tasks to close old pools
        await asyncio.sleep(0.2)

    # Old pools were drained (closed in background)
    assert old_dispatcher.closed
    assert old_heartbeat.closed
    assert old_worker.closed
    # New pools closed at teardown
    assert new_dispatcher.closed
    assert new_heartbeat.closed
    assert new_worker.closed


async def test_reload_skips_caller_owned_pools() -> None:
    """reload_credentials does not touch caller-owned (concrete) pools."""
    settings = _make_settings()
    caller_pool = _FakePool("caller-owned")

    conns = WorkerConnections(
        dispatcher_pool=caller_pool,  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        await reload_credentials(deps, drain_timeout=0.5)
        # Caller-owned pool unchanged
        assert deps.dispatcher_pool is caller_pool

    # Caller-owned pool NOT closed by TaskQ
    assert not caller_pool.closed


async def test_reload_continues_past_one_failed_pool_factory() -> None:
    """A factory failure for one pool (e.g. transient credential-fetch
    error) is caught, logged, and does NOT abort the remaining resources —
    reload_credentials keeps going and reloads dispatcher/worker even
    though heartbeat's factory raised."""
    settings = _make_settings()
    old_dispatcher = _FakePool("old-dispatcher")
    old_heartbeat = _FakePool("old-heartbeat")
    old_worker = _FakePool("old-worker")
    new_dispatcher = _FakePool("new-dispatcher")
    new_worker = _FakePool("new-worker")

    async def failing_heartbeat_factory() -> asyncpg.Pool:
        raise RuntimeError("simulated transient credential-fetch failure")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([old_dispatcher, new_dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([old_heartbeat]),  # only the initial open
        worker_pool_factory=_make_pool_factory([old_worker, new_worker]),
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        # Swap in a failing factory only for the reload call, after startup
        # succeeded with the real one — isolates the failure to the reload.
        deps.heartbeat_pool_factory = failing_heartbeat_factory  # type: ignore[assignment]

        await reload_credentials(deps, drain_timeout=0.5)

        # dispatcher and worker reloaded fine despite heartbeat's failure
        assert deps.dispatcher_pool is new_dispatcher
        assert deps.worker_pool is new_worker
        # heartbeat kept its old (not-yet-expired) pool — no partial/corrupt state
        assert deps.heartbeat_pool is old_heartbeat
        # Not closed by the reload's background drain — only normal
        # open_worker_deps teardown (below) will close it, since it's
        # still the live pool.
        assert not old_heartbeat.closed
        await asyncio.sleep(0.2)

    # Reloaded pools' old copies were drained by the reload itself.
    assert old_dispatcher.closed
    assert old_worker.closed
    # heartbeat never reloaded, so its pool is closed by ordinary
    # open_worker_deps teardown, not the reload's drain path.
    assert old_heartbeat.closed


# ── reload_credentials: notify_conn ────────────────────────────────────


async def test_reload_swaps_notify_conn_via_factory() -> None:
    """reload_credentials replaces notify_conn via its factory."""
    settings = _make_settings(TASKQ_NOTIFY_ENABLED="false")
    old_notify = _FakeConn()
    new_notify = _FakeConn()
    notify_fakes = [old_notify, new_notify]

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn_factory=_make_conn_factory(notify_fakes),
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.notify_conn is old_notify
        await reload_credentials(deps, drain_timeout=0.5)
        # notify_conn was swapped (via the factory — no listener running,
        # so the direct-swap fallback path is used)
        assert deps.notify_conn is new_notify
        # LISTEN was issued on the new connection
        assert any(sql.startswith("LISTEN") for sql in new_notify.executed)
        # Wait for the background drain task to close the old conn
        await asyncio.sleep(0.2)

    # Old notify conn was drained
    assert old_notify.closed


# ── reload_credentials: leader_conn ────────────────────────────────────


async def test_reload_closes_leader_conn_for_watchdog_reopen() -> None:
    """reload_credentials closes leader_conn so the watchdog reopens it."""
    settings = _make_settings()
    old_leader = _FakeConn()
    new_leader = _FakeConn()
    leader_fakes = [old_leader, new_leader]

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn_factory=_make_conn_factory(leader_fakes),
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.leader_conn is old_leader
        await reload_credentials(deps, drain_timeout=0.5)
        # leader_conn is set to None — the watchdog will reopen it
        assert deps.leader_conn is None
        # Wait for the background drain task to close the old conn
        await asyncio.sleep(0.2)

    # Old leader conn was drained
    assert old_leader.closed


# ── reload_credentials: Redis ──────────────────────────────────────────


async def test_reload_swaps_redis_client() -> None:
    """reload_credentials replaces the Redis client via its factory."""
    settings = _make_settings()

    old_redis = MagicMock()
    old_redis.aclose = AsyncMock()
    new_redis = MagicMock()
    new_redis.aclose = AsyncMock()
    redis_fakes = [old_redis, new_redis]
    idx = 0

    async def redis_factory() -> Any:
        nonlocal idx
        r = redis_fakes[idx]
        idx += 1
        return r

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
        redis_client_factory=redis_factory,
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.redis_client is old_redis
        await reload_credentials(deps, drain_timeout=0.5)
        assert deps.redis_client is new_redis
        # Wait for the background drain task to close old Redis
        await asyncio.sleep(0.2)

    # Old Redis was drained exactly once (by the reload's background drain)…
    old_redis.aclose.assert_awaited_once()
    # …and the LIVE client — the one in use at shutdown — was closed by
    # teardown. Before the fix, teardown closed the already-drained startup
    # client and leaked the live one.
    new_redis.aclose.assert_awaited_once()


# ── reload_credentials: no factories ───────────────────────────────────


async def test_reload_noop_when_no_factories() -> None:
    """reload_credentials is a no-op when all resources are DSN-backed."""
    settings = _make_settings()
    # All DSN-backed — but we need to avoid real connections. Override all
    # PG roles with concrete fakes so no DSN fallback is attempted.
    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        # No factories stored — reload is a no-op
        await reload_credentials(deps, drain_timeout=0.5)
        # Nothing changed
        assert deps.dispatcher_pool is conns.dispatcher_pool
        assert deps.notify_conn is conns.notify_conn


# ── reload_credentials: error handling ─────────────────────────────────


async def test_reload_raises_when_called_outside_open_worker_deps() -> None:
    """reload_credentials raises RuntimeError when deps._exit_stack is None."""
    settings = _make_settings()
    # Build a WorkerDeps manually (no open_worker_deps → no _exit_stack)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    with pytest.raises(RuntimeError, match=r"deps\._exit_stack is None"):
        await reload_credentials(deps)


async def test_reload_raises_after_open_worker_deps_exits() -> None:
    """deps._exit_stack must be reset to None when the context exits —
    otherwise a late reload would register new pools on a dead stack and
    they would never be closed."""
    settings = _make_settings()
    pool = _FakePool("dp")
    conns = WorkerConnections(
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        pass
    with pytest.raises(RuntimeError, match=r"deps\._exit_stack is None"):
        await reload_credentials(deps)


# ── reload_credentials: return value ───────────────────────────────────


async def test_reload_returns_reloaded_and_failed_lists() -> None:
    """reload_credentials returns (reloaded, failed) so callers (the
    reload coordinator) can react to partial failures."""
    settings = _make_settings()
    old_worker = _FakePool("old-worker")
    new_worker = _FakePool("new-worker")

    async def failing_factory() -> asyncpg.Pool:
        raise RuntimeError("simulated credential-fetch failure")

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool_factory=_make_pool_factory([_FakePool("hb")]),
        worker_pool_factory=_make_pool_factory([old_worker, new_worker]),
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        deps.heartbeat_pool_factory = failing_factory  # type: ignore[assignment]
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5)
        assert "worker" in reloaded
        assert "heartbeat" in failed
        assert "heartbeat" not in reloaded
        await asyncio.sleep(0.2)


# ── reload_credentials: notify_reconnect_fn ────────────────────────────


async def test_reload_prefers_notify_reconnect_fn_over_direct_swap() -> None:
    """When the listener has registered a reconnect closure, reload must
    use it (it re-registers LISTEN + callbacks) instead of swapping the
    connection directly."""
    settings = _make_settings()
    old_notify = _FakeConn()
    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn_factory=_make_conn_factory([old_notify, _FakeConn()]),
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        reconnect_fn = AsyncMock()
        deps.notify_reconnect_fn = reconnect_fn
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5)

        reconnect_fn.assert_awaited_once()
        assert "notify_conn" in reloaded
        assert failed == []
        # Direct swap NOT performed — conn untouched, no LISTEN re-issued
        assert deps.notify_conn is old_notify


async def test_reload_skips_caller_owned_notify_conn() -> None:
    """A caller-owned notify_conn has no factory — reload must skip it
    cleanly (not record a spurious failure), even when a reconnect
    closure is registered."""
    settings = _make_settings()
    caller_notify = _FakeConn()
    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=caller_notify,  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        reconnect_fn = AsyncMock(side_effect=RuntimeError("no factory"))
        deps.notify_reconnect_fn = reconnect_fn
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5)

        reconnect_fn.assert_not_awaited()
        assert "notify_conn" not in reloaded
        assert "notify_conn" not in failed
        assert deps.notify_conn is caller_notify


async def test_reload_records_notify_factory_failure() -> None:
    """A failing notify factory is recorded in failed; other resources reload."""
    settings = _make_settings()
    old_notify = _FakeConn()

    async def failing_notify_factory() -> asyncpg.Connection:
        raise RuntimeError("simulated credential-fetch failure")

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool_factory=_make_pool_factory([_FakePool("wk-old"), _FakePool("wk-new")]),
        notify_conn_factory=_make_conn_factory([old_notify]),
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        deps.notify_conn_factory = failing_notify_factory  # type: ignore[assignment]
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5)

        assert "notify_conn" in failed
        assert "worker" in reloaded
        # The old notify conn is still live (not drained)
        assert deps.notify_conn is old_notify
        assert not old_notify.closed


async def test_reload_records_redis_factory_failure() -> None:
    """A failing redis factory is recorded in failed; the old client stays."""
    settings = _make_settings()
    old_redis = MagicMock()
    old_redis.aclose = AsyncMock()

    async def redis_factory() -> Any:
        return old_redis

    async def failing_redis_factory() -> Any:
        raise RuntimeError("simulated credential-fetch failure")

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
        redis_client_factory=redis_factory,
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        deps.redis_client_factory = failing_redis_factory  # type: ignore[assignment]
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5)

        assert "redis_client" in failed
        assert "redis_client" not in reloaded
        assert deps.redis_client is old_redis


# ── reload_credentials: drain semantics ────────────────────────────────


async def test_reload_terminates_pool_when_drain_times_out() -> None:
    """A pool that never finishes close() within drain_timeout is
    terminated — checked-out connections must not keep old credentials
    alive indefinitely."""
    settings = _make_settings()
    stuck_pool = _FakePool("stuck")
    stuck_pool.close_wait.clear()  # close() blocks forever
    new_pool = _FakePool("new")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([stuck_pool, new_pool]),
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        await reload_credentials(deps, drain_timeout=0.05)
        assert deps.dispatcher_pool is new_pool
        # Give the background drain task time to hit the timeout
        for _ in range(100):
            if stuck_pool.terminated:
                break
            await asyncio.sleep(0.01)
        assert stuck_pool.terminated


async def test_reload_awaited_leader_close_before_watchdog_reopen() -> None:
    """The old leader conn is closed BEFORE reload returns (bounded), so
    the advisory lock is released before the watchdog's re-election —
    avoiding a guaranteed first-attempt lock conflict."""
    settings = _make_settings()
    old_leader = _FakeConn()

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn_factory=_make_conn_factory([old_leader, _FakeConn()]),
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        await reload_credentials(deps, drain_timeout=0.5)
        assert deps.leader_conn is None
        # Closed synchronously by reload — not left to a background task
        assert old_leader.closed


async def test_reload_leader_close_does_not_null_concurrently_reopened_conn() -> None:
    """While reload awaits the old leader conn's close, the election
    watchdog can reopen via the factory and set deps.leader_conn to the
    NEW conn. Reload must not then null it — an orphaned lock-holding
    conn would block re-election until GC."""
    settings = _make_settings()
    old_leader = _FakeConn()
    new_leader = _FakeConn()
    close_gate = asyncio.Event()

    original_close = old_leader.close

    async def gated_close() -> None:
        await close_gate.wait()
        await original_close()

    old_leader.close = gated_close  # type: ignore[method-assign]  # Why: hold the close open so the watchdog interleaves mid-reload.

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn_factory=_make_conn_factory([old_leader, new_leader]),
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        reload_task = asyncio.create_task(reload_credentials(deps, drain_timeout=0.5))
        await asyncio.sleep(0)  # let reload reach the close await
        # Watchdog reopens concurrently while the close is in flight.
        deps.leader_conn = new_leader  # type: ignore[assignment]
        close_gate.set()
        await reload_task
        assert deps.leader_conn is new_leader  # not nulled by the stale write


# ── reload_credentials: concurrency + factory timeout ──────────────────


async def test_concurrent_reload_calls_are_serialized() -> None:
    """A second reload while one is in flight returns immediately without
    rebuilding anything — overlapping reloads would double-drain pools and
    leak the loser's replacements."""
    settings = _make_settings()
    gate = asyncio.Event()
    gate.set()  # allow the startup build through
    builds = 0

    async def gated_factory() -> asyncpg.Pool:
        nonlocal builds
        builds += 1
        await gate.wait()
        return _FakePool(f"pool-{builds}")  # type: ignore[return-value]

    conns = WorkerConnections(
        dispatcher_pool_factory=gated_factory,
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        gate.clear()  # block the reload phase below

        first = asyncio.create_task(reload_credentials(deps, drain_timeout=0.5))
        await asyncio.sleep(0)  # let the first reload acquire the lock
        second_result = await reload_credentials(deps, drain_timeout=0.5)
        assert second_result == ([], [])

        gate.set()
        await first
        assert builds == 2  # startup + exactly one reload build


async def test_reload_factory_timeout_marks_resource_failed() -> None:
    """A hung factory (e.g. unresponsive token endpoint) must not wedge
    the reload — after factory_timeout the resource is marked failed and
    the remaining resources still reload."""
    settings = _make_settings()

    async def hung_factory() -> asyncpg.Pool:
        await asyncio.sleep(60)
        raise AssertionError("unreachable")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([_FakePool("dp")]),
        heartbeat_pool_factory=_make_pool_factory([_FakePool("hb")]),
        worker_pool_factory=_make_pool_factory([_FakePool("wk-old"), _FakePool("wk-new")]),
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        deps.dispatcher_pool_factory = hung_factory  # type: ignore[assignment]
        reloaded, failed = await reload_credentials(deps, drain_timeout=0.5, factory_timeout=0.05)
        assert "dispatcher" in failed
        assert "worker" in reloaded
        await asyncio.sleep(0.2)


# ── SIGHUP signal handler ──────────────────────────────────────────────


async def test_sighup_sets_reload_event() -> None:
    """The SIGHUP handler sets deps.reload_event."""
    import os
    import signal as _signal

    from taskq._ids import new_uuid
    from taskq.worker.shutdown import install_signal_handlers

    settings = _make_settings()
    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dp"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("hb"),  # type: ignore[arg-type]
        worker_pool=_FakePool("wk"),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()
        escalate_event = asyncio.Event()
        install_signal_handlers(
            loop,
            deps,
            worker_id=new_uuid(),
            shutdown_event=shutdown_event,
            escalate_event=escalate_event,
            backend=MagicMock(),
            orchestrator_holder=[],
        )
        # Send SIGHUP to self
        os.kill(os.getpid(), _signal.SIGHUP)
        await asyncio.sleep(0.1)
        assert deps.reload_event.is_set()

    # Clean up signal handlers
    for sig in (_signal.SIGTERM, _signal.SIGINT, _signal.SIGHUP):
        with contextlib.suppress(NotImplementedError):
            loop.remove_signal_handler(sig)
