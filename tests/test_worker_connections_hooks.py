"""Unit tests for open_worker_deps connection hook points.

Verifies the ownership model (caller-owned vs TaskQ-owned), teardown
semantics, and DSN-elimination when all PG roles are overridden. Uses
fakes — no real Postgres required. Integration tests live in
test_worker_deps.py (marked ``integration``).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import asyncpg

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps

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

    def __init__(self) -> None:
        self.closed = False
        self._acquire_mock = AsyncMock()
        self._acquire_mock.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        self._acquire_mock.return_value.__aexit__ = AsyncMock(return_value=None)

    async def acquire(self, **_kw: object) -> object:
        return self._acquire_mock()

    async def close(self) -> None:
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> _FakePool:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class _FakeConn:
    """Fake asyncpg.Connection that tracks close() and execute() calls."""

    def __init__(self) -> None:
        self.closed = False
        self.executed: list[str] = []
        self.listeners: dict[str, object] = {}

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def add_listener(self, channel: str, callback: object) -> None:
        self.listeners[channel] = callback

    async def remove_listener(self, channel: str, callback: object) -> None:
        self.listeners.pop(channel, None)

    async def close(self) -> None:
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed


def _make_pool_factory(fake: _FakePool) -> object:
    async def factory() -> asyncpg.Pool:
        return fake  # type: ignore[return-value]

    return factory


def _make_conn_factory(fake: _FakeConn) -> object:
    async def factory() -> asyncpg.Connection:
        return fake  # type: ignore[return-value]

    return factory


# ── Concrete (caller-owned) resources ──────────────────────────────────


async def test_concrete_pools_not_closed_on_teardown() -> None:
    """Caller-owned concrete pools are NOT closed by open_worker_deps."""
    settings = _make_settings()
    dispatcher = _FakePool()
    heartbeat = _FakePool()
    worker = _FakePool()
    notify = _FakeConn()
    leader = _FakeConn()

    conns = WorkerConnections(
        dispatcher_pool=dispatcher,  # type: ignore[arg-type]
        heartbeat_pool=heartbeat,  # type: ignore[arg-type]
        worker_pool=worker,  # type: ignore[arg-type]
        notify_conn=notify,  # type: ignore[arg-type]
        leader_conn=leader,  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.dispatcher_pool is dispatcher
        assert deps.heartbeat_pool is heartbeat
        assert deps.worker_pool is worker
        assert deps.notify_conn is notify
        assert deps.leader_conn is leader

    # Teardown must NOT close caller-owned resources.
    assert not dispatcher.closed
    assert not heartbeat.closed
    assert not worker.closed
    assert not notify.closed
    assert not leader.closed


async def test_concrete_notify_conn_still_gets_listen() -> None:
    """TaskQ issues LISTEN on a caller-owned notify_conn."""
    settings = _make_settings(TASKQ_NOTIFY_ENABLED="true")
    notify = _FakeConn()

    # Override all PG roles so no DSN fallback is attempted.
    conns = WorkerConnections(
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=notify,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.notify_conn is notify
        # LISTEN was issued
        assert any(sql.startswith("LISTEN") for sql in notify.executed)

    # Caller-owned — not closed
    assert not notify.closed


# ── Factory (TaskQ-owned) resources ────────────────────────────────────


async def test_factory_pools_closed_on_teardown() -> None:
    """Factory-produced pools ARE closed by open_worker_deps."""
    settings = _make_settings()
    dispatcher = _FakePool()
    heartbeat = _FakePool()
    worker = _FakePool()
    notify = _FakeConn()
    leader = _FakeConn()

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory(dispatcher),
        heartbeat_pool_factory=_make_pool_factory(heartbeat),
        worker_pool_factory=_make_pool_factory(worker),
        notify_conn_factory=_make_conn_factory(notify),
        leader_conn_factory=_make_conn_factory(leader),
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.dispatcher_pool is dispatcher
        assert deps.notify_conn is notify
        assert deps.leader_conn is leader

    # Teardown MUST close TaskQ-owned resources.
    assert dispatcher.closed
    assert heartbeat.closed
    assert worker.closed
    assert notify.closed
    assert leader.closed


async def test_factory_redis_closed_on_teardown() -> None:
    """Factory-produced Redis client IS closed by open_worker_deps."""
    settings = _make_settings()
    fake_redis = MagicMock()
    fake_redis.aclose = AsyncMock()

    async def redis_factory() -> object:
        return fake_redis

    # Override all PG roles so no DSN fallback is attempted.
    conns = WorkerConnections(
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
        redis_client_factory=redis_factory,
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.redis_client is fake_redis

    fake_redis.aclose.assert_awaited_once()


async def test_concrete_redis_not_closed_on_teardown() -> None:
    """Caller-owned Redis client is NOT closed by open_worker_deps."""
    settings = _make_settings()
    fake_redis = MagicMock()
    fake_redis.aclose = AsyncMock()

    # Override all PG roles so no DSN fallback is attempted.
    conns = WorkerConnections(
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=_FakeConn(),  # type: ignore[arg-type]
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
        redis_client=fake_redis,
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.redis_client is fake_redis

    fake_redis.aclose.assert_not_awaited()


# ── DSN elimination ────────────────────────────────────────────────────


async def test_all_pg_overridden_no_dsn_needed() -> None:
    """When every PG role is overridden, pg_dsn_direct/pooled can be None.

    The settings still carry the fake DSN (load_from_dict requires it), but
    this test verifies that no DSN-fallback path is exercised — the fake
    DSN host would fail to connect, so success proves the fallback was
    never reached.
    """
    settings = _make_settings()
    # Corrupt the DSNs to prove they're never used.
    settings.pg_dsn_direct = None  # type: ignore[assignment]
    settings.pg_dsn_pooled = None  # type: ignore[assignment]

    dispatcher = _FakePool()
    heartbeat = _FakePool()
    worker = _FakePool()
    notify = _FakeConn()
    leader = _FakeConn()

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory(dispatcher),
        heartbeat_pool_factory=_make_pool_factory(heartbeat),
        worker_pool_factory=_make_pool_factory(worker),
        notify_conn_factory=_make_conn_factory(notify),
        leader_conn_factory=_make_conn_factory(leader),
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.dispatcher_pool is dispatcher
        assert deps.worker_pool is worker

    # All factory-produced → closed
    assert dispatcher.closed
    assert worker.closed


async def test_partial_override_still_uses_dsn_for_uncovered_roles() -> None:
    """Overriding only worker_pool leaves the direct DSN required for notify/leader.

    This test patches asyncpg.create_pool and open_dedicated_conn to fakes
    so no real connection is attempted, proving the DSN fallback path is
    reached for the non-overridden roles.
    """
    # Use distinct pool sizes so the fake create_pool can distinguish them.
    settings = _make_settings(
        TASKQ_DISPATCHER_POOL_SIZE="4",
        TASKQ_HEARTBEAT_POOL_SIZE="3",
    )
    worker = _FakePool()

    conns = WorkerConnections(
        worker_pool_factory=_make_pool_factory(worker),
    )

    fake_dispatcher = _FakePool()
    fake_heartbeat = _FakePool()
    fake_notify = _FakeConn()
    fake_leader = _FakeConn()

    async def fake_pool(**kw: object) -> _FakePool:
        max_size = kw.get("max_size", 0)
        if max_size == settings.dispatcher_pool_size:
            return fake_dispatcher  # type: ignore[return-value]
        return fake_heartbeat  # type: ignore[return-value]

    async def fake_open_dedicated(_dsn: str, *, label: str, **_kw: object) -> _FakeConn:
        if label == "notify":
            return fake_notify  # type: ignore[return-value]
        return fake_leader  # type: ignore[return-value]

    import taskq.worker.deps as deps_mod

    original_create_pool = asyncpg.create_pool
    original_open_dedicated = deps_mod.open_dedicated_conn
    asyncpg.create_pool = fake_pool  # type: ignore[assignment]
    deps_mod.open_dedicated_conn = fake_open_dedicated  # type: ignore[assignment]
    try:
        async with open_worker_deps(settings, connections=conns) as deps:
            assert deps.dispatcher_pool is fake_dispatcher
            assert deps.heartbeat_pool is fake_heartbeat
            assert deps.worker_pool is worker  # factory-provided
            assert deps.notify_conn is fake_notify
            assert deps.leader_conn is fake_leader
    finally:
        asyncpg.create_pool = original_create_pool  # type: ignore[assignment]
        deps_mod.open_dedicated_conn = original_open_dedicated  # type: ignore[assignment]

    # Factory pool closed; DSN pools closed (via stack); DSN conns closed
    assert worker.closed
    assert fake_dispatcher.closed
    assert fake_heartbeat.closed
    assert fake_notify.closed
    assert fake_leader.closed
