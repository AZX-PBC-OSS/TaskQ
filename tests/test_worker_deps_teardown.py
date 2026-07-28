"""Unit tests for the BOUNDED final teardown in open_worker_deps.

Pre-fix, final teardown performed UNBOUNDED graceful closes: pools were
entered on the AsyncExitStack (``pool.__aexit__`` → ``pool.close()``) and
dedicated connections were closed with a bare ``await conn.close()``. A
dead PG (e.g. a chaos-killed container) can block ``Pool.close()``
indefinitely — a CI chaos run hung >300s that way. The reload path
already bounds its closes (``_drain_old_pool``/``_drain_old_conn`` with
``drain_timeout`` + ``terminate()``); these tests pin the same bound for
the final teardown path via ``CLOSE_TIMEOUT_SECS``.

Docker-free: hand-rolled fakes wired through the REAL ``open_worker_deps``
via ``WorkerConnections`` factories (asyncpg types are C-extensions — no
MagicMock for pool/conn). Fake conventions mirror
``tests/test_reload_credentials.py`` (hang-gate ``close_wait``,
``terminated`` flag, ``close_calls`` counter).

No ``pytestmark`` — must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Self

import asyncpg
import pytest
import structlog.testing

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker import deps as deps_mod
from taskq.worker.deps import open_worker_deps, reload_credentials

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
    """Fake asyncpg.Pool tracking close()/terminate() with a hang gate."""

    def __init__(self, name: str = "", close_events: list[str] | None = None) -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.aexit_calls = 0
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.close_error: Exception | None = None
        self._close_events = close_events

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return  # close-after-close/terminate is a no-op on a real pool
        await self.close_wait.wait()
        if self.close_error is not None:
            raise self.close_error
        self.closed = True
        if self._close_events is not None:
            self._close_events.append(self.name)

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()  # aborts any in-flight close() wait

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        # Why no close() here: pre-fix teardown entered pools via
        # ``stack.enter_async_context`` and relied on this dunder to close
        # them; the fix pushes an explicit bounded-close callback instead.
        # If this fake closed itself on __aexit__, the fast-close and LIFO
        # tests below would pass against the OLD code and prove nothing.
        self.aexit_calls += 1


class _FakeConn:
    """Fake asyncpg.Connection tracking close()/terminate() with a hang gate."""

    def __init__(self, name: str = "", close_events: list[str] | None = None) -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.executed: list[str] = []
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self._close_events = close_events

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        return "OK"

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return
        await self.close_wait.wait()
        self.closed = True
        if self._close_events is not None:
            self._close_events.append(self.name)

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.close_wait.set()  # aborts any in-flight close() wait

    def is_closed(self) -> bool:
        return self.closed


class _FakeRedisClient:
    """Fake Redis client tracking aclose() with a hang gate."""

    def __init__(self) -> None:
        self.aclose_calls = 0
        self.aclose_wait = asyncio.Event()
        self.aclose_wait.set()  # aclose() completes instantly by default

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await self.aclose_wait.wait()


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


def _shrink_teardown_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shrink the teardown close bound so hung-close tests stay fast."""
    monkeypatch.setattr(deps_mod, "CLOSE_TIMEOUT_SECS", 0.05)


# ── Bounded pool teardown ──────────────────────────────────────────────


async def test_teardown_terminates_pool_when_close_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pool whose close() never returns (dead PG) is terminated after the
    bounded teardown timeout; teardown itself completes and other pools are
    closed gracefully (not terminated)."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    dispatcher = _FakePool("dispatcher")
    heartbeat = _FakePool("heartbeat")
    worker = _FakePool("worker")
    notify = _FakeConn("notify")
    leader = _FakeConn("leader")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([heartbeat]),
        worker_pool_factory=_make_pool_factory([worker]),
        notify_conn_factory=_make_conn_factory([notify]),
        leader_conn_factory=_make_conn_factory([leader]),
    )
    # Why the outer timeout: pre-fix teardown closes pools unbounded, so the
    # RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        async with open_worker_deps(settings, connections=conns):
            dispatcher.close_wait.clear()  # close() blocks forever from now on

    assert dispatcher.terminated is True
    assert dispatcher.close_calls == 1
    for pool in (heartbeat, worker):
        assert pool.closed is True
        assert pool.terminated is False
        assert pool.close_calls == 1
    for conn in (notify, leader):
        assert conn.closed is True
        assert conn.terminated is False


async def test_teardown_does_not_terminate_on_fast_close() -> None:
    """Healthy close(): every TaskQ-owned pool/conn is closed exactly once,
    nothing is terminated."""
    settings = _make_settings()
    dispatcher = _FakePool("dispatcher")
    heartbeat = _FakePool("heartbeat")
    worker = _FakePool("worker")
    notify = _FakeConn("notify")
    leader = _FakeConn("leader")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([heartbeat]),
        worker_pool_factory=_make_pool_factory([worker]),
        notify_conn_factory=_make_conn_factory([notify]),
        leader_conn_factory=_make_conn_factory([leader]),
    )
    async with open_worker_deps(settings, connections=conns):
        pass

    for pool in (dispatcher, heartbeat, worker):
        assert pool.closed is True
        assert pool.terminated is False
        assert pool.close_calls == 1
    for conn in (notify, leader):
        assert conn.closed is True
        assert conn.terminated is False
        assert conn.close_calls == 1


async def test_teardown_never_closes_caller_owned_resources() -> None:
    """Caller-owned (concrete) resources are never closed by teardown — the
    ownership contract holds on the bounded path too. Guards the pre-existing
    invariant against regressions from the teardown rework."""
    settings = _make_settings()
    dispatcher = _FakePool("dispatcher")
    heartbeat = _FakePool("heartbeat")
    worker = _FakePool("worker")
    notify = _FakeConn("notify")
    leader = _FakeConn("leader")
    redis_client = _FakeRedisClient()

    conns = WorkerConnections(
        dispatcher_pool=dispatcher,  # type: ignore[arg-type]
        heartbeat_pool=heartbeat,  # type: ignore[arg-type]
        worker_pool=worker,  # type: ignore[arg-type]
        notify_conn=notify,  # type: ignore[arg-type]
        leader_conn=leader,  # type: ignore[arg-type]
        redis_client=redis_client,  # type: ignore[arg-type]
    )
    async with open_worker_deps(settings, connections=conns):
        pass

    for pool in (dispatcher, heartbeat, worker):
        assert pool.close_calls == 0
        assert pool.closed is False
        assert pool.terminated is False
    for conn in (notify, leader):
        assert conn.close_calls == 0
        assert conn.closed is False
        assert conn.terminated is False
    assert redis_client.aclose_calls == 0


async def test_teardown_lifo_order_conns_before_pools() -> None:
    """Teardown unwinds LIFO: dedicated conns close before pools, pools in
    reverse startup order."""
    settings = _make_settings()
    order: list[str] = []
    dispatcher = _FakePool("dispatcher", order)
    heartbeat = _FakePool("heartbeat", order)
    worker = _FakePool("worker", order)
    notify = _FakeConn("notify", order)
    leader = _FakeConn("leader", order)

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([heartbeat]),
        worker_pool_factory=_make_pool_factory([worker]),
        notify_conn_factory=_make_conn_factory([notify]),
        leader_conn_factory=_make_conn_factory([leader]),
    )
    async with open_worker_deps(settings, connections=conns):
        pass

    assert order == ["leader", "notify", "worker", "heartbeat", "dispatcher"]


async def test_teardown_close_error_does_not_propagate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pool close() that raises is logged and swallowed — teardown keeps
    unwinding so LIFO-later resources (dispatcher) are still closed."""
    settings = _make_settings()
    dispatcher = _FakePool("dispatcher")
    heartbeat = _FakePool("heartbeat")
    heartbeat.close_error = RuntimeError("simulated PG close failure")
    worker = _FakePool("worker")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([dispatcher]),
        heartbeat_pool_factory=_make_pool_factory([heartbeat]),
        worker_pool_factory=_make_pool_factory([worker]),
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
    )
    # No pytest.raises: the RuntimeError must not escape the async with.
    async with open_worker_deps(settings, connections=conns):
        pass

    assert heartbeat.close_calls == 1
    assert heartbeat.closed is False  # raise happened before the close completed
    assert worker.closed is True
    # dispatcher unwinds AFTER heartbeat (LIFO) — proof the error was contained.
    assert dispatcher.closed is True
    assert dispatcher.close_calls == 1


# ── Bounded dedicated-conn teardown ────────────────────────────────────


async def test_teardown_terminates_notify_conn_when_close_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung notify_conn close is terminated after the bound and the attr
    is nulled, so teardown completes."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    notify = _FakeConn("notify")

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn_factory=_make_conn_factory([notify]),
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
    )
    # Why the outer timeout: pre-fix teardown awaited conn.close() unbounded,
    # so the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        async with open_worker_deps(settings, connections=conns) as deps:
            notify.close_wait.clear()  # close() blocks forever from now on
        assert deps.notify_conn is None

    assert notify.terminated is True
    assert notify.close_calls == 1


async def test_teardown_terminates_leader_conn_when_close_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung leader_conn close is terminated after the bound and the attr
    is nulled, so teardown completes."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    leader = _FakeConn("leader")

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn_factory=_make_conn_factory([leader]),
    )
    # Why the outer timeout: pre-fix teardown awaited conn.close() unbounded,
    # so the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        async with open_worker_deps(settings, connections=conns) as deps:
            leader.close_wait.clear()  # close() blocks forever from now on
        assert deps.leader_conn is None

    assert leader.terminated is True
    assert leader.close_calls == 1


# ── Bounded teardown after a hot-swap reload ───────────────────────────


async def test_teardown_bounds_hot_swapped_pool_after_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pool swapped in by reload_credentials is registered for the SAME
    bounded teardown: if the NEW pool's close hangs at shutdown, it is
    terminated. Covers the reload_credentials registration site (inside the
    pool loop — default-arg binding must capture the right pool/label)."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    old_dispatcher = _FakePool("old-dispatcher")
    new_dispatcher = _FakePool("new-dispatcher")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([old_dispatcher, new_dispatcher]),
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
    )
    # Why the outer timeout: pre-fix reload registered new pools via
    # enter_async_context (unbounded __aexit__ close), so a hanging close in
    # the RED state would wedge teardown forever instead of failing fast.
    async with asyncio.timeout(5):
        async with open_worker_deps(settings, connections=conns) as deps:
            assert deps.dispatcher_pool is old_dispatcher
            await reload_credentials(deps, drain_timeout=0.05)
            assert deps.dispatcher_pool is new_dispatcher
            # Let the background drain of the old pool finish before exit.
            await asyncio.sleep(0.2)
            new_dispatcher.close_wait.clear()  # teardown close hangs from now on

    assert new_dispatcher.terminated is True
    assert new_dispatcher.close_calls == 1
    assert old_dispatcher.closed is True  # drained by the reload itself
    assert old_dispatcher.terminated is False


# ── Bounded Redis teardown ─────────────────────────────────────────────


async def test_teardown_bounds_redis_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung Redis aclose() is bounded: teardown logs and continues (Redis
    has no terminate()) — no exception escapes. The timeout event carries
    ``close_timeout=`` — NOT ``drain_timeout=``, the reload path's
    drain-event field — so the teardown close bound stays distinguishable
    from the reload drain bound in log alerts (review C9)."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    redis_client = _FakeRedisClient()

    async def redis_factory() -> Any:
        return redis_client

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
        redis_client_factory=redis_factory,
    )
    # Why the outer timeout: pre-fix teardown awaited redis_client.aclose()
    # unbounded, so the RED state would hang forever instead of failing fast.
    with structlog.testing.capture_logs() as captured:
        async with asyncio.timeout(5):
            async with open_worker_deps(settings, connections=conns) as deps:
                assert deps.redis_client is redis_client
                redis_client.aclose_wait.clear()  # aclose() blocks forever from now on

    assert redis_client.aclose_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "redis-teardown-close-timeout"]
    assert len(timeout_events) == 1, f"expected 1 redis timeout event, got {captured!r}"
    event = timeout_events[0]
    assert event.get("close_timeout") == 0.05, f"expected close_timeout= field, got {event!r}"
    assert "drain_timeout" not in event, (
        f"drain_timeout= belongs to the reload path's drain events, got {event!r}"
    )


async def test_teardown_nulls_redis_client_after_close() -> None:
    """After teardown closes a TaskQ-owned Redis client, ``deps.redis_client``
    is None — mirroring the notify/leader conn guards (review C6). Reload
    interplay is safe: ``reload_credentials`` swaps the attr and drains the old
    client itself, so the guard reads the attr once, closes, and nulls — the
    same lifecycle as the conn siblings."""
    settings = _make_settings()
    redis_client = _FakeRedisClient()

    async def redis_factory() -> Any:
        return redis_client

    conns = WorkerConnections(
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
        redis_client_factory=redis_factory,
    )
    async with open_worker_deps(settings, connections=conns) as deps:
        assert deps.redis_client is redis_client

    assert redis_client.aclose_calls == 1
    assert deps.redis_client is None


# ── C9: teardown close events carry close_timeout=, not drain_timeout= ──


async def test_teardown_close_timeout_logs_close_timeout_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bounded-close timeout event carries ``close_timeout=`` — NOT
    ``drain_timeout=``, which is the reload path's drain-event field
    (``pool-draining``, ``pool-drain-timeout-terminating``). Sharing the
    field name conflates the teardown close bound with the reload drain
    bound in log alerts (review C9)."""
    _shrink_teardown_timeout(monkeypatch)
    settings = _make_settings()
    dispatcher = _FakePool("dispatcher")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory([dispatcher]),
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=_FakeConn("notify"),  # type: ignore[arg-type]
        leader_conn=_FakeConn("leader"),  # type: ignore[arg-type]
    )
    with structlog.testing.capture_logs() as captured:
        async with asyncio.timeout(5):
            async with open_worker_deps(settings, connections=conns):
                dispatcher.close_wait.clear()  # close() blocks forever from now on

    timeout_events = [
        e for e in captured if e.get("event") == "pool-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 timeout event, got {captured!r}"
    event = timeout_events[0]
    assert event.get("close_timeout") == 0.05, f"expected close_timeout= field, got {event!r}"
    assert "drain_timeout" not in event, (
        f"drain_timeout= belongs to the reload path's drain events, got {event!r}"
    )
