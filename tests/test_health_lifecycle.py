"""Unit tests for HealthServer lifecycle wiring in worker/run.py:_main."""

import asyncio
import errno
from typing import Literal
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker import (
    _bootstrap as _bootstrap_mod,  # pyright: ignore[reportPrivateImportUsage]  # Why: the namespace whose asyncio binding must carry the Event fake (patch where it is LOOKED UP).
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.health import HealthUnixBindCollisionError
from taskq.worker.run import _main
from tests._ns_patch import module_ns_proxy
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _ws(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _FAKE_DSN}
    for k, v in overrides.items():
        data[f"TASKQ_{k}" if not k.startswith("TASKQ_") else k] = v
    return WorkerSettings.load_from_dict(data)


async def _noop(*args: object, **kwargs: object) -> None:
    pass


class _ImmediateEvent(asyncio.Event):
    """asyncio.Event whose wait() sets and returns immediately."""

    async def wait(self) -> Literal[True]:
        if not self.is_set():
            self.set()
        return True


class _FakeMaintenanceLeader:
    def __init__(
        self,
        deps: object,
        worker_id: object,
        backend: object,
        *,
        clock: object = None,
        rate_limit_registry: object = None,
        actor_policies: object = None,
    ) -> None:
        pass

    async def run(self, shutdown_event: object) -> None:
        pass


class _RecordingHealthServer:
    """Recording fake for HealthServer: appends lifecycle calls to a shared list."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start(self, deps: WorkerDeps) -> None:
        self._events.append("health.start")

    async def stop(self) -> None:
        self._events.append("health.stop")


class _FailingHealthServer:
    """Fake that raises on start(), recording the attempt."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start(self, deps: WorkerDeps) -> None:
        self._events.append("health.start")
        raise RuntimeError("health server start failed")

    async def stop(self) -> None:
        self._events.append("health.stop")


class _UnixCollisionHealthServer:
    """Fake for the partial start: start() raises the collision type
    a real server raises when a live peer owns the socket path but the TCP
    listener is already up: the server owns something, so stop() must
    still run even though start() raised."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start(self, deps: WorkerDeps) -> None:
        self._events.append("health.start")
        raise HealthUnixBindCollisionError(
            "/tmp/tqhl-collided.sock",  # noqa: S108  # Why: stand-in path never bound; /tmp keeps the 104-char sun_path budget honest.
            OSError(errno.EADDRINUSE, "Address '/tmp/tqhl-collided.sock' is already in use"),
        )

    async def stop(self) -> None:
        self._events.append("health.stop")


def _setup_lifecycle_stubs(
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    health_enabled: bool = True,
    failing_health: bool = False,
    unix_collision: bool = False,
) -> WorkerSettings:
    """Install all monkeypatches needed for lifecycle unit tests.

    Returns the WorkerSettings used for the deps stub so callers can
    assert on health_enabled etc.
    """
    ws = _ws(TASKQ_HEALTH_ENABLED=str(health_enabled).lower())

    # ── Stub open_worker_deps ──────────────────────────────────────────
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _stub_open_worker_deps(
        settings: WorkerSettings,
        *,
        connections: WorkerConnections | None = None,
    ):
        fake_pool = _FakePool()
        deps = WorkerDeps(  # type: ignore[call-arg] # Why: WorkerDeps requires a full set of asyncpg pools; lifecycle test only needs the settings field and an is_leader Event - passing class objects as pool stubs avoids spinning up real pools.
            settings=settings,
            dispatcher_pool=fake_pool,  # type: ignore[arg-type]
            heartbeat_pool=fake_pool,  # type: ignore[arg-type]
            worker_pool=fake_pool,  # type: ignore[arg-type]
            notify_conn=None,
            leader_conn=None,
        )
        events.append("pools_open")
        try:
            yield deps
        finally:
            events.append("pools_close")

    monkeypatch.setattr("taskq.worker._bootstrap.open_worker_deps", _stub_open_worker_deps)

    # ── Stub backend and registration ──────────────────────────────────
    monkeypatch.setattr("taskq.worker._bootstrap.PostgresBackend", lambda *a, **kw: object())  # type: ignore[arg-type] # Why: replacing PostgresBackend with a plain object stub; only lifecycle ordering is under test, not backend behaviour.
    _worker_id = new_uuid()

    async def _stub_register_worker(pool: asyncpg.Pool, settings: WorkerSettings) -> UUID:
        return _worker_id

    monkeypatch.setattr("taskq.worker.run.register_worker", _stub_register_worker)
    monkeypatch.setattr("taskq.worker._bootstrap.install_signal_handlers", lambda *a, **kw: None)
    monkeypatch.setattr("taskq.worker.run.deregister_worker", _noop)  # type: ignore[arg-type] # Why: _noop has a broader *args/**kwargs signature than deregister_worker; arg-type mismatch is intentional for the stub.

    # ── Stub task siblings ─────────────────────────────────────────────
    monkeypatch.setattr("taskq.worker._bootstrap.heartbeat_loop", _noop)
    monkeypatch.setattr("taskq.worker._bootstrap.notify_listener_loop", _noop)
    monkeypatch.setattr("taskq.worker._bootstrap.MaintenanceLeader", _FakeMaintenanceLeader)
    monkeypatch.setattr("taskq.worker.run.make_heartbeat_kwargs", lambda *a, **kw: {})
    monkeypatch.setattr("taskq.worker.run.producer_loop", _noop)
    monkeypatch.setattr("taskq.worker.run.consumer_loop_stub", _noop)

    # ── HealthServer fake ──────────────────────────────────────────────
    if failing_health:
        monkeypatch.setattr(
            "taskq.worker._bootstrap.HealthServer",
            lambda: _FailingHealthServer(events),
        )
    elif unix_collision:
        monkeypatch.setattr(
            "taskq.worker._bootstrap.HealthServer",
            lambda: _UnixCollisionHealthServer(events),
        )
    else:
        monkeypatch.setattr(
            "taskq.worker._bootstrap.HealthServer",
            lambda: _RecordingHealthServer(events),
        )

    # ── Make shutdown_event.wait() return immediately ──────────────────
    # Patch where the name is LOOKED UP - the bootstrap module's own
    # ``asyncio`` binding (it constructs shutdown_event/escalate_event at
    # _bootstrap.py) - not through to the global asyncio module, which the
    # old spelling swung for every namespace in the process
    # (tests/_ns_patch.py).
    monkeypatch.setattr(_bootstrap_mod, "asyncio", module_ns_proxy(asyncio, Event=_ImmediateEvent))

    return ws


# ── Lifecycle order ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lifecycle_order_pools_open_health_start_then_stop_then_pools_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HealthServer.start follows pool open; stop precedes pool close."""
    events: list[str] = []
    ws = _setup_lifecycle_stubs(monkeypatch, events, health_enabled=True)

    result = await _main(ws)

    assert result == 0
    assert events == ["pools_open", "health.start", "health.stop", "pools_close"]


# ── health_enabled=False skips the server ────────────────────────────────────


@pytest.mark.asyncio
async def test_health_enabled_false_skips_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When health_enabled=False, HealthServer is never instantiated or called."""
    events: list[str] = []
    ws = _setup_lifecycle_stubs(monkeypatch, events, health_enabled=False)

    result = await _main(ws)

    assert result == 0
    assert events == ["pools_open", "pools_close"]
    assert "health.start" not in events
    assert "health.stop" not in events


# ── Start failure propagates without leaking pools ───────────────────────────


@pytest.mark.asyncio
async def test_start_failure_propagates_deps_stack_unwinds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When health_server.start() raises, the exception propagates and
    the deps stack still unwinds (pools_close observed)."""
    events: list[str] = []
    ws = _setup_lifecycle_stubs(
        monkeypatch,
        events,
        health_enabled=True,
        failing_health=True,
    )

    with pytest.raises(RuntimeError, match="health server start failed"):
        await _main(ws)

    assert "pools_open" in events
    assert "pools_close" in events
    assert "health.stop" not in events


# ── A unix collision with the TCP listener up keeps booting AND stopping ───


@pytest.mark.asyncio
async def test_unix_collision_warns_and_boots_but_still_stops_the_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bootstrap half: ``HealthUnixBindCollisionError`` means the server
    owns the TCP listener it managed to bind, so the boot that continues
    on the strength of that listener must still push the stop callback,
    otherwise the probe port would answer for a dead worker until the
    process exits. The stop ordering is the assertion: it must sit
    between health.start and pools_close exactly as a healthy boot's
    does."""
    events: list[str] = []
    ws = _setup_lifecycle_stubs(
        monkeypatch,
        events,
        health_enabled=True,
        unix_collision=True,
    )

    result = await _main(ws)

    assert result == 0, "a unix-path collision with TCP up must not abort the boot"
    assert events == ["pools_open", "health.start", "health.stop", "pools_close"], (
        "the collision boot must run the full lifecycle: start (partial), "
        "stop (owed: the TCP listener is owned), pools close"
    )
