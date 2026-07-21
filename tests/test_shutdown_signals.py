"""Unit tests for install_signal_handlers and orchestrate_shutdown ownership."""

import asyncio
import inspect
import signal
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import install_signal_handlers, orchestrate_shutdown


def _worker_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": "taskq"},
    )


class _FakeConn:
    """Minimal asyncpg.Connection stand-in for drain_local_queue_to_pending."""

    async def execute(self, query: str, *args: object) -> str:
        return "UPDATE 0"


class _AcquireCtx:
    async def __aenter__(self) -> _FakeConn:
        return _FakeConn()

    async def __aexit__(self, *args: object) -> None:
        pass


class _FakePool:
    """Pool stand-in whose acquire() is a real async context manager."""

    def acquire(self, timeout: float = 30.0) -> _AcquireCtx:
        return _AcquireCtx()


class _FakeLeaderConn:
    """Leader-conn stand-in recording whether TaskQ closed it."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _worker_deps(
    *,
    leader_conn: _FakeLeaderConn | None = None,
    owns_leader_conn: bool = False,
) -> WorkerDeps:
    pool = _FakePool()
    return WorkerDeps(
        settings=_worker_settings(),
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: fake pool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,  # type: ignore[arg-type] # Why: fake conn drop-in; orchestrate_shutdown only awaits .close().
        owns_leader_conn=owns_leader_conn,
    )


def _mock_loop() -> tuple[Mock, list[tuple[int, Callable[[], None]]]]:
    captured: list[tuple[int, Callable[[], None]]] = []
    loop = Mock()
    loop.add_signal_handler = Mock(side_effect=lambda sig, cb: captured.append((sig, cb)))
    # Why close(): the real loop would await the orchestrator coroutine;
    # the mock never does, so close it to avoid "coroutine was never
    # awaited" RuntimeWarnings leaking into unrelated tests at GC time.
    loop.create_task = Mock(side_effect=lambda coro: coro.close())
    return loop, captured


# ── first SIGTERM schedules orchestrator ─────────────────────────────


def test_first_signal_schedules_orchestrator(monkeypatch: pytest.MonkeyPatch) -> None:
    """First signal schedules orchestrate_shutdown and appends task to holder."""
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop, handlers = _mock_loop()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()
    worker_id = new_uuid()

    install_signal_handlers(
        loop,
        deps,
        worker_id,
        shut_event,
        esc_event,
        backend,
        holder,
    )  # type: ignore[arg-type] # Why: Mock not a real AbstractEventLoop but satisfies the interface at runtime.

    assert len(handlers) == 3  # SIGTERM, SIGINT, SIGHUP

    handler = handlers[0][1]
    handler()

    loop.create_task.assert_called_once()
    mock_orch.assert_called_once()
    assert mock_orch.call_args.kwargs["backend"] is backend

    assert len(holder) == 1
    assert not esc_event.is_set()


# ── second SIGTERM sets escalate_event ──────────────────────────────


def test_second_signal_sets_escalate_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second signal sets escalate_event; does NOT append a second task."""
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop, handlers = _mock_loop()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()

    install_signal_handlers(
        loop,
        deps,
        new_uuid(),
        shut_event,
        esc_event,
        backend,
        holder,
    )  # type: ignore[arg-type]

    handler = handlers[0][1]
    handler()
    handler()

    assert mock_orch.call_count == 1
    assert len(holder) == 1
    assert esc_event.is_set()


# ── third SIGTERM calls sys.exit(1) ─────────────────────────────────


def test_third_signal_calls_sys_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Third signal calls sys.exit(1)."""
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop, handlers = _mock_loop()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()

    install_signal_handlers(
        loop,
        deps,
        new_uuid(),
        shut_event,
        esc_event,
        backend,
        holder,
    )  # type: ignore[arg-type]

    handler = handlers[0][1]
    handler()
    handler()

    with pytest.raises(SystemExit) as exc_info:
        handler()

    assert exc_info.value.code == 1


# ── handler is synchronous ─────────────────────────────────────────


def test_handler_is_sync() -> None:
    """Handler callable is NOT a coroutine function."""
    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop, handlers = _mock_loop()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()

    install_signal_handlers(
        loop,
        deps,
        new_uuid(),
        shut_event,
        esc_event,
        backend,
        holder,
    )  # type: ignore[arg-type]

    handler = handlers[0][1]
    assert inspect.iscoroutinefunction(handler) is False


# ── Windows fallback ───────────────────────────────────────────────


def test_windows_fallback_completes_without_handlers() -> None:
    """NotImplementedError on add_signal_handler → function returns normally, no tasks created."""
    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop = Mock()
    loop.add_signal_handler = Mock(side_effect=NotImplementedError("win"))
    loop.create_task = Mock()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()

    install_signal_handlers(
        loop,
        deps,
        new_uuid(),
        shut_event,
        esc_event,
        backend,
        holder,
    )  # type: ignore[arg-type]

    # Function completed without raising; no shutdown tasks or events set.
    assert len(holder) == 0
    assert loop.create_task.call_count == 0
    assert not esc_event.is_set()
    assert not shut_event.is_set()


# ── counter isolation across installer calls ───────────────────────


def test_counter_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two installations → each handler uses its own counter."""
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    esc1 = asyncio.Event()
    esc2 = asyncio.Event()

    loop1, handlers1 = _mock_loop()
    holder1: list[asyncio.Task[int]] = []

    loop2, handlers2 = _mock_loop()
    holder2: list[asyncio.Task[int]] = []

    install_signal_handlers(
        loop1,
        deps,
        new_uuid(),
        asyncio.Event(),
        esc1,
        backend,
        holder1,
    )  # type: ignore[arg-type]

    install_signal_handlers(
        loop2,
        deps,
        new_uuid(),
        asyncio.Event(),
        esc2,
        backend,
        holder2,
    )  # type: ignore[arg-type]

    handler1 = handlers1[0][1]
    handler2 = handlers2[0][1]

    handler1()
    handler2()

    assert loop1.create_task.call_count == 1
    assert loop2.create_task.call_count == 1
    assert len(holder1) == 1
    assert len(holder2) == 1
    assert not esc1.is_set()
    assert not esc2.is_set()


# ── SIGHUP sets reload_event (deterministic, no real signals) ─────


def test_sighup_handler_sets_reload_event() -> None:
    """Captured SIGHUP handler sets deps.reload_event when invoked directly.

    Uses the mock-loop harness (no real process signal), so the test is
    deterministic and cannot leak signal state into the test process.
    """
    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop, handlers = _mock_loop()
    holder: list[asyncio.Task[int]] = []

    install_signal_handlers(
        loop,
        deps,
        new_uuid(),
        asyncio.Event(),
        asyncio.Event(),
        backend,
        holder,
    )  # type: ignore[arg-type] # Why: Mock not a real AbstractEventLoop but satisfies the interface at runtime.

    sighup_handlers = [cb for sig, cb in handlers if sig == signal.SIGHUP]
    assert len(sighup_handlers) == 1
    assert not deps.reload_event.is_set()

    sighup_handlers[0]()
    assert deps.reload_event.is_set()

    # Coalescing is the coordinator's job; the handler stays idempotent.
    sighup_handlers[0]()
    assert deps.reload_event.is_set()

    assert not holder  # SIGHUP must never schedule shutdown work


# ── orchestrate_shutdown leader_conn ownership guard ──────────────


async def test_orchestrate_shutdown_closes_taskq_owned_leader_conn() -> None:
    """TaskQ-owned leader_conn is closed and nulled (early advisory-lock release)."""
    conn = _FakeLeaderConn()
    deps = _worker_deps(leader_conn=conn, owns_leader_conn=True)
    backend = AsyncMock(spec=Backend)
    shut_event = asyncio.Event()

    exit_code = await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        asyncio.Event(),
        backend=backend,
    )

    assert exit_code == 0
    assert conn.closed is True
    assert deps.leader_conn is None
    assert shut_event.is_set()


async def test_orchestrate_shutdown_leaves_caller_owned_leader_conn_unclosed() -> None:
    """Caller-owned leader_conn survives orchestrate_shutdown untouched.

    Ownership contract: "TaskQ never closes caller-owned resources". The
    reference is also left in place — nulling it would make the still-
    running leader election loop open a *fresh* conn and possibly
    re-acquire the advisory lock mid-shutdown.
    """
    conn = _FakeLeaderConn()
    deps = _worker_deps(leader_conn=conn, owns_leader_conn=False)
    backend = AsyncMock(spec=Backend)
    shut_event = asyncio.Event()

    exit_code = await orchestrate_shutdown(
        deps,
        deps.settings,
        new_uuid(),
        shut_event,
        asyncio.Event(),
        backend=backend,
    )

    assert exit_code == 0
    assert conn.closed is False
    assert deps.leader_conn is conn
    assert shut_event.is_set()
