"""Unit tests for install_signal_handlers."""

import asyncio
import inspect
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import install_signal_handlers


def _worker_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": "taskq"},
    )


def _worker_deps() -> WorkerDeps:
    pool = Mock()
    return WorkerDeps(
        settings=_worker_settings(),
        dispatcher_pool=pool,  # type: ignore[arg-type] # Why: Mock drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


def _mock_loop() -> tuple[Mock, list[tuple[int, Callable[[], None]]]]:
    captured: list[tuple[int, Callable[[], None]]] = []
    loop = Mock()
    loop.add_signal_handler = Mock(side_effect=lambda sig, cb: captured.append((sig, cb)))
    loop.create_task = Mock()
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

    assert len(handlers) == 2

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
