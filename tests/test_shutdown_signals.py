"""Unit tests for install_signal_handlers and orchestrate_shutdown ownership."""

import asyncio
import inspect
import os
import signal
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock, patch

import pytest
import structlog.testing

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

    assert len(handlers) == 4  # SIGTERM, SIGINT, SIGHUP, SIGUSR2

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
    reference is also left in place - nulling it would make the still-
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


# ── SIGUSR2 dumps task stacks (deterministic, no real signals) ─────


def test_sigusr2_handler_dumps_task_stacks() -> None:
    """The captured SIGUSR2 handler emits an on-demand task dump.

    This is the on-demand half of the watchdog's diagnostics: answering
    "what is this live-but-idle worker waiting on?" without rebuilding the
    image with instrumentation. It must NOT terminate the worker - SIGUSR1
    would (default action), which is why the dump is on SIGUSR2.
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

    usr2_handlers = [cb for sig, cb in handlers if sig == signal.SIGUSR2]
    assert len(usr2_handlers) == 1

    with patch("taskq.worker.shutdown.dump_task_stacks") as mock_dump:
        usr2_handlers[0]()

    assert mock_dump.call_count == 1


# ── The H2 guard: an in-progress orchestration owns the first signal ────


def test_first_signal_during_orchestration_starts_nothing_and_the_next_escalates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The H2 guard (:95) and the non-reset counter, pinned together.

    The drain monitor can trigger the orchestration before any signal
    arrives: ``deps.shutdown_phase`` is past NONE while
    ``orchestrator_holder`` is still empty. The FIRST signal must then
    start NOTHING - no second orchestration task (double-orchestration
    runs the phases twice against the same rows) - and must NOT arm the
    escalate event either (nothing is mid-CANCELLING to fast-advance;
    arming it here would break the CANCELLING grace of the running
    orchestration). The signal counter is deliberately NOT consumed by
    the guard's return: the NEXT signal reaches the ``== 2`` rung and
    escalates the already-running orchestration - a regression that
    reset the counter here would restart a fresh orchestration on the
    next signal instead of escalating.
    """
    import taskq.worker.shutdown as shutdown_mod
    from taskq.worker.shutdown import ShutdownPhase

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    deps.shutdown_phase = ShutdownPhase.DRAINING  # the drain monitor got here first
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
    )  # type: ignore[arg-type] # Why: Mock not a real AbstractEventLoop but satisfies the interface at runtime.

    handler = handlers[0][1]
    handler()

    assert loop.create_task.call_count == 0, (
        "a first signal during an in-progress orchestration scheduled a "
        "SECOND orchestration task - the phases would run twice against "
        "the same rows"
    )
    assert mock_orch.call_count == 0
    assert not esc_event.is_set(), (
        "the guard's return must not consume the counter as an escalation: "
        "nothing is mid-CANCELLING to fast-advance yet"
    )

    handler()
    assert esc_event.is_set(), (
        "the second signal must escalate the already-running orchestration "
        "- a counter reset inside the guard would restart a fresh one instead"
    )
    assert loop.create_task.call_count == 0
    assert len(holder) == 0


def test_signal_ladder_full_escalation_through_the_handler_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1st SIGTERM orchestrates, 2nd escalates past a live phase stamp,
    3rd exits - the whole ladder driven through the captured handler.

    The phase stamp matters between rungs: by the second signal the first
    signal's orchestration has typically progressed past NONE (here
    DRAINING), so the ladder must read the RUNTIME state (both the holder
    and ``deps.shutdown_phase``, the shared ``_orchestration_in_progress``
    predicate) and still land the escalation on rung two. The three
    regressions pinned: a second ``create_task`` on signal two
    (double-orchestration), the escalate event NOT set on signal two (a
    hung CANCELLING grace no operator can shorten), and a third signal
    that does anything but ``sys.exit(1)`` (the operator's last resort
    before SIGKILL must actually terminate).
    """
    import taskq.worker.shutdown as shutdown_mod
    from taskq.worker.shutdown import ShutdownPhase

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
    assert loop.create_task.call_count == 1
    assert len(holder) == 1
    assert not esc_event.is_set()

    # The orchestration has started executing phases (the first signal's
    # task stamped the phase): the ladder must keep its rungs.
    deps.shutdown_phase = ShutdownPhase.DRAINING

    handler()
    assert esc_event.is_set(), "the second signal must fast-advance CANCELLING → FORCING"
    assert loop.create_task.call_count == 1, (
        "the second signal scheduled a second orchestration task - the "
        "ladder escalated AND re-orchestrated"
    )
    assert len(holder) == 1

    with pytest.raises(SystemExit) as exc_info:
        handler()
    assert exc_info.value.code == 1, "the third signal must exit 1, the pre-SIGKILL backstop"


# ── Registration-failure arms: warn, never crash the installer ──────────


def test_sigterm_registration_unavailable_warns_and_returns_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NotImplementedError on the FIRST registration → the
    `signal-handlers-unavailable` WARN and a clean return.

    On a platform without ``loop.add_signal_handler`` (Windows) the
    installer must degrade to signal-free operation, not crash the worker
    at boot: the warning names the platform (``os_name``) so the operator
    knows WHY Ctrl-C-style escalation is unavailable. Pinned against two
    regressions: the NotImplementedError escaping (the worker never
    boots), and the installer CONTINUING past the SIGTERM failure to
    register SIGHUP/SIGUSR2 handlers that could never deliver a shutdown
    ladder (the early ``return`` exists because the shutdown path is
    the handler that must exist for the others to be meaningful).
    """
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    deps = _worker_deps()
    backend = AsyncMock(spec=Backend)
    loop = Mock()
    loop.add_signal_handler = Mock(side_effect=NotImplementedError("win"))
    loop.create_task = Mock()
    holder: list[asyncio.Task[int]] = []
    esc_event = asyncio.Event()
    shut_event = asyncio.Event()

    with structlog.testing.capture_logs() as captured:
        install_signal_handlers(
            loop,
            deps,
            new_uuid(),
            shut_event,
            esc_event,
            backend,
            holder,
        )  # type: ignore[arg-type]

    warnings = [e for e in captured if e.get("event") == "signal-handlers-unavailable"]
    assert len(warnings) == 1, f"expected one signal-handlers-unavailable WARN, got {warnings!r}"
    assert warnings[0]["os_name"] == os.name
    assert loop.create_task.call_count == 0
    assert len(holder) == 0
    assert not esc_event.is_set() and not shut_event.is_set()


def test_per_signal_registration_failures_warn_without_losing_the_ladder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SIGHUP / SIGUSR2 registration failures degrade to their own WARN.

    These arms are AFTER the shutdown ladder is registered, so their
    failure must NOT take the early-return exit (that would discard the
    already-registered SIGTERM/SIGINT handlers) - only the optional
    handler's WARN fires and the rest of the registrations still land.
    Pinned against the two mutations of the try/except shape: swallowing
    the NotImplementedError without a WARN (the operator never learns
    the reload/dump seam is missing), and letting it escape (a missing
    debug utility kills the shutdown ladder's registration, which
    already succeeded).
    """
    import taskq.worker.shutdown as shutdown_mod

    mock_orch = AsyncMock(return_value=0)
    monkeypatch.setattr(shutdown_mod, "orchestrate_shutdown", mock_orch)

    def _install(*, fail_sig: signal.Signals):  # pyright: ignore[reportMissingTypeStubs]  # Why: the logs list is structlog's invariant list[EventDict]; the local helper's inferred types avoid re-annotating it.
        deps = _worker_deps()
        backend = AsyncMock(spec=Backend)
        loop, handlers = _mock_loop()
        captured: list[tuple[int, Callable[[], None]]] = handlers

        def _side_effect(sig: signal.Signals, cb: Callable[[], None]) -> None:
            if sig == fail_sig:
                raise NotImplementedError(str(sig))
            captured.append((sig, cb))

        loop.add_signal_handler = Mock(side_effect=_side_effect)  # type: ignore[method-assign]
        holder: list[asyncio.Task[int]] = []
        with structlog.testing.capture_logs() as logs:
            install_signal_handlers(
                loop,
                deps,
                new_uuid(),
                asyncio.Event(),
                asyncio.Event(),
                backend,
                holder,
            )  # type: ignore[arg-type]
        assert len(holder) == 0
        return loop, handlers, logs

    # SIGHUP refused: the WARN fires, SIGUSR2 is still registered, and
    # the SIGTERM ladder still works.
    loop, handlers, logs = _install(fail_sig=signal.SIGHUP)
    assert [e for e in logs if e.get("event") == "sighup-handler-unavailable"], (
        "a refused SIGHUP registration must WARN - the hot-reload seam is silently gone otherwise"
    )
    assert any(sig == signal.SIGUSR2 for sig, _ in handlers), (
        "the SIGHUP failure discarded the rest of the registrations"
    )
    sigterm_handler = handlers[0][1]
    sigterm_handler()
    assert loop.create_task.call_count == 1, (
        "the shutdown ladder must survive the optional handlers' registration failures"
    )

    # SIGUSR2 refused: same shape, the SIGHUP handler still lands.
    _, handlers2, logs2 = _install(fail_sig=signal.SIGUSR2)
    assert [e for e in logs2 if e.get("event") == "sigusr2-handler-unavailable"]
    assert any(sig == signal.SIGHUP for sig, _ in handlers2)
    assert not [e for e in logs2 if e.get("event") == "sighup-handler-unavailable"]
