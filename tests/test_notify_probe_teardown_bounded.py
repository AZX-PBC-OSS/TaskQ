"""Contract: the notify health-check probe and the UNLISTEN teardown are
bounded.

Two seams in ``taskq/worker/notify.py`` await the notify connection with no
bound and no detector watching:

* the health-check ``SELECT 1`` - bounded on DSN/AAD conns by the
  connection's own command_timeout, but a hand-rolled factory or
  caller-owned conn carries none, and the health-check loop is deliberately
  exempt from the watchdog's stale-loop detector (see its docstring); an
  unbounded probe on a wedged conn parks the health check forever. The
  bound is ``settings.notify_listener_setup_timeout`` - the SAME bound the
  file already applies to every bounded execute/registration in this loop
  family - and exhaustion is treated like any dead conn: the reconnect
  path runs.
* the ``remove_listener`` round trips - in the health-check error path and
  in ``notify_listener_loop``'s teardown finally. Unbounded, a wedged conn
  parks the error path's reconnect (or the teardown, which the
  ShutdownWatchdog would force-exit the process for). Best-effort by
  design: bounded with ``notify_listener_setup_timeout`` inside the
  existing suppress - a timeout is another suppressed failure, not a
  crash.

Conventions mirror ``tests/test_notify.py`` (Mock conns at the asyncpg
listener-lifecycle surface; full integration lives in integration tests).
No ``pytestmark`` - must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.assertions import wait_for_condition
from taskq.worker.notify import (
    _active_listeners,
    _health_check_loop,
    notify_listener_loop,
)

# ── Test helpers ───────────────────────────────────────────────────────

# Production's documented bound for this loop family's executes, shrunk so
# a production-side bound fires well inside the test budget below.
_PROD_BOUND_SECS = 0.05
# Generous vs the production bound: this budget firing is the red result.
_TEST_BUDGET_SECS = 5.0

_WORKER_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")


def _mock_conn() -> Mock:
    m = Mock()
    m.add_listener = AsyncMock()
    m.remove_listener = Mock(side_effect=lambda channel, cb: asyncio.sleep(0))
    m.execute = Mock(side_effect=lambda sql, *args: asyncio.sleep(0))
    m.close = AsyncMock()
    m.is_closed = Mock(return_value=False)
    return m


def _make_deps(**overrides: Any) -> Mock:
    from taskq.settings import WorkerSettings

    settings_dict: dict[str, str] = {
        "pg_dsn": "postgresql://localhost:5432/taskq",
        "pg_dsn_direct": "postgresql://localhost:5432/taskq",
        "schema_name": "taskq_test",
        "notify_health_check_interval": "0.001",
        "notify_listener_setup_timeout": str(_PROD_BOUND_SECS),
        "reload_factory_timeout": "5.0",
    }
    settings_dict.update(overrides)
    settings = WorkerSettings.load_from_dict(settings_dict)
    deps = Mock()
    deps.settings = settings
    deps.notify_conn = _mock_conn()

    async def _default_factory() -> Mock:
        return _mock_conn()

    deps.notify_conn_factory = _default_factory
    deps.leader_conn_factory = None
    deps.notify_reconnect_lock = asyncio.Lock()
    deps.notify_reconnect_fn = None
    deps.owns_notify_conn = True
    return deps


def _make_backend() -> PostgresBackend:
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    mock_clock = Mock(spec=Clock)
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    return PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


def _make_channels(backend: PostgresBackend) -> list[tuple[str, Any]]:
    from taskq.constants import events_channel, wake_channel, worker_channel
    from taskq.worker.notify import (
        _make_callback,
        _make_events_callback,
        _make_worker_events_callback,
    )

    schema = "taskq_test"
    return [
        (wake_channel(schema), _make_callback(backend)),
        (events_channel(schema), _make_events_callback(backend, _WORKER_ID)),
        (worker_channel(schema, str(_WORKER_ID)), _make_worker_events_callback(backend)),
    ]


# The module-global listener bookkeeping (_active_listeners /
# _connected_lookup) is reset around every test by the conftest-level
# _reset_notify_module_globals autouse fixture - this file's former
# file-local copy of that reset was promoted there (with the four other
# identical copies across the notify suites) so every test gets it.


# ── The health-check probe must be bounded ─────────────────────────────


async def test_health_check_probe_hang_is_treated_as_disconnected() -> None:
    """A notify conn whose SELECT 1 never returns is bounded by
    notify_listener_setup_timeout: the timeout flows into the same
    disconnected-conn handling as any dead connection - error logged, the
    reconnect path runs and swaps the conn - and the loop stays responsive
    to shutdown instead of parking forever on the probe."""
    deps = _make_deps()
    backend = _make_backend()
    channels = _make_channels(backend)
    shutdown = asyncio.Event()

    old_conn = deps.notify_conn
    probe_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the conn black-holes on the probe

    async def hanging_probe(sql: str, *args: object) -> object:
        if sql == "SELECT 1":
            probe_entered.set()
            await never.wait()
            raise AssertionError("unreachable: the hang gate is never set")
        return None

    old_conn.execute = hanging_probe

    new_conn = _mock_conn()

    async def factory() -> Mock:
        return new_conn

    deps.notify_conn_factory = factory

    import taskq.worker.notify as notify_mod

    with pytest.MonkeyPatch().context() as monkeypatch:
        logger_mock = Mock()
        monkeypatch.setattr(notify_mod, "logger", logger_mock)

        task = asyncio.create_task(_health_check_loop(deps, backend, shutdown, channels))
        try:
            await asyncio.wait_for(probe_entered.wait(), timeout=_TEST_BUDGET_SECS)

            # RED pre-fix: the loop is parked inside execute("SELECT 1") -
            # the reconnect path never runs and this bounded wait fails by
            # name.
            await wait_for_condition(
                lambda: deps.notify_conn is new_conn,
                description="probe timeout treated as disconnected: conn swapped via reconnect",
                timeout=_TEST_BUDGET_SECS,
            )

            error_logs = [
                c for c in logger_mock.warning.call_args_list if c.args[0] == "notify-conn-error"
            ]
            assert error_logs, "the probe timeout must be logged as a conn error"
            assert any("TimeoutError" in str(c.kwargs.get("error", "")) for c in error_logs), (
                "the logged conn error must carry the probe's TimeoutError"
            )

            # The loop must stay responsive: shutdown set between checks
            # ends it on the next while-check - a loop still parked in the
            # probe would hang here.
            shutdown.set()
            await asyncio.wait_for(task, timeout=_TEST_BUDGET_SECS)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def test_health_check_error_path_unlisten_hang_is_bounded() -> None:
    """When the probe FAILS (not hangs) but the conn's remove_listener
    never returns, the UNLISTEN round trip is bounded inside the existing
    suppress: the error path still reaches the reconnect loop, swaps the
    conn, and stays responsive to shutdown instead of parking forever on
    the dead conn's UNLISTEN."""
    deps = _make_deps()
    backend = _make_backend()
    channels = _make_channels(backend)
    shutdown = asyncio.Event()

    old_conn = deps.notify_conn
    old_conn.execute = AsyncMock(side_effect=asyncpg.PostgresConnectionError("simulated failure"))
    remove_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the dead conn black-holes on UNLISTEN

    async def hanging_remove(channel: str, cb: object) -> None:
        remove_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    old_conn.remove_listener = hanging_remove

    new_conn = _mock_conn()

    async def factory() -> Mock:
        return new_conn

    deps.notify_conn_factory = factory

    task = asyncio.create_task(_health_check_loop(deps, backend, shutdown, channels))
    try:
        await asyncio.wait_for(remove_entered.wait(), timeout=_TEST_BUDGET_SECS)

        # RED pre-fix: the error path is parked inside remove_listener() -
        # the reconnect path never runs and this bounded wait fails by name.
        await wait_for_condition(
            lambda: deps.notify_conn is new_conn,
            description="bounded UNLISTEN lets the error path reach reconnect and swap",
            timeout=_TEST_BUDGET_SECS,
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── The listener teardown UNLISTEN must be bounded ─────────────────────


async def test_listener_teardown_unlisten_hang_is_bounded() -> None:
    """notify_listener_loop's teardown remove_listener calls are bounded:
    with a conn whose UNLISTEN never returns and shutdown already set, the
    loop's finally completes within the bound instead of parking forever -
    the ShutdownWatchdog would otherwise force-exit the process for it."""
    deps = _make_deps()
    backend = _make_backend()
    shutdown = asyncio.Event()
    shutdown.set()  # teardown path: the listener never enters its TaskGroup wait

    conn = deps.notify_conn
    remove_calls: list[str] = []
    never = asyncio.Event()  # never set: the conn black-holes on UNLISTEN

    async def hanging_remove(channel: str, cb: object) -> None:
        remove_calls.append(channel)
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    conn.remove_listener = hanging_remove

    try:
        # RED pre-fix: the loop parks forever inside the FIRST
        # remove_listener - this bounded wait raises TimeoutError.
        await asyncio.wait_for(
            notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
            timeout=_TEST_BUDGET_SECS,
        )
    except TimeoutError:
        pytest.fail(
            "notify_listener_loop parks forever in its teardown "
            f"remove_listener: still suspended {_TEST_BUDGET_SECS:.0f}s in "
            f"with notify_listener_setup_timeout={_PROD_BOUND_SECS}s "
            "configured. The teardown UNLISTEN round trip is unbounded, so a "
            "wedged notify conn stalls the listener's shutdown and the "
            "ShutdownWatchdog force-exits the process for it."
        )
    assert len(remove_calls) == 4, (
        f"teardown must attempt UNLISTEN for every channel; got {remove_calls!r}"
    )
    assert not _active_listeners, "the teardown must complete its bookkeeping"
