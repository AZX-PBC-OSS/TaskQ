"""RED-team: NOTIFY listener lifecycle under spontaneous connection loss.

The reload-triggered rebuild (test_reload_credentials.py), the
reconnect-re-fires-wake invariant, the backoff jitter sequence, and the
reconnect exception-class matrix are pinned in tests/test_notify.py. This
file pins the unpinned spontaneous-loss observables: the degraded
``taskq.notify.connected`` gauge (the distinguishable degraded outcome the
contract requires), flap recovery (a SECOND loss after a completed
rebuild re-enters the reconnect machinery cleanly), and the missed-wake
bound constant that carries the producer's poll fallback through a
rebuild.
"""

import asyncio
import contextlib
from collections.abc import Iterator
from datetime import timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest

from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.worker.notify import (
    _active_listeners,
    _connected_lookup,
    _health_check_loop,
    _make_callback,
    _make_events_callback,
    _make_worker_events_callback,
)

_GRACE = timedelta(seconds=30)
_WORKER_ID = UUID("00000000-0000-0000-0000-000000000002")


def _mock_conn() -> Mock:
    m = Mock()
    m.add_listener = AsyncMock()
    m.remove_listener = Mock(side_effect=lambda channel, cb: asyncio.sleep(0))
    m.execute = Mock(side_effect=lambda sql, *args: asyncio.sleep(0))
    m.close = AsyncMock()
    m.is_closed = Mock(return_value=False)
    return m


def _make_mock_deps(
    schema_name: str = "taskq_test",
    health_check_interval: float = 0.001,
) -> Mock:
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": schema_name,
            "notify_health_check_interval": str(health_check_interval),
        }
    )
    deps = Mock()
    deps.settings = settings
    deps.notify_conn = _mock_conn()

    async def _default_factory() -> object:
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
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


def _make_channels(
    backend: PostgresBackend, worker_id: UUID = _WORKER_ID
) -> list[tuple[str, object]]:
    from taskq.constants import events_channel, wake_channel, worker_channel

    schema = "taskq_test"
    return [
        (wake_channel(schema), _make_callback(backend)),
        (events_channel(schema), _make_events_callback(backend, worker_id)),
        (worker_channel(schema, str(worker_id)), _make_worker_events_callback(backend)),
    ]


@pytest.fixture(autouse=True)
def _restore_notify_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixtures are consumed by the framework; pyright does not track fixture usage
    _active_listeners.clear()
    _connected_lookup.clear()
    try:
        yield
    finally:
        _active_listeners.clear()
        _connected_lookup.clear()


async def _run_health_loop(
    deps: Mock,
    backend: PostgresBackend,
    shutdown: asyncio.Event,
    channels: list[tuple[str, object]],
) -> asyncio.Task[None]:
    async def _runner() -> None:
        await _health_check_loop(deps, backend, shutdown, channels)

    return asyncio.create_task(_runner())


async def test_spontaneous_loss_flips_connected_gauge_then_recovers() -> None:
    """A spontaneous notify-conn loss must produce a DISTINGUISHABLE
    degraded outcome: ``taskq.notify.connected`` (via
    ``_connected_lookup``) flips to 0 while the listener is down and back
    to 1 after the rebuild — the operator-visible signal that wakeups are
    degraded to the poll fallback, never a silent loss.

    Verdict asserted: DEGRADE-AND-REPORT (gauge observable).
    """
    deps = _make_mock_deps()
    backend = _make_backend()
    channels = _make_channels(backend)
    shutdown = asyncio.Event()

    old_conn = deps.notify_conn
    old_conn.execute = AsyncMock(side_effect=asyncpg.PostgresConnectionError("pg died"))

    factory_gate = asyncio.Event()
    new_conn = _mock_conn()

    async def gated_factory() -> object:
        await factory_gate.wait()
        return new_conn

    deps.notify_conn_factory = gated_factory
    _connected_lookup[backend] = True

    task = await _run_health_loop(deps, backend, shutdown, channels)
    try:
        await wait_for_condition(
            lambda: _connected_lookup.get(backend) is False,
            timeout=2.0,
            description="connected gauge never flipped to 0 during the outage",
        )
        assert old_conn.remove_listener.called, (
            "the dead conn's listeners must be dropped before the rebuild so the "
            "old connection's callbacks can never fire into the new regime"
        )
        factory_gate.set()
        await wait_for_condition(
            lambda: _connected_lookup.get(backend) is True,
            timeout=2.0,
            description="connected gauge never flipped back to 1 after the rebuild",
        )
        assert deps.notify_conn is new_conn
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_notify_flap_second_loss_reenters_reconnect_cleanly() -> None:
    """Flapping PG: after one completed rebuild, a SECOND spontaneous loss
    must re-enter the reconnect machinery cleanly — the health loop keeps
    running (the worker never crashes), a second rebuild lands on the
    newest connection, and each recovery re-fires the wake so a subscriber
    registered across the flap is unblocked both times.

    Verdict asserted: DEGRADE-AND-RECOVER (bounded flap, no crash).
    """
    deps = _make_mock_deps()
    backend = _make_backend()
    channels = _make_channels(backend)
    shutdown = asyncio.Event()

    conn1 = deps.notify_conn
    conn1.execute = AsyncMock(side_effect=asyncpg.PostgresConnectionError("flap 1"))
    conn2 = _mock_conn()
    conn3 = _mock_conn()

    factory_yields: list[object] = [conn2, conn3]

    async def flap_factory() -> object:
        return factory_yields.pop(0)

    deps.notify_conn_factory = flap_factory

    async with backend.subscribe_wake() as subscriber_event:
        task = await _run_health_loop(deps, backend, shutdown, channels)
        try:
            await wait_for_condition(
                lambda: deps.notify_conn is conn2,
                timeout=2.0,
                description="first rebuild never completed",
            )
            assert subscriber_event.is_set(), (
                "the rebuild must re-fire the wake so a subscriber blocked across "
                "the outage window is unblocked (missed-wake catch-up)"
            )
            subscriber_event.clear()

            conn2.execute = AsyncMock(side_effect=asyncpg.PostgresConnectionError("flap 2"))
            await wait_for_condition(
                lambda: deps.notify_conn is conn3,
                timeout=2.0,
                description="second loss never rebuilt onto the newest connection",
            )
            assert _connected_lookup.get(backend) is True, (
                "after the second recovery the gauge must read healthy again"
            )
            assert subscriber_event.is_set(), (
                "the SECOND rebuild must also re-fire the wake — flap recovery "
                "must not skip the missed-wake catch-up"
            )
        finally:
            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def test_notify_poll_interval_default_bounds_missed_wake_at_5s() -> None:
    """The missed-wake bound through a listener rebuild: the producer's
    poll fallback (run.py races the wake wait against a jittered
    ``notify_poll_interval`` sleep) is what bounds wakeups lost while the
    listener is mid-rebuild — its default must stay at 5s so a rebuild (or
    an outage longer than the reconnect backoff) can never strand an idle
    producer beyond that bound.

    Verdict asserted: BOUNDED MISSED-WAKE (constant carrying the bound).
    """
    s = WorkerSettings.load_from_dict({"pg_dsn": "postgresql://u:p@h/d"})
    assert s.notify_poll_interval == 5.0, (
        "DEPENDENCY-FAILURE contract (bounded missed-wake): notify_poll_interval "
        "default 5.0 is the bound that carries the producer through a NOTIFY "
        "listener rebuild — any wake lost mid-rebuild is recovered by the poll "
        "fallback within this cadence. Raising it silently stretches the "
        "degraded-dispatch latency bound. Verdict: SAFE (pin)."
    )
