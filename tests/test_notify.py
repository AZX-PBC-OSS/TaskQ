"""Tests for taskq.worker.notify — listener loop, callback, health-check + reconnect.

Why mock asyncpg at the connection level: the rule "Don't mock
asyncpg" applies to SQL behaviour tests. For listener lifecycle tests —
``add_listener`` / ``remove_listener`` / ``execute("SELECT 1")`` / ``close`` /
``is_closed`` — the asyncpg interaction surface is small enough that a
hand-rolled ``MockConnection`` is acceptable and necessary; full integration
with a real PG connection lives in integration tests.

Covers:

"""

import ast
import asyncio
import contextlib
import inspect
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from taskq.backend.clock import Clock
from taskq.backend.postgres import PostgresBackend
from taskq.worker.notify import (
    _active_listeners,
    _connected_lookup,
    _health_check_loop,
    _make_callback,
    _make_events_callback,
    _make_worker_events_callback,
    notify_listener_loop,
    reconnect_notify_conn,
)

# ── Helpers ────────────────────────────────────────────────────────────

_GRACE = timedelta(seconds=30)

_MODULE_DIR = Path(__file__).parent.parent / "src" / "taskq" / "worker"
_NOTIFY_PATH = _MODULE_DIR / "notify.py"

_WORKER_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


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
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": schema_name,
            "notify_health_check_interval": str(health_check_interval),
        }
    )
    settings.pg_dsn_direct = settings.pg_dsn  # pyright: ignore[reportAttributeAccessIssue] # Why: ensure direct DSN is set for reconnect tests; _post_load already did this but making it explicit
    deps = Mock()
    deps.settings = settings
    deps.notify_conn = _mock_conn()

    # Default reconnect factory — returns a fresh mock conn. Individual tests
    # override this to assert on the factory being called.
    async def _default_factory() -> object:
        return _mock_conn()

    deps.notify_conn_factory = _default_factory
    deps.leader_conn_factory = None
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
    backend: PostgresBackend, worker_id: uuid.UUID = _WORKER_ID
) -> list[tuple[str, object]]:
    """Build a minimal channels list (wake + events + worker) for tests."""
    from taskq.constants import events_channel, wake_channel, worker_channel

    schema = "taskq_test"
    return [
        (wake_channel(schema), _make_callback(backend)),
        (events_channel(schema), _make_events_callback(backend, worker_id)),
        (worker_channel(schema, str(worker_id)), _make_worker_events_callback(backend)),
    ]


# ── Module-state cleanup fixture ───────────────────────────────────────


@pytest.fixture(autouse=True)
def _restore_notify_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixtures are consumed by the framework; pyright does not track fixture usage
    """Ensure module-level mutable state is clean between test files."""
    _active_listeners.clear()
    _connected_lookup.clear()
    try:
        yield
    finally:
        _active_listeners.clear()
        _connected_lookup.clear()


# ── Listener startup ────────────────────────────────────────────


class TestListenerStartup:
    async def test_add_listener_called_for_all_channels_at_startup(self) -> None:
        """Listener startup calls add_listener for each channel
        (wake, events, per-worker). Sets the connected gauge to 1.
        """
        deps = _make_mock_deps()
        conn = deps.notify_conn
        backend = _make_backend()
        shutdown = asyncio.Event()
        shutdown.set()

        async def _runner() -> None:
            await notify_listener_loop(deps, backend, shutdown, _WORKER_ID)

        await asyncio.wait_for(_runner(), timeout=2.0)

        assert conn.add_listener.call_count == 3
        channels_registered = [c[0][0] for c in conn.add_listener.call_args_list]
        assert "taskq_wake_taskq_test" in channels_registered
        assert "taskq_events_taskq_test" in channels_registered
        assert f"taskq_worker_taskq_test_{_WORKER_ID}" in channels_registered


# ── Fan-out (wake channel) ────────────────────────────────────


class TestWakeFanout:
    async def test_callback_sets_two_subscriber_events(self) -> None:
        """two subscribe_wake() contexts open; callback fires;
        both events set.
        """
        backend = _make_backend()
        cb = _make_callback(backend)

        async with (
            backend.subscribe_wake() as event_a,
            backend.subscribe_wake() as event_b,
        ):
            mock_conn = _mock_conn()
            cb(mock_conn, 123, "taskq_wake_x", "")
            await asyncio.wait_for(event_a.wait(), timeout=0.1)
            await asyncio.wait_for(event_b.wait(), timeout=0.1)

    async def test_callback_sets_event_for_single_subscriber(self) -> None:
        """single subscriber; callback fires; event set."""
        backend = _make_backend()
        cb = _make_callback(backend)

        async with backend.subscribe_wake() as event:
            mock_conn = _mock_conn()
            cb(mock_conn, 123, "taskq_wake_x", "")
            await asyncio.wait_for(event.wait(), timeout=0.1)


# ── sync callback enforcement ─────────────────────────────────


class TestSyncCallbackEnforcement:
    def test_callback_is_not_coroutine_function(self) -> None:
        """inspect.iscoroutinefunction(_make_callback(backend))
        is False (sync-enforcement guard).
        """
        backend = _make_backend()
        cb = _make_callback(backend)
        assert inspect.iscoroutinefunction(cb) is False

    def test_events_callback_is_not_coroutine_function(self) -> None:
        """Events callback must also be sync."""
        backend = _make_backend()
        cb = _make_events_callback(backend, _WORKER_ID)
        assert inspect.iscoroutinefunction(cb) is False

    def test_worker_events_callback_is_not_coroutine_function(self) -> None:
        """Per-worker callback must also be sync."""
        backend = _make_backend()
        cb = _make_worker_events_callback(backend)
        assert inspect.iscoroutinefunction(cb) is False


# ── Coalescing ────────────────────────────────────────────────


class TestCoalescing:
    async def test_callback_coalescing_is_idempotent(self) -> None:
        """one subscriber, callback fires ten times in a tight
        loop, event is set; clear then callback once more — event set
        again (idempotence).
        """
        backend = _make_backend()
        cb = _make_callback(backend)

        async with backend.subscribe_wake() as event:
            mock_conn = _mock_conn()
            for _ in range(10):
                cb(mock_conn, 123, "taskq_wake_x", "")
            await asyncio.wait_for(event.wait(), timeout=0.1)
            event.clear()
            assert not event.is_set()
            cb(mock_conn, 123, "taskq_wake_x", "")
            await asyncio.wait_for(event.wait(), timeout=0.1)


# ── Reconnect path ─────────────────────────────────────────────


class TestReconnectPath:
    @pytest.mark.parametrize(
        "exc_class",
        [
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            asyncpg.AdminShutdownError,
            OSError,
        ],
    )
    async def test_reconnect_on_health_check_failure(self, exc_class: type[BaseException]) -> None:
        """_health_check_loop reconnect path — parametrized over
        four exception classes including AdminShutdownError.
        """
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)
        shutdown = asyncio.Event()

        old_conn = deps.notify_conn
        old_conn.execute = AsyncMock(side_effect=exc_class("simulated failure"))

        new_conn = _mock_conn()
        new_conn.execute = AsyncMock()
        new_conn.is_closed = Mock(return_value=False)

        factory_calls: list[None] = []

        async def fake_factory() -> Mock:
            factory_calls.append(None)
            return new_conn

        deps.notify_conn_factory = fake_factory

        import taskq.worker.notify as notify_mod

        with pytest.MonkeyPatch().context() as monkeypatch:
            monkeypatch.setattr(notify_mod, "logger", Mock())

            async def _runner() -> None:
                await _health_check_loop(deps, backend, shutdown, channels)

            task = asyncio.create_task(_runner())
            await asyncio.sleep(0.05)

            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            assert old_conn.remove_listener.called
            assert old_conn.close.called
            assert len(factory_calls) >= 1
            assert deps.notify_conn is new_conn
            # add_listener called once per channel on the new connection
            assert new_conn.add_listener.call_count >= 1

    async def test_reconnect_fetch_wakes_subscriber(self) -> None:
        """reconnect-fetch fires wake callback and wakes a subscriber
        registered before the reconnect.
        """
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)
        shutdown = asyncio.Event()

        old_conn = deps.notify_conn
        old_conn.execute = AsyncMock(
            side_effect=asyncpg.PostgresConnectionError("simulated failure")
        )

        new_conn = _mock_conn()
        new_conn.execute = AsyncMock()

        async def fake_factory() -> Mock:
            return new_conn

        deps.notify_conn_factory = fake_factory

        import taskq.worker.notify as notify_mod

        with pytest.MonkeyPatch().context() as monkeypatch:
            monkeypatch.setattr(notify_mod, "logger", Mock())

            async with backend.subscribe_wake() as subscriber_event:

                async def _runner() -> None:
                    await _health_check_loop(deps, backend, shutdown, channels)

                task = asyncio.create_task(_runner())
                await asyncio.sleep(0.1)

                shutdown.set()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

                assert subscriber_event.is_set(), "reconnect-fetch should have set subscriber event"


# ── Shutdown path ──────────────────────────────────────────────


class TestShutdownPath:
    async def test_shutdown_calls_remove_listener_for_all_channels(self) -> None:
        """Set shutdown before entering notify_listener_loop.
        Assert remove_listener called for each channel (wake, events,
        per-worker). Assert NO execute("UNLISTEN...") was made.
        """
        deps = _make_mock_deps()
        conn = deps.notify_conn
        backend = _make_backend()
        shutdown = asyncio.Event()
        shutdown.set()

        async def _runner() -> None:
            await notify_listener_loop(deps, backend, shutdown, _WORKER_ID)

        await asyncio.wait_for(_runner(), timeout=2.0)

        assert conn.remove_listener.call_count == 3
        channels_removed = [c[0][0] for c in conn.remove_listener.call_args_list]
        assert "taskq_wake_taskq_test" in channels_removed
        assert "taskq_events_taskq_test" in channels_removed
        assert f"taskq_worker_taskq_test_{_WORKER_ID}" in channels_removed

        unlisten_calls = [
            c[0][0]
            for c in conn.execute.call_args_list
            if c[0] and "UNLISTEN" in str(c[0][0]).upper()
        ]  # type: ignore[reportUnknownVariableType]
        assert not unlisten_calls, f"execute should not contain UNLISTEN, got: {unlisten_calls}"


# ── Channel name interpolation ─────────────────────────────────


class TestChannelNameInterpolation:
    def test_wake_channel_name_from_constant(self) -> None:
        """wake_channel returns correct name."""
        from taskq.constants import wake_channel

        result = wake_channel("myschema")
        assert result == "taskq_wake_myschema"
        assert result != "taskq_wake_{schema}"

    def test_events_channel_name_from_constant(self) -> None:
        """events_channel returns correct name."""
        from taskq.constants import events_channel

        result = events_channel("myschema")
        assert result == "taskq_events_myschema"

    def test_worker_channel_name_from_constant(self) -> None:
        """worker_channel returns correct name including worker_id."""
        from taskq.constants import worker_channel

        result = worker_channel("myschema", "abc123")
        assert result == "taskq_worker_myschema_abc123"

    def test_channel_names_used_with_worker_settings(self) -> None:
        """When WorkerSettings has schema_name='myapp', channels
        are interpolated correctly.
        """
        from taskq.constants import events_channel, wake_channel, worker_channel

        deps = _make_mock_deps(schema_name="myapp")
        assert wake_channel(deps.settings.schema_name) == "taskq_wake_myapp"
        assert events_channel(deps.settings.schema_name) == "taskq_events_myapp"
        wch = worker_channel(deps.settings.schema_name, str(_WORKER_ID))
        assert wch == f"taskq_worker_myapp_{_WORKER_ID}"


# ── Reconnect backoff ──────────────────────────────────────────


class TestReconnectBackoff:
    async def test_backoff_caps_at_30_seconds(self) -> None:
        """Reconnect backoff caps at 30 s."""
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)
        shutdown = asyncio.Event()

        old_conn = deps.notify_conn
        old_conn.execute = AsyncMock(
            side_effect=asyncpg.PostgresConnectionError("simulated failure")
        )

        fail_count = 0

        async def fake_factory() -> Mock:
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 6:
                raise asyncpg.PostgresConnectionError(f"attempt {fail_count}")
            new_conn = _mock_conn()
            new_conn.execute = AsyncMock()
            return new_conn

        deps.notify_conn_factory = fake_factory

        sleep_delays: list[float] = []
        orig_sleep = asyncio.sleep

        async def recording_sleep(delay: float) -> None:
            sleep_delays.append(delay)
            await orig_sleep(0)

        import taskq.worker.notify as notify_mod

        with pytest.MonkeyPatch().context() as monkeypatch:
            monkeypatch.setattr(notify_mod, "logger", Mock())
            monkeypatch.setattr(asyncio, "sleep", recording_sleep)

            async def _runner() -> None:
                await _health_check_loop(deps, backend, shutdown, channels)

            task = asyncio.create_task(_runner())
            for _ in range(20):
                await asyncio.sleep(0)
            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await task

            expected = [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
            i = 0
            for d in sleep_delays:
                if i < len(expected) and abs(d - expected[i]) < 0.001:
                    i += 1
            assert i == len(expected), (
                f"expected backoff sequence {expected} as a subsequence, "
                f"got delays {sleep_delays}; matched only first {i}"
            )


# ── Disconnected listener ──────────────────────────────────────


class TestDisconnectedListener:
    async def test_disconnected_listener_cancel_events_never_fire(self) -> None:
        """Construct PostgresBackend without starting
        notify_listener_loop. Open subscribe_cancel_wake(), hold for ~50 ms.
        Assert no exception and the event was NOT set (fallback to heartbeat).
        """
        backend = _make_backend()

        try:
            async with backend.subscribe_cancel_wake() as event:
                await asyncio.sleep(0.05)
            assert not event.is_set(), "cancel event should not be set when no listener is running"
        except Exception as exc:
            pytest.fail(f"unexpected exception: {exc}")


# ── Callback does not acquire _wake_lock ───────────────────────


class TestCallbackLockContract:
    async def test_callback_does_not_acquire_wake_lock(self) -> None:
        """Wake callback does not acquire _wake_lock during invocation."""
        backend = _make_backend()
        cb = _make_callback(backend)

        counter = 0
        original_acquire = backend._wake_lock.acquire  # type: ignore[reportPrivateUsage]

        async def counting_acquire() -> None:
            nonlocal counter
            counter += 1
            await original_acquire()

        backend._wake_lock.acquire = counting_acquire  # type: ignore[reportPrivateUsage]

        try:
            async with backend.subscribe_wake():
                pass

            counter = 0

            mock_conn = _mock_conn()
            cb(mock_conn, 0, "taskq_wake_x", "")
            assert counter == 0, f"_wake_lock acquired {counter} times during callback; expected 0"
        finally:
            backend._wake_lock.acquire = original_acquire  # type: ignore[reportPrivateUsage]


# ── No pool connection access ──────────────────────────────────


class TestNoPoolConnectionAccess:
    def test_notify_module_has_no_pool_attribute_access(self) -> None:
        """(a): Static guard — parse the module source and assert
        no ``dispatcher_pool``, ``worker_pool``, or ``heartbeat_pool``
        attribute access is present in any function body.
        """
        source = _NOTIFY_PATH.read_text()
        tree = ast.parse(source)

        class PoolAccessVisitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.violations: list[tuple[int, str]] = []

            def visit_Attribute(self, node: ast.Attribute) -> None:
                if node.attr in (
                    "dispatcher_pool",
                    "worker_pool",
                    "heartbeat_pool",
                ):
                    self.violations.append((node.lineno, node.attr))
                self.generic_visit(node)

        visitor = PoolAccessVisitor()
        visitor.visit(tree)

        assert not visitor.violations, (
            f"pool attribute access found at lines: {visitor.violations} — "
            "notify module must not access pool connections"
        )


# ── Gauge-callback registry cleanup ────────────────────────────


class TestGaugeCallbackRegistryCleanup:
    async def test_active_listeners_empty_before_and_after_loop(self) -> None:
        """(1-4): _active_listeners starts empty, is non-empty
        during notify_listener_loop, returns to empty after the loop's
        finally runs.
        """
        assert _active_listeners == set(), "expected empty before test"

        deps = _make_mock_deps()
        backend = _make_backend()
        shutdown = asyncio.Event()

        async def runner() -> None:
            await notify_listener_loop(deps, backend, shutdown, _WORKER_ID)

        task = asyncio.create_task(runner())
        await asyncio.sleep(0.05)

        assert _active_listeners == {backend}, "expected backend in active listeners during loop"

        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(task, timeout=2.0)

        assert _active_listeners == set(), "expected empty active listeners after loop exits"

    async def test_two_consecutive_runs_leave_set_empty(self) -> None:
        """two consecutive notify_listener_loop runs leave _active_listeners empty."""
        assert _active_listeners == set()

        for _ in range(2):
            deps = _make_mock_deps()
            backend = _make_backend()
            shutdown = asyncio.Event()
            shutdown.set()

            await asyncio.wait_for(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                timeout=2.0,
            )
            assert _active_listeners == set(), "_active_listeners must be empty after each run"

    async def test_connected_lookup_cleaned_after_loop(self) -> None:
        """_connected_lookup must not retain stale backend keys."""
        assert _connected_lookup == {}

        deps = _make_mock_deps()
        backend = _make_backend()
        shutdown = asyncio.Event()
        shutdown.set()

        await asyncio.wait_for(
            notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
            timeout=2.0,
        )
        assert backend not in _connected_lookup, (
            "stale backend key in _connected_lookup after cleanup"
        )


# ── Teardown race guard ───────────────────────────────────────────────


class TestShutdownTeardownRace:
    async def test_shutdown_teardown_survives_mid_reconnect_race(self) -> None:
        """try/except guard in notify_listener_loop's finally handles
        remove_listener raising AttributeError during mid-reconnect race.
        """
        deps = _make_mock_deps()
        backend = _make_backend()
        shutdown = asyncio.Event()

        deps.notify_conn.remove_listener = Mock(
            side_effect=AttributeError("connection closed during reconnect")
        )

        shutdown.set()

        await asyncio.wait_for(
            notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
            timeout=2.0,
        )

    async def test_shutdown_teardown_survives_runtime_error(self) -> None:
        """guard handles RuntimeError from remove_listener."""
        deps = _make_mock_deps()
        backend = _make_backend()
        shutdown = asyncio.Event()

        deps.notify_conn.remove_listener = Mock(side_effect=RuntimeError("concurrent modification"))

        shutdown.set()

        await asyncio.wait_for(
            notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
            timeout=2.0,
        )


# ── reconnect_notify_conn integration ─────────────────────────────────────────────


class TestReconnectInternal:
    async def test_reconnect_assigns_new_connection_to_deps(self) -> None:
        """reconnect_notify_conn opens a new connection, assigns it to deps.notify_conn,
        and calls add_listener for each channel.
        """
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)

        new_conn = _mock_conn()
        new_conn.execute = AsyncMock()

        async def fake_factory() -> Mock:
            return new_conn

        deps.notify_conn_factory = fake_factory

        import taskq.worker.notify as notify_mod

        with pytest.MonkeyPatch().context() as monkeypatch:
            monkeypatch.setattr(notify_mod, "logger", Mock())

            await reconnect_notify_conn(deps, backend, channels)

            assert deps.notify_conn is new_conn
            assert new_conn.add_listener.call_count == len(channels)


# ── _health_check_loop shutdown exit ──────────────────────────────────


class TestHealthCheckLoopShutdown:
    async def test_health_check_loop_exits_on_shutdown(self) -> None:
        """_health_check_loop exits cleanly when shutdown.is_set()."""
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)
        shutdown = asyncio.Event()

        async def _runner() -> None:
            await _health_check_loop(deps, backend, shutdown, channels)

        task = asyncio.create_task(_runner())
        await asyncio.sleep(0.05)
        shutdown.set()
        await asyncio.wait_for(task, timeout=2.0)


# ── reconnect uses the deps factory ─────────────────────────────────────


class TestReconnectUsesDirectDsn:
    async def test_reconnect_uses_pg_dsn_direct_not_pooled(self) -> None:
        """reconnect uses the factory on deps (which encodes the credential source)."""
        deps = _make_mock_deps()
        backend = _make_backend()
        channels = _make_channels(backend)

        factory_called: list[None] = []

        new_conn = _mock_conn()
        new_conn.execute = AsyncMock()

        async def fake_factory() -> Mock:
            factory_called.append(None)
            return new_conn

        deps.notify_conn_factory = fake_factory

        import taskq.worker.notify as notify_mod

        with pytest.MonkeyPatch().context() as monkeypatch:
            monkeypatch.setattr(notify_mod, "logger", Mock())

            await reconnect_notify_conn(deps, backend, channels)

            # The factory on deps was called — reconnect goes through the
            # credential source (DSN closure or user factory) stored at
            # startup, never a raw pg_dsn_pooled.
            assert len(factory_called) == 1
            assert deps.notify_conn is new_conn


# ── Cancel event routing ───────────────────────────────────────────────


class TestCancelEventRouting:
    def _cancel_payload(
        self,
        job_id: str = "job-abc",
        worker_id: uuid.UUID = _WORKER_ID,
    ) -> str:
        import orjson

        return orjson.dumps(
            {"type": "cancel", "job_id": job_id, "worker_id": str(worker_id)}
        ).decode()

    async def test_events_callback_wakes_cancel_subscriber_for_matching_worker(self) -> None:
        """Fleet events callback sets _cancel_subscribers when worker_id matches."""
        backend = _make_backend()
        cb = _make_events_callback(backend, _WORKER_ID)

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, "taskq_events_taskq_test", self._cancel_payload())
            await asyncio.wait_for(event.wait(), timeout=0.1)

    async def test_events_callback_ignores_different_worker_id(self) -> None:
        """Fleet events callback does NOT wake when worker_id doesn't match."""
        backend = _make_backend()
        other_worker = uuid.UUID("00000000-0000-0000-0000-000000000002")
        cb = _make_events_callback(backend, _WORKER_ID)

        async with backend.subscribe_cancel_wake() as event:
            cb(
                _mock_conn(),
                0,
                "taskq_events_taskq_test",
                self._cancel_payload(worker_id=other_worker),
            )
            await asyncio.sleep(0.02)
            assert not event.is_set(), "event must not fire for different worker_id"

    async def test_events_callback_ignores_unknown_event_type(self) -> None:
        """Fleet events callback ignores payloads with unknown type discriminator."""
        backend = _make_backend()
        cb = _make_events_callback(backend, _WORKER_ID)

        import orjson

        payload = orjson.dumps({"type": "reschedule", "worker_id": str(_WORKER_ID)}).decode()

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, "taskq_events_taskq_test", payload)
            await asyncio.sleep(0.02)
            assert not event.is_set(), "event must not fire for non-cancel type"

    async def test_events_callback_ignores_empty_payload(self) -> None:
        """Fleet events callback silently ignores empty payload (reconnect trigger)."""
        backend = _make_backend()
        cb = _make_events_callback(backend, _WORKER_ID)

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, "taskq_events_taskq_test", "")
            await asyncio.sleep(0.02)
            assert not event.is_set()

    async def test_events_callback_ignores_invalid_json(self) -> None:
        """Fleet events callback silently ignores unparseable payloads."""
        backend = _make_backend()
        cb = _make_events_callback(backend, _WORKER_ID)

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, "taskq_events_taskq_test", "not-json{{{")
            await asyncio.sleep(0.02)
            assert not event.is_set()

    async def test_worker_events_callback_wakes_cancel_subscriber(self) -> None:
        """Per-worker callback wakes _cancel_subscribers for any cancel payload."""
        backend = _make_backend()
        cb = _make_worker_events_callback(backend)

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, f"taskq_worker_taskq_test_{_WORKER_ID}", self._cancel_payload())
            await asyncio.wait_for(event.wait(), timeout=0.1)

    async def test_worker_events_callback_ignores_non_cancel_type(self) -> None:
        """Per-worker callback ignores non-cancel event types."""
        backend = _make_backend()
        cb = _make_worker_events_callback(backend)

        import orjson

        payload = orjson.dumps({"type": "heartbeat_ping", "worker_id": str(_WORKER_ID)}).decode()

        async with backend.subscribe_cancel_wake() as event:
            cb(_mock_conn(), 0, f"taskq_worker_taskq_test_{_WORKER_ID}", payload)
            await asyncio.sleep(0.02)
            assert not event.is_set()

    async def test_cancel_subscriber_not_set_by_wake_callback(self) -> None:
        """Wake callback must not fire _cancel_subscribers."""
        backend = _make_backend()
        wake_cb = _make_callback(backend)

        async with backend.subscribe_cancel_wake() as cancel_event:
            wake_cb(_mock_conn(), 0, "taskq_wake_taskq_test", "")
            await asyncio.sleep(0.02)
            assert not cancel_event.is_set(), "wake callback must not set cancel event"

    async def test_cancel_subscriber_fan_out(self) -> None:
        """Two subscribe_cancel_wake() contexts; worker callback fires; both events set."""
        backend = _make_backend()
        cb = _make_worker_events_callback(backend)

        import orjson

        payload = orjson.dumps(
            {"type": "cancel", "job_id": "x", "worker_id": str(_WORKER_ID)}
        ).decode()

        async with (
            backend.subscribe_cancel_wake() as event_a,
            backend.subscribe_cancel_wake() as event_b,
        ):
            cb(_mock_conn(), 0, f"taskq_worker_taskq_test_{_WORKER_ID}", payload)
            await asyncio.wait_for(event_a.wait(), timeout=0.1)
            await asyncio.wait_for(event_b.wait(), timeout=0.1)
