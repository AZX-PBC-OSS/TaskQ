"""Chaos and property tests for the NOTIFY listener.

Covers:

require a real Postgres container and are marked
``@pytest.mark.integration`` individually. mocks asyncpg and is a unit
test. is a Hypothesis property test on the in-memory subscriber registry
— pure unit, no PG.

The chaos pattern (research §"pg_terminate_backend / chaos pattern")::

    pid = deps.notify_conn.get_server_pid()
    async with raw_pool.acquire() as raw_conn:
        await raw_conn.fetchval("SELECT pg_terminate_backend($1)", pid)
"""

import asyncio
from collections.abc import Iterator
from datetime import timedelta
from enum import Enum, auto

import asyncpg
import pytest
from hypothesis import given
from hypothesis import strategies as st
from testcontainers.postgres import PostgresContainer

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import wake_channel
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import _create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.notify import _active_listeners as _active_notify_listeners
from taskq.worker.notify import (
    _connected_lookup,
    _make_callback,
    _notify_received_counter,
    _notify_reconnects_counter,
    notify_listener_loop,
)

_GRACE = timedelta(seconds=30)
_WORKER_ID = new_uuid()


class _Op(Enum):
    ENTER = auto()
    EXIT = auto()
    NOTIFY = auto()


# ── Module-state cleanup fixture ───────────────────────────────────────


@pytest.fixture(autouse=True)
def _restore_notify_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixtures are consumed by the framework; pyright does not track fixture usage
    _active_notify_listeners.clear()
    _connected_lookup.clear()
    try:
        yield
    finally:
        _active_notify_listeners.clear()
        _connected_lookup.clear()


# ── Helpers ────────────────────────────────────────────────────────────


async def _raw_pg_conn(pg_dsn: str) -> asyncpg.Connection:
    return await asyncpg.connect(pg_dsn)


async def _pg_notify(conn: asyncpg.Connection, channel: str, payload: str = "") -> None:
    await conn.execute("SELECT pg_notify($1, $2)", channel, payload)


async def _pg_terminate_backend(pg_dsn: str, pid: int) -> None:
    conn = await _raw_pg_conn(pg_dsn)
    try:
        await conn.fetchval("SELECT pg_terminate_backend($1)", pid)
    finally:
        await conn.close()


# ── Function-scoped PG container fixture for ────────────────────


@pytest.fixture(scope="function")
def pg_container_function_scoped() -> Iterator[PostgresContainer]:
    with PostgresContainer(
        image="postgres:18-alpine",
        username="taskq",
        password="taskq",
        dbname="taskq",
    ) as container:
        yield container


@pytest.fixture(scope="function")
def pg_dsn_function_scoped(
    pg_container_function_scoped: PostgresContainer,
) -> str:
    return pg_container_function_scoped.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )


# ── Kill listener connection via pg_terminate_backend ───────────


@pytest.mark.integration
@pytest.mark.xdist_group(name="chaos")
async def test_tc1_kill_listener_via_terminate_backend(pg_dsn: str) -> None:
    """Kill listener connection via pg_terminate_backend.
    Spawn notify_listener_loop, kill its connection from another session,
    assert the listener reconnects within 10 s (via the reconnects counter)
    and a subscriber opened after reconnect receives a subsequent NOTIFY.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        reconnect_counter_adds: list[int] = []
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                _notify_reconnects_counter,
                "add",
                lambda n: reconnect_counter_adds.append(n),
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                    name="notify.chaos-tc1",
                )

                notify_conn = deps.notify_conn
                assert notify_conn is not None
                pid = notify_conn.get_server_pid()
                assert pid > 0

                await asyncio.sleep(0.1)

                await _pg_terminate_backend(pg_dsn, pid)

                deadline = asyncio.get_running_loop().time() + 5.0
                reconnect_ok = False
                while asyncio.get_running_loop().time() < deadline and not reconnect_ok:
                    await asyncio.sleep(0.1)
                    reconnect_ok = len(reconnect_counter_adds) >= 1
                assert reconnect_ok, "listener did not reconnect within 5 s"

                async with backend.subscribe_wake() as post_reconnect_event:
                    third_conn = await _raw_pg_conn(pg_dsn)
                    try:
                        await _pg_notify(third_conn, channel)
                    finally:
                        await third_conn.close()

                    await asyncio.wait_for(post_reconnect_event.wait(), timeout=2.0)
                    assert post_reconnect_event.is_set()

                shutdown.set()

        assert sum(reconnect_counter_adds) >= 1, "taskq.notify.reconnects counter should be >= 1"
        assert len(reconnect_counter_adds) == 1, (
            f"expected exactly 1 reconnect, got {reconnect_counter_adds}"
        )


# ── PG container stop/start ────────────────────────────────────


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.xdist_group(name="chaos")
async def test_tc2_pg_container_stop_start(
    pg_container_function_scoped: PostgresContainer,
) -> None:
    """PG container stop/start. Uses a function-scoped container to
    avoid contaminating the session-scoped one. Stops the container, waits
    3 s, restarts it, asserts the listener reconnects within 30 s and a
    subsequent NOTIFY is delivered.

    Marked ``@pytest.mark.slow`` — opt-in for routine CI.
    """
    pg_dsn = pg_container_function_scoped.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    ws = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL": "1",
        }
    )
    channel = wake_channel(ws.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(ws) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        reconnect_counter_adds: list[int] = []
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                _notify_reconnects_counter,
                "add",
                lambda n: reconnect_counter_adds.append(n),
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                    name="notify.chaos-tc2",
                )

                await asyncio.sleep(0.2)

                wrapped = pg_container_function_scoped.get_wrapped_container()
                wrapped.stop()
                await asyncio.sleep(2.0)

                for _attempt in range(3):
                    try:
                        wrapped.start()
                        break
                    except Exception:
                        if _attempt == 2:
                            raise
                        await asyncio.sleep(2)
                # Docker Desktop on macOS may assign a new port after restart;
                # re-derive the DSN and update the reconnect factory so
                # reconnect_notify_conn uses the current port. The factory is
                # a closure that captured the old DSN at startup — it must be
                # replaced with a new closure pointing at the new port.
                pg_dsn = pg_container_function_scoped.get_connection_url().replace(
                    "postgresql+psycopg2://", "postgresql://"
                )
                from taskq.worker.deps import open_dedicated_conn

                async def _new_notify_factory() -> asyncpg.Connection:
                    return await open_dedicated_conn(pg_dsn, label="notify", apply_keepalive=True)

                deps.notify_conn_factory = _new_notify_factory
                await asyncio.sleep(0.5)

                deadline = asyncio.get_running_loop().time() + 30.0
                reconnect_ok = False
                while asyncio.get_running_loop().time() < deadline and not reconnect_ok:
                    await asyncio.sleep(0.5)
                    reconnect_ok = len(reconnect_counter_adds) >= 1
                assert reconnect_ok, (
                    "listener did not reconnect within 30 s after container restart"
                )

                async with backend.subscribe_wake() as post_event:
                    verify_conn = await _raw_pg_conn(pg_dsn)
                    try:
                        await _pg_notify(verify_conn, channel)
                    finally:
                        await verify_conn.close()

                    await asyncio.wait_for(post_event.wait(), timeout=5.0)
                    assert post_event.is_set()

                shutdown.set()


# ── Shutdown arrives mid-reconnect ──────────────────────────────


async def test_tc3_shutdown_mid_reconnect() -> None:
    """Shutdown arrives mid-reconnect. Mocks open_dedicated_conn
    to block on an asyncio.Event so the reconnect path suspends inside
    "open new connection". While suspended, calls shutdown.set(). Asserts
    notify_listener_loop exits cleanly — the try/except guard catches
    the result.

    No @pytest.mark.integration — this is a unit test using mocked asyncpg.
    """
    from unittest.mock import AsyncMock, Mock

    deps = Mock()
    deps.notify_reconnect_lock = asyncio.Lock()  # Why: reconnect_notify_conn serializes on a real lock; a bare Mock would fail the async-CM protocol.
    deps.settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": "taskq_test",
            "notify_health_check_interval": "0.001",
        }
    )
    deps.settings.pg_dsn_direct = deps.settings.pg_dsn  # pyright: ignore[reportAttributeAccessIssue] # Why: ensure direct DSN is set for _reconnect tests

    mock_conn = Mock()
    mock_conn.add_listener = AsyncMock()
    mock_conn.remove_listener = Mock(
        side_effect=asyncpg.InterfaceError("connection closed during mid-reconnect shutdown")
    )
    mock_conn.close = AsyncMock()
    mock_conn.is_closed = Mock(return_value=False)
    deps.notify_conn = mock_conn

    execute_count = 0

    async def execute_side_effect(sql: str, *args: object) -> None:
        nonlocal execute_count
        execute_count += 1
        if execute_count == 1:
            raise asyncpg.InterfaceError("simulated connection loss")

    mock_conn.execute = execute_side_effect

    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    mock_clock = Mock()
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    backend = PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    open_conn_gate = asyncio.Event()
    open_conn_called = asyncio.Event()

    async def blocking_factory() -> Mock:
        open_conn_called.set()
        await open_conn_gate.wait()
        raise asyncpg.InterfaceError("cancelled by shutdown mid-open — teardown must survive")

    deps.notify_conn_factory = blocking_factory

    import taskq.worker.notify as notify_mod

    with pytest.MonkeyPatch().context() as monkeypatch:
        monkeypatch.setattr(notify_mod, "logger", Mock())

        shutdown = asyncio.Event()

        task = asyncio.create_task(
            notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
            name="notify-tc3",
        )

        await asyncio.wait_for(open_conn_called.wait(), timeout=2.0)

        shutdown.set()
        open_conn_gate.set()

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except TimeoutError:
            pytest.fail("notify_listener_loop did not exit after shutdown during mid-reconnect")

    if task.done() and task.exception() is not None:
        pytest.fail(f"unexpected exception from notify_listener_loop: {task.exception()}")


# ── NOTIFY storm ───────────────────────────────────────────────


@pytest.mark.integration
@pytest.mark.xdist_group(name="chaos")
async def test_tc4_notify_storm_coalescing(pg_dsn: str) -> None:
    """NOTIFY storm — coalescing under 1000 wakes/s, no starvation.
    Fires pg_notify 1000 times in a tight loop. Asserts the subscriber
    event is set, then verifies the listener is still responsive by
    clearing the event, firing one more NOTIFY, and asserting it is set
    within 200 ms.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        received_adds: list[int] = []
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                _notify_received_counter,
                "add",
                lambda n: received_adds.append(n),
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                    name="notify.chaos-tc4",
                )

                async with backend.subscribe_wake() as event:
                    second_conn = await _raw_pg_conn(pg_dsn)
                    try:
                        for _ in range(1000):
                            await _pg_notify(second_conn, channel)
                    finally:
                        await second_conn.close()

                    await asyncio.wait_for(event.wait(), timeout=5.0)
                    assert event.is_set(), "event should be set after NOTIFY storm"

                    storm_count = len(received_adds)
                    assert 1 <= storm_count <= 1000, (
                        f"storm callbacks expected 1..1000, got {storm_count}"
                    )

                    event.clear()
                    assert not event.is_set()

                    third_conn = await _raw_pg_conn(pg_dsn)
                    try:
                        await _pg_notify(third_conn, channel)
                    finally:
                        await third_conn.close()

                    await asyncio.wait_for(event.wait(), timeout=0.2)
                    assert event.is_set(), "listener not responsive after storm"

                    assert len(received_adds) >= storm_count + 1, (
                        "verification NOTIFY should produce at least one additional callback"
                    )

                shutdown.set()


# ── Reconnect after pg_terminate_backend delivers missed jobs ──


@pytest.mark.integration
@pytest.mark.xdist_group(name="chaos")
async def test_tc5_reconnect_delivers_missed_jobs(pg_dsn: str) -> None:
    """Reconnect after pg_terminate_backend delivers missed jobs.
    Kills the listener connection, then immediately enqueues two jobs.
    After reconnect, the synthetic callback fires, waking a
    sentinel subscriber registered before the kill. Asserts the sentinel
    event was set after the reconnect window.
    """
    from taskq.testing.fixtures import _open_pg_backend

    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=f"tc5_{new_base62()}".lower())
    # Shorten health check interval to speed up reconnect detection.
    deps.settings.notify_health_check_interval = 1.0
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    raw_conn = await _raw_pg_conn(pg_dsn)
    try:
        await _create_worker(raw_conn, schema, worker_id)
    finally:
        await raw_conn.close()

    shutdown = asyncio.Event()

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.chaos-tc5",
            )

            async with backend.subscribe_wake() as sentinel:
                notify_conn = deps.notify_conn
                assert notify_conn is not None
                pid = notify_conn.get_server_pid()
                assert pid > 0

                await _pg_terminate_backend(pg_dsn, pid)
                await asyncio.sleep(0.1)

                jobs_conn = await _raw_pg_conn(pg_dsn)
                try:
                    await jobs_conn.execute(f"SET search_path TO {schema}")
                    for _ in range(2):
                        await jobs_conn.execute(
                            f'INSERT INTO "{schema}".jobs'  # noqa: S608 # Why: schema name validated by WorkerSettings.post_load; asyncpg has no parameter binding for identifiers
                            " (id, actor, queue, payload, max_attempts, retry_kind)"
                            " VALUES ($1, 'test.actor', 'default', '{}', 3, 'transient')",
                            new_uuid(),
                        )
                finally:
                    await jobs_conn.close()

                try:
                    await asyncio.wait_for(sentinel.wait(), timeout=5.0)
                except TimeoutError:
                    connected = _connected_lookup.get(backend, False)
                    pytest.fail(f"sentinel event was not set within 5 s; connected={connected}")

            shutdown.set()
    finally:
        await stack.aclose()


# ── Property test — concurrent enter/exit/notify on the registry ─


def _op_strategy() -> st.SearchStrategy[list[_Op]]:
    return st.lists(
        st.sampled_from([_Op.ENTER, _Op.EXIT, _Op.NOTIFY]),
        min_size=1,
        max_size=50,
    )


@given(ops=_op_strategy())
def test_tp1_property_concurrent_enter_exit_notify(ops: list[_Op]) -> None:
    """Property test — concurrent enter/exit/notify on the registry.
    Generates sequences of enter/exit/notify operations. A model list
    tracks currently-open events. After each notify, every event still in
    the model's open-set must have ``is_set() == True`` in the SUT.
    Events that have been exited are not in ``_wake_subscribers``.
    """
    from unittest.mock import Mock

    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    mock_deps.dispatcher_pool = Mock()
    mock_clock = Mock()
    mock_clock.now.return_value = NotImplemented
    mock_clock.monotonic.return_value = 0.0
    backend = PostgresBackend(
        deps=mock_deps,
        clock=mock_clock,
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )

    cb = _make_callback(backend)

    open_set: list[tuple[int, asyncio.Event]] = []
    event_id = 0

    async def _run_sequence() -> None:
        nonlocal event_id

        for op in ops:
            if op is _Op.ENTER:
                event = asyncio.Event()
                async with backend._wake_lock:  # pyright: ignore[reportPrivateUsage] # Why: test-only — property test simulates subscribe_wake without the full async context manager to track model state
                    backend._wake_subscribers.add(event)
                open_set.append((event_id, event))
                event_id += 1
            elif op is _Op.EXIT:
                if open_set:
                    _, event = open_set.pop(0)
                    async with backend._wake_lock:  # pyright: ignore[reportPrivateUsage] # Why: test-only — property test
                        backend._wake_subscribers.discard(event)
            elif op is _Op.NOTIFY:
                cb(Mock(), 0, "taskq_wake_taskq_test", "")
                for _, e in list(open_set):
                    assert e.is_set(), "notify should set every open event"

        if open_set:
            async with backend._wake_lock:  # pyright: ignore[reportPrivateUsage] # Why: test-only cleanup after property sequence
                for _, event in open_set:
                    backend._wake_subscribers.discard(event)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run_sequence())
    finally:
        loop.close()
