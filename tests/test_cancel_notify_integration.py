"""Integration tests for the cancel NOTIFY path (pg_notify on running-job cancel).

Tests the full round-trip from write_cancel_request → pg_notify →
subscribe_cancel_wake wake-up, plus unit coverage for the heartbeat_loop
cancel_wake_event interrupt, all against real PG or FakePool as appropriate.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import events_channel, worker_channel
from taskq.testing.pg import create_pending_job, create_running_job, create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.notify import notify_listener_loop

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

_GRACE = timedelta(seconds=30)

# The NOTIFY-firing tests below require real PG and are marked ``integration``.
# The heartbeat_loop interrupt test is a pure unit test (no PG) and carries no
# integration mark.
_integration = pytest.mark.integration


# ── Setup helper (mirrors test_heartbeat_integration._setup) ───────────────


async def _setup(
    pg_dsn: str,
    **overrides: str,
) -> tuple[AsyncExitStack, "WorkerDeps", str]:
    """Open WorkerDeps + PostgresBackend with a clean migrated schema.

    Returns ``(stack, deps, schema)``. Caller MUST ``await stack.aclose()``.
    """
    from taskq.migrate import apply_pending

    settings = make_integration_settings(pg_dsn, **overrides)
    schema = settings.schema_name

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    return stack, deps, schema


# ── NOTIFY fires on running-job cancel ────────────────────────────


@_integration
async def test_cancel_notify_fires_on_running_job(pg_dsn: str) -> None:
    """write_cancel_request on a running job fires pg_notify to both
    the fleet channel (taskq_events_{schema}) and the per-worker channel
    (taskq_worker_{schema}_{worker_id}), each with a JSON payload carrying
    ``"type": "cancel"``, correct ``job_id``, and correct ``worker_id``.
    """
    stack, deps, schema = await _setup(pg_dsn)
    try:
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(conn, schema, worker_id)

        fleet_ch = events_channel(schema)
        worker_ch = worker_channel(schema, str(worker_id))

        # Collect NOTIFY payloads received on each channel via a dedicated
        # listener connection opened before the cancel fires.
        fleet_payloads: list[str] = []
        worker_payloads: list[str] = []

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                fleet_ch,
                lambda _conn, _pid, _ch, payload: fleet_payloads.append(payload),
            )
            await listen_conn.add_listener(
                worker_ch,
                lambda _conn, _pid, _ch, payload: worker_payloads.append(payload),
            )

            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
            result = await backend.write_cancel_request(job_id, "test cancel")
            assert result is True

            # Give asyncpg time to deliver the NOTIFY callbacks.
            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        # Both channels must have received exactly one notification.
        assert len(fleet_payloads) == 1, (
            f"expected 1 NOTIFY on fleet channel, got {len(fleet_payloads)}"
        )
        assert len(worker_payloads) == 1, (
            f"expected 1 NOTIFY on per-worker channel, got {len(worker_payloads)}"
        )

        for raw in (fleet_payloads[0], worker_payloads[0]):
            msg = json.loads(raw)
            assert msg["type"] == "cancel", f"unexpected type in payload: {msg}"
            assert msg["job_id"] == str(job_id), f"wrong job_id in payload: {msg}"
            assert msg["worker_id"] == str(worker_id), f"wrong worker_id in payload: {msg}"
    finally:
        await stack.aclose()


# ── subscribe_cancel_wake wakes on running-job cancel ─────────────


@_integration
async def test_subscribe_cancel_wake_fires_on_cancel(pg_dsn: str) -> None:
    """A subscriber registered via subscribe_cancel_wake() has its
    event set when write_cancel_request fires pg_notify on a running job.

    This exercises the full LISTEN → NOTIFY → _cancel_subscribers.set() path
    with a real PG connection. The event must be set within 2 s of the cancel.
    """
    stack, deps, schema = await _setup(pg_dsn)
    try:
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(conn, schema, worker_id)

        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, worker_id),
                name="notify.listener",
            )

            async with backend.subscribe_cancel_wake() as cancel_event:
                # Issue the cancel via the same write_cancel_request path used
                # in production. pg_notify fires inside the method; the
                # notify_listener_loop above receives it and sets cancel_event.
                result = await backend.write_cancel_request(job_id, "wake test")
                assert result is True

                await asyncio.wait_for(cancel_event.wait(), timeout=2.0)

            shutdown.set()

        assert cancel_event.is_set(), "cancel_wake_event was not set after write_cancel_request"
    finally:
        await stack.aclose()


# ── No NOTIFY on pending/scheduled cancel (case 1) ───────────────


@_integration
async def test_no_notify_on_pending_job_cancel(pg_dsn: str) -> None:
    """Cancelling a pending job (case 1 — immediate terminal) does NOT
    fire pg_notify on the events or worker channels. Only running jobs need
    the interrupt signal.
    """
    stack, deps, schema = await _setup(pg_dsn)
    try:
        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(conn, schema)

        fleet_ch = events_channel(schema)
        received: list[str] = []

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                fleet_ch,
                lambda _conn, _pid, _ch, payload: received.append(payload),
            )

            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
            result = await backend.write_cancel_request(job_id, None)
            assert result is True

            # Allow window for any spurious NOTIFY to arrive.
            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        assert len(received) == 0, (
            f"unexpected NOTIFY on fleet channel for pending job cancel: {received}"
        )
    finally:
        await stack.aclose()


@_integration
async def test_no_notify_on_scheduled_job_cancel(pg_dsn: str) -> None:
    """Cancelling a scheduled job also produces no pg_notify."""
    stack, deps, schema = await _setup(pg_dsn)
    try:
        async with deps.worker_pool.acquire() as conn:
            job_id = await create_pending_job(conn, schema, status="scheduled")

        fleet_ch = events_channel(schema)
        received: list[str] = []

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                fleet_ch,
                lambda _conn, _pid, _ch, payload: received.append(payload),
            )

            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
            result = await backend.write_cancel_request(job_id, None)
            assert result is True

            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        assert len(received) == 0, (
            f"unexpected NOTIFY on fleet channel for scheduled job cancel: {received}"
        )
    finally:
        await stack.aclose()


# ── No NOTIFY on already-cancelled job (case 3) ──────────────────


@_integration
async def test_no_notify_on_already_cancelling_job(pg_dsn: str) -> None:
    """Calling write_cancel_request on a job that already has
    cancel_phase > 0 returns False and fires no pg_notify (idempotency guard).
    """
    stack, deps, schema = await _setup(pg_dsn)
    try:
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            # Insert with cancel_phase=1 to simulate an already-cancelled running job.
            job_id = await create_running_job(conn, schema, worker_id, cancel_phase=1)

        fleet_ch = events_channel(schema)
        received: list[str] = []

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            await listen_conn.add_listener(
                fleet_ch,
                lambda _conn, _pid, _ch, payload: received.append(payload),
            )

            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
            result = await backend.write_cancel_request(job_id, "duplicate")
            assert result is False

            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        assert len(received) == 0, f"unexpected NOTIFY on already-cancelling job: {received}"
    finally:
        await stack.aclose()


@_integration
async def test_no_notify_on_second_cancel_call(pg_dsn: str) -> None:
    """A second write_cancel_request on the same running job (which
    is now cancel_phase=1 after the first call) returns False and fires no
    additional pg_notify.
    """
    stack, deps, schema = await _setup(pg_dsn)
    try:
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(conn, schema, worker_id)

        fleet_ch = events_channel(schema)
        received_after_second: list[str] = []

        listen_conn = await asyncpg.connect(pg_dsn)
        try:
            backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)

            # First cancel — should fire NOTIFY (listener not yet attached, so
            # it arrives on no local listener; we only care about the second).
            r1 = await backend.write_cancel_request(job_id, "first")
            assert r1 is True
            await asyncio.sleep(0.2)

            # Start listening only after the first cancel has fired, so any
            # listener-delivered NOTIFY here is from the second (erroneous) call.
            await listen_conn.add_listener(
                fleet_ch,
                lambda _conn, _pid, _ch, payload: received_after_second.append(payload),
            )

            # Second cancel — case 3, should be a no-op.
            r2 = await backend.write_cancel_request(job_id, "second")
            assert r2 is False
            await asyncio.sleep(0.3)
        finally:
            await listen_conn.close()

        assert len(received_after_second) == 0, (
            f"second write_cancel_request fired unexpected NOTIFY: {received_after_second}"
        )
    finally:
        await stack.aclose()


# ── heartbeat_loop sleep interrupted by cancel_wake_event ─────────
#
# Pure unit test — no PG required. Uses FakePool/FakeConn from test_heartbeat.py
# patterns so the heartbeat loop runs without a real database.


class _FakeConn:
    """Minimal asyncpg.Connection stand-in for heartbeat unit testing."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 1"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakePool:
    """asyncpg.Pool stand-in that yields _FakeConn instances."""

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire signature
        yield _FakeConn()


def _make_heartbeat_deps(heartbeat_interval: float = 10.0) -> "WorkerDeps":
    from taskq.settings import WorkerSettings
    from taskq.worker.deps import WorkerDeps

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": str(heartbeat_interval),
            "TASKQ_LOCK_LEASE": "60.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "3",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type] # Why: not accessed by heartbeat_loop in this test path
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type] # Why: _FakePool is a drop-in for asyncpg.Pool
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def test_heartbeat_loop_sleep_interrupted_by_cancel_wake_event() -> None:
    """When cancel_wake_event fires, heartbeat_loop proceeds to the
    next tick without waiting the full interval.

    Configures a 10-second heartbeat interval, fires cancel_wake_event after
    ~50 ms, and asserts the loop ticks again (i.e., runs another heartbeat
    SQL batch) well within 1 second — proving the wait_for interruption works.
    """
    # Remove the integration mark so this test runs without PG.
    # Short heartbeat so cleanup (await task) doesn't block on a long wait_for.
    deps = _make_heartbeat_deps(heartbeat_interval=1.0)

    cancel_wake_event = asyncio.Event()
    shutdown = asyncio.Event()
    worker_id = new_uuid()

    tick_count = 0

    class _CountingPool(_FakePool):
        @asynccontextmanager
        async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109
            nonlocal tick_count
            tick_count += 1
            yield _FakeConn()

    deps.heartbeat_pool = _CountingPool()  # type: ignore[assignment] # Why: _CountingPool is a drop-in for asyncpg.Pool

    task = asyncio.create_task(
        heartbeat_loop(deps, worker_id, shutdown, cancel_wake_event=cancel_wake_event),
        name="heartbeat.cancel-wake-test",
    )

    # Wait for the first tick to complete (the loop starts ticking immediately).
    await asyncio.sleep(0.05)
    first_tick_count = tick_count

    # Fire the cancel wake event. With a 1-second interval the loop would
    # normally wait, but cancel_wake_event.set() must interrupt it.
    cancel_wake_event.set()

    # Allow up to 0.5 second for the second tick to fire.
    deadline = asyncio.get_running_loop().time() + 0.5
    while tick_count < first_tick_count + 1 and asyncio.get_running_loop().time() < deadline:  # noqa: ASYNC110 — polling external tick_count, not a waitable event
        await asyncio.sleep(0.02)

    shutdown.set()
    await task

    assert tick_count >= first_tick_count + 1, (
        f"expected at least {first_tick_count + 1} ticks after cancel_wake_event.set(), "
        f"got {tick_count} — sleep was not interrupted"
    )


async def test_heartbeat_loop_cancel_wake_event_clears_after_interrupt() -> None:
    """After being woken by cancel_wake_event, the event is cleared
    so the next sleep waits the full interval again unless fired again.

    Fires the event once, confirms a second tick occurs quickly, then confirms
    the event is cleared (not still set) after the loop processes it.
    """
    # Short heartbeat so cleanup (await task) doesn't block on a long wait_for.
    deps = _make_heartbeat_deps(heartbeat_interval=1.0)

    cancel_wake_event = asyncio.Event()
    shutdown = asyncio.Event()
    worker_id = new_uuid()

    class _CountingPool(_FakePool):
        def __init__(self) -> None:
            self.acquire_count = 0

        @asynccontextmanager
        async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109
            self.acquire_count += 1
            yield _FakeConn()

    counting_pool = _CountingPool()
    deps.heartbeat_pool = counting_pool  # type: ignore[assignment]

    task = asyncio.create_task(
        heartbeat_loop(deps, worker_id, shutdown, cancel_wake_event=cancel_wake_event),
        name="heartbeat.clear-test",
    )

    # Let the first tick run.
    await asyncio.sleep(0.05)

    cancel_wake_event.set()

    # Wait for the second tick (woken by the event) to complete.
    deadline = asyncio.get_running_loop().time() + 0.5
    before = counting_pool.acquire_count
    while counting_pool.acquire_count < before + 1 and asyncio.get_running_loop().time() < deadline:  # noqa: ASYNC110 — polling external acquire_count, not a waitable event
        await asyncio.sleep(0.02)

    # Give the loop a moment to clear the event after the tick.
    await asyncio.sleep(0.05)

    is_cleared = not cancel_wake_event.is_set()

    shutdown.set()
    await task

    assert is_cleared, "cancel_wake_event was not cleared after the heartbeat loop processed it"
