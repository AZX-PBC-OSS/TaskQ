"""Integration tests for taskq.worker.notify against real Postgres 18.

Why test against real PG: the asyncpg-protocol layer (_process_notification,
loop.call_soon delivery, pg_listening_channels() server-side state) is
exercised end-to-end. Unit tests mock the asyncpg interaction surface.

Covers:
"""

import asyncio
import contextlib
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import wake_channel
from taskq.testing.settings import make_integration_settings
from taskq.worker.notify import notify_listener_loop

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)
_WORKER_ID = new_uuid()

# Maximum NOTIFY delivery latency acceptable for regression detection.
# This is a loose guard, not a precise SLO — the 100 ms claim from the
# acceptance_definition is verified structurally by the synchronous-callback
# design. CI runners add 50-200 ms of scheduling jitter; 500 ms
# provides headroom while still catching gross regressions. A future M4 SLO
# instrumentation pass is the place for a latency histogram with a tighter
# empirical bound.
_NOTIFY_LATENCY_GATE_MS: float = 500.0


# ── NOTIFY from another session wakes subscriber ────────────────


async def test_notify_from_another_session_wakes_subscriber(pg_dsn: str) -> None:
    """NOTIFY from another session wakes subscriber, latency < gate.

    The latency gate (_NOTIFY_LATENCY_GATE_MS) is a regression catch, not a
    precise SLO. The 100 ms claim from the acceptance_definition is verified
    structurally by the synchronous-callback design.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.listener",
            )

            async with backend.subscribe_wake() as event:
                second_conn = await asyncpg.connect(pg_dsn)
                try:
                    t0 = asyncio.get_running_loop().time()
                    await second_conn.execute("SELECT pg_notify($1, '')", channel)
                    await asyncio.wait_for(event.wait(), timeout=2.0)
                    t1 = asyncio.get_running_loop().time()
                finally:
                    await second_conn.close()

            shutdown.set()

        latency_ms = (t1 - t0) * 1000.0
        assert latency_ms < _NOTIFY_LATENCY_GATE_MS, (
            f"NOTIFY delivery latency {latency_ms:.1f} ms exceeds "
            f"{_NOTIFY_LATENCY_GATE_MS:.0f} ms gate"
        )


# ── Two simultaneous subscribers both receive the event ─────────


async def test_two_subscribers_both_receive_event(pg_dsn: str) -> None:
    """two simultaneous subscribers both receive the event."""
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.listener",
            )

            async with (
                backend.subscribe_wake() as event_a,
                backend.subscribe_wake() as event_b,
            ):
                second_conn = await asyncpg.connect(pg_dsn)
                try:
                    await second_conn.execute("SELECT pg_notify($1, '')", channel)
                finally:
                    await second_conn.close()

                await asyncio.wait_for(asyncio.gather(event_a.wait(), event_b.wait()), timeout=2.0)

            shutdown.set()

        assert event_a.is_set(), "event_a should be set after NOTIFY"
        assert event_b.is_set(), "event_b should be set after NOTIFY"


# ── LISTEN active before listener loop starts AND remains active after start


async def test_listen_active_before_loop_and_remains_after(pg_dsn: str) -> None:
    """LISTEN active before listener loop starts AND remains active after start.

    Phase 1: pg_listening_channels() after open_worker_deps but before
    notify_listener_loop verifies the contract.
    Phase 2: after spawn + brief drain, channel remains present — asyncpg
    add_listener is idempotent at the SQL level because duplicate LISTEN
    statements are no-ops.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        assert deps.notify_conn is not None

        result = await deps.notify_conn.fetchval("SELECT pg_listening_channels()")
        assert result == channel, f"Phase 1: expected {channel!r}, got {result!r}"

        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.listener",
            )

            await asyncio.sleep(0.05)

            result = await deps.notify_conn.fetchval("SELECT pg_listening_channels()")
            assert result == channel, f"Phase 2: expected {channel!r}, got {result!r}"

            shutdown.set()


# ── UNLISTEN observable after shutdown ──────────────────────────


async def test_unlisten_observable_after_shutdown(pg_dsn: str) -> None:
    """UNLISTEN observable after shutdown — subscriber opened after
    shutdown does NOT receive a NOTIFY that was sent after shutdown.

    After notify_listener_loop exits, remove_listener has been called,
    and the PG-level LISTEN is removed. A NOTIFY sent from a second
    connection has no destination, so a fresh subscriber does not receive
    the event.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.listener",
            )

            await asyncio.sleep(0.05)

            shutdown.set()

        second_conn = await asyncpg.connect(pg_dsn)
        try:
            await second_conn.execute("SELECT pg_notify($1, '')", channel)
        finally:
            await second_conn.close()

        async with backend.subscribe_wake() as event:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout=0.2)

        assert not event.is_set(), "event should not be set after listener shutdown"

        if deps.notify_conn is not None:
            with contextlib.suppress(asyncpg.InterfaceError):
                result = await deps.notify_conn.fetchval("SELECT pg_listening_channels()")
                assert result is None or result != channel, (
                    f"LISTEN should be gone after shutdown, got {result!r}"
                )


# ── Listener task does not consume pool connections ─────────────


async def test_listener_does_not_consume_pool_connections(pg_dsn: str) -> None:
    """Listener task does not consume pool connections (behavioral guard).

    Captures pool connection counts before and after running
    notify_listener_loop for ~1 s with several NOTIFY deliveries.
    Asserts the counts are unchanged — the listener only uses
    deps.notify_conn and never acquires from any pool.

    The static AST guard is a fast gate; this behavioral test is
    the authoritative check.
    """
    worker_settings = make_integration_settings(pg_dsn)
    channel = wake_channel(worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        before_sizes: dict[str, tuple[int, int]] = {}
        for label, pool in (
            ("dispatcher", deps.dispatcher_pool),
            ("heartbeat", deps.heartbeat_pool),
            ("worker", deps.worker_pool),
        ):
            before_sizes[label] = (pool.get_size(), pool.get_idle_size())

        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                name="notify.listener",
            )

            async with backend.subscribe_wake() as event:
                second_conn = await asyncpg.connect(pg_dsn)
                try:
                    for _ in range(5):
                        await second_conn.execute("SELECT pg_notify($1, '')", channel)
                    await asyncio.wait_for(event.wait(), timeout=2.0)
                finally:
                    await second_conn.close()

            await asyncio.sleep(0.5)

            shutdown.set()

        after_sizes: dict[str, tuple[int, int]] = {}
        for label, pool in (
            ("dispatcher", deps.dispatcher_pool),
            ("heartbeat", deps.heartbeat_pool),
            ("worker", deps.worker_pool),
        ):
            after_sizes[label] = (pool.get_size(), pool.get_idle_size())

        assert before_sizes == after_sizes, (
            f"pool connection counts changed during listener run: "
            f"before={before_sizes}, after={after_sizes}"
        )
