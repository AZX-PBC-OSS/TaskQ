"""Integration tests for the enqueue → NOTIFY → wake path.

Verifies the end-to-end low-latency dispatch path: ``JobsClient.enqueue()``
→ ``PostgresBackend.enqueue`` ``pg_notify`` at
``src/taskq/backend/postgres.py:717-727`` → listener callback → subscriber
event, plus the polling-fallback path when the listener is killed.
"""

import asyncio
import time
from collections.abc import Iterator
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.settings import make_integration_settings
from taskq.worker.notify import (
    _active_listeners,
    _connected_lookup,
    _notify_reconnects_counter,
    notify_listener_loop,
)

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)
_WORKER_ID = new_uuid()

# ── Module-state cleanup fixture ─────────────────────────────────────────


@pytest.fixture(autouse=True)
def _restore_notify_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture; pyright does not track fixture usage
    _active_listeners.clear()
    _connected_lookup.clear()
    try:
        yield
    finally:
        _active_listeners.clear()
        _connected_lookup.clear()


# ── Helpers ──────────────────────────────────────────────────────────────


async def _setup_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


def _enqueue_args(
    *,
    idempotency_key: str | None = None,
    actor: str = "notify_test_actor",
    queue: str = "default",
    payload: dict[str, object] | None = None,
    payload_schema_ver: int = 1,
) -> EnqueueArgs:
    from dataclasses import replace as _dc_replace

    base = make_enqueue_args(
        idempotency_key=idempotency_key,
        actor=actor,
        queue=queue,
        payload=payload,
    )
    if payload_schema_ver != 1:
        return _dc_replace(base, payload_schema_ver=payload_schema_ver)
    return base


# ── Enqueue → NOTIFY → subscriber event with listener running ──────────


async def test_enqueue_wakes_subscriber_with_listener(pg_dsn: str) -> None:
    """Enqueue → NOTIFY → subscriber event with listener running.

    Demonstrates that a ``PostgresBackend.enqueue(...)`` call wakes a subscriber
    holding a ``subscribe_wake()`` event within a bounded wall-clock window.
    The pg_notify fires at commit from ``src/taskq/backend/postgres.py:717-727``
    and the callback from ``src/taskq/worker/notify.py`` delivers to the
    per-backend subscriber registry.
    """
    worker_settings = make_integration_settings(pg_dsn)
    await _setup_schema(pg_dsn, worker_settings.schema_name)

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
                await asyncio.sleep(0.05)

                args = _enqueue_args()
                t0 = time.perf_counter()
                await backend.enqueue(args)
                await asyncio.wait_for(event.wait(), timeout=2.0)
                t1 = time.perf_counter()

            shutdown.set()

        assert event.is_set(), "event should be set after enqueue NOTIFY"
        latency_ms = (t1 - t0) * 1000.0
        assert latency_ms < 1200.0, f"enqueue→wake latency {latency_ms:.1f} ms exceeds 1200 ms gate"


# ── Polling fallback dispatches when listener is dead ──────────────────


async def test_polling_fallback_with_dead_listener(pg_dsn: str) -> None:
    """Polling fallback dispatches when listener is dead.

    Demonstrates that with the listener ``notify_conn`` killed via
    ``pg_terminate_backend``, the reconnect-fetch plus poll-interval
    bound delivers the subscriber event. A job enqueued during the listener
    outage is surfaced via the synthetic ``on_notify`` call after
    ``_reconnect`` brings up a fresh connection.

    The latency budget for this scenario is
    ``notify_health_check_interval + reconnect_backoff + poll_interval``
    (not just ``poll_interval``) — the reconnect path must run to
    completion before the synthetic callback fires. With defaults
    (5 + 1*2 + 1 = 8 s), the test bounds this at 10 s for CI stability.
    """
    worker_settings = make_integration_settings(pg_dsn)
    await _setup_schema(pg_dsn, worker_settings.schema_name)

    from taskq.worker.deps import open_worker_deps

    async with open_worker_deps(worker_settings) as deps:
        backend = PostgresBackend(deps, SystemClock(), _GRACE, _GRACE)
        shutdown = asyncio.Event()

        reconnect_adds: list[int] = []
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(
                _notify_reconnects_counter,
                "add",
                lambda n: reconnect_adds.append(n),
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    notify_listener_loop(deps, backend, shutdown, _WORKER_ID),
                    name="notify.listener",
                )

                async with backend.subscribe_wake() as event_wake:
                    await asyncio.sleep(0.05)

                    notify_conn = deps.notify_conn
                    assert notify_conn is not None
                    pid = notify_conn.get_server_pid()
                    assert pid > 0

                    killer_conn = await asyncpg.connect(pg_dsn)
                    try:
                        await killer_conn.execute("SELECT pg_terminate_backend($1)", pid)
                    finally:
                        await killer_conn.close()

                    args = _enqueue_args()
                    await backend.enqueue(args)

                    await asyncio.wait_for(event_wake.wait(), timeout=10.0)

                shutdown.set()

        assert event_wake.is_set(), "event should be set after reconnect synthetic callback"
        assert len(reconnect_adds) == 1, f"expected exactly 1 reconnect, got {reconnect_adds}"
