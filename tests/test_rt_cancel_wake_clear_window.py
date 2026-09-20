"""Hunt-gap pin: the heartbeat's cancel-wake CLEAR window.

``heartbeat_loop``'s inter-tick wait wakes early on a cancel NOTIFY and
then clears the event UNCONDITIONALLY
(``worker/heartbeat.py``: the ``wait_for`` + ``clear()`` pair). A NOTIFY
that lands between the wait's timeout and that clear is wiped without
the wait ever observing it - the window is real and the clear cannot be
made conditional (a wait/clear race has no atomic form around a plain
``asyncio.Event``).

The design's answer is that the wake is only a LATENCY optimization:
every tick's ``cancel_controller.run_in_tx`` re-reads the cancel flags
from the database, so a swallowed wake costs nothing in correctness,
only the wait until the next tick, bounded by one heartbeat interval.

Pins:

* unit (no PG): a wake that lands in the wait-to-clear window
  deterministically - the wait ends by its timeout, the set lands, THEN
  the clear runs - is swallowed by the clear, and the very next tick's
  ``run_in_tx`` still observes the cancel, within one heartbeat
  interval;
* integration (real PG, real LISTEN/NOTIFY): a cancel whose NOTIFY is
  confirmed delivered (a listener connection saw it) while the loop's
  wake event provably never fires is still observed by a tick's
  ``run_in_tx`` within one heartbeat interval.

The test stubs follow ``test_cancel_notify_integration.py``'s patterns
(its FakePool heartbeat unit tests and its real-LISTEN integration
shapes).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import events_channel
from taskq.testing.assertions import wait_for_condition
from taskq.testing.pg import create_running_job, create_worker
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

_GRACE_S = 30.0
_HEARTBEAT_INTERVAL_S = 1.0
_INTEGRATION = pytest.mark.integration


# ── Stubs ────────────────────────────────────────────────────────────


class _RecordingCancelController:
    """A ``CancelController`` stand-in recording each phase call.

    The Protocol's two methods are the whole surface the loop needs;
    ``run_in_tx`` is the DB re-read the pins measure.
    """

    def __init__(self) -> None:
        self.in_tx_calls: list[float] = []
        self.post_tx_calls: list[float] = []

    async def run_in_tx(self, conn: object) -> None:
        self.in_tx_calls.append(time.monotonic())

    async def run_post_tx(self) -> None:
        self.post_tx_calls.append(time.monotonic())


class _ClearWindowWake:
    """A wake event whose FIRST ``wait()`` reproduces the clear window.

    Duck-typed to the loop's complete surface (``wait`` + ``clear``):
    ``heartbeat_loop`` never touches the event any other way.

    The heartbeat's bounded wait ends one of two ways: the event sets
    (wait returns) or the timeout fires (``wait_for`` raises). The
    window is a set landing AFTER the timeout has already doomed the
    wait but BEFORE the loop's unconditional ``clear()``. This stub
    replays that order exactly: the test opens the gate once the loop is
    parked in the wake wait, the stub sets itself (the NOTIFY lands),
    then raises ``TimeoutError`` the way the expired ``wait_for`` does.
    The loop's suppress swallows it and the clear runs with the event
    SET - the swallowed wake. Every later ``wait()`` behaves like a real
    event's.
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._gate = asyncio.Event()
        self._window_used = False
        self.cleared_while_set = 0

    def open_window(self) -> None:
        self._gate.set()

    async def wait(self) -> bool:
        if self._window_used:
            return await self._event.wait()
        self._window_used = True
        await self._gate.wait()
        self._event.set()  # the NOTIFY lands in the wait-to-clear window
        raise TimeoutError()  # the bounded wait's expiry

    def clear(self) -> None:
        if self._event.is_set():
            self.cleared_while_set += 1
        self._event.clear()


class _FakeConn:
    """Minimal asyncpg.Connection stand-in for heartbeat unit testing."""

    async def execute(self, sql: str, *args: object) -> str:
        return "UPDATE 1"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakeTransaction:
    """The loop drives start/commit/rollback explicitly, not ``async with``."""

    async def start(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakePool:
    """asyncpg.Pool stand-in yielding ``_FakeConn`` instances."""

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire signature
        yield _FakeConn()


def _make_heartbeat_deps(heartbeat_interval: float) -> WorkerDeps:
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


# ── Unit: the clear window deterministically ─────────────────────────


async def test_a_notify_landing_in_the_clear_window_is_still_observed_by_the_db_reread() -> None:
    """The swallowed wake: the wait expires, the set lands, the clear
    wipes it - and the next tick's ``run_in_tx`` still runs, within one
    heartbeat interval. The DB re-read, not the wake, is what makes the
    cancel observable."""
    deps = _make_heartbeat_deps(_HEARTBEAT_INTERVAL_S)
    controller = _RecordingCancelController()
    wake = _ClearWindowWake()
    shutdown = asyncio.Event()

    task = asyncio.create_task(
        heartbeat_loop(
            deps,
            new_uuid(),
            shutdown,
            cancel_controller=controller,
            cancel_wake_event=wake,
        )
    )
    try:
        await wait_for_condition(
            lambda: len(controller.in_tx_calls) >= 1,
            description="the first tick's DB re-read",
            timeout=5.0,
        )
        # The loop is now parked in the wake wait. Open the gate: the
        # wait gives up, the NOTIFY lands, the clear swallows it.
        window_opened_at = time.monotonic()
        wake.open_window()
        await wait_for_condition(
            lambda: len(controller.in_tx_calls) >= 2,
            description="the tick after the swallowed wake",
            timeout=5.0,
        )
        observed_at = controller.in_tx_calls[1]

        assert wake.cleared_while_set == 1, (
            "the window must have been hit: a set the wait never "
            "observed, then cleared by the loop's unconditional clear"
        )
        assert observed_at - window_opened_at < _HEARTBEAT_INTERVAL_S, (
            "the cancel whose wake was swallowed must still be observed "
            "by the next tick's DB re-read, bounded by one heartbeat "
            f"interval; took {observed_at - window_opened_at:.3f}s"
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ── Integration: real LISTEN/NOTIFY, real PG ─────────────────────────


async def _setup(pg_dsn: str) -> tuple[AsyncExitStack, WorkerDeps, str]:
    """Open WorkerDeps + a migrated clean schema (the
    test_cancel_notify_integration pattern). Caller must ``aclose()``."""
    from taskq.migrate import apply_pending

    settings = make_integration_settings(pg_dsn)
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


@_INTEGRATION
async def test_a_confirmed_notify_that_reaches_no_wake_is_observed_by_a_tick(pg_dsn: str) -> None:
    """Real LISTEN/NOTIFY: the cancel's NOTIFY is confirmed delivered on
    the fleet channel, while the loop's wake event provably never fires
    (a private event nobody sets - the durable stand-in for a wake
    swallowed by the clear window, which the unit pin above reproduces
    deterministically). The next tick's ``run_in_tx`` must still observe
    the cancel, within one heartbeat interval."""
    stack, deps, schema = await _setup(pg_dsn)
    listen_conn: asyncpg.Connection | None = None
    try:
        worker_id = new_uuid()
        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(conn, schema, worker_id)

        backend = PostgresBackend(deps, SystemClock(), _GRACE_S, _GRACE_S)

        notify_landed = asyncio.Event()
        notify_times: list[float] = []
        listen_conn = await asyncpg.connect(pg_dsn)
        await listen_conn.add_listener(
            events_channel(schema),
            lambda *_args: (
                notify_times.append(time.monotonic()),
                notify_landed.set(),
            ),
        )

        controller = _RecordingCancelController()
        swallowed_wake = asyncio.Event()  # nobody sets this
        shutdown = asyncio.Event()
        interval = deps.settings.heartbeat_interval
        loop_task = asyncio.create_task(
            heartbeat_loop(
                deps,
                worker_id,
                shutdown,
                cancel_controller=controller,
                cancel_wake_event=swallowed_wake,
            )
        )
        try:
            await wait_for_condition(
                lambda: len(controller.in_tx_calls) >= 1,
                description="the first tick's DB re-read",
                timeout=5.0,
            )
            assert await backend.write_cancel_request(job_id, "clear window") is True

            # The NOTIFY really fired and was delivered.
            await asyncio.wait_for(notify_landed.wait(), timeout=5.0)
            t_notify = notify_times[0]
            assert not swallowed_wake.is_set(), (
                "fixture broken: the wake the loop reads must never fire, "
                "the pin is the DB re-read without any wake"
            )

            await wait_for_condition(
                lambda: any(t >= t_notify for t in controller.in_tx_calls),
                description="a tick's DB re-read after the delivered NOTIFY",
                timeout=5.0,
            )
            observed_at = min(t for t in controller.in_tx_calls if t >= t_notify)
            assert observed_at - t_notify < 2 * interval, (
                "a delivered cancel whose wake never reached the loop must "
                f"be observed by a tick's DB re-read within one heartbeat "
                f"interval (plus the tick's own duration); took "
                f"{observed_at - t_notify:.3f}s at interval {interval}s"
            )
        finally:
            shutdown.set()
            loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await loop_task
    finally:
        if listen_conn is not None:
            await listen_conn.close()
        await stack.aclose()
