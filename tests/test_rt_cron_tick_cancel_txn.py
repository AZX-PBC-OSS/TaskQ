"""A cron tick cut mid-transaction must not leave its connection inside one.

The leader's cron loop wraps one tick in ``asyncio.timeout`` and enters the
transaction with asyncpg's managed ``async with conn.transaction()``. When the
deadline expires while the tick's own ``BEGIN`` is outstanding, asyncpg's
``Transaction.start()`` has already assigned ``conn._top_xact`` and never clears
it on failure, so the transaction object is left FAILED with the server-side
transaction open. The leader's deadline branch keeps that connection ("the conn
is provably responsive ... keep the conn and retry"), and every later tick then
runs as a nested SAVEPOINT of the transaction nobody will ever commit or roll
back: each tick's ``RELEASE SAVEPOINT`` returns, the cron row's lock is held for
the life of the process, and nothing a tick writes ever becomes visible.

The tests here drive the real ``MaintenanceLeader._cron_loop`` against real
Postgres. A stalling relay in front of Postgres holds the server's replies, so
``BEGIN`` is executed server-side while the client never sees the answer: the
only way to make a statement deadline expire with the work already done, which
is the condition that produces the leaked transaction.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from urllib.parse import urlsplit

import asyncpg
import pytest
from asyncpg.transaction import Transaction as _AsyncpgTransaction

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import (
    _create_worker,  # pyright: ignore[reportPrivateImportUsage]  # Why: the shared test helper the neighbouring cron integration tests use; the private prefix scopes it within the testing package.
)
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.leader import MaintenanceLeader

from .test_cron_integration import _insert_actor_config, _insert_schedule

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.load_sensitive]
# Why load_sensitive: every pin here asserts a wall-clock window at the 1.0s
# tick deadline (30-50ms slack), and the file drives the same wedged-tick /
# tx-unwind machinery as tests/test_rt_cron_hang_livelock.py, which the CI
# lanes already treat as load-sensitive. Under -n 2 with Docker and a
# containerised Postgres on the same cores a scheduling overshoot turns a
# healthy window into the failure the pin is not about. The serial
# load-sensitive lane is the density these bounds assume.

# The whole-tick deadline the leader arms (and, at production parity, the
# connection's per-statement command timeout), and the cron tick period. Both are
# the smallest values the settings accept.
_TICK_DEADLINE_S = 1.0
_TICK_PERIOD_S = 1.0
# The actor whose row the production leak held, due two hours ago so the first
# tick that runs inside a usable transaction fires it.
_ACTOR = "sweep_extraction_runs"
# Several ticks of the 1s cadence, and still a fast failure.
_RECOVERY_TIMEOUT_S = 15.0


class _StallingRelay:
    """A TCP relay in front of Postgres that can hold the server's replies.

    ``hold()`` parks every server→client chunk instead of forwarding it, so a
    statement reaches Postgres, is executed, and its answer never reaches the
    client. That is a stalled network, and it is the only way to make a
    statement's deadline expire with the work already done server-side. The
    client's cancel request arrives on its own connection, exactly as it does
    against a real server; both ends of a connection are closed when either
    pump ends, which is what tells the client a cancel was delivered.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server: asyncio.AbstractServer | None = None
        self._main_client_writer: asyncio.StreamWriter | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._held: list[bytes] = []
        self._hold_marker: str | None = None
        self.holding = asyncio.Event()
        self.held_any = asyncio.Event()
        self.port = 0

    async def __aenter__(self) -> "_StallingRelay":
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        self.release()
        for writer in list(self._writers):
            with contextlib.suppress(Exception):
                writer.close()
        # Let the per-connection pumps observe the closes and finish: the suite
        # fails a test that leaves tasks pending on the module loop.
        for _ in range(10):
            await asyncio.sleep(0)

    def hold(self) -> None:
        self.holding.set()

    def hold_from(self, marker: str) -> None:
        """Start holding replies once the client puts *marker* on the wire.

        ``hold()`` taken before a statement is sent parks the client on the
        statement's Parse, so the statement never runs server-side and takes no
        lock. Holding from the client->server direction instead lets the
        statement that carries *marker* reach Postgres and execute, and holds
        only its answer - the shape in which the server is left with the work
        done and the client never learns it.
        """
        self._hold_marker = marker

    def release(self) -> None:
        """Forward everything held back to the connection that asked for it."""
        self.holding.clear()
        held, self._held = self._held, []
        writer = self._main_client_writer
        if writer is None:
            return
        for chunk in held:
            with contextlib.suppress(Exception):
                writer.write(chunk)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(self._host, self._port)
        self._writers.update({writer, upstream_writer})
        if self._main_client_writer is None:
            # The first connection is the cron connection; every later one is
            # asyncpg's cancel-request connection, which is sent nothing.
            self._main_client_writer = writer

        # The upstream bytes of this connection, kept just long enough to find a
        # marker that straddles two reads.
        upstream_tail = b""

        async def up() -> None:
            nonlocal upstream_tail
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    return
                seen = upstream_tail + chunk
                upstream_tail = seen[-512:]
                marker = self._hold_marker
                if marker is not None and marker.encode() in seen:
                    # Before the write, and synchronously: the statement must
                    # still reach the server, and its answer must be caught by
                    # the downstream pump that is already awaiting it.
                    self._hold_marker = None
                    self.holding.set()
                upstream_writer.write(chunk)
                await upstream_writer.drain()

        async def down() -> None:
            while True:
                chunk = await upstream_reader.read(65536)
                if not chunk:
                    return
                if self.holding.is_set():
                    self._held.append(chunk)
                    self.held_any.set()
                    continue
                writer.write(chunk)
                await writer.drain()

        pumps = {asyncio.ensure_future(up()), asyncio.ensure_future(down())}
        try:
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pump in pumps:
                pump.cancel()
            for w in (writer, upstream_writer):
                with contextlib.suppress(Exception):
                    w.close()
            self._writers.difference_update({writer, upstream_writer})
            if self._main_client_writer is writer:
                self._main_client_writer = None


@dataclass
class _CronRig:
    """The leader, its cron connection, and the reads a test observes it with."""

    schema: str
    deps: WorkerDeps
    leader: MaintenanceLeader
    cron_conn: asyncpg.Connection
    relay: _StallingRelay
    schedule_id: object
    shutdown: asyncio.Event = field(default_factory=asyncio.Event)
    loop: asyncio.Task[None] | None = None

    def start_loop(self) -> asyncio.Task[None]:
        """Run the leader's real cron loop as its own task."""
        task = asyncio.create_task(self.leader._cron_loop(self.shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the loop is the unit under test; driving it as a task is what lets a test cancel it at the window under test.
        self.loop = task
        return task

    async def stop_loop(self) -> None:
        self.shutdown.set()
        if self.loop is not None and not self.loop.done():
            self.loop.cancel()
        if self.loop is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.loop

    async def cron_conn_is_in_transaction(self) -> bool:
        """Whether the cron connection carries an open transaction right now."""
        if self.cron_conn.is_closed():
            return False
        return self.cron_conn.is_in_transaction()

    async def cron_backend_state(self) -> str | None:
        """The cron backend's server-side state, or None once its session is gone."""
        if self.cron_conn.is_closed():
            return None
        # Scoped to this database: pg_stat_activity is cluster-wide and the shared
        # container hosts every xdist worker's database, so a pid-only filter can be
        # answered by a foreign backend that recycled the pid - the cron conn's
        # state read from another test's session. tests/test_suite_hygiene.py pins
        # the scope.
        state = await self.deps.dispatcher_pool.fetchval(
            "SELECT state FROM pg_stat_activity WHERE pid = $1 AND datname = current_database()",
            self.cron_conn.get_server_pid(),
        )
        return None if state is None else str(state)

    async def committed_jobs(self) -> int:
        """Fires another session can see: a tick's work exists only if it committed."""
        count = await self.deps.dispatcher_pool.fetchval(
            f'SELECT count(*) FROM "{self.schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: the schema name is generated in this test and migration-validated; identifiers cannot be bound.
            _ACTOR,
        )
        return int(count)

    async def committed_next_fire(self) -> datetime:
        value = await self.deps.dispatcher_pool.fetchval(
            f'SELECT next_fire_at FROM "{self.schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: the schema name is generated in this test and migration-validated; identifiers cannot be bound.
            self.schedule_id,
        )
        assert isinstance(value, datetime)
        return value

    async def cron_row_is_locked(self) -> bool:
        """Whether a second session is refused the schedule row's lock."""
        try:
            await self.deps.dispatcher_pool.execute(
                f'SELECT 1 FROM "{self.schema}".cron_schedules '  # noqa: S608  # Why: the schema name is generated in this test and migration-validated; identifiers cannot be bound.
                "WHERE id = $1 FOR UPDATE NOWAIT",
                self.schedule_id,
            )
        except asyncpg.LockNotAvailableError:
            return True
        return False


@contextlib.asynccontextmanager
async def _cron_leader(pg_dsn: str) -> AsyncGenerator[_CronRig, None]:
    """A leader whose cron connection is opened through a stalling relay."""
    schema = f"test_cron_{new_base62()}"
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            # The lease must cover the failed-beat cascade the settings
            # validator sizes, which is long because the failure budget is high.
            "TASKQ_LOCK_LEASE": "4000.0",
            "TASKQ_HEARTBEAT_INTERVAL": str(_TICK_PERIOD_S),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
            "TASKQ_CRON_AUTO_DISABLE_THRESHOLD": "3",
            "TASKQ_DISPATCHER_COMMAND_TIMEOUT": str(_TICK_DEADLINE_S),
            # The watchdog would kill the process for the stalled loop the
            # relay creates; the loop's own behaviour is what is under test.
            "TASKQ_WATCHDOG_ENABLED": "false",
        }
    )
    direct_dsn = str(settings.pg_dsn_direct)
    admin = await asyncpg.connect(direct_dsn)
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(admin, schema=settings.schema_name)
    finally:
        await admin.close()

    cron_conn: asyncpg.Connection | None = None
    async with contextlib.AsyncExitStack() as stack:
        deps = await stack.enter_async_context(open_worker_deps(settings))
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
        worker_id = new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, settings.schema_name, worker_id)
            await _insert_actor_config(conn, settings.schema_name, _ACTOR)
            schedule_id = await _insert_schedule(
                conn,
                settings.schema_name,
                _ACTOR,
                next_fire_at=datetime.now(UTC) - timedelta(hours=2),
            )

        parts = urlsplit(direct_dsn)
        relay = await stack.enter_async_context(
            _StallingRelay(parts.hostname or "localhost", parts.port or 5432)
        )
        # Production parity: the connection's per-statement command timeout is
        # the same setting the leader arms its whole-tick deadline with, so both
        # timers fire while BEGIN's answer is stalled. That is deliberate: which
        # of the two cuts the tick is immaterial, because both raise out of
        # asyncpg's Transaction.start() and leave the identical FAILED
        # transaction behind. The relay holds BEGIN's answer past both, so
        # neither can miss.
        cron_conn = await asyncpg.connect(
            direct_dsn,
            host="127.0.0.1",
            port=relay.port,
            command_timeout=_TICK_DEADLINE_S,
            ssl="disable",
        )
        deps.is_leader.set()
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        leader._cron_conn = cron_conn  # pyright: ignore[reportPrivateUsage]  # Why: the cron loop's dedicated connection is the piece under test; wiring it is the point of the rig.
        rig = _CronRig(
            schema=settings.schema_name,
            deps=deps,
            leader=leader,
            cron_conn=cron_conn,
            relay=relay,
            schedule_id=schedule_id,
        )
        try:
            yield rig
        finally:
            assert cron_conn is not None
            await rig.stop_loop()
            if not cron_conn.is_closed():
                # CancelledError as well as Exception: a connection whose
                # cancelled statement never got its answer raises it from
                # close(), and a test's own teardown must not turn that into
                # its failure.
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    async with asyncio.timeout(5):
                        await cron_conn.close()


class _ParkedStatementConn:
    """A connection proxy that parks on one statement, delegating everything else.

    Parking before the statement is sent is how a test lands a cancellation at an
    exact point of the tick: the await here is where the tick sits when the
    caller cancels it.
    """

    def __init__(self, conn: asyncpg.Connection, marker: str, parked: asyncio.Event) -> None:
        self._conn = conn
        self._marker = marker
        self._parked = parked

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)

    async def execute(self, sql: str, *args: object) -> str:
        if self._marker in sql and not self._parked.is_set():
            self._parked.set()
            await asyncio.Event().wait()
        return await self._conn.execute(sql, *args)


class _FailingBeginConn:
    """A proxy that fails the FIRST BEGIN client-side, delegating everything else.

    A BEGIN round trip can fail with any error class, not only the deadline
    family, and the failure lands inside asyncpg's Transaction.start() exactly
    as a timeout does. Writes as well as reads are delegated - Transaction.start
    assigns the top-level transaction object THROUGH the connection - so the
    proxy stays a faithful view of one real connection instead of splitting
    its state between two objects.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self._begin_failed = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)

    def __setattr__(self, name: str, value: object) -> None:
        if name in ("_conn", "_begin_failed"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._conn, name, value)

    def transaction(
        self, *, isolation: object = None, readonly: bool = False, deferrable: bool = False
    ) -> object:
        # Bind the transaction to the PROXY so its start/commit/rollback round
        # trips pass through execute() below like every other statement.
        return _AsyncpgTransaction(self, isolation, readonly, deferrable)

    async def execute(self, sql: str, *args: object) -> str:
        # "BEGIN" as a prefix, not equality: the driver sends 'BEGIN;' (and
        # appends isolation/readonly clauses before the semicolon when set).
        if sql.startswith("BEGIN") and not self._begin_failed:
            self._begin_failed = True
            raise asyncpg.AdminShutdownError("simulated 57P01 inside Transaction.start")
        return await self._conn.execute(sql, *args)


class _FailingResetConn:
    """A connection proxy whose FIRST ``reset()`` fails, then delegates.

    The deferred repair's non-deadline failure path is the one where a tick's
    own success proves nothing: with the leaked top-level transaction still on
    the connection, the next tick runs as a nested SAVEPOINT and appears to
    commit while nothing does. Failing the first repair without touching the
    real connection lets a test drive that window and assert the record
    survives to the next iteration's repair.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self._reset_calls = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)

    async def reset(self) -> None:
        self._reset_calls += 1
        if self._reset_calls == 1:
            raise AttributeError("reset")
        await self._conn.reset()


class _ParkedResetConn:
    """A connection proxy whose deferred repair parks until the loop is cancelled.

    The repair region's real-cancellation arm (``Task.cancelling() > 0``) is
    the shutdown guarantee: a reset that never answers must not outlive the
    loop. Parking before delegating keeps the real connection usable, so the
    test observes the handler's bounded close rather than a server-side park.
    """

    def __init__(self, conn: asyncpg.Connection, parked: asyncio.Event) -> None:
        self._conn = conn
        self._parked = parked

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)

    async def reset(self) -> None:
        self._parked.set()
        await asyncio.Event().wait()


async def _poll(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float,  # noqa: ASYNC109  # Why: the bound is this helper's own poll deadline, not an enclosing asyncio.timeout scope; the repo's established wait-helper shape.
) -> bool:
    """Await *predicate* until it returns true, or give up after *timeout*."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.1)
    return False


async def _await_cancellation_settled(
    conn: asyncpg.Connection,
    *,
    timeout: float,  # noqa: ASYNC109  # Why: the bound is this helper's own wait deadline, not an enclosing asyncio.timeout scope; the repo's established wait-helper shape.
) -> bool:
    """Wait until asyncpg's cancellation bookkeeping lets the connection query.

    A cancelled statement leaves asyncpg awaiting its cancel request, and every
    statement issued before that settles parks. The next tick has to run on this
    connection for the leak's consequences to be observable, so the test waits
    the way the loop itself does: by asking the connection for an answer.
    Returns whether the connection settled (a closed connection counts: its
    session is gone and the callers observe that directly). The tests assert it
    where the wait is a precondition for their claim, so a slow settle fails by
    name instead of degrading the assertions that follow.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if conn.is_closed():
            return True
        try:
            async with asyncio.timeout(0.5):
                await conn.fetchval("SELECT 1")
        except Exception:
            await asyncio.sleep(0.05)
        else:
            return True
    return False


async def test_a_deadline_cut_tick_leaves_no_open_transaction_and_keeps_firing(
    pg_dsn: str,
) -> None:
    """A tick cut by its deadline must not leave a transaction behind.

    The first tick's BEGIN is executed by Postgres and its answer is stalled
    past the tick deadline. From there the leader must recover: the overdue
    schedule's fire has to become visible, its ``next_fire_at`` has to advance
    durably, the cron row must not stay locked, and the cron connection must not
    be left inside an open transaction - the state in which every later tick
    runs as a savepoint of a transaction that is never committed.
    """
    async with _cron_leader(pg_dsn) as rig:
        rig.relay.hold()
        rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        # Past the tick's deadline: it is cancelled with BEGIN outstanding,
        # which is the window this pin is about.
        await asyncio.sleep(_TICK_DEADLINE_S + 0.3)
        rig.relay.release()
        assert await _await_cancellation_settled(rig.cron_conn, timeout=10), (
            "the cancelled statement never settled: the connection still parks "
            "every operation on the held cancellation reply, so the assertions "
            "after this wait would not observe what they claim"
        )

        # The cadence's own proof: a tick that ran after the cut fires the
        # overdue schedule, and another session sees the job.
        async def fired() -> bool:
            return await rig.committed_jobs() > 0

        await _poll(fired, timeout=_RECOVERY_TIMEOUT_S)

        assert not await rig.cron_conn_is_in_transaction(), (
            "the cron connection was left inside an open transaction after a tick's "
            "deadline cut: every later tick runs as a savepoint of it, so nothing a tick "
            "writes can ever commit"
        )
        assert await rig.cron_backend_state() != "idle in transaction", (
            "the cron backend sits idle in transaction after the deadline cut, holding "
            "whatever the failed tick locked"
        )
        assert not await rig.cron_row_is_locked(), (
            f"the {_ACTOR!r} schedule row is still locked by the failed tick; every "
            "worker's startup cron registration queues behind it"
        )
        assert await rig.committed_jobs() > 0, (
            "no fire from a later tick ever committed: the cadence is dead behind the "
            "failed tick's uncommitted transaction"
        )
        server_now = await rig.deps.dispatcher_pool.fetchval("SELECT clock_timestamp()")
        assert await rig.committed_next_fire() > server_now, (
            "the schedule's next_fire_at never advanced durably after the deadline cut"
        )


async def test_a_repair_that_fails_once_still_cleans_the_conn_before_the_next_commit(
    pg_dsn: str,
    structlog_capture: list[Any],
) -> None:
    """A non-deadline repair failure must not be mistaken for a clean conn.

    The leaked state is a top-level transaction whose BEGIN executed
    server-side. While it is there, ``conn.transaction()`` treats every later
    tick as NESTED: the tick's ``RELEASE SAVEPOINT`` returns, the tick reports
    fires, and nothing commits - the incident itself. So a tick that appears to
    succeed after a failed repair is NOT proof the connection is usable; the
    record must survive the failure and the next repair must clear the state.

    The first repair here raises a non-deadline error without touching the
    connection. If that failure dropped the record, the following ticks would
    nest forever and no fire would ever become visible; with the record kept,
    the second repair rolls the leaked transaction back and the next tick's
    fire commits.
    """
    async with _cron_leader(pg_dsn) as rig:
        proxy = cast("asyncpg.Connection", _FailingResetConn(rig.cron_conn))
        rig.leader._cron_conn = proxy  # pyright: ignore[reportPrivateUsage]  # Why: the repair's failure has to be driven through the loop's own connection, which is the piece under test.
        rig.relay.hold()
        rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        # Past the tick's deadline: BEGIN executed server-side, its reply never
        # arrived, so the top-level transaction is leaked on the connection.
        await asyncio.sleep(_TICK_DEADLINE_S + 0.3)
        rig.relay.release()
        assert await _await_cancellation_settled(rig.cron_conn, timeout=10), (
            "the cancelled statement never settled: the connection still parks "
            "every operation on the held cancellation reply, so the assertions "
            "after this wait would not observe what they claim"
        )

        async def fired() -> bool:
            return await rig.committed_jobs() > 0

        assert await _poll(fired, timeout=_RECOVERY_TIMEOUT_S), (
            "no fire ever committed after a non-deadline repair failure: the "
            "failure dropped the dirty record, every later tick nested in the "
            "leaked transaction, and the fires reported success without "
            "committing"
        )
        failures = [e for e in structlog_capture if e["event"] == "cron-conn-repair-failed"]
        repaired = [e for e in structlog_capture if e["event"] == "cron-conn-repaired"]
        assert len(failures) == 1, (
            f"the pin expected exactly one failed repair before the recovery, saw {len(failures)}"
        )
        assert repaired, (
            "the deferred repair never succeeded after its one failed attempt, "
            "so this pin did not exercise the retry that clears the leak"
        )
        assert not await rig.cron_conn_is_in_transaction(), (
            "the cron connection is still inside the leaked transaction after "
            "the retried repair committed a fire"
        )


async def test_a_cancelled_tick_leaves_no_open_transaction(pg_dsn: str) -> None:
    """A tick cancelled at the same window must unwind its own transaction.

    Shutdown cancels the cron loop, and the cancellation lands with BEGIN
    outstanding exactly as a deadline cut does. The leaked transaction cannot be
    allowed to outlive the tick: the leader-owned connection is reused across
    ticks and only an unrelated teardown path closes it, so a cancel that does
    not reach that teardown leaves the row locked behind a live session.
    """
    async with _cron_leader(pg_dsn) as rig:
        rig.relay.hold()
        loop = rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)

        loop.cancel()
        try:
            # Bounded: a regression that parks the cancelled loop must fail this
            # pin by name, not hang the suite until the global timeout.
            async with asyncio.timeout(10):
                with contextlib.suppress(asyncio.CancelledError):
                    await loop
        except TimeoutError:
            pytest.fail(
                "the cancelled cron loop did not settle within 10s: its shutdown path is parked"
            )
        # Let the client see the truth of its own connection before asking it:
        # until the stalled answer is released, asyncpg has not yet observed the
        # transaction the server opened.
        rig.relay.release()
        assert await _await_cancellation_settled(rig.cron_conn, timeout=10), (
            "the cancelled statement never settled: the connection still parks "
            "every operation on the held cancellation reply, so the assertions "
            "after this wait would not observe what they claim"
        )

        assert not await rig.cron_conn_is_in_transaction(), (
            "a cancelled tick left its connection inside an open transaction"
        )
        assert await rig.cron_backend_state() != "idle in transaction", (
            "a cancelled tick left the backend idle in transaction, holding the cron "
            "row's lock until some other path closes the connection"
        )
        assert not await rig.cron_row_is_locked(), (
            f"the {_ACTOR!r} schedule row is still locked after a cancelled tick"
        )


async def test_a_cancellation_during_the_deferred_repair_closes_the_conn(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shutdown while the deferred repair is parked must bound itself.

    The repair region tells a real cancellation (``Task.cancelling() > 0``)
    from asyncpg's spurious cancelled future, and re-raises so the loop's
    ``CancelledError`` handler runs. The tick's transaction never entered, so
    that handler closes the connection through the bounded helper - a reset
    that never answers must not outlive the loop or hang the worker's
    shutdown grace.
    """
    from taskq.worker import leader as leader_mod

    async with _cron_leader(pg_dsn) as rig:
        parked = asyncio.Event()
        rig.leader._cron_conn = cast(  # pyright: ignore[reportPrivateUsage]  # Why: wiring the proxy the repair must park on is the point of the rig.
            "asyncpg.Connection", _ParkedResetConn(rig.cron_conn, parked)
        )

        async def failing_tick(*args: object, **kwargs: object) -> int:
            # A transient failure records the conn; the next iteration's
            # repair is the one this pin cancels.
            raise asyncpg.SerializationError("simulated 40001 in the tick body")

        monkeypatch.setattr(leader_mod, "tick_cron", failing_tick)
        loop = rig.start_loop()
        await asyncio.wait_for(parked.wait(), timeout=10)

        loop.cancel()
        try:
            async with asyncio.timeout(10):
                with contextlib.suppress(asyncio.CancelledError):
                    await loop
        except TimeoutError:
            pytest.fail(
                "the cron loop did not settle when cancelled inside the "
                "deferred repair: shutdown is parked on a reset that will "
                "never answer"
            )

        assert rig.cron_conn.is_closed(), (
            "the repair-parked connection survived the cancellation: the "
            "CancelledError handler must close it through the bounded helper"
        )


async def test_a_tick_cancelled_after_its_savepoint_release_still_rolls_back(
    pg_dsn: str,
) -> None:
    """A cancel between the enqueue savepoint's release and the outer commit unwinds.

    This is the window the leak was first reported in, and it is NOT where the
    leak lives: the managed transaction is still open, so its ``__aexit__`` runs
    and the tick's ``ROLLBACK`` reaches the server, discarding the work the
    released savepoint had done. Pinned so the fix keeps the ordinary unwind
    intact, and so the window is not re-blamed for a leak it does not cause.
    """
    async with _cron_leader(pg_dsn) as rig:
        # The success UPDATE is the first statement after the per-plan enqueue
        # savepoint is released: parking there puts the cancel in exactly that
        # window, with the outer transaction open and uncommitted.
        parked = asyncio.Event()
        rig.leader._cron_conn = cast(  # pyright: ignore[reportPrivateUsage]  # Why: the cron loop's connection is what the parking proxy must replace.
            "asyncpg.Connection",
            _ParkedStatementConn(rig.cron_conn, "last_fired_at = clock_timestamp()", parked),
        )
        loop = rig.start_loop()
        await asyncio.wait_for(parked.wait(), timeout=20)

        loop.cancel()
        try:
            # Bounded, same reason as the cancellation sibling above: a parked
            # unwind is a pin failure, not a suite-wide hang.
            async with asyncio.timeout(10):
                with contextlib.suppress(asyncio.CancelledError):
                    await loop
        except TimeoutError:
            pytest.fail(
                "the cancelled cron loop did not settle within 10s after its "
                "savepoint release: the unwind is parked"
            )
        # The entry gate must have taken the "entered" branch: the conn is
        # still open, so the assertions below observe a real session and a
        # real ROLLBACK rather than a closure that would satisfy them for
        # the wrong reason.
        assert not rig.cron_conn.is_closed(), (
            "a cancel in the savepoint-release window closed the cron conn: its "
            "transaction had entered, so __aexit__ owns the rollback and the conn "
            "must be left to run()'s teardown"
        )
        assert not await rig.cron_conn_is_in_transaction(), (
            "the tick's transaction was left open by a cancel in the savepoint-release window"
        )
        assert await rig.cron_backend_state() != "idle in transaction", (
            "the backend stayed idle in transaction after a cancel in the savepoint-release window"
        )
        assert await rig.committed_jobs() == 0, (
            "the cancelled tick's enqueued fire survived it: the transaction did not roll back"
        )
        assert not await rig.cron_row_is_locked(), (
            f"the {_ACTOR!r} schedule row is still locked after a cancelled tick"
        )


async def test_a_sustained_stall_ends_the_dirty_session_and_hands_leadership_over(
    pg_dsn: str,
) -> None:
    """A stall that never clears must not hold the leaked session for the process's life.

    The deferred repair converges only when the transport answers: asyncpg parks
    every wire operation on the pending statement cancellation whose reply the
    stall is holding, so while the stall persists each repair fails and the
    leaked transaction - the incident's shape, holding the cron row's lock -
    would outlive every restart. The loop must give up on the conn instead,
    after a bounded number of failed repairs: end it (Postgres rolls the
    transaction back on disconnect), hand leadership over through the loop's
    own conn-state path, and stay alive as a loop, because a demotion recovers
    through re-election and a dead cron plane does not.
    """
    async with _cron_leader(pg_dsn) as rig:
        # The pid is unreadable once the conn is terminated (asyncpg drops its
        # protocol object), so capture the session's identity before the stall.
        pid = rig.cron_conn.get_server_pid()
        rig.relay.hold()
        loop = rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)

        async def session_gone() -> bool:
            # Through the dispatcher pool, which never traverses the relay:
            # the escalation's effect is server-side, so it must be observed
            # without the stalled conn's help.
            alive = await rig.deps.dispatcher_pool.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1"
                " AND datname = current_database()",
                pid,
            )
            return int(alive or 0) == 0

        # Two cut iterations, two period sleeps, and one further iteration
        # that ends the conn and hands leadership over: give the poll the
        # same headroom the file's other pins use rather than a tight
        # deadline that flakes.
        session_ended = await _poll(session_gone, timeout=10)

        assert await rig.cron_backend_state() != "idle in transaction", (
            "the cron backend sits idle in transaction for as long as the stall "
            "persists: the deferred repair cannot converge while the transport "
            "is stalled, so the loop must give up on the conn rather than hold "
            "the open transaction"
        )
        assert rig.cron_conn.is_closed(), (
            "the cron conn survived a sustained stall with its transaction state "
            "unknown: no round trip can repair it while the transport is stalled"
        )
        assert not rig.deps.leading(), (
            "the escalation ended the dirty conn but never handed leadership "
            "over: a leader that can no longer tick still owns the cron row"
        )
        assert session_ended, (
            "the cron conn's server-side session survived the escalation: the "
            "transaction it holds - and the cron row's lock - outlive the "
            "conn's local end"
        )
        assert not loop.done(), (
            "the escalation killed the cron loop instead of handing leadership "
            "over: a demotion is recoverable through re-election, a dead loop "
            "is not"
        )


async def test_a_transient_failure_inside_begin_still_repairs_before_reuse(
    pg_dsn: str,
) -> None:
    """A transient failure landing inside BEGIN must not leak the transaction either.

    The deadline family is not the only error class that can land inside
    asyncpg's Transaction.start(): a BEGIN round trip can be answered by any
    server error - 57P01 admin shutdown is a transient one the loop keeps the
    conn for - and the failed start leaves the same FAILED top-level
    transaction object behind. Without the repair, the next tick nests as a
    savepoint of a transaction nobody will end: the same leak, arriving
    through the loop's other-transient branch instead of the deadline branch.
    """
    async with _cron_leader(pg_dsn) as rig:
        rig.leader._cron_conn = cast(  # pyright: ignore[reportPrivateUsage]  # Why: the cron loop's connection is what the failing-BEGIN proxy must replace.
            "asyncpg.Connection",
            _FailingBeginConn(rig.cron_conn),
        )
        rig.start_loop()

        # The cadence's proof: the first tick's BEGIN fails transiently, and a
        # LATER tick must still fire the overdue schedule and commit it - only
        # a repaired conn can, because the failed start left the top-level
        # transaction object behind.
        async def fired() -> bool:
            return await rig.committed_jobs() > 0

        assert await _poll(fired, timeout=_RECOVERY_TIMEOUT_S), (
            "no fire from a later tick ever committed after a transient failure "
            "inside BEGIN: the conn was reused with its transaction state "
            "unknown, so every later tick nested as a savepoint of a "
            "transaction nobody ends"
        )
        assert not await rig.cron_conn_is_in_transaction(), (
            "the cron connection was left inside an open transaction after a "
            "transient failure inside BEGIN"
        )
        assert not await rig.cron_row_is_locked(), (
            f"the {_ACTOR!r} schedule row is still locked after a transient failure inside BEGIN"
        )
        server_now = await rig.deps.dispatcher_pool.fetchval("SELECT clock_timestamp()")
        assert await rig.committed_next_fire() > server_now, (
            "the schedule's next_fire_at never advanced durably after a "
            "transient failure inside BEGIN"
        )


async def test_a_flapping_stall_counts_the_cut_repairs_before_giving_up(
    pg_dsn: str,
    structlog_capture: list[Any],
) -> None:
    """A stall that flaps once must count its cut repairs before giving up.

    The sustained-stall pin drives the escalation through the repair that
    cannot even be sent: asyncpg raises the cancelled cancellation future at
    the next wire operation. This pin drives the other representation of the
    second consecutive failure - the transport answers long enough for the
    repair's own ROLLBACK to be in flight, and the stall returns to cut it -
    so the deadline branch's CONSECUTIVE COUNT is what escalates. The conn
    must still be ended and leadership handed over within a bound, and the
    counting must be visible as two consecutive cut repairs before the
    give-up.
    """
    async with _cron_leader(pg_dsn) as rig:
        # The pid is unreadable once the conn is terminated (asyncpg drops its
        # protocol object), so capture the session's identity before the stall.
        pid = rig.cron_conn.get_server_pid()
        rig.relay.hold()
        loop = rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        # Past the tick's deadline: BEGIN is outstanding and the first tick is
        # cut inside Transaction.start.
        await asyncio.sleep(_TICK_DEADLINE_S + 0.3)
        # Let the held reply through so the pending cancellation settles - the
        # next iteration's repair can then SEND its ROLLBACK. The stall is
        # re-armed on that ROLLBACK's own wire bytes, synchronously, so the
        # hold cannot be missed because the cancellation settle outlasted the
        # loop's period; the reply is held, so the deadline cuts the repair's
        # own statement rather than a preamble park.
        rig.relay.release()
        rig.relay.hold_from("ROLLBACK")
        assert await _await_cancellation_settled(rig.cron_conn, timeout=10), (
            "the cancelled statement never settled: the connection still parks "
            "every operation on the held cancellation reply, so the assertions "
            "after this wait would not observe what they claim"
        )

        async def session_gone() -> bool:
            # Through the dispatcher pool, which never traverses the relay.
            alive = await rig.deps.dispatcher_pool.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1"
                " AND datname = current_database()",
                pid,
            )
            return int(alive or 0) == 0

        async def demoted() -> bool:
            return not rig.deps.leading()

        # Two cut repairs, two period sleeps, one further iteration for the
        # deferred drop, and slack for the flapping handshake. The terminate
        # releases the session immediately, but the drop and the demotion
        # land one iteration later through the conn-state branch, so the
        # demotion - the chain's last observable - is what to wait for.
        assert await _poll(demoted, timeout=15), (
            "the escalation never handed leadership over through a flapping "
            "stall: a leader that can no longer tick still owns the cron row"
        )
        session_ended = await _poll(session_gone, timeout=5)

        assert await rig.cron_backend_state() != "idle in transaction", (
            "the cron backend sits idle in transaction through a flapping "
            "stall: the conn was cut mid-repair twice and never given up on, "
            "so the open transaction is held indefinitely"
        )
        assert rig.cron_conn.is_closed(), (
            "the cron conn survived cut repairs with its transaction state "
            "unknown: no round trip can finish repairing it while the stall "
            "returns"
        )
        assert session_ended, (
            "the cron conn's server-side session survived the escalation: the "
            "transaction it holds outlives the conn's local end"
        )
        assert not loop.done(), (
            "the escalation killed the cron loop instead of handing leadership "
            "over: a demotion is recoverable through re-election, a dead loop "
            "is not"
        )
        cut_counts = [
            e.get("consecutive") for e in structlog_capture if e["event"] == "cron-conn-repair-cut"
        ]
        assert cut_counts == [1, 2], (
            "the cut repairs were not counted as consecutive failures before "
            f"giving up on the conn: counted {cut_counts}"
        )
        escalated = [
            e.get("consecutive")
            for e in structlog_capture
            if e["event"] == "cron-conn-repair-escalated"
        ]
        assert escalated == [2], (
            "the give-up did not come from the deadline branch's consecutive "
            f"count: escalated with consecutive={escalated}"
        )


async def test_a_cancel_on_a_statement_inside_the_tick_body_settles_its_session(
    pg_dsn: str,
) -> None:
    """A shutdown cancellation inside the tick must settle the loop, not park its unwind.

    The RCA's cancellation pin cuts the loop with ``BEGIN`` outstanding, where
    ``tx_entered`` is False: no ``__aexit__`` runs, and the handler's
    ``close_conn_bounded`` ends the connection. This pin lands the cancellation
    one statement LATER - on the nested savepoint's ``RELEASE``, with the
    top-level transaction entered and the tick's advisory lock already taken -
    which is the window the production report's last statement came from. Here
    the entry gate takes the "entered" branch: ``__aexit__`` owns the rollback
    and the connection is left to ``run()``'s teardown.

    What the gate assumes is that the unwind can complete. It reaches
    ``Transaction.__rollback``, which clears ``_top_xact`` and then awaits
    ``Protocol.query``'s ``cancel_waiter`` preamble - a wait no timer bounds,
    because the connection's ``command_timeout`` is armed after that preamble
    and the cancelled tick's own deadline is already spent. If that wait does
    not resume, the loop task never settles: a shutting-down worker waits out
    its whole grace on this loop, and the session keeps the tick's open
    transaction (and the advisory lock that serialises cron ticks across pods)
    until the process is killed. Whether the wait resumes is asyncpg's
    cancellation bookkeeping, so this pin asserts the outcome the contract
    needs - the loop settles, and an entered transaction's connection is handed
    to the teardown rather than closed underneath it - instead of any
    particular internal state.

    No timer cuts anything here: the cancellation is the loop's only cut, so
    the outcome is not a race between the tick deadline and a statement
    timeout.
    """
    async with _cron_leader(pg_dsn) as rig:
        # Hold from the client->server direction, one statement past BEGIN: the
        # release reaches Postgres and its answer is what never comes back, so
        # the server keeps the transaction (and the tick's advisory lock) while
        # the client keeps waiting.
        rig.relay.hold_from("RELEASE SAVEPOINT")
        loop = rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        # The tick is now parked on the held answer, inside its transaction.
        await asyncio.sleep(0.2)
        state_at_cancel = await rig.cron_backend_state()
        assert state_at_cancel == "idle in transaction", (
            "the pin did not land its cancellation with the server-side "
            f"transaction open (backend state {state_at_cancel!r}), so this "
            "test is not observing the window it is about"
        )

        loop.cancel()
        try:
            settled = True
            # A bounded wait, so a parked unwind fails the pin instead of
            # hanging the suite: any correct unwind settles in milliseconds.
            async with asyncio.timeout(5):
                with contextlib.suppress(asyncio.CancelledError):
                    await loop
        except TimeoutError:
            settled = False
        finally:
            # Let the parked unwind finish before the rig tears the connection
            # down, so the test observes (rather than causes) the state below.
            rig.relay.release()
            await asyncio.sleep(0.5)

        assert settled, (
            "a cancellation that landed on a statement inside the tick never "
            "settled: the unwind parked inside __aexit__ on the answer the "
            "transport was holding, so nothing in the loop - the entry-gated "
            "close, the repair, the escalation - was ever reached, and a "
            "shutdown would wait out its whole grace on this loop"
        )
        # The entered branch's half of the contract: the rollback did not reach
        # the server (the transport withheld the answer), so the connection
        # must be left OPEN for run()'s teardown to close - a close here would
        # be the arm the entry gate exists to avoid, and would silence the
        # ordinary-unwind pin in the same window. The teardown is where a
        # production worker ends this session; this rig runs the loop alone, so
        # what it can assert is that the connection was handed over rather than
        # silently dropped.
        assert not rig.cron_conn.is_closed(), (
            "a cancellation inside the tick closed the cron conn: its "
            "transaction had entered, so the connection belongs to run()'s "
            "teardown, not to the loop's own unwind"
        )


async def test_a_transient_tick_failure_does_not_silence_the_cron_commit_gate(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[Any],
) -> None:
    """The deferred repair must not cost the cron plane its commit-gated telemetry.

    A tick that fails transiently records the conn dirty, and the next iteration
    repairs it with ``Connection.reset()``. That reset's query ends in
    ``UNLISTEN *`` and ``Connection._reset`` clears asyncpg's client-side
    listener registry, while the commit gate's own bookkeeping
    (``_confirmed_listening``, keyed by backend pid) survives it. The confirmed
    pid only skips the ``remove_listener`` - a no-op against the cleared
    registry anyway - and the gate's unconditional ``add_listener`` finds the
    channel absent from that registry and re-issues a real ``LISTEN``. If the
    re-arm instead trusted the registration the reset removed, every fire from
    then on would commit while its telemetry - the "cron fired" line, strikes,
    auto-disables - never ran: a degraded outcome reporting success, which is
    the failure the gate exists to prevent. The sequence here is the reachable one: a fire that commits and so
    confirms the pid, a transient failure that dirties the conn, and a fire on
    the repaired conn whose emission must still arrive.
    """
    from taskq.worker import leader as leader_mod

    async with _cron_leader(pg_dsn) as rig:
        real_tick = leader_mod.tick_cron  # pyright: ignore[reportPrivateImportUsage]  # Why: the loop calls the tick through this module attribute, so replacing it there is what drives a transient failure through the real loop.
        calls = 0

        async def flaky_tick(*args: object, **kwargs: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 2:
                # Re-arm the schedule from another session so the tick after
                # the repair has work, then fail the way a deadlock or a
                # serialization failure does: inside the transaction, which
                # rolls back.
                await rig.deps.dispatcher_pool.execute(
                    f'UPDATE "{rig.schema}".cron_schedules '  # noqa: S608  # Why: the schema name is generated in this test and migration-validated; identifiers cannot be bound.
                    "SET next_fire_at = clock_timestamp() - interval '2 hours', "
                    "last_fired_at = NULL WHERE id = $1",
                    rig.schedule_id,
                )
                raise asyncpg.SerializationError("simulated 40001 inside the tick")
            return await real_tick(*args, **kwargs)  # type: ignore[no-any-return]

        monkeypatch.setattr(leader_mod, "tick_cron", flaky_tick)
        rig.start_loop()

        async def both_fires_committed() -> bool:
            return await rig.committed_jobs() >= 2

        assert await _poll(both_fires_committed, timeout=_RECOVERY_TIMEOUT_S), (
            "the tick after the transient failure never fired: the cadence did "
            "not survive the repair"
        )
        # State-anchored, not a fixed settle: the commit-gate emission rides
        # the COMMIT's own NOTIFY, so wait for one "cron fired" line per
        # committed fire rather than betting that 0.5s is enough for the
        # notification to dispatch.
        committed = await rig.committed_jobs()

        async def telemetry_arrived() -> bool:
            events = [e for e in structlog_capture if e["event"] == "cron fired"]
            return len(events) >= committed

        assert await _poll(telemetry_arrived, timeout=5.0), (
            "a fire that committed after the repair reported nothing: the "
            "reset's UNLISTEN cleared the session's LISTEN and the gate's "
            "re-arm did not restore it, so the emission was awaited by a "
            "notification the server no longer delivers"
        )
        repaired = [e for e in structlog_capture if e["event"] == "cron-conn-repaired"]
        assert repaired, (
            "the deferred repair never ran, so this pin did not exercise the "
            "reset's effect on the commit gate"
        )
        fired_events = [e for e in structlog_capture if e["event"] == "cron fired"]
        assert len(fired_events) == committed, (
            "a fire that committed after the repair reported nothing: the "
            "reset's UNLISTEN cleared the session's LISTEN and the gate's "
            "re-arm did not restore it, so the emission was awaited by a "
            f"notification the server no longer delivers ({len(fired_events)} "
            f'"cron fired" events for {committed} committed fires)'
        )


async def test_a_rollback_on_the_repaired_conn_does_not_silence_the_commit_gate(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    structlog_capture: list[Any],
) -> None:
    """A post-repair tick that ROLLS BACK must not cost the gate permanently.

    The deferred repair's ``reset()`` clears both the server's ``LISTEN`` and
    asyncpg's client registry, while the gate's pid-keyed proof
    (``_confirmed_listening``) survives it. The next tick then takes the cheap
    arm (skip ``remove_listener``) and its unconditional ``add_listener``
    re-issues a real ``LISTEN`` -- but if THAT tick rolls back, the server
    discards the registration again while the client registry and the proof
    both keep it. From then on ``add_listener`` would be the silent no-op the
    gate's docstring warns about, and every fire would commit with its
    telemetry missing.

    What makes the sequence safe is the record: the rollback is a failed tick,
    so the conn is dirtied and the NEXT iteration's repair resets it again,
    forcing the next tick's ``add_listener`` to issue a real ``LISTEN``. This
    pin holds that dependency: if a future change lets a failed post-repair
    tick leave the conn clean, the committed fire after it reports nothing.
    """
    from taskq.worker import leader as leader_mod

    async with _cron_leader(pg_dsn) as rig:
        real_tick = leader_mod.tick_cron  # pyright: ignore[reportPrivateImportUsage]  # Why: the loop calls the tick through this module attribute, so replacing it there drives the sequence through the real loop.
        calls = 0
        rolled_back_after_the_arm = False

        async def flaky_tick(*args: object, **kwargs: object) -> int:
            nonlocal calls, rolled_back_after_the_arm
            calls += 1
            if calls == 2:
                # Re-arm from another session (so the post-repair tick has
                # work) and fail BEFORE the tick arms the gate: the first
                # failure, whose repair is the one under test.
                await rig.deps.dispatcher_pool.execute(
                    f'UPDATE "{rig.schema}".cron_schedules '  # noqa: S608  # Why: the schema name is generated in this test and migration-validated; identifiers cannot be bound.
                    "SET next_fire_at = clock_timestamp() - interval '2 hours', "
                    "last_fired_at = NULL WHERE id = $1",
                    rig.schedule_id,
                )
                raise asyncpg.SerializationError("simulated 40001 before the arm")
            fired = await real_tick(*args, **kwargs)  # type: ignore[no-any-return]
            if calls == 3:
                # The first tick on the repaired conn: it fired and ARMED the
                # gate (a real LISTEN inside this transaction), then the
                # transaction rolls back, discarding the registration
                # server-side while the client-side registry keeps it.
                rolled_back_after_the_arm = True
                raise asyncpg.SerializationError("simulated 40001 after the arm")
            return fired

        monkeypatch.setattr(leader_mod, "tick_cron", flaky_tick)
        rig.start_loop()

        # State-anchored, not call-count-anchored: a cold first tick that its
        # own deadline cuts just shifts the sequence earlier, so the pin reads
        # what the loop and the database actually recorded. The decisive
        # record is a "cron fired" line AFTER the last repair: the rollback
        # leaves the conn dirty, so the post-rollback commit is preceded by
        # one more repair - an event after it can only be that commit's.
        def fired_after_the_last_repair() -> bool:
            names = [e["event"] for e in structlog_capture]
            last_repair = max(
                (i for i, name in enumerate(names) if name == "cron-conn-repaired"),
                default=-1,
            )
            return last_repair >= 0 and "cron fired" in names[last_repair + 1 :]

        async def sequence_completed() -> bool:
            return (
                rolled_back_after_the_arm
                and await rig.committed_jobs() >= 1
                and fired_after_the_last_repair()
            )

        assert await _poll(sequence_completed, timeout=_RECOVERY_TIMEOUT_S), (
            "the tick after the post-repair rollback never fired, or its commit "
            "reported nothing: the cadence or the commit gate did not survive "
            "the rollback+repair sequence"
        )
        committed = await rig.committed_jobs()
        fired_events = [e for e in structlog_capture if e["event"] == "cron fired"]
        assert len(fired_events) == committed, (
            "the committed fire after a post-repair rollback reported nothing: "
            "the rolled-back tick left the client's listener registry and the "
            "gate's confirmed pid describing a LISTEN the server no longer "
            f"holds ({len(fired_events)} 'cron fired' events for {committed} "
            "committed fires) - the failed tick must re-dirty the conn so the "
            "next repair forces a real LISTEN"
        )


async def test_a_deadline_cut_on_a_statement_inside_the_tick_body_still_ends_the_session(
    pg_dsn: str,
) -> None:
    """A deadline cut on a statement inside the tick must still end the session.

    The shutdown-cancellation sibling of this window settles because the
    cancellation lands while the iteration's deadline is still live, so a
    timer can cut the unwind's parked ROLLBACK. The deadline cut is the
    harder half: the tick body's own scope has just expired when the
    transaction's ``__aexit__`` parks its ROLLBACK on the held answer, so
    only the reserved unwind share of the iteration's deadline is left to
    cut it. Without that bound the loop parks inside its own unwind forever
    - the deadline branch, the repair and the escalation are never reached -
    and the session, its open transaction and the cron locks it holds,
    outlives the stall. The relay is never released, so a loop that needs
    the transport to answer before it gives up on the conn has nothing to
    wait for.
    """
    async with _cron_leader(pg_dsn) as rig:
        # The pid is unreadable once the conn is terminated, so capture the
        # session's identity before the stall.
        pid = rig.cron_conn.get_server_pid()
        # Hold from the client->server direction, one statement past BEGIN: the
        # release reaches Postgres and its answer is what never comes back, so
        # the server keeps the transaction (and the tick's advisory lock) while
        # the client keeps waiting.
        rig.relay.hold_from("RELEASE SAVEPOINT")
        loop = rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        await asyncio.sleep(_TICK_DEADLINE_S + 0.3)
        # The setup guard: the cut must have landed with the server-side
        # transaction open, or this test is not observing its window.
        state_at_cut = await rig.cron_backend_state()
        assert state_at_cut == "idle in transaction", (
            "the pin did not land its cut with the server-side transaction "
            f"open (backend state {state_at_cut!r}), so this test is not "
            "observing the window it is about"
        )

        async def session_gone() -> bool:
            # Through the dispatcher pool, which never traverses the relay.
            alive = await rig.deps.dispatcher_pool.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = $1"
                " AND datname = current_database()",
                pid,
            )
            return int(alive or 0) == 0

        # The unwind share cuts the parked ROLLBACK, then the repair that
        # cannot even be sent escalates and the conn is ended: a cut
        # iteration, one spurious-cancel repair, one further iteration for
        # the drop. Deterministic rather than a timer race - verified by
        # repeated runs before this pin was committed.
        session_ended = await _poll(session_gone, timeout=10)

        assert await rig.cron_backend_state() != "idle in transaction", (
            "a deadline cut on a statement inside the tick left the backend "
            "idle in transaction for as long as the stall lasts: the unwind "
            "parked inside __aexit__ on the held answer, so the repair and "
            "the escalation were never reached and the transaction - with "
            "the cron locks it holds - outlives the loop"
        )
        assert rig.cron_conn.is_closed(), (
            "the cron conn survived a cut inside its tick body with the "
            "transaction state unknown: no round trip can repair it while "
            "the transport is stalled"
        )
        assert not rig.deps.leading(), (
            "the escalation ended the dirty conn but never handed leadership "
            "over: a leader that can no longer tick still owns the cron row"
        )
        assert session_ended, (
            "the cron conn's server-side session survived the escalation: the "
            "transaction it holds outlives the conn's local end"
        )
        assert not loop.done(), (
            "the escalation killed the cron loop instead of handing leadership "
            "over: a demotion is recoverable through re-election, a dead loop "
            "is not"
        )


async def test_a_cut_on_a_statement_inside_the_tick_body_does_not_park_the_transaction(
    pg_dsn: str,
) -> None:
    """A stall that cuts a statement INSIDE the tick must not hold the session.

    The sustained-stall pin cuts the tick inside ``Transaction.start()``, where
    ``tx_entered`` is False. This pin lands the cut one statement LATER - on the
    nested savepoint's ``RELEASE``, with the top-level transaction entered and
    the tick's advisory lock taken - which is the window the production report's
    last statement came from. The entry gate takes the "entered" branch there on
    the reasoning that ``__aexit__`` owns the rollback, so nothing closes the
    connection; the iteration's reserved unwind share is what must cut a ROLLBACK
    parked on the answer the stall is holding.

    The invariant is the change's own for a sustained stall: the session - and
    the transaction it sits in, holding the cron advisory lock - must not outlive
    the stall. The relay is never released until the assertions have run.
    """
    async with _cron_leader(pg_dsn) as rig:
        # The pid is unreadable once the conn is terminated, so capture the
        # session's identity before the stall.
        pid = rig.cron_conn.get_server_pid()
        # Hold from the client->server direction, one statement past BEGIN: the
        # release reaches Postgres and its answer is what never comes back, so
        # the server keeps the transaction (and the tick's advisory lock) while
        # the client keeps waiting.
        rig.relay.hold_from("RELEASE SAVEPOINT")
        rig.start_loop()
        await asyncio.wait_for(rig.relay.held_any.wait(), timeout=10)
        # Past the tick's deadline: the unwind share cuts the parked unwind.
        await asyncio.sleep(_TICK_DEADLINE_S + 0.4)
        # The setup guard: the cut must have landed with the server-side
        # transaction open, or this test is not observing its window.
        state_at_cut = await rig.cron_backend_state()
        assert state_at_cut == "idle in transaction", (
            "the pin did not land its cut with the server-side transaction "
            f"open (backend state {state_at_cut!r}), so this test is not "
            "observing the window it is about"
        )

        async def leaked_transaction_ended() -> bool:
            # Through the dispatcher pool, which never traverses the relay: the
            # escalation's effect is server-side, so it must be observed
            # without the stalled conn's help.
            row = await rig.deps.dispatcher_pool.fetchrow(
                "SELECT state FROM pg_stat_activity WHERE pid = $1"
                " AND datname = current_database()",
                pid,
            )
            return row is None or str(row["state"]) != "idle in transaction"

        try:
            ended = await _poll(leaked_transaction_ended, timeout=12)
            assert ended, (
                "a stall that cut a statement inside the tick left the backend "
                "idle in transaction for as long as the stall lasts: the "
                "unwind parked inside __aexit__ on the same held reply, so the "
                "reserve and the escalation were never reached and the "
                "transaction - with the advisory lock it holds - outlives the "
                "loop"
            )
        finally:
            # Settle the parked unwind so the rig's teardown has a live task to
            # cancel rather than a permanently held reply.
            rig.relay.release()
            await asyncio.sleep(0.5)
