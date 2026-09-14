"""Unit tests for MaintenanceLeader — pure-Python, no PG required.

Covers election, watchdog, sweep-loop gating, pg_notify, prune/expiry
scheduling, and retention-config helpers against InMemoryBackend.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase, JobId, JobRow
from taskq.backend._sweeps import SweepBatchSizer
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.settings import WorkerSettings
from taskq.testing.assertions import (
    wait_for,
    wait_for_condition,
    wait_for_job_status,
    wait_for_leader,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._leader_shared import (
    _DB_NOW_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the fake must answer the exact DB-clock query the prune/expiry code issues; importing it keeps the two from drifting apart.
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import (
    MaintenanceLeader,
    _build_retention_per_status,
    _load_actor_retention_overrides,
    _schedule_utc_to_cron,
    archive_expiry_sweep,
    prune_terminal_jobs,
)


def _next_minute_cron() -> str:
    """Return a 5-field cron expression that fires at the next UTC minute boundary.

    E.g. if now is 10:45:30 UTC, returns '46 10 * * *'.
    Maximum wait: 60 seconds.
    """
    now = datetime.now(UTC)
    next_minute = now.minute + 1 if now.minute < 59 else 0
    next_hour = now.hour if now.minute < 59 else (now.hour + 1) % 24
    return f"{next_minute} {next_hour} * * *"


def _as_dict(attrs: object) -> dict[str, object]:
    """Convert OTel Attributes to plain dict for test assertions."""
    return dict(attrs)  # type: ignore[arg-type]  # Why: OTel Attributes is Mapping[str, AttributeValue]; dict(attrs) works at runtime but pyright infers wrong overload.


async def _stop_after_tick(
    task: asyncio.Task[object],
    shutdown: asyncio.Event,
    *,
    delay: float = 0.05,
) -> None:
    """Wait briefly then cancel *task* and suppress CancelledError."""
    await asyncio.sleep(delay)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


pytestmark = pytest.mark.asyncio

# ── Test doubles ──────────────────────────────────────────────────────────


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


def _is_server_clock_read(sql: str) -> bool:
    """Whether *sql* asks Postgres for its own clock.

    Postgres answers such a statement with a timestamp unconditionally, so a
    double that hands back its generic ``fetchval_result`` here is lying about
    the row shape: the leader sweeps read the server clock through the same
    connection they take the advisory lock on, and a double configured with
    ``fetchval_result=True`` for the lock was returning that bool where the
    real database returns a datetime.
    """
    return "clock_timestamp()" in sql


class FakeConn:
    """Lightweight asyncpg.Connection stand-in with fetchval + execute recording."""

    def __init__(
        self,
        *,
        fetchval_result: object = None,
        on_fetchval: Callable[[], None] | None = None,
        on_execute: Callable[[], None] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._closed = False
        self.close_calls = 0
        self._fetchval_result = fetchval_result
        self._on_fetchval = on_fetchval
        self._on_execute = on_execute
        self._on_close = on_close
        # Hang-gate + terminate tracking for bounded-close tests (mirrors the
        # _FakeConn conventions in tests/test_worker_deps_teardown.py):
        # clear close_wait to make close() block forever (dead PG).
        self.close_wait = asyncio.Event()
        self.close_wait.set()  # close() completes instantly by default
        self.terminated = False
        # Why an event alongside the flag: the flag is the assertion
        # surface; the event is the WAIT surface — the watchdog closes
        # leader-owned conns on its own task, and a test that needs
        # "closed" can await this instead of polling a fixed interval
        # that races the close under load (same convention as the fakes
        # in tests/test_reload_credentials.py).
        self.closed_event = asyncio.Event()

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if self._on_fetchval is not None:
            self._on_fetchval()
        if _is_server_clock_read(sql):
            return datetime.now(UTC)
        return self._fetchval_result

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._on_execute is not None:
            self._on_execute()
        return "UPDATE 1"

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def fetch(self, sql: str, *args: object) -> Sequence[object]:
        return []

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self._closed = True
        self.closed_event.set()
        if self._on_close is not None:
            self._on_close()

    def terminate(self) -> None:
        self.terminated = True
        self._closed = True
        self.closed_event.set()
        self.close_wait.set()  # aborts any in-flight close() wait

    def is_closed(self) -> bool:
        return self._closed

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    def is_in_transaction(self) -> bool:
        """Model a caller-owned connection with an open transaction."""
        return True


class FakePool:
    """Lightweight asyncpg.Pool stand-in that tracks acquire and connections."""

    def __init__(
        self,
        *,
        fail_acquire_with: BaseException | None = None,
    ) -> None:
        self._fail_acquire_with = fail_acquire_with
        self.acquire_count = 0
        self._conns: list[FakeConn] = []

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: asyncpg.Pool.acquire signature takes timeout; FakePool mirrors it.
        self.acquire_count += 1
        if self._fail_acquire_with is not None:
            raise self._fail_acquire_with
        conn = FakeConn()
        self._conns.append(conn)
        yield conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.execute_calls)
        return result

    @property
    def fetchval_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.fetchval_calls)
        return result


class _PoolWithFixedConn(FakePool):
    """FakePool that always yields a pre-built connection on acquire().

    Used by tests that need the pool-acquired connection to have custom
    fetchval/fetch/execute behaviour (e.g. _FakeConnForPrune subclasses).
    """

    def __init__(self, conn: FakeConn) -> None:
        super().__init__()
        self._fixed_conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        self.acquire_count += 1
        yield self._fixed_conn

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.execute_calls)
        return result

    @property
    def fetchval_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        result: list[tuple[str, tuple[object, ...]]] = []
        for conn in self._conns:
            result.extend(conn.fetchval_calls)
        return result


# ── Factories ─────────────────────────────────────────────────────────────


def _worker_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn}
    for key, value in overrides.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps(
    *,
    dispatcher_pool: FakePool | None = None,
    heartbeat_pool: FakePool | None = None,
    worker_pool: FakePool | None = None,
    leader_conn: FakeConn | None = None,
    is_leader: bool = False,
    heartbeat_interval: float = 0.5,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        LOCK_LEASE="2.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=dispatcher_pool or FakePool(),  # type: ignore[arg-type]  # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=heartbeat_pool or FakePool(),  # type: ignore[arg-type]
        worker_pool=worker_pool or FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn or FakeConn(),  # type: ignore[arg-type]  # Why: FakeConn is a drop-in for asyncpg.Connection in unit tests.
    )
    if is_leader:
        deps.is_leader.set()
    return deps


async def _make_leader(
    *,
    leader_conn: FakeConn | None = None,
    dispatcher_pool: FakePool | None = None,
    is_leader: bool = False,
    monkeypatch: Any | None = None,
) -> tuple[MaintenanceLeader, WorkerDeps, InMemoryBackend, FakeConn, FakePool, asyncio.Event]:
    """Construct MaintenanceLeader wired with fake deps and InMemoryBackend.

    Returns (leader, deps, backend, leader_conn, dispatcher_pool, shutdown).
    """
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    fake_leader_conn = leader_conn or FakeConn()
    fake_dp = dispatcher_pool or FakePool()
    deps = _make_deps(
        dispatcher_pool=fake_dp,
        leader_conn=fake_leader_conn,
        is_leader=is_leader,
        heartbeat_interval=0.01,
    )
    worker_id = new_uuid()
    leader = MaintenanceLeader(deps, worker_id, backend, clock=clock)

    # Ensure open_dedicated_conn is mocked to avoid real asyncpg.connect
    if monkeypatch is not None:
        import taskq.worker.leader as leader_mod

        async def fake_open_conn(
            dsn: str,
            *,
            label: str = "",
            apply_keepalive: bool = True,
            command_timeout: float | None = None,
        ) -> FakeConn:
            return FakeConn()

        monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open_conn)

    shutdown = asyncio.Event()
    return leader, deps, backend, fake_leader_conn, fake_dp, shutdown


# ── Election win sets is_leader ────────────────────────────────────


async def test_election_win_sets_is_leader(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]  # Why: pytest monkeypatch fixture type is only available with pytest-stub; using Any for test ergonomics.
    """Election win: fetchval returns True → is_leader set, monitor opened,
    UPSERT runs, counter incremented, INFO log emitted."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    counter_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod

    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: counter_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]  # Why: test-only instrumentation to observe OTel counter calls.

    task = asyncio.create_task(leader._election_loop(shutdown))
    # is_leader IS an asyncio.Event — a bounded event wait (never a
    # sleep-poll that races the election under load).
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert any("maintenance_leader" in sql for sql, _ in leader_conn.execute_calls)
    assert counter_calls == [(1, {})]


# ── Election loss does not set is_leader ───────────────────────────


async def test_election_loss_does_not_set_is_leader(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Election loss: fetchval returns False → is_leader stays clear,
    no monitor opened, INFO log with kind='leader_retry', counter incremented."""
    # Additive event on the double: set exactly where the election's lock
    # probe is entered, so the test waits for the attempt instead of
    # sleeping and hoping the loop reached it.
    election_attempted = asyncio.Event()
    leader_conn = FakeConn(fetchval_result=False, on_fetchval=election_attempted.set)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    counter_calls: list[tuple[int, dict[str, object]]] = []
    failure_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod

    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: counter_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]
    otel_mod._leader_election_failures.add = lambda amount, attrs=None: failure_calls.append(
        (int(amount), _as_dict(attrs or {}))
    )  # type: ignore[method-assign]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for(election_attempted)
    shutdown.set()
    await task

    assert not deps.is_leader.is_set()
    assert leader._leader_monitor_conn is None
    assert counter_calls == [(1, {})]
    assert failure_calls == [(1, {})]


# ── Watchdog failure clears is_leader (parametrized) ────────────────


@pytest.mark.parametrize(
    "exc",
    [
        asyncpg.PostgresConnectionError("connection lost"),
        asyncpg.InterfaceError("connection is closed"),
        OSError(104, "Connection reset by peer"),
    ],
)
async def test_watchdog_failure_clears_is_leader(exc: BaseException) -> None:
    """Watchdog failure on SELECT 1 clears is_leader, closes both
    connections, logs WARNING with kind='leadership_lost', continues outer loop."""
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)

    class FailingFakeConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise exc

    failing_monitor = FailingFakeConn()
    _clk = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        InMemoryBackend(clock=_clk),
        clock=_clk,
    )
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]  # Why: FakeConn is not asyncpg.Connection; assignment of test double is intentional.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if failing_monitor._closed and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    assert not deps.is_leader.is_set()
    assert failing_monitor._closed
    assert deps.leader_conn is None
    shutdown.set()
    deps.is_leader.set()  # unblock the wait() then exit via shutdown check
    await task


# ── Watchdog continues after error and re-election ──────────────────


async def test_watchdog_continues_after_error_and_reelection(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """After watchdog failure clears is_leader, re-election reactivates
    the watchdog and SELECT 1 runs on the new connection."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)

    # Simulate post-watchdog-failure state: leader_conn is None (it was closed)
    deps.leader_conn = None

    # Mock open_dedicated_conn to return a new FakeConn for leader_conn
    new_leader_conn = FakeConn(fetchval_result=True)
    import taskq.worker.leader as leader_mod

    open_calls: list[str] = []

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        open_calls.append(label)
        if label == "leader":
            return new_leader_conn
        return FakeConn()

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    elect_task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await elect_task

    assert deps.is_leader.is_set()
    assert deps.leader_conn is new_leader_conn


# ── deps.leader_conn replaced after watchdog ────────────────────────


async def test_leader_conn_replaced_after_watchdog(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """deps.leader_conn is replaced after watchdog failure."""
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    # Simulate watchdog failure: clear is_leader, null leader_conn
    deps.leader_conn = None
    deps.is_leader.clear()

    # Mock open_dedicated_conn to return a sentinel new connection
    sentinel_conn = FakeConn(fetchval_result=True)
    import taskq.worker.leader as leader_mod

    async def fake_open(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        return sentinel_conn

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.leader_conn is sentinel_conn
    assert deps.leader_conn is not original_leader_conn


async def test_watchdog_reopen_uses_leader_conn_factory_not_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When deps.leader_conn_factory is set, the watchdog reopens through it —
    never through open_dedicated_conn's raw DSN path. Regression test for a bug
    where the watchdog hardcoded pg_dsn_direct, bypassing WorkerConnections
    entirely (broken for AAD/AWS/Vault deployments with no DSN configured)."""
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    # Simulate watchdog failure: clear is_leader, null leader_conn
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls: list[None] = []
    factory_conns: list[
        object
    ] = []  # Why: identity bag — members are compared with `is`/`in` against deps.leader_conn (asyncpg.Connection), so element typing as FakeConn makes pyright report no-overlap.

    async def fake_factory() -> FakeConn:
        factory_calls.append(None)
        conn = FakeConn(fetchval_result=True)
        factory_conns.append(conn)
        return conn

    deps.leader_conn_factory = fake_factory  # type: ignore[assignment]

    # open_dedicated_conn must NOT be called when a factory is set — fail loud
    # if the watchdog falls back to the DSN path instead of the factory.
    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(
            f"open_dedicated_conn called with dsn={dsn!r} label={label!r} — "
            "leader_conn_factory should have been used instead"
        )

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win: is_leader is the event itself.
    await wait_for_leader(deps)
    shutdown.set()
    await task

    # leader_conn_factory backs leader_conn AND the leader's other dedicated
    # connections (leader_monitor_conn, cron_conn) — all must route through
    # it, never through the raw-DSN open_dedicated_conn path.
    assert len(factory_calls) >= 1
    assert deps.leader_conn in factory_conns


async def test_reload_credentials_rebuilds_leader_monitor_and_cron_conns(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """SIGHUP reload (reload_credentials nulling leader_conn) causes the
    election loop's re-election cascade to rebuild leader_monitor_conn and
    cron_conn through leader_conn_factory too — not just leader_conn itself.

    reload_credentials() only directly touches deps.leader_conn (closing it
    and setting it to None so the watchdog/election loop reopens it — see
    deps.py's reload_credentials docstring). This test verifies the
    downstream effect: _election_loop's re-election path, triggered by
    leader_conn becoming None while is_leader is still set, also rebuilds
    the leader's other dedicated connections (_leader_monitor_conn,
    _cron_conn) via the SAME leader_conn_factory — so a hot-reloaded leader
    doesn't keep querying with monitor/cron connections opened under a
    stale credential until they separately fail.
    """
    original_leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original_leader_conn,
        monkeypatch=monkeypatch,
    )

    factory_calls: list[str] = []

    async def fake_factory() -> FakeConn:
        factory_calls.append("factory")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = fake_factory  # type: ignore[assignment]

    # Drive the election loop through a REAL election first (is_leader=True
    # at construction is an artificial state _election_loop's re-election
    # path never produces — genuine leadership always flows through
    # `if got_lock:`, which is what populates _leader_monitor_conn /
    # _cron_conn in the first place).
    task = asyncio.create_task(leader._election_loop(shutdown))
    # The election loop opens BOTH dedicated conns before it sets
    # is_leader (leader.py: UPSERT → monitor conn → cron conn → set), so
    # the is_leader event wait subsumes polling the conn attributes —
    # and it is a bounded wait on the event itself, never a sleep-poll.
    await wait_for_leader(deps)
    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert leader._cron_conn is not None
    old_monitor_conn = leader._leader_monitor_conn
    old_cron_conn = leader._cron_conn
    factory_calls.clear()  # only count calls from the reload onward

    # Simulate what reload_credentials does to leader_conn on SIGHUP: close
    # it and null it while is_leader remains set (deps.py:599-606).
    deps.leader_conn = None

    # The rebuilt conns are plain attributes the election loop assigns —
    # no event to wait on, so a bounded, deadline-based poll.
    await wait_for_condition(
        lambda: (
            leader._leader_monitor_conn is not old_monitor_conn
            and leader._cron_conn is not old_cron_conn
        ),
        description="the re-election cascade must rebuild leader_monitor_conn and cron_conn",
    )
    shutdown.set()
    await task

    assert leader._leader_monitor_conn is not old_monitor_conn
    assert leader._cron_conn is not old_cron_conn
    # Both the leader_conn reopen and the monitor/cron reopens went through
    # leader_conn_factory (3 calls: leader_conn, leader_monitor_conn, cron_conn).
    assert len(factory_calls) == 3


# ── Sweep loops gate on is_leader ──────────────────────────────────


class StubPool(FakePool):
    """Pool that records acquire calls for behavioural assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.acquire_called = False

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109
        self.acquire_called = True
        async with super().acquire(timeout=timeout) as conn:
            yield conn


async def test_sweep_loops_gate_on_is_leader() -> None:
    """Sweep loops only run when is_leader; both sweeps 1 and 2
    transition jobs; sweep 4 is skipped on InMemoryBackend."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    now = clock.now()

    expired_job_id = new_job_id()
    deadline_job_id = new_job_id()
    worker_id = new_uuid()

    _insert_job(
        backend,
        expired_job_id,
        status="running",
        lock_expires_at=now - timedelta(seconds=1),
        attempt=1,
        max_attempts=3,
        locked_by_worker=worker_id,
        now=now,
    )
    _insert_job(
        backend,
        deadline_job_id,
        status="scheduled",
        schedule_to_close=now - timedelta(seconds=1),
        locked_by_worker=worker_id,
        now=now,
    )

    spy_pool = StubPool()
    deps = _make_deps(
        is_leader=False,
        heartbeat_interval=0.01,
        dispatcher_pool=spy_pool,  # type: ignore[arg-type]  # Why: StubPool extends FakePool for test assertions.
    )

    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)
    shutdown = asyncio.Event()

    # Run _sweep_loop with is_leader=False → gates prevent entry
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    await _stop_after_tick(task, shutdown, delay=0.05)

    expired_row = await backend.get(expired_job_id)
    deadline_row = await backend.get(deadline_job_id)
    assert expired_row is not None
    assert deadline_row is not None
    assert expired_row.status == "running"
    assert deadline_row.status == "scheduled"

    # Run _sweep_loop with is_leader=True → both sweeps fire
    deps.is_leader.set()
    shutdown_2 = asyncio.Event()
    task_2 = asyncio.create_task(leader._sweep_loop(shutdown_2))
    # Bounded waits on the sweeps' own observable (job status), never a
    # fixed 0.05s hoping the tick completed under load.
    await wait_for_job_status(backend, expired_job_id, "pending")
    await wait_for_job_status(backend, deadline_job_id, "failed")
    await _stop_after_tick(task_2, shutdown_2, delay=0.0)

    expired_row = await backend.get(expired_job_id)
    deadline_row = await backend.get(deadline_job_id)
    assert expired_row is not None
    assert deadline_row is not None
    assert expired_row.status == "pending"
    assert expired_row.locked_by_worker is None
    assert expired_row.lock_expires_at is None
    assert deadline_row.status == "failed"
    assert deadline_row.finished_at is not None
    assert not spy_pool.acquire_called  # sweep 4 skipped


# ── Helper: insert a job row into InMemoryBackend ────────────────────────


def _insert_job(
    backend: InMemoryBackend,
    job_id: JobId,
    *,
    status: str = "running",
    lock_expires_at: datetime | None = None,
    schedule_to_close: datetime | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
    locked_by_worker: UUID | None = None,
    cancel_phase: CancelPhase = CancelPhase.NONE,
    now: datetime | None = None,
) -> None:
    _now = now if now is not None else datetime.now(UTC)
    row = JobRow(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        status=status,  # type: ignore[arg-type]  # Why: Literal not narrowed for dynamic status values from test helper.
        priority=0,
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
        schedule_to_close=schedule_to_close,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=_now,
        scheduled_at=_now - timedelta(seconds=10),
        started_at=_now - timedelta(seconds=5),
        finished_at=None,
        last_heartbeat_at=_now - timedelta(seconds=2),
        locked_by_worker=locked_by_worker,
        lock_expires_at=lock_expires_at,
        cancel_requested_at=None,
        cancel_phase=cancel_phase,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        identity_key=None,
        fairness_key=None,
        metadata={},
        tags=(),
    )
    backend._jobs[job_id] = row  # type: ignore[reportPrivateUsage]  # Why: _jobs is internal storage accessed by test helper for direct state setup.


# ── pg_notify issued after non-zero promotion count ────────────────


async def test_pg_notify_issued_after_promotion(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When scheduled_to_pending returns count > 0, exactly one
    pg_notify execute call fires on dispatcher_pool connection."""
    leader, deps, backend, _lc, fake_dp, shutdown = await _make_leader(
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    backend.scheduled_to_pending = lambda **kw: asyncio.sleep(0, result=3)  # type: ignore[method-assign]  # Why: async stub returning int for test setup.
    deps.is_leader.set()

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        # The notify execute lands on a pool-internal conn — no event to
        # wait on, so a bounded, deadline-based poll on the recording.
        await wait_for_condition(
            lambda: bool(fake_dp.execute_calls),
            description="the wake loop must issue its post-promotion pg_notify",
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    pg_notify_calls = [c for c in fake_dp.execute_calls if "pg_notify" in c[0]]
    assert len(pg_notify_calls) == 1


# ── pg_notify NOT issued when count is 0 ────────────────────────────


async def test_pg_notify_not_issued_when_zero(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """When scheduled_to_pending returns 0, no pg_notify fires."""
    leader, deps, backend, _lc, fake_dp, shutdown = await _make_leader(
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    count = 0
    # Additive event on the double: set exactly where the sweep call is
    # entered, so the "no pg_notify" window below is anchored on a sweep
    # that provably ran instead of a sleep hoping the loop reached it.
    promote_attempted = asyncio.Event()

    async def zero_promote(**kw: object) -> int:
        promote_attempted.set()
        return count

    backend.scheduled_to_pending = zero_promote  # type: ignore[method-assign]
    deps.is_leader.set()

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for(promote_attempted)
    # The notify decision is made in the same loop step as the awaited
    # count — one yield for the loop to finish that step, then stop it.
    await asyncio.sleep(0)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    pg_notify_calls = [c for c in fake_dp.execute_calls if "pg_notify" in c[0]]
    assert pg_notify_calls == []


# ── Prune loop runs on schedule when leader ──────────────────────────


async def test_prune_loop_runs_on_schedule(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """_prune_loop fires at the scheduled time when is_leader, acquires
    advisory lock, calls prune_terminal_jobs, and releases the lock."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    prune_result_rows = [
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 3}),
        ],
    ]
    leader_conn = _FakeConnForPrune(
        batch_rows=prune_result_rows, fetchval_result=True, actor_config_rows=[]
    )

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()
    settings.prune_batch_size = 100

    task = asyncio.create_task(leader._prune_loop(shutdown))

    # Bounded poll for the prune cycle's first advisory-lock probe (no
    # event exists on the fake's recording).
    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the prune loop must attempt its advisory lock",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert lock_calls, "expected advisory lock acquisition"
    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "expected advisory lock release"


# ── OTel metrics emitted ────────────────────────────────────────────


async def test_otel_metrics_emitted(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """OTel metrics: elections counter records win, sweep rows counters
    record sweep_name labels."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
        monkeypatch=monkeypatch,
    )

    sweep_rows_calls: list[tuple[int, dict[str, object]]] = []
    election_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    _leader_sweeps_mod._sweep_rows_counter.add = lambda amount, attrs: sweep_rows_calls.append(  # type: ignore[method-assign]
        (int(amount), _as_dict(attrs or {}))
    )
    otel_mod._leader_election_attempts.add = lambda amount, attrs=None: election_calls.append(  # type: ignore[method-assign]
        (int(amount), _as_dict(attrs or {}))
    )

    # Run election
    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert election_calls == [(1, {})]

    # Populate backend with expired jobs, run sweep
    _now = leader._clock.now()  # type: ignore[reportPrivateUsage]  # Why: test reads injected clock to align job timestamps with sweep time.
    expired_id = new_job_id()
    _insert_job(
        backend,
        expired_id,
        status="running",
        lock_expires_at=_now - timedelta(seconds=1),
        attempt=1,
        max_attempts=3,
        now=_now,
    )
    deadline_id = new_job_id()
    _insert_job(
        backend,
        deadline_id,
        status="scheduled",
        schedule_to_close=_now - timedelta(seconds=1),
        now=_now,
    )

    sweep_rows_calls.clear()
    shutdown_2 = asyncio.Event()
    task_2 = asyncio.create_task(leader._sweep_loop(shutdown_2))
    # Bounded poll for both sweeps' non-empty row counters — the wake of
    # the completion asserts, never a fixed 0.05s hoping the tick ran.
    await wait_for_condition(
        lambda: (
            any(c[0] > 0 and c[1].get("sweep_name") == "expired_locks" for c in sweep_rows_calls)
            and any(
                c[0] > 0 and c[1].get("sweep_name") == "deadline_exceeded" for c in sweep_rows_calls
            )
        ),
        description="the sweep loop must run the expired-locks and deadline sweeps",
    )
    await _stop_after_tick(task_2, shutdown_2, delay=0.0)

    names_seen = {call[1].get("sweep_name") for call in sweep_rows_calls}
    assert "expired_locks" in names_seen
    assert "deadline_exceeded" in names_seen
    assert any(c[0] > 0 for c in sweep_rows_calls if c[1].get("sweep_name") == "expired_locks")
    assert any(c[0] > 0 for c in sweep_rows_calls if c[1].get("sweep_name") == "deadline_exceeded")


# ── kind='leadership_lost' log on watchdog error ────────────────────


async def test_leadership_lost_log_on_watchdog_error() -> None:
    """WARNING log with kind='leadership_lost', worker_id, error fields."""
    exc = asyncpg.PostgresConnectionError("watchdog failed")

    class FailingFakeConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise exc

    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    failing_monitor = FailingFakeConn()
    _clk = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        InMemoryBackend(clock=_clk),
        clock=_clk,
    )
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if failing_monitor._closed and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    assert not deps.is_leader.is_set()
    assert failing_monitor._closed
    assert deps.leader_conn is None
    shutdown.set()
    deps.is_leader.set()
    await task


# ── Transaction-scoped lock not used ─────────────────────────────────


async def test_transaction_scoped_lock_not_used(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """pg_try_advisory_xact_lock is never called.
    pg_try_advisory_lock(hashtextextended($1, 0)) IS called with literal 0."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    for sql, _ in leader_conn.fetchval_calls:
        assert "pg_try_advisory_xact_lock" not in sql

    lock_calls = [
        (sql, args) for sql, args in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql
    ]
    assert lock_calls
    for sql, _args in lock_calls:
        assert "hashtextextended($1, 0)" in sql
        assert "hashtextextended($1, $2)" not in sql


# ── Prune loop gates on is_leader ────────────────────────────────────


async def test_prune_loop_gates_on_is_leader() -> None:
    """Without is_leader, no prune lock acquisition or prune call.
    With is_leader, the lock is acquired and prune runs."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
    )

    settings = deps.settings
    settings.prune_schedule_utc = "03:00"

    task = asyncio.create_task(leader._prune_loop(shutdown))
    await asyncio.sleep(0.1)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no advisory lock when not leader"


# ── Injected Clock controls sweep and wake-loop timestamps ────────


async def test_scheduled_wake_passes_no_clock_to_sweep() -> None:
    """_scheduled_wake_loop drives scheduled_to_pending with NO clock
    value — the backend's own clock is the arbiter (seam removal: a
    caller-supplied ``now`` was ignored by PG and honored by InMemory, so
    the two backends never exercised the same contract)."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    called = asyncio.Event()

    async def _capture_scheduled_to_pending() -> int:
        called.set()
        return 0

    backend.scheduled_to_pending = _capture_scheduled_to_pending  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    # The capture closure sets the event exactly where the sweep call is
    # entered — the bounded wait, never a sleep hoping the loop reached it.
    await wait_for(called)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert called.is_set()


async def test_sweep_loop_passes_no_clock_to_sweeps() -> None:
    """_sweep_loop drives reclaim_expired_locks and deadline_sweep with NO
    clock value — the backend's own clock is the arbiter (same seam
    removal as the scheduled-wake loop)."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    reclaim_called = asyncio.Event()
    deadline_called = asyncio.Event()

    async def _capture_reclaim(cg: timedelta, ug: timedelta) -> int:
        reclaim_called.set()
        return 0

    async def _capture_deadline() -> int:
        deadline_called.set()
        return 0

    backend.reclaim_expired_locks = _capture_reclaim  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.
    backend.deadline_sweep = _capture_deadline  # type: ignore[method-assign]  # Why: test-only interception of the sweep call.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # The capture closures set their events exactly where each sweep call
    # is entered — bounded event waits, never sleeps hoping the loop
    # reached them.
    await wait_for(reclaim_called)
    await wait_for(deadline_called)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert reclaim_called.is_set()
    assert deadline_called.is_set()


# ── _schedule_utc_to_cron ────────────────────────────────────────────


async def test_schedule_utc_to_cron_standard() -> None:
    """_schedule_utc_to_cron parses '03:00' → '0 3 * * *'."""
    assert _schedule_utc_to_cron("03:00") == "0 3 * * *"


async def test_schedule_utc_to_cron_half_hour() -> None:
    """_schedule_utc_to_cron parses '00:30' → '30 0 * * *'."""
    assert _schedule_utc_to_cron("00:30") == "30 0 * * *"


async def test_schedule_utc_to_cron_single_digit_hour() -> None:
    """_schedule_utc_to_cron handles single-digit hour '9:15' → '15 9 * * *'."""
    assert _schedule_utc_to_cron("9:15") == "15 9 * * *"


async def test_schedule_utc_to_cron_invalid_format() -> None:
    """_schedule_utc_to_cron raises ValueError on invalid input."""
    with pytest.raises(ValueError, match="invalid HH:MM"):
        _schedule_utc_to_cron("not-a-time")


async def test_schedule_utc_to_cron_invalid_minutes() -> None:
    """_schedule_utc_to_cron raises ValueError on missing colon."""
    with pytest.raises(ValueError, match="invalid HH:MM"):
        _schedule_utc_to_cron("0300")


# ── _build_retention_per_status ──────────────────────────────────────


async def test_build_retention_per_status_defaults() -> None:
    """_build_retention_per_status returns five statuses with crashed → abandoned."""
    settings = _worker_settings("postgresql://x:x@localhost/x")
    result = _build_retention_per_status(settings)
    assert set(result.keys()) == {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
    assert result["crashed"] == settings.prune_retention_abandoned
    assert result["abandoned"] == settings.prune_retention_abandoned
    assert result["succeeded"] == settings.prune_retention_succeeded
    assert result["failed"] == settings.prune_retention_failed
    assert result["cancelled"] == settings.prune_retention_cancelled


async def test_build_retention_per_status_crashed_uses_abandoned() -> None:
    """crashed maps to prune_retention_abandoned, not a separate field."""
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        PRUNE_RETENTION_ABANDONED="P120D",
    )
    result = _build_retention_per_status(settings)
    assert result["crashed"] == timedelta(days=120)
    assert result["abandoned"] == timedelta(days=120)


# ── _load_actor_retention_overrides ────────────────────────────────────


async def test_load_actor_retention_overrides_with_rows() -> None:
    """_load_actor_retention_overrides returns per-actor timedelta overrides."""
    rows = [
        [
            _FakeRecord({"actor": "telemetry_ingest", "retention_days": 7}),
            _FakeRecord({"actor": "audit_critical", "retention_days": 365}),
        ],
    ]
    conn = _FakeConnForPrune(batch_rows=rows)
    result = await _load_actor_retention_overrides(conn, schema="taskq")
    assert result == {
        "telemetry_ingest": timedelta(days=7),
        "audit_critical": timedelta(days=365),
    }


async def test_load_actor_retention_overrides_empty() -> None:
    """_load_actor_retention_overrides returns empty dict when no rows."""
    conn = _FakeConnForPrune(batch_rows=[[]])
    result = await _load_actor_retention_overrides(conn, schema="taskq")
    assert result == {}


async def test_load_actor_retention_overrides_invalid_schema() -> None:
    """_load_actor_retention_overrides returns empty dict on invalid schema."""
    conn = _FakeConnForPrune(batch_rows=[])
    result = await _load_actor_retention_overrides(conn, schema="bad;schema")
    assert result == {}


# ── prune_terminal_jobs with FakeConn ────────────────────────


class _FakeRecord:
    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data


class _FakeConnForPrune(FakeConn):
    def __init__(
        self,
        *,
        batch_rows: list[list[_FakeRecord]] | None = None,
        actor_config_rows: list[_FakeRecord] | None = None,
        fetchval_result: object = None,
        db_now: datetime | None = None,
    ) -> None:
        super().__init__(fetchval_result=fetchval_result)
        self._batch_rows = batch_rows or []
        self._batch_index = 0
        self._actor_config_rows = actor_config_rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        # 7565d3f anchors prune cutoffs and the expiry reference instant to
        # the database clock via a dedicated fetchval, while fetchval_result
        # keeps answering the advisory-lock probes — one scalar cannot serve
        # both, so the fake answers the DB-now query with a datetime.
        self._db_now = db_now

    async def fetchval(self, sql: str, *args: object) -> object:
        if sql != _DB_NOW_SQL:
            return await super().fetchval(sql, *args)
        self.fetchval_calls.append((sql, args))
        return self._db_now if self._db_now is not None else datetime.now(UTC)

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        self.fetch_calls.append((sql, args))
        # The batch machinery reads the session's statement_timeout through
        # fetch() before every batch (SET LOCAL is transaction-scoped, so
        # the sweep must capture and restore it); the fake answers that
        # probe itself rather than consuming a scripted batch.
        if "current_setting" in sql:
            return [_FakeRecord({"current_setting": "0"})]
        if "actor_config" in sql and self._actor_config_rows is not None:
            return self._actor_config_rows
        if self._batch_index < len(self._batch_rows):
            rows = self._batch_rows[self._batch_index]
            self._batch_index += 1
            return rows
        return []


async def test_prune_terminal_jobs_returns_prune_result() -> None:
    """prune_terminal_jobs returns PruneResult with aggregated counts.

    Cutoffs are anchored to the database clock (7565d3f): each terminal
    status's cutoff is exactly ``db_now - retention``, never an app-clock
    instant — the caller feeds ``max(cutoffs.values())`` into
    ``prune_old_batches``, so skew here would prune batches early or late.
    """
    rows = [
        [
            _FakeRecord({"actor": "test_actor", "status": "succeeded", "cnt": 5}),
        ],
    ]
    db_now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    conn = _FakeConnForPrune(batch_rows=rows, db_now=db_now)
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema="taskq",
    )
    assert result.total_deleted == 5
    assert result.archived == 5
    assert result.by_actor == {"test_actor": 5}
    assert result.by_status == {"succeeded": 5}
    assert result.cutoffs == {status: db_now - timedelta(days=30) for status in TERMINAL_STATUSES}
    assert result.duration_ms >= 0


async def test_prune_terminal_jobs_drains_to_empty() -> None:
    """prune_terminal_jobs loops until a batch returns fewer than batch_size rows."""
    rows = [
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 10}),
        ],
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 3}),
        ],
        [],
    ]
    conn = _FakeConnForPrune(batch_rows=rows)
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10,
        schema="taskq",
    )
    assert result.total_deleted == 13
    assert result.by_status["succeeded"] == 13


async def test_prune_terminal_jobs_invalid_schema() -> None:
    """prune_terminal_jobs raises ValueError on invalid schema identifier."""
    conn = _FakeConnForPrune()
    with pytest.raises(ValueError, match="invalid schema"):
        await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema="bad schema",
        )


async def test_archive_expiry_sweep_returns_result() -> None:
    """archive_expiry_sweep returns ArchiveExpiryResult with counts.

    ``expire_before`` is the database clock's instant (7565d3f) so the
    reported value cannot disagree with the server-side
    ``expire_at < clock_timestamp()`` predicate that actually ran.
    """
    rows = [
        [
            _FakeRecord({"status": "succeeded", "cnt": 7}),
        ],
    ]
    db_now = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    conn = _FakeConnForPrune(batch_rows=rows, db_now=db_now)
    result = await archive_expiry_sweep(
        conn,
        batch_size=100,
        schema="taskq",
    )
    assert result.total_deleted == 7
    assert result.by_status == {"succeeded": 7}
    assert result.expire_before == db_now
    assert result.duration_ms >= 0


async def test_archive_expiry_sweep_drains_to_empty() -> None:
    """archive_expiry_sweep loops until drain-to-empty."""
    rows = [
        [
            _FakeRecord({"status": "failed", "cnt": 10}),
        ],
        [
            _FakeRecord({"status": "failed", "cnt": 2}),
        ],
        [],
    ]
    conn = _FakeConnForPrune(batch_rows=rows)
    result = await archive_expiry_sweep(
        conn,
        batch_size=10,
        schema="taskq",
    )
    assert result.total_deleted == 12
    assert result.by_status["failed"] == 12


async def test_archive_expiry_sweep_invalid_schema() -> None:
    """archive_expiry_sweep raises ValueError on invalid schema identifier."""
    conn = _FakeConnForPrune()
    with pytest.raises(ValueError, match="invalid schema"):
        await archive_expiry_sweep(conn, schema="bad;schema")


async def test_prune_terminal_jobs_actor_override_shorter_retention() -> None:
    """Actor override with shorter retention runs per-actor batch for that status."""
    global_rows = [
        [_FakeRecord({"actor": "telemetry_ingest", "status": "succeeded", "cnt": 10})],
        [],  # failed
        [],  # cancelled
        [],  # crashed
        [],  # abandoned
    ]
    actor_rows = [
        [_FakeRecord({"actor": "telemetry_ingest", "status": "succeeded", "cnt": 4})],
    ]
    conn = _FakeConnForPrune(batch_rows=global_rows + actor_rows)
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema="taskq",
        actor_overrides={"telemetry_ingest": timedelta(days=7)},
    )
    assert result.total_deleted == 14
    assert result.by_actor["telemetry_ingest"] == 14
    assert result.by_status["succeeded"] == 14


async def test_prune_terminal_jobs_actor_override_longer_retention_skipped() -> None:
    """Actor override with longer or equal retention is skipped; global batch suffices."""
    global_rows = [
        [_FakeRecord({"actor": "slow_actor", "status": "succeeded", "cnt": 8})],
        [],  # failed
        [],  # cancelled
        [],  # crashed
        [],  # abandoned
    ]
    actor_rows = [
        [_FakeRecord({"actor": "slow_actor", "status": "succeeded", "cnt": 5})],
    ]
    conn = _FakeConnForPrune(batch_rows=global_rows + actor_rows)
    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=100,
        schema="taskq",
        actor_overrides={"slow_actor": timedelta(days=90)},
    )
    assert result.total_deleted == 8
    assert result.by_actor["slow_actor"] == 8


# ── Date guard prevents double-prune on same UTC day ────────────────


async def test_prune_loop_date_guard_prevents_double_run(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """_prune_loop skips when last_pruned_date == today (UTC)."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    prune_result_rows = [
        [
            _FakeRecord({"actor": "a", "status": "succeeded", "cnt": 5}),
        ],
    ]
    leader_conn = _FakeConnForPrune(
        batch_rows=prune_result_rows, fetchval_result=True, actor_config_rows=[]
    )

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = "* * * * *"

    task = asyncio.create_task(leader._prune_loop(shutdown))

    # Bounded poll for the first lock acquisition (no event exists on the
    # fake's recording).
    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the prune loop must attempt its first advisory lock",
    )

    lock_calls_after_first = len(
        [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    )
    assert lock_calls_after_first == 1

    for _ in range(200):
        lock_calls_now = len(
            [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        )
        if lock_calls_now > lock_calls_after_first:
            pytest.fail("date guard should have prevented second lock acquisition on same day")
        await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_archive_expiry_loop_date_guard_prevents_double_run(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """_archive_expiry_loop skips when last_expiry_date == today (UTC)."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    expiry_result_rows = [
        [
            _FakeRecord({"status": "succeeded", "cnt": 5}),
        ],
    ]
    leader_conn = _FakeConnForPrune(batch_rows=expiry_result_rows, fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = "* * * * *"

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    # Bounded poll for the first lock acquisition (no event exists on the
    # fake's recording).
    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the archive expiry loop must attempt its first advisory lock",
    )

    lock_calls_after_first = len(
        [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    )
    assert lock_calls_after_first == 1

    for _ in range(200):
        lock_calls_now = len(
            [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        )
        if lock_calls_now > lock_calls_after_first:
            pytest.fail("date guard should have prevented second lock acquisition on same day")
        await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── Archive expiry loop gates on is_leader ──────────────────────────


async def test_archive_expiry_loop_gates_on_is_leader() -> None:
    """_archive_expiry_loop does not acquire lock when not leader."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=False,
    )

    settings = deps.settings
    settings.archive_expiry_schedule_utc = "04:00"

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    await asyncio.sleep(0.1)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no advisory lock when not leader"


# ── Prune family: bounded, interruptible, retrying batches ──────────
#
# The once-a-day prune used to be the one maintenance family outside the
# repo's bounded-per-transaction discipline: bare while-True drains with
# no shutdown check (a SIGTERM mid-drain hung MaintenanceLeader's
# TaskGroup for the whole backlog while holding the prune advisory lock
# and a pool connection), no per-batch statement timeout or batch-size
# breaker (a 10 000-row archive CTE under the dispatcher pool's 5 s
# client command timeout fails every attempt on a loaded PG), and no
# retry after a failed attempt (the next try was tomorrow's cron fire).
# These tests pin the three halves of that fix: gate-stopped drains,
# server-bounded batches with a latching breaker, and the intra-day
# backoff retry that never runs a second successful prune per day.


def _full_batch_record(batch_size: int) -> _FakeRecord:
    """One aggregate row reporting a full batch — the drain must continue."""
    return _FakeRecord({"actor": "a", "status": "succeeded", "cnt": batch_size})


class _HookedPruneBatchConn(_FakeConnForPrune):
    """Answers every prune batch with one full batch, invoking *on_batch*
    first — a backlog no drain can exhaust, with an observation seam at
    each batch."""

    def __init__(self, *, batch_size: int, on_batch: Callable[[], None]) -> None:
        super().__init__(batch_rows=[], fetchval_result=True)
        self._row = [_full_batch_record(batch_size)]
        self._on_batch = on_batch
        self.batches = 0

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        if "candidate_ids" in sql:
            self.batches += 1
            self._on_batch()
            return list(self._row)
        return await super().fetch(sql, *args)


class _HookedExpiryBatchConn(_FakeConnForPrune):
    """The archive-expiry twin of _HookedPruneBatchConn (``expired`` window)."""

    def __init__(self, *, batch_size: int, on_batch: Callable[[], None]) -> None:
        super().__init__(batch_rows=[], fetchval_result=True)
        self._row = [_FakeRecord({"status": "succeeded", "cnt": batch_size})]
        self._on_batch = on_batch
        self.batches = 0

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        if "expired" in sql:
            self.batches += 1
            self._on_batch()
            return list(self._row)
        return await super().fetch(sql, *args)


async def test_prune_drain_stops_when_gate_closes() -> None:
    """A drain gate returning False stops the drain between batches:
    committed batches stay counted, the unbounded remainder is left for
    the next attempt — a stopped drain is a pause, not a rollback."""
    conn = _HookedPruneBatchConn(batch_size=10, on_batch=lambda: None)
    allowed = 3

    def gate() -> bool:
        nonlocal allowed
        if allowed == 0:
            return False
        allowed -= 1
        return True

    result = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        batch_size=10,
        schema="taskq",
        drain_gate=gate,
    )
    assert conn.batches == 3, (
        f"the drain ran {conn.batches} batches after the gate closed; a "
        "False gate return must stop the drain before the next batch"
    )
    assert result.total_deleted == 30
    assert result.archived == 30


async def test_archive_expiry_drain_stops_when_gate_closes() -> None:
    """The archive-expiry drain honors the same gate contract."""
    conn = _HookedExpiryBatchConn(batch_size=10, on_batch=lambda: None)
    allowed = 2

    def gate() -> bool:
        nonlocal allowed
        if allowed == 0:
            return False
        allowed -= 1
        return True

    result = await archive_expiry_sweep(
        conn,
        batch_size=10,
        schema="taskq",
        drain_gate=gate,
    )
    assert conn.batches == 2
    assert result.total_deleted == 20


async def test_prune_batches_run_under_server_statement_timeout() -> None:
    """Every prune batch — including the empty probe batch each status
    runs — applies the server-side statement_timeout via SET LOCAL inside
    the batch's transaction and restores the session value afterwards
    (the same capture/restore contract the backend sweeps pin in
    tests/test_rt_sweeps_timeout_leak.py)."""
    conn = _FakeConnForPrune(batch_rows=[[_full_batch_record(5)]])
    await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        schema="taskq",
        statement_timeout_ms=4321,
    )
    set_config_calls = [(sql, args) for sql, args in conn.execute_calls if "set_config" in sql]
    status_probes = len(TERMINAL_STATUSES)
    assert len(set_config_calls) == 2 * status_probes, (
        f"expected apply+restore per batch ({status_probes} probe batches, "
        f"one per status); got {len(set_config_calls)} set_config calls"
    )
    applies = [args for _sql, args in set_config_calls[0::2]]
    restores = [args for _sql, args in set_config_calls[1::2]]
    assert applies and all(args == ("4321",) for args in applies), (
        f"the bound must be the caller's statement_timeout_ms on every batch; got {applies}"
    )
    assert restores and all(args == ("0",) for args in restores), (
        f"the captured session value must be restored after every batch; got {restores}"
    )


async def test_archive_expiry_batches_run_under_server_statement_timeout() -> None:
    """The archive-expiry batches apply the same SET LOCAL bound."""
    conn = _FakeConnForPrune(batch_rows=[[_FakeRecord({"status": "succeeded", "cnt": 7})], []])
    await archive_expiry_sweep(conn, schema="taskq", batch_size=7, statement_timeout_ms=4321)
    set_config_calls = [(sql, args) for sql, args in conn.execute_calls if "set_config" in sql]
    # One full batch + the empty probe that ends the drain.
    assert len(set_config_calls) == 4
    assert [args for _sql, args in set_config_calls[0::2]] == [("4321",), ("4321",)]
    assert [args for _sql, args in set_config_calls[1::2]] == [("0",), ("0",)]


async def test_prune_timeout_latches_sizer_and_degrades_next_attempt() -> None:
    """A server-side batch abort (QueryCanceledError) counts against the
    breaker: the sizer latches and the NEXT attempt's windows run at the
    reduced tier — the degradation that makes a loaded database drainable
    under a timeout smaller than its backlog."""

    class _TimeoutConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "candidate_ids" in sql:
                raise asyncpg.QueryCanceledError("canceling statement due to statement timeout")
            return await super().fetch(sql, *args)

    sizer = SweepBatchSizer(default_size=10_000, divisor=4, failure_threshold=1, window_secs=600.0)
    conn = _TimeoutConn(batch_rows=[])
    with pytest.raises(asyncpg.QueryCanceledError):
        await prune_terminal_jobs(
            conn,
            retention_per_status={"succeeded": timedelta(days=30)},
            archive_retention=timedelta(days=365),
            schema="taskq",
            statement_timeout_ms=4321,
            sizer=sizer,
        )
    assert sizer.effective_size() == 2_500, (
        "a cancelled batch must latch the breaker to the reduced tier"
    )

    # The latched tier sizes the next attempt's windows — the retry path's
    # whole point.
    conn2 = _FakeConnForPrune(batch_rows=[[_full_batch_record(5)]])
    await prune_terminal_jobs(
        conn2,
        retention_per_status={"succeeded": timedelta(days=30)},
        archive_retention=timedelta(days=365),
        schema="taskq",
        statement_timeout_ms=4321,
        sizer=sizer,
    )
    batch_fetches = [(sql, args) for sql, args in conn2.fetch_calls if "candidate_ids" in sql]
    assert batch_fetches, "fixture broken: no prune batch ran"
    assert batch_fetches[0][1][2] == 2_500, (
        f"the latched reduced tier must be the window LIMIT; got args {batch_fetches[0][1]!r}"
    )


def _soon_then_far_croniter() -> type:
    """A croniter double whose FIRST fire is immediate and every later one
    an hour out: within a test window, any attempt after the first can
    only be a failure backoff retry, never a scheduled fire. Fresh state
    per call (the loop constructs a new croniter each iteration, so the
    soon/far memory must be shared across instances, not per-instance)."""
    state = {"first": True}

    class _Croniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            if state["first"]:
                state["first"] = False
                return datetime.now(UTC) + timedelta(seconds=0.01)
            return datetime.now(UTC) + timedelta(hours=1)

    return _Croniter


class _AlwaysFailsPruneConn(_FakeConnForPrune):
    """Every prune batch statement raises — the failed-attempt shape."""

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        if "candidate_ids" in sql:
            raise RuntimeError("connection lost")
        return await super().fetch(sql, *args)


def _lock_attempts(conn: FakeConn) -> int:
    return len([sql for sql, _ in conn.fetchval_calls if "pg_try_advisory_lock" in sql])


async def test_prune_loop_retries_failed_attempt_with_backoff(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A failed prune attempt retries within the day on the backoff ladder
    instead of sleeping to tomorrow's cron fire."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _soon_then_far_croniter())

    leader_conn = _AlwaysFailsPruneConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 3,
            description="a failed prune attempt must retry on the backoff ladder "
            "(>=3 attempts) rather than wait for tomorrow's cron fire",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_prune_loop_backoff_doubles_to_cap(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """The retry cadence doubles (60 s, 120 s, … capped) rather than
    retrying at a fixed rate: with initial=0.05 s doubling, a 1 s window
    sees ~5 attempts, where a fixed 0.05 s cadence would see ~20 and a
    no-retry loop exactly 1."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.05)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 10.0)
    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _soon_then_far_croniter())

    leader_conn = _AlwaysFailsPruneConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    await asyncio.sleep(1.0)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    attempts = _lock_attempts(leader_conn)
    assert 3 <= attempts <= 7, (
        f"{attempts} attempts in 1 s — a doubling ladder from 0.05 s lands "
        "at ~5 (attempts at 0.05, 0.1, 0.2, 0.4, 0.8 s); ~20 means a fixed "
        "cadence, 1 means no retry at all"
    )


async def test_prune_loop_success_stops_retry_for_the_day(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Fail once, succeed on the retry, then the day is done: no further
    attempts — the once-per-SUCCESSFUL-prune-per-day guard holds through
    the retry change (a retried day never gets a second successful
    prune)."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _soon_then_far_croniter())

    class _FailOnceConn(_FakeConnForPrune):
        def __init__(self) -> None:
            super().__init__(batch_rows=[], fetchval_result=True)
            self._prune_batches = 0

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "candidate_ids" in sql:
                self._prune_batches += 1
                if self._prune_batches == 1:
                    raise RuntimeError("connection lost")
                return []
            return await super().fetch(sql, *args)

    leader_conn = _FailOnceConn()
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 2,
            description="the failed first attempt must be retried",
            timeout=3.0,
        )
        # The retry succeeded; the day is marked. Give the loop room to
        # (wrongly) attempt again, then hold it to exactly two.
        await asyncio.sleep(0.3)
        assert _lock_attempts(leader_conn) == 2, (
            "a successful prune must end the day's attempts — the retry "
            "must not become a second prune"
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_archive_expiry_loop_retries_failed_attempt_with_backoff(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The archive-expiry loop shares the prune loop's failure-half policy:
    a failed attempt retries on the backoff ladder within the day."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_INITIAL_SECS", 0.02)
    monkeypatch.setattr(_leader_sweeps_mod, "_PRUNE_RETRY_BACKOFF_CAP_SECS", 0.2)
    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _soon_then_far_croniter())

    class _AlwaysFailsExpiryConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                raise RuntimeError("connection lost")
            return await super().fetch(sql, *args)

    leader_conn = _AlwaysFailsExpiryConn(batch_rows=[], fetchval_result=True)
    leader, _deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _lock_attempts(leader_conn) >= 3,
            description="a failed archive-expiry attempt must retry on the "
            "backoff ladder rather than wait for tomorrow's cron fire",
            timeout=3.0,
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_prune_loop_bounds_batches_under_pool_command_timeout(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The loop derives the per-batch server-side bound from the pool it
    runs on: 80% of the default 5 s dispatcher_command_timeout = 4000 ms,
    so the server's QueryCanceledError (breaker-counted, degrading)
    always arrives before the pool's opaque client TimeoutError."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    leader_conn = _FakeConnForPrune(
        batch_rows=[[_full_batch_record(5)]], fetchval_result=True, actor_config_rows=[]
    )
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )
    assert deps.settings.dispatcher_command_timeout == 5.0  # fixture precondition

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)
    deps.settings.prune_cron_expr = "* * * * *"

    task = asyncio.create_task(leader._prune_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: any("set_config" in sql for sql, _ in leader_conn.execute_calls),
            description="the prune loop must bound its batches with a server-side timeout",
        )
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    bound = [
        args for sql, args in leader_conn.execute_calls if "set_config" in sql and args == ("4000",)
    ]
    assert bound, (
        "prune batches must run under the pool-derived server-side bound "
        "(0.8 x dispatcher_command_timeout); no 4000 ms set_config seen"
    )


async def test_prune_loop_drain_stops_on_shutdown_and_ticks_liveness(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """SIGTERM mid-drain ends the attempt between batches — the loop task
    finishes without cancel, after a bounded number of batches, not after
    the whole backlog. The drain registers with detector 2 while it runs
    (so a wedged batch loop is visible to the watchdog) and forgets the
    registration when the attempt ends (so a once-a-day loop cannot read
    as a stale sibling between attempts)."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    shutdown = asyncio.Event()
    # The observation seam closes over a holder: the conn must be built
    # before the leader (whose deps carry the liveness registry it
    # observes), so the callback resolves the holder at batch time.
    deps_holder: list[WorkerDeps] = []
    liveness_snapshots: list[dict[str, float]] = []

    def on_batch() -> None:
        liveness_snapshots.append(dict(deps_holder[0].liveness.ages()))
        if len(liveness_snapshots) == 3:
            shutdown.set()

    leader_conn = _HookedPruneBatchConn(batch_size=10, on_batch=on_batch)
    leader, deps, _backend, _, _, _ = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )
    deps_holder.append(deps)

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)
    deps.settings.prune_cron_expr = "* * * * *"
    # The hooked conn reports 10-row full batches, so the effective window
    # must be 10 too — the sizer's default tier comes from this setting.
    deps.settings.prune_batch_size = 10

    task = asyncio.create_task(leader._prune_loop(shutdown))
    # No cancel, no suppress: a shutdown-responsive drain lets the loop
    # task COMPLETE on its own — the property the TaskGroup hang lacked.
    await asyncio.wait_for(task, timeout=5.0)

    assert leader_conn.batches == 3, (
        f"{leader_conn.batches} batches ran after shutdown was set "
        "mid-drain; the drain must stop between batches, holding the prune "
        "advisory lock and its pool connection for at most one batch"
    )
    assert any("leader.prune" in ages for ages in liveness_snapshots), (
        "the drain must register with detector 2 while batches flow — a "
        "wedged once-a-day drain is otherwise invisible to the watchdog"
    )
    assert "leader.prune" not in deps.liveness.ages(), (
        "the attempt-scoped liveness registration must be forgotten when "
        "the attempt ends — a once-a-day loop must not read as a stale "
        "sibling between attempts"
    )


async def test_archive_expiry_loop_drain_stops_on_shutdown(
    monkeypatch: Any,  # type: ignore[reportUnknownParameterType]
) -> None:
    """The archive-expiry drain honors the same shutdown-between-batches
    contract as the prune drain."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    shutdown = asyncio.Event()
    batches_seen: list[int] = []

    def on_batch() -> None:
        batches_seen.append(1)
        if len(batches_seen) == 2:
            shutdown.set()

    leader_conn = _HookedExpiryBatchConn(batch_size=10, on_batch=on_batch)
    leader, deps, _backend, _, _, _ = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)
    deps.settings.archive_expiry_cron_expr = "* * * * *"
    # The hooked conn reports 10-row full batches, so the effective window
    # must be 10 too — the sizer's default tier comes from this setting.
    deps.settings.prune_batch_size = 10

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    await asyncio.wait_for(task, timeout=5.0)

    assert leader_conn.batches == 2


# ── Prune loop releases lock on error ────────────────────────────────────────


async def test_prune_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop releases advisory lock even when prune_terminal_jobs raises."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    # Additive event on the double: set exactly where the prune query
    # raises, so the test waits for the error path instead of sleeping
    # and hoping the loop reached it.
    error_seen = asyncio.Event()

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "candidate_ids" in sql:
                error_seen.set()
                raise RuntimeError("connection lost")
            # Machinery probes (the batch timeout's current_setting read)
            # answer through the base double so the error surfaces from the
            # prune statement itself, not from the batch wrapper.
            return await super().fetch(sql, *args)

    leader_conn = _ErrorConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for(error_seen)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "lock must be released even on error"


# ── Archive expiry loop releases lock on error ──────────────────────────────


async def test_archive_expiry_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop releases advisory lock even when sweep raises."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    # Additive event on the double: set exactly where the expiry query
    # raises, so the test waits for the error path instead of sleeping
    # and hoping the loop reached it.
    error_seen = asyncio.Event()

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                error_seen.set()
                raise RuntimeError("connection lost")
            # Machinery probes (the batch timeout's current_setting read)
            # answer through the base double so the error surfaces from the
            # expiry statement itself, not from the batch wrapper.
            return await super().fetch(sql, *args)

    leader_conn = _ErrorConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    await wait_for(error_seen)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "lock must be released even on error"


# ── Shutdown wakes prune loop immediately ────────────────────────────────────


async def test_prune_loop_wakes_on_shutdown() -> None:
    """Prune loop breaks immediately when shutdown is set during sleep."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=True,
    )

    settings = deps.settings
    settings.prune_schedule_utc = "03:00"

    shutdown.set()
    task = asyncio.create_task(leader._prune_loop(shutdown))
    # The loop observes the pre-set shutdown immediately — awaiting the
    # task IS the bounded wait for its exit (and it re-raises if the loop
    # died of an error instead of exiting cleanly).
    await asyncio.wait_for(task, timeout=2.0)
    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no lock acquisition during shutdown"


# ── Shutdown wakes archive expiry loop immediately ──────────────────────────


async def test_archive_expiry_loop_wakes_on_shutdown() -> None:
    """Archive expiry loop breaks immediately when shutdown is set during sleep."""
    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        is_leader=True,
    )

    settings = deps.settings
    settings.archive_expiry_schedule_utc = "04:00"

    shutdown.set()
    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    # The loop observes the pre-set shutdown immediately — awaiting the
    # task IS the bounded wait for its exit (and it re-raises if the loop
    # died of an error instead of exiting cleanly).
    await asyncio.wait_for(task, timeout=2.0)
    lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "no lock acquisition during shutdown"


# ── Prune loop skips when lock not acquired ──────────────────────────────────


async def test_prune_loop_skips_when_lock_not_acquired(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop skips the prune run and does not call prune_terminal_jobs
    when pg_try_advisory_lock returns False."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    leader_conn = _FakeConnForPrune(batch_rows=[], fetchval_result=False)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for_condition(
        lambda: any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls),
        description="the prune loop must attempt the advisory lock",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    fetch_calls = [sql for sql, _ in leader_conn.fetch_calls if "candidate_ids" in sql]
    assert not fetch_calls, "prune should not run when lock not acquired"
    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert not unlock_calls, "no unlock needed when lock was never acquired"


# ── Prune loop survives lock acquisition failure ─────────────────────────────


async def test_prune_loop_survives_lock_acquisition_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop continues when pg_try_advisory_lock raises a connection error,
    instead of crashing the TaskGroup."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    lock_call_count = 0

    class _LockFailsConn(_FakeConnForPrune):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_call_count
            if "pg_try_advisory_lock" in sql and "prune" in str(args):
                lock_call_count += 1
                raise asyncpg.PostgresConnectionError("connection lost")
            return await super().fetchval(sql, *args)

    leader_conn = _LockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    # Bounded poll on the failure count (a count on the fake's raise
    # point is the observable; the loop must retry past the first one).
    await wait_for_condition(
        lambda: lock_call_count >= 2,
        description="the prune loop must retry the lock acquisition after a connection failure",
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert lock_call_count >= 2, "loop should continue after lock acquisition failure"
    assert not task.cancelled(), "task should not be cancelled"


# ── Archive expiry loop survives lock acquisition failure ─────────────────────


async def test_archive_expiry_loop_survives_lock_acquisition_failure(
    monkeypatch: Any,
) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop continues when pg_try_advisory_lock raises a connection
    error, instead of crashing the TaskGroup."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    lock_call_count = 0

    class _LockFailsConn(_FakeConnForPrune):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_call_count
            if "pg_try_advisory_lock" in sql and "archive_expiry" in str(args):
                lock_call_count += 1
                raise asyncpg.InterfaceError("connection is closed")
            return await super().fetchval(sql, *args)

    leader_conn = _LockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    # Bounded poll on the failure count (a count on the fake's raise
    # point is the observable; the loop must retry past the first one).
    await wait_for_condition(
        lambda: lock_call_count >= 2,
        description=(
            "the archive expiry loop must retry the lock acquisition after a connection failure"
        ),
    )
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert lock_call_count >= 2, "loop should continue after lock acquisition failure"
    assert not task.cancelled(), "task should not be cancelled"


# ── Prune loop survives unlock failure in finally block ──────────────────────


async def test_prune_loop_survives_unlock_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop continues when pg_advisory_unlock raises in the finally block,
    instead of crashing the TaskGroup. PG releases the lock on session death."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    # Additive event on the double: set exactly where the prune query is
    # entered, so the test waits for the prune run instead of sleeping
    # and hoping the loop reached it.
    prune_ran = asyncio.Event()

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise asyncpg.PostgresConnectionError("connection lost")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "candidate_ids" in sql:
                prune_ran.set()
            # Machinery probes answer through the base double (see the
            # _ErrorConn note above).
            return await super().fetch(sql, *args)

    leader_conn = _UnlockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.prune_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._prune_loop(shutdown))

    await wait_for(prune_ran)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert prune_ran.is_set(), "prune should have run"
    assert not task.cancelled(), "task should not be cancelled after unlock failure"


# ── Archive expiry loop survives unlock failure in finally block ──────────────


async def test_archive_expiry_loop_survives_unlock_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop continues when pg_advisory_unlock raises in the finally
    block, instead of crashing the TaskGroup. PG releases the lock on session death."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    # Additive event on the double: set exactly where the expiry query is
    # entered, so the test waits for the sweep run instead of sleeping
    # and hoping the loop reached it.
    sweep_ran = asyncio.Event()

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise OSError(104, "Connection reset by peer")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            if "expired" in sql:
                sweep_ran.set()
            # Machinery probes answer through the base double (see the
            # _ErrorConn note above).
            return await super().fetch(sql, *args)

    leader_conn = _UnlockFailsConn(batch_rows=[], fetchval_result=True)

    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        dispatcher_pool=_PoolWithFixedConn(leader_conn),
        is_leader=True,
        monkeypatch=monkeypatch,
    )

    class _InstantCroniter:
        def __init__(self, expr: str, start_time: object) -> None:
            pass

        def get_next(self, dt_type: type[datetime]) -> datetime:
            return datetime.now(UTC) + timedelta(seconds=0.05)

    monkeypatch.setattr(_leader_sweeps_mod.cr, "croniter", _InstantCroniter)

    settings = deps.settings
    settings.archive_expiry_cron_expr = _next_minute_cron()

    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))

    await wait_for(sweep_ran)
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert sweep_ran.is_set(), "archive expiry sweep should have run"
    assert not task.cancelled(), "task should not be cancelled after unlock failure"


# ── Reopen retries on credential-provider exceptions ────────────────────


class _FakeProviderError(RuntimeError):
    """Stand-in for azure/hvac/botocore credential-fetch failures, which are
    NOT asyncpg.PostgresConnectionError subclasses (and neither is
    asyncpg.InvalidPasswordError — a fresh-but-rejected token)."""


async def test_election_reopen_retries_on_provider_exception(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A leader_conn_factory raising a provider-style exception once (IdP
    outage) then succeeding must be retried by the election loop — the
    exception must NOT escape and crash the worker TaskGroup."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls = 0

    async def flaky_factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise _FakeProviderError("simulated IdP outage")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = flaky_factory  # type: ignore[assignment]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task  # must not propagate the provider exception

    assert deps.is_leader.is_set()
    assert factory_calls >= 2


async def test_dedicated_conn_reopen_retries_on_provider_exception(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """A provider exception while opening the monitor/cron dedicated conns
    (after the advisory lock is won) must be caught and retried — a
    transient IdP failure mid-election must not crash the worker."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    factory_calls = 0

    async def flaky_factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 2:
            # leader_conn reopened fine; the monitor-conn open hits the outage
            raise _FakeProviderError("simulated IdP outage")
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = flaky_factory  # type: ignore[assignment]

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    # leader, failed monitor, then leader + monitor + cron on the retry
    assert factory_calls >= 4


# ── Ownership contract: caller-owned leader_conn is never closed ────────


async def test_watchdog_does_not_close_caller_owned_leader_conn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Caller-owned leader_conn that dies is abandoned, never closed.

    The ownership contract ("TaskQ never closes caller-owned resources")
    forbids close() even on the dead-conn path — the caller owns the
    corpse. The watchdog must still drop our reference so the election
    loop rebuilds via the factory/DSN path and re-establishes leadership.
    """
    caller_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=caller_conn,
        monkeypatch=monkeypatch,
    )
    assert not deps.owns_leader_conn  # _make_deps default: caller-provided conn
    assert deps.leader_conn is caller_conn

    class FailingMonitor(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise asyncpg.PostgresConnectionError("monitor probe failed")

    deps.is_leader.set()
    failing_monitor = FailingMonitor()
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    factory_conns: list[
        object
    ] = []  # Why: identity bag — members are compared with `is`/`in` against deps.leader_conn (asyncpg.Connection), so element typing as FakeConn makes pyright report no-overlap.

    async def factory() -> FakeConn:
        conn = FakeConn(fetchval_result=True)
        factory_conns.append(conn)
        return conn

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    watchdog_task = asyncio.create_task(leader._watchdog_loop(shutdown))
    election_task = asyncio.create_task(leader._election_loop(shutdown))

    # Wait for the watchdog failure path to run. failing_monitor.closed_event
    # is the stable witness: _close_leader_owned_conns closes it AFTER the
    # leader_conn close/abandon in the same handler, so once it is set the
    # caller-conn decision has definitely been made (and unlike the
    # is_leader flip, it never flips back under a racing re-election).
    await wait_for(failing_monitor.closed_event)
    assert failing_monitor._closed
    assert not caller_conn.is_closed()
    assert caller_conn.close_calls == 0

    # Election loop re-acquires leadership through the factory. The
    # election loop assigns deps.leader_conn from the factory BEFORE it
    # wins the lock and sets is_leader (leader.py), so the event wait
    # implies the factory-conn check asserted below.
    await wait_for_leader(deps)
    shutdown.set()
    watchdog_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watchdog_task
    await election_task

    assert deps.is_leader.is_set()
    assert deps.leader_conn in factory_conns
    assert not caller_conn.is_closed()
    assert caller_conn.close_calls == 0


async def test_watchdog_closes_taskq_owned_leader_conn() -> None:
    """TaskQ-owned leader_conn IS closed on the watchdog failure path — the
    ownership guard must not change TaskQ-owned behaviour."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True
    deps.is_leader.set()

    class FailingMonitor(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            raise asyncpg.PostgresConnectionError("monitor probe failed")

    failing_monitor = FailingMonitor()
    leader._leader_monitor_conn = failing_monitor  # type: ignore[reportAttributeAccessIssue]

    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    for _ in range(200):
        if leader_conn.is_closed() and not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    deps.is_leader.set()  # unblock the wait() so the loop observes shutdown
    await task

    assert leader_conn.is_closed()
    assert leader_conn.close_calls == 1
    assert deps.leader_conn is None


# ── Bounded closes: hung close is terminated, fast close is not ─────────
#
# Issue #38: the election/watchdog/cron paths closed leader-owned dedicated
# conns with a bare ``await conn.close()`` — a dead PG can block that
# indefinitely, stalling the watchdog. These tests pin the bounded-close
# discipline (asyncio.wait_for + terminate on timeout) applied via
# ``close_conn_bounded``; the shrink seam is the same module-global
# monkeypatch convention as tests/test_worker_deps_teardown.py.


async def test_close_leader_owned_conns_terminates_hung_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung close on a leader-owned dedicated conn (dead PG) is terminated
    after the bounded timeout; the loop still closes the remaining conn
    gracefully, nulls both attrs, and clears is_leader."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    monitor.close_wait.clear()  # close() blocks forever from now on

    # Why the outer timeout: pre-fix this awaited conn.close() unbounded, so
    # the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        await leader._close_leader_owned_conns()

    assert monitor.terminated is True
    assert monitor.close_calls == 1
    assert cron.is_closed()
    assert cron.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


async def test_close_leader_owned_conns_fast_close_not_terminated() -> None:
    """Healthy close(): both leader-owned conns close gracefully — nothing is
    terminated; attrs nulled; is_leader cleared. Pins the no-regression
    behaviour (passes pre- and post-fix)."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]

    await leader._close_leader_owned_conns()

    for conn in (cron, monitor):
        assert conn.is_closed()
        assert conn.close_calls == 1
        assert conn.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


class _CloseEntryConn(FakeConn):
    """FakeConn that signals when close() has been ENTERED (not completed).

    Used to observe in-flight closes: the hang gate (close_wait) parks the
    close AFTER entry, so close_entered marks the exact window in which a
    dead-PG close is in progress.
    """

    def __init__(self) -> None:
        super().__init__()
        self.close_entered = asyncio.Event()

    async def close(self) -> None:
        self.close_entered.set()
        await super().close()


async def test_close_leader_owned_conns_clears_is_leader_before_closes_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Demotion is observable IMMEDIATELY: is_leader clears before the
    bounded closes complete, not after. On a dead PG the closes can park
    for seconds, and is_leader backs the taskq.maintenance_leader.is_leader
    gauge, /metrics, and the health report — a pod parked in a hung close
    must not keep advertising leadership while its replacement should be
    taking over (review N2)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = _CloseEntryConn()
    monitor = _CloseEntryConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # both close()s block forever from now on
    monitor.close_wait.clear()

    # Why the outer timeout: pre-fix the clear runs only after BOTH bounded
    # closes finish, so the RED state's assertion window is bounded by the
    # 0.05s shrink seam; the guard keeps a regression from hanging.
    async with asyncio.timeout(5):
        task = asyncio.create_task(leader._close_leader_owned_conns())
        # Wait until BOTH close()s have been ENTERED (not completed)...
        await cron.close_entered.wait()
        await monitor.close_entered.wait()
        # ...then, while the closes are still parked, the flag must ALREADY
        # be clear. Pre-fix it stays set until both closes finish → RED.
        assert not monitor.is_closed()
        assert not deps.is_leader.is_set()
        await task

    assert cron.terminated is True
    assert monitor.terminated is True
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()


async def test_close_leader_owned_conns_mid_run_false_logs_teardown_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Final teardown is not a mid-run emergency: called with mid_run=False,
    a hung close logs the ``conn-teardown-close-*`` family and NEVER the
    mid-run ``conn-close-*`` family, so an ordinary shutdown does not page
    as an unexpected mid-run close timeout (review N6)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader._close_leader_owned_conns(mid_run=False)

    assert cron.terminated is True
    assert cron.close_calls == 1
    timeout_events = [
        e for e in captured if e.get("event") == "conn-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 teardown timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    mid_run_events = [e for e in captured if str(e.get("event", "")).startswith("conn-close-")]
    assert mid_run_events == [], f"mid-run family must not fire on teardown: {captured!r}"


async def test_close_leader_owned_conns_default_mid_run_logs_mid_run_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default stays the mid-run family: the watchdog/election/cron
    demotion paths call without mid_run, and a conn so dead that even
    close() hung while the worker is alive must keep paging as
    ``conn-close-*`` (existing behaviour pinned — passes pre- and
    post-fix)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, _shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader._close_leader_owned_conns()

    assert cron.terminated is True
    assert cron.close_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "conn-close-timeout-terminating"]
    assert len(timeout_events) == 1, f"expected 1 mid-run timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    teardown_events = [
        e for e in captured if str(e.get("event", "")).startswith("conn-teardown-close-")
    ]
    assert teardown_events == [], f"teardown family must not fire mid-run: {captured!r}"


async def test_run_final_teardown_closes_leader_conns_with_teardown_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run()'s finally is final teardown, not a mid-run demotion: it must
    close leader-owned conns with the ``conn-teardown-close-*`` family so an
    ordinary shutdown of a leader pod never pages as a mid-run close
    failure. Drives run() end-to-end with shutdown pre-set — every loop
    observes the set event and exits immediately, so only the finally's
    close path does any work (review N6 wiring pin)."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader, deps, _backend, _, _, shutdown = await _make_leader()
    deps.is_leader.set()
    cron = FakeConn()
    monitor = FakeConn()
    leader._cron_conn = cron  # type: ignore[reportAttributeAccessIssue]
    leader._leader_monitor_conn = monitor  # type: ignore[reportAttributeAccessIssue]
    cron.close_wait.clear()  # close() blocks forever from now on
    shutdown.set()  # all loops exit immediately; only the finally does work

    with structlog.testing.capture_logs() as captured:
        # Why the outer timeout: if the 0.05s bound regresses, fail fast
        # instead of hanging until pytest-timeout.
        async with asyncio.timeout(5):
            await leader.run(shutdown)

    assert cron.terminated is True
    assert cron.close_calls == 1
    assert monitor.is_closed()
    assert monitor.terminated is False
    assert leader._cron_conn is None
    assert leader._leader_monitor_conn is None
    assert not deps.is_leader.is_set()
    timeout_events = [
        e for e in captured if e.get("event") == "conn-teardown-close-timeout-terminating"
    ]
    assert len(timeout_events) == 1, f"expected 1 teardown timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "cron_conn"
    mid_run_events = [e for e in captured if str(e.get("event", "")).startswith("conn-close-")]
    assert mid_run_events == [], f"mid-run family must not fire on teardown: {captured!r}"


async def test_drop_leader_conn_terminates_hung_taskq_owned_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung close on a TaskQ-owned leader_conn is terminated after the
    bound and deps.leader_conn is nulled, so the watchdog/election drop
    path completes instead of stalling."""
    import taskq.worker.leader as leader_mod

    monkeypatch.setattr(leader_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, _shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True
    leader_conn.close_wait.clear()  # close() blocks forever from now on

    # Why the outer timeout: pre-fix this awaited conn.close() unbounded, so
    # the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        await leader._drop_leader_conn(reason="test_hung_close")

    assert leader_conn.terminated is True
    assert leader_conn.close_calls == 1
    assert deps.leader_conn is None


async def test_drop_leader_conn_fast_close_not_terminated() -> None:
    """TaskQ-owned leader_conn with a healthy close(): closed once, never
    terminated, reference dropped. Pins the no-regression behaviour
    (passes pre- and post-fix)."""
    leader_conn = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, _shutdown = await _make_leader(leader_conn=leader_conn)
    deps.owns_leader_conn = True

    await leader._drop_leader_conn(reason="test_fast_close")

    assert leader_conn.is_closed()
    assert leader_conn.close_calls == 1
    assert leader_conn.terminated is False
    assert deps.leader_conn is None


# ── Keepalive applied to factory-built leader connections ───────────────


async def test_factory_built_leader_conns_get_keepalive(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Factory-built leader/monitor/cron conns get the worker's TCP
    keepalive policy applied. The DSN path gets it inside
    open_dedicated_conn; the factory path bypasses that helper, so
    _open_leader_conn/_open_dedicated_conn must apply it explicitly."""
    leader, deps, _backend, _, _, shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn = None
    deps.is_leader.clear()

    async def factory() -> FakeConn:
        return FakeConn(fetchval_result=True)

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    import taskq.worker.leader as leader_mod

    keepalive_calls: list[str] = []

    def fake_keepalive(conn: object, *, label: str) -> bool:
        keepalive_calls.append(label)
        return True

    monkeypatch.setattr(leader_mod, "apply_keepalive_to_conn", fake_keepalive)

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert keepalive_calls == ["leader", "leader_monitor_conn", "cron_conn"]


# ── Fail-fast when no rebuild path exists ───────────────────────────────


async def test_open_leader_conn_fails_fast_without_factory_or_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """With no leader_conn_factory and pg_dsn_direct None, _open_leader_conn
    must fail fast with RuntimeError — never asyncpg.connect(str(None)),
    which would DNS-retry the literal host 'None' forever. Unreachable via
    open_worker_deps (startup validation forbids it); belt-and-braces for
    hand-built WorkerDeps."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn_factory = None
    deps.settings.pg_dsn_direct = None

    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(f"open_dedicated_conn must not be called (dsn={dsn!r})")

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    with pytest.raises(RuntimeError, match="cannot rebuild leader connection"):
        await leader._open_leader_conn()


async def test_open_dedicated_conn_fails_fast_without_factory_or_dsn(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Same fail-fast for the monitor/cron dedicated-conn open path."""
    leader, deps, _backend, _, _, _shutdown = await _make_leader(monkeypatch=monkeypatch)
    deps.leader_conn_factory = None
    deps.settings.pg_dsn_direct = None

    import taskq.worker.leader as leader_mod

    async def fail_if_called(
        dsn: str,
        *,
        label: str = "",
        apply_keepalive: bool = True,
        command_timeout: float | None = None,
    ) -> FakeConn:
        raise AssertionError(f"open_dedicated_conn must not be called (dsn={dsn!r})")

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    with pytest.raises(RuntimeError, match="cannot rebuild leader_monitor_conn"):
        await leader._open_dedicated_conn("leader_monitor_conn")


# ── Leadership gap-window after reload ──────────────────────────────────


async def test_reelection_survives_advisory_lock_gap_window(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """After reload closes leader_conn, the first pg_try_advisory_lock on the
    rebuilt conn can return False — the old session's lock release is still
    propagating. The election loop must treat it as an ordinary lost
    election (clear is_leader, retry with the SAME rebuilt conn), not
    crash, and win on the next attempt."""
    original = FakeConn(fetchval_result=True)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=original,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    await wait_for_leader(deps)
    assert deps.is_leader.is_set()

    # Gap-window conn: first lock attempt loses, subsequent attempts win.
    lock_attempts = 0

    class GapWindowConn(FakeConn):
        async def fetchval(self, sql: str, *args: object) -> object:
            nonlocal lock_attempts
            if "pg_try_advisory_lock" in sql:
                lock_attempts += 1
                return lock_attempts > 1
            return await super().fetchval(sql, *args)

    gap_conn = GapWindowConn()
    factory_calls = 0

    async def factory() -> FakeConn:
        nonlocal factory_calls
        factory_calls += 1
        return gap_conn

    deps.leader_conn_factory = factory  # type: ignore[assignment]

    # Simulate reload_credentials: close leader_conn, null it, is_leader still set.
    await original.close()
    deps.leader_conn = None

    # The demotion is latched on the leader_conn_died event, not sampled on
    # is_leader: the fast path logs it and clears is_leader in the same step
    # (no await between the log and the clear), while the re-win can re-set
    # the flag within one 10ms heartbeat — the transient False window can
    # close between 0.01s samples under load (same class as the tc2 gate
    # flake, tests/test_leader_chaos.py).
    with structlog.testing.capture_logs() as captured:
        # Bounded poll on the captured entries (a capture_logs list is
        # append-only state — no event exists to wait on).
        await wait_for_condition(
            lambda: any(e.get("kind") == "leader_conn_died" for e in captured),
            description="election loop never took the leader-conn-died demotion path after the null",
        )

    # ...then re-sets once the old session's lock release has propagated.
    await wait_for_leader(deps)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert deps.leader_conn is gap_conn
    assert lock_attempts >= 2  # first False (gap window), then True
    # The rebuilt conn is reused across lock attempts — the factory runs
    # once per conn ROLE (leader + monitor + cron), not per attempt.
    assert factory_calls == 3


# ── Watchdog shutdown park ────────────────────────────────────────────────


async def test_watchdog_loop_exits_on_shutdown_while_awaiting_leadership() -> None:
    """Shutdown must wake the watchdog's ``is_leader.wait()`` park.

    Regression: when PG is unreachable the watchdog drops leadership and
    parks on ``is_leader.wait()``; the election loop cannot re-elect (PG
    down), so nothing ever sets ``is_leader`` again. The TaskGroup in
    ``MaintenanceLeader.run`` then waits forever and the worker cannot
    exit — the hang the PG-restart chaos path exposes.
    """
    leader, _deps, _backend, _conn, _pool, shutdown = await _make_leader(is_leader=False)
    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    await asyncio.sleep(0.05)  # let the watchdog park on is_leader.wait()
    shutdown.set()
    # Pre-fix this parks forever; the race wakes it promptly.
    await asyncio.wait_for(task, timeout=2.0)


# ── Leader loops survive PG loss ──────────────────────────────────────────
#
# Regression (PG-restart chaos): a maintenance loop that lets an infra
# connection error escape crashes MaintenanceLeader.run's TaskGroup, which
# propagates into the worker's TaskGroup and cancels every sibling —
# WITHOUT setting shutdown_event. The heartbeat is cancelled before it can
# reach isolate_self, so no job is re-pended and no phase log is written.
# Every one of these loops must treat PG loss as transient, exactly as
# their already-guarded siblings do.


async def test_sweep_loop_survives_reclaim_connection_loss() -> None:
    """A dead PG in sweep 1 must not escape ``_sweep_loop``."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(*_args: object, **_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise OSError(-2, "Name or service not known")

    backend.reclaim_expired_locks = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # The survival assert must be preceded by proof the loop actually
    # faced the failure: the sweep attempts fire at loop entry, but a
    # fixed window can contain zero ticks under scheduler starvation,
    # and ``not task.done()`` would then pass without the dead-PG path
    # having run at all.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the sweep loop never attempted reclaim_expired_locks against the dead PG",
    )
    assert not task.done(), f"sweep loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_sweep_loop_survives_deadline_sweep_connection_loss() -> None:
    """A dead PG in sweep 2 must not escape ``_sweep_loop``."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(*_args: object, **_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise asyncpg.InterfaceError("connection is closed")

    backend.deadline_sweep = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # Same proven-window discipline as the reclaim test above: the
    # survival assert is meaningless unless the loop first faced the
    # dead-PG call.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the sweep loop never attempted deadline_sweep against the dead PG",
    )
    assert not task.done(), f"sweep loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_scheduled_wake_loop_survives_connection_loss() -> None:
    """A dead PG in ``scheduled_to_pending`` must not escape the wake loop."""
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)

    dead_pg_calls = 0

    async def _dead_pg(**_kw: object) -> int:
        nonlocal dead_pg_calls
        dead_pg_calls += 1
        raise OSError(-2, "Name or service not known")

    backend.scheduled_to_pending = _dead_pg  # type: ignore[method-assign]  # Why: test-only injection of a dead-PG failure.

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    # The wake loop attempts its first sweep at loop entry, then parks
    # on its 1s tick — the bounded wait proves the attempt happened
    # before the survival assert speaks.
    await wait_for_condition(
        lambda: dead_pg_calls >= 1,
        description="the scheduled-wake loop never attempted scheduled_to_pending against the dead PG",
    )
    assert not task.done(), f"wake loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_scheduled_wake_loop_survives_notify_connection_loss() -> None:
    """A dead PG in the post-promotion ``pg_notify`` must not escape."""
    # FakePool(fail_acquire_with=...) raises the dead-PG error at the
    # acquire seam and counts every attempt: the wake loop only reaches
    # the acquire after scheduled_to_pending reports a promotion, so a
    # counted acquire is the proof the loop faced the failure.
    dead_pool = FakePool(fail_acquire_with=OSError(-2, "Name or service not known"))
    leader, _deps, backend, _conn, _pool, shutdown = await _make_leader(
        is_leader=True,
        dispatcher_pool=dead_pool,
    )
    backend.scheduled_to_pending = lambda **_kw: asyncio.sleep(0, result=3)  # type: ignore[method-assign]  # Why: async stub returning a positive promotion count.

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for_condition(
        lambda: dead_pool.acquire_count >= 1,
        description="the scheduled-wake loop never reached its post-promotion pg_notify against the dead pool",
    )
    assert not task.done(), f"wake loop died: {task.exception() if task.done() else None!r}"
    await _stop_after_tick(task, shutdown, delay=0.0)


async def test_election_upsert_survives_connection_loss(monkeypatch: Any) -> None:
    """A dead PG in the maintenance_leader UPSERT must not escape the election loop."""

    def _dead_pg() -> None:
        raise asyncpg.InterfaceError("connection is closed")

    leader_conn = FakeConn(fetchval_result=True, on_execute=_dead_pg)
    leader, deps, _backend, _conn, _pool, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    task = asyncio.create_task(leader._election_loop(shutdown))
    # FakeConn records every execute before the failure hook fires, so
    # execute_calls is the proof the loop reached (and lost) the
    # maintenance_leader UPSERT — the failure this test exists for.
    await wait_for_condition(
        lambda: len(leader_conn.execute_calls) >= 1,
        description="the election loop never attempted the maintenance_leader UPSERT against the dead PG",
    )
    assert not task.done(), f"election loop died: {task.exception() if task.done() else None!r}"
    assert not deps.is_leader.is_set(), "leadership must not be claimed when the UPSERT failed"
    await _stop_after_tick(task, shutdown, delay=0.0)


# ── Scheduled-wake loop transient PG guard ─────────────────────────────────


async def test_scheduled_wake_loop_survives_transient_pg_errors() -> None:
    """Transient PG failures from ``scheduled_to_pending`` must not escape
    into ``MaintenanceLeader.run``'s TaskGroup (same defect class as the
    unguarded reclaim/deadline sweeps)."""

    class _DeadPgBackend:
        """Backend whose ``scheduled_to_pending`` raises the dead-PG error
        class, counting attempts so the survival assert is preceded by
        proof the loop faced the failure.

        Argless to match the production call site — the sweep's
        server-side clock predicate is the single arbiter, so a ``now``
        parameter would TypeError into the generic backstop instead of
        the transient branch this test pins.
        """

        def __init__(self) -> None:
            self.scheduled_to_pending_calls = 0

        async def scheduled_to_pending(self) -> int:
            self.scheduled_to_pending_calls += 1
            raise OSError(111, "Connect call failed")

    backend = _DeadPgBackend()
    deps = _make_deps(is_leader=True)
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        backend,  # type: ignore[arg-type]  # Why: stub satisfying only the called method.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await wait_for_condition(
        lambda: backend.scheduled_to_pending_calls >= 1,
        description="the scheduled-wake loop never attempted scheduled_to_pending against the dead PG",
    )
    assert not task.done(), "scheduled-wake loop died on a transient PG error"
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── Gated-loop liveness registration ─────────────────────────────────────


async def test_watchdog_loop_forgets_tick_registration_on_demotion() -> None:
    """Demotion must drop the ``leader.watchdog`` liveness registration.

    Regression: the tick is stamped inside the ``is_leader``-gated inner
    loop, so on demotion (probe failure -> is_leader cleared -> break) the
    loop re-parks on ``is_leader.wait()`` and stops ticking through no
    fault of its own. A lingering registration goes stale while parked,
    so watchdog detector 2 would force-exit a perfectly healthy non-leader
    worker ~grace seconds after every ordinary leadership change. The
    ``forget`` must therefore run where the inner loop is LEFT, not after
    the park (which only returns once is_leader is set again).
    """
    from taskq.worker._watchdog import LoopLiveness

    def _boom() -> None:
        raise OSError("pg gone")

    leader, deps, _backend, _conn, _pool, shutdown = await _make_leader(is_leader=True)
    now = [1000.0]
    deps.liveness = LoopLiveness(clock=lambda: now[0])
    # Drive the real demotion path: the monitor probe raises, which clears
    # is_leader via _close_leader_owned_conns and breaks the inner loop.
    leader._leader_monitor_conn = FakeConn(on_fetchval=_boom)  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    task = asyncio.create_task(leader._watchdog_loop(shutdown))
    await asyncio.sleep(0.05)
    assert not deps.is_leader.is_set(), "probe failure should have demoted this worker"

    await asyncio.sleep(0.05)  # the loop has re-parked as a non-leader
    now[0] += 3600.0  # far beyond any staleness budget
    stale = deps.liveness.stale()

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert stale == [], (
        f"stale registrations after demotion would force-exit a healthy worker: {stale}"
    )


async def test_close_leader_owned_conns_identity_guard() -> None:
    """_close_leader_owned_conns must only null the attribute if it still
    points to the SAME connection object — not a fresh one created by the
    election loop during the close suspension.

    Simulates the race directly: start the bounded close of the stale
    conn, suspend it mid-``await`` (via ``close_wait``), assign a fresh
    conn to the same attribute while suspended, then let the close
    complete. The fresh conn must survive - the attribute must still
    point at it, not be nulled out from under the election loop.
    """
    leader, _deps, _backend, _leader_conn, _dp, _shutdown = await _make_leader()

    # _CloseEntryConn marks the exact point close() is entered — the
    # bounded wait for the suspended close, never a sleep-poll hoping
    # the loop reached it.
    stale_conn = _CloseEntryConn()
    stale_conn.close_wait.clear()  # close() blocks until we release it below
    leader._cron_conn = stale_conn  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    close_task = asyncio.create_task(leader._close_leader_owned_conns())

    # Wait until the close is suspended inside stale_conn.close().
    await wait_for(stale_conn.close_entered)
    assert stale_conn.close_calls == 1, "close() was never entered"

    # While the close is suspended, the election loop creates and assigns
    # a fresh connection to the same attribute.
    fresh_conn = FakeConn()
    leader._cron_conn = fresh_conn  # type: ignore[assignment]  # Why: FakeConn is the asyncpg.Connection stand-in used throughout this module.

    # Let the suspended close() complete.
    stale_conn.close_wait.set()
    await close_task

    assert leader._cron_conn is not None, (
        "identity guard regressed: fresh conn created during the close "
        "suspension was nulled out, orphaning it and busy-spinning"
    )
    assert leader._cron_conn is fresh_conn
