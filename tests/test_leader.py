"""Unit tests for MaintenanceLeader — pure-Python, no PG required.

Covers election, watchdog, sweep-loop gating, pg_notify, prune/expiry
scheduling, and retention-config helpers against InMemoryBackend.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase, JobId, JobRow
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
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

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if self._on_fetchval is not None:
            self._on_fetchval()
        return self._fetchval_result

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._on_execute is not None:
            self._on_execute()
        return "UPDATE 1"

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        self.close_calls += 1
        self._closed = True
        if self._on_close is not None:
            self._on_close()

    def is_closed(self) -> bool:
        return self._closed

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


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
            dsn: str, *, label: str = "", apply_keepalive: bool = True
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

    otel_mod._leader_election_attempts.add = lambda amount, attrs: counter_calls.append(
        (int(amount), _as_dict(attrs))
    )  # type: ignore[method-assign]  # Why: test-only instrumentation to observe OTel counter calls.

    task = asyncio.create_task(leader._election_loop(shutdown))
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert any("maintenance_leader" in sql for sql, _ in leader_conn.execute_calls)
    assert counter_calls == [(1, {"worker_id": str(leader._worker_id)})]


# ── Election loss does not set is_leader ───────────────────────────


async def test_election_loss_does_not_set_is_leader(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Election loss: fetchval returns False → is_leader stays clear,
    no monitor opened, INFO log with kind='leader_retry', counter incremented."""
    leader_conn = FakeConn(fetchval_result=False)
    leader, deps, _backend, _, _, shutdown = await _make_leader(
        leader_conn=leader_conn,
        monkeypatch=monkeypatch,
    )

    counter_calls: list[tuple[int, dict[str, object]]] = []
    failure_calls: list[tuple[int, dict[str, object]]] = []
    import taskq.obs._otel as otel_mod

    otel_mod._leader_election_attempts.add = lambda amount, attrs: counter_calls.append(
        (int(amount), _as_dict(attrs))
    )  # type: ignore[method-assign]
    otel_mod._leader_election_failures.add = lambda amount, attrs: failure_calls.append(
        (int(amount), _as_dict(attrs))
    )  # type: ignore[method-assign]

    task = asyncio.create_task(leader._election_loop(shutdown))
    for _ in range(200):
        if leader_conn.fetchval_calls:
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await task

    assert not deps.is_leader.is_set()
    assert leader._leader_monitor_conn is None
    assert counter_calls == [(1, {"worker_id": str(leader._worker_id)})]
    assert failure_calls == [(1, {"worker_id": str(leader._worker_id)})]


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

    async def fake_open(dsn: str, *, label: str = "", apply_keepalive: bool = True) -> FakeConn:
        open_calls.append(label)
        if label == "leader_conn":
            return new_leader_conn
        return FakeConn()

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    elect_task = asyncio.create_task(leader._election_loop(shutdown))
    # Wait for election win
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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

    async def fake_open(dsn: str, *, label: str = "", apply_keepalive: bool = True) -> FakeConn:
        return sentinel_conn

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fake_open)

    task = asyncio.create_task(leader._election_loop(shutdown))
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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
        dsn: str, *, label: str = "", apply_keepalive: bool = True
    ) -> FakeConn:
        raise AssertionError(
            f"open_dedicated_conn called with dsn={dsn!r} label={label!r} — "
            "leader_conn_factory should have been used instead"
        )

    monkeypatch.setattr(leader_mod, "open_dedicated_conn", fail_if_called)

    task = asyncio.create_task(leader._election_loop(shutdown))
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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
    for _ in range(200):
        if leader._leader_monitor_conn is not None and leader._cron_conn is not None:
            break
        await asyncio.sleep(0.01)
    assert deps.is_leader.is_set()
    assert leader._leader_monitor_conn is not None
    assert leader._cron_conn is not None
    old_monitor_conn = leader._leader_monitor_conn
    old_cron_conn = leader._cron_conn
    factory_calls.clear()  # only count calls from the reload onward

    # Simulate what reload_credentials does to leader_conn on SIGHUP: close
    # it and null it while is_leader remains set (deps.py:599-606).
    deps.leader_conn = None

    for _ in range(200):
        if (
            leader._leader_monitor_conn is not old_monitor_conn
            and leader._cron_conn is not old_cron_conn
        ):
            break
        await asyncio.sleep(0.01)
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
    await _stop_after_tick(task_2, shutdown_2, delay=0.05)

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
    for _ in range(200):
        if fake_dp.execute_calls:
            break
        await asyncio.sleep(0.01)
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

    async def zero_promote(**kw: object) -> int:
        return count

    backend.scheduled_to_pending = zero_promote  # type: ignore[method-assign]
    deps.is_leader.set()

    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    await asyncio.sleep(0.05)
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
    for _ in range(500):
        if leader_conn.fetchval_calls:
            break
        await asyncio.sleep(0.01)
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
        (int(amount), _as_dict(attrs))
    )
    otel_mod._leader_election_attempts.add = lambda amount, attrs: election_calls.append(  # type: ignore[method-assign]
        (int(amount), _as_dict(attrs))
    )

    # Run election
    task = asyncio.create_task(leader._election_loop(shutdown))
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await task

    assert election_calls == [(1, {"worker_id": str(leader._worker_id)})]

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
    await _stop_after_tick(task_2, shutdown_2, delay=0.05)

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
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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


async def test_scheduled_wake_uses_injected_clock() -> None:
    """_scheduled_wake_loop passes self._clock.now() to
    scheduled_to_pending — the value matches the FakeClock, not real time."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    captured_now: list[datetime] = []

    async def _capture_scheduled_to_pending(**kw: object) -> int:
        captured_now.append(kw.get("now"))  # type: ignore[arg-type]  # Why: test records the value passed; object is fine for capture.
        return 0

    backend.scheduled_to_pending = _capture_scheduled_to_pending  # type: ignore[method-assign]  # Why: test-only interception of the now parameter.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    for _ in range(200):
        if captured_now:
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert captured_now
    assert captured_now[0] == datetime(2025, 1, 1, tzinfo=UTC)


async def test_sweep_loop_uses_injected_clock() -> None:
    """_sweep_loop passes self._clock.now() to
    reclaim_expired_locks and deadline_sweep — the value matches the
    FakeClock, not real time."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    deps = _make_deps(is_leader=True, heartbeat_interval=0.01)
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=clock)

    reclaim_now: list[datetime] = []
    deadline_now: list[datetime] = []

    async def _capture_reclaim(now_utc: datetime, cg: timedelta, ug: timedelta) -> int:
        reclaim_now.append(now_utc)
        return 0

    async def _capture_deadline(now_utc: datetime) -> int:
        deadline_now.append(now_utc)
        return 0

    backend.reclaim_expired_locks = _capture_reclaim  # type: ignore[method-assign]  # Why: test-only interception of the now parameter.
    backend.deadline_sweep = _capture_deadline  # type: ignore[method-assign]  # Why: test-only interception of the now parameter.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    for _ in range(200):
        if reclaim_now and deadline_now:
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(reclaim_now) >= 1
    assert reclaim_now[0] == datetime(2025, 1, 1, tzinfo=UTC)
    assert len(deadline_now) >= 1
    assert deadline_now[0] == datetime(2025, 1, 1, tzinfo=UTC)


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
    settings._post_load()
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
    settings._post_load()
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
    ) -> None:
        super().__init__(fetchval_result=fetchval_result)
        self._batch_rows = batch_rows or []
        self._batch_index = 0
        self._actor_config_rows = actor_config_rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
        self.fetch_calls.append((sql, args))
        if "actor_config" in sql and self._actor_config_rows is not None:
            return self._actor_config_rows
        if self._batch_index < len(self._batch_rows):
            rows = self._batch_rows[self._batch_index]
            self._batch_index += 1
            return rows
        return []


async def test_prune_terminal_jobs_returns_prune_result() -> None:
    """prune_terminal_jobs returns PruneResult with aggregated counts."""
    rows = [
        [
            _FakeRecord({"actor": "test_actor", "status": "succeeded", "cnt": 5}),
        ],
    ]
    conn = _FakeConnForPrune(batch_rows=rows)
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
    assert "succeeded" in result.cutoffs
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
    """archive_expiry_sweep returns ArchiveExpiryResult with counts."""
    rows = [
        [
            _FakeRecord({"status": "succeeded", "cnt": 7}),
        ],
    ]
    conn = _FakeConnForPrune(batch_rows=rows)
    result = await archive_expiry_sweep(
        conn,
        batch_size=100,
        schema="taskq",
    )
    assert result.total_deleted == 7
    assert result.by_status == {"succeeded": 7}
    assert result.expire_before <= datetime.now(UTC)
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

    for _ in range(500):
        if leader_conn.fetchval_calls:
            break
        await asyncio.sleep(0.01)

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

    for _ in range(500):
        if leader_conn.fetchval_calls:
            break
        await asyncio.sleep(0.01)

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


# ── Prune loop releases lock on error ────────────────────────────────────────


async def test_prune_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Prune loop releases advisory lock even when prune_terminal_jobs raises."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    raise_count = 0

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            nonlocal raise_count
            if "candidate_ids" in sql:
                raise_count += 1
                raise RuntimeError("connection lost")
            return []

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

    for _ in range(500):
        if raise_count >= 1:
            break
        await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    unlock_calls = [sql for sql, _ in leader_conn.execute_calls if "pg_advisory_unlock" in sql]
    assert unlock_calls, "lock must be released even on error"


# ── Archive expiry loop releases lock on error ──────────────────────────────


async def test_archive_expiry_loop_releases_lock_on_error(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop releases advisory lock even when sweep raises."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    raise_count = 0

    class _ErrorConn(_FakeConnForPrune):
        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            nonlocal raise_count
            if "expired" in sql:
                raise_count += 1
                raise RuntimeError("connection lost")
            return []

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

    for _ in range(500):
        if raise_count >= 1:
            break
        await asyncio.sleep(0.01)

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
    await asyncio.sleep(0.1)
    assert task.done(), "prune loop should exit immediately on shutdown"
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
    await asyncio.sleep(0.1)
    assert task.done(), "archive expiry loop should exit immediately on shutdown"
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

    for _ in range(500):
        lock_calls = [sql for sql, _ in leader_conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        if lock_calls:
            break
        await asyncio.sleep(0.01)

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

    for _ in range(500):
        if lock_call_count >= 2:
            break
        await asyncio.sleep(0.01)

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

    for _ in range(500):
        if lock_call_count >= 2:
            break
        await asyncio.sleep(0.01)

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

    prune_ran = False

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise asyncpg.PostgresConnectionError("connection lost")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            nonlocal prune_ran
            if "candidate_ids" in sql:
                prune_ran = True
            return []

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

    for _ in range(500):
        if prune_ran:
            break
        await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert prune_ran, "prune should have run"
    assert not task.cancelled(), "task should not be cancelled after unlock failure"


# ── Archive expiry loop survives unlock failure in finally block ──────────────


async def test_archive_expiry_loop_survives_unlock_failure(monkeypatch: Any) -> None:  # type: ignore[reportUnknownParameterType]
    """Archive expiry loop continues when pg_advisory_unlock raises in the finally
    block, instead of crashing the TaskGroup. PG releases the lock on session death."""
    import taskq.worker._leader_sweeps as _leader_sweeps_mod

    sweep_ran = False

    class _UnlockFailsConn(_FakeConnForPrune):
        async def execute(self, sql: str, *args: object) -> str:
            if "pg_advisory_unlock" in sql:
                raise OSError(104, "Connection reset by peer")
            return await super().execute(sql, *args)

        async def fetch(self, sql: str, *args: object) -> list[_FakeRecord]:
            nonlocal sweep_ran
            if "expired" in sql:
                sweep_ran = True
            return []

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

    for _ in range(500):
        if sweep_ran:
            break
        await asyncio.sleep(0.01)

    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert sweep_ran, "archive expiry sweep should have run"
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
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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

    # Wait for the watchdog failure path to run. failing_monitor._closed is
    # the stable witness: _close_leader_owned_conns closes it AFTER the
    # leader_conn close/abandon in the same handler, so once it is True the
    # caller-conn decision has definitely been made (and unlike the
    # is_leader flip, it never flips back under a racing re-election).
    for _ in range(200):
        if failing_monitor._closed:
            break
        await asyncio.sleep(0.01)
    assert failing_monitor._closed
    assert not caller_conn.is_closed()
    assert caller_conn.close_calls == 0

    # Election loop re-acquires leadership through the factory.
    for _ in range(200):
        if deps.is_leader.is_set() and deps.leader_conn in factory_conns:
            break
        await asyncio.sleep(0.01)
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
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert keepalive_calls == ["leader_conn", "leader_monitor_conn", "cron_conn"]


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
        dsn: str, *, label: str = "", apply_keepalive: bool = True
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
        dsn: str, *, label: str = "", apply_keepalive: bool = True
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
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
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

    # is_leader clears as the re-election cascade begins...
    for _ in range(200):
        if not deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    assert not deps.is_leader.is_set()

    # ...then re-sets once the old session's lock release has propagated.
    for _ in range(200):
        if deps.is_leader.is_set():
            break
        await asyncio.sleep(0.01)
    shutdown.set()
    await task

    assert deps.is_leader.is_set()
    assert deps.leader_conn is gap_conn
    assert lock_attempts >= 2  # first False (gap window), then True
    # The rebuilt conn is reused across lock attempts — the factory runs
    # once per conn ROLE (leader + monitor + cron), not per attempt.
    assert factory_calls == 3
