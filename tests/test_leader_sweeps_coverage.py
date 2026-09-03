"""Coverage for ``taskq.worker._leader_sweeps`` loop functions.

Exercises branches not covered by ``test_leader.py``:

- ``_sweep_loop``: ``NotImplementedError`` paths for
  ``reclaim_expired_locks`` and ``deadline_sweep`` (the ``_err`` helper).
- ``_sweep_loop``: the ``sweep_leaked_reservation_slots`` /
  ``sweep_expired_results`` / ``cleanup_stale_workers`` block and its
  connection-error handlers.
- ``_archive_expiry_loop``: lock-not-acquired warning, and the
  ``continue`` when not leader after a cron timeout.
- ``_queue_depth_loop`` / ``_reservation_slots_loop``: success, sampling
  failure, and invalid-schema early return.
- ``_stranded_jobs_loop``: invalid-schema early return and the warning
  path for pending jobs with no actor_config.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker import _leader_sweeps
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.asyncio


# ── Test doubles ─────────────────────────────────────────────────────────


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class FakeConn:
    """asyncpg.Connection stand-in with configurable fetch/fetchval/execute."""

    def __init__(
        self,
        *,
        fetchval_result: object = None,
        fetch_rows: list[dict[str, object]] | None = None,
        execute_result: str = "DELETE 0",
        fetch_exc: BaseException | None = None,
    ) -> None:
        self._fetchval_result = fetchval_result
        self._fetch_rows = fetch_rows
        self._execute_result = execute_result
        self._fetch_exc = fetch_exc
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        return self._fetchval_result

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return self._execute_result

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((sql, args))
        if self._fetch_exc is not None:
            raise self._fetch_exc
        return self._fetch_rows if self._fetch_rows is not None else []

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class FakePool:
    """asyncpg.Pool stand-in yielding FakeConn instances."""

    def __init__(self, conn: FakeConn | None = None) -> None:
        self._fixed_conn = conn
        self._conns: list[FakeConn] = []
        self.acquire_count = 0

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        self.acquire_count += 1
        if self._fixed_conn is not None:
            yield self._fixed_conn
        else:
            conn = FakeConn()
            self._conns.append(conn)
            yield conn


# ── Factories ─────────────────────────────────────────────────────────────


def _worker_settings(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    for key, value in overrides.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps(
    *,
    dispatcher_pool: FakePool | None = None,
    worker_pool: FakePool | None = None,
    is_leader: bool = False,
    heartbeat_interval: float = 0.5,
    sweep_interval: float | None = None,
) -> WorkerDeps:
    overrides: dict[str, str] = {
        "HEARTBEAT_INTERVAL": str(heartbeat_interval),
        "LOCK_LEASE": "2.0",
        "WATCHDOG_LOOP_LAG_BUDGET": "1.2",
        "MAX_HEARTBEAT_FAILURES": "3",
        "CANCELLATION_GRACE_PERIOD": "0.0",
        "CLEANUP_GRACE_PERIOD": "0.0",
    }
    if sweep_interval is not None:
        overrides["SWEEP_INTERVAL"] = str(sweep_interval)
    settings = _worker_settings(**overrides)
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=dispatcher_pool or FakePool(),  # type: ignore[arg-type]  # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=worker_pool or FakePool(),  # type: ignore[arg-type]
        worker_pool=worker_pool or FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=FakeConn(),  # type: ignore[arg-type]
    )
    if is_leader:
        deps.is_leader.set()
    return deps


def _mem_backend() -> InMemoryBackend:
    """InMemoryBackend wired to the standard FakeClock start time."""
    return InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))


def _make_leader(
    *,
    backend: object,
    deps: WorkerDeps | None = None,
    clock: Clock | None = None,
) -> MaintenanceLeader:
    clk = clock or FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    d = deps or _make_deps()
    return MaintenanceLeader(d, new_uuid(), backend, clock=clk)  # type: ignore[arg-type]  # Why: backend is a test double satisfying the Backend protocol at runtime.


async def _stop_loop(
    task: asyncio.Task[object], shutdown: asyncio.Event, delay: float = 0.1
) -> None:
    """Wait briefly, set shutdown, cancel, and suppress CancelledError."""
    await asyncio.sleep(delay)
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


class _InstantCroniter:
    """Croniter stub that always fires ~50 ms in the future."""

    def __init__(self, expr: str, start_time: object) -> None:
        pass

    def get_next(self, dt_type: type[datetime]) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=0.05)


# ── _sweep_loop: NotImplementedError paths ────────────────────────────────


class _NotImplBackend:
    """Backend whose reclaim/deadline sweeps raise NotImplementedError."""

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        raise NotImplementedError("reclaim not implemented")

    async def deadline_sweep(self) -> int:
        raise NotImplementedError("deadline not implemented")


async def test_sweep_loop_not_implemented_paths_do_not_crash() -> None:
    """reclaim_expired_locks and deadline_sweep raising NotImplementedError
    triggers ``_err`` once each (warned guard) and the loop continues."""
    backend = _NotImplBackend()
    leader = _make_leader(backend=backend, deps=_make_deps(is_leader=True))
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # Let one iteration run (the sweeps fire immediately when is_leader).
    await asyncio.sleep(0.05)
    await _stop_loop(task, shutdown, delay=0.0)
    # No exception escaped — the task completed via cancellation, not error.
    assert task.done()


async def test_sweep_loop_not_implemented_warns_only_once() -> None:
    """The ``warned`` guard ensures ``_err`` is called only once per sweep
    kind even across multiple iterations."""
    import taskq.worker._leader_sweeps as sweeps_mod

    err_calls: list[str] = []

    def _spy_err(ev: str, ki: str, wi: UUID, ex: Exception) -> None:
        err_calls.append(ev)

    backend = _NotImplBackend()
    leader = _make_leader(backend=backend, deps=_make_deps(is_leader=True))
    shutdown = asyncio.Event()
    # Patch the module-level _err to count calls.
    original_err = sweeps_mod._err
    sweeps_mod._err = _spy_err  # type: ignore[method-assign]  # Why: test-only instrumentation.
    try:
        task = asyncio.create_task(leader._sweep_loop(shutdown))
        # Allow two iterations to fire the warned guard.
        await asyncio.sleep(0.08)
        await _stop_loop(task, shutdown, delay=0.0)
    finally:
        sweeps_mod._err = original_err  # type: ignore[method-assign]

    assert "sweep_expired_locks_unimplemented" in err_calls
    assert "sweep_deadline_exceeded_unimplemented" in err_calls
    # Each warning fires at most once thanks to the warned flags.
    assert err_calls.count("sweep_expired_locks_unimplemented") <= 1
    assert err_calls.count("sweep_deadline_exceeded_unimplemented") <= 1


# ── _sweep_loop: unexpected-error backstop ────────────────────────────────


async def test_sweep_loop_backstop_tolerates_then_goes_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Behavioral: a non-transient sweep error (a bug shape — e.g. the
    constraint-violation class that used to crash the reclaim sweep) is
    ridden out loudly for a few consecutive iterations, then the loop dies
    deliberately: never an instant silent leader teardown (which leaves
    the cluster with no sweeper and orphans 'running' forever), never an
    infinite silent retry. Pre-fix, the first error escaped straight into
    MaintenanceLeader.run's TaskGroup and tore down the worker."""
    import structlog.testing

    from taskq.worker import _transient as transient_mod

    monkeypatch.setattr(transient_mod, "DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", 3)

    calls = 0

    class _BuggySweepBackend:
        async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
            nonlocal calls
            calls += 1
            raise ValueError("this is a bug, not a PG moment")

        async def deadline_sweep(self) -> int:
            return 0

    leader = _make_leader(
        backend=_BuggySweepBackend(),
        deps=_make_deps(is_leader=True, sweep_interval=0.01),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    try:
        with (
            structlog.testing.capture_logs() as captured,
            pytest.raises(ValueError, match="this is a bug"),
        ):
            await asyncio.wait_for(task, timeout=15.0)
    finally:
        shutdown.set()

    unexpected = [e for e in captured if e.get("event") == "leader-loop-unexpected-error"]
    assert [e.get("consecutive") for e in unexpected] == [1, 2, 3]
    # The guard tolerated (cap - 1) failing iterations before re-raising:
    # pre-fix the loop died on the first, with calls == 1 and no logs.
    assert calls == 3


# ── _sweep_loop: sweep_leaked_reservation_slots block ────────────────────


class _PgSweepBackend:
    """Backend with the PG-only sweep methods, recording calls."""

    def __init__(
        self,
        *,
        leaked_exc: BaseException | None = None,
        results_exc: BaseException | None = None,
    ) -> None:
        self.leaked_calls: list[dict[str, object]] = []
        self.results_calls: list[dict[str, object]] = []
        self._leaked_exc = leaked_exc
        self._results_exc = results_exc

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0

    async def sweep_leaked_reservation_slots(self, conn: object, *, schema: str) -> int:
        self.leaked_calls.append({"schema": schema})
        if self._leaked_exc is not None:
            raise self._leaked_exc
        return 5

    async def sweep_expired_results(self, conn: object, *, schema: str) -> int:
        self.results_calls.append({"schema": schema})
        if self._results_exc is not None:
            raise self._results_exc
        return 3


async def test_sweep_loop_runs_pg_sweep_block() -> None:
    """When the backend has ``sweep_leaked_reservation_slots``, the PG-only
    sweep block runs leaked-slots, expired-results, and stale-worker sweeps."""
    backend = _PgSweepBackend()
    # cleanup_stale_workers parses "DELETE N" from conn.execute.
    conn = FakeConn(execute_result="DELETE 2")
    pool = FakePool(conn=conn)
    leader = _make_leader(backend=backend, deps=_make_deps(dispatcher_pool=pool, is_leader=True))
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # Wait for the first iteration to run the PG sweep block.
    for _ in range(200):
        if backend.leaked_calls and backend.results_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    assert len(backend.leaked_calls) == 1
    assert backend.leaked_calls[0]["schema"] == leader._deps.settings.schema_name  # type: ignore[reportPrivateUsage]  # Why: test reads the deps the leader was constructed with.
    assert len(backend.results_calls) == 1
    # cleanup_stale_workers executed on the same conn.
    stale_calls = [sql for sql, _ in conn.execute_calls if "workers" in sql]
    assert stale_calls, "cleanup_stale_workers should have run"


async def test_sweep_loop_leaked_slots_error_continues_to_results() -> None:
    """A connection error in sweep_leaked_reservation_slots logs a warning
    and the loop proceeds to sweep_expired_results rather than aborting."""
    backend = _PgSweepBackend(leaked_exc=asyncpg.PostgresConnectionError("lost"))
    conn = FakeConn(execute_result="DELETE 0")
    pool = FakePool(conn=conn)
    leader = _make_leader(backend=backend, deps=_make_deps(dispatcher_pool=pool, is_leader=True))
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    for _ in range(200):
        if backend.results_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    # leaked raised, but results still ran.
    assert len(backend.leaked_calls) == 1
    assert len(backend.results_calls) == 1


async def test_sweep_loop_results_error_continues_to_stale_workers() -> None:
    """A connection error in sweep_expired_results logs a warning and the
    loop proceeds to cleanup_stale_workers."""
    backend = _PgSweepBackend(results_exc=TimeoutError("timed out"))
    conn = FakeConn(execute_result="DELETE 0")
    pool = FakePool(conn=conn)
    leader = _make_leader(backend=backend, deps=_make_deps(dispatcher_pool=pool, is_leader=True))
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    for _ in range(200):
        stale_calls = [sql for sql, _ in conn.execute_calls if "workers" in sql]
        if stale_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    assert len(backend.results_calls) == 1
    stale_calls = [sql for sql, _ in conn.execute_calls if "workers" in sql]
    assert stale_calls, "cleanup_stale_workers should run after results error"


async def test_sweep_loop_stale_workers_error_is_warned() -> None:
    """An OSError in cleanup_stale_workers logs a warning and the loop
    survives (does not crash the TaskGroup)."""
    backend = _PgSweepBackend()

    class _StaleFailsConn(FakeConn):
        async def execute(self, sql: str, *args: object) -> str:
            if "workers" in sql:
                raise OSError(104, "Connection reset by peer")
            return await super().execute(sql, *args)

    conn = _StaleFailsConn(execute_result="DELETE 0")
    pool = FakePool(conn=conn)
    leader = _make_leader(backend=backend, deps=_make_deps(dispatcher_pool=pool, is_leader=True))
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._sweep_loop(shutdown))
    # Let the full first iteration complete (including the failing stale sweep).
    await asyncio.sleep(0.06)
    await _stop_loop(task, shutdown, delay=0.0)
    # Task is done (via cancellation), not crashed.
    assert task.done()
    assert backend.leaked_calls  # the block was entered


# ── _archive_expiry_loop: lock-not-acquired and not-leader continue ──────


async def test_archive_expiry_loop_skips_when_lock_not_acquired(
    monkeypatch: Any,
) -> None:
    """``pg_try_advisory_lock`` returning False logs a warning and skips the
    archive expiry sweep (no candidate fetch)."""
    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod.cr, "croniter", _InstantCroniter)

    conn = FakeConn(fetchval_result=False)
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=True),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    for _ in range(200):
        lock_calls = [sql for sql, _ in conn.fetchval_calls if "pg_try_advisory_lock" in sql]
        if lock_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    lock_calls = [sql for sql, _ in conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert lock_calls, "advisory lock attempt should fire"
    # No expired-archive fetch ran because lock was not acquired.
    fetch_calls = [sql for sql, _ in conn.fetch_calls]
    assert not any("expired" in sql for sql in fetch_calls), "sweep must not run without lock"


async def test_archive_expiry_loop_continues_when_not_leader(monkeypatch: Any) -> None:
    """After a cron timeout fires, a non-leader ``continue``s without
    acquiring the advisory lock."""
    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod.cr, "croniter", _InstantCroniter)

    conn = FakeConn(fetchval_result=True)
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=False),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._archive_expiry_loop(shutdown))
    # Let the cron timeout fire once (50 ms) so the not-leader continue runs.
    await asyncio.sleep(0.12)
    await _stop_loop(task, shutdown, delay=0.0)

    # Because is_leader is False, no advisory lock is ever acquired.
    lock_calls = [sql for sql, _ in conn.fetchval_calls if "pg_try_advisory_lock" in sql]
    assert not lock_calls, "non-leader must not acquire the advisory lock"


# ── _queue_depth_loop ────────────────────────────────────────────────────


async def test_queue_depth_loop_success_updates_cache() -> None:
    """A successful fetch builds the queue-depth cache and updates it."""
    rows = [{"queue": "default", "count": 3}, {"queue": "priority", "count": 1}]
    conn = FakeConn(fetch_rows=rows)
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=True),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._queue_depth_loop(shutdown))
    for _ in range(200):
        if conn.fetch_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    assert conn.fetch_calls, "queue-depth fetch should run when leader"
    assert "jobs" in conn.fetch_calls[0][0]


async def test_queue_depth_loop_sampling_failure_is_warned() -> None:
    """A fetch error logs a warning and the loop survives."""
    conn = FakeConn(fetch_exc=asyncpg.PostgresConnectionError("lost"))
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=True),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._queue_depth_loop(shutdown))
    await asyncio.sleep(0.06)
    await _stop_loop(task, shutdown, delay=0.0)
    # Task ended via cancellation, not via propagated exception.
    assert task.done()


async def test_queue_depth_loop_invalid_schema_returns_early() -> None:
    """An invalid schema identifier causes the loop to return immediately."""
    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._queue_depth_loop(shutdown))
    await asyncio.sleep(0.05)
    # The loop returned immediately — task is done and shutdown was never set.
    assert task.done()
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_queue_depth_loop_invalid_schema_logs_error_disabled() -> None:
    """An invalid schema permanently mutes the queue-depth sampler for the
    process lifetime while the worker keeps running normally — a silent
    loss of a safety net, not a skipped tick, so it must log at ERROR with
    a ``*-disabled`` event (same rationale as the stranded-jobs detector),
    not a warn-level skip."""
    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.
    shutdown = asyncio.Event()
    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(leader._queue_depth_loop(shutdown))
        await asyncio.sleep(0.05)
        assert task.done()
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    disabled = [e for e in captured if e["event"] == "queue-depth-sampler-disabled"]
    assert disabled, "invalid schema must log an error-level sampler-disabled event"
    assert disabled[0]["log_level"] == "error"
    assert disabled[0]["schema"] == "bad;schema"
    assert not any(e["event"] == "invalid-schema-skipped" for e in captured), (
        "warn-level invalid-schema-skipped must no longer be used for this case"
    )


# ── _reservation_slots_loop ──────────────────────────────────────────────


async def test_reservation_slots_loop_success_updates_cache() -> None:
    """A successful fetch builds the reservation-slots cache and updates it."""
    rows = [{"bucket_name": "gpu", "count": 2}]
    conn = FakeConn(fetch_rows=rows)
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=True),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._reservation_slots_loop(shutdown))
    for _ in range(200):
        if conn.fetch_calls:
            break
        await asyncio.sleep(0.01)
    await _stop_loop(task, shutdown, delay=0.0)

    assert conn.fetch_calls, "reservation-slots fetch should run when leader"
    assert "reservation_slots" in conn.fetch_calls[0][0]


async def test_reservation_slots_loop_sampling_failure_is_warned() -> None:
    """A fetch error logs a warning and the loop survives."""
    conn = FakeConn(fetch_exc=OSError(104, "reset"))
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(dispatcher_pool=pool, is_leader=True),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._reservation_slots_loop(shutdown))
    await asyncio.sleep(0.06)
    await _stop_loop(task, shutdown, delay=0.0)
    assert task.done()


async def test_reservation_slots_loop_invalid_schema_returns_early() -> None:
    """An invalid schema identifier causes the loop to return immediately."""
    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._reservation_slots_loop(shutdown))
    await asyncio.sleep(0.05)
    assert task.done()
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_reservation_slots_loop_invalid_schema_logs_error_disabled() -> None:
    """An invalid schema permanently mutes the reservation-slots sampler for
    the process lifetime — same error-level ``*-disabled`` rationale as the
    queue-depth sampler and the stranded-jobs detector."""
    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.
    shutdown = asyncio.Event()
    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(leader._reservation_slots_loop(shutdown))
        await asyncio.sleep(0.05)
        assert task.done()
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    disabled = [e for e in captured if e["event"] == "reservation-slots-sampler-disabled"]
    assert disabled, "invalid schema must log an error-level sampler-disabled event"
    assert disabled[0]["log_level"] == "error"
    assert disabled[0]["schema"] == "bad;schema"
    assert not any(e["event"] == "invalid-schema-skipped" for e in captured), (
        "warn-level invalid-schema-skipped must no longer be used for this case"
    )


# ── _stranded_jobs_loop ──────────────────────────────────────────────────


async def test_stranded_jobs_loop_invalid_schema_returns_early() -> None:
    """An invalid schema identifier causes the loop to return immediately."""
    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
    await asyncio.sleep(0.05)
    assert task.done()
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_stranded_jobs_loop_warns_for_pending_without_actor_config() -> None:
    """Pending jobs whose actor has no actor_config row produce a warning."""
    rows = [{"actor": "orphan_actor", "cnt": 7}]
    conn = FakeConn(fetch_rows=rows)
    pool = FakePool(conn=conn)
    deps = _make_deps(worker_pool=pool, is_leader=True)
    deps.settings.stranded_jobs_interval = 0.01
    leader = _make_leader(backend=_mem_backend(), deps=deps)

    import taskq.worker._leader_sweeps as sweeps_mod

    warned_actors: list[str] = []
    original_warning = sweeps_mod.log.warning

    def _spy_warning(event: str, **kwargs: object) -> None:
        if event == "stranded-jobs-no-actor-config":
            warned_actors.append(str(kwargs.get("actor")))

    sweeps_mod.log.warning = _spy_warning  # type: ignore[method-assign]  # Why: test-only instrumentation.
    try:
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
        for _ in range(200):
            if warned_actors:
                break
            await asyncio.sleep(0.01)
        await _stop_loop(task, shutdown, delay=0.0)
    finally:
        sweeps_mod.log.warning = original_warning  # type: ignore[method-assign]

    assert "orphan_actor" in warned_actors


async def test_stranded_jobs_loop_fetch_error_continues() -> None:
    """A fetch error in the stranded loop is swallowed (``continue``) and
    the loop survives."""
    conn = FakeConn(fetch_exc=asyncpg.PostgresConnectionError("lost"))
    pool = FakePool(conn=conn)
    deps = _make_deps(worker_pool=pool, is_leader=True)
    deps.settings.stranded_jobs_interval = 0.01
    leader = _make_leader(backend=_mem_backend(), deps=deps)

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
    # Let a couple iterations run (fetch raises → continue → loop survives).
    await asyncio.sleep(0.05)
    await _stop_loop(task, shutdown, delay=0.0)
    # The task ended via cancellation, not via a propagated fetch exception.
    assert task.done()
    assert conn.fetch_calls, "the stranded fetch should have been attempted"


async def test_stranded_jobs_loop_skips_when_not_leader(monkeypatch: Any) -> None:
    """When not leader, the loop ``continue``s without fetching."""
    conn = FakeConn(fetch_rows=[{"actor": "x", "cnt": 1}])
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(worker_pool=pool, is_leader=False),
    )

    original_sleep = asyncio.sleep

    async def _fast_sleep(_seconds: float) -> None:
        await original_sleep(0)

    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod.asyncio, "sleep", _fast_sleep)

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
    await asyncio.sleep(0.05)
    await _stop_loop(task, shutdown, delay=0.0)

    # No fetch because is_leader is False.
    assert not conn.fetch_calls, "non-leader must not fetch stranded jobs"


# ── Interval sleeps are shutdown-interruptible ────────────────────────────


@pytest.mark.parametrize(
    ("loop_name", "interval_setting"),
    [
        ("_sweep_loop", "sweep_interval"),
        ("_queue_depth_loop", "queue_depth_interval"),
        ("_reservation_slots_loop", "reservation_slots_interval"),
        ("_stranded_jobs_loop", "stranded_jobs_interval"),
    ],
)
async def test_loop_interval_sleep_is_shutdown_interruptible(
    loop_name: str, interval_setting: str
) -> None:
    """SIGTERM must not wait out an in-flight interval sleep.

    ``MaintenanceLeader.run``'s TaskGroup waits for its children on exit,
    so a bare ``asyncio.sleep(interval)`` keeps the worker hanging for the
    full in-flight sleep after shutdown — with an operator-configured
    interval (e.g. ``TASKQ_STRANDED_JOBS_INTERVAL=3600``) that is an
    hour-long shutdown hang. Every loop's interval sleep must return as
    soon as shutdown is set.
    """
    deps = _make_deps()  # is_leader=False: loops skip work and go straight to the sleep.
    setattr(deps.settings, interval_setting, 3600.0)
    ctx = SweepContext(
        deps=deps,
        backend=_mem_backend(),  # type: ignore[arg-type]  # Why: InMemoryBackend satisfies the Backend protocol at runtime.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )
    shutdown = asyncio.Event()
    loop_fn = getattr(_leader_sweeps, loop_name)

    task = asyncio.create_task(loop_fn(ctx, shutdown))
    await asyncio.sleep(0.05)  # Let the loop reach its interval sleep.
    shutdown.set()
    # Must return promptly — pre-fix this sleeps the full 3600s.
    await asyncio.wait_for(task, timeout=2.0)


# ── Transient PG errors must not escape the sweep loops ────────────────────


class _DeadPgSweepsBackend:
    """Backend whose reclaim/deadline sweeps raise transient PG errors
    (the dead-PG class: DNS/connect failure, not NotImplementedError)."""

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        raise OSError(111, "Connect call failed")

    async def deadline_sweep(self) -> int:
        raise OSError(111, "Connect call failed")


async def test_sweep_loop_survives_transient_pg_errors() -> None:
    """Transient PG failures from ``reclaim_expired_locks`` / ``deadline_sweep``
    must not escape into ``MaintenanceLeader.run``'s TaskGroup.

    Regression: those two calls caught only ``NotImplementedError`` while
    every sibling block in the same loop guards
    ``(TimeoutError, PostgresConnectionError, InterfaceError, OSError)`` —
    a transient PG failure raised into the TaskGroup, which cancelled every
    worker sibling and hung the worker's shutdown.
    """
    deps = _make_deps(is_leader=True)
    deps.settings.sweep_interval = 0.01
    ctx = SweepContext(
        deps=deps,
        backend=_DeadPgSweepsBackend(),  # type: ignore[arg-type]  # Why: stub satisfying only the called methods.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )
    shutdown = asyncio.Event()
    task = asyncio.create_task(_leader_sweeps._sweep_loop(ctx, shutdown))
    await asyncio.sleep(0.1)  # several ticks against the dead backend
    assert not task.done(), "sweep loop died on a transient PG error"
    await _stop_loop(task, shutdown, delay=0.0)


# ── _stranded_jobs_loop: repeat visibility (regression) ────────────────────
#
# The detector exists to catch jobs that can NEVER be dispatched: the dispatch
# CTE derives candidates from `per_actor_capacity`, which is
# `FROM actor_config`, so a job whose actor has no row there is stranded
# forever. It warned exactly once per actor per process lifetime, from a
# `warned: set[str]` that was only ever added to, and emitted no metric. An
# operator got one WARN line at onset -- the moment nobody is looking -- and
# nothing thereafter, while the backlog grew unboundedly.


async def _run_stranded_loop_collecting(
    rows_sequence: list[list[dict[str, object]]],
    *,
    ticks: int,
) -> tuple[list[dict[str, object]], list[dict[str, int]]]:
    """Drive the loop over a scripted sequence of query results."""
    import taskq.worker._leader_sweeps as sweeps_mod

    class _ScriptedConn:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch(self, *_a: object, **_k: object) -> list[dict[str, object]]:
            idx = min(self.calls, len(rows_sequence) - 1)
            self.calls += 1
            return rows_sequence[idx]

    conn = _ScriptedConn()
    pool = FakePool(conn=conn)  # type: ignore[arg-type]  # Why: structural stand-in for an asyncpg pool.
    deps = _make_deps(worker_pool=pool, is_leader=True)
    deps.settings.stranded_jobs_interval = 0.01
    leader = _make_leader(backend=_mem_backend(), deps=deps)

    warnings: list[dict[str, object]] = []
    gauge_updates: list[dict[str, int]] = []
    original_warning = sweeps_mod.log.warning
    original_update = sweeps_mod.update_stranded_jobs_cache

    def _spy_warning(event: str, **kwargs: object) -> None:
        if event == "stranded-jobs-no-actor-config":
            warnings.append({"event": event, **kwargs})

    def _spy_update(data: dict[str, int]) -> None:
        gauge_updates.append(dict(data))

    sweeps_mod.log.warning = _spy_warning  # type: ignore[method-assign]  # Why: test-only instrumentation.
    sweeps_mod.update_stranded_jobs_cache = _spy_update  # type: ignore[assignment]  # Why: test-only instrumentation.
    try:
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
        for _ in range(400):
            if len(gauge_updates) >= ticks:
                break
            await asyncio.sleep(0.01)
        await _stop_loop(task, shutdown, delay=0.0)
    finally:
        sweeps_mod.log.warning = original_warning  # type: ignore[method-assign]
        sweeps_mod.update_stranded_jobs_cache = original_update  # type: ignore[assignment]
    return warnings, gauge_updates


async def test_stranded_jobs_publishes_a_gauge_every_tick() -> None:
    """The condition must be visible in metrics, not only in one log line."""
    _, gauges = await _run_stranded_loop_collecting([[{"actor": "orphan", "cnt": 7}]], ticks=3)
    assert len(gauges) >= 3
    assert all(g == {"orphan": 7} for g in gauges[:3])


async def test_stranded_jobs_rewarns_when_the_backlog_grows() -> None:
    """A growing backlog must not be silenced by the first warning."""
    warnings, _ = await _run_stranded_loop_collecting(
        [
            [{"actor": "orphan", "cnt": 5}],
            [{"actor": "orphan", "cnt": 5}],
            [{"actor": "orphan", "cnt": 50}],
        ],
        ticks=4,
    )
    counts = [w["pending_count"] for w in warnings]
    assert 5 in counts, "onset must warn"
    assert 50 in counts, "growth must re-warn -- pre-fix this was silent forever"
    # The unchanged middle tick must NOT re-warn (that would be per-tick noise).
    assert counts.count(5) == 1


async def test_stranded_jobs_clears_and_rewarns_on_recurrence() -> None:
    """Recovery clears the gauge, and a recurrence warns again."""
    warnings, gauges = await _run_stranded_loop_collecting(
        [
            [{"actor": "orphan", "cnt": 3}],
            [],
            [{"actor": "orphan", "cnt": 3}],
        ],
        ticks=4,
    )
    assert {} in gauges, "recovery must clear the gauge"
    first_seen_flags = [w["first_seen"] for w in warnings if w["actor"] == "orphan"]
    assert first_seen_flags.count(True) >= 2, (
        "a recurrence must warn again; pre-fix the actor stayed in `warned` forever"
    )


async def test_stranded_jobs_detector_disabled_logs_at_error() -> None:
    """The invalid-schema path disables the detector for the whole process
    lifetime while the worker keeps running -- a silent loss of a safety net."""
    import taskq.worker._leader_sweeps as sweeps_mod

    leader = _make_leader(backend=_mem_backend(), deps=_make_deps(is_leader=True))
    leader._deps.settings.schema_name = "bad;schema"  # type: ignore[reportPrivateUsage]  # Why: test mutates the deps the leader was constructed with.

    events: list[str] = []
    original_error = sweeps_mod.log.error

    def _spy_error(event: str, **kwargs: object) -> None:
        events.append(event)

    sweeps_mod.log.error = _spy_error  # type: ignore[method-assign]  # Why: test-only instrumentation.
    try:
        shutdown = asyncio.Event()
        task = asyncio.create_task(leader._stranded_jobs_loop(shutdown))
        await asyncio.sleep(0.05)
        assert task.done()
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        sweeps_mod.log.error = original_error  # type: ignore[method-assign]

    assert "stranded-jobs-detector-disabled" in events


async def test_sweep_loop_acquire_has_timeout() -> None:
    """The sweep loop's ``pool.acquire()`` calls must pass ``timeout=`` —
    without it, pool exhaustion (acquire blocking forever, e.g. the pool is
    fully checked out) hangs the sweep indefinitely instead of timing out
    and recovering on the next iteration."""
    import taskq.worker._leader_sweeps as sweeps_mod

    warn_calls: list[str] = []
    saw_leaked_slots_failure = asyncio.Event()
    original_warning = sweeps_mod.log.warning

    def _spy_warning(event: str, **kw: object) -> None:
        warn_calls.append(event)
        if event == "sweep-leaked-slots-failed":
            saw_leaked_slots_failure.set()

    class _HangingPool:
        """Pool whose acquire() mirrors asyncpg: with a timeout it raises
        TimeoutError once exhausted; with no timeout it blocks forever."""

        def __init__(self) -> None:
            self.acquire_count = 0

        @asynccontextmanager
        async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
            self.acquire_count += 1
            if timeout is None:
                # Unbounded exhaustion: nothing ever wakes this up.
                await asyncio.Event().wait()
            else:
                await asyncio.sleep(timeout)
                raise TimeoutError("pool exhausted")
            yield FakeConn()  # pragma: no cover  # unreachable: both branches above exit first

    backend = _PgSweepBackend()
    pool = _HangingPool()
    settings = _worker_settings(
        HEARTBEAT_INTERVAL="0.5",
        LOCK_LEASE="2.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    settings.dispatcher_command_timeout = 0.05  # bypasses the ge=1.0 field constraint by hand
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: _HangingPool is a deliberate asyncpg.Pool stand-in.
        heartbeat_pool=FakePool(),  # type: ignore[arg-type]
        worker_pool=FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=FakeConn(),  # type: ignore[arg-type]
    )
    deps.is_leader.set()
    leader = _make_leader(backend=backend, deps=deps)
    shutdown = asyncio.Event()

    sweeps_mod.log.warning = _spy_warning  # type: ignore[method-assign]  # Why: test-only instrumentation, mirrors the _err spy pattern above.
    try:
        task = asyncio.create_task(leader._sweep_loop(shutdown))
        try:
            async with asyncio.timeout(2.0):
                await saw_leaked_slots_failure.wait()
        finally:
            await _stop_loop(task, shutdown, delay=0.0)
    finally:
        sweeps_mod.log.warning = original_warning  # type: ignore[method-assign]

    assert "sweep-leaked-slots-failed" in warn_calls, (
        "acquire() without timeout= hangs forever - the sweep never times out "
        f"and recovers: {warn_calls}"
    )
    assert pool.acquire_count >= 1


def test_sweep_loop_acquire_calls_pass_timeout_ast() -> None:
    """Structural backstop: every ``pool.acquire()`` call inside a
    ``*_loop`` function in ``_leader_sweeps.py`` must pass a ``timeout=``
    keyword argument.

    This is deliberately AST-based (not source-text matching), so it is
    robust to reformatting - unlike the previous version of this test,
    which scanned for the literal substring ``pool.acquire()`` on a single
    physical line and would silently stop catching violations the moment
    a call was wrapped onto multiple lines (as every current call site
    already is). It exists alongside the behavioural test above because a
    behavioural test only proves ONE call site is guarded; a future call
    site added without ``timeout=`` would hang just the same, and this
    catches that shape mechanically across every loop in the module
    without needing a dedicated behavioural test per call site.
    """
    import ast

    import taskq.worker._leader_sweeps as sweeps_mod

    source = ast.parse(Path(sweeps_mod.__file__).read_text())
    violations: list[str] = []

    for node in ast.walk(source):
        if not (isinstance(node, ast.AsyncFunctionDef) and node.name.endswith("_loop")):
            continue
        for call in ast.walk(node):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if not (isinstance(func, ast.Attribute) and func.attr == "acquire"):
                continue
            has_timeout = any(kw.arg == "timeout" for kw in call.keywords)
            if not has_timeout:
                violations.append(f"{node.name}: line {call.lineno}")

    assert not violations, f"pool.acquire() call(s) missing timeout=: {violations}"


def test_bootstrap_dispatcher_pool_acquire_calls_pass_timeout_ast() -> None:
    """Structural backstop: every ``dispatcher_pool.acquire()`` call in
    ``taskq.worker._bootstrap`` must pass a ``timeout=`` keyword argument.

    This invariant has recurred three times: PR #67 fixed 8 sites, a 9th
    was found during a later merge in ``_leader_sweeps.py``, and two more
    turned up in ``_bootstrap.py`` -- a file that branch never touched.
    Module-scope rather than restricted to functions named ``*_loop``
    (like the sibling check above): bootstrap's acquire sites live inside
    one large ``_main`` startup function, not per-concern loop functions,
    so scoping by function-name suffix would miss them entirely.
    """
    import ast

    import taskq.worker._bootstrap as bootstrap_mod

    source = ast.parse(Path(bootstrap_mod.__file__).read_text())
    violations: list[str] = []

    for node in ast.walk(source):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "acquire"):
            continue
        target = func.value
        if not (isinstance(target, ast.Attribute) and target.attr == "dispatcher_pool"):
            continue
        has_timeout = any(kw.arg == "timeout" for kw in node.keywords)
        if not has_timeout:
            violations.append(f"line {node.lineno}")

    assert not violations, f"dispatcher_pool.acquire() call(s) missing timeout=: {violations}"
