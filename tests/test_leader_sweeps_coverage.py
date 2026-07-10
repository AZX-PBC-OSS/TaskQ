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
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
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
) -> WorkerDeps:
    settings = _worker_settings(
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        LOCK_LEASE="2.0",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
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

    async def reclaim_expired_locks(self, now: datetime, cg: timedelta, ug: timedelta) -> int:
        raise NotImplementedError("reclaim not implemented")

    async def deadline_sweep(self, now: datetime) -> int:
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

    async def reclaim_expired_locks(self, now: datetime, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self, now: datetime) -> int:
        return 0

    async def sweep_leaked_reservation_slots(
        self, conn: object, now: datetime, *, schema: str
    ) -> int:
        self.leaked_calls.append({"now": now, "schema": schema})
        if self._leaked_exc is not None:
            raise self._leaked_exc
        return 5

    async def sweep_expired_results(self, conn: object, now: datetime, *, schema: str) -> int:
        self.results_calls.append({"now": now, "schema": schema})
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


async def test_stranded_jobs_loop_warns_for_pending_without_actor_config(
    monkeypatch: Any,
) -> None:
    """Pending jobs whose actor has no actor_config row produce a warning."""
    rows = [{"actor": "orphan_actor", "cnt": 7}]
    conn = FakeConn(fetch_rows=rows)
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(worker_pool=pool, is_leader=True),
    )

    # Replace asyncio.sleep with a no-op so the 60 s initial wait is skipped.
    original_sleep = asyncio.sleep

    async def _fast_sleep(_seconds: float) -> None:
        await original_sleep(0)

    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod.asyncio, "sleep", _fast_sleep)

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


async def test_stranded_jobs_loop_fetch_error_continues(monkeypatch: Any) -> None:
    """A fetch error in the stranded loop is swallowed (``continue``) and
    the loop survives."""
    conn = FakeConn(fetch_exc=asyncpg.PostgresConnectionError("lost"))
    pool = FakePool(conn=conn)
    leader = _make_leader(
        backend=_mem_backend(),
        deps=_make_deps(worker_pool=pool, is_leader=True),
    )

    original_sleep = asyncio.sleep

    async def _fast_sleep(_seconds: float) -> None:
        await original_sleep(0)

    import taskq.worker._leader_sweeps as sweeps_mod

    monkeypatch.setattr(sweeps_mod.asyncio, "sleep", _fast_sleep)

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
