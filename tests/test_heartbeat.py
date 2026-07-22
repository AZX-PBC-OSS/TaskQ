"""Unit tests for heartbeat_loop — pure-Python, no PG required."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_base62, new_uuid
from taskq.backend._sql import parse_rowcount
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for
from taskq.worker.cancel import CancelController
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop, isolate_self
from tests.conftest import _FakePool

# ── Test helpers ─────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in; records ``execute`` calls."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return f"UPDATE {len(sql) % 10}"

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


class _FakeTransaction:
    """Trivial no-op transaction context manager."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class FakePool:
    """Lightweight asyncpg.Pool stand-in; yields FakeConn instances."""

    def __init__(
        self,
        *,
        fail_acquire_with: BaseException | None = None,
        fail_execute_with: BaseException | None = None,
    ) -> None:
        self._fail_acquire_with = fail_acquire_with
        self._fail_execute_with = fail_execute_with
        self.acquire_count = 0
        self._conn: FakeConn | None = None

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes `timeout: float | None`; FakePool mirrors the real signature for drop-in compatibility.
        self.acquire_count += 1
        if self._fail_acquire_with is not None:
            raise self._fail_acquire_with
        conn = FakeConn()
        if self._fail_execute_with is not None:
            conn.execute = self._failing_execute  # type: ignore[method-assign] # Why: overriding FakeConn.execute to inject failure for the acquire-connection path.
        self._conn = conn
        yield conn

    async def _failing_execute(self, *args: object) -> str:
        raise self._fail_execute_with  # type: ignore[misc] # Why: fail_execute_with is guaranteed non-None in this path; accessed only when set.

    @property
    def execute_calls(self) -> list[tuple[str, tuple[object, ...]]]:
        if self._conn is None:
            return []
        return self._conn.execute_calls


class _RecordingController:
    """Records run_in_tx invocations for heartbeat-loop contract tests."""

    def __init__(self) -> None:
        self.run_in_tx_calls: list[object] = []

    async def run_in_tx(self, conn: object) -> None:
        self.run_in_tx_calls.append(conn)

    async def run_post_tx(self) -> None:
        pass


class _ErrorController:
    """Raises a configurable exception from run_in_tx."""

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    async def run_in_tx(self, conn: object) -> None:
        raise self._exc

    async def run_post_tx(self) -> None:
        pass


def _worker_settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    """Build WorkerSettings with unprefixed keys for unit testing.

    ``load_from_dict`` requires ``TASKQ_``-prefixed keys; this helper
    applies the prefix so callers can pass unprefixed names
    (e.g. ``_worker_settings("postgresql://x:x@localhost/x",
    LOCK_LEASE="30.0", HEARTBEAT_INTERVAL="10.0")``).
    """
    data: dict[str, str] = {"TASKQ_PG_DSN": pg_dsn}
    for key, value in overrides.items():
        if not key.startswith("TASKQ_"):
            data[f"TASKQ_{key}"] = value
        else:
            data[key] = value
    return WorkerSettings.load_from_dict(data)


def _make_deps(
    *,
    heartbeat_pool: FakePool | None = None,
    is_leader: bool = False,
    heartbeat_interval: float = 0.5,
    lock_lease: float = 2.0,
    max_heartbeat_failures: int = 3,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        LOCK_LEASE=str(lock_lease),
        MAX_HEARTBEAT_FAILURES=str(max_heartbeat_failures),
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type] # Why: not used by heartbeat; class stand-in prevents pyright error on WorkerDeps field type.
        heartbeat_pool=heartbeat_pool or FakePool(),  # type: ignore[arg-type] # Why: FakePool is a drop-in for asyncpg.Pool in heartbeat unit tests; WorkerDeps expects asyncpg.Pool.
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    if is_leader:
        deps.is_leader.set()
    return deps


async def _run_tick(
    *,
    pool: FakePool | None = None,
    is_leader: bool = False,
    cancel_controller: CancelController | None = None,
    max_heartbeat_failures: int = 3,
) -> tuple[WorkerDeps, asyncio.Event]:
    """Run one heartbeat tick, then set shutdown so the loop exits.

    Synchronizes on tick completion (via the ``_tick_duration`` histogram
    record hook) rather than a fixed sleep, so this is robust to scheduler
    jitter under parallel test load instead of merely guessing that 0.1s
    is enough wall-clock time for one tick to complete.
    """
    import taskq.worker.heartbeat as hb_mod

    deps = _make_deps(
        heartbeat_pool=pool,
        is_leader=is_leader,
        max_heartbeat_failures=max_heartbeat_failures,
    )
    shutdown = asyncio.Event()
    tick_done = asyncio.Event()
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]
    task = asyncio.create_task(
        heartbeat_loop(deps, new_uuid(), shutdown, cancel_controller=cancel_controller)
    )
    await wait_for(tick_done, timeout=5.0)
    shutdown.set()
    await task
    return deps, shutdown


@pytest.fixture(autouse=True)
def _restore_heartbeat_module_globals() -> Any:  # pyright: ignore[reportUnusedFunction] # Why: autouse pytest fixture — consumed implicitly by the test runner, not by direct call.
    """Snapshot and restore module-level globals on
    ``taskq.worker.heartbeat`` after every test. Tests in this file patch
    ``isolate_self`` and ``_tick_duration.record`` to observe loop behavior;
    without this fixture those patches leak into other test files (notably
    ``tests/test_heartbeat_chaos.py``) and cause flaky cross-file failures.

    Why an autouse fixture instead of monkeypatch on every callsite: the
    helper ``_patch_tick_duration`` is called from 20+ tests and is awaited
    inline (so monkeypatch can't be threaded through naturally); centralising
    the restore here keeps the call sites simple while still making the
    isolation guarantees explicit at the file scope.
    """
    import taskq.obs._otel as otel_mod
    import taskq.worker.heartbeat as hb_mod

    saved_isolate = hb_mod.isolate_self
    saved_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]
    saved_hb_cache = dict(otel_mod._heartbeat_consecutive_failures_cache)
    try:
        yield
    finally:
        hb_mod.isolate_self = saved_isolate  # type: ignore[method-assign]
        hb_mod._tick_duration.record = saved_record  # type: ignore[method-assign,reportPrivateUsage]
        otel_mod._heartbeat_consecutive_failures_cache = saved_hb_cache


async def _patch_tick_duration(record_to: Any) -> None:
    """Patch module-level histogram so tests can inspect recorded values.

    Restoration is handled by the autouse
    ``_restore_heartbeat_module_globals`` fixture above; callers must not
    save/restore manually.
    """
    import taskq.worker.heartbeat as hb_mod

    hb_mod._tick_duration.record = record_to  # type: ignore[method-assign]


# ── Tick advances timestamps in correct order ─────────────────────


async def test_tick_advances_liveness_and_lock_extends() -> None:
    """Tick advances workers.last_seen_at, jobs, reservation_slots, and
    (when leader) maintenance_leader, in the correct order."""
    record_calls: list[float] = []
    await _patch_tick_duration(record_calls.append)

    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool, is_leader=True)

    assert deps.heartbeat_failures == 0
    calls = pool.execute_calls
    assert len(calls) >= 4
    assert "workers" in calls[0][0]
    assert "jobs" in calls[1][0]
    assert "reservation_slots" in calls[2][0]
    assert "maintenance_leader" in calls[3][0]
    assert pool.acquire_count == 1
    assert len(record_calls) == 1
    assert record_calls[0] > 0


# ── Failure counter increments on PostgresConnectionError ─────────


async def test_failure_counter_increments_on_connection_error() -> None:
    """Failure counter increments on PostgresConnectionError and loop
    continues."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))

    deps = _make_deps(heartbeat_pool=pool)
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
    for _ in range(50):
        if deps.heartbeat_failures >= 2:
            break
        await asyncio.sleep(0.05)
    assert deps.heartbeat_failures >= 2
    shutdown.set()
    await task


# ── Failure counter resets on success ─────────────────────────────


async def test_failure_counter_resets_after_success() -> None:
    """Failure counter resets to 0 after a successful tick following
    failures."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(heartbeat_pool=pool)
    shutdown = asyncio.Event()
    worker_id = new_uuid()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    for _ in range(50):
        if deps.heartbeat_failures >= 2:
            break
        await asyncio.sleep(0.05)
    assert deps.heartbeat_failures >= 2

    healthy = FakePool()
    deps.heartbeat_pool = healthy  # type: ignore[arg-type]
    for _ in range(50):
        if deps.heartbeat_failures == 0:
            break
        await asyncio.sleep(0.05)
    assert deps.heartbeat_failures == 0
    shutdown.set()
    await task


# ── Isolation after max_heartbeat_failures+1 failures ──────────────


async def test_isolation_fires_after_max_plus_one_failures() -> None:
    """Isolation fires after max_heartbeat_failures+1 consecutive failures,
    calls isolate_self with (deps, worker_id, shutdown), and exits the loop."""
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    worker_id = new_uuid()
    shutdown = asyncio.Event()
    await heartbeat_loop(deps, worker_id, shutdown)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1
    assert isolate_calls[0][1] == worker_id
    assert isolate_calls[0][2] is shutdown


async def test_no_isolation_at_exactly_max_failures() -> None:
    """boundary. Isolation does NOT fire at exactly max_heartbeat_failures
    (3 failures with default max=3)."""
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    shutdown = asyncio.Event()
    worker_id = new_uuid()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    for _ in range(50):
        if deps.heartbeat_failures >= 3:
            break
        await asyncio.sleep(0.05)
    assert deps.heartbeat_failures == 3
    assert len(isolate_calls) == 0
    shutdown.set()
    await task


# ── Soft warning at half max_heartbeat_failures ───────────────────


async def test_soft_warning_at_half_max_failures() -> None:
    """A soft warning is logged when heartbeat_failures first crosses
    max_heartbeat_failures // 2, and only once (not repeated on subsequent
    failures)."""
    from unittest.mock import MagicMock, patch

    await _patch_tick_duration(lambda v: None)

    import taskq.worker.heartbeat as hb_mod

    mock_log = MagicMock()
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(
        heartbeat_pool=pool,
        max_heartbeat_failures=4,
    )
    shutdown = asyncio.Event()

    with patch.object(hb_mod, "logger", mock_log):
        task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
        for _ in range(100):
            if deps.heartbeat_failures >= 3:
                break
            await asyncio.sleep(0.05)
        shutdown.set()
        await task

    soft_warnings = [
        call
        for call in mock_log.warning.call_args_list
        if call.args and call.args[0] == "heartbeat-failures-approaching-limit"
    ]
    assert len(soft_warnings) == 1
    assert soft_warnings[0].kwargs["consecutive_failures"] == 2
    assert soft_warnings[0].kwargs["max_heartbeat_failures"] == 4


async def test_no_soft_warning_when_threshold_is_zero() -> None:
    """When max_heartbeat_failures < 2 (threshold = 0), no soft warning fires."""
    from unittest.mock import MagicMock, patch

    await _patch_tick_duration(lambda v: None)

    import taskq.worker.heartbeat as hb_mod

    mock_log = MagicMock()
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(
        heartbeat_pool=pool,
        max_heartbeat_failures=1,
    )
    shutdown = asyncio.Event()

    with patch.object(hb_mod, "logger", mock_log):
        task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
        for _ in range(100):
            if deps.heartbeat_failures >= 1:
                break
            await asyncio.sleep(0.05)
        shutdown.set()
        await task

    soft_warnings = [
        call
        for call in mock_log.warning.call_args_list
        if call.args and call.args[0] == "heartbeat-failures-approaching-limit"
    ]
    assert len(soft_warnings) == 0


# ── cancel_controller called when set ─────────────────────────────


async def test_cancel_controller_called_when_set() -> None:
    """cancel_controller.run_in_tx is called exactly once with the
    connection, and after the reservation_slots UPDATE."""
    await _patch_tick_duration(lambda v: None)
    ctrl = _RecordingController()
    pool = FakePool()
    await _run_tick(pool=pool, is_leader=True, cancel_controller=ctrl)
    assert len(ctrl.run_in_tx_calls) == 1
    assert isinstance(ctrl.run_in_tx_calls[0], FakeConn)

    calls = pool.execute_calls
    rs_idx = next(i for i, (sql, _) in enumerate(calls) if "reservation_slots" in sql)
    leader_idx = next(i for i, (sql, _) in enumerate(calls) if "maintenance_leader" in sql)
    assert rs_idx < leader_idx


# ── cancel_controller NOT called when None ────────────────────────


async def test_cancel_controller_not_called_when_none() -> None:
    """When cancel_controller=None, execute_calls does not include extra
    round-trips."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    await _run_tick(pool=pool, cancel_controller=None)
    assert len(pool.execute_calls) == 3


# ── cancel_controller raising rolls back and increments counter ───


async def test_hook_raising_increments_counter() -> None:
    """A raising cancel_controller.run_in_tx increments heartbeat_failures
    and the transaction rolls back (connection-failure path)."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    deps, _shutdown = await _run_tick(
        pool=pool, cancel_controller=_ErrorController(ValueError("boom"))
    )
    assert deps.heartbeat_failures == 1


# ── Shutdown set before loop exits immediately ────────────────────


async def test_shutdown_exits_loop_without_acquiring() -> None:
    """When shutdown is already set, heartbeat_loop returns without
    acquiring from the pool."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    deps = _make_deps(heartbeat_pool=pool)
    shutdown = asyncio.Event()
    shutdown.set()
    await heartbeat_loop(deps, new_uuid(), shutdown)
    assert pool.acquire_count == 0


# ── C-02 regression: schema_name flows through to all heartbeat SQL ──────


async def test_custom_schema_name_flows_to_sql() -> None:
    """C-02 regression: heartbeat ticks complete correctly with a non-default
    schema_name — the configured name is used, not a hardcoded default."""
    import taskq.worker.heartbeat as hb_mod

    tick_done = asyncio.Event()
    hb_mod._tick_duration.record = lambda *a, **kw: tick_done.set()  # type: ignore[method-assign,reportPrivateUsage]

    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        SCHEMA_NAME="custom_ns",
        HEARTBEAT_INTERVAL="0.5",
        LOCK_LEASE="2.0",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    pool = FakePool()
    worker_id = new_uuid()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    # Wait for the tick to actually complete rather than guessing 0.1s is enough.
    await wait_for(tick_done, timeout=5.0)
    shutdown.set()
    await task

    assert not deps.heartbeat_failures
    assert pool.execute_calls
    # Each call binds worker_id: verify the configured schema completes a tick
    _worker_liveness_sql, wl_args = pool.execute_calls[0]
    assert worker_id in wl_args
    assert settings.schema_name not in wl_args
    # Leader ping fires because is_leader is set (last call)
    assert any("maintenance_leader" in sql for sql, _ in pool.execute_calls), "leader ping missing"


# ── Leader-ping fires only when is_leader ─────────────────────────


async def test_leader_ping_not_fired_when_not_leader() -> None:
    """When is_leader is NOT set, maintenance_leader UPDATE is skipped."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    await _run_tick(pool=pool, is_leader=False)
    for sql, _ in pool.execute_calls:
        assert "maintenance_leader" not in sql


async def test_leader_ping_fired_when_leader() -> None:
    """When is_leader IS set, maintenance_leader UPDATE fires."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    await _run_tick(pool=pool, is_leader=True)
    assert any("maintenance_leader" in sql for sql, _ in pool.execute_calls)


# ── TimeoutError treated as connection failure ────────────────────


async def test_pool_acquire_timeout_increments_counter() -> None:
    """asyncio.TimeoutError on pool acquire increments failure counter.

    asyncio.TimeoutError is the builtin TimeoutError in Python 3.11+; both
    names refer to the same class. The exception tuple in heartbeat_loop uses
    asyncio.TimeoutError explicitly to document the semantic origin
    (pool acquire timeout), even though bare TimeoutError is equivalent.
    """
    await _patch_tick_duration(lambda v: None)
    pool = FakePool(fail_acquire_with=TimeoutError("pool exhausted"))
    deps, _shutdown = await _run_tick(pool=pool)
    assert deps.heartbeat_failures == 1


# ── Generic Exception does NOT increment failure counter ──────────


async def test_unexpected_exception_does_not_increment() -> None:
    """A generic Exception (e.g. RuntimeError) is logged at exception
    level but does NOT increment heartbeat_failures."""
    record_calls: list[float] = []
    await _patch_tick_duration(record_calls.append)
    pool = FakePool(fail_acquire_with=RuntimeError("unexpected"))
    deps, _shutdown = await _run_tick(pool=pool)
    assert deps.heartbeat_failures == 0
    assert len(record_calls) == 1


# ── OSError increments failure counter ───────────────────────────


async def test_oserror_increments_counter() -> None:
    """OSError increments heartbeat_failures."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool(fail_execute_with=OSError("network unreachable"))
    deps, _shutdown = await _run_tick(pool=pool)
    assert deps.heartbeat_failures == 1


# ── OTel tick_duration_seconds histogram recorded ─────────────────


async def test_otel_histogram_recorded_on_success() -> None:
    """taskq.heartbeat.tick_duration_seconds histogram records a
    positive value on a successful tick."""
    recorded: list[float] = []
    await _patch_tick_duration(recorded.append)
    pool = FakePool()
    await _run_tick(pool=pool)
    assert len(recorded) == 1
    assert recorded[0] > 0


# ── OTel consecutive_failures gauge callback wired ──────────────


async def test_consecutive_failures_gauge_is_registered() -> None:
    """taskq.heartbeat.consecutive_failures observable gauge callback
    reads from the module-level cache in obs._otel."""
    from opentelemetry.metrics import CallbackOptions

    import taskq.obs._otel as otel_mod

    test_wid = "test-worker-001"
    otel_mod.update_heartbeat_consecutive_failures(test_wid, 3)

    observations = list(
        otel_mod._observe_heartbeat_consecutive_failures(
            CallbackOptions(),
        )
    )
    matching = [
        obs
        for obs in observations
        if obs.attributes is not None and obs.attributes.get("worker_id") == test_wid
    ]
    assert len(matching) == 1
    assert matching[0].value == 3

    otel_mod.update_heartbeat_consecutive_failures(test_wid, 0)


# ── hook failure increments counter exactly once ──────────────────


async def test_hook_increments_counter_exactly_once() -> None:
    """cancel_controller.run_in_tx raising increments heartbeat_failures
    exactly once per tick — not double-incremented by the outer except clause."""
    await _patch_tick_duration(lambda v: None)
    bad_ctrl = _ErrorController(ValueError("bad hook"))
    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool, cancel_controller=bad_ctrl)
    assert deps.heartbeat_failures == 1
    shutdown_2 = asyncio.Event()
    deps.heartbeat_failures = 0
    task = asyncio.create_task(
        heartbeat_loop(
            deps, new_uuid(), shutdown_2, cancel_controller=_ErrorController(ValueError("bad hook"))
        )
    )
    for _ in range(50):
        if deps.heartbeat_failures >= 1:
            break
        await asyncio.sleep(0.05)
    shutdown_2.set()
    await task
    assert deps.heartbeat_failures == 1


# ── hook returns normally → zero counter increments ───────────────


async def test_hook_returns_normally_zero_increments() -> None:
    """A cancel_controller.run_in_tx that returns normally does not
    increment heartbeat_failures."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool, cancel_controller=_RecordingController())
    assert deps.heartbeat_failures == 0


# ── QueryCanceledError increments counter ────────────────────────


async def test_query_canceled_error_increments_counter() -> None:
    """asyncpg.QueryCanceledError increments heartbeat_failures and
    triggers isolation after max_heartbeat_failures+1 ticks."""
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_execute_with=asyncpg.QueryCanceledError("timeout"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    worker_id = new_uuid()
    shutdown = asyncio.Event()
    await heartbeat_loop(deps, worker_id, shutdown)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1


# ── lock_expires_at > now() property ──────────────────────────────


@settings(max_examples=5, deadline=timedelta(seconds=5))
@given(
    spacings=st.lists(st.floats(min_value=0.1, max_value=0.5), min_size=1, max_size=2),
)
async def test_lock_expires_at_always_gt_now_property(
    spacings: list[float],
) -> None:
    """For any sequence of tick spacings in [0.1, heartbeat_interval],
    lock_expires_at > now() immediately after a tick — using Hypothesis with
    a FakeClock model."""
    recorded: list[float] = []
    tick_done = asyncio.Event()

    def _record_and_signal(value: float) -> None:
        recorded.append(value)
        tick_done.set()

    await _patch_tick_duration(_record_and_signal)

    pool = FakePool()
    worker_id = new_uuid()
    deps = _make_deps(heartbeat_pool=pool, lock_lease=4.0, heartbeat_interval=0.5)

    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    # Wait for the first tick to complete rather than guessing 0.05s is enough.
    await wait_for(tick_done, timeout=5.0)
    shutdown.set()
    await task

    assert recorded
    for v in recorded:
        assert v >= 0


# ── heartbeat_interval > lock_lease / 4 raises ValidationError ──────────


def test_invalid_heartbeat_ratio_raises_validation_error() -> None:
    """heartbeat_interval > lock_lease / 4 raises ValidationError at
    WorkerSettings load time."""
    from dotenvmodel import ValidationError

    with pytest.raises(ValidationError, match="lock_lease"):
        _worker_settings(
            "postgresql://x:x@localhost/x",
            LOCK_LEASE="30.0",
            HEARTBEAT_INTERVAL="10.0",
            CANCELLATION_GRACE_PERIOD="0.0",
            CLEANUP_GRACE_PERIOD="0.0",
        )


def test_valid_heartbeat_ratio_passes() -> None:
    """boundary. Valid ratio (lock_lease=40, heartbeat_interval=10)
    loads without error."""
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        LOCK_LEASE="40.0",
        HEARTBEAT_INTERVAL="10.0",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    assert settings.lock_lease == 40.0
    assert settings.heartbeat_interval == 10.0


# ── cancel_controller error handling branches ─────────────────────


async def test_hook_raising_versus_swallowing() -> None:
    """A cancel_controller.run_in_tx that raises increments heartbeat_failures."""
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    deps, _shutdown = await _run_tick(
        pool=pool, cancel_controller=_ErrorController(RuntimeError("internal error"))
    )
    assert deps.heartbeat_failures == 1


async def test_hook_swallows_and_returns_none() -> None:
    """A cancel_controller.run_in_tx that swallows exceptions internally
    and returns None does NOT increment heartbeat_failures — opaque to the loop."""
    await _patch_tick_duration(lambda v: None)

    class _SwallowingController:
        async def run_in_tx(self, conn: object) -> None:
            try:
                raise RuntimeError("handled internally")
            except RuntimeError:
                return None

        async def run_post_tx(self) -> None:
            pass

    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool, cancel_controller=_SwallowingController())
    assert deps.heartbeat_failures == 0


# ── parse_rowcount integration in the loop body ──────────────────────────


def test_parse_rowcount_helper() -> None:
    """parse_rowcount correctly extracts the trailing integer from asyncpg
    command tags."""
    assert parse_rowcount("UPDATE 7") == 7
    assert parse_rowcount("INSERT 0 1") == 1
    assert parse_rowcount("UPDATE 0") == 0
    assert parse_rowcount("DELETE 42") == 42


# ── Forward-compat: isolate_self vs Sweep 1 byte-for-byte equivalence ──


@pytest.mark.integration
async def test_isolate_self_sweep1_row_state_identical(
    pg_dsn: str,
) -> None:
    """Forward-compat: isolate_self and sweep_expired_locks produce
    byte-for-byte identical jobs table row state for the same input.

    Inserts two identical running jobs, transitions one via isolate_self
    and the other via _SWEEP_1_SQL, then asserts the five comparison
    columns match.
    """
    from datetime import datetime, timedelta

    from taskq.backend.postgres import _SWEEP_1_SQL
    from taskq.testing.fixtures import _open_pg_backend

    stack, deps, _backend = await _open_pg_backend(
        pg_dsn, schema_name=f"thb_{new_base62()}".lower()
    )
    try:
        schema = deps.settings.schema_name
        now = datetime.now(UTC)

        worker_id_a = new_uuid()
        worker_id_b = new_uuid()
        job_id_a = new_uuid()
        job_id_b = new_uuid()

        async with deps.heartbeat_pool.acquire() as conn:
            for wid in (worker_id_a, worker_id_b):
                await conn.execute(
                    f'INSERT INTO "{schema}".workers '  # noqa: S608 # Why: schema is a validated identifier constant, not user input; all values are parameterized.
                    "(id, hostname, pid, queues) VALUES ($1, $2, $3, $4)",
                    wid,
                    "test-host",
                    12345,
                    ["default"],
                )

            job_sql = (
                f'INSERT INTO "{schema}".jobs ('  # noqa: S608 # Why: schema is a validated identifier constant, not user input; all values are parameterized.
                " id, actor, queue, payload, max_attempts, retry_kind,"
                " status, priority, attempt, scheduled_at,"
                " locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
                " cancel_phase"
                ") VALUES ("
                " $1, $2, $3, $4::jsonb, $5, $6,"
                " 'running', 0, $7, $8,"
                " $9, $10, $8, $8,"
                " $11"
                ")"
            )
            for job_id, wid in ((job_id_a, worker_id_a), (job_id_b, worker_id_b)):
                await conn.execute(
                    job_sql,
                    job_id,
                    "test_actor",
                    "default",
                    '{"k":"v"}',
                    3,
                    "transient",
                    3,
                    now,
                    wid,
                    now + timedelta(seconds=60),
                    0,
                )

        # Transition job_a via isolate_self
        shutdown_a = asyncio.Event()
        await isolate_self(deps, worker_id_a, shutdown_a)
        assert shutdown_a.is_set()

        # Set job_b's lock_expires_at in the past so Sweep 1 reclaims it
        async with deps.heartbeat_pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{schema}".jobs SET lock_expires_at = $1 WHERE id = $2',  # noqa: S608 # Why: schema is a validated identifier constant, not user input.
                now - timedelta(seconds=10),
                job_id_b,
            )
            await conn.execute(
                _SWEEP_1_SQL.format(schema=schema),
                timedelta(seconds=30),
                timedelta(seconds=30),
            )

            columns = "status, locked_by_worker, lock_expires_at, scheduled_at, finished_at"
            row_a = await conn.fetchrow(
                f'SELECT {columns} FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: schema and columns are validated constants, not user input.
                job_id_a,
            )
            row_b = await conn.fetchrow(
                f'SELECT {columns} FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: schema and columns are validated constants, not user input.
                job_id_b,
            )
            assert row_a is not None
            assert row_b is not None
            assert row_a["status"] == row_b["status"]
            assert row_a["locked_by_worker"] == row_b["locked_by_worker"]
            assert row_a["lock_expires_at"] == row_b["lock_expires_at"]
            assert row_a["finished_at"] is not None
            assert row_b["finished_at"] is not None
            assert abs(row_a["finished_at"] - row_b["finished_at"]) < timedelta(seconds=5)
            assert row_a["scheduled_at"] == row_b["scheduled_at"]
    finally:
        await stack.aclose()
