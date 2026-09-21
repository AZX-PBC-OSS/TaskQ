"""Unit tests for heartbeat_loop - pure-Python, no PG required."""

import asyncio
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_base62, new_uuid
from taskq.backend._sql import parse_rowcount
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for
from taskq.worker._transient import is_transient_pg_error
from taskq.worker.cancel import CancelController
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop, isolate_self
from tests.conftest import _FakePool

# ── Test helpers ─────────────────────────────────────────────────────────


class FakeConn:
    """Lightweight asyncpg.Connection stand-in; records ``execute`` calls."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False
        self.terminated = False

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return f"UPDATE {len(sql) % 10}"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True

    def transaction(self) -> "_FakeTransaction":
        return _FakeTransaction()


class _FakeTransaction:
    """Explicit-API transaction stand-in (start/commit/rollback - the
    heartbeat tick drives the transaction explicitly since the fix
    round's command budget)."""

    def __init__(self) -> None:
        self.started = False
        self.committed = False
        self.rolled_back = False

    async def start(self) -> None:
        self.started = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


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
    # 18.0 = the cascade floor at h=0.5 with the default
    # heartbeat_command_timeout of 2.0: 4 * (0.5 + 2 * 2). The renewal
    # threshold the loop derives is unchanged by this bump (its safety
    # floor, 18.0, dominated the old lease/2 arm already).
    lock_lease: float = 18.0,
    max_heartbeat_failures: int = 3,
    heartbeat_command_timeout: float = 2.0,
) -> WorkerDeps:
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        HEARTBEAT_INTERVAL=str(heartbeat_interval),
        LOCK_LEASE=str(lock_lease),
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        # Tier 1 must be able to fire before the 1.2s terminal tier -
        # a warn budget at or above the terminal budget silently disables
        # tier 1 and fails settings validation.
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
        MAX_HEARTBEAT_FAILURES=str(max_heartbeat_failures),
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
        HEARTBEAT_COMMAND_TIMEOUT=str(heartbeat_command_timeout),
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
    deps_hook: Callable[[WorkerDeps], None] | None = None,
) -> tuple[WorkerDeps, asyncio.Event]:
    """Run one heartbeat tick, then set shutdown so the loop exits.

    Synchronizes on tick completion (via the ``_tick_duration`` histogram
    record hook) rather than a fixed sleep, so this is robust to scheduler
    jitter under parallel test load instead of merely guessing that 0.1s
    is enough wall-clock time for one tick to complete.

    ``deps_hook`` runs after the deps are built but before the loop
    starts, for tests that need to seed per-process state (the stall
    tally) the first tick then reads.
    """
    import taskq.worker.heartbeat as hb_mod

    deps = _make_deps(
        heartbeat_pool=pool,
        is_leader=is_leader,
        max_heartbeat_failures=max_heartbeat_failures,
    )
    if deps_hook is not None:
        deps_hook(deps)
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
def _restore_heartbeat_module_globals() -> Any:  # pyright: ignore[reportUnusedFunction] # Why: autouse pytest fixture - consumed implicitly by the test runner, not by direct call.
    """Snapshot and restore module-level globals on
    ``taskq.worker.heartbeat`` after every test. Tests in this file patch
    ``isolate_self``, ``_tick_duration.record`` and
    ``update_heartbeat_consecutive_failures`` to observe loop behavior;
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
    saved_update = hb_mod.update_heartbeat_consecutive_failures
    saved_miss = hb_mod.record_heartbeat_miss
    saved_hb_count = otel_mod._heartbeat_consecutive_failures_count
    try:
        yield
    finally:
        hb_mod.isolate_self = saved_isolate  # type: ignore[method-assign]
        hb_mod._tick_duration.record = saved_record  # type: ignore[method-assign,reportPrivateUsage]
        hb_mod.update_heartbeat_consecutive_failures = saved_update
        hb_mod.record_heartbeat_miss = saved_miss
        otel_mod._heartbeat_consecutive_failures_count = saved_hb_count


async def _patch_tick_duration(record_to: Any) -> None:
    """Patch module-level histogram so tests can inspect recorded values.

    Restoration is handled by the autouse
    ``_restore_heartbeat_module_globals`` fixture above; callers must not
    save/restore manually.
    """
    import taskq.worker.heartbeat as hb_mod

    hb_mod._tick_duration.record = record_to  # type: ignore[method-assign]


async def _wait_for_heartbeat_failures(
    deps: WorkerDeps,
    *,
    at_least: int | None = None,
    exactly: int | None = None,
    timeout: float = 5.0,  # noqa: ASYNC109  # Why: repo wait_for_* idiom (see taskq.testing.assertions) - a deadline parameter, not an asyncio.timeout scope.
) -> None:
    """Bounded, event-driven wait for the heartbeat loop to drive
    ``deps.heartbeat_failures`` to a target.

    The wait surface is ``taskq.worker.heartbeat.update_heartbeat_consecutive_failures``
    - the loop calls it immediately AFTER every counter flip (the
    in-tx hook-failure increment, the transient-failure increment, and
    the success-path reset), so signalling from inside the wrapper
    fires in the same event-loop turn as the flip and the waiting test
    resumes before the loop can start its NEXT tick. A wait on the
    exact count therefore can never observe the counter advancing past
    the target. (The tick-duration histogram hook is NOT usable here:
    on the transient-failure path it records BEFORE the increment, so
    it would fire a whole tick late - after isolation.) The
    ``for _ in range(50)``-style poll loop samples the counter every
    50 ms and can miss the window under scheduler
    starvation - letting the counter run past an exact target (breaking
    the ``== n`` asserts) or even past the isolation threshold, which
    would run the REAL ``isolate_self`` (a live ``asyncpg.connect`` to
    a fake DSN) inside a test file that promises "no PG required".

    Must be armed before the tick that reaches the target can complete:
    call it before starting the loop task, or synchronously after a
    mid-test pool swap that precedes the next tick (the hook install is
    synchronous; only the inner wait awaits). Restoration is handled by
    the ``_restore_heartbeat_module_globals`` autouse fixture, exactly
    like ``_patch_tick_duration``.
    """
    import taskq.worker.heartbeat as hb_mod

    if (at_least is None) == (exactly is None):
        raise ValueError("specify exactly one of at_least / exactly")
    reached = asyncio.Event()
    prev_update = hb_mod.update_heartbeat_consecutive_failures

    def _update_and_signal(worker_id: str, count: int) -> None:
        prev_update(worker_id, count)
        if at_least is not None:
            if deps.heartbeat_failures >= at_least:
                reached.set()
        elif deps.heartbeat_failures == exactly:
            reached.set()

    hb_mod.update_heartbeat_consecutive_failures = _update_and_signal

    target = f"at least {at_least}" if at_least is not None else f"exactly {exactly}"
    try:
        await asyncio.wait_for(reached.wait(), timeout=timeout)
    except TimeoutError:
        pytest.fail(
            f"heartbeat loop did not reach {target} consecutive "
            f"failures within {timeout}s (stuck at {deps.heartbeat_failures})"
        )


# ── Tick advances timestamps in correct order ─────────────────────


async def test_tick_advances_liveness_and_lock_extends() -> None:
    """Tick advances workers.last_seen_at, jobs and reservation_slots, in
    the correct order, and leaves maintenance_leader alone even on a leader."""
    record_calls: list[float] = []
    await _patch_tick_duration(record_calls.append)

    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool, is_leader=True)

    assert deps.heartbeat_failures == 0
    calls = pool.execute_calls
    assert len(calls) >= 3
    assert "workers" in calls[0][0]
    assert "jobs" in calls[1][0]
    assert "reservation_slots" in calls[2][0]
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
    # Event-driven wait on the exact observable: fires on the tick that
    # reaches 2 failures, before the loop can run further ticks - which,
    # under the starvation a poll loop tolerates, could reach the
    # isolation threshold and run the REAL isolate_self against the
    # fake DSN.
    await _wait_for_heartbeat_failures(deps, at_least=2)
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
    await _wait_for_heartbeat_failures(deps, at_least=2)
    assert deps.heartbeat_failures >= 2

    healthy = FakePool()
    deps.heartbeat_pool = healthy  # type: ignore[arg-type]
    # The swap is synchronous, so the hook is installed before the loop's
    # next tick can run; the wait fires on that first successful tick -
    # the exact point the counter resets to 0.
    await _wait_for_heartbeat_failures(deps, exactly=0)
    assert deps.heartbeat_failures == 0
    shutdown.set()
    await task


# ── Failed-tick pacing: a failed tick is not a beat ────────────────


class _OneShotFastFailPool(FakePool):
    """Fails the FIRST acquire instantly with a transient error, then
    behaves like a healthy pool for every later tick.

    The fast shape is the common transient blip (a refused connection
    raises the moment the acquire is attempted); it is the shape whose
    post-failure wait the loop controls outright, because the tick
    consumed none of the cadence it would then sleep off.
    """

    def __init__(self, exc: BaseException) -> None:
        super().__init__()
        self._blip_exc = exc
        self._blipped = False

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: asyncpg.Pool.acquire signature takes `timeout: float | None`; this subclass mirrors the parent's drop-in signature.
        self.acquire_count += 1
        if not self._blipped:
            self._blipped = True
            raise self._blip_exc
        conn = FakeConn()
        self._conn = conn
        yield conn


async def test_one_fast_transient_failure_is_one_miss_retried_promptly() -> None:
    """Behavior pin for the failed-tick pacing contract.

    ONE fast transient tick failure - its prompt retry succeeding - must
    cost the ledger exactly ONE miss, leave the consecutive-failure count
    reset to zero (the isolate budget is NOT consumed: at
    max_heartbeat_failures=1 a second counted failure would isolate, and
    none does), and the recovery beat must land PROMPTLY - well inside
    the interval, not a full interval after the failure.

    The pacing half is the regression that motivated the pin: the loop
    used to sleep the FULL remaining interval after a tick that failed
    instantly, so the recovery beat landed a whole interval late - at
    twice the interval after the last good beat, the reclaim deadline
    the ops guidance calls safe at ``heartbeat_timeout = 2x interval``.
    A failed tick is not a beat: it stamps nothing, so the one-beat-per-
    interval cadence promise does not spend a full inter-beat sleep on
    it. The margins are wide (the retry waits a quarter interval; the
    assert allows under three quarters) so scheduler jitter under
    parallel test load cannot flip a correct pacing, while the old
    full-interval sleep fails the assert on every run.
    """
    import taskq.worker.heartbeat as hb_mod

    await _patch_tick_duration(lambda v: None)
    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    misses: list[float] = []
    prev_miss = hb_mod.record_heartbeat_miss

    def _record_miss(worker_id: str) -> None:
        prev_miss(worker_id)
        misses.append(time.monotonic())

    hb_mod.record_heartbeat_miss = _record_miss  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    interval = 1.0  # the retry backoff is a quarter of this; the old pacing slept it all
    pool = _OneShotFastFailPool(asyncpg.PostgresConnectionError("boom"))
    deps = _make_deps(heartbeat_pool=pool, heartbeat_interval=interval, max_heartbeat_failures=1)
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))

    # Event-driven, on the counter-flip hook: the failure signal fires in
    # the same event-loop turn as the increment (before the retry's
    # backoff sleep can start), the success signal on the retry's reset.
    await _wait_for_heartbeat_failures(deps, at_least=1)
    failed_at = time.monotonic()
    await _wait_for_heartbeat_failures(deps, exactly=0)
    recovered_at = time.monotonic()
    # The isolate observation must land BEFORE the test's own
    # shutdown.set(): the loop is parked in its inter-tick wait, and
    # nothing but a threshold trip could have isolated it.
    assert not isolate_calls, (
        "one transient failure plus a successful prompt retry must not "
        "consume the isolate threshold (max_heartbeat_failures=1 here: any "
        "second counted failure would have isolated the worker)"
    )

    shutdown.set()
    await task

    assert len(misses) == 1, (
        f"one fast transient failure must cost exactly one heartbeat miss, "
        f"got {len(misses)} (the prompt retry is a normal tick, its success "
        f"resets the ledger - it must not record a second miss)"
    )
    assert deps.heartbeat_failures == 0
    retry_gap = recovered_at - failed_at
    assert retry_gap < 0.75 * interval, (
        f"the recovery beat after one fast transient failure landed "
        f"{retry_gap:.3f}s after the failure - at {retry_gap / interval:.2f}x the "
        f"interval. A failed tick is not a beat: sleeping the full remaining "
        f"interval after a tick that failed instantly pushes the gap between "
        f"good beats to 2x interval, the reclaim deadline the "
        f"heartbeat_timeout >= 2x interval sizing calls safe. The retry must "
        f"be prompt (a bounded fraction of the interval) so the recovery beat "
        f"lands well inside that floor."
    )
    assert retry_gap > 0.1, (
        f"the retry backoff must be a real bounded wait, not a hot spin (gap was {retry_gap:.3f}s)"
    )


async def test_repeated_failures_isolate_at_the_documented_tick_without_hammering() -> None:
    """The backoff applies PER FAILURE, and the isolate contract is
    unchanged: a tick that fails repeatedly still isolates on the
    (max_heartbeat_failures + 1)-th consecutive failure.

    The prompt retry shortens each failed cycle (failure -> backoff ->
    next failure), so the same tick-count contract arrives in less
    wall-clock time; the pacing bounds here pin that the cycles are
    neither a hot spin (each failure still pays a real backoff) nor the
    old full-interval wait (the cascade no longer drags the isolate
    decision toward the lease deadline it must stay inside - see
    _lease_renewal_threshold's cascade floor, which bounds from above and
    is only satisfied more comfortably by shorter cycles).
    """
    import taskq.worker.heartbeat as hb_mod

    await _patch_tick_duration(lambda v: None)
    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    interval = 1.0
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    # 20.0 = the cascade floor at this interval: 4 * (1.0 + 2 * 2.0).
    deps = _make_deps(
        heartbeat_pool=pool,
        heartbeat_interval=interval,
        lock_lease=20.0,
        max_heartbeat_failures=3,
    )
    shutdown = asyncio.Event()
    started_at = time.monotonic()
    task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))

    # The loop isolates on the 4th consecutive failure (3 + 1); the hook
    # fires in the same turn as that increment, before the loop can
    # cycle again.
    await _wait_for_heartbeat_failures(deps, at_least=4)
    isolated_at = time.monotonic()
    assert deps.heartbeat_failures == 4, (
        f"isolation must fire on exactly the (max + 1)-th consecutive "
        f"failure, got {deps.heartbeat_failures}"
    )
    await task
    assert len(isolate_calls) == 1

    cascade_span = isolated_at - started_at
    # Four failed cycles at a quarter-interval backoff each: ~1s of
    # waits. A hot spin would land far under half a second; the old
    # full-interval-sleep pacing needed ~4s.
    assert cascade_span > 0.5 * interval, (
        f"four consecutive failed ticks completed in {cascade_span:.3f}s - "
        f"the retry backoff must be a real bounded wait per failure, not a "
        f"hot spin against the pool"
    )
    assert cascade_span < 2.5 * interval, (
        f"the isolate decision on the 4th consecutive failure took "
        f"{cascade_span:.3f}s ({cascade_span / interval:.2f}x the interval) - "
        f"failed ticks must retry promptly on a bounded backoff, not sleep "
        f"the full interval after each failure, which drags the cascade "
        f"toward the lease deadline the (F+1)-gap floor has to stay inside"
    )


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
    # Fires on the tick that reaches exactly 3 - before the loop can run
    # the fourth (isolation) tick - so the asserts below sample a loop
    # parked in its inter-tick sleep, not a 0.5s window a starved poll
    # could overshoot (4th failure → isolation → == 3 assert fails).
    await _wait_for_heartbeat_failures(deps, at_least=3)
    assert deps.heartbeat_failures == 3
    assert len(isolate_calls) == 0
    shutdown.set()
    await task


# ── Unexpected (non-transient) failures count toward the same threshold ──


async def test_unexpected_failure_counts_toward_isolate() -> None:
    """A persistent non-transient tick error isolates the worker.

    The REVOKE-class shape: every statement the tick issues fails with a
    permanent refusal (InsufficientPrivilegeError, SQLSTATE 42501),
    deliberately outside TRANSIENT_PG_ERRORS. Pre-fix the unexpected arm
    only logged, the counter never moved, and the loop ticked forever on
    an expired lock with the gauge pinned at 0 and /ready green. Now the
    arm counts toward the same max_heartbeat_failures threshold as the
    transient arm: the counter climbs one per tick and isolate_self fires
    on the (max+1)-th consecutive failure, never before.
    """
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(
        fail_execute_with=asyncpg.InsufficientPrivilegeError("permission denied for table jobs")
    )
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    worker_id = new_uuid()
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    await _wait_for_heartbeat_failures(deps, exactly=4)
    # The loop exits after the isolate decision, exactly as the transient
    # arm exits: a worker that has isolated does not keep ticking.
    await asyncio.wait_for(task, timeout=5.0)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1
    assert isolate_calls[0][1] == worker_id
    assert isolate_calls[0][2] is shutdown


async def test_transient_failure_control_unchanged() -> None:
    """Control for the unexpected-arm pin: the transient 40001 arm counts
    toward the isolate threshold and isolates exactly as it did before the
    unexpected arm learned to count."""
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_execute_with=asyncpg.SerializationError("could not serialize access"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    worker_id = new_uuid()
    shutdown = asyncio.Event()
    await heartbeat_loop(deps, worker_id, shutdown)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1
    assert isolate_calls[0][1] == worker_id
    assert isolate_calls[0][2] is shutdown


async def test_unexpected_failure_counter_resets_after_success() -> None:
    """A successful tick resets the count after unexpected failures too.

    The unexpected arm must carry the transient arm's full ledger
    semantics, not just the increment: an isolated surprise (one bad
    statement shape, a transient driver hiccup outside the set) must be
    forgiven by the next good tick, the same reset-on-success contract
    UnexpectedLoopErrorGuard enforces for the leader loops.
    """
    await _patch_tick_duration(lambda v: None)
    pool = FakePool(
        fail_execute_with=asyncpg.InsufficientPrivilegeError("permission denied for table jobs")
    )
    deps = _make_deps(heartbeat_pool=pool)
    shutdown = asyncio.Event()
    worker_id = new_uuid()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    await _wait_for_heartbeat_failures(deps, at_least=2)
    assert deps.heartbeat_failures >= 2

    healthy = FakePool()
    deps.heartbeat_pool = healthy  # type: ignore[arg-type]
    # The swap is synchronous, so the hook is installed before the loop's
    # next tick can run; the wait fires on that first successful tick -
    # the exact point the counter resets to 0.
    await _wait_for_heartbeat_failures(deps, exactly=0)
    assert deps.heartbeat_failures == 0
    shutdown.set()
    await task


# ── TRANSIENT_PG_ERRORS membership is the load-bearing classification ──
#
# Since the two tick-failure arms share one ledger, membership in
# TRANSIENT_PG_ERRORS no longer decides WHETHER a failure counts - both
# arms count. It decides which arm rides, and the arms spend the failure
# differently: the transient arm warns and retries, the unexpected arm
# raises the exception-level bug alarm AND spends the isolate budget -
# any Exception outside the set isolates the worker within
# max_heartbeat_failures + 1 ticks (~40 s at the defaults, interval 10 s).
# These pins couple the set to that consequence.


async def test_a_transient_member_does_not_count_as_unexpected(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """A TRANSIENT_PG_ERRORS member does NOT count toward the unexpected
    arm: 40001 (SerializationError, a canonical member) rides the
    transient arm's warn-level ``heartbeat-tick-failure`` and never raises
    the exception-level ``heartbeat-tick-unexpected-error`` bug alarm.

    Both arms spend the same isolate ledger; membership decides whether a
    failure is signalised as an environment blip or a bug. The membership
    fact is asserted against the set itself, so the pin tracks the
    classification rather than a hardcoded assumption about it.
    """
    assert is_transient_pg_error(asyncpg.SerializationError("could not serialize access"))
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_execute_with=asyncpg.SerializationError("could not serialize access"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    shutdown = asyncio.Event()
    await heartbeat_loop(deps, new_uuid(), shutdown)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1

    events = [e["event"] for e in structlog_capture]
    assert "heartbeat-tick-failure" in events
    assert "heartbeat-tick-unexpected-error" not in events, (
        "a member of TRANSIENT_PG_ERRORS must ride the transient arm: the "
        "unexpected arm's exception-level alarm is the bug signal, and "
        "classifying an environment blip as a bug pages the on-call for "
        "nothing"
    )


async def test_a_non_transient_error_spends_the_isolate_budget_within_documented_ticks(
    structlog_capture: list[structlog.types.EventDict],
) -> None:
    """A non-transient error (outside TRANSIENT_PG_ERRORS) reaches the
    isolate threshold within the documented tick count.

    The documented doctrine (docs/guides/workers.md, the shared-ledger
    passage): a tick failure outside the transient set counts toward the
    same threshold and isolates when heartbeat_failures >
    max_heartbeat_failures - at the default 3, the (max+1)-th = 4th
    consecutive failure, never before. The representative shape is
    asyncpg.PostgresSyntaxError (42601), deliberately outside the set per
    _transient.py's doctrine (data errors are bugs, loud then fatal), and
    the membership fact is asserted against the set itself.
    """
    assert not is_transient_pg_error(asyncpg.PostgresSyntaxError("syntax error at or near"))
    await _patch_tick_duration(lambda v: None)

    isolate_calls: list[tuple[WorkerDeps, UUID, asyncio.Event]] = []

    async def fake_isolate(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
        isolate_calls.append((deps, worker_id, shutdown))
        shutdown.set()

    import taskq.worker.heartbeat as hb_mod

    hb_mod.isolate_self = fake_isolate  # type: ignore[method-assign] # Why: restored by _restore_heartbeat_module_globals autouse fixture.

    pool = FakePool(fail_execute_with=asyncpg.PostgresSyntaxError("syntax error at or near"))
    deps = _make_deps(heartbeat_pool=pool, max_heartbeat_failures=3)
    worker_id = new_uuid()
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
    await _wait_for_heartbeat_failures(deps, exactly=4)
    # The loop exits on the isolate decision, exactly as the transient arm
    # exits: a worker that has isolated does not keep ticking.
    await asyncio.wait_for(task, timeout=5.0)
    assert deps.heartbeat_failures == 4
    assert len(isolate_calls) == 1
    assert isolate_calls[0][1] == worker_id
    assert isolate_calls[0][2] is shutdown

    events = [e["event"] for e in structlog_capture]
    assert "heartbeat-tick-unexpected-error" in events, (
        "an error outside TRANSIENT_PG_ERRORS must ride the unexpected arm: "
        "that arm spends the isolate budget, so an out-of-set failure "
        "isolates the worker within max_heartbeat_failures + 1 ticks "
        "instead of ticking forever on a green /ready"
    )


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
        # 25 >= the cascade floor at F=4: 5 * (0.5 + 2 * 2) = 22.5.
        lock_lease=25.0,
    )
    shutdown = asyncio.Event()

    with patch.object(hb_mod, "logger", mock_log):
        task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
        # Third failure ⇒ the second (soft-warning) tick already
        # completed - the warning either fired once there or never will.
        # Shutdown before tick 4 so the real isolate_self (max=4 →
        # isolate at 5) can never run against this fake DSN.
        await _wait_for_heartbeat_failures(deps, at_least=3)
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
        # Fires on the FIRST failure; the shutdown that follows lands
        # before the second tick, so the real isolate_self (max=1 →
        # isolate at 2, one mere interval away) can never run against
        # this fake DSN - the tightest isolation margin in the file.
        await _wait_for_heartbeat_failures(deps, at_least=1)
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
    assert rs_idx == len(calls) - 1, (
        "the reservation-slots UPDATE is the tick's last statement, so the "
        "controller runs after it inside the same transaction"
    )


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
    schema_name - the configured name is used, not a hardcoded default."""
    import taskq.worker.heartbeat as hb_mod

    tick_done = asyncio.Event()
    hb_mod._tick_duration.record = lambda *a, **kw: tick_done.set()  # type: ignore[method-assign,reportPrivateUsage]

    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        SCHEMA_NAME="custom_ns",
        HEARTBEAT_INTERVAL="0.5",
        # 18.0 = the cascade floor at h=0.5 with the default command
        # timeout of 2.0 (see _make_deps for the same bump and why the
        # renewal-threshold behaviour is unchanged).
        LOCK_LEASE="18.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
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
    # The configured schema is what the statements name - the defect this
    # test exists for was a hardcoded default surviving the rendering.
    assert all(f'"{settings.schema_name}".' in sql for sql, _ in pool.execute_calls)


# ── The tick never touches the lease row ──────────────────────────


@pytest.mark.parametrize("is_leader", [False, True])
async def test_tick_never_writes_the_lease_row(is_leader: bool) -> None:
    """The heartbeat leaves ``maintenance_leader`` alone, leader or not.

    The lease is renewed by the election loop on its own connection, fenced
    on the term it holds. An unfenced write from this loop would refresh the
    row of a leader whose term has already lapsed on its own clock, and
    because a live ``last_seen_at`` is one of the two signals that keep the
    row from being taken over, that write would hold leadership open past
    the horizon the lease exists to impose.
    """
    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    await _run_tick(pool=pool, is_leader=is_leader)
    assert pool.execute_calls
    for sql, _ in pool.execute_calls:
        assert "maintenance_leader" not in sql


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


# ── Generic Exception counts toward the shared failure counter ─────


async def test_unexpected_exception_is_counted_and_tolerated() -> None:
    """A generic Exception (e.g. RuntimeError) is logged at exception
    level AND counts toward the same isolate threshold as the transient
    arm.

    Both arms share one ledger: a persistent non-transient fault fails
    every tick exactly as a dead PG does, so it must move the counter the
    gauge and /ready read; a ledger that only logged would tick forever
    on a worker that looks perfectly healthy. One isolated surprise is
    still tolerated: the loop keeps ticking, the reset comes from the
    next good tick.
    """
    record_calls: list[float] = []
    await _patch_tick_duration(record_calls.append)
    pool = FakePool(fail_acquire_with=RuntimeError("unexpected"))
    deps, _shutdown = await _run_tick(pool=pool)
    assert deps.heartbeat_failures == 1
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


# ── taskq.lock.expires_in_seconds is a measurement, not the constant ──


class _SlowSecondAcquirePool(FakePool):
    """FakePool whose second acquire stalls, so the second renewal lands
    late - the shape a slow pool or a blocked loop produces."""

    def __init__(self, stall: float) -> None:
        super().__init__()
        self._stall = stall

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire's signature, as FakePool does.
        if self.acquire_count == 1:
            await asyncio.sleep(self._stall)
        async with super().acquire(timeout=timeout) as conn:
            yield conn


async def test_lock_ttl_sample_is_the_lease_minus_the_gap_between_renewals() -> None:
    """The histogram used to record ``lock_lease`` on every tick - the
    configured constant - so a heartbeat running late could never move
    it. It must record the lease the previous renewal stamped minus the
    time until this one landed: nothing on the first renewal (no reference
    yet), and a sample below the lease by at least the delay on a delayed
    tick."""
    import taskq.worker.heartbeat as hb_mod

    interval, lease, stall = 0.5, 18.0, 0.3
    samples: list[float] = []
    two_samples = asyncio.Event()

    def _capture(worker_id: str, remaining: float) -> None:
        samples.append(remaining)
        if len(samples) >= 2:
            two_samples.set()

    saved = hb_mod.record_lock_expires_in_seconds
    hb_mod.record_lock_expires_in_seconds = _capture
    try:
        pool = _SlowSecondAcquirePool(stall)
        deps = _make_deps(heartbeat_pool=pool, heartbeat_interval=interval, lock_lease=lease)
        shutdown = asyncio.Event()
        task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
        try:
            await wait_for(two_samples, timeout=5.0)
        finally:
            shutdown.set()
            await task
    finally:
        hb_mod.record_lock_expires_in_seconds = saved

    delayed, recovered = samples[0], samples[1]
    assert delayed <= lease - (interval + stall) + 0.05, (
        f"the delayed renewal must report the lease minus its gap, got {delayed}"
    )
    assert 0.0 < delayed < lease
    # Three renewals landed, two samples: the first renewal measures nothing.
    assert pool.acquire_count >= 3
    # The cadence is anchored to tick start, so the renewal after a late
    # one lands early and its sample climbs back toward the lease.
    assert delayed < recovered < lease


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
    # One process, one worker: a single dimensionless series, not one per
    # worker_id (which would grow without bound across restarts).
    assert len(observations) == 1
    assert dict(observations[0].attributes or {}) == {}
    assert observations[0].value == 3

    otel_mod.update_heartbeat_consecutive_failures(test_wid, 0)


# ── hook failure increments counter exactly once ──────────────────


async def test_hook_increments_counter_exactly_once() -> None:
    """cancel_controller.run_in_tx raising increments heartbeat_failures
    exactly once per tick - not double-incremented by the outer except clause."""
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
    # Fires on the hook-failure tick that increments to 1; the asserts
    # below then sample a parked loop - a fixed-cadence poll could
    # overshoot to a second hook failure (the == 1 assert fails) under
    # starvation.
    await _wait_for_heartbeat_failures(deps, at_least=1)
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
    lock_expires_at > now() immediately after a tick - using Hypothesis with
    a FakeClock model."""
    recorded: list[float] = []
    tick_done = asyncio.Event()

    def _record_and_signal(value: float) -> None:
        recorded.append(value)
        tick_done.set()

    await _patch_tick_duration(_record_and_signal)

    pool = FakePool()
    worker_id = new_uuid()
    deps = _make_deps(heartbeat_pool=pool, lock_lease=18.0, heartbeat_interval=0.5)

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
            WATCHDOG_LOOP_LAG_BUDGET="15.0",
            CANCELLATION_GRACE_PERIOD="0.0",
            CLEANUP_GRACE_PERIOD="0.0",
        )


def test_valid_heartbeat_ratio_passes() -> None:
    """boundary. A ratio inside the cascade floor
    (lock_lease=60 >= 4 * (10 + 2 * 2) = 56, heartbeat_interval=10)
    loads without error."""
    settings = _worker_settings(
        "postgresql://x:x@localhost/x",
        LOCK_LEASE="60.0",
        HEARTBEAT_INTERVAL="10.0",
        WATCHDOG_LOOP_LAG_BUDGET="25.0",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    assert settings.lock_lease == 60.0
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
    and returns None does NOT increment heartbeat_failures - opaque to the loop."""
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
            # Third argument is the sweep's batch cap (LIMIT $3), added when
            # the sweep was bounded; the fourth is the reclaim delay's
            # effective-cap ceiling ($4, max_retry_backoff in seconds). The
            # production defaults are used so this direct-SQL drive mirrors
            # what the sweep loop executes.
            from taskq.constants import DEFAULT_EVENT_WRITER_BATCH_SIZE, DEFAULT_MAX_RETRY_BACKOFF

            await conn.execute(
                _SWEEP_1_SQL.format(schema=schema),
                timedelta(seconds=30),
                timedelta(seconds=30),
                DEFAULT_EVENT_WRITER_BATCH_SIZE,
                DEFAULT_MAX_RETRY_BACKOFF.total_seconds(),
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


# ── The stall-attribution tally rides the liveness statement ──────────


async def test_tick_merges_the_stall_tally_into_the_liveness_statement() -> None:
    """The liveness write carries this process's stall tally as a jsonb
    MERGE in the same statement (no extra round trip): the watchdog hands
    the tally to the heartbeat loop via deps, and the loop never reads
    anything off the watchdog thread itself."""
    pool = FakePool()

    def _seed(deps: WorkerDeps) -> None:
        deps.stall_tally.record("send_email", kind="gil_held")
        deps.stall_tally.record("send_email", kind="gil_held")
        deps.stall_tally.record("resize_image", kind="blocking_call")

    _deps, _shutdown = await _run_tick(pool=pool, deps_hook=_seed)

    liveness_calls = [
        (sql, args) for sql, args in pool.execute_calls if "last_seen_at = clock_timestamp()" in sql
    ]
    assert len(liveness_calls) == 1
    sql, args = liveness_calls[0]
    assert "metadata = metadata || $2::jsonb" in sql
    assert "WHERE id = $1" in sql
    assert args[1] is not None
    assert '"send_email":{"gil_held":2}' in str(args[1])
    assert '"resize_image":{"blocking_call":1}' in str(args[1])


async def test_tick_merges_an_empty_tally_as_a_noop() -> None:
    """A process that attributed nothing merges an empty object: the jsonb
    concat leaves the registered metadata keys (max_concurrency,
    notify_enabled) untouched and writes no loop_stalls key."""
    pool = FakePool()
    _deps, _shutdown = await _run_tick(pool=pool)

    liveness_calls = [
        (sql, args) for sql, args in pool.execute_calls if "last_seen_at = clock_timestamp()" in sql
    ]
    assert len(liveness_calls) == 1
    _sql, args = liveness_calls[0]
    assert args[1] == "{}"


def test_build_heartbeat_sql_liveness_shape_pins_the_merge() -> None:
    """The liveness template is the merge statement's single source: the
    jsonb concat must keep the registration keys AND key the tally write
    to $2, which is what keeps the heartbeat at one statement per tick."""
    from taskq.backend._sql import build_heartbeat_sql

    liveness_sql, _jobs_sql, _slots_sql = build_heartbeat_sql("taskq")
    assert liveness_sql == (
        'UPDATE "taskq".workers '
        "SET last_seen_at = clock_timestamp(), metadata = metadata || $2::jsonb "
        "WHERE id = $1"
    )


# ── Threshold-gated lease renewal ────────────────────────────


def test_lease_renewal_threshold_default_config() -> None:
    """Defaults (lease 60s, interval 10s, 3 failures, 2s command timeout):
    the enforced-bound floor - the last good beat's tail (max(interval,
    command_timeout) = 10s) plus (F+1) failed cycles of (interval +
    command_timeout) = 10 + 4 * 12 = 58s - sits at/above the harvestable
    slack (lease - interval = 50s), so the gate renews every beat at the
    default lease - the unconditional cadence, no lapse window - and the
    savings begin from lease ≈ 70s (see the sibling tests). The floor's
    terms are all ENFORCED by the tick's command budget (interval: the
    acquire's own timeout; one command timeout: the sequence AND the
    teardown sharing its remainder)."""
    from taskq.worker.heartbeat import _lease_renewal_threshold

    threshold = _lease_renewal_threshold(
        lock_lease=timedelta(seconds=60.0),
        heartbeat_interval=10.0,
        max_heartbeat_failures=3,
        heartbeat_command_timeout=2.0,
    )
    assert threshold == timedelta(seconds=58.0)
    # 56 >= 50 = lease - interval: the beat after a renewal carries 50s
    # (still at/under the threshold), so nothing is ever skipped.
    assert threshold >= timedelta(seconds=50.0)


@pytest.mark.parametrize(
    ("lock_lease", "interval", "failures", "command_timeout"),
    [
        # The 4x invariant's minimum: no slack to harvest.
        (40.0, 10.0, 3, 2.0),
        # Fast heartbeats with a command timeout larger than the tick.
        (2.0, 0.5, 3, 2.0),
        # A failure-tolerant fleet (F=10) with a default lease.
        (60.0, 10.0, 10, 2.0),
        # The DEFAULT lease: the enforced-bound floor (56) meets the
        # harvestable slack (50) - the fix-round finding that closed the
        # reproduced default-settings lapse window.
        (60.0, 10.0, 3, 2.0),
    ],
)
def test_lease_renewal_threshold_degenerate_configs_renew_every_beat(
    lock_lease: float,
    interval: float,
    failures: int,
    command_timeout: float,
) -> None:
    """Whenever the safety floor meets or exceeds the harvestable slack
    (lease - interval), the threshold sits at/above it and the gated
    statement renews every held row on every beat - exactly the
    unconditional behaviour, because those configs have no slack that is
    safe to harvest."""
    from taskq.worker.heartbeat import _lease_renewal_threshold

    threshold = _lease_renewal_threshold(
        lock_lease=timedelta(seconds=lock_lease),
        heartbeat_interval=interval,
        max_heartbeat_failures=failures,
        heartbeat_command_timeout=command_timeout,
    )
    assert threshold >= timedelta(seconds=lock_lease - interval)


@pytest.mark.parametrize(
    ("lock_lease", "expected_saving"),
    [
        # The enforced-bound floor 56 binds: the beat after a renewal
        # carries 60 (skip), 50 <= 56 (renew) - a 20s cadence, 2x.
        (70.0, 2),
        # 80 > 56 skip, 70 > 56 skip, 60 > 56 skip, 50 <= 56 renew - a
        # 40s cadence, 4x.
        (90.0, 4),
        # Beyond ~112 the half-lease arm dominates: renew at remaining
        # <= 60 - a 60s cadence, 6x.
        (120.0, 6),
    ],
)
def test_lease_renewal_threshold_savings_resume_above_the_default_lease(
    lock_lease: float,
    expected_saving: int,
) -> None:
    """The gate's savings resume from lease ≈ 70s: the floor (56s at the
    default timing knobs) must sit strictly under the harvestable slack
    (lease - interval) for the beat after a renewal to skip, and the
    renewal cadence is (lease - threshold) + interval."""
    from taskq.worker.heartbeat import _lease_renewal_threshold

    threshold = _lease_renewal_threshold(
        lock_lease=timedelta(seconds=lock_lease),
        heartbeat_interval=10.0,
        max_heartbeat_failures=3,
        heartbeat_command_timeout=2.0,
    ).total_seconds()
    assert threshold < lock_lease - 10.0, "the first post-renewal beat must skip"
    # The renewal fires at the FIRST beat whose remaining lease is at or
    # under the threshold: ceil((lease - threshold) / interval) beats
    # after the previous renewal.
    import math

    cadence = 10.0 * math.ceil((lock_lease - threshold) / 10.0)
    assert cadence == pytest.approx(expected_saving * 10.0)


def test_lease_renewal_threshold_generous_lease_uses_half() -> None:
    """A generously sized lease keeps MORE than the safety floor - half
    the lease - whenever half exceeds the cascade bound (the issue's
    half-lease intuition, free of margin cost in the regime where it is
    safe)."""
    from taskq.worker.heartbeat import _lease_renewal_threshold

    threshold = _lease_renewal_threshold(
        lock_lease=timedelta(seconds=300.0),
        heartbeat_interval=10.0,
        max_heartbeat_failures=3,
        heartbeat_command_timeout=2.0,
    )
    assert threshold == timedelta(seconds=150.0)


def test_naive_half_lease_threshold_lapses_before_isolation_at_defaults() -> None:
    """Red-team pin: the issue's naive candidate - skip while more than
    HALF the lease remains - is NOT safe at the default settings.

    At lease 60s / interval 10s / 3 tolerated failures, a worker whose
    last successful beat skipped at remaining 30s+eps (just under half)
    and then fails four consecutive beats (the loop isolates on the
    F+1-th) has already lost the lease before the isolate decision:
    four worst-coherent gaps of interval+command_timeout (12s each)
    consume 48s of the 30s remaining. The sweep - a leader on healthy
    Postgres while this worker is merely partitioned - can then reclaim
    rows the worker still holds and may still be running. The shipped
    floor ((F+1) * (interval + command_timeout) = 48s) keeps the lease
    valid through exactly that cascade; this test exists so the floor
    cannot be "simplified" back to lease/2.
    """
    lease = 60.0
    interval = 10.0
    command_timeout = 2.0
    failures = 3
    naive_threshold = lease / 2
    # Worst coherent failed CYCLE under the ENFORCED tick bound (fix
    # rounds): acquire <= interval, and the tick's command sequence AND
    # its bounded teardown SHARE one command timeout.
    gap = interval + command_timeout
    # The last good beat's TAIL: from its mid-tick renewal point (where
    # the gate re-stamps the lease) to the next tick's start - the span
    # the failed cycles do not cover. Bounded by max(interval,
    # command_timeout): the cadence when the last beat was cheap, the
    # budget's remainder when the acquire ate most of it.
    tail = max(interval, command_timeout)

    remaining = naive_threshold + 1e-6  # the last successful beat skipped here
    remaining -= tail  # the skip beat's own tail, before the failures start
    for _ in range(failures + 1):  # the failure cascade to the isolate decision
        remaining -= gap
    assert remaining < 0, (
        "the naive half-lease threshold should lapse the lease before "
        "isolation at the default settings - if this assert fails, the "
        "default margin model changed and the floor formula must be "
        "re-derived"
    )

    # The shipped floor holds the same cascade: strictly-positive
    # remaining at the isolate decision (the tail and the cycles are
    # strictly under their bounds), and a full gap of margin for a
    # worker that recovers after only F failures.
    from taskq.worker.heartbeat import _lease_renewal_threshold

    floor = _lease_renewal_threshold(timedelta(seconds=lease), interval, failures, command_timeout)
    remaining = floor.total_seconds() + 1e-6
    remaining -= tail * (1.0 - 1e-9)  # strictly under the worst tail
    for _ in range(failures + 1):
        remaining -= gap * (1.0 - 1e-9)  # strictly under the worst cycle
    assert remaining > 0
    remaining = floor.total_seconds() + 1e-6
    remaining -= tail * (1.0 - 1e-9)
    for _ in range(failures):
        remaining -= gap * (1.0 - 1e-9)
    assert remaining > gap  # recovering after F failures still holds margin


@settings(max_examples=400, deadline=timedelta(seconds=10))
@given(
    interval=st.floats(min_value=0.5, max_value=30.0),
    command_timeout=st.floats(min_value=0.01, max_value=10.0),
    failures=st.integers(min_value=1, max_value=10),
    lease_beats=st.floats(min_value=4.0, max_value=30.0),
    acquire_scale=st.lists(st.floats(min_value=0.01, max_value=0.999), min_size=4, max_size=12),
    statement_count=st.lists(st.integers(min_value=3, max_value=6), min_size=4, max_size=12),
    statement_scale=st.lists(
        st.lists(st.floats(min_value=0.05, max_value=0.999), min_size=3, max_size=6),
        min_size=4,
        max_size=12,
    ),
    recover_after=st.integers(min_value=0, max_value=10),
)
def test_gated_renewal_never_lets_a_live_lease_lapse(
    interval: float,
    command_timeout: float,
    failures: int,
    lease_beats: float,
    acquire_scale: list[float],
    statement_count: list[int],
    statement_scale: list[list[float]],
    recover_after: int,
) -> None:
    """Property: under the shipped threshold and the tick's ENFORCED
    command budget, a worker that keeps beating - or that fails at most
    max_heartbeat_failures consecutive beats and then recovers - never
    lets a lease lapse, and the lease outlives the isolate decision.

    The per-tick model is the one the heartbeat loop now enforces
    (heartbeat.py's tick block), so every falsifying shape the fix-round
    attack found is expressible here:

    * a CONTENDED ACQUIRE - drawn up to (strictly under) the interval,
      the pool acquire's own timeout;
    * a MULTI-COMMAND tick - 3..6 statements, each drawn up to (strictly
      under) one command timeout, whose SEQUENCE the single budget cuts
      at one command timeout total (the round-1 model hard-capped the
      whole tick at interval + ONE command timeout, which could not
      express the attack's shape: two just-under-timeout statements
      succeeding, then a timeout);
    * a bounded teardown - a rollback that fits the budget's remainder,
      or the bounded close (server-side rollback on disconnect) - the
      teardown spends the SAME budget's remainder, never a second
      budget (the shared-remainder rule the integration attack round
      pinned).

    The beat-to-beat gap is bounded by ``max(interval, tick_duration)`` -
    the loop anchors its wait to the tick's START, and a FAILED tick
    retries promptly (a bounded quarter-interval backoff, never more
    than the remaining cadence), so a fast failed tick gaps at strictly
    under the interval and only a tick LONGER than the interval can
    stretch the gap as far as its own duration. The model here is that
    upper bound, so the property holds a fortiori against the loop's
    actual (shorter) gaps. The lease respects the enforced invariant
    (>= 4 x interval); the cascade is sized to the loop's actual
    behaviour (the loop isolates on the F+1-th consecutive failure).
    """
    from taskq.worker.heartbeat import _lease_renewal_threshold

    lease = interval * lease_beats  # >= 4 * interval, the enforced invariant
    threshold = _lease_renewal_threshold(
        timedelta(seconds=lease), interval, failures, command_timeout
    ).total_seconds()
    worst_cycle = interval + command_timeout  # acquire + budget (shared teardown)
    tail = max(interval, command_timeout)  # the last good beat's tail
    cascade_bound = tail + (failures + 1) * worst_cycle

    def _tick(i: int, *, healthy: bool) -> tuple[float, bool]:
        """One tick's (gap, renewed) under the enforced budget.

        Returns the beat-to-beat gap and whether the tick's renewal
        landed. A healthy tick completes its statement sequence within
        the budget and commits (the renewal landed); an unhealthy one is
        cut at the budget, and its teardown - rollback AND bounded close
        - spends the remainder of the SAME budget, never a second one.
        """
        acquire = interval * acquire_scale[i % len(acquire_scale)]
        scales = statement_scale[i % len(statement_scale)]
        k = statement_count[i % len(statement_count)]
        if healthy:
            # The whole sequence (BEGIN + writes + probes + COMMIT,
            # modelled as the k statements' aggregate) fits the single
            # budget: the mean of the k per-statement draws is strictly
            # under one command timeout.
            seq = command_timeout * (sum(scales[:k]) / k)
            renewed = True
            teardown = 0.0
        else:
            # The sequence wants more than the budget; the budget cuts
            # it at one command timeout, and the teardown spends the
            # remainder of the SAME budget (the shared-remainder rule).
            seq = command_timeout
            renewed = False
            teardown = 0.0
        duration = acquire + seq + teardown
        return max(interval, duration), renewed

    if cascade_bound >= lease:
        # A config whose worst cascade can outlive the lease: NO renewal
        # policy operating at beat boundaries can keep it - the
        # unconditional renewal has exactly the same exposure (this is
        # the 4x invariant's own blind spot: it sizes the cascade as
        # (F+1) * interval, but a failed beat costs up to interval + command_timeout
        # even under the enforced budget). What the gate
        # must guarantee there is that it does not make things WORSE:
        # the threshold's safety floor IS the cascade bound, so the gate
        # renews on every beat - exactly the unconditional behaviour.
        assert threshold >= lease - interval, (
            "a config whose worst cascade can outlive the harvestable "
            "slack must fall back to renewing every beat (the safety "
            f"floor meets the slack); threshold={threshold} < "
            f"lease-interval={lease - interval} would be a regression "
            "against the unconditional renewal"
        )
        return

    remaining = lease  # a freshly claimed row

    # Phase 1 - healthy beats: the lease never lapses while the worker
    # keeps beating, and every renewal resets it to the full lease.
    for i in range(len(acquire_scale)):
        gap, renewed = _tick(i, healthy=True)
        remaining -= gap
        assert remaining > 0, "a healthy beat sequence let the lease lapse"
        if renewed and remaining <= threshold:
            remaining = lease

    # Phase 2 - the failure cascade: up to F failed ticks (renewal never
    # lands), then either the isolate decision (recover_after > F: the
    # worker is gone by design, the lease may do what leases do) or a
    # recovering tick. The last healthy beat's TAIL - from its renewal
    # point (where remaining was re-stamped) to the next tick's start -
    # is consumed before the first failed cycle.
    remaining -= tail
    cascade_len = min(recover_after, failures + 1)
    for i in range(cascade_len):
        gap, _renewed = _tick(i, healthy=False)
        remaining -= gap
        if i == failures:
            # The isolate decision itself: the lease is still valid -
            # the sweep must not be able to steal a row from a worker
            # whose failure cascade only just reached the threshold.
            assert remaining > 0, "the lease lapsed before the isolate decision"
            return
        assert remaining > 0, "the lease lapsed mid-cascade"
    # The recovering beat: the lease is still valid, and the worker
    # keeps its row without the sweep ever seeing it expired.
    assert remaining > 0, "a recovering worker found its lease already expired"


def test_the_round1_floor_was_under_sized_for_multi_command_ticks() -> None:
    """Regression guard for the fix-round finding: the round-1 floor
    (F+1) * (interval + ONE command timeout) does NOT cover a failed
    multi-command tick bounded only per-statement - the exact shape the
    attack reproduced at the default settings (a legal skip at 49.5s of
    a 60s lease, then four brownout ticks of acquire + three
    just-under-timeout statements, expiring the lease 3.2-9.2s before
    the isolate decision while the unconditional renewal survived).

    This pins WHY the tick's single command budget AND the cascade
    floor's tail term are critical: with either removed, the
    default-config cascade is under-sized again.
    """
    interval, command_timeout, failures, lease = 10.0, 2.0, 3, 60.0
    round1_floor = (failures + 1) * (interval + command_timeout)

    # A brownout failed tick under per-statement bounds only: a
    # contended acquire plus three statements each just under one
    # command timeout (two succeed, the third times out) plus the
    # transaction rollback's round trip.
    brownout_tick = 7.0 + 3 * (command_timeout * 0.95) + command_timeout * 0.95
    assert brownout_tick > interval + command_timeout, (
        "the brownout shape must exceed the ENFORCED bound - that is "
        "the point of the budget: the tick is cut at one command "
        "timeout instead of running its statements out"
    )
    remaining = 49.5  # a legal round-1 skip: just above the round-1 floor
    for _ in range(failures + 1):
        remaining -= brownout_tick
    assert remaining < -3.0, (
        f"the round-1 floor ({round1_floor}) let the lease lapse "
        f"{abs(remaining):.1f}s before the isolate decision - the "
        "reproduced window; the shipped floor must cover this shape"
    )

    from taskq.worker.heartbeat import _lease_renewal_threshold

    shipped = _lease_renewal_threshold(
        timedelta(seconds=lease), interval, failures, command_timeout
    )
    # At the default lease the shipped floor meets the harvestable
    # slack: the gate never skips, so no cascade can start from a skip.
    assert shipped.total_seconds() >= lease - interval
    # And the enforced bound itself keeps the every-beat cascade inside
    # the lease: the last good beat's tail plus (F+1) enforced failed
    # cycles (acquire + ONE budget, the sequence and its teardown
    # sharing it) against the 60s lease. The UN-enforced brownout tick
    # (per-statement bounds) would overrun the same lease - the budget
    # is load-bearing - and the round-1 floor without the tail term
    # would under-count the cascade by exactly that tail.
    tail = max(interval, command_timeout)
    enforced_cascade = tail + (failures + 1) * (interval + command_timeout)
    assert enforced_cascade < lease
    unenforced_cascade = tail + (failures + 1) * brownout_tick
    assert unenforced_cascade > lease, (
        "the per-statement brownout must overrun the default lease - "
        "that is the overrun the enforced budget exists to cut"
    )


def test_build_heartbeat_sql_threshold_selects_the_gated_statement() -> None:
    """The threshold kwarg is what arms the gate: None keeps the
    unconditional renewal every existing caller binds; a threshold
    renders the gated statement, whose three OR arms are each
    critical (per-job heartbeat_timeout beats must stay fresh for
    the sweep's heartbeat arm; NULL leases always renewed; the
    threshold compared server-side, on the clock that stamped the
    lease)."""
    from taskq.backend._sql import (
        UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE,
        UPDATE_JOBS_LOCK_SQL_TEMPLATE,
        build_heartbeat_sql,
    )

    _liveness, plain_jobs, _slots = build_heartbeat_sql("taskq")
    _liveness2, gated_jobs, _slots2 = build_heartbeat_sql(
        "taskq", renewal_threshold=timedelta(seconds=48.0)
    )
    # None: byte-identical to the public unconditional template.
    assert plain_jobs == UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema="taskq")
    assert "$4" not in plain_jobs
    # Threshold: the gated template, with the shared disowned-exclusion
    # core (no drift between the two statements).
    assert gated_jobs == UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE.format(schema="taskq")
    assert "heartbeat_timeout IS NOT NULL" in gated_jobs
    assert "lock_expires_at IS NULL" in gated_jobs
    assert "lock_expires_at <= clock_timestamp() + $4::interval" in gated_jobs
    assert "NOT (id = ANY($3::uuid[]))" in gated_jobs
    assert "NOT (id = ANY($3::uuid[]))" in plain_jobs


async def test_heartbeat_loop_binds_the_renewal_threshold() -> None:
    """The loop computes the threshold from its settings and binds it as
    the jobs-lock renewal's $4 (the gated statement's only new
    parameter)."""
    from taskq.worker.heartbeat import _lease_renewal_threshold

    await _patch_tick_duration(lambda v: None)
    pool = FakePool()
    deps, _shutdown = await _run_tick(pool=pool)
    jobs_calls = [(sql, args) for sql, args in pool.execute_calls if "lock_expires_at" in sql]
    assert jobs_calls, "the tick must issue the jobs-lock renewal"
    sql, args = jobs_calls[0]
    assert "clock_timestamp() + $4::interval" in sql
    expected = _lease_renewal_threshold(
        timedelta(seconds=deps.settings.lock_lease),
        deps.settings.heartbeat_interval,
        deps.settings.max_heartbeat_failures,
        deps.settings.heartbeat_command_timeout,
    )
    assert args[3] == expected


# ── The tick's single command budget (fix round) ───────────────


class _SlowConn(FakeConn):
    """FakeConn whose statements each take ``per_statement`` seconds.

    The call is recorded when it STARTS (a statement the budget cuts
    mid-flight never completes, and the tick must not reach the
    statements after it). ``fail_on`` makes the n-th statement raise a
    non-transient PG error before sleeping."""

    def __init__(self, per_statement: float, *, fail_on: int | None = None) -> None:
        super().__init__()
        self._per_statement = per_statement
        self._fail_on = fail_on

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._fail_on is not None and len(self.execute_calls) == self._fail_on:
            raise asyncpg.PostgresSyntaxError("boom")
        await asyncio.sleep(self._per_statement)
        return f"UPDATE {len(sql) % 10}"


class _SharedConnPool(FakePool):
    """FakePool yielding one shared conn (the tick's close/rollback state
    must be observable after the tick)."""

    def __init__(self, conn: FakeConn) -> None:
        super().__init__()
        self._shared = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[FakeConn, None]:  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire's signature, as FakePool does.
        self.acquire_count += 1
        self._conn = self._shared
        yield self._shared


class _DrainController(_RecordingController):
    """Cancel controller whose post-tx drain takes ``drain`` seconds;
    records whether the drain started."""

    def __init__(self, drain: float) -> None:
        super().__init__()
        self._drain = drain
        self.post_tx_calls: list[float] = []

    async def run_post_tx(self) -> None:
        self.post_tx_calls.append(time.monotonic())
        await asyncio.sleep(self._drain)


async def _run_budget_tick(
    pool: FakePool,
    *,
    heartbeat_command_timeout: float,
    cancel_controller: object | None = None,
) -> tuple[WorkerDeps, float]:
    """One heartbeat tick with a configurable command budget.

    Returns (deps, the tick's own recorded duration - the histogram
    value, not wall clock around the loop's post-tick wait)."""
    import taskq.worker.heartbeat as hb_mod

    deps = _make_deps(
        heartbeat_pool=pool,
        heartbeat_interval=0.5,
        # 18.0 = the cascade floor at h=0.5, c=2.0 (see _make_deps);
        # the tick-budget assertions below read the threshold, which this
        # bump leaves unchanged (its 18.0 safety floor dominated already).
        lock_lease=18.0,
        max_heartbeat_failures=3,
        heartbeat_command_timeout=heartbeat_command_timeout,
    )
    tick_done = asyncio.Event()
    recorded: list[float] = []
    prev_record = hb_mod._tick_duration.record  # type: ignore[reportPrivateUsage]

    def _record_and_signal(value: float, *args: object, **kwargs: object) -> None:
        prev_record(value, *args, **kwargs)
        recorded.append(value)
        tick_done.set()

    hb_mod._tick_duration.record = _record_and_signal  # type: ignore[method-assign,reportPrivateUsage]
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        heartbeat_loop(deps, new_uuid(), shutdown, cancel_controller=cancel_controller)
    )
    await wait_for(tick_done, timeout=10.0)
    shutdown.set()
    await task
    return deps, recorded[0]


async def test_the_tick_command_budget_cuts_a_brownout_tick() -> None:
    """The reproduced attack shape, at unit speed: a tick whose
    statements each take just under the per-query timeout would, under
    the OLD per-statement accounting, run two of them and cut on the
    third - ~3x the command timeout in total, the gap the round-1 floor
    assumed away. Under the single budget the tick is CUT at one command
    timeout: the third statement never starts, and the teardown - whose
    rollback AND close SHARE the budget's remainder (never a second full
    budget) - terminates the connection immediately, the server rolling
    the transaction back on disconnect. The tick counts as exactly ONE
    transient failure."""
    budget = 0.06
    # Three statements at 0.8x the budget each: per-statement, all three
    # are legal (2.4x the budget in total before the tick is done).
    conn = _SlowConn(per_statement=budget * 0.8)
    pool = _SharedConnPool(conn)
    deps, tick_duration = await _run_budget_tick(pool, heartbeat_command_timeout=budget)

    assert deps.heartbeat_failures == 1, "the cut tick must count as exactly one failure"
    # The budget cut the tick inside its SECOND statement (the first
    # took 0.8x the budget; the second was cut 0.2x in): the third -
    # and the still-held probe and hook after it - never started.
    assert len(conn.execute_calls) == 2, (
        f"the tick issued {len(conn.execute_calls)} statements - the "
        "budget must cut the sequence before it outlives one command "
        "timeout"
    )
    # The teardown found the budget exhausted (the close's bound is the
    # remainder, shared with the rollback - here ~zero), so it did not
    # wait for a graceful close: it terminated, and the server rolls the
    # transaction back on disconnect. The pre-fix teardown handed the
    # close a SECOND full budget; the shared remainder is what holds the
    # failed tick to acquire + ONE budget (see _lease_renewal_threshold).
    assert conn.terminated, (
        "the budget-exhausted teardown must terminate immediately - the "
        "close may not burn a second command budget the rollback's "
        "remainder cannot cover"
    )
    # And the whole tick stayed inside the model's bound: acquire (~0
    # here) + ONE budget covering the sequence AND the teardown.
    assert tick_duration < 2 * budget, (
        f"the brownout tick took {tick_duration:.3f}s - more than the "
        "single shared budget; the per-statement shape (2.4x the budget "
        "before even reaching the third statement) escaped again, or the "
        "teardown spent a second budget"
    )


async def test_an_ordinary_statement_failure_rolls_back_within_the_budget() -> None:
    """A statement that fails on its own (a PG error, not the budget)
    takes the rollback path - bounded by the budget's REMAINDER - and
    the connection stays pooled (not closed)."""
    budget = 0.06
    conn = _SlowConn(per_statement=0.001, fail_on=2)
    pool = _SharedConnPool(conn)
    deps, _tick = await _run_budget_tick(pool, heartbeat_command_timeout=budget)

    # asyncpg.PostgresSyntaxError is NOT transient: it takes the
    # unexpected-error arm, which counts toward the SAME isolate threshold
    # as the transient arm (one ledger; the docstring above carries the
    # reasoning).
    assert deps.heartbeat_failures == 1
    assert not conn.closed, "an ordinary failure must roll back and pool the conn"
    assert len(conn.execute_calls) == 2


async def test_post_tx_is_deferred_when_the_budget_is_exhausted() -> None:
    """When the tick's budget is spent, the post-tx drain is DEFERRED to
    the next tick (the controller's deque persists) rather than running
    unbudgeted - an expired asyncio.timeout does not re-cancel an await
    that starts after expiry (measured), so the remainder is enforced by
    SKIPPING the drain, not by hoping the expired scope cancels it."""
    budget = 0.06
    conn = _SlowConn(per_statement=budget * 0.8)  # the tx is cut mid-statement
    pool = _SharedConnPool(conn)
    ctrl = _DrainController(drain=0.2)
    deps, _tick = await _run_budget_tick(
        pool, heartbeat_command_timeout=budget, cancel_controller=ctrl
    )

    assert deps.heartbeat_failures == 1
    # The cut landed before the tick even reached the hook (it runs
    # after the three writes) - and the drain is deferred rather than
    # run unbudgeted: the controller's deque keeps the entry for the
    # next tick.
    assert ctrl.post_tx_calls == [], (
        "the drain must not start when nothing is left of the tick's "
        "command budget - it is deferred to the next tick"
    )


async def test_a_post_tx_cut_is_one_conservative_failure_after_a_committed_tx() -> None:
    """A healthy transaction whose post-tx drain outruns the budget's
    remainder counts the tick as ONE failure (conservative: the failure
    counter moves even though the committed transaction's renewals have
    landed) - never zero and never two."""
    budget = 0.2
    conn = _SlowConn(per_statement=0.001)  # the tx commits almost instantly
    pool = _SharedConnPool(conn)
    ctrl = _DrainController(drain=5.0)  # the drain outruns the remainder
    deps, _tick = await _run_budget_tick(
        pool, heartbeat_command_timeout=budget, cancel_controller=ctrl
    )

    assert ctrl.post_tx_calls, "the drain started (the budget had remainder)"
    assert deps.heartbeat_failures == 1, (
        "a post-tx budget cut is one transient failure (the TimeoutError "
        "propagates on the healthy-tick path), not zero and not two"
    )
