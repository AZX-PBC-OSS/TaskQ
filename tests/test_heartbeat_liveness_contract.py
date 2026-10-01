"""The heartbeat loop's liveness contract with the in-worker watchdog.

``heartbeat_loop`` calls ``deps.liveness.tick("heartbeat", period=interval)``
once per iteration; :class:`LoopLiveness` is detector 2's registry, and the
staleness budget it applies to the registration is ``period * 5`` with a
10s floor. What the watchdog must be able to rely on:

* the REAL loop registers the tick - not just the registry's own unit
  tests against a synthetic name. A mutation that deletes the
  ``deps.liveness.tick`` call from the loop survived every existing pin
  (the heartbeat tests hand the loop a ``MagicMock`` liveness and never
  assert on it), so the wiring was red-first here;
* the registration carries the loop's configured period, so the watchdog
  budgets against the operator's cadence, not a hardcoded one;
* a FAILED tick still ticks the registry: a failed tick is not a beat
  for the lease (it stamps nothing), but the loop IS alive - a worker
  partitioned from PG (every tick failing transiently) must read as
  loop-alive to detector 2 for exactly as long as it keeps trying, and
  only the isolation decision (which exits the loop) may end that.
  Detector 2 tripping on a beating-but-partitioned worker would
  contradict the failure doctrine that reserves the force exit for a
  wedged loop;
* the registry's staleness verdict itself (fresh never stale, past the
  budget stale) is the other half of the contract the detector consumes.
"""

import asyncio
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.assertions import wait_for_condition
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop
from tests.test_heartbeat import FakePool, _make_deps, _patch_tick_duration


@pytest.fixture(autouse=True)
def _restore_heartbeat_module_globals() -> Any:  # pyright: ignore[reportUnusedFunction] # Why: autouse pytest fixture - consumed implicitly by the test runner, not by direct call.
    """Same isolation tests/test_heartbeat.py's autouse fixture provides.

    This file runs OUTSIDE that module, so its fixture does not apply:
    ``_patch_tick_duration`` (imported from there) and the local failure-
    seam patch would otherwise leak into subsequently-collected files -
    the exact cross-file flake that fixture's docstring records.
    """
    import taskq.obs._otel as otel_mod
    import taskq.worker.heartbeat as hb_mod

    saved_isolate = hb_mod.isolate_self
    saved_record = hb_mod._tick_duration.record  # pyright: ignore[reportPrivateUsage]
    saved_update = hb_mod.update_heartbeat_consecutive_failures  # pyright: ignore[reportPrivateImportUsage]  # Why: same narrow re-export seam as the fixture twin in tests/test_heartbeat.py.
    saved_miss = hb_mod.record_heartbeat_miss  # pyright: ignore[reportPrivateImportUsage]  # Why: same narrow re-export seam.
    saved_hb_count = otel_mod._heartbeat_consecutive_failures_count
    try:
        yield
    finally:
        hb_mod.isolate_self = saved_isolate  # type: ignore[method-assign]
        hb_mod._tick_duration.record = saved_record  # type: ignore[method-assign,reportPrivateUsage]
        hb_mod.update_heartbeat_consecutive_failures = saved_update  # pyright: ignore[reportPrivateImportUsage]
        hb_mod.record_heartbeat_miss = saved_miss  # pyright: ignore[reportPrivateImportUsage]
        otel_mod._heartbeat_consecutive_failures_count = saved_hb_count


class _CountingLiveness(LoopLiveness):
    """LoopLiveness that counts its ticks, so a test can wait on the exact
    observable (the loop ticked N times) instead of sampling ages."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tick_count = 0

    def tick(self, name: str, *, period: float) -> None:
        self.tick_count += 1
        super().tick(name, period=period)


async def _run_loop(
    pool: FakePool,
    liveness: LoopLiveness,
    *,
    heartbeat_interval: float = 0.5,
    max_heartbeat_failures: int = 3,
    lock_lease: float = 18.0,
) -> tuple[WorkerDeps, asyncio.Task[None], asyncio.Event]:
    deps = _make_deps(
        heartbeat_pool=pool,
        heartbeat_interval=heartbeat_interval,
        max_heartbeat_failures=max_heartbeat_failures,
        lock_lease=lock_lease,
    )
    deps.liveness = liveness  # type: ignore[assignment]  # Why: the production loop reads the field; the test substitutes the real registry the watchdog reads.
    shutdown = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(deps, new_uuid(), shutdown))
    return deps, task, shutdown


async def test_the_loop_registers_itself_with_the_watchdog_liveness_registry() -> None:
    """The real loop ticks ``deps.liveness`` under the name the watchdog's
    registry keys on, with the loop's configured period.

    Mutation-red first: deleting the ``deps.liveness.tick`` call from
    ``heartbeat_loop`` left every existing heartbeat pin green (the tests
    hand the loop a ``MagicMock`` liveness and never assert on it), so the
    watchdog's detector 2 could silently lose its only signal for the one
    loop whose death every other detector depends on seeing.
    """
    await _patch_tick_duration(lambda v: None)
    liveness = _CountingLiveness()
    pool = FakePool()
    deps, task, shutdown = await _run_loop(pool, liveness)
    try:
        await _wait_for_ticks(liveness, at_least=2)
        ages = liveness.ages()
        assert "heartbeat" in ages, (
            "the heartbeat loop never registered itself with the watchdog's "
            "liveness registry - detector 2 is blind to the one loop whose "
            "death every lease guarantee depends on seeing"
        )
        assert ages["heartbeat"] <= 0.5, (
            f"the heartbeat registration went stale while the loop was alive "
            f"(age {ages['heartbeat']:.3f}s) - the loop must tick the registry "
            "every iteration"
        )
        assert liveness._periods.get("heartbeat") == 0.5, (  # pyright: ignore[reportPrivateUsage]  # Why: the period is the watchdog's budget input; the registry exposes no reader and the pin asserts the loop passes its configured cadence through.
            "the registration must carry the loop's configured period - the "
            "watchdog sizes the staleness budget from it"
        )
        _ = deps
    finally:
        shutdown.set()
        await task


async def test_a_failed_tick_still_ticks_the_watchdog_registry() -> None:
    """A tick that fails against PG (a partitioned worker's every tick)
    must still tick the liveness registry: the loop is alive, only the
    database is unreachable.

    Detector 2's force exit exists for a WEDGED loop, not an unreachable
    one; the transient-failure doctrine (retry promptly, isolate after
    max_heartbeat_failures) owns the partitioned worker. A registry that
    only advanced on successful ticks would read a beating-but-partitioned
    worker as wedged and trip the watchdog on top of the isolation
    countdown.
    """
    await _patch_tick_duration(lambda v: None)
    liveness = _CountingLiveness()
    pool = FakePool(fail_acquire_with=asyncpg.PostgresConnectionError("boom"))
    # lock_lease must cover the F=50 cascade floor the validator enforces
    # (max(0.5, 2) + 51 * 3 = 155): the big failure budget exists so the
    # loop keeps failing transiently for the whole window without
    # isolating, which is exactly the partitioned-worker shape the pin
    # reads.
    deps, task, shutdown = await _run_loop(
        pool, liveness, max_heartbeat_failures=50, lock_lease=155.0
    )
    try:
        await _wait_for_failures(deps, at_least=3)
        ages = liveness.ages()
        assert ages.get("heartbeat", float("inf")) <= 0.5, (
            f"failed ticks stopped ticking the liveness registry (age "
            f"{ages.get('heartbeat', float('inf'))!r}s after "
            f"{deps.heartbeat_failures} consecutive failures) - a worker "
            "partitioned from PG must keep reading loop-alive to the "
            "watchdog for exactly as long as it keeps trying"
        )
    finally:
        shutdown.set()
        await task


async def test_the_registry_trips_stale_only_after_the_loop_stops_ticking() -> None:
    """The watchdog's staleness verdict on a heartbeat registration: at the
    budget it is not yet stale (the budget is a grace), past it the
    registry names the loop - the verdict detector 2 acts on.

    The budget is ``period * 5`` floored at 10s; the registry's comparison
    is strict (``now - ts > budget``), exercised here at the exact edge on
    a mutable fake clock instead of sleeping real time.
    """
    now = [100.0]

    def _clock() -> float:
        return now[0]

    fresh = LoopLiveness(clock=_clock)
    fresh.tick("heartbeat", period=0.5)
    assert fresh.stale() == [], "a fresh registration must not read stale"

    now[0] = 110.0  # exactly the budget: the 10s floor at a 0.5s period
    assert fresh.stale() == [], (
        "at exactly the staleness budget (the 10s floor at a 0.5s period) "
        "the loop is not yet stale - the budget is a grace, not a deadline"
    )

    now[0] = 110.5  # past the budget with no further tick
    assert fresh.stale() == ["heartbeat"], (
        "past the budget with no tick, the registry must name the heartbeat "
        "loop stale - the verdict detector 2 acts on"
    )


async def _wait_for_ticks(liveness: _CountingLiveness, *, at_least: int) -> None:
    """Bounded wait for the loop to land ``at_least`` liveness ticks."""
    await wait_for_condition(
        lambda: liveness.tick_count >= at_least,
        description=f"{at_least} heartbeat liveness ticks",
        timeout=5.0,
    )


async def _wait_for_failures(deps: WorkerDeps, *, at_least: int) -> None:
    """Bounded wait for the loop's failure counter to reach ``at_least``.

    Same seam as tests/test_heartbeat.py's ``_wait_for_heartbeat_failures``
    (the loop calls ``update_heartbeat_consecutive_failures`` immediately
    after every counter flip), restored in ``finally`` here because the
    autouse restore fixture lives in that file, not this one.
    """
    import taskq.worker.heartbeat as hb_mod

    reached = asyncio.Event()
    prev_update = hb_mod.update_heartbeat_consecutive_failures  # pyright: ignore[reportPrivateImportUsage]  # Why: same narrow re-export seam as the assignment below.

    def _update_and_signal(worker_id: str, count: int) -> None:
        prev_update(worker_id, count)
        if deps.heartbeat_failures >= at_least:
            reached.set()

    hb_mod.update_heartbeat_consecutive_failures = _update_and_signal  # type: ignore[method-assign,reportPrivateImportUsage]  # Why: the obs re-export is intentionally narrow (module __all__), the loop's seam is the module attribute; restored in the finally below.
    try:
        await asyncio.wait_for(reached.wait(), timeout=5.0)
    except TimeoutError:
        pytest.fail(
            f"heartbeat loop did not reach {at_least} consecutive failures "
            f"within 5s (stuck at {deps.heartbeat_failures})"
        )
    finally:
        hb_mod.update_heartbeat_consecutive_failures = prev_update  # type: ignore[method-assign]
