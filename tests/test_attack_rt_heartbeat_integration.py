"""ATTACK tests for the heartbeat live-loop INTEGRATION surfaces.

All four surfaces landed together on the same loop; these attacks aim at
the seams BETWEEN them, behavior-level only:

1. The beat-gap bound. Every point a tick can fail (the pool acquire's
   own timeout, an instant acquire error, a mid-tick command inside the
   transaction, the in-tx cancel hook, the post-ledger post-tx drain)
   and every point it can recover from, chained in ONE loop run: the
   worst gap between two GOOD beats must stay under
   ``heartbeat_interval + 2 * heartbeat_command_timeout`` (the bound
   ``_lease_renewal_threshold`` sizes the renewal gate against), and the
   recovery beat after a FAST failed tick must land inside
   ``(1 + _FAILED_TICK_RETRY_FRACTION) * interval`` - the prompt-retry
   promise that makes the ops guidance' ``heartbeat_timeout >= 2x
   interval`` floor actually tolerate one blip.

2. The isolate cadence against the settings validator's own floor math
   (real PG): the isolate decision fires on exactly the
   ``(max_heartbeat_failures + 1)-th`` consecutive failure, its observed
   wall clock fits the validator's cascade floor, and at the decision
   instant the job's lease is STILL VALID - the sweep never beat the
   cascade.

3. The stamp window under contention (looped, forced interleavings):
   a sibling crash racing fresh claims leaves zero phantom cancels, zero
   lost interrupts, and every job in exactly one legal state.

4. The noisy neighbor (CPU starvation while the loop runs): a contended
   event loop must degrade gracefully - the ledger never advances on
   ticks that eventually succeed, a live worker is never isolated, and
   the beat-gap bound holds modulo the observed starvation.
"""

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._bootstrap import (  # pyright: ignore[reportPrivateUsage]  # Why: the crash site under test is the spawner's own guard; the pin drives it through the real spawner.
    _make_sibling_spawner,
    _stamp_interrupt_origins,
)
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import heartbeat_loop
from tests.conftest import (  # type: ignore[attr-defined]  # Why: the consumer harness's fakes live in conftest.
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)

if TYPE_CHECKING:
    from taskq.testing.actor import FakeBackend as _FakeBackend

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

# Tick timing for the fake-pool runs: small enough that a full scenario
# finishes in a second or two, large enough that the bounds under assert
# are not eaten by scheduler jitter.
_I = 0.5  # heartbeat_interval (the settings floor)
_C = 0.15  # heartbeat_command_timeout
_EPS = 0.08  # event-loop jitter allowance on every measured bound
_BEAT_BOUND = _I + 2 * _C + _EPS  # the lease arithmetic's worst beat gap
_RECOVERY_BOUND = (1 + 0.25) * _I + _EPS  # the prompt-retry promise


# ── The scripted-tick harness ────────────────────────────────────────────

#: Tick shapes. Every point the loop can fail, and the recovery that
#: follows, is one of these:
#: - ``ok``: a clean tick (liveness write, renewal, commit).
#: - ``acquire_error``: the acquire raises a transient error instantly
#:   (a refused connection).
#: - ``acquire_timeout``: the acquire burns its whole timeout
#:   (``heartbeat_loop`` acquires with ``timeout=interval``).
#: - ``max_cost``: the compound-brownout tick the ENFORCED budget admits:
#:   the acquire succeeds slowly (a stalled pool create, just under its
#:   own timeout), the tick's command sequence stalls and is cut at the
#:   budget, and the teardown's bounded close stalls to its bound before
#:   terminating. The tick's duration is the enforced worst case.
#: - ``mid_command_2`` / ``mid_command_3``: the tick's command sequence
#:   dies on the 2nd / 3rd statement (inside the transaction, after the
#:   liveness write / after the jobs-lock renewal).
#: - ``hook_fail``: the in-tx cancel hook raises (its own carve-out).
#: - ``post_tx_fail``: the tick COMMITS, then the post-ledger post-tx
#:   drain raises - the last failure point before the wait block.
OK = "ok"
_ACQUIRE_ERROR = "acquire_error"
_ACQUIRE_TIMEOUT = "acquire_timeout"
_MAX_COST = "max_cost"
_HOOK_FAIL = "hook_fail"
_POST_TX_FAIL = "post_tx_fail"


class _ScriptedConn:
    """A fake heartbeat connection: clean statements, scripted commit,
    optionally a mid-sequence command failure (call N raises) or a
    stalled sequence (every statement hangs until the budget cuts it)."""

    def __init__(
        self,
        commits: list[float],
        fail_on_call: int = 0,
        stall: bool = False,
    ) -> None:
        self._commits = commits
        self._fail_on_call = fail_on_call
        self._stall = stall
        self._calls = 0

    async def execute(self, _sql: str, *_args: object) -> str:
        self._calls += 1
        if self._fail_on_call and self._calls == self._fail_on_call:
            raise asyncpg.PostgresConnectionError("chaos: mid-tick command lost")
        if self._stall:
            # The browned-out server: accepts the connection, answers
            # nothing. The tick's single command budget cuts this.
            await asyncio.sleep(30)
        return "UPDATE 1"

    async def fetch(self, _sql: str, *_args: object) -> list[dict[str, object]]:
        if self._stall:
            await asyncio.sleep(30)
        return []

    async def close(self) -> None:
        if self._stall:
            # A graceful close on a hung server stalls: the bounded close
            # consumes whatever budget it was given, then terminates.
            await asyncio.sleep(30)
        return None

    def terminate(self) -> None:
        return None

    def transaction(self) -> "_ScriptedTx":
        return _ScriptedTx(self._commits, self._stall)


class _ScriptedTx:
    def __init__(self, commits: list[float], stall: bool = False) -> None:
        self._commits = commits
        self._stall = stall

    async def start(self) -> None:
        return None

    async def commit(self) -> None:
        # The GOOD-beat observation point: the moment the tick's renewal
        # became durable. The lease arithmetic's beat gap is measured
        # between these.
        self._commits.append(time.monotonic())

    async def rollback(self) -> None:
        # The teardown's rollback round trip on a hung server: it stalls
        # into whatever budget remainder it was given.
        if self._stall:
            await asyncio.sleep(30)
        return None


class _ScriptedController:
    """Cancel-controller double scripting ``hook_fail`` / ``post_tx_fail``.

    ONE script entry per tick: ``run_in_tx`` pops the tick's shape (or,
    when the acquire itself failed and ``run_in_tx`` never ran,
    ``run_post_tx`` pops it) so the pool's and the controller's scripts
    stay aligned tick for tick."""

    def __init__(self, script: list[str]) -> None:
        self._script = list(script)
        self._current: str | None = None

    def _pop(self) -> str:
        if self._current is not None:
            shape = self._current
            self._current = None
            return shape
        return self._script.pop(0) if self._script else OK

    async def run_in_tx(self, _conn: object) -> None:
        self._current = self._script.pop(0) if self._script else OK
        if self._current == _HOOK_FAIL:
            raise OSError("cancel-poll statement lost its connection")

    async def run_post_tx(self) -> None:
        if self._pop() == _POST_TX_FAIL:
            raise OSError("post-tx drain lost its connection")


class _ScriptedPool:
    """One scripted shape per acquire: one tick of the loop per entry.

    After the script runs out, ticks succeed (the recovery the scenarios
    end on). Records every acquire so the tests can count ticks.
    """

    def __init__(self, script: list[str]) -> None:
        self._script = list(script)
        self._script_length = len(script)
        self.acquires = 0
        self.commits: list[float] = []
        self.starts: list[float] = []

    @property
    def script_length(self) -> int:
        return self._script_length

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_ScriptedConn]:
        self.starts.append(time.monotonic())
        caller_timeout = timeout if timeout is not None else _I
        self.acquires += 1
        shape = self._script.pop(0) if self._script else OK
        if shape == _ACQUIRE_ERROR:
            raise asyncpg.PostgresConnectionError("chaos: instant connection blip")
        if shape == _ACQUIRE_TIMEOUT:
            # Burn the caller's whole acquire allowance: the caller
            # acquires with timeout=interval, so a sleep just past that
            # raises TimeoutError (a TRANSIENT_PG_ERRORS member) at
            # exactly the bound the lease arithmetic sizes against.
            await asyncio.wait_for(asyncio.sleep(_I * 10), timeout=caller_timeout)
        if shape == _MAX_COST:
            # The compound-brownout acquire: a stalled pool create that
            # still SUCCEEDS, just under the caller's timeout (a failover
            # gray window: accepts connections, answers nothing).
            await asyncio.sleep(_I * 0.9)
        stall = shape == _MAX_COST
        fail_on_call = 0
        if shape.startswith("mid_command_"):
            fail_on_call = int(shape.rsplit("_", 1)[1])
        yield _ScriptedConn(self.commits, fail_on_call, stall)


def _settings(max_heartbeat_failures: int = 5) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_HEARTBEAT_INTERVAL": str(_I),
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": str(_C),
            # The lease must cover the cascade floor the validator
            # enforces: (F+1) * (I + 2c) at F=5 is 6 * 0.7 = 4.2, and the
            # lag-watchdog knobs must sit inside the lease beside it.
            "TASKQ_LOCK_LEASE": "18.0",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(max_heartbeat_failures),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )


def _deps(pool: _ScriptedPool, max_heartbeat_failures: int) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = _settings(max_heartbeat_failures)
    deps.heartbeat_pool = pool  # type: ignore[assignment]
    deps.disowned_jobs = set()
    deps.heartbeat_failures = 0
    deps.is_leader = asyncio.Event()
    deps.liveness = MagicMock()
    deps.stall_tally = MagicMock()
    deps.stall_tally.metadata_value.return_value = {}
    deps.progress_buffers = {}
    deps.shutdown_started_at = None
    deps.producer_stop_event = asyncio.Event()
    deps.shutdown_phase = 0
    return deps


async def _drive(
    pool: _ScriptedPool,
    *,
    max_heartbeat_failures: int = 5,
    controller: _ScriptedController | None = None,
    isolate_sink: list[int] | None = None,
    isolate_times: list[float] | None = None,
) -> MagicMock:
    """Run the heartbeat loop until the script is exhausted, then a
    settle beat, and return the deps. Isolation is recorded, never run."""
    import taskq.worker.heartbeat as hb_mod

    deps = _deps(pool, max_heartbeat_failures)
    shutdown = asyncio.Event()

    async def _recording_isolate(
        _deps: WorkerDeps, _worker_id: object, _shutdown: asyncio.Event
    ) -> None:
        if isolate_sink is not None:
            isolate_sink.append(pool.acquires)
        if isolate_times is not None:
            isolate_times.append(time.monotonic())
        _shutdown.set()

    saved = hb_mod.isolate_self
    hb_mod.isolate_self = _recording_isolate  # type: ignore[method-assign]
    task = asyncio.create_task(
        heartbeat_loop(deps, _WORKER_ID, shutdown, cancel_controller=controller)
    )
    try:
        deadline = asyncio.get_running_loop().time() + 30.0
        while pool.acquires < pool.script_length and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        # One settle beat: the final scripted tick's ledger decision and
        # its commit record land inside the same tick.
        await asyncio.sleep(2.5 * _I)
    finally:
        shutdown.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)
        hb_mod.isolate_self = saved  # type: ignore[method-assign]
    return deps


def _gaps(commits: list[float]) -> list[float]:
    from itertools import pairwise

    return [b - a for a, b in pairwise(commits)]


# ── 1. The beat-gap bound under adversarial scheduling ──────────────────


def _recovery_gap(commits: list[float]) -> float:
    """The gap between the last good beat BEFORE the failure and the
    first good beat AFTER it (the first commit gap in a 4-tick script
    ``[OK, shape, OK, OK]``: commit → recovery-commit)."""
    gaps = _gaps(commits)
    assert len(gaps) >= 2
    return gaps[0]


@pytest.mark.parametrize(
    ("shape", "fast"),
    [
        (_ACQUIRE_ERROR, True),
        (_ACQUIRE_TIMEOUT, False),
        ("mid_command_2", True),
        ("mid_command_3", True),
        (_HOOK_FAIL, True),
        (_POST_TX_FAIL, True),
    ],
)
async def test_every_fail_point_recovers_inside_the_beat_gap_bound(shape: str, fast: bool) -> None:
    """Fail at every point the loop can fail, recover at that point, and
    hold BOTH bounds: every beat gap ≤ interval + 2*command_timeout, and
    the recovery beat after a FAST failed tick lands inside
    (1 + retry fraction) * interval. The first bound is what
    _lease_renewal_threshold sizes the renewal gate against; the second
    is the pacing promise that makes the 2x-interval heartbeat_timeout
    floor tolerate one transient blip."""
    pool_script = [OK, shape, OK, OK]
    controller = _ScriptedController(pool_script)
    pool = _ScriptedPool(pool_script)
    deps = await _drive(pool, controller=controller)

    assert len(pool.commits) >= 3, (
        f"attack broken: expected 3 good beats, got {len(pool.commits)} (commits={pool.commits})"
    )
    commits = pool.commits[:3]
    starts = pool.starts[:4]
    gaps = _gaps(commits)
    # The tick-START to tick-START cycle bound: max(interval, tick duration),
    # a failed tick's duration bounded by acquire + budget + teardown.
    cycle_bound = _I + 2 * _C + _EPS
    cycle_gaps = _gaps(starts)
    assert max(cycle_gaps) <= cycle_bound, (
        f"RED ({shape}): a tick cycle gap of {max(cycle_gaps):.3f}s exceeded the "
        f"lease arithmetic's worst beat gap {_I + 2 * _C}s "
        f"(cycle gaps={cycle_gaps!r}) - the renewal gate's floor no longer "
        "covers what the loop can actually gap"
    )
    # The renewal-to-renewal bound (what the LEASE actually consumes): the
    # last good beat's tail (its cadence sleep, from the renewal point to
    # the next tick's start) plus one failed cycle.
    renewal_bound = 2 * (_I + _C) + _EPS
    assert max(gaps) <= renewal_bound, (
        f"RED ({shape}): a renewal-to-renewal gap of {max(gaps):.3f}s exceeded "
        f"the enforced worst renewal span {2 * (_I + _C)}s (gaps={gaps!r}) - "
        "the lease is consumed faster between good renewals than the floor "
        "arithmetic accounts for"
    )
    if fast:
        recovery = _recovery_gap(pool.commits)
        assert recovery <= _RECOVERY_BOUND, (
            f"RED ({shape}): the recovery beat landed {recovery:.3f}s after the "
            f"last good beat, past the (1 + retry fraction) * interval promise "
            f"{_RECOVERY_BOUND:.3f}s (gaps={gaps!r}) - a failed tick is not a "
            "beat, it must not also consume the inter-beat wait"
        )
    assert deps.heartbeat_failures == 0, (
        "the recovery beat must reset the ledger - the scenario ends healthy"
    )


async def test_a_failure_chain_holds_the_bound_at_every_link() -> None:
    """One run, every fail point in sequence, each followed by its
    recovery: EVERY consecutive good-beat gap holds the bound."""
    pool_script = [
        OK,
        _ACQUIRE_ERROR,
        OK,
        "mid_command_3",
        OK,
        _POST_TX_FAIL,
        OK,
        _ACQUIRE_TIMEOUT,
        OK,
        _HOOK_FAIL,
        OK,
    ]
    controller = _ScriptedController(pool_script)
    pool = _ScriptedPool(pool_script)
    await _drive(pool, controller=controller)

    gaps = _gaps(pool.commits[:7])
    assert len(gaps) == 6, f"attack broken: the chain did not all run ({gaps!r})"
    cycle_gaps = _gaps(pool.starts[:11])
    worst_cycle = max(cycle_gaps)
    assert worst_cycle <= _I + 2 * _C + _EPS, (
        f"RED: in the failure chain a tick cycle gap of {worst_cycle:.3f}s "
        f"exceeded the worst beat gap {_I + 2 * _C}s "
        f"(cycle gaps={cycle_gaps!r}) - the bound must hold at EVERY link, "
        "not on average"
    )
    worst_renewal = max(gaps)
    assert worst_renewal <= 2 * (_I + _C) + _EPS, (
        f"RED: in the failure chain a renewal-to-renewal gap of "
        f"{worst_renewal:.3f}s exceeded the enforced worst renewal span "
        f"{2 * (_I + _C)}s (gaps={gaps!r})"
    )


async def test_consecutive_fast_failures_still_hold_the_bound() -> None:
    """Two consecutive fast failed ticks between good beats: the failed
    cycles stack, the bound must still hold across the whole outage."""
    pool_script = [OK, _ACQUIRE_ERROR, "mid_command_2", OK, OK]
    controller = _ScriptedController(pool_script)
    pool = _ScriptedPool(pool_script)
    await _drive(pool, controller=controller)

    gaps = _gaps(pool.commits[:3])
    assert len(gaps) == 2, f"attack broken: the scenario did not all run ({gaps!r})"
    cycle_gaps = _gaps(pool.starts[:5])
    worst_cycle = max(cycle_gaps)
    assert worst_cycle <= _I + 2 * _C + _EPS, (
        f"RED: two consecutive fast failures cycled {worst_cycle:.3f}s past "
        f"the worst beat gap {_I + 2 * _C}s (cycle gaps={cycle_gaps!r})"
    )
    worst = max(gaps)
    assert worst <= 2 * (_I + _C) + _EPS, (
        f"RED: two consecutive fast failures gapped {worst:.3f}s past the "
        f"enforced worst renewal span {2 * (_I + _C)}s (gaps={gaps!r})"
    )


# ── 2. Ledger honesty: the prompt retry must not double-count ───────────


async def test_the_prompt_retry_counts_a_blip_exactly_once() -> None:
    """One failed tick followed by its prompt-retried recovery: the blip
    counts exactly once (the recovery resets the ledger to zero), the
    loop consumes exactly two ticks for the two shapes, and the halfway
    early warning never fires for a run that peaked at one failure."""
    pool_script = [_ACQUIRE_ERROR, OK, OK]
    pool = _ScriptedPool(pool_script)
    deps = await _drive(pool)

    assert pool.acquires >= 3, f"attack broken: the script did not run out ({pool.acquires})"
    assert len(pool.commits) >= 2, "attack broken: the recovery beat never committed"
    assert deps.heartbeat_failures == 0, (
        "RED: the prompt retry double-counted the blip - the failed tick "
        "incremented AND its retry left the counter non-zero, so repeated "
        "blips would consume the isolate budget twice as fast as documented"
    )


async def test_post_tx_failures_count_toward_the_isolate_threshold() -> None:
    """A post-ledger pre-sleep failure (the post-tx drain raising on an
    otherwise-committed tick) is a failed tick: F consecutive ones must
    isolate on the (F+1)-th, exactly like any other failure point."""
    pool_script = [_POST_TX_FAIL] * 3
    controller = _ScriptedController(pool_script)
    pool = _ScriptedPool(pool_script)
    isolate_sink: list[int] = []
    await _drive(pool, max_heartbeat_failures=2, controller=controller, isolate_sink=isolate_sink)

    assert isolate_sink, (
        "RED: post-tx drain failures never reached the isolate threshold - "
        "the ledger missed the last failure point before the wait block"
    )
    assert isolate_sink[0] == 3, (
        f"RED: post-tx failures isolated at tick {isolate_sink[0]}, the "
        "documented tick is 3 (the (F+1)-th consecutive failure)"
    )


async def test_a_max_cost_brownout_tick_holds_the_renewal_span() -> None:
    """The compound-brownout blip - a stalled-but-successful acquire, a
    budget-cut command sequence, a teardown close that stalls to its
    bound - is the worst tick the ENFORCED budget admits. One such blip
    between two good beats must gap the renewals by at most the last
    good beat's tail plus the blip's own enforced cost: two
    (interval + command_timeout) spans."""
    pool_script = [OK, _MAX_COST, OK]
    pool = _ScriptedPool(pool_script)
    await _drive(pool)

    assert len(pool.commits) >= 2, "attack broken: the recovery beat never committed"
    renewal_gap = pool.commits[1] - pool.commits[0]
    assert renewal_gap <= 2 * (_I + _C) + _EPS, (
        f"RED: one max-cost brownout tick gapped the renewals "
        f"{renewal_gap:.3f}s - past the enforced worst renewal span "
        f"{2 * (_I + _C)}s (the last good beat's tail + the blip's own "
        "acquire + budget + teardown). The teardown's bounded close is "
        "burning a SECOND command budget after the sequence's budget "
        "remainder: the enforced per-tick cost exceeds what the lease "
        "arithmetic's worst-beat-gap term sizes."
    )


async def test_the_max_cost_cascade_fits_the_validator_floor() -> None:
    """F+1 max-cost failed ticks (the validator's own worst cascade, each
    term enforced) from the last good renewal: the isolate decision must
    land INSIDE the cascade floor the settings validator computes from
    the SAME settings - the lease the validator accepts must outlive the
    cascade its own arithmetic describes. Measured, not modelled: the
    loop runs the real pacing against the scripted worst-case ticks and
    the decision's wall clock is compared to the floor."""
    max_failures = 2
    pool_script = [OK, _MAX_COST, _MAX_COST, _MAX_COST]
    pool = _ScriptedPool(pool_script)
    isolate_times: list[float] = []
    isolate_sink: list[int] = []
    deps = await _drive(
        pool,
        max_heartbeat_failures=max_failures,
        isolate_sink=isolate_sink,
        isolate_times=isolate_times,
    )
    del deps

    assert isolate_sink, "attack broken: the max-cost cascade never isolated"
    assert isolate_sink[0] == max_failures + 2, (
        f"RED: the cascade isolated at tick {isolate_sink[0]}; with the seed "
        f"beat first, the (F+1)-th = {max_failures + 1} consecutive failure "
        f"is tick {max_failures + 2}"
    )
    # The last GOOD renewal (the seed tick's commit) -> the isolate decision.
    cascade = isolate_times[0] - pool.commits[0]
    # The settings validator's own floor math, mirrored from post_load:
    # the last good beat's tail plus (F+1) enforced failed cycles
    # (acquire + ONE budget, the sequence and its teardown sharing it).
    floor = max(_I, _C) + (max_failures + 1) * (_I + _C)
    assert cascade <= floor + _EPS, (
        f"RED: the observed max-cost cascade took {cascade:.3f}s from the "
        f"last good renewal to the isolate decision, past the settings "
        f"validator's own floor {floor:.3f}s - the validator accepts a "
        "lease equal to this floor, so the lease can expire BEFORE the "
        "worker isolates and the reclaim sweep steals a live worker's "
        "rows. The un-accounted span is the last good beat's tail (its "
        "cadence sleep from the renewal point to the next tick's start) "
        "and the teardown close's second command budget - the close must "
        "share the tick budget's remainder with the rollback, and the "
        "floor must count the tail."
    )


async def test_a_wake_storm_never_advances_the_ledger() -> None:
    """A cancel-NOTIFY storm (the wake event set continuously) wakes every
    wait early: the ticks all succeed, the ledger never advances, and a
    healthy worker is never isolated by the wake path."""
    pool = _ScriptedPool([])
    deps = _deps(pool, max_heartbeat_failures=2)
    shutdown = asyncio.Event()
    wake = asyncio.Event()
    isolate_sink: list[int] = []

    import taskq.worker.heartbeat as hb_mod

    async def _recording_isolate(
        _deps: WorkerDeps, _worker_id: object, _shutdown: asyncio.Event
    ) -> None:
        isolate_sink.append(pool.acquires)
        _shutdown.set()

    async def _storm() -> None:
        while not shutdown.is_set():
            wake.set()
            await asyncio.sleep(0.005)

    saved = hb_mod.isolate_self
    hb_mod.isolate_self = _recording_isolate  # type: ignore[method-assign]
    storm = asyncio.create_task(_storm())
    task = asyncio.create_task(heartbeat_loop(deps, _WORKER_ID, shutdown, cancel_wake_event=wake))
    try:
        await asyncio.sleep(10 * _I)
    finally:
        shutdown.set()
        storm.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)
            await asyncio.wait_for(storm, timeout=5.0)
        hb_mod.isolate_self = saved  # type: ignore[method-assign]

    assert not isolate_sink, "RED: a wake storm isolated a healthy worker"
    assert deps.heartbeat_failures == 0, (
        "RED: a wake storm advanced the failure ledger without any failed tick"
    )
    assert pool.acquires >= 5, "attack broken: the storm scenario did not tick"


# ── 3. The isolate cadence against the validator's own floor math ────────

# Real-PG numbers: F=2, I=0.5, c=0.1 -> the validator's cascade floor is
# max(I, c) + (F+1) * (I + c) = 0.5 + 3 * 0.6 = 2.3s, and the lease is set
# to EXACTLY that floor (the validator allows equality): the cascade must
# fit inside the lease with nothing to spare but the validator's own math.
_PG_I = 0.5
_PG_C = 0.1
_PG_F = 2
_PG_LEASE = max(_PG_I, _PG_C) + (_PG_F + 1) * (_PG_I + _PG_C)


async def _pg_setup(pg_dsn: str, overrides: dict[str, str]) -> tuple[object, WorkerDeps, str]:
    from taskq.migrate import apply_pending
    from taskq.testing.settings import make_integration_settings
    from taskq.worker.deps import open_worker_deps

    merged: dict[str, str] = {
        "HEARTBEAT_INTERVAL": str(_PG_I),
        "HEARTBEAT_COMMAND_TIMEOUT": str(_PG_C),
        "MAX_HEARTBEAT_FAILURES": str(_PG_F),
        "CANCELLATION_GRACE_PERIOD": "0.0",
        "CLEANUP_GRACE_PERIOD": "0.0",
    }
    merged.update(overrides)
    settings = make_integration_settings(pg_dsn, **merged)
    schema = settings.schema_name
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    from contextlib import AsyncExitStack

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    return stack, deps, schema


@pytest.mark.integration
async def test_the_isolate_decision_fits_the_validator_floor_and_beats_the_lease(
    pg_dsn: str,
) -> None:
    """Real PG, the lease at EXACTLY the validator's cascade floor: every
    acquire fails, the isolate decision fires on exactly the (F+1)-th
    consecutive failure, its observed wall clock fits the floor the
    validator computes from the SAME settings, and at the decision
    instant the running job's lease is STILL VALID - the reclaim sweep's
    deadline never beat the cascade, the isolate's own guarded UPDATE is
    what transitions the row."""
    from taskq.testing.pg import create_running_job, create_worker

    stack, deps, schema = await _pg_setup(pg_dsn, {"LOCK_LEASE": str(_PG_LEASE)})
    try:
        assert deps.settings.pg_dsn_direct is not None
        worker_id = new_uuid()
        real_pool = deps.heartbeat_pool

        async with real_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            job_id = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_PG_LEASE),
            )

        class _DeadPool:
            """Every acquire fails instantly with a transient error."""

            def __init__(self) -> None:
                self.acquires = 0

            def acquire(self, *, timeout: float | None = None) -> object:
                del timeout
                self.acquires += 1
                raise asyncpg.PostgresConnectionError("chaos: PG is gone")

            async def close(self) -> None:
                return None

        dead_pool = _DeadPool()
        deps.heartbeat_pool = dead_pool  # type: ignore[assignment]

        import taskq.worker.heartbeat as hb_mod

        observed: dict[str, object] = {}
        observed_row: dict[str, object] | None = None
        real_isolate = hb_mod.isolate_self

        async def _spy_isolate(
            spy_deps: WorkerDeps, spy_worker_id: UUID, spy_shutdown: asyncio.Event
        ) -> None:
            # The observation point: the isolate DECISION's wall clock,
            # and the row's state at that instant - before isolate's own
            # writes move it.
            nonlocal observed_row
            observed["elapsed"] = time.monotonic() - started
            async with real_pool.acquire() as conn:
                observed_row = dict(
                    await conn.fetchrow(
                        f"SELECT status::text AS status, "  # noqa: S608
                        f"locked_by_worker, "
                        f"lock_expires_at > clock_timestamp() AS lease_valid "
                        f'FROM "{schema}".jobs WHERE id = $1',
                        job_id,
                    )  # type: ignore[arg-type]
                )
            await real_isolate(spy_deps, spy_worker_id, spy_shutdown)

        hb_mod.isolate_self = _spy_isolate  # type: ignore[method-assign]
        shutdown = asyncio.Event()
        started = time.monotonic()
        task = asyncio.create_task(heartbeat_loop(deps, worker_id, shutdown))
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=15.0)
            await asyncio.wait_for(task, timeout=5.0)
        finally:
            hb_mod.isolate_self = real_isolate  # type: ignore[method-assign]

        assert dead_pool.acquires == _PG_F + 1, (
            f"RED: the isolate decision fired after {dead_pool.acquires} failed "
            f"ticks, the documented cadence is exactly the (F+1)-th = "
            f"{_PG_F + 1}"
        )
        elapsed = float(observed["elapsed"])
        floor = max(_PG_I, _PG_C) + (_PG_F + 1) * (_PG_I + _PG_C)
        assert elapsed <= floor + 0.5, (
            f"RED: the cascade took {elapsed:.3f}s to reach the isolate "
            f"decision, past the settings validator's own floor math "
            f"({floor}s) + scheduling slack - the observed cascade no longer "
            "fits the invariant the validator enforces"
        )
        row = observed_row
        assert row is not None, "attack broken: the spy never read the row"
        assert row["status"] == "running", (
            f"RED: at the isolate decision the job was {row['status']!r} - a "
            "reclaim beat the cascade to the lease, the worker never got the "
            "chance to hand its own rows back"
        )
        assert row["lease_valid"], (
            "RED: the lease had already expired at the isolate decision - the "
            "cascade's wall clock exceeded the lease the validator sizes at "
            "max(interval, command_timeout) + (F+1) * (interval + command_timeout)"
        )
        # The isolate completed its own transitions: the job (transient,
        # attempt 1 of 3) is re-pended for the fleet.
        async with real_pool.acquire() as conn:
            final = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
                job_id,
            )
        assert final == "pending", f"RED: after isolate_self the re-pend arm left the job {final!r}"
    finally:
        await stack.aclose()  # type: ignore[attr-defined]


# ── 4. The stamp window under contention ─────────────────────────────────


def _crash_deps(settings: WorkerSettings, registry: ActiveJobRegistry) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.settings = settings
    deps.disowned_jobs = set()
    deps.progress_buffers = {}
    deps.worker_pool = None
    deps.redis_client = None
    deps.shutdown_started_at = None
    deps.active_jobs = registry
    return deps


def _crash_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.4",
            "TASKQ_TERMINATION_GRACE_PERIOD": "20.0",
        }
    )


@pytest.mark.parametrize("gate_at", ["pre_stamp", "post_stamp"])
async def test_a_crash_racing_fresh_claims_never_phantom_cancels(gate_at: str) -> None:
    """Looped, forced interleavings: a pre-registered job AND a fresh
    claim whose registration completes around the crash's stamp - the
    stamp-to-delivery gap - must BOTH end in exactly one legal state,
    with zero phantom cancels and zero lost interrupts, whatever order
    the registration, the stamp, and the group's cancellation sweep land
    in."""
    for i in range(15):
        backend: _FakeBackend = FakeBackend()
        settings = _crash_settings()
        registry = ActiveJobRegistry()
        deps = _crash_deps(settings, registry)
        job = make_job_row()  # the pre-registered job the crash interrupts
        racing = make_job_row()  # the fresh claim racing the stamp
        crash_go = asyncio.Event()
        shutdown_event = asyncio.Event()
        stamped = asyncio.Event()
        claim_gate = asyncio.Event()
        real_register = registry.register

        async def gated_register(job_id: object, task: object, ctx: object) -> object:
            # Only the RACING claim is parked: until the test's chosen
            # instant in the stamp window, then a scheduling breath, then
            # the real registration. The pre-registered job passes through.
            # (The signature mirrors ActiveJobRegistry.register's
            # (job_id, task, ctx) - a gate with the wrong arity kills the
            # consumer at registration and the probe degenerates to
            # asserting nothing, the vacuous-pin trap.)
            if job_id == racing.id:
                await claim_gate.wait()
                await asyncio.sleep(0)
            return await real_register(job_id, task, ctx)  # type: ignore[arg-type]

        registry.register = gated_register  # type: ignore[method-assign]

        async def body(_running: object, _ctx: object) -> object:
            await asyncio.sleep(3600)
            return {"unreachable": True}

        async def consumer(target: object) -> None:
            await consume_one_job(
                as_backend(backend),
                target,  # type: ignore[arg-type]
                _WORKER_ID,
                deps=deps,  # type: ignore[arg-type]
                run_actor=body,  # type: ignore[arg-type]
                actor_config=default_actor_config(),
                payload_type=EmptyPayload,
                clock=FakeClock(_NOW),
                active_jobs=registry,
            )

        async def crashing() -> None:
            await crash_go.wait()
            if gate_at == "pre_stamp":
                # Release the racing claim BEFORE the stamp: it must be
                # registered (and so stamped) before the group cancels.
                claim_gate.set()
                await wait_for_condition(
                    lambda: registry.get(racing.id) is not None,
                    description="the racing claim's registration",
                    timeout=5.0,
                )
            _stamp_interrupt_origins(deps)
            stamped.set()
            if gate_at == "post_stamp":
                # Release the racing claim INSIDE the stamp-to-delivery
                # gap: the stamp has run, the group's cancellation sweep
                # has not reached this consumer yet.
                claim_gate.set()
            shutdown_event.set()
            raise RuntimeError("leader sweep hit a dead PG")

        try:
            async with asyncio.TaskGroup() as tg:
                spawn = _make_sibling_spawner(tg, shutdown_event, deps)
                spawn(consumer(job))
                spawn(consumer(racing))
                spawn(crashing())
                await wait_for_condition(
                    lambda: registry.get(job.id) is not None,
                    description="the first consumer's registration",
                    timeout=5.0,
                )
                crash_go.set()
        except BaseExceptionGroup:
            pass

        cancelled = list(backend.mark_cancelled_calls)  # type: ignore[union-attr]
        interrupted = list(backend.mark_interrupted_calls)  # type: ignore[union-attr]
        assert not cancelled, (
            f"RED iter {i} ({gate_at}): a claim racing the crash terminalised "
            f"as a cancel - a phantom operator cancel no operator issued "
            f"({cancelled!r})"
        )
        for target in (job, racing):
            its_cancels = [c for c in cancelled if c["job_id"] == target.id]  # type: ignore[union-attr]
            its_interrupts = [c for c in interrupted if c["job_id"] == target.id]  # type: ignore[union-attr]
            assert not its_cancels, (
                f"RED iter {i} ({gate_at}): job {target.id} terminalised as a "
                "cancel - a phantom operator cancel no operator issued"
            )
            assert len(its_interrupts) <= 1, (
                f"RED iter {i} ({gate_at}): job {target.id} wrote more than "
                "one interrupt - not exactly one legal state"
            )
            if registry.get(target.id) is not None:
                assert its_interrupts, (
                    f"RED iter {i} ({gate_at}): job {target.id} was registered "
                    "when the crash tore the group down but was never released "
                    "to the fleet - a lost interrupt"
                )
        assert interrupted, (
            f"RED iter {i} ({gate_at}): no job was released to the fleet - "
            "the pre-registered job's interrupt was lost"
        )


# ── 5. The noisy neighbor: CPU starvation while the loop runs ────────────


class _ContendedPool:
    """A real-shaped pool whose every tick succeeds, slowly: each acquire
    waits out a bounded contention stall (well under the interval) before
    handing out a clean connection. The event loop itself is contended
    separately, by the busy-loop competitors the test spawns."""

    def __init__(self, stall: float) -> None:
        self._stall = stall
        self.commits: list[float] = []
        self.isolates = 0

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_ScriptedConn]:
        del timeout
        await asyncio.sleep(self._stall)
        yield _ScriptedConn(self.commits)


async def _spin_contended(seconds: float, stop: asyncio.Event) -> None:
    """CI-like CPU contention: busy-spin slices with breaths, so the
    event loop is heavily loaded but still functions (the documented
    graceful-degradation regime, not a total wedge)."""
    deadline = time.monotonic() + seconds
    while not stop.is_set() and time.monotonic() < deadline:
        spin_until = time.monotonic() + 0.02
        while time.monotonic() < spin_until:
            pass  # the noisy neighbor eating the loop's slices
        await asyncio.sleep(0.005)


async def test_starvation_never_isolates_a_live_worker_and_degrades_gracefully() -> None:
    """Under CI-like CPU contention (busy-loop neighbors spinning through
    the whole run) every heartbeat tick still SUCCEEDS: the ledger never
    advances, no isolate fires, and the beat-gap bound holds modulo the
    starvation the contention actually inflicts - measured, not assumed,
    by a monitor task that records every event-loop stall. The bound may
    degrade by exactly the observed stalls (the documented graceful
    degradation), never by the loop's own arithmetic."""
    stall = _I * 0.5  # every tick is slow, but every tick lands
    pool = _ContendedPool(stall)
    deps = _deps(pool, max_heartbeat_failures=2)
    shutdown = asyncio.Event()
    isolate_sink: list[int] = []
    stalls: list[float] = []

    async def _monitor() -> None:
        # Record the worst event-loop scheduling lag the contention
        # inflicts on a 10ms timer.
        while not shutdown.is_set():
            t0 = time.monotonic()
            await asyncio.sleep(0.01)
            stalls.append(time.monotonic() - t0 - 0.01)

    import taskq.worker.heartbeat as hb_mod

    async def _recording_isolate(
        _deps: WorkerDeps, _worker_id: object, _shutdown: asyncio.Event
    ) -> None:
        isolate_sink.append(pool.isolates)
        _shutdown.set()

    saved = hb_mod.isolate_self
    hb_mod.isolate_self = _recording_isolate  # type: ignore[method-assign]
    run_seconds = 8 * _I
    stop = asyncio.Event()
    neighbors = [asyncio.create_task(_spin_contended(run_seconds + 1.0, stop)) for _ in range(3)]
    monitor = asyncio.create_task(_monitor())
    task = asyncio.create_task(heartbeat_loop(deps, _WORKER_ID, shutdown))
    try:
        await asyncio.sleep(run_seconds)
    finally:
        shutdown.set()
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5.0)
            await asyncio.gather(*neighbors, return_exceptions=True)
            await asyncio.wait_for(monitor, timeout=5.0)
        hb_mod.isolate_self = saved  # type: ignore[method-assign]

    assert not isolate_sink, (
        "RED: CPU starvation isolated a LIVE worker - every tick succeeded, "
        "the ledger must never have reached the isolate threshold"
    )
    assert deps.heartbeat_failures == 0, (
        "RED: starvation advanced the failure ledger though every tick "
        "eventually succeeded - slow beats are not failed beats"
    )
    assert len(pool.commits) >= 4, (
        f"attack broken: the contended loop only landed {len(pool.commits)} "
        "good beats - it starved outright instead of degrading gracefully"
    )
    worst_stall = max(stalls) if stalls else 0.0
    worst_gap = max(_gaps(pool.commits))
    assert worst_gap <= _I + 2 * _C + worst_stall + _EPS, (
        f"RED: under starvation a beat gap of {worst_gap:.3f}s exceeded the "
        f"worst beat gap {_I + 2 * _C}s plus the observed worst event-loop "
        f"stall {worst_stall:.3f}s - the loop's own arithmetic, not the "
        "neighbor's contention, broke the bound"
    )
