"""Pins for the shutdown-ordering contract: no leadership while stopping.

The owner's invariant (leader.py's doc header, "Shutdown ordering
contract"): a worker that is shutting down NEVER assumes the leader role,
and a leader that begins shutting down hands the lease over EARLY, at
shutdown START, before the job drain, so a rolling deploy never has a
leaderless window and never sees a dying pod win or hold leadership.

Four behaviours are pinned here at unit level against the REAL
``_election_loop`` with hand-rolled fakes (the conventions mirror
``tests/test_leader.py``; no PG required, no wall-clock dependencies
beyond generous event-driven waits):

* the park: once the stop signal (``deps.shutdown_start_event``) is
  observed, the elect attempt count stays frozen across would-be ticks
  and across broadcast wakes;
* the race: an elect statement in flight when the stop lands that wins
  anyway is handed straight back (fenced resign over the conn the elect
  just used), the assume path never starts, is_leader is never set;
* the early handover: a leader's resign lands while the worker-wide
  shutdown event is STILL CLEAR (the drain is still running), not at
  teardown;
* the wake seam: ``wake_election`` no-ops while stopping;
* the quiesce: the handover demotes BEFORE the resign write issues (no
  leader-gated loop can start a new sweep mid-handover) and cancels
  nothing (an in-flight sweep's transaction completes or aborts whole on
  its own; the handover never closes or terminates a conn).

Two integration pins (real PG, ``integration`` marker) cover what fakes
cannot: the lease row's server-side state during a live drain, and a
drain that completes under a DIFFERENT worker's leadership.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for, wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.deps import LeaderTerm, WorkerDeps
from taskq.worker.leader import MaintenanceLeader

# ruff: noqa: S608  # Why: every interpolated identifier is the test-built schema name, validated against _IDENT_RE at settings load; all values are $-bound.

pytestmark = pytest.mark.asyncio

#: The election cadence for these loops. Every wait below is event-driven;
#: this only sizes the would-be ticks the park must outlast.
_HEARTBEAT = 0.01
_LEADER_LEASE = 40.0

_ELECT_PREFIX = "INSERT INTO"
_RENEW_PREFIX = "UPDATE"


def _worker_settings() -> WorkerSettings:
    data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT),
        # 3.0 + the tiny command timeout satisfies the cascade floor:
        # 4 * (0.5 + 2 * 0.1) = 2.8 <= 3.0 (the lease value itself is
        # arbitrary for these unit doubles).
        "TASKQ_LEADER_LEASE": str(_LEADER_LEASE),
        "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
        "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
        "TASKQ_MAX_HEARTBEAT_FAILURES": "3",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    return WorkerSettings.load_from_dict(data, validate=False)


class FakeConn:
    """asyncpg.Connection stand-in with recording fetchval/execute.

    Lease statements are answered the way the real driver would: the
    elect and the renew return the term's timestamp when the double is
    configured to hold the row and no row at all otherwise; the resign's
    command tag is a real DELETE count, which ``resign()`` reads.
    """

    def __init__(
        self,
        *,
        holds_row: bool = False,
        on_fetchval: Any | None = None,
        on_execute: Any | None = None,
    ) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._closed = False
        self.close_calls = 0
        self.terminated = False
        self._holds_row = holds_row
        self._on_fetchval = on_fetchval
        self._on_execute = on_execute

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if self._on_fetchval is not None:
            self._on_fetchval(sql, args)
        if sql.lstrip().upper().startswith("SELECT CLOCK_TIMESTAMP()"):
            return datetime.now(UTC)
        if "maintenance_leader" in sql:
            return datetime.now(UTC) if self._holds_row else None
        return None

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._on_execute is not None:
            self._on_execute(sql, args)
        if sql.lstrip().upper().startswith("DELETE FROM"):
            return "DELETE 1"
        return "UPDATE 1"

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        self.close_calls += 1
        self._closed = True

    def terminate(self) -> None:
        self.terminated = True
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


class _InFlightElectConn(FakeConn):
    """A conn whose ELECT statement hangs until the test releases it.

    Models the SIGTERM-mid-elect race: the statement is on the wire (the
    round trip started) before the stop signal lands, and the win comes
    back afterwards. Everything else behaves like :class:`FakeConn`.
    """

    def __init__(self) -> None:
        super().__init__(holds_row=False)
        self.elect_started = asyncio.Event()
        self.elect_gate = asyncio.Event()
        self.won_elected_at = datetime(2025, 1, 1, tzinfo=UTC)

    async def fetchval(self, sql: str, *args: object) -> object:
        if sql.lstrip().upper().startswith(_ELECT_PREFIX) and "maintenance_leader" in sql:
            self.elect_started.set()
            await self.elect_gate.wait()
            # The win comes back after the (test-arranged) stop.
            return self.won_elected_at
        return await super().fetchval(sql, *args)


def _make_deps(conn: FakeConn) -> WorkerDeps:
    deps = WorkerDeps(
        settings=_worker_settings(),
        dispatcher_pool=cast(Any, SimpleNamespace()),
        heartbeat_pool=cast(Any, SimpleNamespace()),
        worker_pool=cast(Any, SimpleNamespace()),
        notify_conn=None,
        leader_conn=cast(Any, conn),
    )
    return deps


def _make_leader(deps: WorkerDeps, conn: FakeConn) -> MaintenanceLeader:
    worker_id = new_uuid()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return MaintenanceLeader(
        deps,
        worker_id,
        cast(Any, InMemoryBackend(clock=clock)),
        clock=cast(Any, SystemClock()),
    )


def _elect_attempts(conn: FakeConn) -> int:
    return sum(
        1 for sql, _ in conn.fetchval_calls if sql.lstrip().upper().startswith(_ELECT_PREFIX)
    )


def _renew_attempts(conn: FakeConn) -> int:
    return sum(
        1 for sql, _ in conn.fetchval_calls if sql.lstrip().upper().startswith(_RENEW_PREFIX)
    )


def _resign_calls(conn: FakeConn) -> list[tuple[str, tuple[object, ...]]]:
    return [
        (sql, args)
        for sql, args in conn.execute_calls
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql
    ]


async def _stop_task(task: asyncio.Task[Any], shutdown: asyncio.Event) -> None:
    shutdown.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)


# ── Pin 1: the park ─────────────────────────────────────────────────────


async def test_pin_a_stopping_worker_never_attempts_the_election_again() -> None:
    """Once the stop signal is observed, the elect attempt count stays 0.

    The loop attempts on its cadence up to the stop; after it, N would-be
    ticks and several broadcast wakes produce ZERO further attempts: the
    loop is parked attempt-free for the rest of the shutdown, and the
    wake handler's refusal (pin 4) has no attempt to wake into anyway.
    """
    conn = FakeConn()  # every elect loses: the loop attempts once per tick
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _elect_attempts(conn) >= 1,
            description="the follower's first election attempt",
        )

        deps.shutdown_start_event.set()
        # The park is observable the moment it happens: the loop drops its
        # liveness registration on entering the stop branch, before it
        # parks. From that instant the attempt count must stay frozen.
        await wait_for_condition(
            lambda: "leader.election" not in deps.liveness.ages(),
            description="the parked loop to drop its liveness registration",
        )
        attempts_at_stop = _elect_attempts(conn)
        assert attempts_at_stop >= 1, "test premise: the loop attempted before the stop"

        for _ in range(5):
            leader.wake_election()  # the broadcast wake, pumped while stopping
        # Several would-be ticks pass; a live loop would have attempted
        # once per _HEARTBEAT.
        await asyncio.sleep(20 * _HEARTBEAT + 0.05)

        assert _elect_attempts(conn) == attempts_at_stop, (
            "a stopping worker attempted the election after the stop signal - "
            "the park is broken and a dying pod can win mid-shutdown"
        )
        assert not leader._wake_event.is_set(), (
            "wake_election armed the wake while stopping: the handler must "
            "no-op, no elect may be waiting on a wake during shutdown"
        )
    finally:
        await _stop_task(task, shutdown)


# ── Pin 2: the mid-elect race ───────────────────────────────────────────


async def test_pin_elect_in_flight_at_stop_wins_then_is_resigned_never_assumed() -> None:
    """SIGTERM mid-elect: the win is handed straight back, nothing assumes.

    The elect statement was on the wire when the stop signal landed and
    won anyway. The lease must never be held across the drain: the won
    row is resigned immediately (fenced on the won term, over the conn
    the elect just used, while the worker-wide shutdown event is still
    clear), the assume path never runs - no monitor/cron conn opens, no
    courtesy-lock probe issues, ``lead()`` never sets the flag, so no
    leader-gated sweep can start - and the row is available to the
    successor from that instant.
    """
    conn = _InFlightElectConn()
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        # The elect is now IN FLIGHT. Land the stop while it hangs.
        await wait_for(conn.elect_started, description="the elect statement to go in flight")
        assert _resign_calls(conn) == [], "test premise: no resign before the win"
        deps.shutdown_start_event.set()
        conn.elect_gate.set()  # the win comes back after the stop

        await wait_for_condition(
            lambda: len(_resign_calls(conn)) == 1,
            description="the immediate fenced resign of the won row",
        )
        _resign_sql, resign_args = _resign_calls(conn)[0]
        assert resign_args == (leader._worker_id, conn.won_elected_at), (
            "the resign must be fenced on the WON term (worker_id, elected_at), "
            f"got {resign_args!r}"
        )
        assert not deps.is_leader.is_set(), (
            "is_leader was set for a lease the pod is handing straight back: "
            "the dying pod held leadership across the stop"
        )
        assert leader._leader_monitor_conn is None and leader._cron_conn is None, (
            "the assume path started (leader conns opened) for a stopping worker"
        )
        assert not any("pg_try_advisory_lock" in sql for sql, _ in conn.fetchval_calls), (
            "the assume path ran (the courtesy-lock probe issued): a stopping "
            "worker began acting on a won election"
        )
        assert not shutdown.is_set(), (
            "the resign must land at shutdown START - the worker-wide event "
            "still clear means the drain is still ahead of the pod, and the "
            "row is already back on the market"
        )
        assert conn.close_calls == 0 and not conn.terminated, (
            "the hand-back closed or terminated the conn it rode on"
        )
    finally:
        await _stop_task(task, shutdown)


# ── Pin 3: the early handover ───────────────────────────────────────────


async def test_pin_leader_resigns_at_stop_start_not_after_the_drain() -> None:
    """A leader that begins shutting down resigns BEFORE shutdown completes.

    The pod is leading (flag, term, fence all live). The stop signal
    lands mid-renew-cadence. The resign must issue while the worker-wide
    shutdown event is STILL CLEAR - the drain runs after it - and the
    pod must already be demoted and parked when it does. Today's shape
    resigned in run()'s teardown, after the whole drain: this pin fails
    under that ordering, the resign never comes while the event is clear.
    """
    conn = FakeConn(holds_row=True)  # renewals succeed: the pod is leading
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)
    loop = asyncio.get_running_loop()
    term = LeaderTerm(
        elected_at=datetime.now(UTC),
        trusted_until=loop.time() + _LEADER_LEASE - 1.0,
    )
    deps.lead(term)
    leader._resign_fence = term
    assert deps.leading(), "test premise: the pod is leading"
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        await wait_for_condition(
            lambda: _renew_attempts(conn) >= 1,
            description="the leader's first lease renewal",
        )

        deps.shutdown_start_event.set()
        await wait_for_condition(
            lambda: len(_resign_calls(conn)) == 1,
            description="the resign at shutdown START",
        )

        assert not shutdown.is_set(), (
            "the resign only ran at teardown: the handover is late by the "
            "whole drain window, the successor waits out the drain"
        )
        assert not deps.is_leader.is_set() and deps.leader_term is None, (
            "the handover must demote: the pod is not leader the instant its resign is in flight"
        )
        assert "leader.election" not in deps.liveness.ages(), (
            "the parked election loop must drop its liveness registration, or "
            "detector 2 trips on a loop that stopped by design mid-drain"
        )
        assert conn.close_calls == 0 and not conn.terminated, (
            "the handover closed a conn: the resign must ride one that "
            "survives the close, and an in-flight sweep must not be cancelled"
        )
        # The parked loop never attempts: zero elect statements the whole time.
        assert _elect_attempts(conn) == 0, (
            "a leading pod that just handed the lease over re-attempted the "
            "election during its own shutdown"
        )
    finally:
        await _stop_task(task, shutdown)


# ── Pin 4: the wake handler no-ops while stopping ───────────────────────


async def test_pin_wake_handler_noops_while_stopping() -> None:
    """``wake_election`` arms the wake only on a worker that is not stopping.

    The resign broadcast (the leadership-channel wake that triggers
    immediate election attempts) lands here on every peer, this pod
    included while it stops: the handler must refuse it outright.
    """
    conn = FakeConn()
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)

    # Control: with no stop in progress the wake arms.
    leader.wake_election()
    assert leader._wake_event.is_set(), "the wake seam must arm for a live follower"
    leader._wake_event.clear()

    deps.shutdown_start_event.set()
    leader.wake_election()
    assert not leader._wake_event.is_set(), (
        "a stopping worker armed the election wake: the broadcast must be "
        "IGNORED while stopping, no elect may follow it"
    )


# ── Pin 5: the in-flight quiesce at the resign ──────────────────────────


async def test_pin_handover_demotes_before_the_resign_write_and_cancels_nothing() -> None:
    """At the resign write, the flag is already down and nothing is cancelled.

    A sweep in flight when the stop lands must quiesce cleanly: the
    handover demotes BEFORE the resign write issues, so no leader-gated
    loop can pass its gate and start a new sweep mid-handover, and the
    handover itself neither closes nor terminates any conn - an
    in-flight sweep transaction finishes or aborts whole on its own
    (Postgres transaction semantics), never half-applied by us.
    """
    conn = FakeConn(holds_row=True)
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)
    loop = asyncio.get_running_loop()
    term = LeaderTerm(
        elected_at=datetime.now(UTC),
        trusted_until=loop.time() + _LEADER_LEASE - 1.0,
    )
    deps.lead(term)
    leader._resign_fence = term

    leading_at_resign: list[bool] = []

    def _on_execute(sql: str, args: tuple[object, ...]) -> None:
        if sql.lstrip().upper().startswith("DELETE FROM") and "maintenance_leader" in sql:
            leading_at_resign.append(deps.leading())

    conn._on_execute = _on_execute

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        deps.shutdown_start_event.set()
        await wait_for_condition(
            lambda: len(_resign_calls(conn)) == 1,
            description="the resign at shutdown START",
        )
        assert leading_at_resign == [False], (
            "the resign write issued while the pod still claimed leadership: "
            "a leader-gated loop could start a new sweep mid-handover"
        )
        assert conn.close_calls == 0 and not conn.terminated, (
            "the handover cancelled conn state: an in-flight sweep must be "
            "left to complete or abort whole, never torn down half-applied"
        )
    finally:
        await _stop_task(task, shutdown)


# ── Pin 7: the failed-resign retry is paced, never a hot spin ───────────


async def test_pin_a_failed_resign_retries_on_the_tick_cadence_not_hot() -> None:
    """A resign that cannot reach the database retries paced, attempt-free.

    The park's retry loop keeps the fence alive until the resign lands.
    With no conn left to ride (the failure path already dropped them),
    the resign fails without touching the network - so nothing else in
    the retry iteration awaits, and an unpaced retry would spin the event
    loop hot for the whole drain. The retry must wait out the tick
    cadence between attempts. The ratio does the work: 0.2s holds ~20
    tick cadences but tens of thousands of hot-loop iterations, a margin
    no plausible CI load crosses.
    """
    conn = FakeConn()
    conn._closed = True  # no live conn anywhere: every resign fails without I/O
    deps = _make_deps(conn)
    leader = _make_leader(deps, conn)
    loop = asyncio.get_running_loop()
    term = LeaderTerm(
        elected_at=datetime.now(UTC),
        trusted_until=loop.time() + _LEADER_LEASE - 1.0,
    )
    deps.lead(term)
    leader._resign_fence = term

    hand_over_calls = 0
    original = leader._hand_over_at_stop

    async def counting() -> None:
        nonlocal hand_over_calls
        hand_over_calls += 1
        await original()

    leader._hand_over_at_stop = counting  # type: ignore[method-assign]  # Why: the counter rides the instance so the production loop body runs unmodified.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._election_loop(shutdown))
    try:
        deps.shutdown_start_event.set()
        await asyncio.sleep(0.2)
        assert hand_over_calls <= 40, (
            f"the failing resign retried {hand_over_calls} times in 0.2s: the "
            "retry loop is unpaced and spinning the event loop hot mid-drain"
        )
        assert _elect_attempts(conn) == 0, (
            "the retry loop must stay attempt-free: a stopping worker never "
            "attempts the election, it only re-tries the hand-back"
        )
    finally:
        await _stop_task(task, shutdown)


# ── Pin 6: the orchestrator fires the stop signal before the drain ─────


async def test_pin_orchestrator_sets_the_stop_signal_before_the_drain_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``orchestrate_shutdown`` arms ``shutdown_start_event`` before DRAINING.

    The election loop's park and the early handover hang off the stop
    signal, so the signal must precede the first phase work: the drain
    write must observe it already set, and a full orchestration must
    leave it set.
    """
    import taskq.worker.shutdown as shutdown_mod
    from taskq.backend._protocol import Backend
    from taskq.worker.shutdown import ShutdownPhase, orchestrate_shutdown

    deps = WorkerDeps(
        settings=_worker_settings(),
        dispatcher_pool=cast(Any, MagicMock()),
        heartbeat_pool=cast(Any, MagicMock()),
        worker_pool=cast(Any, MagicMock()),
        notify_conn=None,
        leader_conn=None,
    )
    backend = AsyncMock(spec=Backend)

    seen_at_drain: list[bool] = []

    async def spy_drain(deps: WorkerDeps, worker_id: UUID) -> int:
        seen_at_drain.append(deps.shutdown_start_event.is_set())
        return 0

    monkeypatch.setattr(shutdown_mod, "drain_local_queue_to_pending", spy_drain)

    shutdown = asyncio.Event()
    result = await orchestrate_shutdown(
        deps, deps.settings, new_uuid(), shutdown, None, backend=backend
    )

    assert result == 0
    assert seen_at_drain == [True], (
        "the drain write ran before the stop signal was armed: the election "
        "loop would keep attempting (and could win) into the drain"
    )
    assert deps.shutdown_start_event.is_set()
    assert deps.shutdown_phase is ShutdownPhase.RELEASING
    assert shutdown.is_set()


# ── Integration pins: the row's server-side state during a live drain ──


@pytest.mark.integration
async def test_pin_row_gone_before_the_drain_write_and_successor_elects_during_it(
    pg_dsn: str,
) -> None:
    """The early handover against the real database.

    The lease row stops being the stopping pod's BEFORE the DRAINING
    phase's write completes, and a successor elects and wins while the
    drain is still held open: the handover is early, and the drain
    window is the successor's to take.
    """
    from taskq._ids import new_base62, new_uuid
    from taskq.testing.fixtures import _create_worker, _open_pg_backend, _open_pg_backend_on_schema
    from taskq.worker import shutdown as shutdown_mod
    from taskq.worker.leader import MaintenanceLeader as Leader

    schema = f"tsn_{new_base62()}".lower()
    stack_a, deps_a, backend_a = await _open_pg_backend(pg_dsn, schema_name=schema)
    stack_b, deps_b, backend_b = await _open_pg_backend_on_schema(pg_dsn, schema_name=schema)
    wid_a, wid_b = new_uuid(), new_uuid()
    async with deps_a.dispatcher_pool.acquire() as c:
        await _create_worker(c, schema, wid_a)
    async with deps_b.dispatcher_pool.acquire() as c:
        await _create_worker(c, schema, wid_b)

    leader_a = Leader(deps_a, wid_a, backend_a, clock=SystemClock())
    leader_b = Leader(deps_b, wid_b, backend_b, clock=SystemClock())
    shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
    task_a = asyncio.create_task(leader_a.run(shutdown_a))
    task_b = asyncio.create_task(leader_b.run(shutdown_b))
    drain_released = asyncio.Event()
    drain_holding = asyncio.Event()
    observed: dict[str, UUID | None] = {}

    async def raw_row() -> Any:
        async with deps_b.dispatcher_pool.acquire() as c:
            return await c.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
            )

    async def slow_drain(deps: WorkerDeps, worker_id: UUID) -> int:
        # Runs INSIDE the DRAINING phase, in place of the drain's own
        # write. The resign must land while the drain is still running:
        # a bounded wait for the row to stop being the stopping pod's.
        # Under the old ordering (resign at run()'s teardown, after the
        # phases) this wait can never succeed - the drain is held open
        # until the test releases it, and the resign waits for it.
        async def row_is_no_ours() -> bool:
            rec = await raw_row()
            return rec is None or UUID(str(rec["worker_id"])) != stop_wid

        await wait_for_condition(
            row_is_no_ours,
            description="the lease row to stop being the stopping pod's WHILE the drain runs",
            timeout=5.0,
        )
        rec = await raw_row()
        observed["holder_at_drain"] = None if rec is None else UUID(str(rec["worker_id"]))
        # Hold the drain open: the successor's election must land inside
        # the drain window.
        drain_holding.set()
        await drain_released.wait()
        return 0

    original_drain = shutdown_mod.drain_local_queue_to_pending
    shutdown_mod.drain_local_queue_to_pending = slow_drain
    try:
        await wait_for_condition(
            lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
            description="one pod to win the initial election",
            timeout=5.0,
        )
        # Roles are read off the race, not fixed: whoever won is the
        # incumbent this test stops.
        if deps_a.is_leader.is_set():
            stop_deps, stop_wid, stop_shutdown = deps_a, wid_a, shutdown_a
        else:
            stop_deps, stop_wid, stop_shutdown = deps_b, wid_b, shutdown_b

        orchestration = asyncio.create_task(
            shutdown_mod.orchestrate_shutdown(
                stop_deps,
                stop_deps.settings,
                stop_wid,
                stop_shutdown,
                None,
                backend=backend_a if stop_wid == wid_a else backend_b,
            )
        )
        # The successor elects while the stopping pod's drain is still
        # held open inside DRAINING.
        await wait_for_condition(
            lambda: drain_holding.is_set(),
            description="the drain to reach its hold (the row check passed)",
            timeout=5.0,
        )
        successor_deps = deps_b if stop_wid == wid_a else deps_a
        await wait_for_condition(
            lambda: successor_deps.is_leader.is_set(),
            description="a successor to elect DURING the stopping pod's drain",
            timeout=5.0,
        )
        assert observed["holder_at_drain"] != stop_wid, (
            "the lease row still named the stopping pod when the drain write "
            "ran: the handover was late, not at shutdown START"
        )
        drain_released.set()
        result = await asyncio.wait_for(orchestration, timeout=10.0)
        assert result == 0
        assert not stop_deps.is_leader.is_set()
    finally:
        shutdown_mod.drain_local_queue_to_pending = original_drain
        drain_released.set()
        shutdown_a.set()
        shutdown_b.set()
        for t in (task_a, task_b):
            with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
                await asyncio.wait_for(t, timeout=5.0)
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.integration
async def test_pin_drain_completes_with_a_different_worker_as_leader(pg_dsn: str) -> None:
    """The drain's machinery is self-contained: no phase reads leadership.

    The stopping pod's full orchestration (real drain write, all four
    phases) completes with exit code 0 while the OTHER pod holds the
    lease: nothing in the shutdown assumes this pod remains leader.
    """
    from taskq._ids import new_base62, new_uuid
    from taskq.testing.fixtures import _create_worker, _open_pg_backend, _open_pg_backend_on_schema
    from taskq.worker import shutdown as shutdown_mod
    from taskq.worker.leader import MaintenanceLeader as Leader

    schema = f"tsn_{new_base62()}".lower()
    stack_a, deps_a, backend_a = await _open_pg_backend(pg_dsn, schema_name=schema)
    stack_b, deps_b, backend_b = await _open_pg_backend_on_schema(pg_dsn, schema_name=schema)
    wid_a, wid_b = new_uuid(), new_uuid()
    async with deps_a.dispatcher_pool.acquire() as c:
        await _create_worker(c, schema, wid_a)
    async with deps_b.dispatcher_pool.acquire() as c:
        await _create_worker(c, schema, wid_b)

    leader_a = Leader(deps_a, wid_a, backend_a, clock=SystemClock())
    leader_b = Leader(deps_b, wid_b, backend_b, clock=SystemClock())
    shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
    task_a = asyncio.create_task(leader_a.run(shutdown_a))
    task_b = asyncio.create_task(leader_b.run(shutdown_b))
    try:
        await wait_for_condition(
            lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
            description="one pod to win the initial election",
            timeout=5.0,
        )
        if deps_a.is_leader.is_set():
            stop_deps, stop_wid, stop_shutdown, stop_backend = deps_a, wid_a, shutdown_a, backend_a
        else:
            stop_deps, stop_wid, stop_shutdown, stop_backend = deps_b, wid_b, shutdown_b, backend_b
        successor_deps, successor_wid = (deps_b, wid_b) if stop_wid == wid_a else (deps_a, wid_a)

        result = await asyncio.wait_for(
            shutdown_mod.orchestrate_shutdown(
                stop_deps,
                stop_deps.settings,
                stop_wid,
                stop_shutdown,
                None,
                backend=stop_backend,
            ),
            timeout=10.0,
        )
        assert result == 0
        # The successor holds the lease by the time the drain finished:
        # every phase ran under someone else's leadership.
        await wait_for_condition(
            lambda: successor_deps.is_leader.is_set(),
            description="the successor to hold the lease while the stopping pod drains",
            timeout=5.0,
        )
        async with deps_b.dispatcher_pool.acquire() as c:
            rec = await c.fetchrow(
                f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
            )
        assert rec is not None and UUID(str(rec["worker_id"])) == successor_wid
        assert not stop_deps.is_leader.is_set()
        assert not stop_deps.leading(), "a finished orchestration must leave the pod demoted"
    finally:
        shutdown_a.set()
        shutdown_b.set()
        for t in (task_a, task_b):
            with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
                await asyncio.wait_for(t, timeout=5.0)
        await stack_b.aclose()
        await stack_a.aclose()
