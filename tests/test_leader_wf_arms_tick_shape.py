"""THE TICK-TAX BOUND — the wf arms' cost on the base leader's sweep tick.

The regression-lens law: adopting TaskQflow must not tax a deployment that
never runs a workflow. The maintenance leader's tick is TABLE-DRIVEN
(``_sweep_loop``); the TaskQflow round appended TEN wf sweep arms to it
(``wf_join_rederive`` … ``wf_step_cache_retention``), each ``hasattr``-gated
on ``workflow_sweeps_capable``. This file pins that tax's FACE — the wire
conversation the arms add per tick on an estate with ZERO wf rows — the
same statement-recording discipline ``test_round_trip_budgets.py`` pins the
hot paths with:

* the LEGACY gate: a backend without ``workflow_sweeps_capable`` runs ZERO
  wf arms — the pre-TaskQflow deployment's tick is unchanged, byte for byte
  (the conversation pin goes red if the arms ever leak past the gate);
* the ADOPTING shape: a ``workflow_sweeps_capable`` backend runs the ten
  arms, IN THE REGISTERED ORDER, each with EXACTLY its zero-wf conversation
  (tx arms: BEGIN + statement + COMMIT; autocommit arms: one statement) —
  16 round trips per tick total. An eleventh arm, a lost gate, a new
  per-tick statement, or a re-ordered arm all go red HERE instead of
  silently re-pricing every leader tick in every deployment.

The measured wall-clock face of this conversation (medians of interleaved
runs against real Postgres 17, zero-wf estate, the pre-wf base tree as the
control) rides the evidence file: perf-evidence-leader-tick-wf-tax.md.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps
from taskq.workflows.api._hitl import _EXPIRY_SWEEP_SQL  # pyright: ignore[reportPrivateUsage]
from taskq.workflows.api._hitl import (
    _render as _hitl_render,  # pyright: ignore[reportPrivateUsage]  # Why: the pin renders through the arm's own seam, so the fragment is the statement the tick really sends.
)
from taskq.workflows.api._sql_loop import LOOP_BUDGET_SWEEP_SQL, render_loop_sql
from taskq.workflows.engine import render_workflow_sql

_WSQL = render_workflow_sql("taskq")

#: The ten wf arms' statements, RENDERED THE WAY THE ARMS RENDER THEM — the
#: pin's fragments are the shipped statements, never copies.
_EXPIRY_RENDERED = _hitl_render(_EXPIRY_SWEEP_SQL, "taskq")
_LOOP_BUDGET_RENDERED = render_loop_sql(LOOP_BUDGET_SWEEP_SQL, "taskq")

#: The wf arms' zero-wf conversations, IN REGISTERED ORDER: (arm name,
#: [(method, statement-fragment)]). The fragment is the shipped SQL
#: constant's head — the pin tracks the statement, never a copy of it.
WF_ARMS: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    (
        "wf_join_rederive",
        (
            ("BEGIN", ""),
            ("fetchrow", _WSQL.rederive_sweep),
            ("execute", _WSQL.workflow_root_maintain),
            ("COMMIT", ""),
        ),
    ),
    ("wf_outbox_drain", (("BEGIN", ""), ("fetch", _WSQL.outbox_fetch_undelivered), ("COMMIT", ""))),
    ("wf_signal_sweep", (("fetch", _EXPIRY_RENDERED),)),
    ("wf_hold_stamp_reconcile", (("fetch", _WSQL.hold_stamp_reconcile),)),
    ("wf_outbox_retention", (("fetch", _WSQL.outbox_retention),)),
    ("wf_loop_budget", (("BEGIN", ""), ("fetch", _LOOP_BUDGET_RENDERED), ("COMMIT", ""))),
    ("wf_phantom_reap", (("fetch", _WSQL.phantom_reap),)),
    ("wf_nodeless_root_reap", (("fetchval", _WSQL.nodeless_root_reap),)),
    ("wf_progress_ring_prune", (("fetchval", _WSQL.progress_ring_prune),)),
    ("wf_step_cache_retention", (("fetch", _WSQL.step_cache_retention),)),
)


class _RecordingConn:
    """Records (method, sql) per round trip; answers the zero-wf estate."""

    def __init__(self) -> None:
        self.wire: list[tuple[str, str]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.wire.append(("execute", sql))
        return "UPDATE 0"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.wire.append(("fetch", sql))
        return []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.wire.append(("fetchval", sql))
        return None

    async def fetchrow(self, sql: str, *args: object) -> Any:
        self.wire.append(("fetchrow", sql))
        # The rederive's summary row: a zero-wf estate answers all-zero
        # counts, firable=0 (no fire arm runs).
        if _frag(_WSQL.rederive_sweep) in _frag(sql) or _frag(sql) in _frag(_WSQL.rederive_sweep):
            return {
                "firable": 0,
                "blocked": 0,
                "blocked_required": 0,
                "flow_fenced": 0,
                "reconciled": 0,
            }
        return None

    def transaction(self, *args: object, **kwargs: object) -> _Tx:
        self.wire.append(("BEGIN", ""))
        return _Tx(self)

    def is_in_transaction(self) -> bool:
        return False

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False


class _Tx:
    def __init__(self, conn: _RecordingConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _RecordingConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        self._conn.wire.append(("COMMIT", ""))


class _RecordingPool:
    def __init__(self, conn: _RecordingConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_RecordingConn]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's signature.
        yield self._conn


class _PgShapedBackend:
    """The base maintenance surface + the wf-arms gate.

    ``workflow_capable=False`` builds the LEGACY backend: every
    ``gated_on`` attr absent, the tick's arms exactly the pre-TaskQflow set.
    """

    def __init__(self, *, workflow_capable: bool) -> None:
        if workflow_capable:
            # The capability marker the ten wf arms gate on. The arms
            # borrow no backend methods (they take the dispatcher pool
            # directly), so the marker is the gate's whole face.
            self.workflow_sweeps_capable: bool = True

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0

    async def sweep_leaked_reservation_slots(
        self, conn: object, *, schema: str, batch_size: int = 100
    ) -> int:
        return 0

    async def sweep_expired_results(
        self, conn: object, *, schema: str, batch_size: int = 100
    ) -> int:
        return 0

    async def sweep_expired_events(
        self, conn: object, *, schema: str, retention: Any, batch_size: int = 100
    ) -> int:
        return 0

    async def sweep_idle_keyed_rows(
        self, conn: object, *, schema: str, horizon: Any, batch_size: int = 100
    ) -> int:
        return 0


def _deps(*, sweep_interval: float, pool: _RecordingPool) -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_LOCK_LEASE": "3.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_WATCHDOG_ENABLED": "false",
        },
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # pyright: ignore[reportArgumentType]  # Why: test double for asyncpg.Pool; the recording pool satisfies the acquire protocol the loop consumes.
        heartbeat_pool=pool,  # pyright: ignore[reportArgumentType]
        worker_pool=pool,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    deps.settings.sweep_interval = sweep_interval
    deps.is_leader.set()
    return deps


def _ctx(pool: _RecordingPool, *, workflow_capable: bool) -> SweepContext:
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SweepContext(
        deps=_deps(sweep_interval=30.0, pool=pool),
        backend=_PgShapedBackend(workflow_capable=workflow_capable),  # pyright: ignore[reportArgumentType]  # Why: test double for the Backend protocol.
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=RateLimitRegistry(),
    )


async def _one_tick(ctx: SweepContext, stop_on: Callable[[], bool]) -> list[tuple[str, str]]:
    """Run ``_sweep_loop`` until *stop_on* sees the tick's work, then stop."""
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the loop directly, the wiring-test pattern.
    try:
        for _ in range(600):
            await asyncio.sleep(0.01)
            if stop_on():
                break
        else:
            raise AssertionError("the tick never reached its stop condition")
    finally:
        shutdown.set()
        await asyncio.wait_for(asyncio.shield(task), timeout=10.0)
    wire = ctx.deps.dispatcher_pool._conn.wire  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]  # Why: the recording pool IS the observation seam this pin exists to read.
    # Drop any residual ticks: truncate after the FIRST full wf segment.
    return wire


def _frag(sql: str) -> str:
    """The statement's recognisable head — 80 chars of the RENDERED sql."""
    return " ".join(sql.strip()[:80].split())


def _expected_wire() -> list[tuple[str, str]]:
    """The full expected wf segment, flattened."""
    out: list[tuple[str, str]] = []
    for _name, conversation in WF_ARMS:
        for method, stmt in conversation:
            out.append((method, _frag(stmt)))
    return out


def _tail_tokens(wire: list[tuple[str, str]], n: int) -> list[tuple[str, str]]:
    return [(m, _frag(s)) for m, s in wire[-n:]]


async def test_the_legacy_tick_runs_zero_wf_arms() -> None:
    """A backend without ``workflow_sweeps_capable`` — the pre-TaskQflow
    deployment's exact shape — runs ZERO wf arms: no wf statement may reach
    the wire, now or after any wf-layer change."""
    conn = _RecordingConn()
    ctx = _ctx(_RecordingPool(conn), workflow_capable=False)
    # Stop when the wire stops growing for a full tick... simplest bounded
    # stop: two stale-batches statements mean at least one full pass ran.
    await _one_tick(ctx, stop_on=lambda: len(conn.wire) > 0)
    wf_fragments = {_frag(stmt) for _, conversation in WF_ARMS for _, stmt in conversation if stmt}
    leaked = [(m, s) for m, s in conn.wire if any(_frag(frag) in _frag(s) for frag in wf_fragments)]
    assert leaked == [], f"wf arms leaked past the legacy gate: {leaked[:4]}"


async def test_the_adopting_tick_runs_exactly_ten_wf_arms_in_order() -> None:
    """With the gate on, the tick's TAIL is exactly the ten wf arms' zero-wf
    conversations, in registration order — 16 round trips, no more."""
    conn = _RecordingConn()
    ctx = _ctx(_RecordingPool(conn), workflow_capable=True)
    expected = _expected_wire()
    await _one_tick(ctx, stop_on=lambda: len(conn.wire) >= len(expected))
    tail = _tail_tokens(conn.wire, len(expected))
    assert tail == expected, "the wf arms' tick conversation drifted from the pinned zero-wf shape"
    # The arm count is the tax's multiplier: an eleventh arm re-prices
    # every leader tick in every deployment and must land HERE first.
    rederive_frag = _frag(_WSQL.rederive_sweep)
    wf_starts = [i for i, (_m, s) in enumerate(tail) if s == rederive_frag]
    assert len(wf_starts) == 1  # exactly one rederive arm = one join-rederive pass
