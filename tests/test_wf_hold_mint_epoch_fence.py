# ruff: noqa: S608  # Why: every schema interpolation is the module fixture's validated test identifier; every value is $-bound.
"""Claim-epoch fencing on fresh hold mints across signal, timer, child-join, and loop waits.

A step body whose lock lease lapses before its first wait must never park a
newer claim's running row or commit an orphaned ``wf_signals`` row. Fresh hold
mints pass the executing claim's ``claim_epoch`` into ``_HOLD_HOLD_NODE_SQL``
and roll back the transaction whenever the park update matches zero rows.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._records import _job_row_from_record
from taskq.backend._sweeps import sweep_expired_locks
from taskq.exceptions import Snooze
from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    cancel_workflow_run,
    loop,
    step,
)
from taskq.workflows._childflow import FLOW_WORKFLOW_NAME_SQL_TEMPLATE
from taskq.workflows._worker_execution import execute_flow_job
from taskq.workflows.api._ctx_wait import HoldMintFencedError, NodeHeldError
from taskq.workflows.api._hitl import (
    CHILD_JOIN_SIGNAL_PREFIX,
    TIMER_SIGNAL_PREFIX,
    sweep_child_joins,
    sweep_due_timers,
)
from taskq.workflows.api._sql_runner import render_sql
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import claim_step_ledger
from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

_TIMED_S = 600.0


class _Leaf(BaseModel):
    n: int


class _Approval(BaseModel):
    verdict: str


def _compile_step(suffix: str, wrap: Any) -> tuple[Any, Any]:
    child_app = WorkflowApp()

    @child_app.workflow(f"hold_mint_child_{suffix}")
    def child() -> Promise[object]:
        async def leaf(ctx: StepContext) -> _Leaf:
            return _Leaf(n=1)

        return build(step(leaf, key="leaf"))

    parent_app = WorkflowApp()

    @parent_app.workflow(f"hold_mint_parent_{suffix}")
    def parent() -> Promise[object]:
        return build(step(wrap, key="wrap"))

    return child_app.get(f"hold_mint_child_{suffix}"), parent_app.get(f"hold_mint_parent_{suffix}")


def _compile_loop(suffix: str, body: Any) -> Any:
    app = WorkflowApp()

    @app.workflow(f"hold_mint_loop_{suffix}")
    def flow() -> Promise[object]:
        return build(loop("wrap", body, max_iterations=2, budget_s=600.0))

    return app.get(f"hold_mint_loop_{suffix}")


async def _register_fleet_worker(
    conn: asyncpg.Connection, pool: asyncpg.Pool, schema: str, pg_dsn: str
) -> JobId:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_id = await register_worker(
        pool,
        WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": schema}),
        workflow_execution=True,
    )
    return JobId(worker_id)


async def _claim_wrap(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: JobId,
    *,
    lock_lease: timedelta,
) -> dict[str, Any]:
    rows = await dispatch_batch(
        conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=lock_lease,
    )
    wraps = [dict(r) for r in rows if r["step_key"] == "wrap"]
    assert len(wraps) == 1, [dict(r) for r in rows]
    return wraps[0]


async def _run_claimed(runner: FlowRunner, row: dict[str, Any]) -> str:
    meta = _meta(row["metadata"])
    flow_id = JobId(meta["flow_id"])
    name = await runner.pool.fetchval(
        render_sql(FLOW_WORKFLOW_NAME_SQL_TEMPLATE, runner.schema), flow_id
    )
    assert str(name) == str(runner.compiled.name)
    return await runner.run_fleet_claimed_step(
        flow_id,
        row,
        attempt=int(row["attempt"]),
        claim_epoch=int(row["claim_epoch"]),
    )


async def _wrap_row(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> asyncpg.Record:
    row = await conn.fetchrow(
        f"SELECT id, status, attempt, claim_epoch, locked_by_worker, lock_expires_at, "
        f'budget_paused, metadata FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'wrap'",
        flow_id,
    )
    assert row is not None
    return row


async def _signal_rows(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f'SELECT id, signal_name, status, hold_epoch FROM "{schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = 'wrap' ORDER BY hold_epoch",
        flow_id,
    )
    return [dict(r) for r in rows]


async def _ledger_rows(
    conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f'SELECT step_key, attempt, claim_epoch, status FROM "{schema}".wf_step_ledger '
        "WHERE flow_id = $1 ORDER BY step_key, attempt",
        flow_id,
    )
    return [dict(r) for r in rows]


def _attempt_rows(rows: list[dict[str, Any]], attempt: int) -> list[dict[str, Any]]:
    return [r for r in rows if int(r["attempt"]) == attempt]


def _meta(raw: object) -> dict[str, Any]:
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    return decoded  # pyright: ignore[reportUnknownVariableType]


async def _lapse_and_make_due(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> None:
    await asyncio.sleep(1.2)
    assert await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema) >= 1
    row = await _wrap_row(conn, schema, flow_id)
    assert row["status"] == "pending"
    await conn.execute(f'UPDATE "{schema}".jobs SET scheduled_at = now() WHERE id = $1', row["id"])


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["signal", "timer", "join"])
async def test_stale_first_wait_does_not_park_newer_claim_or_leave_orphan_hold(
    kind: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A stale attempt reaching its first wait after a newer claim holds the
    row must not park the newer claim or commit an orphaned hold row."""
    suffix = new_uuid().hex[:12]
    child_name = f"hold_mint_child_{suffix}"
    at_gate = {1: asyncio.Event(), 2: asyncio.Event()}
    gates = {1: asyncio.Event(), 2: asyncio.Event()}

    async def wrap(ctx: StepContext) -> int:
        if kind == "join":
            handle = await ctx.spawn_flow(child_name, result=_Leaf)
            at_gate[ctx.attempt].set()
            await gates[ctx.attempt].wait()
            await handle.wait(timeout_s=_TIMED_S)
        elif kind == "signal":
            at_gate[ctx.attempt].set()
            await gates[ctx.attempt].wait()
            await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        else:
            at_gate[ctx.attempt].set()
            await gates[ctx.attempt].wait()
            await ctx.wait_until(_TIMED_S, key="remind")
        return 0

    _child_c, parent_c = _compile_step(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_1 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    worker_2 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner_1 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_1)
    runner_2 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_2)

    tasks: list[asyncio.Task[str]] = []
    try:
        claim_1 = await _claim_wrap(wf_conn, wf_schema, worker_1, lock_lease=timedelta(seconds=1))
        assert int(claim_1["attempt"]) == 1
        t1 = asyncio.create_task(_run_claimed(runner_1, claim_1))
        tasks.append(t1)
        await asyncio.wait_for(at_gate[1].wait(), timeout=10.0)

        await _lapse_and_make_due(wf_conn, wf_schema, flow_id)

        claim_2 = await _claim_wrap(wf_conn, wf_schema, worker_2, lock_lease=timedelta(seconds=30))
        assert int(claim_2["attempt"]) == 2
        assert int(claim_2["claim_epoch"]) > int(claim_1["claim_epoch"])
        t2 = asyncio.create_task(_run_claimed(runner_2, claim_2))
        tasks.append(t2)
        await asyncio.wait_for(at_gate[2].wait(), timeout=10.0)

        # Release the stale claim while claim 2 actively owns the row. The
        # mint's park matches nothing, so the whole mint rolls back and this
        # attempt unwinds as fenced (never "held" — nothing was parked).
        gates[1].set()
        assert await asyncio.wait_for(t1, timeout=10.0) == "fenced"
        ledger = await _ledger_rows(wf_conn, wf_schema, flow_id)
        stale_ledger = _attempt_rows(ledger, 1)
        assert stale_ledger and all(r["status"] == "fenced" for r in stale_ledger), (
            f"the fenced mint left the stale claim's own ledger row standing: {stale_ledger}"
        )
        live_ledger = _attempt_rows(ledger, 2)
        assert live_ledger and all(r["status"] == "running" for r in live_ledger), (
            f"the stale claim touched claim 2's ledger row: {live_ledger}"
        )

        during = await _wrap_row(wf_conn, wf_schema, flow_id)
        during_meta = _meta(during["metadata"])
        during_signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert during["status"] == "running", (
            f"stale {kind} mint (claim_epoch={claim_1['claim_epoch']}) parked "
            f"live claim_epoch={claim_2['claim_epoch']}: status={during['status']!r}, "
            f"locked_by_worker={during['locked_by_worker']}, "
            f"hold={during_meta.get('hold')!r}, signals={during_signals}"
        )
        assert during["locked_by_worker"] == worker_2
        assert during["lock_expires_at"] is not None
        assert int(during["claim_epoch"]) == int(claim_2["claim_epoch"])
        assert "hold" not in during_meta and "held_signal" not in during_meta
        assert during_signals == [], (
            f"stale {kind} mint committed orphan hold row(s): {during_signals}"
        )

        # Live claim 2 now reaches its own wait and parks cleanly.
        gates[2].set()
        assert await asyncio.wait_for(t2, timeout=10.0) == "held"

        after = await _wrap_row(wf_conn, wf_schema, flow_id)
        after_meta = _meta(after["metadata"])
        after_signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert len(after_signals) == 1
        assert after["status"] == "pending"
        assert after["locked_by_worker"] is None
        assert after_meta.get("hold") == str(after_signals[0]["id"])
    finally:
        for gate in gates.values():
            gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.integration
async def test_stale_first_wait_in_loop_does_not_park_or_pause_newer_claim(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A stale loop attempt reaching its first wait after a newer claim owns
    the loop row must leave claim 2 running, locked, unpaused, and unstamped
    with no orphan hold row."""
    suffix = new_uuid().hex[:12]
    at_gate = {1: asyncio.Event(), 2: asyncio.Event()}
    gates = {1: asyncio.Event(), 2: asyncio.Event()}

    async def body(ctx: StepContext, carry: object) -> object:
        at_gate[ctx.attempt].set()
        await gates[ctx.attempt].wait()
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return Done({"ok": True})

    loop_c = _compile_loop(suffix, body)
    flow_id = (await FlowRunner(loop_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_1 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    worker_2 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner_1 = FlowRunner(loop_c, wf_pool, wf_schema, worker_id=worker_1)
    runner_2 = FlowRunner(loop_c, wf_pool, wf_schema, worker_id=worker_2)

    tasks: list[asyncio.Task[str]] = []
    try:
        claim_1 = await _claim_wrap(wf_conn, wf_schema, worker_1, lock_lease=timedelta(seconds=1))
        t1 = asyncio.create_task(_run_claimed(runner_1, claim_1))
        tasks.append(t1)
        await asyncio.wait_for(at_gate[1].wait(), timeout=10.0)

        await _lapse_and_make_due(wf_conn, wf_schema, flow_id)

        claim_2 = await _claim_wrap(wf_conn, wf_schema, worker_2, lock_lease=timedelta(seconds=30))
        assert int(claim_2["claim_epoch"]) > int(claim_1["claim_epoch"])
        t2 = asyncio.create_task(_run_claimed(runner_2, claim_2))
        tasks.append(t2)
        await asyncio.wait_for(at_gate[2].wait(), timeout=10.0)

        gates[1].set()
        assert await asyncio.wait_for(t1, timeout=10.0) == "fenced"
        ledger = await _ledger_rows(wf_conn, wf_schema, flow_id)
        stale_ledger = _attempt_rows(ledger, 1)
        assert stale_ledger and all(r["status"] == "fenced" for r in stale_ledger), (
            f"the fenced loop mint left the stale claim's ledger rows standing: {stale_ledger}"
        )
        # both of the stale driver's rows are fenced: the attempt's (marked
        # 'awaited' by the wait site) and the iteration's (still 'running'
        # from its own claim).
        assert any(r["step_key"] == "wrap.iter0" for r in stale_ledger), stale_ledger
        live_ledger = _attempt_rows(ledger, 2)
        assert live_ledger and all(r["status"] == "running" for r in live_ledger), (
            f"the stale loop attempt touched claim 2's ledger row: {live_ledger}"
        )

        during = await _wrap_row(wf_conn, wf_schema, flow_id)
        during_meta = _meta(during["metadata"])
        during_signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert during["status"] == "running", (
            f"stale loop mint parked live claim: status={during['status']!r}, "
            f"budget_paused={during['budget_paused']!r}, "
            f"hold={during_meta.get('hold')!r}, signals={during_signals}"
        )
        assert during["locked_by_worker"] == worker_2
        assert during["lock_expires_at"] is not None
        assert during["budget_paused"] is False
        assert "hold" not in during_meta and "held_signal" not in during_meta
        assert during_signals == []

        gates[2].set()
        await asyncio.wait_for(t2, timeout=10.0)

        after = await _wrap_row(wf_conn, wf_schema, flow_id)
        after_meta = _meta(after["metadata"])
        after_signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert len(after_signals) == 1
        assert after["status"] == "pending"
        assert after["locked_by_worker"] is None
        assert after["budget_paused"] is True
        assert after_meta.get("hold") == str(after_signals[0]["id"])
    finally:
        for gate in gates.values():
            gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["signal", "timer", "join"])
async def test_stale_first_wait_after_repend_rolls_back_orphan_hold(
    kind: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """When a stale attempt reaches its first wait after lease expiry has
    re-pended the row (before any successor claims it), the park update
    touches zero rows and the transaction must roll back without leaving an
    orphaned ``wf_signals`` row."""
    suffix = new_uuid().hex[:12]
    child_name = f"hold_mint_child_{suffix}"
    at_gate = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> int:
        if kind == "join":
            handle = await ctx.spawn_flow(child_name, result=_Leaf)
            at_gate.set()
            await release.wait()
            await handle.wait(timeout_s=_TIMED_S)
        elif kind == "signal":
            at_gate.set()
            await release.wait()
            await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        else:
            at_gate.set()
            await release.wait()
            await ctx.wait_until(_TIMED_S, key="remind")
        return 0

    _child_c, parent_c = _compile_step(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_1 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner_1 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_1)

    t1: asyncio.Task[str] | None = None
    try:
        claim_1 = await _claim_wrap(wf_conn, wf_schema, worker_1, lock_lease=timedelta(seconds=1))
        t1 = asyncio.create_task(_run_claimed(runner_1, claim_1))
        await asyncio.wait_for(at_gate.wait(), timeout=10.0)

        await _lapse_and_make_due(wf_conn, wf_schema, flow_id)

        release.set()
        assert await asyncio.wait_for(t1, timeout=10.0) == "fenced"

        after = await _wrap_row(wf_conn, wf_schema, flow_id)
        after_meta = _meta(after["metadata"])
        signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert after["status"] == "pending"
        assert "hold" not in after_meta and "held_signal" not in after_meta
        assert signals == [], (
            f"fenced {kind} mint on re-pended row committed orphan hold row(s): {signals}"
        )
    finally:
        release.set()
        if t1 is not None:
            await asyncio.gather(t1, return_exceptions=True)


@pytest.mark.integration
async def test_fenced_mint_returns_fenced_through_the_fleet_execution_door(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The production fleet door preserves the fenced outcome through its
    row decode, registry resolution, and start-to-close task boundary; it
    neither translates the control flow into an unresolvable body nor leaves
    an orphan hold behind."""
    at_gate = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> int:
        at_gate.set()
        await release.wait()
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return 0

    suffix = new_uuid().hex[:12]
    _child_c, parent_c = _compile_step(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)

    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=1),
    )
    claims = [r for r in dispatched if r["step_key"] == "wrap"]
    assert len(claims) == 1, [dict(r) for r in dispatched]
    task = asyncio.create_task(
        execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=worker_id,
            job=_job_row_from_record(claims[0]),
            default_start_to_close=timedelta(seconds=30),
        )
    )
    try:
        await asyncio.wait_for(at_gate.wait(), timeout=10.0)
        await _lapse_and_make_due(wf_conn, wf_schema, flow_id)

        release.set()
        execution = await asyncio.wait_for(task, timeout=10.0)

        assert execution.outcome == "fenced", execution
        assert execution.step_key == "wrap"
        assert execution.flow_id == str(flow_id)
        assert await _signal_rows(wf_conn, wf_schema, flow_id) == []
        stale_ledger = _attempt_rows(await _ledger_rows(wf_conn, wf_schema, flow_id), 1)
        assert stale_ledger and all(r["status"] == "fenced" for r in stale_ledger)
        after = await _wrap_row(wf_conn, wf_schema, flow_id)
        assert after["status"] == "pending"
        assert "hold" not in _meta(after["metadata"])
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.integration
async def test_a_non_positive_claim_epoch_is_refused_before_any_write(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The mint's epoch is required and must be positive: at ``0`` the park's
    fence matches no executing row (both claim doors grant at least 1), so a
    mint would report a stale claim as held. The refusal is typed and lands
    before any row is written or parked."""
    from taskq.workflows.api._hitl import register_hold, register_timer_hold
    from taskq.workflows.api._runner_errors import WorkflowRunError

    async def never_runs(ctx: StepContext) -> int:  # pragma: no cover - no body runs
        return 0

    suffix = new_uuid().hex[:12]
    _child_c, parent_c = _compile_step(suffix, never_runs)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    node_id = JobId((await _wrap_row(wf_conn, wf_schema, flow_id))["id"])
    payload_schema: dict[str, object] = {"_Approval": _Approval.model_json_schema()}

    for epoch in (0, -1):
        with pytest.raises(WorkflowRunError, match="claim_epoch"):
            await register_hold(
                wf_conn,
                schema=wf_schema,
                workflow_id=flow_id,
                node_id=node_id,
                node_key="wrap",
                signal_name="_Approval",
                hold_epoch=1,
                call_id="call:1:0",
                payload_schema=payload_schema,
                timeout_s=60.0,
                claim_epoch=epoch,
            )
        with pytest.raises(WorkflowRunError, match="claim_epoch"):
            await register_timer_hold(
                wf_conn,
                schema=wf_schema,
                workflow_id=flow_id,
                node_id=node_id,
                node_key="wrap",
                signal_name="__timer__remind",
                hold_epoch=1,
                absolute=None,
                relative=60.0,
                payload_schema=payload_schema,
                claim_epoch=epoch,
            )

    signals = await _signal_rows(wf_conn, wf_schema, flow_id)
    assert signals == [], f"a refused epoch minted a hold row: {signals}"
    after = await _wrap_row(wf_conn, wf_schema, flow_id)
    assert (after["status"], _meta(after["metadata"]).get("hold")) == ("pending", None), (
        f"a refused epoch parked the row: status={after['status']!r}, "
        f"hold={_meta(after['metadata']).get('hold')!r}"
    )


# ── the mirror ordering: the successor mints FIRST, the stale attempt's
#    re-entry park arrives afterwards ──────────────────────────────────────


def _waiter(
    kind: str,
    child_name: str,
    at_gate: dict[int, asyncio.Event],
    gates: dict[int, asyncio.Event],
    raised: dict[int, str | None],
) -> Any:
    """The body: stall before the FIRST wait so the successor can pass it."""

    async def wrap(ctx: StepContext) -> int:
        handle = await ctx.spawn_flow(child_name, result=_Leaf) if kind == "join" else None
        at_gate[ctx.attempt].set()
        await gates[ctx.attempt].wait()
        try:
            if kind == "signal":
                await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
            elif kind == "timer":
                await ctx.wait_until(_TIMED_S, key="remind")
            else:
                assert handle is not None
                await handle.wait(timeout_s=_TIMED_S)
        except (HoldMintFencedError, NodeHeldError) as exc:
            raised[ctx.attempt] = type(exc).__name__
            raise
        return 0

    return wrap


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["signal", "timer", "join"])
async def test_a_stale_wait_arriving_after_the_successors_mint_is_fenced(
    kind: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A stale attempt whose own park matched nothing must unwind as fenced —
    never as ``held`` — even when it reaches its wait after the successor
    minted, and it must leave the successor's hold and ledger row untouched.

    The signal door's standing-hold read filters on the site's own ``call_id``,
    so a stale attempt cannot see a successor's hold and falls through to the
    fresh mint (already fenced). The timer and child-join reads carry no
    ``call_id`` filter, so the stale attempt finds the successor's held row and
    takes ``reassert_hold``'s re-entry park — whose miss now unwinds with the
    same fence label.
    """
    suffix = new_uuid().hex[:12]
    child_name = f"hold_mint_child_{suffix}"
    at_gate = {1: asyncio.Event(), 2: asyncio.Event()}
    gates = {1: asyncio.Event(), 2: asyncio.Event()}
    raised: dict[int, str | None] = {}

    _child_c, parent_c = _compile_step(suffix, _waiter(kind, child_name, at_gate, gates, raised))
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_1 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    worker_2 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner_1 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_1)
    runner_2 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_2)

    tasks: list[asyncio.Task[str]] = []
    try:
        claim_1 = await _claim_wrap(wf_conn, wf_schema, worker_1, lock_lease=timedelta(seconds=1))
        t1 = asyncio.create_task(_run_claimed(runner_1, claim_1))
        tasks.append(t1)
        await asyncio.wait_for(at_gate[1].wait(), timeout=10.0)

        await _lapse_and_make_due(wf_conn, wf_schema, flow_id)

        claim_2 = await _claim_wrap(wf_conn, wf_schema, worker_2, lock_lease=timedelta(seconds=30))
        assert int(claim_2["claim_epoch"]) > int(claim_1["claim_epoch"])
        t2 = asyncio.create_task(_run_claimed(runner_2, claim_2))
        tasks.append(t2)
        await asyncio.wait_for(at_gate[2].wait(), timeout=10.0)

        # The live claim reaches its wait and mints + parks FIRST.
        gates[2].set()
        assert await asyncio.wait_for(t2, timeout=10.0) == "held", raised
        standing = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert len(standing) == 1 and standing[0]["status"] == "held", standing
        parked = await _wrap_row(wf_conn, wf_schema, flow_id)
        assert _meta(parked["metadata"]).get("hold") == str(standing[0]["id"]), dict(parked)

        # Now the stale attempt reaches its own wait, at an epoch the row no
        # longer carries. Every write it attempts matches nothing.
        gates[1].set()
        outcome_1 = await asyncio.wait_for(t1, timeout=10.0)

        # (1) the successor's hop is untouched — the stale attempt wrote nothing
        after_row = await _wrap_row(wf_conn, wf_schema, flow_id)
        after_signals = await _signal_rows(wf_conn, wf_schema, flow_id)
        assert after_row["status"] == "pending", dict(after_row)
        assert int(after_row["claim_epoch"]) == int(claim_2["claim_epoch"]), dict(after_row)
        assert _meta(after_row["metadata"]).get("hold") == str(standing[0]["id"]), dict(after_row)
        assert after_signals == standing, after_signals
        ledger = await _ledger_rows(wf_conn, wf_schema, flow_id)
        stale_ledger = _attempt_rows(ledger, 1)
        live_ledger = _attempt_rows(ledger, 2)
        assert live_ledger and all(r["status"] == "awaited" for r in live_ledger), live_ledger

        # (2) the stale attempt owns no park, so it must not claim one: its own
        # ledger row is fenced (never left 'awaited' for a hold it does not
        # stand on) ...
        assert stale_ledger and all(r["status"] == "fenced" for r in stale_ledger), (
            f"the stale {kind} attempt's park matched nothing, but its ledger row was left "
            f"{[r['status'] for r in stale_ledger]} (raised {raised.get(1)!r}, "
            f"outcome {outcome_1!r})"
        )
        # ... and the attempt unwinds as fenced, the label the fresh-mint park
        # miss uses for exactly this condition.
        assert outcome_1 == "fenced", (
            f"the stale {kind} attempt's park matched nothing (raised {raised.get(1)!r}) yet the "
            f"tail reported {outcome_1!r} — a resume and a consumed outcome recorded for a row "
            f"this execution does not own (ledger: "
            f"{[(r['step_key'], r['status']) for r in stale_ledger]}, "
            f"successor ledger: {[(r['step_key'], r['status']) for r in live_ledger]})"
        )
    finally:
        for gate in gates.values():
            gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _stage_standing_hold(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, call_id: str
) -> JobId:
    """The pre-fence state: a ``'held'`` signal row for the wait site stands
    while the node row is NOT stamped with it (the residue a pre-fix mint
    could leave behind)."""
    hold_id = JobId(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, payload_schema, "
        "status, expires_at) "
        "VALUES ($1, $2, 'wrap', '_Approval', 1, $3, $4::jsonb, 'held', "
        "clock_timestamp() + interval '600 seconds')",
        hold_id,
        flow_id,
        call_id,
        json.dumps({"_Approval": _Approval.model_json_schema()}),
    )
    return hold_id


@pytest.mark.integration
async def test_a_stale_claims_reentry_park_miss_unwinds_fenced_like_the_mint(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The re-entry park's miss must unwind the stale claim exactly as the
    fresh mint's miss does: ``"fenced"`` and a fenced ledger row."""
    suffix = new_uuid().hex[:12]
    at_gate = {1: asyncio.Event(), 2: asyncio.Event()}
    gates = {1: asyncio.Event(), 2: asyncio.Event()}

    async def wrap(ctx: StepContext) -> int:
        at_gate[ctx.attempt].set()
        await gates[ctx.attempt].wait()
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return 0

    _child_c, parent_c = _compile_step(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_1 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    worker_2 = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner_1 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_1)
    runner_2 = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_2)

    tasks: list[asyncio.Task[str]] = []
    try:
        claim_1 = await _claim_wrap(wf_conn, wf_schema, worker_1, lock_lease=timedelta(seconds=1))
        assert int(claim_1["attempt"]) == 1
        t1 = asyncio.create_task(_run_claimed(runner_1, claim_1))
        tasks.append(t1)
        await asyncio.wait_for(at_gate[1].wait(), timeout=10.0)

        # the standing hold the stale claim will re-enter onto (attempt 1's
        # first wait call: ``call:{attempt}:{seq}`` = "call:1:0").
        await _stage_standing_hold(wf_conn, wf_schema, flow_id, "call:1:0")

        # lapse claim 1, let claim 2 take the row.
        await asyncio.sleep(1.2)
        assert await sweep_expired_locks(wf_conn, timedelta(0), timedelta(0), schema=wf_schema) >= 1
        repended = await _wrap_row(wf_conn, wf_schema, flow_id)
        assert repended["status"] == "pending"
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() WHERE id = $1', repended["id"]
        )
        claim_2 = await _claim_wrap(wf_conn, wf_schema, worker_2, lock_lease=timedelta(seconds=30))
        assert int(claim_2["attempt"]) == 2
        assert int(claim_2["claim_epoch"]) > int(claim_1["claim_epoch"])
        t2 = asyncio.create_task(_run_claimed(runner_2, claim_2))
        tasks.append(t2)
        await asyncio.wait_for(at_gate[2].wait(), timeout=10.0)

        # the stale claim reaches its wait site, finds the standing hold, and
        # its re-park misses (claim 2 owns the row at a newer epoch).
        gates[1].set()
        stale_outcome = await asyncio.wait_for(t1, timeout=10.0)

        # claim 2's row is untouched — the park missed, as it must.
        during = await _wrap_row(wf_conn, wf_schema, flow_id)
        assert during["status"] == "running"
        assert during["locked_by_worker"] == worker_2

        ledger = await _ledger_rows(wf_conn, wf_schema, flow_id)
        stale_ledger = _attempt_rows(ledger, 1)
        assert stale_ledger, ledger

        gates[2].set()
        live_outcome = await asyncio.wait_for(t2, timeout=10.0)
        assert live_outcome == "held", live_outcome

        assert stale_outcome == "fenced", (
            "a stale claim whose re-entry park matched nothing reported "
            f"{stale_outcome!r}: the fresh mint raises HoldMintFencedError on the "
            "identical park miss, so this attempt is recorded as a resume (a "
            "consumed 'scheduled' outcome) for a row claim 2 owns. It must "
            "unwind 'fenced'."
        )
        assert all(r["status"] == "fenced" for r in stale_ledger), (
            "the stale claim's re-entry park missed, yet its own ledger row was "
            f"left {[r['status'] for r in stale_ledger]!r} instead of 'fenced' — "
            "the fence never ran, so the record claims an await this execution "
            "does not own."
        )
    finally:
        for gate in gates.values():
            gate.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.integration
async def test_a_cancel_that_lands_before_the_first_wait_leaves_no_live_hold(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """A run cancelled while its body is between the claim and its first wait
    must not end up holding a live approval row: the fence refuses a
    cancel-marked row, so the mint unwinds instead of parking."""
    suffix = new_uuid().hex[:12]
    at_gate = asyncio.Event()
    release = asyncio.Event()

    async def wrap(ctx: StepContext) -> int:
        at_gate.set()
        await release.wait()
        await ctx.wait_signal(_Approval, timeout_s=_TIMED_S)
        return 0

    _child_c, parent_c = _compile_step(suffix, wrap)
    flow_id = (await FlowRunner(parent_c, wf_pool, wf_schema).create_flow()).flow_id
    worker_id = await _register_fleet_worker(wf_conn, wf_pool, wf_schema, pg_dsn)
    runner = FlowRunner(parent_c, wf_pool, wf_schema, worker_id=worker_id)

    task: asyncio.Task[str] | None = None
    try:
        claim = await _claim_wrap(wf_conn, wf_schema, worker_id, lock_lease=timedelta(seconds=30))
        task = asyncio.create_task(_run_claimed(runner, claim))
        await asyncio.wait_for(at_gate.wait(), timeout=10.0)

        assert await cancel_workflow_run(wf_pool, schema=wf_schema, flow_id=flow_id) >= 1
        root = await wf_conn.fetchval(
            f"SELECT status FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__' AND id = $1",
            flow_id,
        )
        assert root == "cancelled", root

        release.set()
        outcome = await asyncio.wait_for(task, timeout=10.0)
        # the fence, not a hold: the mint wrote nothing and the attempt's
        # record must say so.
        assert outcome == "fenced", outcome

        node = await _wrap_row(wf_conn, wf_schema, flow_id)
        meta = _meta(node["metadata"])
        held = await wf_conn.fetch(
            f'SELECT id, signal_name, status FROM "{wf_schema}".wf_signals '
            "WHERE workflow_id = $1 AND status = 'held'",
            flow_id,
        )
        assert held == [], (
            f"the run is cancelled but its node minted {[dict(r) for r in held]!r}; "
            f"the attempt unwound {outcome!r} with the node at "
            f"status={node['status']!r} hold={meta.get('hold')!r} "
            f"cancel_phase={meta.get('cancel_phase')!r} — a live approval row on a "
            "cancelled run is a hold no operator can usefully answer and nothing "
            "but the hold's own deadline ever clears"
        )
        assert node["status"] != "pending" or "hold" not in meta, (
            f"the cancelled run's node rests in a hold representation: "
            f"status={node['status']!r}, hold={meta.get('hold')!r}, "
            f"cancel_phase={meta.get('cancel_phase')!r}"
        )
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


# ── the loop deferral's outcome echo ─────────────────────────────────────

_MODULE_NAME = "wf_hold_mint_epoch_fence_flows"


@pytest.fixture
def echo_app_module() -> Iterator[types.ModuleType]:
    """The registered definition (the compile's own door — the body resolves
    from the D1 registry, never a per-call map)."""
    module = types.ModuleType(_MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("loop_defer_cancel_flow")
    def loop_defer_cancel() -> Promise[object]:
        async def _body(ctx: Any, carry: object) -> object:
            # the iteration's control-flow signal: the loop's deferral route
            # runs, and the cancel mark on the row outranks it.
            raise Snooze(timedelta(seconds=1.0))

        return build(loop("counter", _body, initial=0, max_iterations=5))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[_MODULE_NAME] = module
    yield module
    del sys.modules[_MODULE_NAME]


@pytest.mark.integration
async def test_a_cancelled_loop_deferral_echoes_cancelled_through_the_claim_tail(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: Any,
    clean_pg_conn: asyncpg.Connection,
    echo_app_module: types.ModuleType,
) -> None:
    """A cancelled loop deferral's ROW terminalises 'cancelled'; the claim
    tail's echo must agree instead of reporting the loop's success face."""
    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(clean_pg_conn, schema, status="running")
    node_id = await seed_running_node(clean_pg_conn, schema, flow_id, step_key="counter")
    # the admin job-page cancel's own column mark on the loop node's row.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET cancel_requested_at = clock_timestamp(), '
        "cancel_phase = 1, metadata = metadata || "
        '\'{"kind": "loop", "iteration": 0, "carry": 0}\'::jsonb WHERE id = $1',
        node_id,
    )
    worker_id, attempt, epoch = await claim_view(clean_pg_conn, schema, node_id)
    wsql = render_workflow_sql(schema)
    claim = await claim_step_ledger(
        clean_pg_conn,
        wsql,
        flow_id=flow_id,
        job_id=node_id,
        step_key="counter",
        map_index=None,
        attempt=int(attempt),
        claim_epoch=0,
    )

    compiled = sys.modules[_MODULE_NAME].app.get("loop_defer_cancel_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema, worker_id=worker_id)
    node_row = await module_pg_pool.fetchrow(
        f'SELECT id, step_key, map_index, payload, trace_id FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert node_row is not None

    outcome = await runner._execute_claimed(  # pyright: ignore[reportPrivateUsage]  # Why: the pin drives the runner's own claim tail — the echo's own seam.
        flow_id,
        dict(node_row),
        int(attempt),
        claim.ledger_id,
        runner.compiled.nodes.get("counter"),
        None,
        claim_epoch=int(epoch),
        ledger_claimed_at=claim.claimed_at,
    )

    row = await module_pg_pool.fetchrow(
        f'SELECT status, cancel_requested_at FROM "{schema}".jobs WHERE id = $1', node_id
    )
    assert row is not None and row["status"] == "cancelled", dict(row) if row is not None else None

    # the record's echo must agree with the row it echoes.
    assert outcome == "cancelled", outcome


# ── the wake arms resolve the node by the EXACT hold id ──────────────────


async def _seed_held_sibling(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    map_index: int,
    hold_id: JobId,
    signal_name: str,
) -> JobId:
    """A held map sibling: pending, stamped with its own hold id (the shape
    the mint leaves), at the shared step key every sibling carries."""
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, claim_epoch, step_key, map_index, metadata, "
        "fairness_key) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'pending', 1, 1, "
        "'wrap', $2, $3::jsonb, $4)",
        node_id,
        map_index,
        json.dumps({"flow_id": str(flow_id), "hold": str(hold_id), "held_signal": signal_name}),
        str(flow_id),
    )
    return JobId(node_id)


async def _seed_timer_signal(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    hold_id: JobId,
    map_index: int,
    seconds_from_now: float,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, map_index, call_id, "
        "payload_schema, status, expires_at) "
        "VALUES ($1, $2, 'wrap', $3, 1, $4, $5, $6::jsonb, 'held', "
        "clock_timestamp() + $7::double precision * interval '1 second')",
        hold_id,
        flow_id,
        f"{TIMER_SIGNAL_PREFIX}remind",
        map_index,
        f"timer:{hold_id}",
        json.dumps({"TimerWake": {}}),
        seconds_from_now,
    )


async def _seed_child_join_signal(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    hold_id: JobId,
    map_index: int,
    child_id: JobId,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, map_index, call_id, "
        "payload_schema, status) "
        "VALUES ($1, $2, 'wrap', $3, 1, $4, $5, $6::jsonb, 'held')",
        hold_id,
        flow_id,
        f"{CHILD_JOIN_SIGNAL_PREFIX}{child_id}",
        map_index,
        f"call:{hold_id}",
        json.dumps({"_Leaf": {}}),
    )


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["timer", "join"])
async def test_waking_one_mapped_sibling_resumes_only_that_sibling(
    kind: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Both wake arms resolve the held node by the exact hold id: two map
    siblings share one ``step_key``, so a bare ``metadata ? 'hold'`` lookup
    could resume sibling A when sibling B's hold fired. Only B is due here, and
    only B's stamp may clear."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    hold_a = JobId(new_uuid())
    hold_b = JobId(new_uuid())
    signal_a = f"{TIMER_SIGNAL_PREFIX}remind" if kind == "timer" else f"{CHILD_JOIN_SIGNAL_PREFIX}a"
    signal_b = f"{TIMER_SIGNAL_PREFIX}remind" if kind == "timer" else f"{CHILD_JOIN_SIGNAL_PREFIX}b"
    node_a = await _seed_held_sibling(
        wf_conn,
        wf_schema,
        flow_id,
        map_index=0,
        hold_id=hold_a,
        signal_name=signal_a,
    )
    node_b = await _seed_held_sibling(
        wf_conn,
        wf_schema,
        flow_id,
        map_index=1,
        hold_id=hold_b,
        signal_name=signal_b,
    )

    if kind == "timer":
        # B is due; A is far from due. One sweep pass fires only B.
        await _seed_timer_signal(
            wf_conn, wf_schema, flow_id, hold_id=hold_a, map_index=0, seconds_from_now=3600
        )
        await _seed_timer_signal(
            wf_conn, wf_schema, flow_id, hold_id=hold_b, map_index=1, seconds_from_now=-60
        )
        assert await sweep_due_timers(wf_pool, schema=wf_schema) >= 1
    else:
        # B's child is terminal; A's is still running. One sweep wakes only B.
        # The terminal root is 'cancelled' (the G7 fixture check exempts a
        # cancelled root from the rows' derivation; a bare 'succeeded' root
        # with no child rows would be a cache lie).
        child_a = await seed_flow(wf_conn, wf_schema, status="running")
        child_b = await seed_flow(wf_conn, wf_schema, status="cancelled")
        await _seed_child_join_signal(
            wf_conn, wf_schema, flow_id, hold_id=hold_a, map_index=0, child_id=child_a
        )
        await _seed_child_join_signal(
            wf_conn, wf_schema, flow_id, hold_id=hold_b, map_index=1, child_id=child_b
        )
        assert await sweep_child_joins(wf_pool, render_workflow_sql(wf_schema)) >= 1

    after_b = await wf_conn.fetchrow(
        f'SELECT status, metadata FROM "{wf_schema}".jobs WHERE id = $1', node_b
    )
    after_a = await wf_conn.fetchrow(
        f'SELECT status, metadata FROM "{wf_schema}".jobs WHERE id = $1', node_a
    )
    assert after_b is not None and after_a is not None
    # the woken sibling's own stamp clears ...
    assert "hold" not in _meta(after_b["metadata"]), dict(after_b)
    assert "held_signal" not in _meta(after_b["metadata"]), dict(after_b)
    # ... and the sibling that did NOT fire is untouched.
    assert _meta(after_a["metadata"]).get("hold") == str(hold_a), dict(after_a)
    assert _meta(after_a["metadata"]).get("held_signal") == signal_a, dict(after_a)

    fired_b = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold_b
    )
    still_a = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_signals WHERE id = $1', hold_a
    )
    assert (fired_b, still_a) == ("delivered", "held"), (fired_b, still_a)
