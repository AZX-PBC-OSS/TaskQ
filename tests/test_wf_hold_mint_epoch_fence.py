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
from taskq.settings import WorkerSettings
from taskq.worker.run import register_worker
from taskq.workflows import (
    Done,
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    loop,
    step,
)
from taskq.workflows._childflow import FLOW_WORKFLOW_NAME_SQL_TEMPLATE
from taskq.workflows._worker_execution import execute_flow_job
from taskq.workflows.api._sql_runner import render_sql

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
        # both of the stale driver's rows are fenced: the attempt's and the
        # iteration's (wrap.iter0 — the one the wait site marked 'awaited').
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
