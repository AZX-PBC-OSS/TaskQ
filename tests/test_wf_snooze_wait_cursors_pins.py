# ruff: noqa: S608  # Why: schema interpolation uses the fixture's validated test identifier; every value is $-bound.
"""Non-consuming deferrals after resolved waits must replay, never re-hold.

A ``Snooze`` or ``RetryAfter(consume_budget=False)`` refunds ``jobs.attempt``
so the re-claim runs under the same attempt ordinal with a strictly higher
``claim_epoch``. Any wait cursor or expiry-face marker stored in
``jobs.metadata`` keyed only by ``attempt`` survives the refund into the next
claim and causes the re-executed wait site to treat an already-delivered
signal, an already-fired timer, or an already-abandoned child join as spent,
minting a second hold epoch instead of replaying the resolved outcome.
"""

from __future__ import annotations

import enum
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._sweeps import sweep_scheduled_to_pending
from taskq.exceptions import RetryAfter, Snooze
from taskq.obs._structlog import setup_logging
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
from taskq.workflows._childflow import (
    FLOW_WORKFLOW_NAME_SQL_TEMPLATE,
    ChildHandle,
    JoinSite,
)
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.api._ctx import build_step_context
from taskq.workflows.api._ctx_timer import TimerWake
from taskq.workflows.api._ctx_wait import (
    Expired,
    read_claim_marker,
    require_claim_epoch,
    write_claim_marker,
)
from taskq.workflows.api._hitl import (
    CHILD_JOIN_SIGNAL_PREFIX,
    TIMER_SIGNAL_PREFIX,
    HitlClient,
    sweep_due_timers,
    sweep_expired_signals,
)
from taskq.workflows.api._runner_errors import ClaimEpochMissingError, WorkflowRunError
from taskq.workflows.api._runner_ladder import is_deterministic_authoring_failure
from taskq.workflows.api._sql_runner import render_sql
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration


class _Approval(BaseModel):
    verdict: str


class _ChildResult(BaseModel):
    value: int


def _deferral(kind: str) -> Snooze | RetryAfter:
    if kind == "snooze":
        return Snooze(timedelta(0))
    return RetryAfter(timedelta(0), consume_budget=False)


async def _make_due(conn: asyncpg.Connection, schema: str, flow_id: JobId, step_key: str) -> None:
    """Fast-forward a deferred node's ``scheduled_at`` past the deferral floor."""
    await conn.execute(
        f'UPDATE "{schema}".jobs '
        "SET scheduled_at = now() - interval '1 second' "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        flow_id,
        step_key,
    )


async def _signals_for_node(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, node_key: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        f'SELECT hold_epoch, signal_name, status FROM "{schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = $2 ORDER BY hold_epoch",
        flow_id,
        node_key,
    )
    return [dict(r) for r in rows]


async def _flow_status(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> str:
    status = await conn.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert status is not None
    return str(status)


async def _make_hold_due(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, node_key: str
) -> None:
    """Push the node's standing hold past its deadline (the DB clock owns it)."""
    await conn.execute(
        f'UPDATE "{schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = $2 AND status = 'held'",
        flow_id,
        node_key,
    )


@pytest.mark.parametrize("deferral_kind", ["snooze", "retry_after_free"])
async def test_deferral_after_answered_wait_signal_replays_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    deferral_kind: str,
) -> None:
    """After an answered ``ctx.wait_signal``, a refunding deferral must replay
    the delivered answer on the re-claim instead of minting a second hold."""
    suffix = new_uuid().hex[:12]
    deferred_once = False
    seen_verdicts: list[str] = []

    async def gate(ctx: StepContext) -> str:
        nonlocal deferred_once
        outcome = await ctx.wait_signal(_Approval)
        assert isinstance(outcome, _Approval), f"expected Approval, got {outcome!r}"
        seen_verdicts.append(outcome.verdict)
        if not deferred_once:
            deferred_once = True
            raise _deferral(deferral_kind)
        return outcome.verdict

    app = WorkflowApp()

    @app.workflow(f"wait_snooze_{suffix}")
    def wf() -> Promise[object]:
        return build(step(gate, key="gate"))

    runner = FlowRunner(app.get(f"wait_snooze_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: registers hold_epoch=1 and parks the node.
    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    hitl = HitlClient(wf_pool, schema=wf_schema)
    holds = await hitl.list(flow_id)
    assert len(holds) == 1
    await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")

    # Claim 2: consumes delivered answer, then raises Snooze / free RetryAfter.
    assert await runner.tick(flow_id) is True
    assert deferred_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "gate")

    # Claim 3 (same attempt, higher claim_epoch): must replay answer and succeed.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "gate")
    assert len(signals) == 1, (
        f"re-claim after {deferral_kind} minted a second signal hold instead of "
        f"replaying the delivered answer: signals={signals}"
    )
    assert signals[0]["status"] == "delivered"
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "approve"
    assert seen_verdicts == ["approve", "approve"]


@pytest.mark.parametrize("deferral_kind", ["snooze", "retry_after_free"])
async def test_deferral_after_fired_wait_until_replays_without_resleeping(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    deferral_kind: str,
) -> None:
    """After a fired ``ctx.wait_until`` timer, a refunding deferral must replay
    the fired wake on the re-claim instead of sleeping another full period."""
    suffix = new_uuid().hex[:12]
    deferred_once = False
    seen_keys: list[str] = []

    async def sleeper(ctx: StepContext) -> str:
        nonlocal deferred_once
        wake = await ctx.wait_until(60.0, key="remind")
        seen_keys.append(wake.key)
        if not deferred_once:
            deferred_once = True
            raise _deferral(deferral_kind)
        return wake.key

    app = WorkflowApp()

    @app.workflow(f"timer_snooze_{suffix}")
    def wf() -> Promise[object]:
        return build(step(sleeper, key="sleeper"))

    runner = FlowRunner(app.get(f"timer_snooze_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: registers timer hold_epoch=1.
    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = 'sleeper'",
        flow_id,
    )
    assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1

    # Claim 2: consumes fired TimerWake, then raises Snooze / free RetryAfter.
    assert await runner.tick(flow_id) is True
    assert deferred_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "sleeper")

    # Claim 3 (same attempt, higher claim_epoch): must replay TimerWake and succeed.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "sleeper")
    assert len(signals) == 1, (
        f"re-claim after {deferral_kind} minted a second timer hold instead of "
        f"replaying the fired wake: signals={signals}"
    )
    assert signals[0]["status"] == "delivered"
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "remind"
    assert seen_keys == ["remind", "remind"]


@pytest.mark.parametrize("deferral_kind", ["snooze", "retry_after_free"])
async def test_deferral_after_expired_child_join_replays_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    deferral_kind: str,
) -> None:
    """After an expired ``ChildHandle.wait()`` join, a refunding deferral must
    replay ``Expired`` on the re-claim instead of minting a second join hold."""
    suffix = new_uuid().hex[:12]
    child_name = f"join_snooze_child_{suffix}"
    deferred_once = False
    seen_expired: list[str] = []

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> Promise[object]:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_Approval)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def parent_step(ctx: StepContext) -> str:
        nonlocal deferred_once
        handle = await ctx.spawn_flow(child_name, result=_ChildResult)
        outcome = await handle.wait(timeout_s=60.0)
        assert isinstance(outcome, Expired), f"expected Expired, got {outcome!r}"
        seen_expired.append(outcome.hold_id)
        if not deferred_once:
            deferred_once = True
            raise _deferral(deferral_kind)
        return "expired-handled"

    parent_app = WorkflowApp()

    @parent_app.workflow(f"join_snooze_parent_{suffix}")
    def parent_wf() -> Promise[object]:
        return build(step(parent_step, key="parent"))

    runner = FlowRunner(parent_app.get(f"join_snooze_parent_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: spawns child and registers join hold_epoch=1.
    assert await runner.tick(flow_id) is True
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = 'parent'",
        flow_id,
    )
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 2: adopts same child, receives Expired, raises Snooze / free RetryAfter.
    assert await runner.tick(flow_id) is True
    assert deferred_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "parent")

    # Claim 3 (same attempt, higher claim_epoch): must replay Expired and succeed.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "parent")
    assert len(signals) == 1, (
        f"re-claim after {deferral_kind} minted a second join hold instead of "
        f"replaying Expired: signals={signals}"
    )
    assert signals[0]["status"] == "abandoned"
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "expired-handled"
    assert len(seen_expired) == 2 and seen_expired[0] == seen_expired[1]


@pytest.mark.parametrize("deferral_kind", ["snooze", "retry_after_free"])
async def test_loop_snooze_after_expired_child_join_replays_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    deferral_kind: str,
) -> None:
    """In a loop body, a ``Snooze`` / free ``RetryAfter`` after an expired
    ``ChildHandle.wait()`` must also replay ``Expired`` on the re-claim
    without minting a second hold."""
    suffix = new_uuid().hex[:12]
    child_name = f"loop_join_snooze_child_{suffix}"
    deferred_once = False
    seen_expired: list[str] = []

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> Promise[object]:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_Approval)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def body(ctx: StepContext, carry: int | None) -> Done[str]:
        nonlocal deferred_once
        handle = await ctx.spawn_flow(child_name, result=_ChildResult)
        outcome = await handle.wait(timeout_s=60.0)
        assert isinstance(outcome, Expired), f"expected Expired, got {outcome!r}"
        seen_expired.append(outcome.hold_id)
        if not deferred_once:
            deferred_once = True
            raise _deferral(deferral_kind)
        return Done("loop-expired-handled")

    parent_app = WorkflowApp()

    @parent_app.workflow(f"loop_join_snooze_parent_{suffix}")
    def parent_wf() -> Promise[object]:
        return build(loop("iter", body, max_iterations=3))

    runner = FlowRunner(parent_app.get(f"loop_join_snooze_parent_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: spawns child in iter0 and registers join hold_epoch=1.
    assert await runner.tick(flow_id) is True
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = 'iter'",
        flow_id,
    )
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 2: adopts iter0 child, receives Expired, raises Snooze.
    assert await runner.tick(flow_id) is True
    assert deferred_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "iter")

    # Claim 3 (same attempt, higher claim_epoch): must replay Expired and finish.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "iter")
    assert len(signals) == 1, (
        f"loop re-claim after Snooze minted a second join hold instead of "
        f"replaying Expired: signals={signals}"
    )
    assert signals[0]["status"] == "abandoned"
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "loop-expired-handled"
    assert len(seen_expired) == 2 and seen_expired[0] == seen_expired[1]


async def test_fleet_dispatch_snooze_after_answered_wait_signal_replays_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """Fleet dispatch (``dispatch_batch`` + ``run_fleet_claimed_step``) hits the
    same attempt-only cursor defect after a ``Snooze`` and must replay the
    delivered answer without minting a second hold."""
    suffix = new_uuid().hex[:12]
    wf_name = f"fleet_wait_snooze_{suffix}"
    deferred_once = False
    seen_verdicts: list[str] = []

    async def gate(ctx: StepContext) -> str:
        nonlocal deferred_once
        outcome = await ctx.wait_signal(_Approval)
        assert isinstance(outcome, _Approval), f"expected Approval, got {outcome!r}"
        seen_verdicts.append(outcome.verdict)
        if not deferred_once:
            deferred_once = True
            raise Snooze(timedelta(0))
        return outcome.verdict

    app = WorkflowApp()

    @app.workflow(wf_name)
    def wf() -> Promise[object]:
        return build(step(gate, key="gate"))

    compiled = app.get(wf_name)

    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_id = JobId(
        await register_worker(
            wf_pool,
            WorkerSettings.load_from_dict({"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": wf_schema}),
            workflow_execution=True,
        )
    )
    runner = FlowRunner(compiled, wf_pool, wf_schema, worker_id=worker_id)
    flow_id = (await runner.create_flow()).flow_id

    async def claim_and_run_one() -> str:
        claimed = await dispatch_batch(
            wf_conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
            queues=["default"],
            limit_n=1,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
        assert len(claimed) == 1, f"expected 1 claimed row, got {len(claimed)}"
        row = dict(claimed[0])
        meta = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
        row_flow_id = JobId(meta["flow_id"])
        stamped_name = await wf_pool.fetchval(
            render_sql(FLOW_WORKFLOW_NAME_SQL_TEMPLATE, wf_schema), row_flow_id
        )
        assert stamped_name == wf_name
        return await runner.run_fleet_claimed_step(
            row_flow_id,
            row,
            attempt=int(row["attempt"]),
            claim_epoch=int(row["claim_epoch"]),
        )

    # Claim 1: fleet claims and parks on hold_epoch=1.
    assert await claim_and_run_one() == "held"
    hitl = HitlClient(wf_pool, schema=wf_schema)
    holds = await hitl.list(flow_id)
    assert len(holds) == 1
    await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")

    # Claim 2: fleet claims, consumes delivered answer, raises Snooze -> scheduled.
    assert await claim_and_run_one() == "deferred"
    assert deferred_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "gate")
    promoted = await sweep_scheduled_to_pending(wf_conn, schema=wf_schema, batch_size=10)
    assert promoted == 1

    # Claim 3 (same refunded attempt, higher claim_epoch): must replay and succeed.
    outcome = await claim_and_run_one()
    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "gate")
    assert len(signals) == 1, (
        f"fleet re-claim after Snooze minted a second signal hold instead of "
        f"replaying the delivered answer: outcome={outcome!r} signals={signals}"
    )
    assert outcome == "succeeded"
    await sweep_join_rederive(wf_pool, render_workflow_sql(wf_schema))
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "approve"
    assert seen_verdicts == ["approve", "approve"]


# ── the controls: what the claim-scoped marker must NOT change ─────────────
#
# The marker resets per claim and still counts WITHIN one, so the pins below
# hold the two halves of that contract that a re-claim repair can break: the
# intra-claim ordinal (a second wait in one execution must take the next
# answer, or hold), the deliberate re-wait after ``Expired``, the retry path
# that the attempt key already covered (a consuming retry runs at a NEW
# attempt ordinal and must still replay), the loop kind whose cursor is the
# iteration counter (never the marker), a row stamped by the previous release
# (bare integer under the old key), and the loud refusal when the claim epoch
# a marker scopes on was never supplied.


@pytest.mark.parametrize("timer_key", ["remind", None], ids=["named", "anonymous"])
async def test_three_waits_on_one_timer_key_in_one_claim_keep_counting(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    timer_key: str | None,
) -> None:
    """Three calls on one timer key in ONE execution share the cursor,
    whether the key is named or anonymous: each call consumes the next wake
    and a call past the queue's end holds for its own wake, never replaying
    an earlier one. And the next claim re-executes from the top, so its
    first call starts at 0 again — both halves of the per-claim contract.
    The third call is what makes the fence's ``<=`` observable: at ``<`` the
    second same-epoch write would be dropped and this call would re-consume
    the second wake."""
    suffix = new_uuid().hex[:12]
    seen_scheduled: list[datetime] = []

    async def sleeper(ctx: StepContext) -> str:
        for _ in range(3):
            wake = await ctx.wait_until(60.0, key=timer_key)
            seen_scheduled.append(wake.scheduled)
        return "three-wakes"

    app = WorkflowApp()

    @app.workflow(f"timer_same_key_{suffix}")
    def wf() -> Promise[object]:
        return build(step(sleeper, key="sleeper"))

    runner = FlowRunner(app.get(f"timer_same_key_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: the first wait registers hold_epoch=1.
    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    await _make_hold_due(wf_conn, wf_schema, flow_id, "sleeper")
    assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1

    # Claim 2: the first call consumes the wake, the second must HOLD.
    assert await runner.tick(flow_id) is True
    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "sleeper")
    assert [(s["hold_epoch"], s["status"]) for s in signals] == [
        (1, "delivered"),
        (2, "held"),
    ], f"the second same-key wait consumed the first's wake instead of holding: {signals}"
    assert len(seen_scheduled) == 1, seen_scheduled

    # Claim 3: calls 1 and 2 replay from the claim's own start (0) and
    # consume wake 1 then wake 2; call 3 must HOLD for its own wake. This is
    # the ``<=`` shape: the second same-epoch write advanced the cursor to 2.
    await _make_hold_due(wf_conn, wf_schema, flow_id, "sleeper")
    assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1
    assert await runner.tick(flow_id) is True
    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "sleeper")
    assert [(s["hold_epoch"], s["status"]) for s in signals] == [
        (1, "delivered"),
        (2, "delivered"),
        (3, "held"),
    ], f"the third same-key wait replayed a consumed wake instead of holding: {signals}"
    assert len(seen_scheduled) == 3, seen_scheduled
    first_wake, second_wake = seen_scheduled[0], seen_scheduled[2]
    assert first_wake == seen_scheduled[1]
    assert second_wake != first_wake

    # Claim 4: the three calls consume three distinct wakes in order.
    await _make_hold_due(wf_conn, wf_schema, flow_id, "sleeper")
    assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1
    assert await runner.tick(flow_id) is True

    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "three-wakes"
    assert len(seen_scheduled) == 6, seen_scheduled
    third_wake = seen_scheduled[5]
    assert seen_scheduled == [
        first_wake,
        first_wake,
        second_wake,
        first_wake,
        second_wake,
        third_wake,
    ], seen_scheduled
    assert len({first_wake, second_wake, third_wake}) == 3


async def test_an_older_claim_cannot_overwrite_a_live_replay_marker(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """A stale body can share the live body's attempt ordinal, but its older
    claim epoch must not reset the live cursor. This pins the durable fence,
    not merely the replay outcome: deleting the epoch predicate would let the
    stale write replace the live marker and the next read would consume the
    wrong queue position."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    key = "hold_replay_signal_1_0"

    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
        marker={"n": 1},
    )
    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=6,
        marker={"n": 0},
    )

    assert await read_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
    ) == {"epoch": 7, "n": 1}
    assert (
        await read_claim_marker(
            wf_conn,
            schema=wf_schema,
            job_id=node_id,
            key=key,
            claim_epoch=6,
        )
        == {}
    )


async def test_a_deliberate_second_join_wait_in_one_claim_reholds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The join face's marker surfaces ``Expired`` once per abandoned row per
    EXECUTION: a body that matches the expiry member and deliberately waits
    again in the same claim must mint a NEW join hold (never fall through the
    face twice, and never replay it forever)."""
    suffix = new_uuid().hex[:12]
    child_name = f"join_twice_child_{suffix}"

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> Promise[object]:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_Approval)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def parent_step(ctx: StepContext) -> str:
        handle = await ctx.spawn_flow(child_name, result=_ChildResult)
        first = await handle.wait(timeout_s=60.0)
        assert isinstance(first, Expired), f"expected Expired, got {first!r}"
        # the deliberate re-wait (a new body decision at the same call site)
        second = await handle.wait(timeout_s=60.0)
        raise AssertionError(f"the deliberate re-wait returned {second!r} instead of holding")

    parent_app = WorkflowApp()

    @parent_app.workflow(f"join_twice_parent_{suffix}")
    def parent_wf() -> Promise[object]:
        return build(step(parent_step, key="parent"))

    runner = FlowRunner(parent_app.get(f"join_twice_parent_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: spawns the child and registers the join hold_epoch=1.
    assert await runner.tick(flow_id) is True
    await _make_hold_due(wf_conn, wf_schema, flow_id, "parent")
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 2: the first wait returns Expired, the deliberate second re-holds.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "parent")
    assert [(s["hold_epoch"], s["status"]) for s in signals] == [
        (1, "abandoned"),
        (2, "held"),
    ], f"the deliberate same-claim re-wait did not mint a new join hold: {signals}"
    # the node rests parked on the NEW hold (not merely "not succeeded")
    node = await wf_conn.fetchrow(
        f"SELECT status, metadata->>'hold' AS hold FROM \"{wf_schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'parent'",
        flow_id,
    )
    assert node is not None and node["status"] == "pending", node
    assert node["hold"] == str(
        await wf_conn.fetchval(
            f'SELECT id::text FROM "{wf_schema}".wf_signals '
            "WHERE workflow_id = $1 AND node_key = 'parent' AND hold_epoch = 2",
            flow_id,
        )
    ), node


async def test_a_consuming_retry_after_the_answer_replays_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The class's control: a CONSUMING ``RetryAfter`` runs the next claim at a
    new attempt ordinal, so it never shared the attempt-only key's defect —
    and it must still replay the delivered answer, not re-ask the operator."""
    suffix = new_uuid().hex[:12]
    retried_once = False
    seen_verdicts: list[str] = []

    async def gate(ctx: StepContext) -> str:
        nonlocal retried_once
        outcome = await ctx.wait_signal(_Approval)
        assert isinstance(outcome, _Approval), f"expected Approval, got {outcome!r}"
        seen_verdicts.append(outcome.verdict)
        if not retried_once:
            retried_once = True
            raise RetryAfter(timedelta(0))
        return outcome.verdict

    app = WorkflowApp()

    @app.workflow(f"consuming_retry_{suffix}")
    def wf() -> Promise[object]:
        return build(step(gate, key="gate"))

    runner = FlowRunner(app.get(f"consuming_retry_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    hitl = HitlClient(wf_pool, schema=wf_schema)
    holds = await hitl.list(flow_id)
    assert len(holds) == 1
    await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")

    # Claim 2: consumes the answer, then a CONSUMING retry (attempt advances).
    assert await runner.tick(flow_id) is True
    assert retried_once is True
    await _make_due(wf_conn, wf_schema, flow_id, "gate")

    # Claim 3: the retry's own attempt replays the answer.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "gate")
    assert len(signals) == 1, (
        f"a consuming retry minted a second signal hold instead of replaying the "
        f"delivered answer: signals={signals}"
    )
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "approve"
    assert seen_verdicts == ["approve", "approve"]


def _loop_wait_body(wait_kind: str, state: dict[str, Any]) -> Any:
    """The loop control's body: wait on the human (``signal``) or the clock
    (``timer``), then defer ONCE with a ``Snooze``."""

    async def body(ctx: StepContext, carry: int | None) -> Done[str]:
        if wait_kind == "signal":
            outcome = await ctx.wait_signal(_Approval)
            assert isinstance(outcome, _Approval), f"expected Approval, got {outcome!r}"
            state["seen"].append(outcome.verdict)
        else:
            wake = await ctx.wait_until(60.0, key="tick")
            state["seen"].append(wake.key)
        if not state["deferred"]:
            state["deferred"] = True
            raise Snooze(timedelta(0))
        return Done(f"loop-{wait_kind}-ok")

    return body


@pytest.mark.parametrize("wait_kind", ["signal", "timer"])
async def test_a_loop_body_snooze_replays_its_wait_without_reholding(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wait_kind: str,
) -> None:
    """The loop kind's cursor is the row's iteration counter, never the
    claim-scoped marker (``Snooze`` does not advance the iteration): the
    re-claim must replay the iteration's own answer for both waits."""
    suffix = new_uuid().hex[:12]
    state: dict[str, Any] = {"deferred": False, "seen": []}

    app = WorkflowApp()

    @app.workflow(f"loop_wait_snooze_{suffix}")
    def wf() -> Promise[object]:
        return build(loop("iter", _loop_wait_body(wait_kind, state), max_iterations=3))

    runner = FlowRunner(app.get(f"loop_wait_snooze_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.tick(flow_id) is True
    if wait_kind == "signal":
        hitl = HitlClient(wf_pool, schema=wf_schema)
        holds = await hitl.list(flow_id)
        assert len(holds) == 1
        await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")
    else:
        await _make_hold_due(wf_conn, wf_schema, flow_id, "iter")
        assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1

    # Claim 2: the re-executed iteration replays its answer, then snoozes.
    assert await runner.tick(flow_id) is True
    assert state["deferred"] is True
    await _make_due(wf_conn, wf_schema, flow_id, "iter")

    # Claim 3: the same iteration replays the same answer again.
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "iter")
    assert len(signals) == 1, (
        f"a loop body's Snooze minted a second hold instead of replaying the "
        f"{wait_kind}: signals={signals}"
    )
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == f"loop-{wait_kind}-ok"
    expected = "approve" if wait_kind == "signal" else "tick"
    assert state["seen"] == [expected, expected]


async def test_a_row_stamped_by_the_previous_release_replays_its_answer(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A row mid-flight across the upgrade carries the previous release's
    bare-integer cursor under ``hold_cursor_{attempt}_{seq}``. The new marker
    lives under its own key, so the old key reads as NO cursor — the new
    claim replays its queue from the start (what a from-the-top execution
    wants) instead of casting a JSON object or inheriting a spent cursor."""
    suffix = new_uuid().hex[:12]

    async def gate(ctx: StepContext) -> str:
        outcome = await ctx.wait_signal(_Approval)
        assert isinstance(outcome, _Approval), f"expected Approval, got {outcome!r}"
        return outcome.verdict

    app = WorkflowApp()

    @app.workflow(f"legacy_row_{suffix}")
    def wf() -> Promise[object]:
        return build(step(gate, key="gate"))

    runner = FlowRunner(app.get(f"legacy_row_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    hitl = HitlClient(wf_pool, schema=wf_schema)
    holds = await hitl.list(flow_id)
    assert len(holds) == 1
    await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")

    # the previous release's stamp on the row the resume is about to claim
    resume_attempt = int(
        await wf_conn.fetchval(
            f'SELECT attempt + 1 FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gate'",
            flow_id,
        )
    )
    legacy_key = f"hold_cursor_{resume_attempt}_0"
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".jobs SET metadata = metadata || $2::jsonb '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gate'",
        flow_id,
        json.dumps({legacy_key: 1}),
    )

    # the re-claim replays the delivered answer despite the legacy cursor
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "gate")
    assert len(signals) == 1, (
        f"a legacy bare-integer cursor changed the replay decision: signals={signals}"
    )
    assert await _flow_status(wf_conn, wf_schema, flow_id) == "succeeded"
    assert await runner.result(flow_id) == "approve"
    # the legacy key is left inert, never migrated nor cast
    row = await wf_conn.fetchrow(
        f'SELECT metadata, claim_epoch FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gate'",
        flow_id,
    )
    assert row is not None
    metadata = row["metadata"]
    stamp = json.loads(metadata) if isinstance(metadata, str) else metadata
    assert stamp[legacy_key] == 1
    # the new marker lives under its own name and carries the executing
    # claim's epoch — the one shape an old worker's ``::int`` cast can never
    # meet under the names it knows. The payload's per-claim count is not
    # pinned: at this site ``seq`` already gives each call its own key, so
    # the epoch is the whole mechanism and the count is shape parity.
    marker_key = f"hold_replay_signal_{resume_attempt}_0"
    assert isinstance(stamp[marker_key], dict), stamp
    assert stamp[marker_key]["epoch"] == int(row["claim_epoch"]), stamp


# ── the claim epoch a marker scopes on, refused loudly when unsupplied ─────


async def _stage_delivered_signal(
    conn: asyncpg.Connection,
    schema: str,
    *,
    flow_id: JobId,
    node_key: str,
    signal_name: str,
    payload: str,
    call_id: str = "call:1:0",
) -> None:
    """Stage one DELIVERED answer row on the node (the row the operator door
    writes: the payload rides the row, the call id names the wait site)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, payload, "
        "status, resolved_at) "
        "VALUES ($1, $2, $3, $4, 1, $5, $6::jsonb, 'delivered', now())",
        new_uuid(),
        flow_id,
        node_key,
        signal_name,
        call_id,
        payload,
    )


def _ctx_for(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    *,
    flow_id: JobId,
    job_id: JobId,
    node_key: str,
    claim_epoch: int,
) -> StepContext:
    """The runner's own construction, with the epoch under test."""
    return build_step_context(
        flow_id=flow_id,
        job_id=job_id,
        node_key=node_key,
        attempt=1,
        claim_epoch=claim_epoch,
        pool=wf_pool,
        wsql=render_workflow_sql(wf_schema),
        input=None,
    )


async def test_a_zero_claim_epoch_is_refused_before_a_wait_cursor_replays(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """At epoch 0 the cursor never resets, so the re-executed wait would treat
    its delivered answer as spent and mint a second hold. The refusal is
    loud, typed, and names the epoch — never a silent re-ask."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    await _stage_delivered_signal(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        node_key="source",
        signal_name="_Approval",
        payload=json.dumps({"verdict": "approve"}),
    )

    ctx = _ctx_for(
        wf_pool, wf_schema, flow_id=flow_id, job_id=node_id, node_key="source", claim_epoch=0
    )
    with pytest.raises(WorkflowRunError) as refused:
        await ctx.wait_signal(_Approval)
    assert "claim_epoch" in str(refused.value), refused.value


async def test_a_zero_claim_epoch_is_refused_before_a_timer_cursor_replays(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The timer's leg of the same refusal: a fired wake replayed under an
    epoch that can never reset would sleep a second period."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    now = datetime.now(UTC)
    await _stage_delivered_signal(
        wf_conn,
        wf_schema,
        flow_id=flow_id,
        node_key="source",
        signal_name=f"{TIMER_SIGNAL_PREFIX}remind",
        payload=TimerWake(key="remind", scheduled=now, fired_at=now).model_dump_json(),
    )

    ctx = _ctx_for(
        wf_pool, wf_schema, flow_id=flow_id, job_id=node_id, node_key="source", claim_epoch=0
    )
    with pytest.raises(WorkflowRunError) as refused:
        await ctx.wait_until(60.0, key="remind")
    assert "claim_epoch" in str(refused.value), refused.value


async def test_a_zero_claim_epoch_is_refused_by_the_join_face(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The join face's leg: an abandoned join whose face marker can never
    reset would report ``Expired`` forever instead of re-holding."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="parent")
    child_id = await seed_flow(wf_conn, wf_schema, status="running")
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_signals '
        "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status, resolved_at) "
        "VALUES ($1, $2, 'parent', $3, 1, 'call:1', 'abandoned', now())",
        new_uuid(),
        flow_id,
        f"{CHILD_JOIN_SIGNAL_PREFIX}{child_id}",
    )

    handle = ChildHandle[_Approval](
        site=JoinSite(
            pool=wf_pool,
            wsql=render_workflow_sql(wf_schema),
            flow_id=flow_id,
            job_id=node_id,
            node_key="parent",
            attempt=1,
            ledger_id=None,
            claim_epoch=0,
        ),
        flow_id=child_id,
        result_model=_Approval,
        terminal_key=None,
    )
    with pytest.raises(WorkflowRunError) as refused:
        await handle.wait(timeout_s=60.0)
    assert "claim_epoch" in str(refused.value), refused.value


# ── the marker's fence: the payload cannot displace the epoch, a fenced write
# is reported, and the ``<=`` predicate keeps intra-claim counting ──────────


async def test_the_stored_epoch_cannot_be_displaced_by_the_callers_marker(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """The epoch is the only fact making a marker belong to one execution, so
    it must not be overridable by the marker payload the caller passes."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    key = "hold_replay_signal_1_0"

    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
        marker={"epoch": 0, "n": 1},
    )

    assert await read_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
    ) == {"epoch": 7, "n": 1}


async def test_a_marker_storing_epoch_zero_lets_every_stale_claim_write(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """A stored epoch of 0 satisfies ``stored <= claimed`` for every claim, so a
    marker that carries one loses the fence entirely: the oldest writer lands
    last and a stale body can reset the live claim's cursor."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    key = "hold_replay_signal_1_0"

    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
        marker={"epoch": 0, "n": 1},
    )
    # a long-superseded claim's write must not land over the live marker
    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=2,
        marker={"n": 0},
    )

    assert await read_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
    ) == {"epoch": 7, "n": 1}


async def test_two_writes_under_one_epoch_both_land(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
) -> None:
    """The fence is ``<=``, not ``<``: one claim writes the same timer key
    once per consumption, so its own second write at the same epoch must
    land. At ``<`` the count would stay 1 and a third consumption would
    replay the second wake."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="sleeper")
    key = "hold_replay_timer_1_remind"

    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=5,
        marker={"n": 1},
    )
    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=5,
        marker={"n": 2},
    )

    assert await read_claim_marker(
        wf_conn, schema=wf_schema, job_id=node_id, key=key, claim_epoch=5
    ) == {"epoch": 5, "n": 2}


async def test_a_fenced_marker_write_leaves_a_diagnosis_trail(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The drop the fence exists to cause must still be findable.

    A write the fence refused (a stale body's older epoch over a live
    claim's marker) is otherwise byte-identical to a write that landed: the
    site returns the replayed answer either way. The neighbouring fenced
    statement (``reassert_hold``) emits ``wf-hold-reassert-fenced`` at DEBUG;
    the marker write emits ``wf-claim-marker-fenced`` with the same fields.
    """
    # ``setup_logging`` is idempotent, so this only matters when nothing has
    # configured logging yet; ``caplog.at_level`` sets the level for the
    # window either way (structlog's ``filter_by_level`` reads the emitting
    # logger's effective stdlib level at call time).
    setup_logging(level="DEBUG", log_format="json")
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="source")
    key = "hold_replay_signal_1_0"

    # The live claim's marker: epoch 7 has consumed queue position 1.
    await write_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
        marker={"n": 1},
    )

    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        # The stale body: epoch 6 shares the attempt ordinal but no longer
        # owns the row — the fence drops the write.
        await write_claim_marker(
            wf_conn,
            schema=wf_schema,
            job_id=node_id,
            key=key,
            claim_epoch=6,
            marker={"n": 0},
        )

    # The fence did its job (so the window really is the drop path, not a
    # write that landed and logged something for another reason).
    assert await read_claim_marker(
        wf_conn,
        schema=wf_schema,
        job_id=node_id,
        key=key,
        claim_epoch=7,
    ) == {"epoch": 7, "n": 1}, "the stale epoch must not overwrite the live claim's marker"

    trail = [
        record
        for record in caplog.records
        if key in record.getMessage() or str(node_id) in record.getMessage()
    ]
    assert trail, (
        "a fenced claim-marker write matched no row and left NO trace on the "
        "log stream: a stale execution replying from another claim's marker "
        f"is indistinguishable from a live one (node {node_id}, key {key!r}, "
        f"stale epoch 6 over live epoch 7). Records seen: "
        f"{[r.getMessage()[:160] for r in caplog.records]}"
    )
    assert any("wf-claim-marker-fenced" in record.getMessage() for record in caplog.records), (
        "the fenced marker write was reported, but not under the house-style "
        "wf-claim-marker-fenced event: "
        f"{[r.getMessage()[:160] for r in caplog.records]}"
    )


# ── the unsupplied claim epoch routes as a deterministic authoring failure ──


def test_the_claim_epoch_refusal_is_a_deterministic_authoring_failure() -> None:
    """A missing claim epoch cannot be fixed by re-running.

    ``require_claim_epoch``'s own docstring calls the state a loud defect and
    the message names the remedy; ``_deterministic_classes``' stated
    membership criterion is "a defect that re-running cannot fix"
    (``VanishedNodeError`` — itself a ``WorkflowRunError`` — is in the tuple
    for exactly that reason). A re-run cannot supply an epoch the claim door
    never granted, so the refusal takes the deterministic route and
    terminal-fails on the first attempt instead of burning the retry budget
    and reporting a transient failure.
    """
    with pytest.raises(WorkflowRunError) as excinfo:
        require_claim_epoch(
            "ctx.wait_signal",
            flow_id=JobId(new_uuid()),
            node_key="source",
            claim_epoch=0,
        )
    assert isinstance(excinfo.value, ClaimEpochMissingError), excinfo.value
    assert is_deterministic_authoring_failure(excinfo.value), (
        "the claim-epoch refusal routes as a TRANSIENT failure: the ladder "
        "re-pends the node with backoff and re-raises the identical refusal "
        "until max_attempts"
    )


# ── the rollback direction: an older worker reads no marker under the new
# names ────────────────────────────────────────────────────────────────────

#: The base release's (``18c995002``) wait/timer cursor read, verbatim from
#: ``api/_ctx_wait.CtxWaitOps.wait_signal`` and
#: ``api/_ctx_timer.CtxTimerOps.wait_until`` (identical text at both sites).
#: ``$2`` is the key the older worker knows; the ``::int`` cast is the leg the
#: key-family change is about.
_PREVIOUS_RELEASE_CURSOR_READ = (
    "SELECT (metadata ->> $2)::int AS cursor, "
    "metadata->>'kind' AS kind, "
    "(metadata->>'iteration')::int AS iteration "
    "FROM {schema}.jobs WHERE id = $1"
)

#: The base release's ``_childflow.ChildHandle.wait`` face read, verbatim.
_PREVIOUS_RELEASE_FACE_READ = "SELECT (metadata ->> $2)::int FROM {schema}.jobs WHERE id = $1"


async def _node_metadata(conn: asyncpg.Connection, schema: str, node_id: JobId) -> dict[str, Any]:
    raw = await conn.fetchval(f'SELECT metadata FROM "{schema}".jobs WHERE id = $1', node_id)
    decoded = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict), decoded
    return decoded


async def _delivered_answer_count(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, node_key: str
) -> int:
    count = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = $2 AND status = 'delivered'",
        flow_id,
        node_key,
    )
    assert count is not None
    return int(count)


async def test_an_older_worker_reads_no_marker_under_the_names_it_knows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The rollback direction, pinned: one plain step crosses all three wait
    sites in one run, so its row ends up carrying all three NEW marker
    families — and every one of the base release's own read statements, run
    against that row, still returns NULL (no cursor) without a SQL error. The
    older worker's derived cursor is therefore 0 while its answer queue is
    non-empty: it replays the queue from its start (what a from-the-top
    re-execution wants) and never treats the new marker's JSON object as an
    integer."""
    suffix = new_uuid().hex[:12]
    child_name = f"rolling_child_{suffix}"

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> Promise[object]:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_Approval)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def crossing(ctx: StepContext) -> str:
        verdict = await ctx.wait_signal(_Approval)
        assert isinstance(verdict, _Approval), f"expected Approval, got {verdict!r}"
        wake = await ctx.wait_until(60.0, key="remind")
        assert isinstance(wake, TimerWake), f"expected TimerWake, got {wake!r}"
        handle = await ctx.spawn_flow(child_name, result=_ChildResult)
        joined = await handle.wait(timeout_s=60.0)
        assert isinstance(joined, Expired), f"expected Expired, got {joined!r}"
        return f"{verdict.verdict}:{wake.key}:expired"

    app = WorkflowApp()

    @app.workflow(f"rolling_parent_{suffix}")
    def wf() -> Promise[object]:
        return build(step(crossing, key="crossing"))

    runner = FlowRunner(app.get(f"rolling_parent_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    hitl = HitlClient(wf_pool, schema=wf_schema)

    # Claim 1: the signal hold (hold_epoch 1) parks the node.
    assert await runner.drive(flow_id, until="held", max_ticks=20) == "held"
    holds = await hitl.list(flow_id)
    assert len(holds) == 1
    await hitl.resolve(holds[0].hold_id, {"verdict": "approve"}, principal="operator")

    # Claim 2: replays the answer, then registers the timer hold and parks.
    assert await runner.tick(flow_id) is True
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = 'crossing' AND status = 'held'",
        flow_id,
    )
    assert await sweep_due_timers(wf_pool, schema=wf_schema) == 1

    # Claim 3: replays the answer and the wake, then spawns the child and parks
    # on the join hold.
    assert await runner.tick(flow_id) is True
    await wf_conn.execute(
        f'UPDATE "{wf_schema}".wf_signals '
        "SET expires_at = now() - interval '1 second' "
        "WHERE workflow_id = $1 AND node_key = 'crossing' AND status = 'held'",
        flow_id,
    )
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 4: replays all three sites to their resolved faces and finishes.
    assert await runner.tick(flow_id) is True
    assert await runner.result(flow_id) == "approve:remind:expired"

    node_id = JobId(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'crossing'",
            flow_id,
        )
    )
    metadata = await _node_metadata(wf_conn, wf_schema, node_id)

    # The three new families are on the row (the writer end), each carrying the
    # claim epoch that wrote it. A marker is keyed by the attempt ordinal of the
    # claim that wrote it, so the row carries one per consuming claim.
    signal_keys = [k for k in metadata if k.startswith("hold_replay_signal_")]
    timer_keys = [k for k in metadata if k.startswith("hold_replay_timer_")]
    join_keys = [k for k in metadata if k.startswith("child_join_replay_")]
    assert signal_keys and timer_keys and join_keys, metadata
    for key in signal_keys + timer_keys + join_keys:
        assert set(metadata[key]) == {"epoch", "n"} or {"epoch", "hold_epoch"} == set(
            metadata[key]
        ), metadata[key]
    for key in signal_keys + timer_keys:
        assert metadata[key]["n"] == 1, metadata[key]
    for key in join_keys:
        abandoned_epoch = await wf_conn.fetchval(
            f'SELECT hold_epoch FROM "{wf_schema}".wf_signals '
            "WHERE workflow_id = $1 AND node_key = 'crossing' AND status = 'abandoned'",
            flow_id,
        )
        assert metadata[key]["hold_epoch"] == abandoned_epoch, metadata[key]
    # ... and no writer in this tree still stamps the previous release's names,
    # which is what makes the older worker's read inert.
    assert not [k for k in metadata if k.startswith("hold_cursor_")], metadata
    assert not [k for k in metadata if k.startswith("child_join_face_")], metadata

    # The older worker's own statements, against this row: no cursor, no error,
    # for every site the new release stamped. (``kind`` is NULL on a plain step,
    # which the older worker reads as "not a loop" — the same answer it got
    # before the upgrade.)
    for marker_key in signal_keys + timer_keys:
        # the older worker derives its key from its own (attempt, seq|signal)
        code = marker_key.replace("hold_replay_signal_", "hold_cursor_").replace(
            "hold_replay_timer_", "hold_cursor_"
        )
        row = await wf_conn.fetchrow(
            _PREVIOUS_RELEASE_CURSOR_READ.format(schema=wf_schema), node_id, code
        )
        assert row is not None
        assert row["cursor"] is None, (
            f"the previous release would read a cursor under {code!r} on a row the new "
            f"release stamped: {row['cursor']!r}"
        )

    for marker_key in join_keys:
        face_key = marker_key.replace("child_join_replay_", "child_join_face_")
        face = await wf_conn.fetchval(
            _PREVIOUS_RELEASE_FACE_READ.format(schema=wf_schema), node_id, face_key
        )
        assert face is None, (
            f"the previous release would read a face under {face_key!r} on a row the new "
            f"release stamped: {face!r}"
        )

    # The older worker's derived cursor is 0 while the site's answer queue is
    # non-empty: it replays the queue from its start, never mints a second hold.
    assert await _delivered_answer_count(wf_conn, wf_schema, flow_id, "crossing") >= 2
    timer_delivered = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals '
        "WHERE workflow_id = $1 AND node_key = 'crossing' "
        "AND signal_name = $2 AND status = 'delivered'",
        flow_id,
        TIMER_SIGNAL_PREFIX + "remind",
    )
    assert timer_delivered == 1


# ── the EmitBackpressure park's attempt window ─────────────────────────────
#
# The park is the one refunding disposition that zeroes ``jobs.attempt``
# instead of subtracting one. A ``chain_source`` row restores its logical
# ordinal from the carrier the park persists, so a wait site there keeps its
# attempt-ordered answer window and replays both delivered answers. The
# plain-step shape is the limitation the module comment above
# ``_CLAIM_MARKER_READ_SQL`` names.


class _FirstAnswer(BaseModel):
    verdict: str


class _SecondAnswer(BaseModel):
    verdict: str


class _Ok(enum.Enum):
    OK = "ok"


async def _leaf_ok(ctx: StepContext, item: object) -> _Ok:
    del ctx, item
    return _Ok.OK


def _chain() -> Any:
    from taskq.retry import RetryPolicy
    from taskq.workflows import DONE, Chain, Route, Step
    from taskq.workflows._policy import ExecutionPolicy, RetrySpec

    return Chain(
        name=f"park-replay-{new_uuid().hex}",
        start="leaf",
        steps={
            "leaf": Step(
                body=_leaf_ok,
                outcomes=_Ok,
                route=Route({_Ok.OK: DONE}),
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(
                            max_attempts=3,
                            base=timedelta(seconds=0.01),
                            cap=timedelta(seconds=0.02),
                            jitter=0,
                        )
                    )
                ),
            )
        },
    )


async def _node_row(
    conn: asyncpg.Connection, schema: str, node_id: JobId
) -> tuple[Any, dict[str, Any]]:
    row = await conn.fetchrow(
        f"""SELECT status::text AS status, attempt::int AS attempt, metadata
            FROM "{schema}".jobs WHERE id = $1""",
        node_id,
    )
    assert row is not None
    meta = (
        json.loads(row["metadata"]) if isinstance(row["metadata"], str) else dict(row["metadata"])
    )
    return row, meta


async def _charge_one_decoy(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> str:
    """One ordinary scheduled row of the same flow, charging 1: with
    ``max_in_flight=2`` a page of 2 cannot fit, so the emit refuses for real."""
    decoy_id = str(new_uuid())
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (id, actor, queue, payload, status,
            max_attempts, retry_kind, step_key, metadata, scheduled_at, created_at,
            fairness_key, idempotency_scope, idempotency_key)
            VALUES ($1, 'probe', 'default', '{{}}', 'scheduled', 3, 'transient', 'decoy',
            $2::jsonb, statement_timestamp() + interval '1 hour', clock_timestamp(),
            $3, 'workflow-run', $4)""",
        decoy_id,
        json.dumps({"flow_id": str(flow_id)}),
        str(flow_id),
        f"wf:{flow_id}:decoy",
    )
    return decoy_id


async def _release_and_promote(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    schema: str,
    flow_id: JobId,
    decoy_id: str,
) -> None:
    """The park's own recovery: the competitor terminalizes, the capacity
    intent wakes, the promotion CAS moves the parked row back to ``pending``."""
    from taskq.workflows._emit import ensure_emit_capacity, wake_emit_backpressure
    from taskq.workflows.engine import render_workflow_sql as _render

    await conn.execute(
        f"""UPDATE "{schema}".jobs SET status = 'succeeded',
            finished_at = clock_timestamp() WHERE id = $1::uuid""",
        decoy_id,
    )
    wsql = _render(schema)
    async with pool.acquire() as acquired:
        await ensure_emit_capacity(acquired, wsql, flow_id=flow_id, bound=2)
    await wake_emit_backpressure(pool, wsql, flow_id=flow_id)


async def _drive_to_terminal(
    runner: Any, conn: asyncpg.Connection, schema: str, flow_id: JobId
) -> str:
    status: object = None
    for _ in range(20):
        await runner.tick(flow_id)
        status = await conn.fetchval(
            f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
            flow_id,
        )
        if status in ("succeeded", "failed", "cancelled"):
            return str(status)
    return str(status)


async def test_park_after_two_answered_waits_on_a_chain_source_replays(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A ``chain_source`` re-reads the logical attempt the park persists, so
    its two answered waits stay inside the answer queue's window and the
    re-claim replays both instead of asking the operator again."""
    from taskq.workflows import FlowRunner as _FlowRunner
    from taskq.workflows.api._graph import chain_source
    from taskq.workflows.chain import chain_start

    suffix = new_uuid().hex[:12]
    chain = _chain()
    parked_once = False
    seen: list[tuple[str, str]] = []

    async def source_body(ctx: StepContext) -> None:
        nonlocal parked_once
        first = await ctx.wait_signal(_FirstAnswer)
        assert isinstance(first, _FirstAnswer), f"expected an answer, got {first!r}"
        second = await ctx.wait_signal(_SecondAnswer)
        assert isinstance(second, _SecondAnswer), f"expected an answer, got {second!r}"
        seen.append((first.verdict, second.verdict))
        if not parked_once:
            parked_once = True
            await ctx.emit_batch(
                [
                    chain_start(
                        chain,
                        {"item": offset},
                        map_index=offset,
                        trace_id=f"park-src-{suffix}-{offset}",
                    )
                    for offset in (0, 1)
                ],
                cursor={"page": 0},
            )

    app = WorkflowApp()

    @app.workflow(f"park_replay_src_{suffix}", max_in_flight=2)
    def wf() -> object:
        return build(chain_source(chain, source_body, key="gate", max_attempts=3))

    runner = _FlowRunner(app.get(f"park_replay_src_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    node_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'gate'""",
        flow_id,
    )
    assert node_id is not None

    assert await runner.tick(flow_id) is True
    hitl = HitlClient(wf_pool, schema=wf_schema)
    holds = await hitl.list(flow_id)
    assert len(holds) == 1, holds
    await hitl.resolve(holds[0].hold_id, {"verdict": "first"}, principal="operator")

    assert await runner.tick(flow_id) is True
    holds = await hitl.list(flow_id)
    assert len(holds) == 1, holds
    await hitl.resolve(holds[0].hold_id, {"verdict": "second"}, principal="operator")

    decoy_id = await _charge_one_decoy(wf_conn, wf_schema, flow_id)

    assert await runner.tick(flow_id) is True
    node, meta = await _node_row(wf_conn, wf_schema, JobId(node_id))
    assert meta.get("blocking_reason") == "backpressure", (
        f"no park: status={node['status']} attempt={node['attempt']} keys={sorted(meta)}"
    )
    assert int(node["attempt"]) == 0
    # a chain_source row is BORN with the carrier at 1 and only a genuine
    # source failure advances it, so its ctx attempt never moves.
    assert meta.get("wf_source_logical_attempt") == 1, meta.get("wf_source_logical_attempt")

    await _release_and_promote(wf_conn, wf_pool, wf_schema, flow_id, decoy_id)
    await _drive_to_terminal(runner, wf_conn, wf_schema, flow_id)

    signals = await wf_conn.fetch(
        f"""SELECT hold_epoch, signal_name, status::text AS status, call_id
            FROM "{wf_schema}".wf_signals WHERE workflow_id = $1 ORDER BY hold_epoch""",
        flow_id,
    )
    observed = [dict(s) for s in signals]
    assert [len(observed), [o["status"] for o in observed]] == [
        2,
        ["delivered", "delivered"],
    ], f"the chain_source re-claim re-held too: signals={observed}"
    assert seen == [("first", "second"), ("first", "second")]


async def test_a_loop_bodys_join_face_expiry_is_not_shared_across_iterations(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The join face's marker key carries no iteration leg
    (``child_join_replay_{attempt}``), unlike the spawn cursor
    (``spawn_slot_{attempt}.iter{i}``): a loop body's iterations share ONE
    slot. This pins why that is sound — a new iteration's child mints a new
    ``hold_epoch`` (the node's monotone mint), so the shared slot never reports
    a previous iteration's ``Expired`` as this iteration's, and both iterations
    surface their own expiry without a second hold."""
    from taskq.workflows import Done as _Done
    from taskq.workflows import FlowRunner as _FlowRunner
    from taskq.workflows import Refine as _Refine

    suffix = new_uuid().hex[:12]
    child_name = f"iter_join_child_{suffix}"
    expired: list[int] = []

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> object:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_FirstAnswer)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def body(ctx: StepContext, carry: int | None) -> Any:
        iteration = 0 if carry is None else carry
        handle = await ctx.spawn_flow(child_name, result=_ChildResult)
        outcome = await handle.wait(timeout_s=60.0)
        assert isinstance(outcome, Expired), f"expected Expired, got {outcome!r}"
        expired.append(iteration)
        if iteration == 0:
            return _Refine(1)
        return _Done("both-iterations-expired")

    app = WorkflowApp()

    @app.workflow(f"iter_join_replay_{suffix}")
    def wf() -> object:
        return build(loop("iter", body, max_iterations=3))

    runner = _FlowRunner(app.get(f"iter_join_replay_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    async def expire_join_hold() -> None:
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".wf_signals '
            "SET expires_at = now() - interval '1 second' "
            "WHERE workflow_id = $1 AND node_key = 'iter' AND status = 'held'",
            flow_id,
        )
        assert await sweep_expired_signals(wf_pool, schema=wf_schema) >= 1

    # iteration 0 registers its join hold, which is abandoned -> Expired
    assert await runner.tick(flow_id) is True
    await expire_join_hold()
    # iteration 0 re-executes: its own Expired, then Refine(1); iteration 1
    # registers ITS OWN join hold (a new hold_epoch on the shared slot).
    assert await runner.tick(flow_id) is True
    await expire_join_hold()
    # iteration 1 re-executes: its own Expired (never iteration 0's), then Done
    assert await runner.tick(flow_id) is True

    flow_status = await _drive_to_terminal(runner, wf_conn, wf_schema, flow_id)
    assert flow_status == "succeeded", flow_status
    assert await runner.result(flow_id) == "both-iterations-expired"
    assert expired == [0, 1], expired
    signals = await wf_conn.fetch(
        f"""SELECT hold_epoch, status::text AS status FROM "{wf_schema}".wf_signals
            WHERE workflow_id = $1 AND node_key = 'iter' ORDER BY hold_epoch""",
        flow_id,
    )
    assert [(int(s["hold_epoch"]), s["status"]) for s in signals] == [
        (1, "abandoned"),
        (2, "abandoned"),
    ], [dict(s) for s in signals]


async def test_two_expired_children_share_one_join_replay_slot(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The documented single-slot limitation, pinned: two children of one node
    expired in one execution share ``child_join_replay_{attempt}``, so the
    slot holds the second child's ``hold_epoch`` and a deliberate re-wait on
    the first replays ``Expired`` instead of minting a new hold. The guide and
    the ``ChildHandle.wait`` comment state this shape."""
    suffix = new_uuid().hex[:12]
    child_name = f"join_two_children_{suffix}"
    outcomes: list[str] = []

    child_app = WorkflowApp()

    @child_app.workflow(child_name)
    def child_wf() -> Promise[object]:
        async def slow_leaf(ctx: StepContext) -> _ChildResult:
            await ctx.wait_signal(_Approval)
            return _ChildResult(value=1)

        return build(step(slow_leaf, key="leaf"))

    child_app.get(child_name)

    async def parent_step(ctx: StepContext) -> str:
        first_child = await ctx.spawn_flow(child_name, result=_ChildResult)
        second_child = await ctx.spawn_flow(child_name, result=_ChildResult)
        first = await first_child.wait(timeout_s=60.0)
        second = await second_child.wait(timeout_s=60.0)
        assert isinstance(first, Expired), f"expected Expired, got {first!r}"
        assert isinstance(second, Expired), f"expected Expired, got {second!r}"
        if not outcomes:
            again = await first_child.wait(timeout_s=60.0)
            outcomes.append("expired" if isinstance(again, Expired) else "held")
            return f"rewait-{outcomes[0]}"
        return "done"

    app = WorkflowApp()

    @app.workflow(f"join_two_parent_{suffix}")
    def wf() -> Promise[object]:
        return build(step(parent_step, key="parent"))

    runner = FlowRunner(app.get(f"join_two_parent_{suffix}"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # Claim 1: both children spawn; the first join mints hold_epoch=1.
    assert await runner.tick(flow_id) is True
    await _make_hold_due(wf_conn, wf_schema, flow_id, "parent")
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 2: the first join replays Expired; the second child's join mints
    # hold_epoch=2.
    assert await runner.tick(flow_id) is True
    await _make_hold_due(wf_conn, wf_schema, flow_id, "parent")
    assert await sweep_expired_signals(wf_pool, schema=wf_schema) == 1

    # Claim 3: both joins replay Expired, then the deliberate re-wait on the
    # FIRST replays Expired from the shared slot (the second child's epoch).
    assert await runner.tick(flow_id) is True

    signals = await _signals_for_node(wf_conn, wf_schema, flow_id, "parent")
    assert [(s["hold_epoch"], s["status"]) for s in signals] == [
        (1, "abandoned"),
        (2, "abandoned"),
    ], f"the two-expired-children slot minted an unexpected third hold: {signals}"
    assert outcomes == ["expired"], outcomes
    assert await runner.result(flow_id) == "rewait-expired"
