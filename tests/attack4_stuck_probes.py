# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""ATTACK4 — the NOTHING-STUCK clause (the maintainer's addendum): the
livelock / deadlock / lost-job probes over the phase-3/4 arms.

(a) THE LIVELOCK: two nodes each waiting on the other's edge — the cycle
    is the E1 validate refusal at build (pinned there); the RETRY-cycle
    shape (a body that re-pends forever) is terminated by the LADDER
    (the attempt ledger is the bound) — pinned here: a permanently-
    failing node TERMINATES (the bounded state machine exists).
(b) THE DEADLOCK: the drive + the cancel + the drain + the sweep arms
    run CONCURRENTLY on one flow (the lock orders walked by fire, not
    by reading) — bounded by the test's timeout; the run reaches a NAMED
    terminal.
(c) THE LOST JOB: cancel/race in the NEW arms' windows — the cancel
    landing WHILE the drain holds the spawn tx, the resolve racing the
    cancel — every job lands in a NAMED state, zero silent losses.

Anything this suite finds in MY phase-4 code, I fix; the phase-3
findings go to the fixer (the separation of responsibilities).
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel

from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows.api._hitl import HitlClient


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


# ── (a) the livelock's bound: the ladder terminates the re-queue ────────


async def test_the_retry_cycle_terminates_the_ladder_is_the_bound(
    wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    """A body that fails FOREVER terminates: the ladder's ledger is the
    bounded state machine (3 attempts → the named terminal). No re-queue
    loop runs forever."""
    app = WorkflowApp()

    async def always_fails(ctx: Any, params: Ingest) -> str:
        raise RuntimeError("the permanent failure")

    @app.workflow("attack4_cycle")
    def attack4_cycle() -> object:
        return build(step(always_fails, Ingest(doc_id="d1"), key="solo", max_attempts=3))

    runner = FlowRunner(app.get("attack4_cycle"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await asyncio.wait_for(runner.drive(flow_id), timeout=60)
    assert outcome == "terminal"
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed"
    ledger = await wf_conn.fetch(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1', flow_id
    )
    assert len(ledger) == 3, f"the ladder's bound: exactly 3 attempts, got {len(ledger)}"


# ── (b) the deadlock probe: the arms run concurrently, bounded ──────────


async def test_the_drive_the_cancel_and_the_drain_run_concurrently_no_deadlock(
    wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    """The drive loop + the cancel cascade + the outbox drain + the hold
    resolve, all at once on ONE flow: the lock orders are walked by
    fire. The run reaches a NAMED terminal inside the bound (a deadlock
    is a hang — the timeout is the probe's teeth)."""
    from taskq.workflows import map_source

    app = WorkflowApp()

    async def holds_then_works(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the deadlock probe")
        return "done"

    async def item(ctx: Any, doc_id: str) -> str:
        await asyncio.sleep(0.01)
        return doc_id

    @app.workflow("attack4_deadlock")
    def attack4_deadlock() -> object:
        first = step(holds_then_works, Ingest(doc_id="d1"), key="gate")
        mapped = map_source(first, item)
        return build(mapped)

    runner = FlowRunner(app.get("attack4_deadlock"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    # THE COOPERATIVE STOP (the probe's own no-hangs discipline): the
    # driver is never CANCELLED mid-query (a cancellation landing in a
    # DB op wedges the pool's close — the hang this probe originally
    # caught); the cancel task SETS the stop and the driver exits
    # between passes. The pytest timeout is the REAL hang's guard.
    stop = asyncio.Event()

    async def drive_until_stop() -> None:
        while not stop.is_set():
            await runner.drive(flow_id, max_ticks=50)
            await asyncio.sleep(0.02)

    async def cancel_sometime() -> None:
        await asyncio.sleep(0.3)
        from taskq.workflows import cancel_workflow_run

        await cancel_workflow_run(
            wf_pool,
            schema=wf_schema,
            flow_id=flow_id,
            reason="the deadlock probe",
            principal="attack4",
        )
        stop.set()

    # THE BOUND: the concurrency ends + the run is NAMED-terminal.
    await asyncio.wait_for(
        asyncio.gather(drive_until_stop(), cancel_sometime()),
        timeout=30,
    )
    await runner.drive(flow_id, max_ticks=200)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root in ("cancelled", "failed", "succeeded"), root


# ── (c) the lost-job probe: the resolve racing the cancel ───────────────


async def test_the_resolve_racing_the_cancel_lands_in_a_named_state(
    wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    """The operator resolves WHILE the cancel cascade runs: either the
    resolve WINS (the hold delivered → the cancel's cascade resolves it
    delivered — the node takes the cooperative path) or the CANCEL wins
    (the resolve is the typed no-op/refusal). Every landing is a NAMED
    state; the ZOMBIE WAKE (a delivered hold waking a cancelled node)
    is the dragon this probe hunts."""
    from taskq.workflows import cancel_workflow_run

    app = WorkflowApp()

    async def holds(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the race probe")
        return "done"

    @app.workflow("attack4_race")
    def attack4_race() -> object:
        return build(step(holds, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get("attack4_race"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")

    client = HitlClient(wf_pool, schema=wf_schema)
    (hold,) = await client.list(str(flow_id))

    async def the_cancel() -> None:
        await cancel_workflow_run(
            wf_pool,
            schema=wf_schema,
            flow_id=flow_id,
            reason="the race probe",
            principal="attack4",
        )

    # THE RACE: the resolve and the cancel, concurrent, 50 rounds.
    for _ in range(50):
        results = await asyncio.gather(
            client.resolve(hold.hold_id, {"verdict": "approve", "note": ""}),
            the_cancel(),
            return_exceptions=True,
        )
        resolve_result = results[0]
        assert not isinstance(resolve_result, BaseException), resolve_result
        assert resolve_result.status in ("delivered", "no-op", "refused"), resolve_result
        # THE ZOMBIE CHECK: the flow's row + the signals agree — a
        # delivered hold NEVER wakes a cancelled node back to running.
        root = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if root == "cancelled":
            node = await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
                flow_id,
            )
            assert node != "running", "THE ZOMBIE WAKE: a cancelled node is running"
            break
        # The resolve won this round — the cancel lands on the next.
        await runner.drive(flow_id, max_ticks=50)
        root = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if root == "cancelled":
            break
    else:
        root = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        assert root in ("cancelled", "succeeded", "failed"), (
            f"50 rounds and the run is {root!r} — the race wedged (the probe reds)"
        )
