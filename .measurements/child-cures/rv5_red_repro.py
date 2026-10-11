"""THE CHILD-FLOW CURES' LIVE RED REPRO (the rv5-child pin pack's red
receipt — run against the PRE-CURE base head f8bfc5033 on :5812).

Each section captures the convicted shape LIVE:

* C1 — the asyncio.gather dragon: K=5 gather spawns → ONE child, every
  handle the same flow_id, the parent's join serves tenant-0's verdict
  5 times.
* C2b — the recursive cancel cascade: a 1200-deep chain + cancel →
  RecursionError at the operator + runs left running.
* C3 — the retention-respawn: purge the held child's rows, resume →
  the child SILENTLY RE-EXECUTES (a fresh child root), the parent
  SUCCEEDS.
* C4 — the orphan window: the receipt-less child (the crash window's
  committed state) ESCAPES the parent's cancel forever.
* C5a — the dev drive's inert expiry arm: a timeout_s wait parks the
  drive to max_ticks (never the Expired member).
* C5c — the resolve bounce on a child-join hold: the resolve DELIVERS,
  the body re-executes, the live child re-holds (the no-op bounce).

Run: TASKQ_TEST_PG_DSN=postgresql://postgres:taskq@localhost:5812/taskq \\
     uv run python .measurements/child-cures/rv5_red_repro.py
"""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_mod

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.backend._protocol import JobId
from taskq.workflows import (
    ChildOk,
    Expired,
    FlowRunner,
    HitlClient,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    cancel_workflow_run,
    step,
)
import os

DSN = os.environ.get("TASKQ_TEST_PG_DSN", "postgresql://postgres:taskq@localhost:5812/taskq")
SCHEMA = "rv5_child_red"

TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


class SubProbe(BaseModel):
    doc_id: str


class SubReport(BaseModel):
    findings: list[str]


class GenIn(BaseModel):
    gen: int = 0


class GenOut(BaseModel):
    gen: int


# ── C1: the dragon's app pair ───────────────────────────────────────────

dragon = WorkflowApp()


@dragon.actor(queue="cpu", max_attempts=1)
async def dragon_child_step(ctx: StepContext) -> SubReport:
    p = SubProbe.model_validate(ctx.input)
    return SubReport(findings=[f"found:{p.doc_id}"])


@dragon.workflow("rv5_red_child")
def rv5_red_child() -> Promise[object]:
    return build(step(dragon_child_step, key="probe"))


@dragon.actor(queue="cpu", max_attempts=3)
async def dragon_parent_step(ctx: StepContext, params: SubProbe) -> dict[str, object]:
    probes = [SubProbe(doc_id=f"{params.doc_id}-{i}") for i in range(5)]
    handles = await asyncio.gather(
        *[ctx.spawn_flow("rv5_red_child", input=p, result=SubReport) for p in probes]
    )
    flow_ids = [str(h.flow_id) for h in handles]
    print("   the K handles' flow_ids:", flow_ids)
    print("   DISTINCT children behind the K handles:", len(set(flow_ids)))
    verdicts = []
    for h in handles:
        outcome = await h.wait()
        match outcome:
            case ChildOk(result=report):
                verdicts.append(report.findings[0])
            case Expired():
                verdicts.append("expired")
            case _:
                verdicts.append("not-ok")
    print("   the join's verdicts:", verdicts)
    return {"verdicts": verdicts}


@dragon.workflow("rv5_red_dragon_parent")
def rv5_red_dragon_parent() -> Promise[object]:
    return build(step(dragon_parent_step, SubProbe(doc_id="d1"), key="spawn_join"))


# ── C2b: the fabricated chain + the recursive cascade ───────────────────


async def chain_rows(conn: asyncpg.Connection, depth: int) -> list[JobId]:
    ids = [JobId(str(uuid_mod.uuid4())) for _ in range(depth)]
    for i, fid in enumerate(ids):
        meta: dict[str, object] = {"flow_id": str(fid), "workflow": "rv5_chain"}
        if i > 0:
            meta["parent_flow_id"] = str(ids[i - 1])
        if i < depth - 1:
            meta["child_flows"] = [str(ids[i + 1])]
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, step_key, status, metadata) "
            "VALUES ($1, 'wf', 'cpu', '{}', 1, 'transient', '__flow__', 'running', $2::jsonb)",
            fid,
            json.dumps(meta),
        )
    return ids


# ── C3/C4: the purge/orphan app pair (the plain join parent) ────────────

retention = WorkflowApp()


@retention.actor(queue="cpu", max_attempts=1)
async def retention_child_step(ctx: StepContext) -> SubReport:
    p = SubProbe.model_validate(ctx.input)
    print("   >>> THE CHILD BODY RAN (side effects fired) for", p.doc_id)
    return SubReport(findings=[f"found:{p.doc_id}"])


@retention.workflow("rv5_red_ret_child")
def rv5_red_ret_child() -> Promise[object]:
    return build(step(retention_child_step, key="probe"))


@retention.actor(queue="cpu", max_attempts=3)
async def retention_parent_step(ctx: StepContext, params: SubProbe) -> dict[str, object]:
    handle = await ctx.spawn_flow(
        "rv5_red_ret_child", input=SubProbe(doc_id=params.doc_id), result=SubReport
    )
    outcome = await handle.wait()
    match outcome:
        case ChildOk(result=report):
            return {"verdict": "ok", "findings": report.findings}
        case Expired():
            return {"verdict": "expired"}
        case _:
            return {"verdict": "not-ok"}


@retention.workflow("rv5_red_ret_parent")
def rv5_red_ret_parent() -> Promise[object]:
    return build(step(retention_parent_step, SubProbe(doc_id="d1"), key="spawn_join"))


# ── C5a: the inert expiry arm ───────────────────────────────────────────


class ContinueApproval(BaseModel):
    verdict: str


class Verdict(BaseModel):
    v: str


timer = WorkflowApp()


@timer.actor(queue="cpu", max_attempts=3)
async def timer_waiter(ctx: StepContext, params: SubProbe) -> Verdict:
    _ = params
    outcome = await ctx.wait_signal((ContinueApproval,), reason="rv5-c5a", timeout_s=0.05)
    match outcome:
        case ContinueApproval():
            return Verdict(v="approved")
        case Expired():
            return Verdict(v="expired")


@timer.workflow("rv5_red_timer")
def rv5_red_timer() -> Promise[object]:
    return build(step(timer_waiter, SubProbe(doc_id="d1"), key="hold"))


async def child_roots(conn: asyncpg.Connection, parent: JobId) -> list[str]:
    rows = await conn.fetch(
        f"SELECT id FROM \"{SCHEMA}\".jobs WHERE step_key = '__flow__' "
        "AND (metadata->>'parent_flow_id')::uuid = $1 ORDER BY id",
        parent,
    )
    return [str(r["id"]) for r in rows]


async def node_row(conn: asyncpg.Connection, flow_id: JobId, step_key: str) -> dict[str, object]:
    r = await conn.fetchrow(
        f"SELECT id, status::text AS status, metadata FROM \"{SCHEMA}\".jobs "
        "WHERE step_key = $1 AND (metadata->>'flow_id')::uuid = $2",
        step_key,
        flow_id,
    )
    assert r is not None
    d = dict(r)
    d["metadata"] = json.loads(d["metadata"]) if isinstance(d["metadata"], str) else d["metadata"]
    return d


async def main() -> None:
    # THE REGISTRATION DOOR: the compile registry is fed by app.get —
    # every definition referenced by a spawn must be materialized in
    # THIS process.
    dragon.get("rv5_red_child")
    dragon.get("rv5_red_dragon_parent")
    retention.get("rv5_red_ret_child")
    retention.get("rv5_red_ret_parent")

    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)

    print("=" * 72)
    print("C1 — THE ASYNCIO.GATHER DRAGON (K=5 gather spawns, NO keys)")
    print("=" * 72)
    runner = FlowRunner(dragon.get("rv5_red_dragon_parent"), pool, SCHEMA)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=2000)
    conn = await pool.acquire()
    roots = await child_roots(conn, claim.flow_id)
    print("   outcome:", outcome)
    print("   DISTINCT child roots minted under the parent:", len(roots), "(the law: 5)")
    await pool.release(conn)

    print("=" * 72)
    print("C2b — THE RECURSIVE CANCEL CASCADE (a 1200-deep chain)")
    print("=" * 72)
    conn = await asyncpg.connect(DSN)
    ids = await chain_rows(conn, 1200)
    await conn.close()
    try:
        await cancel_workflow_run(pool, schema=SCHEMA, flow_id=ids[0], reason="rv5-red")
        print("   the cancel returned clean")
    except RecursionError as exc:
        print("   THE BOMB: RecursionError at the operator's face:", exc)
    except Exception as exc:  # noqa: BLE001
        print(f"   THE CASCADE RAISED: {type(exc).__name__}: {exc}")
    conn = await asyncpg.connect(DSN)
    left = await conn.fetchval(
        f'SELECT count(*) FROM "{SCHEMA}".jobs WHERE step_key = \'__flow__\' '
        "AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')"
    )
    print("   runs left NON-TERMINAL after the 500:", int(left or 0), "(the law: 0)")
    await conn.close()

    print("=" * 72)
    print("C3 — THE RETENTION-RESPAWN FAIL-CLOSE (purge + resume)")
    print("=" * 72)
    runner = FlowRunner(retention.get("rv5_red_ret_parent"), pool, SCHEMA)
    claim = await runner.create_flow()
    rows = await runner._claimable(claim.flow_id)
    await runner._run_node(claim.flow_id, rows[0])  # spawn + hold
    conn = await pool.acquire()
    parent_node = await node_row(conn, claim.flow_id, "spawn_join")
    child_id = str(parent_node["metadata"]["child_flows"][0])
    print("   the parent holds; the child:", child_id)
    await conn.execute(
        f"DELETE FROM \"{SCHEMA}\".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid", child_id
    )
    print("   the child's rows PURGED (the retention sweep's face)")
    await pool.release(conn)
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=400)
    conn = await pool.acquire()
    roots = await child_roots(conn, claim.flow_id)
    root_row = await conn.fetchrow(
        f'SELECT status::text AS status, error_class FROM "{SCHEMA}".jobs WHERE id = $1',
        claim.flow_id,
    )
    print("   outcome:", outcome)
    print("   child roots after the resume:", len(roots), "(the law: 0 — NEVER a respawn)")
    print(
        "   the parent's face:",
        root_row["status"],
        root_row["error_class"],
        "(the law: failed / ChildFlowLostError)",
    )
    await pool.release(conn)

    print("=" * 72)
    print("C4 — THE ORPHAN WINDOW (crash between create and receipt)")
    print("=" * 72)
    runner = FlowRunner(retention.get("rv5_red_ret_parent"), pool, SCHEMA)
    claim = await runner.create_flow()
    rows = await runner._claimable(claim.flow_id)
    await runner._run_node(claim.flow_id, rows[0])  # spawn + hold (receipt written)
    conn = await pool.acquire()
    parent_node = await node_row(conn, claim.flow_id, "spawn_join")
    child_id = str(parent_node["metadata"]["child_flows"][0])
    # rewind to the crash window's committed state: the receipt un-lands
    await conn.execute(
        f'UPDATE "{SCHEMA}".jobs SET metadata = metadata - \'child_flows\' WHERE id = $1',
        parent_node["id"],
    )
    await pool.release(conn)
    await cancel_workflow_run(pool, schema=SCHEMA, flow_id=claim.flow_id, reason="rv5-red-c4")
    conn = await pool.acquire()
    child_status = await conn.fetchval(
        f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1::uuid', child_id
    )
    print(
        "   the crash-orphan child's status after the parent's cancel:",
        child_status,
        "(the law: cancelled)",
    )
    await pool.release(conn)

    print("=" * 72)
    print("C5a — THE DEV DRIVE'S INERT EXPIRY ARM (timeout_s in a dev drive)")
    print("=" * 72)
    runner = FlowRunner(timer.get("rv5_red_timer"), pool, SCHEMA)
    claim = await runner.create_flow()
    outcome = await runner.drive(claim.flow_id, until="terminal", max_ticks=200)
    conn = await pool.acquire()
    print("   the drive's outcome:", outcome, "(the law: terminal — Expired fired)")
    if outcome != "terminal":
        held = await conn.fetchval(
            f'SELECT status::text FROM "{SCHEMA}".wf_signals WHERE workflow_id = $1', claim.flow_id
        )
        print("   the hold's status:", held, "— the deadline NEVER fired in the dev drive")
    await pool.release(conn)

    print("=" * 72)
    print("C5c — THE RESOLVE BOUNCE (a human resolve on a child-join hold)")
    print("=" * 72)
    runner = FlowRunner(retention.get("rv5_red_ret_parent"), pool, SCHEMA)
    claim = await runner.create_flow()
    rows = await runner._claimable(claim.flow_id)
    await runner._run_node(claim.flow_id, rows[0])  # spawn + hold
    client = HitlClient(pool, schema=SCHEMA)
    (hold,) = await client.list(claim.flow_id)
    print("   the child-join hold:", hold.signal_name)
    result = await client.resolve(hold.hold_id, {"findings": ["the operator's fake"]})
    print("   resolve:", result.status, "-", result.reason)
    after = await client.list(claim.flow_id)
    print(
        "   the hold after the resolve:",
        [h.status for h in after] or "(gone — the delivered row's list face)",
    )
    print(
        "   THE BOUNCE: the resolve DELIVERED, the body will re-execute, "
        "the live child re-holds — the no-op wearing a 200"
        if result.status == "delivered"
        else "   (the honest refusal — cured)"
    )

    try:
        await asyncio.wait_for(pool.close(), timeout=10)
    except TimeoutError:
        print("(the repro's own teardown: a leaked drive connection — not the conviction)")


if __name__ == "__main__":
    asyncio.run(main())
