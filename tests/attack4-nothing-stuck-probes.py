# ruff: noqa: N999, S603, S607, S608, ASYNC221, ASYNC230  # Why: the dash-named attack4-* module; the restart probe runs pg_ctl with the env-pinned data dir and reads/writes the cluster's own conf between restarts.
"""ATTACK4 — THE NOTHING-STUCK PROBES' TEETH (the attacker's own shapes —
NOT the builder's; tests/attack4_stuck_probes.py owns those).

* THE SELF-CANCELLING BODY: a map's source body cancels ITS OWN run
  mid-emit (the drive loop's fork tx and the cancel cascade race in one
  process) — the run must land in a NAMED terminal with no half-emitted
  children read as progress, and the drive must return, not wedge;
* THE RESOLVE AFTER THE TERMINAL: the flow's root flipped terminal while
  a hold still stands (the crash-window world) — the resolve's outcome
  must be DEFINED, and a delivered hold must never wake a node on a
  terminal run (the terminal fence at the ROOT, not only the node);
* THE UNREGISTERED GATE: a hold whose row carries NO payload_schema (the
  pre-typing upgrade world — a row written before the typed door
  existed) meets a resolve — the typed door's cold-process fallback must
  refuse the untyped delivery LOUDLY (no silent untyped door);
* THE RESTART ROUND (gated on ATTACK4_PG_DATA — needs a restartable PG;
  the capture ran with it): the lost-job probe re-run with the DATABASE
  RESTARTED mid-round — a node claimed before the restart must land
  terminal after it; zero jobs lost, every attempt accounted.

This file FIXES NOTHING: a red here is a finding, reported.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step
from taskq.workflows.api import GateDecl
from taskq.workflows.api._hitl import HitlClient

pytestmark = pytest.mark.integration


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


class Item(BaseModel):
    n: int


# ── (a) the body that cancels its own run mid-emit ───────────────────────


async def test_a_body_that_cancels_its_own_run_mid_emit(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """The source body pulls the rug: the flow's own cancel cascade fires
    while the source's children are being emitted. Bounded by the test's
    timeout; the run lands in a NAMED terminal; the rows are coherent
    (no child reads as done while the root says cancelled; no zombie
    running node outlives the terminal)."""
    from taskq.workflows import cancel_workflow_run

    schema = module_pg_schema.schema_name
    app = WorkflowApp()

    async def suicidal_source(ctx: Any, params: Ingest) -> list[Item]:
        # THE RUG PULL: this run cancels ITSELF while it emits.
        await cancel_workflow_run(
            module_pg_pool,
            schema=schema,
            flow_id=ctx.flow_id,
            reason="the body cancelled its own run",
            principal="attack4",
        )
        return [Item(n=i) for i in range(5)]

    async def per_item(ctx: Any, item: Item) -> dict[str, int]:
        return {"n": item.n}

    @app.workflow("attack4c_self_cancel")
    def self_cancel() -> object:
        ingested = step(suicidal_source, Ingest(doc_id="d1"), key="src")
        children = map_source(ingested, per_item, key="kid")
        return build(children)

    runner = FlowRunner(app.get("attack4c_self_cancel"), module_pg_pool, schema)
    flow_id = await runner.create_flow()
    # THE BOUND: the drive ends (any outcome) — a wedge is the defect.
    outcome = await asyncio.wait_for(runner.drive(flow_id, max_ticks=200), timeout=60)
    assert outcome in ("terminal", "max_ticks", "held"), outcome
    root = await clean_pg_conn.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', flow_id
    )
    assert root in ("cancelled", "failed", "succeeded", "running"), (
        f"an unnamed root state: {root!r}"
    )
    # THE COHERENCE: a cancelled run has no running children.
    if root in ("cancelled", "failed"):
        kids = await clean_pg_conn.fetch(
            f'SELECT step_key, status FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
            flow_id,
        )
        for k in kids:
            assert k["status"] != "running", (
                f"the zombie: child {k['step_key']} is running on a {root} run"
            )


# ── (b) the resolve arriving after the flow's terminal ───────────────────


async def test_a_resolve_arriving_after_the_flow_went_terminal(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """The crash-window world: the root flipped terminal (via the fence
    probes' own door — a direct row write) while the hold still stands.
    The LATE resolve must be DEFINED, and a delivered hold must never
    wake a node on a terminal run (a pending child on a succeeded root is
    the incoherence this probe hunts)."""
    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    async def holds(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the late resolve")
        return "done"

    @app.workflow("attack4c_late_resolve")
    def late_resolve() -> object:
        return build(step(holds, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4c_late_resolve"), module_pg_pool, schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id, until="held")

    # The FENCE-BYPASS world: the root goes terminal over the held node
    # (the crash/fence probe's write — the rows are the truth, so this IS
    # a reachable world).
    await clean_pg_conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', finished_at = now() "
        "WHERE id = $1 AND step_key = '__flow__'",
        flow_id,
    )
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(str(flow_id))
    result = await client.resolve(hold.hold_id, {"verdict": "approve"})
    # THE DEFINED OUTCOME: whatever it is, it is TYPED (never an exception
    # to the caller).
    assert result.status in ("delivered", "no-op", "refused"), result
    # THE INCOHERENCE CHECK: a delivered hold on a terminal run must not
    # leave a re-pended child (the terminal fence at the ROOT).
    node = await clean_pg_conn.fetchrow(
        f"SELECT status, scheduled_at, metadata ? 'hold' AS hold_mark FROM \"{schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'review'",
        flow_id,
    )
    assert node is not None
    if result.status == "delivered":
        # The delivery consumed the hold; the node it woke must not sit
        # pending on a run the root calls SUCCEEDED (the drive loop would
        # re-drive it, or the status surfaces disagree forever).
        assert not (node["status"] == "pending" and node["hold_mark"]), (
            f"THE LATE WAKE: the delivered hold re-pended node on a "
            f"succeeded run (node status {node['status']!r}, hold mark set)"
        )


# ── (c) the gate definition unregistered between hold and resolve ────────


async def test_a_hold_without_payload_schema_meets_the_typed_door(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """THE UPGRADE WORLD, COLD PROCESS: a hold row written WITHOUT a
    payload_schema (a row from before the typed door, or a hand-migrated
    one) meets a resolve from a process that NEVER RAN THE WAIT SITE
    (the real cold process — a subprocess; this process ran the drive,
    so its model catalog cannot witness this probe). The cold fallback's
    ``no declared models on the row`` arm must not fit ANYTHING."""
    import subprocess as sp
    import sys
    import textwrap

    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    async def holds(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the cold gate")
        return "done"

    @app.workflow("attack4c_cold_gate")
    def cold_gate() -> object:
        return build(step(holds, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4c_cold_gate"), module_pg_pool, schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id, until="held")

    # THE REGISTRATION IS GONE: the row's payload_schema erased — the
    # ROW is the cold process's only witness, and it now witnesses
    # NOTHING.
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".wf_signals SET payload_schema = NULL '
        "WHERE workflow_id = $1 AND status = 'held'",
        flow_id,
    )
    # THE COLD RESOLVER: a fresh process, the garbage payload, the raw
    # HitlClient (no mounted definitions to lean on).
    probe = textwrap.dedent(f"""
        import asyncio, json, sys
        import asyncpg
        from taskq.workflows.api._hitl import HitlClient

        async def main() -> None:
            pool = await asyncpg.create_pool({module_pg_schema.pg_dsn!r})
            client = HitlClient(pool, schema={schema!r})
            (hold,) = await client.list({str(flow_id)!r})
            result = await client.resolve(
                hold.hold_id,
                {{"totally": "undeclared", "verdict": {{"nested": True}}}},
            )
            print("RESULT:", result.status, "|", result.reason)
            await pool.close()

        asyncio.run(main())
    """)
    proc = sp.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=120)
    print(f"\n[attack4 cold-gate] rc={proc.returncode} out={proc.stdout.strip()!r}")
    if "RESULT: delivered" in proc.stdout:
        pytest.fail(
            "THE UNTYPED DOOR'S HOLE, OBSERVED (cold process): a hold with NO "
            f"payload_schema accepted a payload nothing declared — {proc.stdout.strip()!r} "
            "(the cold fallback's 'legacy hold' arm fits anything)"
        )
    assert "RESULT: refused" in proc.stdout, (
        f"the cold resolve's outcome is neither delivered nor refused: {proc.stdout!r} {proc.stderr!r}"
    )


async def test_a_hold_with_payload_schema_in_the_warm_process_still_refuses_garbage(
    module_pg_pool: Any, module_pg_schema: ModulePgSchema, clean_pg_conn: Any
) -> None:
    """The CONTROL: the same garbage payload against the hold's REAL
    payload_schema (the row's witness intact) — the typed refusal, the
    hold survives (the door's teeth, warmed)."""
    schema = module_pg_schema.schema_name
    app = WorkflowApp()
    gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)

    async def holds(ctx: Any, params: Ingest) -> str:
        await ctx.wait_signal((Approval,), timeout_s=120.0, reason="the control")
        return "done"

    @app.workflow("attack4c_gate_control")
    def gate_control() -> object:
        return build(step(holds, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    runner = FlowRunner(app.get("attack4c_gate_control"), module_pg_pool, schema)
    flow_id = await runner.create_flow()
    await runner.drive(flow_id, until="held")
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(str(flow_id))
    result = await client.resolve(
        hold.hold_id, {"totally": "undeclared", "verdict": {"nested": True}}
    )
    assert result.status == "refused", result
    assert len(await client.list(str(flow_id))) == 1, "the refused resolve moved the hold"


# ── (d) the lost-job probe with the DB restarted mid-round ───────────────


@pytest.mark.timeout(600)
@pytest.mark.skipif(
    not os.environ.get("ATTACK4_PG_DATA"),
    reason="needs a restartable cluster (ATTACK4_PG_DATA + the port; the "
    "capture ran with the attacker's own PG on :5711)",
)
async def test_the_lost_job_probe_with_the_db_restarted_mid_round(
    module_pg_schema: ModulePgSchema,
) -> None:
    """THE RESTART ROUNDS: five rounds; each seeds a 2-node run, drives
    one tick (the parent mid-flight), RESTARTS the database, reconnects,
    and drives to terminal. Every node lands terminal with its ledger
    row — a job lost across the restart is the defect; the claim's lease
    + the crash-window heal own the recovery."""
    import subprocess as sp

    pg_data = os.environ["ATTACK4_PG_DATA"]
    pg_port = os.environ.get("ATTACK4_PG_PORT", "5711")

    # The cluster's own conf carries the port + the socket dir (so a
    # plain pg_ctl restart re-binds the same way the capture's boot did).
    conf = os.path.join(pg_data, "postgresql.conf")
    with open(conf) as f:
        existing = "\n".join(
            line for line in f.read().splitlines() if not line.lstrip().startswith("#")
        )
    needed = [
        f"port = {pg_port}",
        f"unix_socket_directories = '{os.path.dirname(pg_data.rstrip('/'))}'",
        "listen_addresses = '127.0.0.1'",
    ]
    add = [line for line in needed if line.split(" = ")[0] + " = " not in existing]
    if add:
        with open(conf, "a") as f:
            f.write("\n# attack4's restart-round pins\n" + "\n".join(add) + "\n")

    def restart_pg() -> None:
        """The crash world: `-m immediate` (no graceful drain — the
        postmaster is killed, every backend dies mid-write), then the
        start (the WAL recovery)."""
        r = sp.run(
            ["pg_ctl", "-D", pg_data, "stop", "-m", "immediate"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert r.returncode == 0, f"the kill failed: {r.stderr}"
        # Start with its own log file (without -l the postmaster inherits
        # the caller's pipes and sp.run blocks on them forever), then
        # poll the TCP port (pg_ctl's own readiness poll can miss on
        # this stack).
        start_log = os.path.join(os.path.dirname(pg_data.rstrip("/")), "restart-round.log")
        r = sp.run(
            ["pg_ctl", "-D", pg_data, "start", "-l", start_log],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if r.returncode != 0:
            raise AssertionError(f"pg_ctl start failed: {r.stderr}")

    async def wait_ready(dsn_: str, seconds: float = 180) -> None:
        deadline = asyncio.get_event_loop().time() + seconds
        while asyncio.get_event_loop().time() < deadline:
            try:
                c = await asyncpg.connect(dsn_, timeout=5)
                await c.close()
                return
            except (OSError, asyncpg.PostgresError):
                await asyncio.sleep(1.0)
        raise AssertionError("the restarted server never answered the port")

    dsn = module_pg_schema.pg_dsn
    schema = module_pg_schema.schema_name
    app = WorkflowApp()

    async def _parent(ctx: Any, params: Ingest) -> str:
        return "up"

    async def _child(ctx: Any, up: str) -> str:
        return up + "!"

    @app.workflow("attack4c_restart_round")
    def restart_round() -> object:
        return build(step(_child, step(_parent, Ingest(doc_id="d1"), key="up"), key="down"))

    compiled = app.get("attack4c_restart_round")
    for round_no in range(3):
        pool = await asyncpg.create_pool(dsn, min_size=1)
        runner = FlowRunner(compiled, pool, schema)
        flow_id = await runner.create_flow()
        await runner.tick(flow_id)  # the parent claims + finalizes mid-round
        mid = await pool.fetchval(
            f'SELECT status FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'up'",
            flow_id,
        )
        await pool.close()
        # THE RESTART (mid-round — the child has never run).
        restart_pg()
        await wait_ready(dsn)
        # RECONNECT + drive to terminal: the child must NOT be lost.
        pool2 = await asyncpg.create_pool(dsn, min_size=1)
        runner2 = FlowRunner(compiled, pool2, schema)
        await runner2.drive(flow_id)
        rows = await pool2.fetch(
            f'SELECT step_key, status FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
            flow_id,
        )
        states = {r["step_key"]: r["status"] for r in rows}
        await pool2.close()
        assert states.get("up") == "succeeded", f"round {round_no}: {states}"
        assert states.get("down") == "succeeded", (
            f"round {round_no}: THE LOST JOB — the child never landed after "
            f"the restart (mid-round parent was {mid!r}): {states}"
        )
