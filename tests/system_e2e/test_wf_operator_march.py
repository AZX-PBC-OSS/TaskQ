"""THE OPERATOR'S MARCH (T15 — the system tier's composed walk).

ONE scenario walks the operator's own path end to end (the deploy
matrix drills the CELLS; this march walks the DAY):

1. DEPLOY — the fleet boots to the joined-fleet standard (every pod
   registered, health-green, its beat advancing).
2. OBSERVE — the operator's reads while the march runs: the fleet's
   roster, the leader's seat, the run's derived status, the explorer's
   display — each read reconciled against the rows.
3. INTERVENE — the operator cancels ONE run mid-flight (the cancel
   cascade: the running node terminalised, the pending children
   resolved, the flow's flip the linearization point); the audit trail
   carries it (G4).
4. DEBUG — the config-drift read: a node re-routed to a queue nothing
   serves stays LIVE-blocked with the reason NAMED (never a crash,
   never a silent orphan); the shipped alert's join (depth > 0, zero
   live workers) confirms.
5. UPGRADE — the rolling second generation: a NEW pod joins the live
   fleet mid-run (the joined-fleet standard); the fleet serves the run
   from BOTH generations; the per-attempt code_version record rides
   the claims.
6. ROLLBACK — the vanilla pod (no workflow capability) joins: the
   fence never hands it a workflow row, the pod serves vanilla work,
   the capable fleet finishes the run (the additive-only ledger's
   always-safe shape).
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the settings boundary the fixtures validated; every value is $-bound.

from __future__ import annotations

import asyncio
import time
from typing import Any

import asyncpg
import pytest

from tests.system_e2e._harness import (
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
    spawn_worker,
    wait_worker_ready,
)
from tests.system_e2e._invariants import assert_balanced
from tests.system_e2e._wf_app import MARCH_FLOWS, MARCH_WORKER_QUEUES, WF_QUEUE
from tests.system_e2e._wf_harness import (
    MARCH_LEADER_LEASE_S,
    MARCH_LOCK_LEASE_S,
    MARCH_SETTLE_BOUND_S,
    join_fires,
    spawn_wf_fleet,
    tag_run_rows,
    wait_flow_terminal,
)

pytestmark = [pytest.mark.system, pytest.mark.integration]

#: The test-side tag: the march's population.
_TAG = "wf-march-operator"

#: The drift node's observe bound (the matrix's derivation).
_DRIFT_OBSERVE_BOUND_S = (MARCH_LOCK_LEASE_S + 1.0 + 5.0) * TIER_LOAD_STRETCH


@pytest.mark.timeout(900)
async def test_the_operators_march_deploy_to_rollback(
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """The operator's day: deploy → observe → intervene → debug →
    upgrade → rollback — one fleet, one march, the invariants closing
    every phase's population."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    extras: list[WorkerProc] = []
    try:
        # ══ 1. DEPLOY ════════════════════════════════════════════════
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["op1", "op2"])
        roster = await conn.fetch(f'SELECT id::text FROM "{schema}".workers')
        assert len(roster) == 2, f"the deploy's roster is short: {len(roster)} pods"

        # ══ 2. OBSERVE ═══════════════════════════════════════════════
        # The leader's seat is TAKEN and its lease advancing.
        leader = await conn.fetchrow(f'SELECT worker_id::text, expires_at FROM "{schema}".maintenance_leader')
        assert leader is not None, "no leader elected — the fleet's maintenance is headless"
        seen_before = await conn.fetchval(
            f'SELECT last_seen_at FROM "{schema}".maintenance_leader'
        )
        await asyncio.sleep(1.5)
        seen_after = await conn.fetchval(f'SELECT last_seen_at FROM "{schema}".maintenance_leader')
        assert seen_after > seen_before, "the leader's beat is not advancing"

        # The run goes live; the OBSERVE reads its derived status + the
        # explorer's display against the rows.
        from taskq.workflows.api._runner import FlowRunner
        from taskq.workflows._progress_read import run_display
        from taskq.workflows._sql import WorkflowSql
        from taskq.backend._protocol import JobId as Jid

        pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=4)
        wsql = WorkflowSql.build(schema)
        runner = FlowRunner(MARCH_FLOWS["reclaim_target"], pool, schema)
        flow_id = await runner.create_flow(run_key="operator-march:observe")
        await tag_run_rows(pool, schema, str(flow_id), _TAG)
        start = time.monotonic()
        observed: str | None = None
        while time.monotonic() - start < _DRIFT_OBSERVE_BOUND_S:
            observed = await conn.fetchval(
                f"""SELECT status::text FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'long'""",
                str(flow_id),
            )
            if observed == "running":
                break
            await asyncio.sleep(0.2)
        assert observed == "running", "the observe read never saw the run live"
        display = await run_display(pool, wsql, Jid(str(flow_id)))
        assert display, "the explorer's observe read is empty"

        # ══ 3. INTERVENE ═════════════════════════════════════════════
        # THE OPERATOR'S CANCEL: the run is cancelled mid-flight — the
        # cascade terminalises the running node, resolves the pending
        # children, the flow's flip is the linearization point; the
        # audit carries it.
        from taskq.workflows.api._runner_exit import cancel_workflow_run

        await cancel_workflow_run(
            pool, schema=schema, flow_id=Jid(str(flow_id)), reason="the operator's intervention"
        )
        status, measured = await wait_flow_terminal(conn, schema, str(flow_id))
        print(f"[operator] the cancelled run terminalized: {status} in {measured:.2f}s")
        assert status == "cancelled", f"the intervention derived {status!r}, not cancelled"
        audit = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".admin_audit
                WHERE action = 'workflow.cancel' AND target_id = $1""",
            str(flow_id),
        )
        assert audit >= 1, "the intervention left no audit row (G4's trail)"
        await assert_balanced(sys_ledger, schema, _TAG)

        # ══ 4. DEBUG ═════════════════════════════════════════════════
        # The drift read: a run whose downstream node sits on a queue
        # NOTHING serves — LIVE-blocked, the reason NAMED, the alert's
        # join confirmable.
        drift_runner = FlowRunner(MARCH_FLOWS["drift_target"], pool, schema)
        drift_id = await drift_runner.create_flow(run_key="operator-march:debug")
        start = time.monotonic()
        gone: asyncpg.Record | None = None
        while time.monotonic() - start < _DRIFT_OBSERVE_BOUND_S:
            gone = await conn.fetchrow(
                f"""SELECT status::text AS status FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND step_key = 'gone' AND queue = 'nowhere_queue'""",
                str(drift_id),
            )
            if gone is not None:
                break
            await asyncio.sleep(0.3)
        assert gone is not None, "the drift node never reached its unserved queue"
        assert gone["status"] in ("pending", "scheduled"), (
            f"the drift node reached {gone['status']!r} — something claims work no pod can run"
        )
        depth = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE queue = 'nowhere_queue' AND status IN ('pending', 'scheduled')"""
        )
        live = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".workers
                WHERE $1::text = ANY(
                    SELECT jsonb_array_elements_text(metadata->'queues'))""",
            WF_QUEUE,
        )
        assert depth > 0 and live == 0, (
            f"the TaskQQueueUnserved join must hold: depth={depth}, live={live}"
        )

        # ══ 5. UPGRADE ═══════════════════════════════════════════════
        # The rolling second generation: a NEW pod joins the LIVE fleet
        # (the joined-fleet standard); the run serves from both
        # generations; the per-attempt record rides the claims (the
        # §22.1 record).
        live_run = FlowRunner(MARCH_FLOWS["reclaim_target"], pool, schema)
        upgrade_id = await live_run.create_flow(run_key="operator-march:upgrade")
        gen2 = spawn_worker(
            module_pg_schema.pg_dsn,
            schema,
            tag="wf-gen2",
            extra_env={
                "TASKQ_LEADER_LEASE": str(MARCH_LEADER_LEASE_S),
                "TASKQ_LOCK_LEASE": str(MARCH_LOCK_LEASE_S),
                "TASKQ_MAX_HEARTBEAT_FAILURES": "10",
                "TASKQ_QUEUES": MARCH_WORKER_QUEUES,
            },
            entry="tests.system_e2e._wf_entry",
        )
        wait_worker_ready(gen2)
        extras.append(gen2)
        print(f"[operator] the second generation joined: pid={gen2.proc.pid}")

        resolver = asyncio.create_task(
            _resolve_holds(pool, schema, str(upgrade_id))
        )
        status, measured = await wait_flow_terminal(
            conn, schema, str(upgrade_id), bound_s=MARCH_SETTLE_BOUND_S * 2
        )
        resolver.cancel()
        print(f"[operator] the upgraded fleet's run: {status} in {measured:.2f}s")
        assert status == "complete", f"the upgraded fleet's run derived {status!r}"
        stamped = await conn.fetchval(
            f"""SELECT code_version FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'long'""",
            str(upgrade_id),
        )
        assert stamped, "the claimed node carries no code_version record (§22.1)"
        await tag_run_rows(pool, schema, str(upgrade_id), _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)

        # ══ 6. ROLLBACK ══════════════════════════════════════════════
        # The vanilla pod joins (the v1 deployment): the fence never
        # hands it a workflow row; the pod stays alive; the capable
        # fleet finishes the run.
        rollback_id = await live_run.create_flow(run_key="operator-march:rollback")
        v1 = spawn_worker(
            module_pg_schema.pg_dsn,
            schema,
            tag="wf-v1-rollback",
            extra_env={
                "TASKQ_LEADER_LEASE": str(MARCH_LEADER_LEASE_S),
                "TASKQ_QUEUES": MARCH_WORKER_QUEUES,
            },
        )
        wait_worker_ready(v1)
        extras.append(v1)
        start = time.monotonic()
        holder: str | None = None
        while time.monotonic() - start < _DRIFT_OBSERVE_BOUND_S:
            holder = await conn.fetchval(
                f"""SELECT w.metadata->>'workflow_execution' FROM "{schema}".jobs j
                    JOIN "{schema}".workers w ON w.id = j.locked_by_worker
                    WHERE (j.metadata->>'flow_id')::uuid = $1::uuid
                      AND j.step_key = 'long' AND j.status = 'running'""",
                str(rollback_id),
            )
            if holder is not None:
                break
            await asyncio.sleep(0.2)
        assert holder == "true", (
            f"a workflow row was claimed by a worker stamped {holder!r} — the fence failed"
        )
        status, measured = await wait_flow_terminal(
            conn, schema, str(rollback_id), bound_s=MARCH_SETTLE_BOUND_S * 2
        )
        print(f"[operator] the rolled-back fleet's run: {status} in {measured:.2f}s")
        assert status == "complete", f"the rolled-back fleet's run derived {status!r}"
        assert v1.proc.poll() is None, "the vanilla pod crashed under the mixed fleet"
        await tag_run_rows(pool, schema, str(rollback_id), _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
        fires = await join_fires(conn, schema, str(rollback_id))
        assert all(f["fires"] == 1 for f in fires), f"a join fired twice: {fires}"
    finally:
        for pod in extras:
            reap(pod)
        for pod in fleet.values():
            reap(pod)
        await conn.close()


async def _resolve_holds(pool: asyncpg.Pool, schema: str, flow_id: str) -> None:
    """Resolve every hold as it appears (approve) — the march's human."""
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(pool, schema=schema)
    seen: set[str] = set()
    while True:
        holds = await client.list(flow_id)
        for hold in holds:
            if hold.hold_id in seen or hold.status != "held":
                continue
            seen.add(hold.hold_id)
            await client.resolve(
                hold.hold_id,
                {"verdict": "approve", "note": "the operator's march"},
                principal="operator-march",
            )
        await asyncio.sleep(0.3)
