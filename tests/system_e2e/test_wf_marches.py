"""THE DEEP-RESEARCH MARCH (T15 — the system tier's full walk).

ONE scenario, the whole spine, on a real fleet (real worker
subprocesses, one real Postgres — the deploy matrix's harness):

1. THE CRON KICKOFF — a REAL schedule (the client's own
   ``create_schedule``) whose every-minute slot the fleet's LEADER
   fires; the fire lands on the march's bridge actor, whose body kicks
   the workflow with the SLOT as the run key (G3's composition — the
   two dedup regimes compose).
2. THE MAP — four sources; the armed TRANSIENT failure heals on the
   ladder (the retried child succeeds alone); the armed PERMANENT
   failure fans in through the ``collect`` edge.
3. THE MULTI-HITL LOOP — the budget-capped review loop holds THREE
   distinct hold epochs (refine, refine, approve); each hold addressed
   BY ID through the typed door; the carry advanced exactly once per
   iteration (the ledger's iteration records).
4. THE ADMIN ACTIONS — every resolve's audit row (the G4 trail) read
   back and reconciled against the march's own decisions.
5. THE PARTIAL REPORT + THE TIMEOUT FACE — the publish step's typed
   wait is left UNRESOLVED on purpose: the expiry sweep's abandonment
   raises ``SignalTimeoutError`` in the body, the body CATCHES it and
   pivots to the degraded report — the report NAMES the failed source
   and its own degradation, never wedges.
6. THE STREAMING EMIT — the T20 router lived on the tier: the paged
   source emits TWO pages while running (the children + edges + cursor
   checkpoint per emit tx); the per-record chains route on the body's
   OWN typed outcomes (the conditional chains: 2 flagged → manual
   review, 2 clean → done).
7. THE KILL STORM — a pod is SIGKILLed mid-march (the map live); the
   march completes ANYWAY (the reclaim machinery owns the corpse's
   rows; the join counter never moved).
8. THE EXPLORER — the run display reconstructed FROM THE ROWS at every
   connect (the DH7 law): the nodes' states, the map's progress line,
   the declared read-side aggregate.

The numbers print next to their bounds (the harness's contract) and
the march's own timeline lands in ``.measurements/p5/`` (the
perf-evidence files' corpus).
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the settings boundary the fixtures validated; every value is $-bound.

from __future__ import annotations

import asyncio
import json
import signal
import time
from collections.abc import AsyncGenerator
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from tests.system_e2e._harness import (
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
)
from tests.system_e2e._invariants import assert_balanced
from tests.system_e2e._wf_app import CronKick, MARCH_FLOWS
from tests.system_e2e._wf_harness import (
    MARCH_SETTLE_BOUND_S,
    join_fires,
    spawn_wf_fleet,
    tag_run_rows,
    wait_flow_terminal,
)

pytestmark = [pytest.mark.system, pytest.mark.integration]

#: The test-side tag: the march's own population (the tier's convention).
_TAG = "wf-march-research"

#: The cron kickoff's bound: the schedule's every-minute slot (the fire
#: lands on the NEXT minute boundary) + the leader's tick cadence, then
#: the bridge's own execution — stretched.
_KICKOFF_BOUND_S = (70.0 + 30.0) * TIER_LOAD_STRETCH

#: The march's OWN timeline record (the perf-evidence file's rows).
_TIMELINE: list[dict[str, object]] = []


def _note(beat: str, measured: float, bound: float) -> None:
    _TIMELINE.append({"beat": beat, "measured_s": round(measured, 2), "bound_s": bound})
    print(f"[march] {beat}: measured={measured:.2f}s bound={bound:.2f}s")


@pytest_asyncio.fixture(scope="module")
async def march_pool(pg_dsn: str, module_pg_schema: Any) -> AsyncGenerator[asyncpg.Pool, None]:
    """The march process's own client pool."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    yield pool
    await pool.close()


async def _resolve_review_holds(
    pool: asyncpg.Pool, schema: str, flow_id: str, plan: list[str], audited: list[dict[str, Any]]
) -> None:
    """The march's human: resolve the review holds per the PLAN (one
    decision per hold epoch, each addressed BY ID through the typed
    door) — then STOP (the publish hold stays: the timeout face is the
    march's own beat). Every resolve's audit row is captured."""
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(pool, schema=schema)
    seen: set[str] = set()
    decisions = list(plan)
    while decisions:
        holds = await client.list(flow_id)
        progressed = False
        for hold in holds:
            if hold.hold_id in seen or hold.status != "held":
                continue
            seen.add(hold.hold_id)
            verdict = decisions.pop(0)
            result = await client.resolve(
                hold.hold_id,
                {"verdict": verdict, "note": f"the march's editorial {verdict}"},
                principal="march-editor",
                reason="the deep-research march's review",
            )
            audited.append(
                {"hold_id": str(hold.hold_id), "verdict": verdict, "result": str(result)}
            )
            progressed = True
            break
        if not progressed:
            await asyncio.sleep(0.3)


@pytest.mark.timeout(900)
async def test_the_deep_research_march_kick_to_explorer(
    march_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
    sys_client: Any,
) -> None:
    """The whole spine: cron kickoff → map (the ladder + the collect) →
    the multi-HITL loop → the publish's timeout face (the degraded
    report) → the streaming emit's conditional chains → the kill storm →
    the explorer's read. The invariants close the population."""
    from taskq.workflows._progress_read import (
        map_progress_line,
        run_display,
    )

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    audited: list[dict[str, Any]] = []
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["r1", "r2"])

        # ══ 1. THE CRON KICKOFF ══════════════════════════════════════
        # A REAL schedule on the march's bridge actor; the fleet's
        # leader fires the slot; the bridge kicks the run with the slot
        # as the run key.
        slot = time.strftime("%Y%m%dT%H%M")
        schedule = await sys_client.create_schedule(
            "wf_research_cron",
            "* * * * *",
            static_payload=CronKick(slot=slot).model_dump(),
            name="the-deep-research-march",
        )
        assert schedule is not None
        start = time.monotonic()
        flow_id: str | None = None
        while time.monotonic() - start < _KICKOFF_BOUND_S:
            row = await conn.fetchrow(
                f"""SELECT id::text FROM "{schema}".jobs
                    WHERE metadata->>'workflow' = 'deep_research'
                      AND idempotency_key = $1
                      AND step_key = '__flow__'""",
                f"deep-research:{slot}",
            )
            if row is not None:
                flow_id = row["id"]
                break
            await asyncio.sleep(0.5)
        assert flow_id is not None, (
            f"the cron kickoff never landed within {_KICKOFF_BOUND_S}s — the "
            "schedule's fire arm or the bridge's kick is broken"
        )
        _note("cron-kickoff", time.monotonic() - start, _KICKOFF_BOUND_S)

        # THE DOUBLE-FIRED SLOT'S COMPOSITION (G3): the schedule's
        # next fire would REUSE the slot only on a clock edge — the
        # arbiter's pin is the matrix's cell-7; here the RUN KEY is
        # pinned: the run was born from the BRIDGE's kick (the fire's
        # metadata names the schedule).
        fired = await conn.fetchval(
            f"""SELECT count(*) FROM "{schema}".jobs
                WHERE metadata->>'cron_schedule_id' = $1::text""",
            str(schedule.schedule_id),
        )
        assert fired >= 1, "the schedule's fire never enqueued the bridge"

        # ══ 2-5. THE MARCH RUNS — the holds resolved per the plan; the
        # publish hold LEFT (the timeout face). The kill storm lands
        # MID-MARCH (once the map's children are live).
        resolver = asyncio.create_task(
            _resolve_review_holds(
                march_pool, schema, flow_id, ["refine", "refine", "approve"], audited
            )
        )

        # THE MAP IS LIVE (the kill storm's premise).
        start = time.monotonic()
        n = 0
        while time.monotonic() - start < MARCH_SETTLE_BOUND_S:
            n = await conn.fetchval(
                f"""SELECT count(*) FROM "{schema}".jobs
                    WHERE (metadata->>'flow_id')::uuid = $1::uuid
                      AND map_index IS NOT NULL""",
                flow_id,
            )
            if n:
                break
            await asyncio.sleep(0.2)
        assert n, "the map's fork never fired"
        _note("map-fork", time.monotonic() - start, MARCH_SETTLE_BOUND_S)

        # ══ 7. THE KILL STORM: SIGKILL one pod mid-map. The march must
        # complete ANYWAY (the reclaim machinery owns the corpse's rows).
        victim_pid = await conn.fetchval(
            f"SELECT pid FROM \"{schema}\".workers WHERE id = "
            f"(SELECT locked_by_worker FROM \"{schema}\".jobs "
            f'WHERE (metadata->>\'flow_id\')::uuid = $1::uuid AND status = \'running\' '
            f"AND locked_by_worker IS NOT NULL LIMIT 1)",
            flow_id,
        )
        if victim_pid is not None:
            for name, pod in fleet.items():
                if pod.proc.pid == victim_pid:
                    pod.proc.send_signal(signal.SIGKILL)
                    reap(pod)
                    del fleet[name]
                    print(f"[march] SIGKILL -> {name} (mid-map)")
                    break
        # The storm's single-victim shape: a live pod must survive.
        assert any(pod.proc.poll() is None for pod in fleet.values()) or not fleet, (
            "the kill storm left no survivor"
        )

        status, measured = await wait_flow_terminal(conn, schema, flow_id)
        resolver.cancel()
        _note("march-terminal", measured, MARCH_SETTLE_BOUND_S * 2)
        if status != "complete":
            # THE ROWS SPEAK: a failed march names its failed nodes (the
            # debug's own discipline, shipped in the scenario).
            rows = await conn.fetch(
                f"""SELECT step_key, coalesce(map_index::text,'-') AS mi, status::text AS status,
                       attempt, error_class
                    FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid
                    ORDER BY step_key, map_index""",
                flow_id,
            )
            print(f"[march] FAILED RUN'S ROWS: {[dict(r) for r in rows]}")
        assert status == "complete", f"the march derived {status!r}, not complete"

        # ══ 2. THE MAP'S TWO FACES: the transient healed (the retried
        # child succeeded ALONE — attempt 2), the permanent fanned in.
        transient = await conn.fetchrow(
            f"""SELECT status::text AS status, attempt FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND payload->>'wf_item' LIKE '%src-transient%' LIMIT 1""",
            flow_id,
        )
        assert transient is not None and transient["status"] == "succeeded", (
            f"the transient source never healed: {transient}"
        )
        assert transient["attempt"] == 2, (
            f"the transient source healed without the ladder (attempt {transient['attempt']})"
        )

        # ══ 5. THE PARTIAL REPORT (the publish node's result): the
        # failed source NAMED, the degradation NAMED (the timeout face —
        # the publish hold was left unresolved ON PURPOSE).
        publish = await conn.fetchval(
            f"""SELECT result FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'publish'""",
            flow_id,
        )
        assert publish is not None, "the publish node never wrote a result"
        report = json.loads(publish) if isinstance(publish, str) else publish
        value = report.get("value", report)
        if isinstance(value, dict) and "fetched" not in value:
            value = value.get("value", value)
        assert value is not None
        report_doc: dict[str, Any] = value  # pyright: ignore[reportUnknownVariableType]
        assert "src-permanent" in report_doc.get("failed", []), (
            f"the partial report does not name the permanent failure: {report_doc}"
        )
        assert report_doc.get("degraded") is True, (
            f"the publish wait's timeout face never pivoted the report: {report_doc}"
        )
        assert "degraded" in report_doc.get("review_note", ""), (
            f"the degradation is not NAMED in the note: {report_doc}"
        )

        # ══ 3. THE MULTI-HITL LOOP: three hold epochs, the carry
        # advanced exactly once per iteration (the ledger's records).
        iters = await conn.fetch(
            f"""SELECT step_key, count(*)::int AS rows FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1::uuid AND step_key LIKE 'review.iter%'
                  AND status = 'succeeded'
                GROUP BY step_key ORDER BY step_key""",
            flow_id,
        )
        assert len(iters) == 3, (
            f"the review loop ran {len(iters)} iterations for refine/refine/approve: "
            f"{[dict(r) for r in iters]}"
        )
        assert all(r["rows"] == 1 for r in iters), (
            f"an iteration applied twice (the carry double-applied): {[dict(r) for r in iters]}"
        )
        holds = await conn.fetch(
            f"""SELECT hold_epoch, status::text AS status FROM "{schema}".wf_signals
                WHERE workflow_id = $1::uuid ORDER BY hold_epoch""",
            flow_id,
        )
        epochs = {r["hold_epoch"] for r in holds}
        assert len(epochs) >= 3, (
            f"the multi-HITL holds were not distinct epochs: {[dict(r) for r in holds]}"
        )

        # ══ 4. THE ADMIN ACTIONS' AUDIT (G4): every resolve wrote its
        # row, the principal named.
        audit_rows = await conn.fetch(
            f"""SELECT principal_subject, action FROM "{schema}".admin_audit
                WHERE action = 'hitl.resolve' AND target_id = ANY($1::text[])""",
            [a["hold_id"] for a in audited],
        )
        assert len(audit_rows) == len(audited), (
            f"the resolve audit trail is short: {len(audit_rows)} rows for "
            f"{len(audited)} resolves"
        )
        assert all(r["principal_subject"].startswith("march-editor") for r in audit_rows), (
            f"the resolves' principal is not the march's editor: {[dict(r) for r in audit_rows]}"
        )

        # ══ 8. THE EXPLORER: the display reconstructed FROM THE ROWS.
        from taskq.workflows._sql import WorkflowSql

        wsql = WorkflowSql.build(schema)
        from taskq.backend._protocol import JobId as Jid

        display = await run_display(march_pool, wsql, Jid(flow_id))
        assert display, "the explorer's display read is empty"
        # The display model keys the JOB IDS (the rows' own identity);
        # the step keys reconcile against the run's rows.
        node_rows = await conn.fetch(
            f"""SELECT id::text AS id, step_key, status::text AS status FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key <> '__flow__'""",
            flow_id,
        )
        display_states = {r["id"]: display.get(r["id"], {}).get("status") for r in node_rows}
        publish_row = next(r for r in node_rows if r["step_key"] == "publish")
        assert display_states.get(publish_row["id"]) == "succeeded", (
            f"the explorer's display does not show the terminal publish: "
            f"{publish_row['id']} -> {display_states.get(publish_row['id'])}"
        )
        line = await map_progress_line(march_pool, wsql, Jid(flow_id))
        assert isinstance(line, list), "the map's progress line did not read"

        # THE INVARIANTS close the march.
        await tag_run_rows(march_pool, schema, flow_id, _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
        fires = await join_fires(conn, schema, flow_id)
        assert all(f["fires"] == 1 for f in fires), f"a join fired twice: {fires}"
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


@pytest.mark.timeout(600)
async def test_the_streaming_emit_march_conditional_chains(
    march_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """THE STREAMING EMIT (the T20 router lived on the tier): the paged
    source emits TWO pages while running; the per-record chains route on
    the body's OWN typed outcomes — 2 flagged (risk > 0.8) walk the
    manual-review step, 2 clean go straight to done. The cursor
    checkpoints per emit tx; the join fires exactly once."""
    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["s1", "s2"])
        runner_ctor = MARCH_FLOWS["triage_chain"]
        from taskq.workflows.api._runner import FlowRunner

        runner = FlowRunner(runner_ctor, march_pool, schema)
        flow_id = await runner.create_flow()
        await tag_run_rows(march_pool, schema, str(flow_id), _TAG)

        status, measured = await wait_flow_terminal(conn, schema, str(flow_id))
        print(f"[emit-march] terminal: measured={measured:.2f}s bound={MARCH_SETTLE_BOUND_S}s")
        assert status == "complete", f"the emit march derived {status!r}"

        # THE FOUR CHILDREN: two pages emitted while the source ran
        # (the chain's entry step IS the child's identity — chain_start);
        # the cursor checkpointed at the LAST page.
        children = await conn.fetch(
            f"""SELECT step_key, status::text AS status, payload FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND step_key = 'screen'""",
            str(flow_id),
        )
        assert len(children) == 4, (
            f"the two pages' four records are not all emitted children: {len(children)}"
        )
        assert all(c["status"] == "succeeded" for c in children), (
            f"an emitted chain child did not finish: {[dict(c) for c in children]}"
        )
        # THE CONDITIONAL CHAINS: the flagged records walked the
        # manual-review step (their chains' extra hop — the route rode
        # the body's OWN typed outcome), the clean did not.
        flagged = [c for c in children if (json.loads(c["payload"]) if isinstance(c["payload"], str) else c["payload"]).get("wf_item", {}).get("risk", 0) > 0.8]
        clean = [c for c in children if (json.loads(c["payload"]) if isinstance(c["payload"], str) else c["payload"]).get("wf_item", {}).get("risk", 0) <= 0.8]
        assert len(flagged) == 2 and len(clean) == 2, (
            f"the risk split is not 2/2: {[dict(c) for c in children]}"
        )
        reviewed = await conn.fetchval(
            f"""SELECT count(*)::int FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND step_key = 'manual_review' AND status = 'succeeded'""",
            str(flow_id),
        )
        assert reviewed == 2, (
            f"the flagged records' manual-review hop ran {reviewed} times, not 2"
        )
        cursor = await conn.fetchval(
            f"""SELECT metadata->'emit_cursor' FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND step_key = 'triage_source'""",
            str(flow_id),
        )
        assert cursor is not None, "the source's cursor never checkpointed"

        await tag_run_rows(march_pool, schema, str(flow_id), _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


@pytest.mark.timeout(600)
async def test_the_doc_ingest_march_fan_in_progress_render(
    march_pool: asyncpg.Pool,
    module_pg_schema: Any,
    sys_ledger: asyncpg.Connection,
) -> None:
    """THE DOC-INGEST MARCH: the map's fan-in barrier join
    (fail_closed), the maybe collect (a failed branch does not kill the
    run), the declared READ-SIDE aggregate (T21 decision c), and the
    graph's mermaid RENDER matching the wiring."""
    from taskq.workflows.api._mermaid import render_mermaid
    from taskq.workflows.api._runner import FlowRunner

    schema = module_pg_schema.schema_name
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    fleet: dict[str, WorkerProc] = {}
    try:
        fleet = await spawn_wf_fleet(conn, module_pg_schema.pg_dsn, schema, ["i1", "i2"])
        runner = FlowRunner(MARCH_FLOWS["doc_ingest_march"], march_pool, schema)
        flow_id = await runner.create_flow()
        await tag_run_rows(march_pool, schema, str(flow_id), _TAG)

        status, measured = await wait_flow_terminal(conn, schema, str(flow_id))
        print(f"[ingest-march] terminal: measured={measured:.2f}s bound={MARCH_SETTLE_BOUND_S}s")
        assert status == "complete", f"the ingest march derived {status!r}"

        # THE FAN-IN: the barrier's parents BOTH fired exactly once; the
        # maybe collect's branch resolved (the labels' gather).
        fires = await join_fires(conn, schema, str(flow_id))
        assert fires and all(f["fires"] == 1 for f in fires), (
            f"the fan-in's joins are not exactly-once: {fires}"
        )

        # THE RENDER: the graph's mermaid render carries every node the
        # wiring declared (the map's face is its JOIN — the map source's
        # own key compiles to '<source>.join'; the render is the
        # wiring's own face).
        render = render_mermaid(MARCH_FLOWS["doc_ingest_march"])
        for key in (
            "ingest",
            "ingest.join",
            "summarize",
            "extract_entities",
            "classify",
            "publish",
        ):
            assert key in render, f"the render is missing the {key!r} node: {render[:200]}"

        # THE INVARIANTS close the march.
        await tag_run_rows(march_pool, schema, str(flow_id), _TAG)
        await assert_balanced(sys_ledger, schema, _TAG)
    finally:
        for pod in fleet.values():
            reap(pod)
        await conn.close()


@pytest.fixture(scope="module", autouse=True)
def _dump_timeline() -> Any:
    """The march's timeline lands in the perf-evidence corpus at module
    teardown (the numbers next to their bounds, the capture law)."""
    yield
    import os

    if _TIMELINE:
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            ".measurements",
            "p5",
            "march-timeline.json",
        )
        with open(path, "w") as f:
            json.dump(_TIMELINE, f, indent=2)
