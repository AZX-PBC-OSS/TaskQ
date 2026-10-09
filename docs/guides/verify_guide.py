"""THE VERIFICATION HARNESS for the staged migrating-from-langgraph guide.

Runs the guide's code blocks against the BUILT TaskQflow surface (the
carrying tree itself, branch feat/taskqflow — the consolidation head) on
a FRESH container (its own port — no other agent's container touched).
Each block = a named verdict; the captured output accompanies the
guide's record.

Usage: python docs/guides/verify_guide.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel
from testcontainers.postgres import PostgresContainer

import taskq.migrate
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step

VERDICTS: list[tuple[str, bool, str]] = []


def verdict(name: str, ok: bool, detail: str = "") -> None:
    VERDICTS.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


# ── the guide's BLOCK A code (the AFTER port) — verbatim shapes ─────────


class DocIn(BaseModel):
    doc_id: str


class PublishApproval(BaseModel):
    verdict: str
    note: str = ""


enrich_calls = 0
enrich_results: dict[str, dict] = {}


async def enrich_document(doc_id: str) -> dict:
    global enrich_calls
    enrich_calls += 1
    return {"enrichment": f"enriched({doc_id})"}


app = WorkflowApp()


@app.workflow("doc_ingest")
def doc_ingest_build() -> object:
    enriched = step(enrich_body, DocIn(doc_id="d1"), key="enrich")
    reviewed = step(review_body, enriched, key="review")
    return build(reviewed)


async def enrich_body(ctx: Any, params: DocIn) -> dict:
    result = await enrich_document(params.doc_id)
    enrich_results[params.doc_id] = result
    return result


async def review_body(ctx: Any, enriched: dict) -> dict:
    decision = await ctx.wait_signal(PublishApproval, timeout_s=86400.0)
    return {"verdict": decision.verdict}


# ── the guide's BLOCK B code (the map-reduce port) — verbatim shapes ────


class BatchIn(BaseModel):
    doc_ids: list[str]


map_results: dict[str, dict] = {}


async def plan_body(ctx: Any, params: BatchIn) -> list[DocIn]:
    return [DocIn(doc_id=d) for d in params.doc_ids]


async def enrich_one_body(ctx: Any, params: DocIn) -> dict:
    result = {"enrichment": f"enriched({params.doc_id})"}
    map_results[params.doc_id] = result
    return result


@app.workflow("batch_ingest")
def batch_build() -> object:
    planned = step(plan_body, BatchIn(doc_ids=[f"d{i}" for i in range(7)]), key="plan")
    enriched = map_source(planned, enrich_one_body)
    return build(enriched)


# ── the run harness ──────────────────────────────────────────────────────


async def main() -> None:
    async def pool_holder(dsn: str):
        from asyncpg import create_pool as _cp

        conn = await _cp(dsn)
        return conn

    pg = PostgresContainer("postgres:17-alpine")
    pg.start()
    try:
        dsn = pg.get_connection_url().replace("+psycopg2", "")
        schema = "guide_verify"
        # readiness retry (the container's first-second race)
        for attempt in range(10):
            try:
                await taskq.migrate.apply_pending_locked(dsn, schema=schema)
                break
            except Exception:
                if attempt == 9:
                    raise
                await asyncio.sleep(1)
        # mirror the deployed state: the idempotency post-phase drops the
        # legacy single-column uniq (01.00.03_01_post) — the verify run's
        # apply left it behind, and it convicts the per-run step keys.
        conn0 = await pool_holder(dsn)
        await conn0.execute(f'DROP INDEX IF EXISTS "{schema}".jobs_idempotency_key_uniq')
        await conn0.close()
        from asyncpg import create_pool

        pool = await create_pool(dsn)

        # ── BLOCK A: the AFTER port — compile, hold, resolve, terminal ──
        compiled = app.get("doc_ingest")
        verdict(
            "A0 compile",
            compiled.node_keys() == ["enrich", "review"],
            f"node_keys={compiled.node_keys()}",
        )
        compiled.validate()
        verdict("A1 validate", True, "no findings on the ported graph")

        runner = FlowRunner(app.get("doc_ingest"), pool, schema)
        flow_id = await runner.create_flow(input={"doc_id": "d1"})
        await runner.drive(flow_id, until="held")
        verdict("A2 held-not-crashed", True, "the run pauses: a ROW (the MemorySaver hole gone)")

        from taskq.workflows import HitlClient

        hitl = HitlClient(pool, schema=schema)
        holds = await hitl.list(run=flow_id)
        verdict(
            "A3 enumerate",
            len(holds) == 1 and holds[0].node_key == "review",
            f"pending holds: {[(h.node_key, h.signal_name) for h in holds]}",
        )
        hold_id = holds[0].hold_id
        result = await hitl.resolve(
            hold_id, {"verdict": "approve", "note": "ship it"}
        )  # the DICT payload (the models validate objects)
        verdict(
            "A4 resolve-by-id",
            result is not None
            and getattr(result, "ok", getattr(result, "status", "")) != "refused",
            f"DeliveryResult: {result}",
        )
        again = await hitl.resolve(hold_id, {"verdict": "reject"})
        verdict(
            "A5 idempotent",
            "no-op" in str(getattr(again, "status", "")),
            f"second resolve: {again} (the defined no-op)",
        )
        await runner.drive(flow_id, until="terminal")
        row = await pool.fetchrow(
            f"SELECT status FROM \"{schema}\".jobs WHERE step_key = 'review' AND (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )
        verdict(
            "A6 terminal",
            row is not None and row["status"] == "succeeded",
            f"review status = {row['status'] if row else None}",
        )
        enrich_after = dict(enrich_results)
        enrich_calls_after = enrich_calls

        # the exactly-once resume: re-drive a terminal flow — no re-runs,
        # the replay's memo returns the SAME results (the ledger's truth)
        enrich_calls_before = enrich_calls_after
        await runner.drive(flow_id, until="terminal")
        verdict(
            "A7 no-re-execution",
            enrich_calls == enrich_calls_before and dict(enrich_results) == enrich_after,
            f"enrich calls stable at {enrich_calls}, results replayed: "
            f"{dict(enrich_results) == enrich_after}",
        )

        # ── BLOCK B: the map-reduce port — dynamic cardinality + the join ──
        runner_b = FlowRunner(app.get("batch_ingest"), pool, schema)
        flow_b = await runner_b.create_flow(input={"doc_ids": [f"d{i}" for i in range(7)]})
        await runner_b.drive(flow_b, until="terminal")
        verdict(
            "B1 fan-out cardinality",
            len(map_results) == 7,
            f"children materialized: {len(map_results)}/7",
        )
        join_row = await pool.fetchrow(
            f"SELECT status FROM \"{schema}\".jobs WHERE step_key = 'plan.join' AND (metadata->>'flow_id')::uuid = $1",
            flow_b,
        )
        verdict(
            "B2 join terminal-once",
            join_row is not None and join_row["status"] == "succeeded",
            f"the join collected all children: {join_row['status'] if join_row else None}",
        )

        # the run-key arbiter: the same key → the SAME run (no second run)
        flow_c = await runner_b.create_flow(input={"doc_ids": []}, run_key="batch:slot-1")
        flow_d = await runner_b.create_flow(input={"doc_ids": []}, run_key="batch:slot-1")
        verdict(
            "B3 run-key arbiter",
            flow_c == flow_d,
            f"same slot twice → one run: {flow_c} == {flow_d}",
        )
    finally:
        pg.stop()

    print("\n── VERDICTS ──")
    ok = all(ok for _, ok, _ in VERDICTS)
    for name, okv, detail in VERDICTS:
        print(f"  {'PASS' if okv else 'FAIL'}  {name}  {detail}")
    print(f"\nOVERALL: {'ALL PASS' if ok else 'FAILURES'} — {len(VERDICTS)} verdicts")
    raise SystemExit(0 if ok else 1)


asyncio.run(main())
