"""THE INTEGRATION'S E2E STRANGER SANITY — the composed system working.

The doc_ingest shape, end-to-end on ONE Postgres, through the PUBLIC
authoring surface only (a stranger's path — the docs' own example):

1. THE MAP OVER A SOURCE, PAGED — the T20 streaming source: the body is
   a paged generator, each ``ctx.emit_batch`` yield = ONE page's emit tx
   (the children + edges + the cursor checkpoint, one transaction), the
   source's own ``ctx.progress`` riding the same context.
2. THE CONDITIONAL CHAIN PER ITEM — the T20 router: each emitted record
   instantiates the declared Chain; the bodies route on TYPED outcomes
   (screen → enrich | manual_review).
3. SOME FAILURES — one record's body raises (the poisoned doc); the
   chain dies loudly, the run's derivation reads the ROWS.
4. THE RUN DERIVES FROM THE ROWS — the flow root's terminal state from
   the rows (no memory, no join), the report envelope the per-trace
   rollup the docs pin ({failed_chains, total}).
5. THE REPORT ENVELOPE + THE PROGRESS LINE + THE EXPLORER READ + THE
   MERMAID RENDER — FlowRunner.result, map_progress_line, run_display,
   compiled.mermaid.

The capture law: the run's full verdict prints; the caller tees it to a
timestamped .measurements/ file.

Run: python .measurements/integration/e2e_doc_ingest.py [--dsn DSN]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from enum import Enum
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import asyncpg

from taskq.migrate import apply_pending
from taskq.workflows import (
    DONE,
    Chain,
    FlowRunner,
    Route,
    Step,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)
from taskq.workflows._progress_read import map_progress_line, run_display
from taskq.workflows.engine import render_workflow_sql

DSN = "postgresql://taskq:taskq@localhost:5706/taskq"
SCHEMA = "int_e2e"

PAGES: list[list[dict[str, object]]] = [
    [{"doc_id": f"doc-{i}", "risk": i / 12} for i in range(1, 4)],
    [{"doc_id": f"doc-{i}", "risk": i / 12} for i in range(4, 7)],  # doc-4: risk 4/12 — the poisoned one
]
POISONED_DOC = "doc-4"


class ScreenOutcome(Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


class EnrichOutcome(Enum):
    OK = "ok"
    SPARSE = "sparse"


class ReviewOutcome(Enum):
    APPROVE = "approve"
    REJECT = "reject"


async def screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
    doc_id = str(item["doc_id"])
    await ctx.progress(30, f"screening {doc_id}", {"doc": doc_id})
    return ScreenOutcome.CLEAN if float(item["risk"]) < 0.5 else ScreenOutcome.FLAGGED


async def enrich(ctx: Any, item: dict[str, object]) -> EnrichOutcome:
    doc_id = str(item["doc_id"])
    if doc_id == POISONED_DOC:
        raise ValueError(f"the poisoned doc {doc_id} — the partial-failure arm")
    await ctx.progress(70, f"enriching {doc_id}", {"doc": doc_id})
    return EnrichOutcome.OK if float(item["risk"]) > 0.2 else EnrichOutcome.SPARSE


async def manual_review(ctx: Any, item: dict[str, object]) -> ReviewOutcome:
    return ReviewOutcome.APPROVE


DOC_INGEST_CHAIN = Chain(
    name="doc-ingest-enrichment",
    start="screen",
    steps={
        "screen": Step(
            body=screen,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.CLEAN: "enrich", ScreenOutcome.FLAGGED: "manual_review"}),
        ),
        "enrich": Step(
            body=enrich,
            outcomes=EnrichOutcome,
            route=Route({EnrichOutcome.OK: DONE, EnrichOutcome.SPARSE: DONE}),
        ),
        "manual_review": Step(
            body=manual_review,
            outcomes=ReviewOutcome,
            route=Route({ReviewOutcome.APPROVE: DONE, ReviewOutcome.REJECT: DONE}),
        ),
    },
)


async def source_body(ctx: Any) -> None:
    """The paged source: each yield = ONE page's emit tx (the T20 shape);
    the source's own progress rides the same context (the T21 shape)."""
    for page_no, page in enumerate(PAGES):
        await ctx.progress(10 + page_no * 40, f"page {page_no}", {"page": page_no})
        await ctx.emit_batch(
            [
                chain_start(DOC_INGEST_CHAIN, dict(item), map_index=int(str(item["doc_id"]).split("-")[1]), trace_id=str(item["doc_id"]))
                for item in page
            ],
            cursor={"page": page_no},
        )


app = WorkflowApp()


@app.workflow("doc_ingest")
def doc_ingest() -> object:
    src = chain_source(DOC_INGEST_CHAIN, source_body, key="doc_source")
    return build(src)


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=SCHEMA)
    wsql = render_workflow_sql(SCHEMA)
    pool = await asyncpg.create_pool(DSN)
    compiled = app.get("doc_ingest")

    print("== THE MERMAID RENDER (the compile-time emission)")
    print(compiled.mermaid())

    runner = FlowRunner(compiled, pool, SCHEMA)
    flow_id = await runner.create_flow()
    print(f"== THE FLOW CREATED: {flow_id}")
    verdict = await runner.drive(flow_id)
    print(f"== THE DRAIN: {verdict!r}")

    # THE RUN DERIVES FROM THE ROWS: the root's terminal state + the
    # per-trace rollup (the docs' report envelope: per-trace
    # bool_or(status='failed')).
    root = await conn.fetchrow(
        f'SELECT status, error_class FROM "{SCHEMA}".jobs WHERE id = $1', flow_id
    )
    rows = await conn.fetch(
        f"""SELECT trace_id, bool_or(status = 'failed') AS failed, count(*) AS steps
            FROM "{SCHEMA}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1 AND trace_id IS NOT NULL
            GROUP BY trace_id ORDER BY trace_id""",
        flow_id,
    )
    failed_chains = sum(1 for r in rows if r["failed"])
    envelope = {"failed_chains": failed_chains, "total": len(rows)}
    print(f"== THE ROOT (derived from the rows): status={root['status']} error_class={root['error_class']}")
    print(f"== THE REPORT ENVELOPE: {envelope}")
    for r in rows:
        print(f"   trace {r['trace_id']}: failed={r['failed']} steps={r['steps']}")

    # THE POISONED DOC's row: the loud failure, named.
    poisoned = await conn.fetchrow(
        f"""SELECT status, error_class, error_message FROM "{SCHEMA}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1 AND trace_id = $2
            AND step_key = 'enrich'""",
        flow_id,
        POISONED_DOC,
    )
    print(f"== THE POISONED DOC'S ROW: {dict(poisoned) if poisoned else None}")

    # THE TERMINAL READ (the runner's own door).
    result = await runner.result(flow_id)
    print(f"== FlowRunner.result: {result!r}")

    # THE PROGRESS LINE (the certified grouped read — the map's children).
    source_row = await conn.fetchval(
        f"SELECT id FROM \"{SCHEMA}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'doc_source'",
        flow_id,
    )
    line = await map_progress_line(pool, wsql, source_row)
    print(f"== THE PROGRESS LINE ({len(line)} rows):")
    for r in line:
        print(f"   {dict(r)}")

    # THE EXPLORER READ (the run explorer's display model — the ledger +
    # the state channel, ledger-derived at every connect).
    display = await run_display(pool, wsql, flow_id)
    print(f"== THE EXPLORER READ ({len(display)} nodes):")
    for k, v in sorted(display.items()):
        print(f"   {k}: {v}")

    # THE STREAM RING: the source's coalesced progress + the autos.
    src_stream = await conn.fetch(
        f"""SELECT class, kind, payload FROM "{SCHEMA}".wf_node_stream
            WHERE node_id = $1 ORDER BY seq""",
        source_row,
    )
    print(f"== THE SOURCE'S STREAM RING ({len(src_stream)} events, coalesced):")
    for r in src_stream:
        print(f"   {r['class']}/{r['kind']}: {r['payload']}")

    ok = (
        root["status"] == "failed"
        and envelope == {"failed_chains": 1, "total": len(PAGES) * len(PAGES[0])}
        and poisoned is not None
        and poisoned["status"] == "failed"
        and verdict == "terminal"
        and len(display) > 0
        and len(src_stream) > 0
    )
    print(f"== THE E2E VERDICT: {'THE COMPOSED SYSTEM WORKS' if ok else 'RED — see above'}")
    await pool.close()
    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DSN)
    args = parser.parse_args()
    DSN = args.dsn
    asyncio.run(main())
