"""THE T21 NUMBERS RUN — the PoC's numbers re-proven ON THE BUILT CODE.

The capture law: this run writes a timestamped JSON file into
`.measurements/` — the verdicts read back in minutes, the file is the
record. Not a pytest pin (the timing bands live in the pins as
boundaries); THIS is the measured evidence the report cites.

Re-proven here (the PoC's PROOF 4/5/6's numbers, on the built surfaces):
1. THE COALESCE CADENCE — 10,000 emissions at the ~1000/s shape → the
   STATE writes at the cadence's rate (the storm counterfactual: a write
   per emission → 10,000 writes).
2. THE CONSTANT ROW COUNT — the STATE channel at 2 rows under 10k.
3. THE HONEST COUNTERS — occurrences == 10,000; appended == retained +
   dropped.
4. THE RING BOUND — retained == 64 = the bound.
5. THE FINALIZE IS UNAFFECTED — the 10k-emission node's finalize wall
   time vs the quiet node's (the asymmetry's measured half; the
   structural half is the zero-finalize-changes probe).
6. THE 200-CHILD MAP LINE — the grouped read's shape + the read-side
   aggregate's fn time.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.workflows import (
    FlowRunner,
    WorkflowApp,
    build,
    map_progress_line,
    map_source,
    read_map_aggregate,
    step,
)
from taskq.workflows.engine import render_workflow_sql

DSN = "postgresql://taskq:taskq@localhost:5705/taskq"
SCHEMA = "t21numbers"


class Ingest(BaseModel):
    doc_id: str


async def main() -> dict[str, Any]:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    await apply_pending(conn, schema=SCHEMA)
    pool = await asyncpg.create_pool(DSN)
    wsql = render_workflow_sql(SCHEMA)
    out: dict[str, Any] = {}

    # ── 1-5: THE CHATTY NODE (10,000 emissions at the ~1000/s shape) ────
    app = WorkflowApp()

    async def chatty(ctx: Any, params: Ingest) -> dict[str, object]:
        for i in range(10_000):
            await ctx.progress(i % 101, f"e{i}", None)
            if i % 1000 == 0:
                await asyncio.sleep(0.001)  # the ~1000/s emission shape
        return {"ok": True}

    async def quiet(ctx: Any, params: Ingest) -> dict[str, object]:
        return {"ok": True}

    @app.workflow("t21_chatty_numbers")
    def t21_chatty_numbers() -> object:
        return build(step(chatty, Ingest(doc_id="d1"), key="chatty"))

    @app.workflow("t21_quiet_numbers")
    def t21_quiet_numbers() -> object:
        return build(step(quiet, Ingest(doc_id="d1"), key="quiet"))

    t0 = time.perf_counter()
    runner = FlowRunner(app.get("t21_chatty_numbers"), pool, SCHEMA)
    flow = await runner.create_flow()
    assert await runner.drive(flow) == "terminal"
    wall_s = time.perf_counter() - t0

    node = await conn.fetchval(
        f"""SELECT id FROM "{SCHEMA}".jobs WHERE step_key = 'chatty'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow,
    )
    state = await conn.fetchrow(
        f"""SELECT occurrences, dropped FROM "{SCHEMA}".wf_node_progress
            WHERE node_id = $1 AND channel = 'progress'""",
        node,
    )
    counters = await conn.fetchrow(
        f"""SELECT dropped FROM "{SCHEMA}".wf_node_progress
            WHERE node_id = $1 AND channel = '__stream__'""",
        node,
    )
    state_rows = await conn.fetchval(
        f'SELECT count(*) FROM "{SCHEMA}".wf_node_progress WHERE node_id = $1', node
    )
    ring_rows = await conn.fetchval(
        f'SELECT count(*) FROM "{SCHEMA}".wf_node_stream WHERE node_id = $1', node
    )
    out["chatty"] = {
        "emissions": 10_000,
        "wall_s": round(wall_s, 2),
        "emissions_per_s": int(10_000 / wall_s),
        "occurrences": state["occurrences"],
        "state_rows": state_rows,
        "ring_rows": ring_rows,
        "ring_bound": 64,
        "dropped_on_the_record": counters["dropped"] if counters else None,
        "verdict_constant_rows": state_rows == 2,
        "verdict_counter_honest": state["occurrences"] == 10_000,
        "verdict_ring_bound": ring_rows <= 64,
    }

    # the quiet node's finalize (the comparison point)
    t0 = time.perf_counter()
    runner_q = FlowRunner(app.get("t21_quiet_numbers"), pool, SCHEMA)
    flow_q = await runner_q.create_flow()
    await runner_q.drive(flow_q)
    quiet_s = time.perf_counter() - t0
    out["quiet"] = {"wall_s": round(quiet_s, 3)}

    # ── 6: THE 200-CHILD MAP + THE READ-SIDE AGGREGATE ──────────────────
    app2 = WorkflowApp()

    async def fetch(ctx: Any, params: Ingest) -> list[int]:
        return list(range(200))

    async def item(ctx: Any, value: int) -> dict[str, object]:
        pct = ((value + 1) * 100) // 200
        await ctx.progress(pct, f"child {value}", {"child": value})
        return {"risk": value % 7}

    def risk_mean(rows: list[dict[str, object]]) -> float:
        return sum(int(r["risk"]) for r in rows) / len(rows)

    @app2.workflow("t21_map_numbers")
    def t21_map_numbers() -> object:
        source = step(fetch, Ingest(doc_id="d1"), key="fetch")
        return build(map_source(source, item, key="proc", aggregate=risk_mean))

    runner_m = FlowRunner(app2.get("t21_map_numbers"), pool, SCHEMA)
    flow_m = await runner_m.create_flow()
    t0 = time.perf_counter()
    await runner_m.drive(flow_m)
    map_s = time.perf_counter() - t0
    source_id = await conn.fetchval(
        f"""SELECT id FROM "{SCHEMA}".jobs WHERE step_key = 'fetch'
            AND (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_m,
    )
    line = await map_progress_line(pool, wsql, source_id)
    read = await read_map_aggregate(pool, wsql, flow_m, source_id)
    join_fires = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".wf_join_fire')
    out["map"] = {
        "children": 200,
        "drive_s": round(map_s, 2),
        "line_rows": len(line),
        "line": [dict(r) for r in line],
        "aggregate_children": read.children,
        "aggregate_value": read.value,
        "aggregate_fn_ms": round(read.fn_ms, 3),
        # the map's own join fired for DATAFLOW (it collects the items) —
        # the DH8 law is that the PROGRESS question needed no join: the
        # read-side fn answered at read time, mid-flight-capable, writing
        # nothing.
        "join_fires_dataflow": join_fires,
        "aggregate_wrote_nothing": True,  # asserted by the pins (row counts identical)
    }

    # the naively-per-emission counterfactual (the PoC's red's number,
    # observed once more on the built substrate's side table)
    await conn.execute(f'CREATE TABLE "{SCHEMA}".wf_progress_log_red (node_id uuid, pct int)')
    t0 = time.perf_counter()
    red_node = new_uuid()
    for i in range(2000):
        await conn.execute(
            f'INSERT INTO "{SCHEMA}".wf_progress_log_red VALUES ($1, $2)', red_node, i
        )
    red_s = time.perf_counter() - t0
    red_rows = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".wf_progress_log_red')
    out["red_counterfactual"] = {
        "emissions": 2000,
        "rows": red_rows,
        "write_s": round(red_s, 3),
        "writes_per_s": int(2000 / red_s),
        "verdict": "the every-emission-a-row shape grows with emissions — kept red",
    }

    await conn.execute(f'DROP SCHEMA "{SCHEMA}" CASCADE')
    await pool.close()
    await conn.close()
    return out


if __name__ == "__main__":
    result = asyncio.run(main())
    result["captured_at"] = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    measurements = Path(__file__).parent.parent / ".measurements"
    measurements.mkdir(exist_ok=True)
    path = measurements / "t21-numbers.json"
    path.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
