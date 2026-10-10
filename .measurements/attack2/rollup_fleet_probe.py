"""ATTACK2: the rollup's index-serviceability at a REAL fleet shape —
many flows + a large vanilla population, ANALYZEd. The T08 cost-gate pin
measures a table that contains ONLY the measured flow's own rows; this
probe reproduces the production shape (fleet table, one run among many)
and asks whether jobs_wf_flow_nodes_idx can serve the grouped rollup at
all (the expression the index keys is TEXT; the query casts to uuid)."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, "src")

import asyncpg

DSN = "postgresql://taskq:taskq@localhost:5691/taskq"
SCHEMA = "attack2"
OUT = Path(".measurements/attack2/rollup-fleet-explain.txt")


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    # a second flow among many, on a fleet-shaped table (the 20k vanilla
    # rows + the first probe's 1001-row flow are still there)
    flow_b = "22222222-2222-2222-2222-222222222222"
    await conn.execute(
        f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind,
             status, step_key, metadata, scheduled_at)
             SELECT gen_random_uuid(), 'wf', 'default', '{{}}', 3, 'transient',
             (ARRAY['succeeded','running','pending']::text[])[1 + (g % 3)]::"{SCHEMA}".job_status, 'fbc',
             jsonb_build_object('flow_id', $1::text), now() - interval '1 hour'
             FROM generate_series(1, 500) g""",
        flow_b,
    )
    await conn.execute("ANALYZE attack2.jobs")
    queries = {
        "rollup (uuid cast — the shipped shape)": f"""EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT status, count(*) FROM {SCHEMA}.jobs
            WHERE (metadata->>'flow_id')::uuid = '{flow_b}'::uuid GROUP BY status""",
        "rollup (text equality — the index's own shape)": f"""EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT status, count(*) FROM {SCHEMA}.jobs
            WHERE metadata->>'flow_id' = '{flow_b}' GROUP BY status""",
        "nodes (uuid cast — the shipped shape)": f"""EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT id FROM {SCHEMA}.jobs
            WHERE (metadata->>'flow_id')::uuid = '{flow_b}'::uuid AND step_key <> '__flow__'""",
        "nodes (text equality — the index's own shape)": f"""EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT id FROM {SCHEMA}.jobs
            WHERE metadata->>'flow_id' = '{flow_b}' AND step_key <> '__flow__'""",
    }
    report: list[str] = []
    for name, q in queries.items():
        rows = await conn.fetch(q)
        raw = rows[0][0]
        plan = json.loads(raw) if isinstance(raw, str) else raw

        def walk(node: dict[str, object], depth: int) -> list[str]:
            line = (
                "  " * depth
                + f"{node.get('Node Type')}"
                + (f" on {node.get('Relation Name')}" if node.get("Relation Name") else "")
                + (f" idx={node.get('Index Name')}" if node.get("Index Name") else "")
                + (f" rows={node.get('Actual Rows')}" if "Actual Rows" in node else "")
                + (f" time={node.get('Actual Total Time'):.3f}ms" if "Actual Total Time" in node else "")
            )
            out = [line]
            for child in node.get("Plans", []) or []:
                out.extend(walk(child, depth + 1))  # pyright: ignore[reportArgumentType]
            return out

        tree = "\n".join(walk(plan[0]["Plan"], 0))  # pyright: ignore[reportIndexType,reportUnknownArgumentType]
        report.append(f"===== {name}\n{tree}\n")
    OUT.write_text("\n".join(report))
    print(OUT.read_text())


asyncio.run(main())
