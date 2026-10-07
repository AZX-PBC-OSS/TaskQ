"""FIX2 (the phase-2 attack's H3b): the flow-link index BEFORE vs AFTER
the uuid-cast rebuild — the fleet shape, both rounds measured.

The representation under test: the flow_id linkage is the uuid STRING in
metadata.flow_id; every shipped read compares the uuid it names
((metadata->>'flow_id')::uuid = $1::uuid). 01.00.25_01 keyed the index on
the RAW TEXT key — the cast between expression and comparison broke the
match. This probe builds the fleet (40 flows x 500 nodes + 200k vanilla
rows), ANALYZEs, measures the four flow-scoped queries + the
fleet-sampler read against BOTH index shapes, and records the plans.

Run:  .venv/bin/python .measurements/fix2/index_fleet_probe.py
(owns the fix2 schema on the fix2-pg container @ :5692; the container is
the dev loop's own and is destroyed after).
"""
from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, "src")
sys.path.insert(0, ".")

import asyncpg  # noqa: E402

from taskq.migrate import apply_pending  # noqa: E402
from taskq.workflows.engine import render_workflow_sql  # noqa: E402

DSN = "postgresql://taskq:taskq@localhost:5692/taskq"
SCHEMA = "fix2"
HERE = Path(__file__).parent
OUT_BEFORE = HERE / "index-before-text-index.txt"
OUT_AFTER = HERE / "index-after-uuid-index.txt"
JSON_OUT = HERE / "index-fleet-bands.json"

FLOWS = 40
NODES_PER_FLOW = 500
VANILLA = 200_000
SAMPLES = 7

THE_FLOW = FLOWS - 1  # the measured run: one among many


def _walk(node: dict[str, object], depth: int) -> list[str]:
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
        out.extend(_walk(child, depth + 1))  # pyright: ignore[reportArgumentType]
    return out


async def seed(conn: asyncpg.Connection) -> list[str]:
    """The fleet: FLOWS workflow runs + the vanilla population."""
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.execute(f'CREATE SCHEMA "{SCHEMA}"')
    await apply_pending(conn, schema=SCHEMA)

    flow_ids = [f"11111111-0000-0000-0000-{i:012d}" for i in range(FLOWS)]
    wsql = render_workflow_sql(SCHEMA)
    meta = json.dumps({"flow_id": "FLOW", "blocking_reason": "join"})
    for f, flow_id in enumerate(flow_ids):
        # the root + one node population per run (statuses mixed)
        await conn.execute(
            f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts,
                 retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key)
                 VALUES ($1::uuid, 'flow', 'default', '{{}}', 3, 'transient', 'running',
                 '__flow__', jsonb_build_object('flow_id', $1::text), 'workflow-run', $2)""",
            flow_id,
            f"flow:{flow_id}",
        )
        await conn.execute(
            f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts,
                 retry_kind, status, step_key, deps_pending, metadata,
                 idempotency_scope, idempotency_key)
                 SELECT gen_random_uuid(), 'wf', 'default', '{{}}', 3, 'transient',
                 (ARRAY['succeeded','running','pending','failed']::text[])[1 + (g % 4)]::{SCHEMA}.job_status,
                 'c', 0,
                 jsonb_build_object('flow_id', $1::text, 'blocking_reason', 'join'),
                 'workflow:' || $1::text, 'wf:' || $1::text || ':' || g
                 FROM generate_series(1, $2) g""",
            flow_id,
            NODES_PER_FLOW,
        )
    _ = wsql, meta
    # THE VANILLA POPULATION: production's fleet table is mostly NOT
    # workflow rows — the seq-scan shape's cost is proportional to THIS.
    await conn.execute(
        f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts,
             retry_kind, status, step_key, scheduled_at, idempotency_scope, idempotency_key)
             SELECT gen_random_uuid(), 'van', 'default', '{{}}', 3, 'transient',
             (ARRAY['succeeded','failed']::text[])[1 + (g % 2)]::{SCHEMA}.job_status, 'v',
             now() - interval '1 hour', 'scope', 'van-' || g
             FROM generate_series(1, $1) g""",
        VANILLA,
    )
    return flow_ids


async def measure(conn: asyncpg.Connection, flow_ids: list[str], label: str, out: Path) -> dict[str, object]:
    wsql = render_workflow_sql(SCHEMA)
    flow_id = flow_ids[THE_FLOW]
    report: list[str] = [f"===== {label} — the fleet shape ({FLOWS} runs x {NODES_PER_FLOW} nodes + {VANILLA} vanilla rows)\n"]
    bands: dict[str, object] = {}

    async def timed(name: str, sql: str, *args: object, explain: str | None = None) -> None:
        # warm + the plan
        await conn.fetch(sql, *args)
        if explain:
            plan_rows = await conn.fetch(f"EXPLAIN (ANALYZE, FORMAT JSON) {explain}", *args)
            raw = plan_rows[0][0]
            plan = json.loads(raw) if isinstance(raw, str) else raw
            tree = "\n".join(_walk(plan[0]["Plan"], 0))  # pyright: ignore[reportIndexType]
            report.append(f"----- {name}: the plan\n{tree}\n")
        samples: list[float] = []
        for _ in range(SAMPLES):
            t0 = time.perf_counter_ns()
            await conn.fetch(sql, *args)
            samples.append((time.perf_counter_ns() - t0) / 1e6)
        p50 = statistics.median(samples)
        bands[name] = {"p50_ms": round(p50, 3), "samples_ms": [round(s, 3) for s in samples]}
        report.append(f"----- {name}: p50 {p50:.3f} ms (samples {[round(s, 3) for s in samples]})\n")

    # 1. the grouped rollup (the status panel + the gauge's read class)
    await timed("rollup", wsql.workflow_rollup, flow_id, explain=wsql.workflow_rollup)
    # 2. the per-node read (the admin's run page)
    await timed("nodes", wsql.workflow_nodes, flow_id, explain=wsql.workflow_nodes)
    # 3. the maintenance leg (the roots' per-flow node scan — batch of 200)
    await timed(
        "root-maintain",
        wsql.workflow_root_maintain,
        200,
        explain=wsql.workflow_root_maintain,
    )
    # 4. the wf-progress gauge's fleet sampler (the leader's metrics tick)
    from taskq.worker._leader_shared import _QUERY_WF_PROGRESS_SQL_TEMPLATE  # pyright: ignore[reportPrivateImport]

    gauge_sql = _QUERY_WF_PROGRESS_SQL_TEMPLATE.format(schema=SCHEMA)
    await timed("wf-progress-sampler", gauge_sql, explain=gauge_sql)

    out.write_text("\n".join(report))
    print(out.read_text())
    return bands


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    flow_ids = await seed(conn)
    await conn.execute(f'ANALYZE "{SCHEMA}".jobs')
    await conn.execute(f'VACUUM (ANALYZE) "{SCHEMA}".jobs')

    total = await conn.fetchval(f'SELECT count(*) FROM "{SCHEMA}".jobs')
    print(f"fleet rows: {total}")

    # The BEFORE round must measure the SHIPPED-BEFORE-CURE index shape
    # (the migrations bundle now carries the cure — force 01.00.25_01's
    # shape for this round: the raw-TEXT key, the step_key-only partial).
    await conn.execute(f'DROP INDEX IF EXISTS "{SCHEMA}".jobs_wf_flow_nodes_idx')
    await conn.execute(
        f'CREATE INDEX jobs_wf_flow_nodes_idx ON "{SCHEMA}".jobs '
        f"((metadata->>'flow_id'), status) WHERE step_key IS NOT NULL"
    )
    await conn.execute(f'VACUUM (ANALYZE) "{SCHEMA}".jobs')

    before = await measure(conn, flow_ids, "BEFORE (01.00.25_01 — the raw-TEXT index key)", OUT_BEFORE)

    # THE CURE, applied inline (the migration file's own statements — the
    # dev loop's schema already carries it after this; the probe is the
    # measurement, the migration is the shipped cure).
    await conn.execute(f'DROP INDEX IF EXISTS "{SCHEMA}".jobs_wf_flow_nodes_idx')
    await conn.execute(
        f'CREATE INDEX jobs_wf_flow_nodes_idx ON "{SCHEMA}".jobs '
        f"(((metadata->>'flow_id')::uuid), status) WHERE step_key IS NOT NULL"
    )
    await conn.execute(f'VACUUM (ANALYZE) "{SCHEMA}".jobs')

    after = await measure(conn, flow_ids, "AFTER (01.00.25_02 — the uuid-cast index)", OUT_AFTER)

    JSON_OUT.write_text(
        json.dumps(
            {
                "fleet": {
                    "flows": FLOWS,
                    "nodes_per_flow": NODES_PER_FLOW,
                    "vanilla_rows": VANILLA,
                    "total_rows": total,
                },
                "before_text_index": before,
                "after_uuid_index": after,
                "representation": (
                    "the flow_id linkage is the uuid STRING in metadata.flow_id; "
                    "every read casts it to uuid; the index expression carries "
                    "the same cast (01.00.25_02) — index + reads + stamp aligned "
                    "on ONE representation"
                ),
            },
            indent=2,
        )
    )
    await conn.close()


asyncio.run(main())
