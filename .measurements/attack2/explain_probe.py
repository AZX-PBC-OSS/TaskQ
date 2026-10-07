"""ATTACK2 §6: the new phase-2 statements' plans on the attacker's own PG
container (:5691). The EXPLAIN + index-serviceability probe for every
statement constant the four commits added. Output captured to
.measurements/attack2/explain-hot-statements-attack2.txt (this run)."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, "src")

import asyncpg

from taskq.workflows.engine import render_workflow_sql

SCHEMA = "attack2"
DSN = "postgresql://taskq:taskq@localhost:5691/taskq"
OUT = Path(".measurements/attack2/explain-hot-statements-attack2.txt")

# Seed: a 1000-edge fail_closed fan-in + one failed parent + peers, so the
# plans see real cardinalities; plus 20k vanilla rows for the table shape.
SEED = f"""
INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind,
    status, attempt, locked_by_worker, lock_expires_at, claim_epoch, step_key, deps_pending,
    metadata, scheduled_at)
SELECT gen_random_uuid(), 'wf', 'default', '{{}}', 3, 'transient',
    (ARRAY['succeeded','failed','running','pending','scheduled']::text[])[1 + (g % 5)],
    1, gen_random_uuid(), now() + interval '90 seconds', 0, 'c',
    0,
    jsonb_build_object('flow_id', '11111111-1111-1111-1111-111111111111'),
    now() - interval '1 hour'
FROM generate_series(1, 20000) g;
"""

TERMINAL_SET = "('abandoned','cancelled','crashed','failed','succeeded')"


def stmt(name: str, sql: str) -> str:
    return f"\n-- ===== {name} =====\n{sql}\n"


async def main() -> None:
    wsql = render_workflow_sql(SCHEMA)
    conn = await asyncpg.connect(DSN)

    flow_id = "11111111-1111-1111-1111-111111111111"
    # one flow: 1000 parents + a failed one + a join
    join_id = await conn.fetchval(
        f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind,
             status, step_key, deps_pending, metadata, scheduled_at)
             VALUES (gen_random_uuid(), 'wf', 'default', '{{}}', 3, 'transient',
             'pending', 'reduce', 1000,
             jsonb_build_object('flow_id', $1::text, 'blocking_reason', 'join'),
             now() - interval '1 hour') RETURNING id""",
        flow_id,
    )
    parents = await conn.fetch(
        f"""INSERT INTO "{SCHEMA}".jobs (id, actor, queue, payload, max_attempts, retry_kind,
              status, step_key, metadata, scheduled_at)
              SELECT gen_random_uuid(), 'wf', 'default', '{{}}', 3, 'transient', 'running', 'c',
              jsonb_build_object('flow_id', $1::text), now() - interval '1 hour'
              FROM generate_series(1,1000) RETURNING id""",
        flow_id,
    )
    await conn.executemany(
        f'INSERT INTO "{SCHEMA}".wf_edge (child_id, parent_id, flow_id, failure_policy) '
        "VALUES ($1, $2, $3, 'fail_closed')",
        [(join_id, p["id"], flow_id) for p in parents],
    )

    out: list[str] = []
    out.append(stmt(
        "FAIL_CLOSED_CASCADE_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH edges AS (
    SELECT e.child_id FROM {SCHEMA}.wf_edge e
    WHERE e.parent_id = '{parents[0]['id']}'::uuid AND e.failure_policy = 'fail_closed'
),
blocked AS (
    UPDATE {SCHEMA}.jobs j SET metadata = j.metadata || '{{"k":1}}'::jsonb
    FROM edges WHERE j.id = edges.child_id AND j.status = 'pending' AND j.deps_pending > 0
      AND j.metadata @> '{{"blocking_reason": "join"}}'::jsonb
      AND NOT j.metadata @> '{{"blocking_reason": "failed_parent"}}'::jsonb
    RETURNING j.id
),
peers AS (
    UPDATE {SCHEMA}.jobs p SET status = 'cancelled', finished_at = clock_timestamp(),
        error_class = 'CancelledByPeerFailure', metadata = p.metadata || '{{"k":1}}'::jsonb
    FROM edges e JOIN {SCHEMA}.wf_edge sib ON sib.child_id = e.child_id
    WHERE p.id = sib.parent_id AND p.id <> '{parents[0]['id']}'::uuid
      AND p.status IN ('pending','scheduled','running')
      AND NOT (p.deps_pending > 0 AND p.metadata @> '{{"blocking_reason": "join"}}'::jsonb)
    RETURNING p.id
)
SELECT (SELECT count(*) FROM blocked) b, (SELECT count(*) FROM peers) p""",
    ))
    out.append(stmt(
        "DECREMENT_ABSORBED_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH flow_alive AS (SELECT 1 AS ok FROM {SCHEMA}.jobs f WHERE f.id = '{flow_id}'::uuid
                    AND f.status NOT IN {TERMINAL_SET})
UPDATE {SCHEMA}.jobs j SET deps_pending = j.deps_pending - 1
FROM {SCHEMA}.wf_edge e
WHERE e.parent_id = '{parents[1]['id']}'::uuid AND e.child_id = j.id
  AND e.failure_policy IN ('collect','maybe') AND j.deps_pending > 0 AND j.status = 'pending'
  AND EXISTS (SELECT 1 FROM flow_alive)
RETURNING j.id, j.deps_pending""",
    ))
    out.append(stmt(
        "COLLECT_FAN_IN_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH history AS (
    SELECT jsonb_agg(jsonb_build_object('attempt', l.attempt) ORDER BY l.attempt) AS attempts
    FROM {SCHEMA}.wf_step_ledger l
    WHERE l.flow_id = '{flow_id}'::uuid AND l.step_key = 'c'
      AND COALESCE(l.map_index,-1) = COALESCE(NULL::smallint,-1)
      AND l.status IN ('succeeded','failed')
)
SELECT j.id AS join_job_id, e.failure_policy AS policy,
       COALESCE((SELECT history.attempts FROM history), '[]'::jsonb) AS attempts
FROM {SCHEMA}.jobs j JOIN {SCHEMA}.wf_edge e ON e.child_id = j.id
WHERE e.parent_id = '{parents[2]['id']}'::uuid
  AND e.failure_policy IN ('collect','maybe')
  AND j.status = 'pending' AND j.deps_pending > 0""",
    ))
    out.append(stmt(
        "COLLECT_FAN_IN_APPEND_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
UPDATE {SCHEMA}.jobs j SET metadata = jsonb_set(j.metadata, '{{failures}}', '[]'::jsonb, true)
WHERE j.id = '{join_id}'::uuid RETURNING j.id""",
    ))
    out.append(stmt(
        "WORKFLOW_ROLLUP_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT status, count(*) AS count FROM {SCHEMA}.jobs
WHERE (metadata->>'flow_id')::uuid = '{flow_id}'::uuid GROUP BY status""",
    ))
    out.append(stmt(
        "WORKFLOW_NODES_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT j.id, j.step_key, j.status, j.deps_pending,
       j.metadata->>'blocking_reason' AS blocking_reason,
       EXISTS (SELECT 1 FROM {SCHEMA}.wf_edge e WHERE e.parent_id = j.id
               AND e.failure_policy IN ('collect','maybe')) AS absorbed,
       j.metadata->>'error' AS error_jsonb, j.error_class, j.error_message
FROM {SCHEMA}.jobs j
WHERE (j.metadata->>'flow_id')::uuid = '{flow_id}'::uuid AND j.step_key <> '__flow__'
ORDER BY j.id""",
    ))
    out.append(stmt(
        "WORKFLOW_ROOT_MAINTAIN_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH roots AS (
    SELECT f.id FROM {SCHEMA}.jobs f WHERE f.step_key = '__flow__' AND f.status = 'running'
    ORDER BY f.id LIMIT 200 FOR UPDATE SKIP LOCKED
),
per_flow AS (
    SELECT r.id AS flow_id,
        bool_or(n.status = 'failed' AND NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.wf_edge e WHERE e.parent_id = n.id
            AND e.failure_policy IN ('collect','maybe'))) AS has_failed,
        bool_or(n.status = 'cancelled') AS has_cancelled,
        bool_or(n.status IN ('pending','running','scheduled','crashed','abandoned')) AS has_live
    FROM roots r JOIN {SCHEMA}.jobs n
      ON (n.metadata->>'flow_id')::uuid = r.id AND n.step_key <> '__flow__'
    GROUP BY r.id
),
maintained AS (
    UPDATE {SCHEMA}.jobs f SET status = 'failed'::"{SCHEMA}".job_status, finished_at = clock_timestamp()
    FROM per_flow pf WHERE f.id = pf.flow_id AND NOT pf.has_live RETURNING f.id
)
SELECT count(*)::int AS roots_updated FROM maintained""",
    ))
    out.append(stmt(
        "WORKFLOW_MAP_PROGRESS_SQL",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT c.step_key, count(*) AS total,
  count(*) FILTER (WHERE c.status IN ('succeeded','failed','cancelled','crashed','abandoned')) AS done,
  count(*) FILTER (WHERE c.status = 'running') AS running,
  count(*) FILTER (WHERE c.status = 'pending' AND c.deps_pending > 0) AS blocked
FROM {SCHEMA}.jobs c WHERE c.parent_id = '{join_id}'::uuid GROUP BY c.step_key""",
    ))
    # The expiry sweep's guarded candidate (T18's guard probe): the shape
    # at the fleet table (20k vanilla rows + 1001 flow rows).
    out.append(stmt(
        "SWEEP_RESULT_TTL (T18 guarded candidate)",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH expired AS MATERIALIZED (
    SELECT id FROM {SCHEMA}.jobs
    WHERE result_expires_at < statement_timestamp() AND result IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM {SCHEMA}.wf_edge e JOIN {SCHEMA}.jobs c ON c.id = e.child_id
          WHERE e.parent_id = {SCHEMA}.jobs.id AND c.status = 'pending'
            AND c.deps_pending > 0
            AND c.metadata @> '{{"blocking_reason": "join"}}'::jsonb)
    LIMIT 500
)
UPDATE {SCHEMA}.jobs j SET result = NULL FROM expired WHERE j.id = expired.id RETURNING j.id""",
    ))
    out.append(stmt(
        "ARCHIVE_CANDIDATE (T18 guarded)",
        f"""EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT id FROM {SCHEMA}.jobs
WHERE status = 'succeeded'::"{SCHEMA}".job_status
  AND finished_at < statement_timestamp() - '30 days'::interval
  AND NOT EXISTS (
      SELECT 1 FROM {SCHEMA}.wf_edge e JOIN {SCHEMA}.jobs fl ON fl.id = e.flow_id
      WHERE e.parent_id = {SCHEMA}.jobs.id AND fl.status NOT IN {TERMINAL_SET})
ORDER BY finished_at LIMIT 100""",
    ))

    text = "\n".join(out)
    parts = text.split("-- ===== ")
    report: list[str] = []
    for part in parts[1:]:
        name, _, body = part.partition(" =====\n")
        lines = body.strip().splitlines()
        explain_sql = "\n".join(lines[:-1]) if False else body.strip()
        # drop the trailing SELECT the EXPLAIN plan belongs to? No: EXPLAIN
        # (ANALYZE) already EXECUTES; the statements above are complete.
        try:
            rows = await conn.fetch(explain_sql)
            raw = rows[0][0] if rows else []
            plan = json.loads(raw) if isinstance(raw, str) else raw
            # extract the operator tree: node types + relations + the timing
            def walk(node: dict[str, object], depth: int) -> list[str]:
                line = (
                    "  " * depth
                    + f"{node.get('Node Type')}"
                    + (f" on {node.get('Relation Name')}" if node.get("Relation Name") else "")
                    + (f" idx={node.get('Index Name')}" if node.get("Index Name") else "")
                    + (f" loops={node.get('Actual Loops')}" if "Actual Loops" in node else "")
                    + (
                        f" time={node.get('Actual Total Time'):.3f}ms"
                        if "Actual Total Time" in node
                        else ""
                    )
                )
                out_lines = [line]
                for child in node.get("Plans", []) or []:
                    out_lines.extend(walk(child, depth + 1))  # pyright: ignore[reportArgumentType]
                return out_lines

            tree = "\n".join(walk(plan[0]["Plan"], 0))  # pyright: ignore[reportIndexType,reportUnknownArgumentType]
            ttop = plan[0]["Plan"].get("Actual Total Time")  # pyright: ignore[reportIndexType]
            report.append(f"===== {name}\n{tree}\n[total {ttop} ms]\n")
        except Exception as exc:  # noqa: BLE001 — the probe records, never fixes
            report.append(f"===== {name}\nERROR: {exc}\n{explain_sql[:400]}\n")
    OUT.write_text("\n".join(report))
    print(f"wrote {OUT}")


asyncio.run(main())
