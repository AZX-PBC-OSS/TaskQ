"""The status-rollup statement constants (T08): the grouped rollup (the
ledger IS the status store — no status cache to corrupt), the per-node
variant (the admin page's read, by run id — bounded), and the per-map
done/total (the counter's complement, computed IN the query — not a
second instrument, one fewer series to keep consistent).

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` positional
parameter binding (the JSONB-landmine rule, _sql.py's docstring).
"""

from __future__ import annotations

# The WORKFLOW-LEVEL grouped rollup: one grouped read per status read —
# the admin page's status panel and the wf-progress gauge share the same
# read (the query-count pin). The flow root row itself (step_key='__flow__')
# is the run's linearization point, read BESIDE the nodes (its status is
# the reported status the G7 always-on assertion reconstructs against),
# never counted among them.
WORKFLOW_ROLLUP_SQL = """\
SELECT status, count(*) AS count
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1::uuid
GROUP BY status
"""


# The PER-NODE variant (the admin page's read, bounded by the run's own
# node count): the node's derived view fields ride the row — the join
# counter (join-wait), the blocked-with-reason stamp, the hold shape (the
# future scheduled_at + the unresolved signal), and the ABSORPTION record
# (the edge ledger's declared policy for this node's failure + the
# absorbing join's failures array — the derivation reads the record, never
# a heuristic).
WORKFLOW_NODES_SQL = """\
SELECT j.id, j.step_key, j.status, j.deps_pending,
       j.metadata->>'blocking_reason' AS blocking_reason,
       EXISTS (
           SELECT 1
           FROM "{schema}".wf_edge e
           WHERE e.parent_id = j.id
             AND e.failure_policy IN ('collect', 'maybe')
       ) AS absorbed,
       j.metadata->>'error' AS error_jsonb,
       j.error_class, j.error_message
FROM {schema}.jobs j
WHERE (j.metadata->>'flow_id')::uuid = $1::uuid
  AND j.step_key <> '__flow__'
ORDER BY j.id
"""


# The PER-MAP done/total (the counter's complement — "417/1000 · 3
# retrying · 580 blocked" from ONE read, computed IN the query, never a
# second instrument).
WORKFLOW_MAP_PROGRESS_SQL = """\
SELECT c.step_key,
       count(*) AS total,
       count(*) FILTER (WHERE c.status IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned'))
           AS done,
       count(*) FILTER (WHERE c.status = 'running') AS running,
       count(*) FILTER (WHERE c.status = 'pending' AND c.deps_pending > 0) AS blocked
FROM {schema}.jobs c
WHERE c.parent_id = $1::uuid
GROUP BY c.step_key
"""


# THE DERIVATION'S MAINTENANCE LEG (T08: the reported status is the
# DERIVATION'S output, never a cache the engine hand-maintains): the
# rederive arm maintains each flow ROOT's status from the rows, per the
# §17.5 table's terminal rows — a non-absorbed failed node → the root
# 'failed' (§17.2's cascade's crash-window heal); all nodes
# terminal-succeeded/absorbed → the root 'succeeded' (the run completed;
# an ABSORBED failure derives through its parent — B2's clause). The
# cancel flip and the direct cascade own their own legs (the
# linearization points); this heals the windows. Bounded: the roots batch
# (FOR UPDATE SKIP LOCKED, LIMIT $2), and the per-root node scan rides
# jobs_wf_flow_nodes_idx (01.00.25_01) — never a seq scan.
WORKFLOW_ROOT_MAINTAIN_SQL = """\
WITH roots AS (
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status = 'running'
    ORDER BY f.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
per_flow AS (
    SELECT r.id AS flow_id,
           bool_or(
               n.status = 'failed'
               AND NOT EXISTS (
                   SELECT 1 FROM {schema}.wf_edge e
                   WHERE e.parent_id = n.id
                     AND e.failure_policy IN ('collect', 'maybe')
               )
           ) AS has_failed,
           bool_or(n.status = 'cancelled') AS has_cancelled,
           bool_or(n.status IN ('pending', 'running', 'scheduled', 'crashed', 'abandoned'))
               AS has_live
    FROM roots r
    JOIN {schema}.jobs n
      ON (n.metadata->>'flow_id')::uuid = r.id
     AND n.step_key <> '__flow__'
    GROUP BY r.id
),
maintained AS (
    UPDATE {schema}.jobs f
    SET status = CASE
            WHEN pf.has_failed THEN 'failed'
            WHEN pf.has_cancelled THEN 'cancelled'
            ELSE 'succeeded'
        END::"{schema}".job_status,
        finished_at = clock_timestamp()
    FROM per_flow pf
    WHERE f.id = pf.flow_id
      AND NOT pf.has_live
    RETURNING f.id
)
SELECT count(*)::int AS roots_updated FROM maintained
"""
