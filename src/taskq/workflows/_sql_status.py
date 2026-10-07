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
# §17.5 derivation's output, never a cache the engine hand-maintains): the
# rederive arm maintains each flow ROOT's status from the rows, per the
# §17.5 table's terminal rows — a non-absorbed failed node → the root
# 'failed' (§17.2's cascade's crash-window heal); all nodes
# terminal-succeeded/absorbed → the root 'succeeded' (the run completed;
# an ABSORBED failure derives through its parent — B2's clause). The
# cancel flip and the direct cascade own their own legs (the
# linearization points); this heals the windows. Bounded: the roots batch
# (FOR UPDATE SKIP LOCKED, LIMIT $2), and the per-root node scan rides
# jobs_wf_flow_nodes_idx (01.00.25_01/02) — never a seq scan.
#
# THE ROWS ARE TRUTH, THE ROOT ROW IS A CACHE (the phase-2 attack's H1
# cure): the maintenance leg derives the root's terminal state from THE
# NODE ROWS — the same reconstruction the debug view uses — and finalizes
# the root IN THE SAME TX when the derivation is terminal and the root
# row isn't. THE PRECEDENCE IS THE DERIVATION'S (§17.5, applied in
# order): a non-absorbed FAILURE finalizes the root 'failed' even while
# resolved-blocked rows rest on the run — the failed row outranks the
# blocked row. THE WEDGE THIS CURES: the crash-window fail_closed run
# whose blocked_required heal stamps the join but whose root nothing
# ever failed — the old gate required EVERY row terminal before the CASE
# ran, and the stamped join row is terminalizable by nothing (nothing
# dispatches it, nothing fires it): the root wedged 'running' forever
# (unbounded retention — the pruner's liveness guard held every row —
# and the dead run re-scanned every sweep pass). The finalize names its
# terminal state AND the reason: error_class carries the maintenance
# stamp when the root has no error of its own (the cascade's flip stamps
# the peer-cancel origin; a root the sweep finalizes carries
# 'UnabsorbedNodeFailure').
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
           -- The derivation's ROW 1 (the reclaim's input): a
           -- crashed/abandoned node is live work, and a running node
           -- outranks even a non-absorbed failure — the run is live.
           bool_or(n.status IN ('running', 'crashed', 'abandoned')) AS has_active,
           bool_or(n.status = 'cancelled') AS has_cancelled,
           -- EVERY non-terminal row is a live run: pending rows (join-wait,
           -- held, blocked-with-reason — the blocked representations
           -- derive 'blocked', a LIVE state) and scheduled rows derive
           -- 'pending'/'blocked' — never a terminal verdict.
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
        finished_at = clock_timestamp(),
        error_class = COALESCE(
            f.error_class,
            CASE WHEN pf.has_failed THEN 'UnabsorbedNodeFailure' END
        )
    FROM per_flow pf
    WHERE f.id = pf.flow_id
      AND (
          -- THE FAILED ROOT (precedence row 2): the non-absorbed failure
          -- finalizes the root through the blocked rows — the wedge's
          -- cure. Row 1 outranks it: a running/crashed/abandoned node
          -- keeps the run live.
          (pf.has_failed AND NOT COALESCE(pf.has_active, false))
          -- THE COMPLETED/CANCELLED ROOT: every row terminal (rows 4-5).
          OR NOT COALESCE(pf.has_live, true)
      )
    RETURNING f.id
)
SELECT count(*)::int AS roots_updated FROM maintained
"""
