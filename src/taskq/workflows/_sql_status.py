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


# ── THE ABSORPTION RECORD (T06/T07 + the phase-2 attack's H2 cure) ──────
# The SQL predicate deciding whether a FAILED node's failure was
# ABSORBED — shared VERBATIM by the per-node rollup (the derivation's
# input) and the maintenance leg (the root finalize), so the two can
# never drift. THE FENCE IS NOT ABSORPTION: the edge's declared POLICY
# (collect | maybe) alone absorbs nothing — the record must show the
# absorption RAN, and a join row blocked with a TERMINAL reason
# ('failed_parent' — a failed parent's fail-closed/fenced resolution —
# 'orphan_parent', or 'flow_dead' — the flow's own death fencing the
# fire) can NEVER deliver the fan-in. The EXISTS-any-absorbing-edge
# variant marked every mixed-policy failure absorbed, and the derivation
# could NEVER say 'failed' for a failed-closed run — the envelope lied
# (T07's C). ``{node}`` is the failed node's row alias.
def _absorbed_exists(node_alias: str) -> str:
    return (
        "EXISTS (\n"
        "            SELECT 1\n"
        '            FROM "{schema}".wf_edge e\n'
        '            JOIN "{schema}".jobs j2 ON j2.id = e.child_id\n'
        f"            WHERE e.parent_id = {node_alias}.id\n"
        "              AND e.failure_policy IN ('collect', 'maybe')\n"
        "              AND NOT (j2.status = 'pending' AND j2.metadata->>'blocking_reason' IN ('failed_parent', 'orphan_parent', 'flow_dead'))\n"
        "        )"
    )


# The WORKFLOW-LEVEL grouped rollup: one grouped read per status read —
# the admin page's status panel and the wf-progress gauge share the same
# read (the query-count pin). The flow root row itself (step_key='__flow__')
# is the run's linearization point, read BESIDE the nodes (its status is
# the reported status the G7 always-on assertion reconstructs against),
# never counted among them — the WHERE names that (the root's own status
# is not a NODE's status, and counting it manufactured a phantom one).
# The two exact clauses (step_key <> '__flow__' + the flow link present)
# are what let the planner prove the partial index (01.00.26_02 — the
# uuid-cast expression, the workflow-rows-only partial) serves this read
# at the fleet shape: without them the read was a Seq Scan of the whole
# jobs table per status read (the H3 conviction).
WORKFLOW_ROLLUP_SQL = """\
SELECT status, count(*) AS count
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1::uuid
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
GROUP BY status
"""


# The PER-NODE variant (the admin page's read, bounded by the run's own
# node count): the node's derived view fields ride the row — the join
# counter (join-wait), the blocked-with-reason stamp, the hold shape (the
# future scheduled_at + the unresolved signal), the ABSORPTION record
# (the edge ledger's declared policy for this node's failure + the
# absorbing join's failures array — the derivation reads the record, never
# a heuristic), and the PARENT LINK (the map-child mark's input — the
# admin run view collapses a row parented at another run node into its
# source's hexagon; static nodes are parented at NULL). The absorption record is _absorbed_exists's POLICY-vs-
# FENCE predicate — the edge's declaration alone absorbs nothing (the
# envelope never lies, T07's C).
WORKFLOW_NODES_SQL = (
    """\
SELECT j.id, j.step_key, j.status, j.deps_pending, j.parent_id,
       j.metadata->>'blocking_reason' AS blocking_reason,
       j.attempt, j.max_attempts,
       """
    + _absorbed_exists("j")
    + """ AS absorbed,
       j.metadata->>'error' AS error_jsonb,
       j.error_class, j.error_message
FROM {schema}.jobs j
WHERE (j.metadata->>'flow_id')::uuid = $1::uuid
  AND j.metadata ? 'flow_id'
  AND j.step_key <> '__flow__'
ORDER BY j.id
"""
)


# The PER-MAP done/total (the counter's complement — "417/1000 · 3
# retrying · 580 blocked" from ONE read, computed IN the query, never a
# second instrument).
#
# T21 EXTENSION (the aggregation's map line, decision c): the same grouped
# read LEFT JOINed to the STATE channel — the children's emitted progress
# (avg pct, the freshest update) rides the SAME read, computed IN the
# query, never a second instrument and never a per-child series (DH5's
# fence: per-child progress lives in the ROWS this read serves on demand,
# never in a metric label). A child that never emitted reads NULL avg_pct
# — the LEFT JOIN keeps the counts complete without it.
WORKFLOW_MAP_PROGRESS_SQL = """\
SELECT c.step_key,
       count(*) AS total,
       count(*) FILTER (WHERE c.status IN {terminal}) AS done,
       count(*) FILTER (WHERE c.status = 'running') AS running,
       count(*) FILTER (WHERE c.status = 'pending' AND c.deps_pending > 0) AS blocked,
       count(*) FILTER (WHERE c.status = 'failed') AS failed,
       round(avg(p.pct))::int AS avg_pct,
       max(p.updated_at) AS freshest
FROM {schema}.jobs c
LEFT JOIN {schema}.wf_node_progress p ON p.node_id = c.id AND p.channel = 'progress'
WHERE c.parent_id = $1::uuid
GROUP BY c.step_key
"""


# The PER-SOURCE variant (the admin run-explorer's collapse feed — the
# dead map-collapse's cure, Q1d): the same done/total aggregate, grouped
# by the children's TRUE parent — the map-source node the fork/emit
# parented every child at (the fork/emit INSERT binds the SOURCE's job
# id, never the run root; the root's own id matches NOTHING). The fan-in
# JOIN row is NOT a counted child: it is born parented at the source
# too, but it is a NODE (the joined-row family's blocking_reason marker
# carries on its metadata) — counting it read the hexagon 3/4 for a
# 3-item map. The admin view passes THE RUN'S NODE IDS and gets one row
# per source node: the hexagon collapse renders per source, and the
# ROOT's counter is the SUM across sources (assembled in _wf_rows). One
# bound statement (ANY($1)), computed IN the query — never a per-source
# round trip (the fan-out tx band's shape), never a second instrument
# (the single-source read above keeps its own signature: it answers ONE
# source's id, this one MANY).
WORKFLOW_MAP_PROGRESS_SOURCES_SQL = """\
SELECT p.id AS source_id,
       p.step_key AS source_key,
       count(*) AS total,
       count(*) FILTER (WHERE c.status IN {terminal}) AS done
FROM {schema}.jobs c
JOIN {schema}.jobs p ON p.id = c.parent_id
WHERE c.parent_id = ANY($1::uuid[])
  AND c.metadata->>'blocking_reason' IS NULL
GROUP BY p.id, p.step_key
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
# jobs_wf_flow_nodes_idx (01.00.26_02) — never a seq scan.
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
WORKFLOW_ROOT_MAINTAIN_SQL = (
    """\
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
           -- THE ABSORPTION RECORD (the same _absorbed_exists POLICY-vs-
           -- FENCE predicate the per-node rollup serves — the derivation
           -- and the root finalize can never drift): a failed node whose
           -- failure the flow's fence never delivered is NOT absorbed.
           bool_or(
               n.status IN ('failed', 'crashed', 'abandoned')
               AND NOT """
    + _absorbed_exists("n")
    + """
           ) AS has_failed,
           -- THE FAILED ARM'S LIVENESS (T20, the spike's live finding;
           -- RE-DERIVED by the T20/T21 fixer's zombie audit against the
           -- state machine's totality table): live UNRESOLVED work holds
           -- a failing run — a RUNNING row (the reclaim arms' ONLY
           -- input) and a pending/scheduled row that is NOT a
           -- resolved-blocked stamp. THE TERMINAL-CRASH CLASS IS GONE
           -- FROM THIS PREDICATE: crashed/abandoned have ZERO outbound
           -- transitions (statemachine.VALID_TRANSITIONS), no sweep arm
           -- ever reclaims them (the reclaim's input is 'running' — its
           -- crashed branch wrote the row BECAUSE the budget was
           -- exhausted), so they are TERMINALS, never liveness — the old
           -- 'running/crashed/abandoned' spelling was the crashed-
           -- terminal wedge: a {succeeded, crashed} run's corpse root
           -- stayed 'running' forever (the att_t20 evidence roots).
           -- THE 149-STRANDED-CHAINS WEDGE this cures: a no-fan-in
           -- streaming batch's FIRST chain failure must not finalize the
           -- root while the other chains are pending — the dispatch
           -- fence (workflow children of a terminal flow are
           -- unclaimable) would strand them forever. THE EXCLUSION IS
           -- LOAD-BEARING: a blocked-with-reason row (the H1 wedge's
           -- stamped join) is RESOLVED — it must not hold the run, or
           -- the original wedge returns.
           bool_or(n.status = 'running'
                   OR (n.status IN ('pending', 'scheduled')
                       AND NOT (n.metadata ? 'blocking_reason')))
               AS has_unresolved,
           bool_or(n.status = 'cancelled') AS has_cancelled,
           -- EVERY non-terminal row is a live run: pending rows (join-wait,
           -- held, blocked-with-reason — the blocked representations
           -- derive 'blocked', a LIVE state) and scheduled rows derive
           -- 'pending'/'blocked' — never a terminal verdict. THE
           -- TERMINAL-CRASH CLASS IS GONE (the zombie audit's same
           -- re-derivation): crashed/abandoned are terminal statuses —
           -- has_live counting them held the corpse root's finalize
           -- forever (the wedge's second seat).
           bool_or(n.status IN ('pending', 'running', 'scheduled'))
               AS has_live
    FROM roots r
    JOIN {schema}.jobs n
      ON (n.metadata->>'flow_id')::uuid = r.id
     AND n.metadata ? 'flow_id'
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
          -- finalizes the root only when NO live UNRESOLVED work rests
          -- on the run (T20: pending/scheduled chain rows hold it;
          -- resolved-blocked stamps do not — the H1 wedge's cure).
          -- Row 1 outranks it: live unresolved work keeps the run live.
          (pf.has_failed AND NOT COALESCE(pf.has_unresolved, false))
          -- THE COMPLETED/CANCELLED ROOT: every row terminal (rows 4-5).
          OR NOT COALESCE(pf.has_live, true)
      )
    RETURNING f.id, f.status::text AS to_state
),
-- THE AUDIT ROW RIDES THE DERIVATION (the deploy matrix's audit cure):
-- the root's terminal is a mutation like any other — the events reader
-- gets the state_change that names it (the shared tier invariant reads
-- a terminal row's OWN event here; a derived write is not exempt).
evt AS (
    INSERT INTO {schema}.job_events
    (job_id, occurred_at, kind, detail)
    SELECT m.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running',
                              'to_state', m.to_state,
                              'reason', 'workflow-maintained')
    FROM maintained m
)
SELECT count(*)::int AS roots_updated FROM maintained
"""
)
