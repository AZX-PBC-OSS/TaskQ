"""The status-rollup statement constants: the grouped rollup (the
current jobs rows are the current-node record — no status cache to
corrupt; wf_step_ledger is claim/attempt history and replay data, never
a current-status source), the per-node
variant (the admin page's read, by run id — bounded), the fleet
wf-progress sampler (the maintenance leader's gauge read, deriving the
node-state classes IN the query — the gauge's contract is the
derivation's), and the per-map
done/total (the counter's complement, computed IN the query — not a
second instrument, one fewer series to keep consistent).

The schema identifier is the only interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` positional
parameter binding (the JSONB-landmine rule, _sql.py's docstring).
"""

from __future__ import annotations

# ── the absorption record (the flow-fenced join fix) ──────
# One predicate, and every rendering site applies its own status gate:
# ``absorbed_exists`` is the POLICY-vs-FENCE probe and is status-blind on
# the node it is handed. Its consumers:
#
# * ``absorbed_projection`` wraps it in the failure-class CASE for the
#   per-node rollup (``WORKFLOW_NODES_SQL``) and ``taskq flows list``'s
#   node projection — only failed / crashed / abandoned rows can be
#   absorbed, so a cancelled skip on a collect/maybe edge stays cancelled
#   rather than deriving through the edge as an absorbed failure;
# * ``WORKFLOW_ROOT_MAINTAIN_SQL`` (the maintenance leg) conjoins the
#   probe with the same failure-class gate at both of its sites, spelled
#   inline because ``bool_or`` / ``array_agg`` need the predicate, not a
#   projected column;
# * ``FLOW_FAILURE_SQL_TEMPLATE`` (``api/_sql_runner.py`` — the run's
#   failure face) negates the probe to find the newest non-absorbed failed
#   node;
# * ``WORKFLOW_PROGRESS_SQL`` (the fleet sampler) builds the same
#   policy-vs-fence set once in its ``absorbed`` CTE, sharing
#   ``_ABSORBING_POLICIES_SQL`` and ``_child_unfenced_predicate`` with the
#   probe, and joins it as ``a.id IS NOT NULL``.
#
# The probe itself: the edge's declared policy (collect | maybe) alone
# absorbs nothing — the record must show the absorption ran, and a join row
# blocked with a terminal reason ('failed_parent' — a failed parent's
# fail-closed/fenced resolution — 'orphan_parent', or 'flow_dead' — the
# flow's own death fencing the fire) can never deliver the fan-in. The
# EXISTS-any-absorbing-edge variant marked every mixed-policy failure
# absorbed, and the derivation could never say 'failed' for a failed-closed
# run — the envelope lied. ``{node}`` is the failed node's row alias.

#: The edge policies whose declared fan-in can absorb a failure.
_ABSORBING_POLICIES_SQL = "('collect', 'maybe')"

#: The terminal ``blocking_reason`` stamps the fence writes on a join row
#: that can never fire; such a row cannot deliver the fan-in.
_FENCED_BLOCKING_REASONS_SQL = "('failed_parent', 'orphan_parent', 'flow_dead')"


def _child_unfenced_predicate(alias: str) -> str:
    """The child row is not fenced out of the fan-in: a pending row blocked
    with a terminal reason can never deliver its edge's absorption."""
    return (
        f"NOT ({alias}.status = 'pending'"
        f" AND {alias}.metadata->>'blocking_reason' IN {_FENCED_BLOCKING_REASONS_SQL})"
    )


def absorbed_exists(node_alias: str) -> str:
    return (
        "EXISTS (\n"
        "            SELECT 1\n"
        '            FROM "{schema}".wf_edge e\n'
        '            JOIN "{schema}".jobs j2 ON j2.id = e.child_id\n'
        f"            WHERE e.parent_id = {node_alias}.id\n"
        f"              AND e.failure_policy IN {_ABSORBING_POLICIES_SQL}\n"
        f"              AND {_child_unfenced_predicate('j2')}\n"
        "        )"
    )


# The failure-class gate every absorption projection applies. A node can be
# absorbed only when it failed (the derivation folds crashed/abandoned into
# the failed class); a cancelled or otherwise non-failed row that happens to
# parent a collect/maybe edge is NOT absorbed. One rendering of the gate for
# the per-node rollup, ``taskq flows list`` and the fleet sampler, so the
# surfaces cannot disagree about which rows are even candidates.
#
# ``absorbed`` overrides the correlated probe for the fleet sampler, which
# precomputes the same set once and passes ``a.id IS NOT NULL`` against its
# materialized CTE: PostgreSQL decorrelates the correlated form inside the
# sampler into a hashed subplan built before any row is classified (a full
# ``jobs`` + ``wf_edge`` walk — measured), while the CTE form stays
# per-relation index-driven.
def absorbed_projection(node_alias: str, *, absorbed: str | None = None) -> str:
    probe = absorbed if absorbed is not None else absorbed_exists(node_alias)
    return (
        "CASE WHEN "
        f"{node_alias}.status IN ('failed', 'crashed', 'abandoned')\n"
        f"            THEN {probe}\n"
        "            ELSE FALSE\n"
        "       END"
    )


# The workflow-level grouped rollup: one grouped read per status read —
# the admin page's status panel and the wf-progress gauge share the same
# read (the query-count pin). The flow root row itself (step_key='__flow__')
# is the run's linearization point, read beside the nodes (its status is
# the reported status the G7 always-on assertion reconstructs against),
# never counted among them — the WHERE names that (the root's own status
# is not a node's status, and counting it manufactured a phantom one).
# The two exact clauses (step_key <> '__flow__' + the flow link present)
# are what let the planner prove the partial index (01.00.26_02 — the
# uuid-cast expression, the workflow-rows-only partial) serves this read
# at the fleet shape: without them the read was a Seq Scan of the whole
# jobs table per status read.
WORKFLOW_ROLLUP_SQL = """\
SELECT status, count(*) AS count
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1::uuid
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
GROUP BY status
"""


# The per-node variant (the admin page's read, bounded by the run's own
# node count): the node's identity rides the row too (step_key +
# map_index — the display's named view reads them), the derived view
# fields ride the row — the join
# counter (join-wait), the blocked-with-reason stamp, the hold shape (the
# future scheduled_at + the unresolved signal), the absorption record
# (the edge ledger's declared policy for this node's failure + the
# absorbing join's failures array — the derivation reads the record, never
# a heuristic), and the parent link (the map-child mark's input — the
# admin run view collapses a row parented at another run node into its
# source's hexagon; static nodes are parented at NULL).
#
# The 3am columns at the source: the row
# also carries started_at (the running face's age source), the
# failed_parent stamp (metadata.failed_parent — the dead parent's id,
# named on the record by the failed-parent cascade), and the lease face's columns
# (lock_expires_at + last_heartbeat_at — the reclaim-requires-a-leader
# marker's derivation input). Every status surface reads this query; the
# columns at the source are one read — no CLI-side supplement fetch
# (two reads where one suffices).
# The absorption record is absorbed_projection's failure-class gate around
# absorbed_exists — the edge's declaration alone absorbs nothing (the
# envelope never lies). Project it only for the failure class: the predicate
# is status-blind on the node, and a cancelled skip on a collect/maybe edge
# is cancelled, never an absorbed failure.
WORKFLOW_NODES_SQL = (
    """\
SELECT j.id, j.step_key, j.map_index, j.status, j.deps_pending, j.parent_id,
       j.metadata->>'blocking_reason' AS blocking_reason,
       j.attempt, j.max_attempts,
       j.started_at,
       -- THE DEFERRAL FACES (the run page's "why is this step sitting
       -- here" answer — the snoozed step's own columns): the next fire
       -- time (the disposition's reschedule), the row's deferral counter,
       -- and the deferral stamp (the signal + the budget face + the
       -- disposition's own post-attempt ordinal — the discriminator the
       -- label renders from).
       j.scheduled_at, j.snooze_count,
       j.metadata->'wf_deferral' AS deferral,
       -- THE DEADLINE FACE (the wf start_to_close bound): the row's own
       -- declared bound — the node panel's "the bound where declared +
       -- the deadline's reality for a RUNNING node" line (the claim's
       -- started_at is already read; the label derives from the pair).
       j.start_to_close,
       -- THE CLOCK ARBITER (the single-arbiter doctrine — the label
       -- reads the DATABASE's clock, ``running_for_ms``'s own law): the
       -- deadline's remaining computed HERE against clock_timestamp() —
       -- started_at is DATABASE-written, so measuring it against the
       -- app's clock skews the span by the app-to-database offset (a
       -- DB two minutes ahead read as "expired 120s ago" on a step that
       -- just started: the label contradicting its own row). Positive =
       -- the time left; negative = the expiry's age; NULL when the
       -- reality's face doesn't render (a non-running row, no bound, no
       -- claim instant — the bound-alone face).
       CASE WHEN j.status = 'running' AND j.started_at IS NOT NULL
                 AND j.start_to_close IS NOT NULL
            THEN extract(epoch from (j.started_at + j.start_to_close) - clock_timestamp())
            ELSE NULL END AS deadline_remaining_s,
       j.metadata->>'failed_parent' AS failed_parent,
       j.lock_expires_at, j.last_heartbeat_at,
       j.code_version,
       """
    + absorbed_projection("j")
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


# The fleet wf-progress sampler: per (declared workflow, DERIVED node
# state) node counts — one grouped read for ``taskq.wf_progress_nodes_total``
# and its ``TaskQWorkflowBlockedStuck`` alert. The gauge's documented
# contract (docs/guides/observability.md) is the derivation's node-state
# classes, NOT the raw ``job_status`` enum: the enum has no 'blocked' and
# no 'complete' member (01.00.00_01), so a raw projection left
# ``state="blocked"`` emit-able by nothing and the alert permanently dead.
# The classification below is ``NodeView.is_blocked_row`` /
# ``derive_workflow_status`` (``_status.py``) applied per ROW — this gauge
# counts individual nodes, so the derivation's run-level
# ``cancel_in_flight`` reads the node's own ``cancel_phase`` rather than
# the root's ``cancel_requested_at``.
#
# THE PLAN IS PART OF THE CONTRACT (measured, not asserted). Every
# relation is index-served: the ``nodes`` pass rides
# ``jobs_wf_flow_nodes_idx`` (01.00.26_02 — the uuid-cast expression, the
# workflow-rows-only partial, which this WHERE names exactly) exactly
# ONCE; ``failed_nodes`` is derived from that materialized projection (no
# second walk of the node index). The root population rides
# ``jobs_wf_root_retention_idx`` (the root-only partial, 01.00.43_01 — one
# row per run), the absorption candidate scan rides ``wf_edge_parent_idx``
# ONCE (driven by the failed ids as an array, so the edge ledger is
# probed, never walked), and each candidate's fence check rides
# ``jobs_pkey`` over the parent's aggregated child ids (``LIMIT 1`` stops
# at the first absorbing edge). Measured with EXPLAIN (ANALYZE, BUFFERS) at
# 140,050 jobs / 40,050 workflow nodes / 10,000 failed-class rows:
# zero Seq Scan on ``jobs`` and zero Seq Scan on ``wf_edge``; one scan of
# ``jobs_wf_flow_nodes_idx`` and one of ``wf_edge_parent_idx``; total cost
# ~1.1e4, p50 ~56 ms and ~15.7k shared buffer hits on the development host
# (warm cache) — equal-or-better than the prior two-scan CTE form on the
# same host (~1.5e4, ~58 ms, ~28k hits). Below ``jit_above_cost`` so
# nothing is JIT-amplified. The same access paths hold at a 110,000-edge
# shape (one bitmap index probe of ``wf_edge_parent_idx``, ~174 ms, still
# zero Seq Scan on ``wf_edge``).
# The correlated form (``absorbed_exists`` inline in the CASE) is NOT
# used here: PostgreSQL decorrelates it into a hashed subplan built
# before any row is classified — a full ``Seq Scan`` of ``jobs`` (130k
# rows at that shape) plus ``wf_edge``, cost ~1.2e6, ~451-600 ms. The
# branch guard was inert in this statement, whatever it buys the
# per-node read. Precomputing the same predicate into ``absorbed``
# keeps the gate literal (only failed/crashed/abandoned rows are
# probed) and the plan linear in the node population.
#
# Held detection is the in-row ``metadata ? 'hold'`` JSONB check — the
# dispatch fence itself (``_dispatch_sql``), so no ``wf_signals`` join.
# The ``nodes`` projection carries only the scalar columns the
# classification reads (never the raw JSONB blob). The ``_other_``
# collapse stays in the sampler (it knows the registered names); the read
# groups by the run's own stamped name, registered or not.
# The fallback is ``n.status::text``, not ``'pending'``: every current enum
# member is classified explicitly, so a future one surfaces loudly instead
# of silently folding into the queue bucket.
WORKFLOW_PROGRESS_SQL = (
    """\
WITH nodes AS MATERIALIZED (
    SELECT n.id, n.status, n.deps_pending, n.cancel_phase,
           (n.metadata ? 'hold') AS held,
           n.metadata->>'blocking_reason' AS blocking_reason,
           (n.metadata->>'flow_id')::uuid AS flow_id
    FROM "{schema}".jobs n
    WHERE n.metadata ? 'flow_id'
      AND n.step_key IS NOT NULL AND n.step_key <> '__flow__'
),
roots AS MATERIALIZED (
    SELECT r.id, r.metadata->>'workflow' AS workflow
    FROM "{schema}".jobs r
    WHERE r.step_key = '__flow__'
),
failed_nodes AS MATERIALIZED (
    SELECT id FROM nodes WHERE status IN ('failed', 'crashed', 'abandoned')
),
failed_ids AS MATERIALIZED (
    SELECT array_agg(id) AS ids FROM failed_nodes
),
edge_parents AS MATERIALIZED (
    SELECT e.parent_id AS id, array_agg(e.child_id) AS child_ids
    FROM failed_ids fi
    CROSS JOIN LATERAL (
        SELECT e.parent_id, e.child_id
        FROM "{schema}".wf_edge e
        WHERE e.parent_id = ANY(fi.ids)
          AND e.failure_policy IN """
    + _ABSORBING_POLICIES_SQL
    + """
    ) e
    GROUP BY e.parent_id
),
absorbed AS MATERIALIZED (
    SELECT DISTINCT ep.id
    FROM edge_parents ep
    JOIN LATERAL (
        SELECT 1 AS hit
        FROM "{schema}".jobs j2
        WHERE j2.id = ANY(ep.child_ids)
          AND """
    + _child_unfenced_predicate("j2")
    + """
        LIMIT 1
    ) hit ON TRUE
)
SELECT COALESCE(rt.workflow, '_other_') AS workflow,
       CASE
           WHEN n.status = 'running' THEN 'running'
           WHEN n.status IN ('pending', 'scheduled') AND n.cancel_phase > 0
               THEN 'running'
           WHEN """
    + absorbed_projection("n", absorbed="a.id IS NOT NULL")
    + """
               THEN 'complete'
           WHEN n.status IN ('failed', 'crashed', 'abandoned') THEN 'failed'
           WHEN n.status = 'succeeded' THEN 'complete'
           WHEN n.status = 'cancelled' THEN 'cancelled'
           WHEN n.status = 'pending' AND (
                   n.deps_pending > 0
                   OR n.held
                   OR (n.blocking_reason IS NOT NULL
                       AND n.blocking_reason NOT IN ('join', 'body_unavailable'))
               ) THEN 'blocked'
           WHEN n.status IN ('pending', 'scheduled') THEN 'pending'
           ELSE n.status::text
       END AS state,
       count(*) AS count
FROM nodes n
JOIN roots rt ON rt.id = n.flow_id
LEFT JOIN absorbed a ON a.id = n.id
GROUP BY 1, 2
"""
)


# The per-map done/total (the counter's complement — "417/1000 · 3
# retrying · 580 blocked" from one read, computed in the query, never a
# second instrument).
#
# extension (the aggregation's map line): the same grouped
# read LEFT JOINed to the STATE channel — the children's emitted progress
# (avg pct, the freshest update) rides the same read, computed in the
# query, never a second instrument and never a per-child series (the
# fence: per-child progress lives in the rows this read serves on demand,
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


# The per-source variant (the admin run-explorer's collapse feed — the
# dead map-collapse's fix, Q1d): the same done/total aggregate, grouped
# by the children's true parent — the map-source node the fork/emit
# parented every child at (the fork/emit INSERT binds the source's job
# id, never the run root; the root's own id matches NOTHING). The fan-in
# JOIN row is not a counted child: it is born parented at the source
# too, but it is a node (the joined-row family's blocking_reason marker
# carries on its metadata) — counting it read the hexagon 3/4 for a
# 3-item map. The admin view passes the run's node IDS and gets one row
# per source node: the hexagon collapse renders per source, and the
# root's counter is the sum across sources (assembled in _wf_rows). One
# bound statement (any($1)), computed in the query — never a per-source
# round trip (the fan-out tx band's shape), never a second instrument
# (the single-source read above keeps its own signature: it answers one
# source's id, this one many).
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


# the rederive arm maintains each flow root's status from the rows, per the
# table's terminal rows — a non-absorbed failed node → the root 'failed'
# (§17.2's cascade's crash-window heal); all nodes terminal-succeeded/absorbed
# → the root 'succeeded' (the run completed; an absorbed failure derives
# through its parent — B2's clause). The cancel flip and the direct cascade own
# their own legs (the linearization points); this heals the windows. Bounded:
# the roots batch (FOR UPDATE SKIP LOCKED, LIMIT $2) — THE LOCK-ORDER
# EXEMPTION (A9.2's named list, the real reason): SKIP LOCKED never queues
# behind a root a drain/cascade transaction already holds (contended roots
# are deferred to a later pass), so this seat cannot form a root/drain
# cycle and takes no separate admission serialization — and the per-root
# node scan rides jobs_wf_flow_nodes_idx (01.00.26_02) — never a seq scan.
# The rows
# are truth, the root row is a cache: the maintenance leg derives the root's
# terminal state from the node rows — the same reconstruction the debug view
# uses — and finalizes the root in the same tx when the derivation is terminal
# and the root row isn't. The precedence is the derivation's, applied in order:
# a non-absorbed failure finalizes the root 'failed' even while
# resolved-blocked rows rest on the run — the failed row outranks the blocked
# row. The wedge this guards: the crash-window fail_closed run whose
# blocked_required heal stamps the join — the stamped join row is
# terminalizable by nothing (nothing dispatches it, nothing fires it), so a
# gate that required every row terminal before the case ran would leave the
# root wedged 'running' forever (unbounded retention — the pruner's liveness
# guard holds every row — and the dead run re-scanned every sweep pass). The
# finalize names its terminal state and the reason: error_class carries the
# maintenance stamp when the root has no error of its own (the cascade's flip
# stamps the peer-cancel origin; a root the sweep finalizes carries
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
           -- THE ABSORPTION RECORD (the same absorbed_exists POLICY-vs-
           -- FENCE predicate the per-node rollup serves — the derivation
           -- and the root finalize can never drift): a failed node whose
           -- failure the flow's fence never delivered is NOT absorbed.
           bool_or(
               n.status IN ('failed', 'crashed', 'abandoned')
               AND NOT """
    + absorbed_exists("n")
    + """
           ) AS has_failed,
           -- THE CAUSAL CHILD (F11): the ACTUAL current non-root node that
           -- made has_failed true — the same failed/crashed/abandoned AND
           -- NOT-absorbed rows the failure predicate reads, deterministically
           -- ordered (the freshest finish, then the id) — never the latest
           -- failed LEDGER attempt (an old failed attempt whose job later
           -- succeeded, or a crashed/abandoned terminal with no failed
           -- ledger row, named the wrong child). The §5.4 fence's causal
           -- record is the node rows' own truth.
           (array_agg(n.id ORDER BY n.finished_at DESC NULLS LAST, n.id DESC)
               FILTER (WHERE n.status IN ('failed', 'crashed', 'abandoned')
                       AND NOT """
    + absorbed_exists("n")
    + """))[1] AS failure_cause_id,
           -- THE FAILED ARM'S LIVENESS (derived against the state machine's
           -- totality table): live UNRESOLVED work holds
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
           -- THE 149-STRANDED-CHAINS WEDGE this fixes: a no-fan-in
           -- streaming batch's FIRST chain failure must not finalize the
           -- root while the other chains are pending — the dispatch
           -- fence (workflow children of a terminal flow are
           -- unclaimable) would strand them forever. THE EXCLUSION IS
           -- LOAD-BEARING: a blocked-with-reason row (the wedge's
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
          -- on the run (pending/scheduled chain rows hold it;
          -- resolved-blocked stamps do not — the wedge's fix).
          -- Row 1 outranks it: live unresolved work keeps the run live.
          (pf.has_failed AND NOT COALESCE(pf.has_unresolved, false))
          -- THE COMPLETED/CANCELLED ROOT: every row terminal (rows 4-5).
          OR NOT COALESCE(pf.has_live, true)
      )
    RETURNING f.id, f.status::text AS to_state,
          CASE WHEN pf.has_failed THEN 1 ELSE 0 END AS failed_root,
          pf.failure_cause_id
),
-- THE TERMINAL WAITERS' FENCE (the second seat of the flow-fenced fix):
-- the roots THIS pass terminalizes can still hold pending join-wait rows —
-- a never-fired ``<source>.join`` (``blocking_reason='join'``), or a
-- consumer an EARLIER pass stamped ``orphan_parent`` while its root was
-- still running (the bounded roots batch, or a root held by a concurrent
-- transaction, deferred that root past the stamp pass). Both are
-- live-flow records on a run that is now dead: the fire's flow-status leg
-- refuses a terminal flow forever, so the truthful resolution is the
-- fence's — ``failed_parent`` naming a failed parent when one exists,
-- ``flow_dead`` otherwise.
--
-- The candidate join rides jobs_wf_flow_nodes_idx (01.00.26_02 — the
-- uuid-cast expression + the workflow-rows-only partial): ``maintained``
-- is bounded by $1, and each maintained root probes its own node rows, so
-- the arm is O(the pass's terminalized roots' nodes), never a jobs walk.
-- ``FOR UPDATE OF jj SKIP LOCKED`` is the seat's lock-order rule: a
-- contended waiter is skipped, never queued behind while the pass holds its
-- maintained roots and join rows. A row stamped in THIS pass cannot be
-- skipped — the ``blocked`` arm's own UPDATE holds its lock in this
-- transaction — so the skip only defers a waiter an unrelated transaction
-- already holds; a ``join`` row is then healed by the next pass's rederive
-- fence (this statement is the terminalizing-pass half only).
--
-- The ``join`` rows of flows that were ALREADY terminal when the pass
-- began are REDERIVE_SWEEP_SQL's ``flow_fenced`` arm, whose predicate is
-- exactly jobs_wf_join_fire_probe_idx's partial — keep that arm pure so
-- it stays index-served. This CTE is the terminalizing-pass half only.
fenced_candidates AS (
    SELECT jj.id
    FROM maintained m
    JOIN {schema}.jobs jj
      ON (jj.metadata->>'flow_id')::uuid = m.id
     AND jj.metadata ? 'flow_id'
     AND jj.step_key <> '__flow__'
    WHERE jj.status = 'pending'
      AND (
          jj.metadata @> '{{"blocking_reason": "join"}}'::jsonb
          OR jj.metadata @> '{{"blocking_reason": "orphan_parent"}}'::jsonb
      )
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.wf_join_fire fire WHERE fire.join_job_id = jj.id
      )
    ORDER BY jj.id
    FOR UPDATE OF jj SKIP LOCKED
),
fenced_waiters AS (
    UPDATE {schema}.jobs j
    SET metadata = j.metadata || jsonb_build_object(
            'blocking_reason',
            CASE WHEN fp.parent_id IS NOT NULL THEN 'failed_parent' ELSE 'flow_dead' END,
            'failed_parent', fp.parent_id,
            'failed_step', fp.step_key
        )
    FROM fenced_candidates fc
    LEFT JOIN LATERAL (
        SELECT p.id AS parent_id, p.step_key
        FROM {schema}.wf_edge e
        JOIN {schema}.jobs p ON p.id = e.parent_id
        WHERE e.child_id = fc.id
          AND p.status = 'failed'
        ORDER BY p.id
        LIMIT 1
    ) fp ON true
    WHERE j.id = fc.id
    RETURNING j.id
),
-- THE AUDIT ROW RIDES THE DERIVATION (the deploy matrix's audit fix):
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
),
-- THE §5.4 NON-DRAINAGE FENCE (the derivation's failure arm's second
-- seat): a root this maintenance leg just terminal-failed, while an exact
-- parked source still holds un-emitted work (including the first page
-- when no cursor has committed — the absent key means zero pages have
-- committed, not that the park is empty), is the proven non-drainage
-- shape — the same transaction fences every such parked source (the
-- refusal's causal proof is the root's own failed terminal behind the
-- exhausted child failure the derivation saw).
not_draining AS (
    SELECT {schema}.fence_not_draining_parked(maintained.id, maintained.failure_cause_id) AS fenced
    FROM maintained
    WHERE maintained.failed_root = 1
)
SELECT (SELECT count(*)::int FROM maintained) AS roots_updated,
       COALESCE((SELECT sum(fenced)::int FROM not_draining), 0) AS not_draining_fenced,
       (SELECT count(*)::int FROM fenced_waiters) AS flow_fenced
"""
)
