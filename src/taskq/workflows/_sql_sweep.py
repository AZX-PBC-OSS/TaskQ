"""The sweep-arm statement constants (T04): the lock-first re-derive, the sweep's fire arm, the outbox drain, the phantom-ledger reaper.

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

# The sweep's lock-first re-derive — ONE batched statement (the fanout cut #4
# cure; the per-join round-trip variant measured p50 477 ms / p95 1.73 s @
# ~340 live joins vs the set-based 14.9 ms @ 200). Lock FIRST (FOR UPDATE
# SKIP LOCKED the join-wait children), then count un-terminal parents from
# the edge ledger INSIDE the same transaction. In-flight decrements are
# skipped this pass (the healthy worker wins); committed decrements are
# visible to the count. Stale writes impossible by construction: there is no
# snapshot-derived counter write — the reconcile UPDATE re-evaluates
# `deps_pending <> unterminal` under the child's own row lock (the
# snapshot-sweep variant that wrote deps=0 from a stale read is the deps=-1
# fingerprint's sibling and stays RED forever).
#
# Missing parents (a MISNAMED-CHILD edge) never fire silently: the blocked
# arm stamps metadata.blocking_reason='orphan_parent' (the blocked-with-
# reason state, §17.2's chains render from it) and the firable arm excludes
# them — "the record looked healthy while the work was wrong" is the
# convicted shape.
REDERIVE_SWEEP_SQL = """\
WITH locked AS (
    SELECT c.id, c.step_key, c.metadata->>'flow_id' AS flow_id, c.deps_pending
    FROM {schema}.jobs c
    WHERE c.status = 'pending'
      AND c.deps_pending > 0
      AND c.metadata @> '{{"blocking_reason": "join"}}'::jsonb
      -- HELD-ROW EXCLUSIVITY (P3 rule 1, the invariant every sweep arm
      -- obeys): a held row's scheduled_at (the signal deadline) is the only
      -- live timer on it — a row scheduled into the future is invisible to
      -- this arm, whatever its counter says.
      AND c.scheduled_at <= statement_timestamp()
    ORDER BY c.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
counts AS (
    SELECT l.id AS child_id,
           count(*) FILTER (WHERE e.child_id IS NOT NULL) AS edge_count,
           count(*) FILTER (WHERE e.child_id IS NOT NULL AND p.id IS NULL) AS missing_parents,
           count(*) FILTER (
               WHERE e.child_id IS NOT NULL
                 AND p.id IS NOT NULL
                 AND p.status NOT IN {terminal}
           ) AS unterminal,
           -- T06's compose: a FAILED parent on a FAIL_CLOSED edge never
           -- fires the join — the heal resolves it the way the direct
           -- path's cascade would have (the crash window ate the child's
           -- tx2): the blocked_required arm stamps it, the firable arm
           -- excludes it. A collect join's failed parent is RESOLVED
           -- (terminal — the fan-in landed at its finalize); the count
           -- above is all it reads.
           count(*) FILTER (
               WHERE e.child_id IS NOT NULL
                 AND p.id IS NOT NULL
                 AND p.status = 'failed'
                 AND e.failure_policy = 'fail_closed'
           ) AS failed_required
    FROM locked l
    -- LEFT-JOIN, not INNER: a join-wait row with NO edge rows (a public
    -- path that never wrote its edges) must be ENUMERATED — the inner
    -- join made it invisible (never reconciled, never fired, never
    -- stamped blocked-with-reason): silently stranded in join-wait
    -- forever with a healthy-looking record. The edge-less row blocks
    -- below (edge_count = 0 → orphan_parent), the 'record healthy, work
    -- wrong' class convicted with a reason.
    LEFT JOIN {schema}.wf_edge e ON e.child_id = l.id
    LEFT JOIN {schema}.jobs p ON p.id = e.parent_id
    GROUP BY l.id
),
blocked AS (
    UPDATE {schema}.jobs j
    SET metadata = jsonb_set(
            j.metadata,
            '{{blocking_reason}}',
            to_jsonb($2::text),
            true
        )
    FROM counts c
    WHERE j.id = c.child_id
      AND (c.missing_parents > 0 OR c.edge_count = 0)
      AND NOT j.metadata @> '{{"blocking_reason": "orphan_parent"}}'::jsonb
    RETURNING j.id
),
-- T06's heal: the fail_closed join whose parent TERMINAL-FAILED while
-- this join's own tx2 never ran (the crash window) — the same
-- blocked-with-reason resolution the direct path's cascade stamps, so
-- the record never shows a hanging join whatever process healed.
blocked_required AS (
    UPDATE {schema}.jobs j
    SET metadata = jsonb_set(
            j.metadata,
            '{{blocking_reason}}',
            to_jsonb($3::text),
            true
        )
    FROM counts c
    WHERE j.id = c.child_id
      AND c.failed_required > 0
      AND c.missing_parents = 0
      AND c.edge_count > 0
      AND NOT j.metadata @> '{{"blocking_reason": "failed_parent"}}'::jsonb
    RETURNING j.id
),
-- THE FLOW-FENCED JOIN ARM (the phase-2 attack's H2 cure): a never-fired
-- join row whose resolution the FLOW'S OWN DEATH fenced — the fire's
-- flow-status leg refuses a terminal flow, so this row can never fire
-- again, and (the convicted shape) the sweep's recount then RECONCILED
-- its counter to 0, leaving a claimable never-fired join row on a failed
-- flow. THE FENCE IS NOT ABSORPTION: the resolution the direct path's
-- cascade stamps is stamped HERE — 'failed_parent' naming the failed
-- parent when one exists (a fenced collect's failed parent IS the
-- cause), 'flow_dead' when the parents terminalized fine and the fire
-- the flow's death fenced was the join's last resort. The reconciled and
-- firable arms exclude the stamped rows (a stamped row is resolved: the
-- dead-run rescan ends here).
flow_fenced AS (
    UPDATE {schema}.jobs j
    SET metadata = j.metadata
        || jsonb_build_object(
               'blocking_reason',
               CASE WHEN cand.parent_id IS NOT NULL THEN $3::text ELSE $4::text END,
               'failed_parent',
               cand.parent_id,
               'failed_step',
               cand.step_key
           )
    FROM (
        SELECT jj.id, fp.parent_id, fp.step_key
        FROM {schema}.jobs jj
        JOIN {schema}.jobs f
          ON f.id = (jj.metadata->>'flow_id')::uuid
        LEFT JOIN LATERAL (
            SELECT p.id AS parent_id, p.step_key
            FROM {schema}.wf_edge e
            JOIN {schema}.jobs p ON p.id = e.parent_id
            WHERE e.child_id = jj.id
              AND p.status = 'failed'
            ORDER BY p.id
            LIMIT 1
        ) fp ON true
        WHERE f.step_key = '__flow__'
          AND f.status IN {terminal}
          AND jj.status = 'pending'
          AND jj.metadata @> '{{"blocking_reason": "join"}}'::jsonb
          AND NOT EXISTS (
              SELECT 1 FROM {schema}.wf_join_fire fire WHERE fire.join_job_id = jj.id
          )
          -- One resolution per row per pass: the arms all see the same
          -- statement snapshot, so the exclusion (not the write order)
          -- keeps the double-update undefined-result class out.
          AND NOT EXISTS (SELECT 1 FROM blocked b WHERE b.id = jj.id)
          AND NOT EXISTS (SELECT 1 FROM blocked_required br WHERE br.id = jj.id)
    ) cand
    WHERE j.id = cand.id
    RETURNING j.id
),
reconciled AS (
    UPDATE {schema}.jobs j
    SET deps_pending = c.unterminal::smallint
    FROM counts c
    WHERE j.id = c.child_id
      AND c.missing_parents = 0
      AND c.failed_required = 0
      AND c.edge_count > 0
      AND NOT EXISTS (SELECT 1 FROM flow_fenced ff WHERE ff.id = j.id)
      AND j.deps_pending <> c.unterminal::smallint
    RETURNING j.id
),
firable AS (
    SELECT l.id, l.flow_id, l.step_key
    FROM locked l
    JOIN counts c ON c.child_id = l.id
    WHERE c.edge_count > 0
      AND c.missing_parents = 0
      AND c.unterminal = 0
      -- A fail_closed join with a failed parent is never firable (T06):
      -- the blocked_required arm owns its resolution.
      AND c.failed_required = 0
      -- Nor is a flow-fenced join (the H2 arm owns its resolution — the
      -- fire's flow-status leg would refuse it every pass forever).
      AND NOT EXISTS (SELECT 1 FROM flow_fenced ff WHERE ff.id = l.id)
)
SELECT
    (SELECT count(*) FROM blocked) AS blocked,
    (SELECT count(*) FROM blocked_required) AS blocked_required,
    (SELECT count(*) FROM flow_fenced) AS flow_fenced,
    (SELECT count(*) FROM reconciled) AS reconciled,
    (SELECT count(*) FROM firable) AS firable
"""


# The sweep's fire arm: SET-BASED (never one round trip per join — the
# cut #4 crime), computing the firable set ITSELF inside the caller's
# still-open transaction (the rederive statement's row locks are held by
# the same connection, so the re-derivation is deterministic). Fire ids are
# minted APP-SIDE (uuid7 via the seam, never DB-side generation) as a bound
# array, matched by the firable row's ordinal — a row beyond the minted
# pool (a parent terminalized between the count and this statement, whose
# tx2 blocks on the held child lock) stays firable next pass, never NULL-id
# fired. The flow-status leg rides INSIDE the fire statement (pin 5): a
# flow that cancelled refuses here, in the sweep's own fire statement.
SWEEP_FIRE_SQL = """\
WITH locked AS (
    SELECT c.id, c.step_key,
           -- The metadata flow link CAST HERE: the flow-status leg compares
           -- it against jobs.id (uuid) -- an uncast ->>'flow_id' (text)
           -- leaves the comparison unresolvable (uuid = text).
           (c.metadata->>'flow_id')::uuid AS flow_id
    FROM {schema}.jobs c
    WHERE c.status = 'pending'
      -- NOT `deps_pending > 0`: the rederive statement (same tx, same
      -- locks) already reconciled the CACHE to the ledger's truth for the
      -- firable rows -- the fire arm identifies them by the COUNT
      -- (unterminal = 0), never by the counter (the counter-as-cache).
      -- The fire's own `j.deps_pending = 0` + the PK make this safe:
      -- a row the cache says is still waiting cannot fire (the guard),
      -- and a row already fired cannot fire twice (the UNIQUE).
      AND c.metadata @> '{{"blocking_reason": "join"}}'::jsonb
      -- HELD-ROW EXCLUSIVITY (P3 rule 1): the held row's scheduled_at
      -- (the signal deadline) is the only live timer on it.
      AND c.scheduled_at <= statement_timestamp()
    ORDER BY c.id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
counts AS (
    SELECT l.id AS child_id,
           count(*) FILTER (WHERE e.child_id IS NOT NULL) AS edge_count,
           count(*) FILTER (WHERE e.child_id IS NOT NULL AND p.id IS NULL) AS missing_parents,
           count(*) FILTER (
               WHERE e.child_id IS NOT NULL
                 AND p.id IS NOT NULL
                 AND p.status NOT IN {terminal}
           ) AS unterminal,
           -- T06: a failed parent on a fail_closed edge never fires (the
           -- rederive's blocked_required arm owns that row's resolution;
           -- the fire arm excludes it from the firable set — the same
           -- guard, stated twice because both statements re-derive).
           count(*) FILTER (
               WHERE e.child_id IS NOT NULL
                 AND p.id IS NOT NULL
                 AND p.status = 'failed'
                 AND e.failure_policy = 'fail_closed'
           ) AS failed_required
    FROM locked l
    -- LEFT-JOIN, not INNER (the rederive arm's hardened shape): an
    -- edge-less join-wait row is enumerated and EXCLUDED from firable
    -- (edge_count = 0) — the rederive stamps it orphan_parent.
    LEFT JOIN {schema}.wf_edge e ON e.child_id = l.id
    LEFT JOIN {schema}.jobs p ON p.id = e.parent_id
    GROUP BY l.id
),
firable AS (
    SELECT f.id, f.flow_id, f.step_key,
           row_number() OVER (ORDER BY f.id) AS rn
    FROM (
        SELECT l.id, l.flow_id, l.step_key
        FROM locked l
        JOIN counts c ON c.child_id = l.id
        WHERE c.edge_count > 0
          AND c.missing_parents = 0
          AND c.unterminal = 0
          AND c.failed_required = 0
    ) f
),
wins AS (
    INSERT INTO {schema}.wf_join_fire (id, join_job_id, flow_id, step_key, fired_by)
    SELECT fire_ids[f.rn], f.id, f.flow_id, f.step_key, 'sweep'
    FROM firable f
    CROSS JOIN (SELECT $1::uuid[] AS fire_ids) pool
    WHERE f.rn <= COALESCE(array_length(pool.fire_ids, 1), 0)
      AND EXISTS (
          SELECT 1 FROM {schema}.jobs fl
          WHERE fl.id = f.flow_id
            AND fl.status NOT IN {terminal}
      )
    ON CONFLICT (join_job_id) DO NOTHING
    -- The inserted row's OWN id IS the fire id (fire_ids[f.rn] — the
    -- RETURNING list cannot reference the statement's source relations).
    RETURNING id AS fire_id, join_job_id, flow_id, step_key
)
SELECT w.join_job_id, w.step_key, w.flow_id, w.fire_id, j.trace_id,
       j.metadata->'consumers' AS consumers,
       -- THE WORKFLOW NAME STAMP (the reducer resolution's durable leg):
       -- the flow root's metadata names the workflow whose REGISTERED
       -- DEFINITION carries the fired join's reducer body — the healer
       -- resolves the body from the definition registry via this name,
       -- whatever process finalized (the memo is a cache, never the
       -- source). NULL when the root predates the stamp.
       root.metadata->>'workflow' AS workflow_name
FROM wins w
JOIN {schema}.jobs j ON j.id = w.join_job_id
JOIN {schema}.jobs root ON root.id = w.flow_id
"""


OUTBOX_FETCH_UNDELIVERED_SQL = """\
SELECT o.id, o.join_job_id, o.flow_id, o.consumer_step_key, o.map_index, o.bindings
FROM {schema}.wf_outbox o
WHERE NOT o.delivered
ORDER BY o.id
LIMIT $1
FOR UPDATE OF o SKIP LOCKED
"""


# The outbox drain's consumer insert: IDEMPOTENT on the consumer step key —
# the composite (idempotency_scope, idempotency_key) arbiter
# (jobs_idempotency_scope_key_uniq, 01.00.03) is the dedup authority. A crash
# between fire-commit and consumer-insert re-drains; the arbiter makes the
# retry a no-op (pin 20: the drain completes the dispatch EXACTLY ONCE).
OUTBOX_DRAIN_CONSUMERS_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::text[], $7::uuid[], $8::smallint[], $9::text[], $10::text[],
    $11::jsonb[], $12::text[], $13::text[]
)
ON CONFLICT (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id
"""


OUTBOX_DRAIN_FLIP_SQL = """\
UPDATE {schema}.wf_outbox
SET delivered = true
WHERE id = ANY($1::uuid[])
  AND NOT delivered
RETURNING id
"""


# The body-unavailable stamp (the loudness cure, R2-2): the fire arm
# delivers a fired join's consumers even when no reducer body resolves —
# at-least-once delivery is the contract — but the RECORD must not look
# healthy while the work was wrong. Keyed single join row; the stamp rides
# the fire's own transaction (a rolled-back pass rolls it back with it).
JOIN_BODY_UNAVAILABLE_SQL = """\
UPDATE {schema}.jobs
SET metadata = jsonb_set(
        metadata,
        '{{blocking_reason}}',
        to_jsonb($2::text),
        true
    )
WHERE id = $1
  AND NOT metadata @> '{{"blocking_reason": "body_unavailable"}}'::jsonb
RETURNING id
"""


# The fenced-attempt sweep arm (hardening H1-H3): reap phantom 'running'
# ledger rows on terminal flows so the rows-alone reconstruction reconciles
# (a terminal flow is reconstructible from rows alone; pin 15).
PHANTOM_REAP_SQL = """\
UPDATE {schema}.wf_step_ledger l
SET status = 'fenced',
    updated_at = clock_timestamp()
WHERE l.status = 'running'
  AND EXISTS (
      SELECT 1 FROM {schema}.jobs f
      WHERE f.id = l.flow_id
        AND f.status IN {terminal}
  )
RETURNING l.id, l.flow_id
"""


# ── THE NODELESS-ROOT REAP (the create-seam's belt) ─────────────────────
# The create's atomicity makes the orphan root UNREPRESENTABLE (root +
# nodes + edges + ROOT_START are one transaction); this arm is the SECOND
# fence: any nodeless root that could ever exist — a future statement-
# order regression's debris, a hand-crafted row — is reaped 'failed' with
# the LOUD error class once it is past the grace. THE SHAPES REAPED: a
# root (step_key '__flow__') with ZERO node rows in 'pending' or
# 'running' — the maintenance derivation can never develop either (its
# rollup INNER-JOINS the node rows: a nodeless root never derives, never
# terminalizes, never prunes — unbounded retention + a run key squatted
# forever). THE GRACE ($1) is the belt's own conservatism: an in-flight
# create of a buggy future shape is not reaped mid-flight; the clock is
# PG's own (the DB-clock doctrine), the batch bounded ($2, SKIP LOCKED).
# The event leg rides the same statement (a terminal transition an
# events reader cannot see never happened). The reap is the DEFINED
# verdict, not a heal: the run's key stays with the reaped run — the
# caller's re-run is a NEW key (the typed claim surface states the
# existing-terminal verdict loudly).
NODELESS_ROOT_REAP_SQL = """\
WITH orphans AS (
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN ('pending', 'running')
      AND f.created_at < now() - $1::interval
      AND NOT EXISTS (
          SELECT 1
          FROM {schema}.jobs n
          WHERE (n.metadata->>'flow_id')::uuid = f.id
            AND n.metadata ? 'flow_id'
            AND n.step_key <> '__flow__'
      )
    ORDER BY f.id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
), evt AS (
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT o.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'pending', 'to_state', 'failed',
                              'error_class', 'NodelessRunReaped')
    FROM orphans o
)
UPDATE {schema}.jobs f
SET status = 'failed',
    error_class = 'NodelessRunReaped',
    error_message = 'the run root has no node rows past the reap grace — '
                     'the create did not commit atomically (the orphan '
                     'root: undevelopable by the derivation, the run key '
                     'squatted); re-run with a NEW run key',
    finished_at = clock_timestamp()
FROM orphans o
WHERE f.id = o.id
RETURNING f.id
"""
