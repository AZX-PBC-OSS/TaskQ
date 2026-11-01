"""The sweep-arm statement constants (T04): the lock-first re-derive, the sweep's fire arm, the outbox drain, the phantom-ledger reaper.

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

from taskq.constants import require_schema

# The sweep's lock-first re-derive — ONE batched statement (the fanout cut #4
# cure; the per-join round-trip variant is the convicted shape — its
# MEASURED conviction is P1's sweep-cost curve, 14.9 ms @ 200 joins vs the
# unscoped seq-scan monster 83.7 ms @ 75k, the CITED-IMPORT artifact
# .measurements/runs/sweep-cost-scale-curve-*.json — the spike's session
# evidence directory is gone; the imported record IS the provenance. An
# earlier revision of this comment cited the per-join variant at a p50/p95
# figure for which no capture exists; the dead figure is DELETED —
# provenance or silence). Lock FIRST (FOR UPDATE
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
SELECT o.id, o.join_job_id, o.flow_id, o.consumer_step_key, o.map_index, o.bindings,
       -- THE RUN-PRIORITY STAMP'S RESOLUTION (the wf-priority cure's
       -- drain leg): the flow root's stamped per-call override, resolved
       -- with THE FLEET DOOR'S OWN ARITHMETIC mirrored in SQL — the
       -- regex and the domain bounds below are the shared constants the
       -- Python face's resolve_priority_stamp parses with
       -- (taskq.constants.PRIORITY_STAMP_INT_PATTERN, SMALLINT_MIN/MAX;
       -- unparseable or out-of-smallint-domain → NO override) and
       -- COALESCEd to the column default's own value (0 — the unset
       -- case binds the column default's own value, the pre-cure
       -- byte-identical row). The escalation is an interactive run's
       -- most latency-sensitive follow-up: its consumer row materializes
       -- at the RUN's priority, never the default.
       COALESCE(
           CASE
             WHEN root.metadata->>'wf_priority' ~ '{priority_stamp_int_pattern}'
                  AND (root.metadata->>'wf_priority')::int
                      BETWEEN {smallint_min} AND {smallint_max}
             THEN (root.metadata->>'wf_priority')::int
           END,
           0
       ) AS priority
FROM {schema}.wf_outbox o
LEFT JOIN {schema}.jobs root ON root.id = o.flow_id
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
# priority is the RUN's stamped override the drain's fetch resolved (the
# wf-priority cure — the loop escalation's consumer materializes at the
# run's priority; the fleet door's own arithmetic, mirrored in the fetch's
# SQL): resolved by OUTBOX_FETCH_UNDELIVERED_SQL, COALESCEd to the column
# default's own value (0 — the pre-cure byte-identical row).
# start_to_close is the CONSUMER'S DECLARED DEADLINE (the wf
# start_to_close cure): the fire's bindings carry the downstream node's
# own resolution (the join row's metadata `consumers` array, serialized
# as SECONDS — the jsonb wire form); the unset case binds NULL
# (pre-cure identical).
OUTBOX_DRAIN_CONSUMERS_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key, priority, start_to_close)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::text[], $7::uuid[], $8::smallint[], $9::text[], $10::text[],
    $11::jsonb[], $12::text[], $13::text[], $14::smallint[], $15::interval[]
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
), reaped AS (
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
)
-- THE COUNT IS THE COUNT (finding 14's cure): the caller reads ONE scalar
-- through ``fetchval`` — a RETURNING f.id here handed the caller the FIRST
-- reaped row's UUID, and ``int(uuid)`` converts the 128-BIT IDENTITY into
-- a garbage row-count (a hash fragment) straight into
-- ``taskq.sweep.rows``'s sample. The statement answers count(*) — the
-- number the metric's name promises.
SELECT count(*) FROM reaped
"""

# THE HOLD-STAMP WEDGE'S CURE (the D2 soak's P1): a non-terminal node row
# carrying the held representation's stamp (``metadata.hold`` — the
# claimable fence ``AND NOT metadata ? 'hold'`` excludes it from every
# claim) whose HOLD ROW is no longer 'held' is UNCLAIMABLE FOREVER — the
# SIGKILL-during-a-held-loop end state the soak measured TWENTY of (the
# rows sat pending, stamped, deps_pending 0, their signals delivered,
# hours unclaimed; nothing detected them and no stranded-row reason named
# them). THE HOLD'S STATE DECIDES:
#
# * still 'held'  → the row is the legitimate HELD REPRESENTATION (the
#   hold is the truth — the resume re-dispatches): the arm leaves it.
# * delivered / abandoned / cancelled / ABSENT → the stamp is a DEAD
#   POINTER: cleared (the same clear the resume writes —
#   ``metadata - 'hold' - 'held_signal'``, the loop's budget pause
#   released with it), the row re-claims (``scheduled_at = now()``). The
#   body's re-execution owns the next NAMED state from there: a delivered
#   hold's answer replays from the queue (the resume contract), an
#   abandoned one RETURNS the union's Expired member at the wait site
#   (the fail-close arm the checker forces — the body that wants the
#   failure raises SignalTimeoutError itself off the member), an absent
#   one re-asks. NOTHING wedges: every path lands in a named state.
#
# Lock-first (FOR UPDATE SKIP LOCKED the stamped candidates) so a
# concurrent deliver's resume — the same clear, one tx — serializes
# against this arm: whoever lands second finds no stamp (the clear is
# idempotent, the race is free). The pending/scheduled scope is the
# wedge's own: terminal rows need no fence and running rows are the
# lease machinery's.
HOLD_STAMP_RECONCILE_SQL = """\
WITH stamped AS (
    SELECT j.id, j.metadata->>'hold' AS hold_id
    FROM {schema}.jobs j
    WHERE j.metadata ? 'hold'
      AND j.metadata ? 'flow_id'
      AND j.status IN ('pending', 'scheduled')
    ORDER BY j.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE {schema}.jobs j
SET metadata = j.metadata - 'hold' - 'held_signal',
    scheduled_at = clock_timestamp(),
    budget_paused = false
FROM stamped s
LEFT JOIN {schema}.wf_signals sig ON sig.id::text = s.hold_id
WHERE j.id = s.id
  AND (sig.id IS NULL OR sig.status <> 'held')
RETURNING j.id, (sig.id IS NULL) AS hold_absent
"""


# THE DELIVERED OUTBOX'S TTL (the D2 soak's P3): a delivered outbox row is
# narration — the consumer rows exist (the drain's arbiter inserted them),
# wf_join_fire is the exactly-once ledger, and the D2 soak measured the
# delivered population growing monotonically forever (5.9k at close, no
# pruner touched it). The retention policy's own row: delivered rows past
# the TTL delete, one bounded committed batch per pass; UNDELIVERED rows
# are never touched here (the drain owns them — deleting an undelivered
# row would lose the delivery it exists to make). The period is the
# sweep-family's zero-means-OFF sentinel (settings level): a brand-new
# deletion loop's safe misconfiguration is off.
OUTBOX_RETENTION_SQL = """\
WITH doomed AS (
    SELECT id FROM {schema}.wf_outbox
    WHERE delivered
      AND created_at <= clock_timestamp() - $2::interval
    ORDER BY id
    LIMIT $1
)
DELETE FROM {schema}.wf_outbox o USING doomed d
WHERE o.id = d.id
RETURNING o.id
"""


# THE STEP CACHE'S RETENTION ARM (T25): the cross-run cache's expired
# corpses are dead weight — the lookup's freshness leg already refuses
# them (a miss, the body re-runs), and without this arm the expired
# population grows monotone-forever (the delivered-outbox arm's own
# conviction class). The store's CAS re-fills an expired row on the
# re-run's success; this arm deletes what no re-run ever visits. The
# DB clock is the comparison (the estate's doctrine); one bounded
# committed batch per pass.
STEP_CACHE_RETENTION_SQL = """\
WITH doomed AS (
    SELECT content_address FROM {schema}.wf_step_cache
    WHERE expires_at <= clock_timestamp()
    ORDER BY expires_at
    LIMIT $1
)
DELETE FROM {schema}.wf_step_cache o USING doomed d
WHERE o.content_address = d.content_address
RETURNING o.content_address
"""


# ── THE SIX FLOW-HISTORY RETENTION ARMS ──────────────────────────────────
# Six tables grow monotone-forever with no retention arm and no FK for
# the job prune to cascade: wf_edge, wf_step_ledger, wf_join_fire,
# wf_signals, wf_node_progress, wf_node_stream. The outbox/step-cache
# arms prune DERIVED data (a deleted outbox row's narration is already
# delivered; a deleted cache row re-fills); THESE prune the FLOW HISTORY
# itself.
#
# THE TWO-PASS LAW (H-B1's cure — the convicted shape this replaces):
# the first cut selected doomed rows out of each wf TABLE — every row's
# predicate probed the jobs side twice (terminal-EXISTS + rootless
# NOT-EXISTS) and wf_edge, having no clock of its own, evaluated the
# probe for EVERY row: the zero-doomed steady-state pass was a
# full-table walk with 2 jobs-probes per row, TimeoutError at 3M rows,
# the arms PERMANENTLY stopped pruning at exactly the scale they exist
# for. The cure flips the selection to the FLOWS SIDE — two passes:
#
# * THE FLOWS-SIDE PASS (every call): the TERMINAL-AGED ROOTS are
#   selected FIRST — a tiny jobs-side set served by jobs_finished_at_idx
#   (status, finished_at): ``step_key = '__flow__'`` (the root identity,
#   H-B3 — the old EXISTS legs never checked it), a terminal status, and
#   the root's own ``finished_at`` past the period. Each selected flow's
#   rows then delete through the table's flow_id index (the delete joins
#   the table's flow column), row-LIMITed to the batch. The floor is
#   O(terminal flows + batch) — never O(table). The node_progress arm's
#   pass carries TWO batch-bounded deletes in one statement (the
#   round-2 R3 fold): its own table's rows AND the doomed flows'
#   ``wf_node_stream`` rows — the STREAM channel (a dead node's ring,
#   nodes x 64 rows unbounded) has the same doomed-flow identity, its
#   rows carry the flow_id column directly, and wf_node_stream_flow_seq_idx
#   (01.00.29_01) serves the delete — two bounded deletes per pass is the
#   family's one-batch-per-pass law's documented allowance (each CTE
#   carries its own LIMIT $1; the pass's cost stays
#   O(terminal flows + 2x batch)).
# * THE ROOTLESS PASS (a separate, coarser-cadence statement — the
#   leader wiring runs it every Nth call): the orphan class — the row's
#   pointee is NOT a flow root AT ALL (absent, or a live NON-root job;
#   H-B3's third orphan class the un-checked EXISTS legs could never
#   classify) — cannot be found from the flows side, so its statement is
#   a PK-ordered walk that visits AT MOST the batch (the LIMIT bounds
#   the SCAN, not the result — the property the convicted shape lacked)
#   and probes each visited row's pointee once. The walk advances as the
#   table's aged front drains (the flows-side pass deletes the
#   terminal-aged flows' rows ahead of it).
#
# THE PERSISTED CURSOR (the round-2 R1's cure — the walk that never
# advanced at a live PK front): ``ORDER BY pk LIMIT batch`` re-visits
# the SAME first batch every pass when that batch's probes all answer
# "live root" — the red-team's proof: a 3,000-row live front, batch
# 1,000, five passes, deleted=0, the orphans beyond the front stranded
# forever. The cure is the estate's own watermark precedent
# (``job_events_prune_state``, 01.00.20_02): a persisted cursor row per
# arm (``wf_rootless_sweep_state``, 01.00.36_01) that the walk SEEKS
# STRICTLY PAST — the cursor rides the last VISITED row (visited, not
# deleted: a live front's rows ARE the advance), and it advances in the
# SAME statement/transaction as the deletes (the watermark's own law:
# never lag what is already gone, never claim what is not — a crashed
# pass rolls the cursor back with its deletes and re-walks them). When
# the walk finds NOTHING above its cursor it REWINDS the cursor to the
# start (the wraparound: a row whose root died after the walk had
# passed is re-probed on the next sweep; one empty pass per full sweep,
# the batch still bounding every pass). The seek's fresh-cursor face is
# a COALESCE to the below-everything sentinel (the nil uuid / seq 0 /
# the empty channel) — deliberately NOT an ``OR cursor IS NULL`` leg,
# which would cost the walk its PK index (the whole point of H-B1).
#
# THE AGE CLOCKS (one per leg, stated once): the flows-side pass ages by
# the ROOT's ``finished_at`` — the flow's terminalization stamp, the
# latest clock any of the flow's rows can carry (a row written before
# the flow terminalized is older than the stamp: the root's age SUBSUMES
# the row clocks, and one selection serves all the arms). The rootless
# pass ages by the ROW's own clock where the table has one
# (created_at/fired_at/updated_at/emitted_at — a rootless row cannot
# ride a root's stamp); wf_edge has NO timestamp column at all (checked:
# 01.00.24_02 + 01.00.25_01 — child/parent/flow_id/failure_policy), so
# its rootless class deletes on sight: the pointee's absence IS the age
# proof (a committed edge's root can only be absent by deletion).
#
# THE NULL-STAMP LEG (H-B4's conservative shape, the round-2 R2 flip):
# a terminal root whose ``finished_at`` was never stamped is SKIPPED —
# NEVER doomed. The first cut included it ("aged by nothing") and was
# degenerate BOTH ways: [idle] a seconds-old flow's entire history
# deleted under a 30-day period because its stamp never landed (the
# delete eager, the period meaningless); [busy] the ``ORDER BY
# finished_at NULLS LAST`` tail put the NULL-stamped roots past the
# LIMIT — never selected, never deleted, and the tripwire (keyed on
# deletion) NEVER fired. The cure: the delete never eager — no history
# is removed that has no honest age — and the tripwire always live: the
# flows-side statements OBSERVE the class (a ``null_observed`` count
# over the same jobs_finished_at_idx face — the NULLs sit contiguous per
# status; the observation is not the selection and cannot starve behind
# the batch), and the arm function warns, rate-limited to the leader's
# warn cadence (``wf-retention-null-finished-at``): fix the stamping bug
# or backfill the stamps — the rows stay until they carry an honest age.
#
# THE CLOCK VOCABULARY (H-B7): every comparison here is
# ``statement_timestamp()`` — the prune family's own selection-bound
# clock (STABLE: the bound is fixed at the statement's start, the
# two-clock law test_index_audit.py documents — a VOLATILE
# ``clock_timestamp()`` bound cannot be a btree index condition).
# One face across all the family's statements.
#
# The per-table row sources (checked against the actual migrations —
# 01.00.24_02, 01.00.25_01, 01.00.28_01, 01.00.29_01):
#
# * wf_edge keys rows by (child_id, parent_id) with a flow_id column;
# * wf_join_fire's creation clock is ``fired_at`` (its own name for
#   created_at, DEFAULT clock_timestamp());
# * wf_step_ledger / wf_signals carry ``created_at`` (wf_signals names
#   its flow column ``workflow_id``);
# * wf_node_progress carries ``updated_at`` (the upsert's latest-wins
#   stamp) and NO flow column — the flow is reached through the NODE's
#   jobs row (metadata.flow_id), the flow-nodes expression index
#   (01.00.26_02) serving the probe;
# * wf_node_stream carries ``emitted_at``, the flow_id column directly
#   (wf_node_stream_flow_seq_idx serves both the flows-side delete and
#   the SSE run-tail read), and the seq bigserial PK — its orphan class
#   (the node gone, or the carried flow not a root) rides the folded
#   rootless pass's second batch-bounded walk.
#
# The step ledger's per-row statuses ('running'/'succeeded'/'failed'/
# 'fenced'/'awaited') need NO guard here: a terminal flow's 'running'
# ledger row is the phantom class the reap arm fences first, and
# deleting it outright reconstructs the same (the row is gone, not
# lying — the flow stays reconstructible from rows alone). A terminal
# flow's HELD signals are exactly the abandoned-hold class — they
# delete with the flow (nothing will ever deliver them: the wait site
# is terminal).
EDGE_RETENTION_SQL = """\
WITH doomed_flows AS (
    -- THE FLOWS-SIDE DOOMED SELECTION (H-B1): the terminal-aged ROOTS
    -- first — a tiny jobs-side set via jobs_finished_at_idx; LIMIT $1
    -- bounds the flow set, the same batch bounds the row deletion
    -- below. The root identity (step_key = '__flow__', H-B3) is stated
    -- here, once per arm. The NULL-stamped terminal roots are NOT
    -- selected (R2's conservative shape: the delete never eager — a
    -- root with no honest age dooms nothing).
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at <= statement_timestamp() - $2::interval
    ORDER BY f.finished_at, f.id
    LIMIT $1
),
null_observed AS (
    -- THE STAMPING-BUG OBSERVER (R2): the NULL-stamped TERMINAL roots,
    -- counted — NOT selected (the tripwire is the observation, and the
    -- observation cannot starve behind the batch the way the old
    -- NULLS LAST tail did). The NULLs sit contiguous per status on
    -- jobs_finished_at_idx: the count is an index range probe, not a
    -- scan. The arm function warns, rate-limited, when this is > 0.
    SELECT count(*) AS n
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at IS NULL
),
doomed AS (
    -- Each selected flow's rows, through the table's flow_id index
    -- (wf_edge_flow_idx, 01.00.35_01); LIMIT $1 bounds the ROWS a pass
    -- deletes (a deep flow drains over passes, never unbounded in one).
    SELECT e.child_id, e.parent_id
    FROM {schema}.wf_edge e
    JOIN doomed_flows df ON df.id = e.flow_id
    LIMIT $1
),
deleted AS (
    DELETE FROM {schema}.wf_edge o USING doomed d
    WHERE o.child_id = d.child_id
      AND o.parent_id = d.parent_id
    RETURNING o.child_id
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT n FROM null_observed) AS null_observed
"""


EDGE_ROOTLESS_RETENTION_SQL = """\
WITH cur AS (
    -- THE PERSISTED CURSOR (01.00.36_01, the job_events_prune_state
    -- precedent; the round-2 R1's cure): the walk seeks strictly PAST
    -- the arm's last VISITED row — a live PK front's rows are visited
    -- once, answered 'live root', and the next pass starts behind them
    -- no more. FOR UPDATE serializes a concurrent pass of the same arm;
    -- the advance rides the DELETE's own transaction below.
    SELECT s.cursor_a AS cursor_a, s.cursor_b AS cursor_b
    FROM {schema}.wf_rootless_sweep_state s
    WHERE s.arm = 'wf_edge'
    FOR UPDATE
),
visited AS (
    -- THE ROOTLESS LEG'S BOUNDED WALK (H-B1's second pass): the
    -- PK-ordered scan visits AT MOST $1 rows (the LIMIT bounds the SCAN
    -- — the convicted shape's predicate walked the whole table when
    -- nothing matched), each visited row's pointee probed once by PK.
    -- THE SEEK past the cursor: COALESCE to the below-everything
    -- sentinel (the nil uuid) on a fresh/reset cursor — no OR leg, the
    -- PK index walk stays the plan.
    SELECT e.child_id, e.parent_id, e.flow_id
    FROM {schema}.wf_edge e
    WHERE (e.child_id, e.parent_id) > (
        COALESCE((SELECT cursor_a FROM cur),
                 '00000000-0000-0000-0000-000000000000')::uuid,
        COALESCE((SELECT cursor_b FROM cur),
                 '00000000-0000-0000-0000-000000000000')::uuid
    )
    ORDER BY e.child_id, e.parent_id
    LIMIT $1
),
doomed AS (
    -- THE ROOT IDENTITY (H-B3): the pointee must be a FLOW ROOT for the
    -- row to have a live owner — absent, or a live NON-root job, are
    -- one NOT-EXISTS. wf_edge has no clock of its own: the pointee's
    -- absence (or non-rootness) IS the age proof — a committed edge's
    -- flow column can only name a deleted or corrupted link — so this
    -- leg carries no age condition. A LIVE root (any status) survives;
    -- a TERMINAL root's rows are the flows-side pass's own (it deletes
    -- them by the same identity, aged).
    SELECT v.child_id, v.parent_id
    FROM visited v
    WHERE NOT EXISTS (
        SELECT 1 FROM {schema}.jobs f
        WHERE f.id = v.flow_id
          AND f.step_key = '__flow__'
    )
),
deleted AS (
    DELETE FROM {schema}.wf_edge o USING doomed d
    WHERE o.child_id = d.child_id
      AND o.parent_id = d.parent_id
    RETURNING o.child_id
),
last_visit AS (
    -- THE ADVANCE'S KEY: the last VISITED row — visited, not deleted
    -- (a live front's rows ARE the advance; R1).
    SELECT v.child_id, v.parent_id
    FROM visited v
    ORDER BY v.child_id DESC, v.parent_id DESC
    LIMIT 1
),
advanced AS (
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, cursor_b, updated_at)
    SELECT 'wf_edge',
           (SELECT child_id::text FROM last_visit),
           (SELECT parent_id::text FROM last_visit),
           statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a,
            cursor_b = EXCLUDED.cursor_b,
            updated_at = EXCLUDED.updated_at
),
rewound AS (
    -- THE WRAPAROUND: nothing above the cursor — the walk re-seeks from
    -- the start next pass (a row whose root died after the walk passed
    -- is re-probed; one empty pass per full sweep).
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, cursor_b = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_edge'
      AND (SELECT cursor_a FROM cur) IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM visited)
    RETURNING s.arm
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM visited) AS visited
"""


JOIN_FIRE_RETENTION_SQL = """\
WITH doomed_flows AS (
    -- THE FLOWS-SIDE DOOMED SELECTION (H-B1): see EDGE_RETENTION_SQL —
    -- the same terminal-aged ROOTS first, the same batch bounding both
    -- the flow set and the row deletion, the same NULL-stamp SKIP (R2).
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at <= statement_timestamp() - $2::interval
    ORDER BY f.finished_at, f.id
    LIMIT $1
),
null_observed AS (
    -- THE STAMPING-BUG OBSERVER (R2): see EDGE_RETENTION_SQL.
    SELECT count(*) AS n
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at IS NULL
),
doomed AS (
    -- The selected flows' fire rows, through wf_join_fire_flow_idx
    -- (01.00.35_01), LIMITed to the batch.
    SELECT o.id
    FROM {schema}.wf_join_fire o
    JOIN doomed_flows df ON df.id = o.flow_id
    LIMIT $1
),
deleted AS (
    DELETE FROM {schema}.wf_join_fire x USING doomed d
    WHERE x.id = d.id
    RETURNING x.id
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT n FROM null_observed) AS null_observed
"""


JOIN_FIRE_ROOTLESS_RETENTION_SQL = """\
WITH cur AS (
    -- THE PERSISTED CURSOR (01.00.36_01; R1's cure): see
    -- EDGE_ROOTLESS_RETENTION_SQL.
    SELECT s.cursor_a AS cursor_a
    FROM {schema}.wf_rootless_sweep_state s
    WHERE s.arm = 'wf_join_fire'
    FOR UPDATE
),
visited AS (
    -- THE ROOTLESS LEG'S BOUNDED WALK (H-B1's second pass): the
    -- PK-ordered scan visits AT MOST $1 rows, one PK probe each, SEEKING
    -- strictly past the cursor (the nil-uuid sentinel on a fresh/reset
    -- cursor — no OR leg, the PK index walk stays the plan).
    SELECT o.id, o.flow_id, o.fired_at
    FROM {schema}.wf_join_fire o
    WHERE o.id > COALESCE((SELECT cursor_a FROM cur),
                          '00000000-0000-0000-0000-000000000000')::uuid
    ORDER BY o.id
    LIMIT $1
),
doomed AS (
    -- THE ROOT IDENTITY (H-B3) + the row's own clock (the rootless
    -- class cannot ride a root's finished_at — the row's creation clock
    -- is its honest age).
    SELECT v.id
    FROM visited v
    WHERE v.fired_at <= statement_timestamp() - $2::interval
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.jobs f
          WHERE f.id = v.flow_id
            AND f.step_key = '__flow__'
      )
),
deleted AS (
    DELETE FROM {schema}.wf_join_fire o USING doomed d
    WHERE o.id = d.id
    RETURNING o.id
),
last_visit AS (
    -- max() does not exist over uuid: the last row in the walk's own
    -- order, picked by the walk's ORDER BY in reverse.
    SELECT v.id
    FROM visited v
    ORDER BY v.id DESC
    LIMIT 1
),
advanced AS (
    -- THE ADVANCE (R1): the last VISITED row — a live front's rows ARE
    -- the advance. The same transaction as the deletes above.
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, updated_at)
    SELECT 'wf_join_fire', (SELECT id::text FROM last_visit), statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a, updated_at = EXCLUDED.updated_at
),
rewound AS (
    -- THE WRAPAROUND: see EDGE_ROOTLESS_RETENTION_SQL.
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_join_fire'
      AND (SELECT cursor_a FROM cur) IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM visited)
    RETURNING s.arm
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM visited) AS visited
"""


STEP_LEDGER_RETENTION_SQL = """\
WITH doomed_flows AS (
    -- THE FLOWS-SIDE DOOMED SELECTION (H-B1): see EDGE_RETENTION_SQL.
    -- The NULL-stamped terminal roots are SKIPPED (R2's conservative
    -- shape), observed instead.
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at <= statement_timestamp() - $2::interval
    ORDER BY f.finished_at, f.id
    LIMIT $1
),
null_observed AS (
    -- THE STAMPING-BUG OBSERVER (R2): see EDGE_RETENTION_SQL.
    SELECT count(*) AS n
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at IS NULL
),
doomed AS (
    -- The selected flows' ledger rows through the CLAIM ARBITER's
    -- leading flow_id column (flow_id, step_key, COALESCE(map_index,-1),
    -- attempt — 01.00.28_01): the narrowest flow probe the table ships
    -- (wf_step_ledger_flow_idx is NOT built — redundant write
    -- amplification on the hottest table, H-B6).
    SELECT o.id
    FROM {schema}.wf_step_ledger o
    JOIN doomed_flows df ON df.id = o.flow_id
    LIMIT $1
),
deleted AS (
    DELETE FROM {schema}.wf_step_ledger x USING doomed d
    WHERE x.id = d.id
    RETURNING x.id
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT n FROM null_observed) AS null_observed
"""


STEP_LEDGER_ROOTLESS_RETENTION_SQL = """\
WITH cur AS (
    -- THE PERSISTED CURSOR (01.00.36_01; R1's cure): see
    -- EDGE_ROOTLESS_RETENTION_SQL.
    SELECT s.cursor_a AS cursor_a
    FROM {schema}.wf_rootless_sweep_state s
    WHERE s.arm = 'wf_step_ledger'
    FOR UPDATE
),
visited AS (
    -- THE ROOTLESS LEG'S BOUNDED WALK (H-B1's second pass): the
    -- PK-ordered scan visits AT MOST $1 rows, one PK probe each, SEEKING
    -- strictly past the cursor.
    SELECT o.id, o.flow_id, o.created_at
    FROM {schema}.wf_step_ledger o
    WHERE o.id > COALESCE((SELECT cursor_a FROM cur),
                          '00000000-0000-0000-0000-000000000000')::uuid
    ORDER BY o.id
    LIMIT $1
),
doomed AS (
    -- THE ROOT IDENTITY (H-B3) + the row's own clock.
    SELECT v.id
    FROM visited v
    WHERE v.created_at <= statement_timestamp() - $2::interval
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.jobs f
          WHERE f.id = v.flow_id
            AND f.step_key = '__flow__'
      )
),
deleted AS (
    DELETE FROM {schema}.wf_step_ledger o USING doomed d
    WHERE o.id = d.id
    RETURNING o.id
),
last_visit AS (
    -- max() does not exist over uuid: the walk's own ORDER BY in
    -- reverse (see JOIN_FIRE_ROOTLESS_RETENTION_SQL).
    SELECT v.id
    FROM visited v
    ORDER BY v.id DESC
    LIMIT 1
),
advanced AS (
    -- THE ADVANCE (R1): see EDGE_ROOTLESS_RETENTION_SQL.
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, updated_at)
    SELECT 'wf_step_ledger', (SELECT id::text FROM last_visit), statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a, updated_at = EXCLUDED.updated_at
),
rewound AS (
    -- THE WRAPAROUND: see EDGE_ROOTLESS_RETENTION_SQL.
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_step_ledger'
      AND (SELECT cursor_a FROM cur) IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM visited)
    RETURNING s.arm
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM visited) AS visited
"""


SIGNALS_RETENTION_SQL = """\
WITH doomed_flows AS (
    -- THE FLOWS-SIDE DOOMED SELECTION (H-B1): see EDGE_RETENTION_SQL.
    -- The NULL-stamped terminal roots are SKIPPED (R2's conservative
    -- shape), observed instead.
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at <= statement_timestamp() - $2::interval
    ORDER BY f.finished_at, f.id
    LIMIT $1
),
null_observed AS (
    -- THE STAMPING-BUG OBSERVER (R2): see EDGE_RETENTION_SQL.
    SELECT count(*) AS n
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at IS NULL
),
doomed AS (
    -- The selected flows' signal rows (the flow column's own name is
    -- workflow_id) through wf_signals_run_status_idx's leading flow
    -- key (01.00.28_01 — the table ships no separate flow index for
    -- exactly this reason), LIMITed to the batch.
    SELECT o.id
    FROM {schema}.wf_signals o
    JOIN doomed_flows df ON df.id = o.workflow_id
    LIMIT $1
),
deleted AS (
    DELETE FROM {schema}.wf_signals x USING doomed d
    WHERE x.id = d.id
    RETURNING x.id
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT n FROM null_observed) AS null_observed
"""


SIGNALS_ROOTLESS_RETENTION_SQL = """\
WITH cur AS (
    -- THE PERSISTED CURSOR (01.00.36_01; R1's cure): see
    -- EDGE_ROOTLESS_RETENTION_SQL.
    SELECT s.cursor_a AS cursor_a
    FROM {schema}.wf_rootless_sweep_state s
    WHERE s.arm = 'wf_signals'
    FOR UPDATE
),
visited AS (
    -- THE ROOTLESS LEG'S BOUNDED WALK (H-B1's second pass): the
    -- PK-ordered scan visits AT MOST $1 rows, one PK probe each, SEEKING
    -- strictly past the cursor.
    SELECT o.id, o.workflow_id, o.created_at
    FROM {schema}.wf_signals o
    WHERE o.id > COALESCE((SELECT cursor_a FROM cur),
                          '00000000-0000-0000-0000-000000000000')::uuid
    ORDER BY o.id
    LIMIT $1
),
doomed AS (
    -- THE ROOT IDENTITY (H-B3) + the row's own clock. A terminal
    -- flow's HELD signals (the abandoned-hold class) delete with the
    -- flow via the flows-side pass; the rootless class here is the
    -- same narration with no root left to age by.
    SELECT v.id
    FROM visited v
    WHERE v.created_at <= statement_timestamp() - $2::interval
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.jobs f
          WHERE f.id = v.workflow_id
            AND f.step_key = '__flow__'
      )
),
deleted AS (
    DELETE FROM {schema}.wf_signals o USING doomed d
    WHERE o.id = d.id
    RETURNING o.id
),
last_visit AS (
    -- max() does not exist over uuid: the walk's own ORDER BY in
    -- reverse (see JOIN_FIRE_ROOTLESS_RETENTION_SQL).
    SELECT v.id
    FROM visited v
    ORDER BY v.id DESC
    LIMIT 1
),
advanced AS (
    -- THE ADVANCE (R1): see EDGE_ROOTLESS_RETENTION_SQL.
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, updated_at)
    SELECT 'wf_signals', (SELECT id::text FROM last_visit), statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a, updated_at = EXCLUDED.updated_at
),
rewound AS (
    -- THE WRAPAROUND: see EDGE_ROOTLESS_RETENTION_SQL.
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_signals'
      AND (SELECT cursor_a FROM cur) IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM visited)
    RETURNING s.arm
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM visited) AS visited
"""


NODE_PROGRESS_RETENTION_SQL = """\
WITH doomed_flows AS (
    -- THE FLOWS-SIDE DOOMED SELECTION (H-B1): see EDGE_RETENTION_SQL.
    -- The NULL-stamped terminal roots are SKIPPED (R2's conservative
    -- shape), observed instead.
    SELECT f.id
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at <= statement_timestamp() - $2::interval
    ORDER BY f.finished_at, f.id
    LIMIT $1
),
null_observed AS (
    -- THE STAMPING-BUG OBSERVER (R2): see EDGE_RETENTION_SQL.
    SELECT count(*) AS n
    FROM {schema}.jobs f
    WHERE f.step_key = '__flow__'
      AND f.status IN {terminal}
      AND f.finished_at IS NULL
),
doomed AS (
    -- The selected flows' NODE ids first (the flow-nodes expression
    -- index, 01.00.26_02 — the uuid-cast expression the reads must
    -- spell letter for letter, the workflow-rows-only partial the
    -- quals must imply), then each node's progress rows by the PK probe
    -- (node_id, channel), LIMITed to the batch.
    SELECT wp.node_id, wp.channel
    FROM {schema}.wf_node_progress wp
    JOIN {schema}.jobs n ON n.id = wp.node_id
    JOIN doomed_flows df ON df.id = (n.metadata->>'flow_id')::uuid
    WHERE n.step_key <> '__flow__'
      AND n.metadata ? 'flow_id'
    LIMIT $1
),
deleted AS (
    DELETE FROM {schema}.wf_node_progress x USING doomed d
    WHERE x.node_id = d.node_id
      AND x.channel = d.channel
    RETURNING x.node_id
),
stream_doomed AS (
    -- THE SIXTH TABLE'S FLOWS-SIDE LEG (the round-2 R3's FOLD —
    -- documented call): wf_node_stream grew monotone-forever with the
    -- same five (a dead node's ring — nodes x 64 rows — never had a
    -- pruner); its doomed-flow identity is the SAME terminal-aged root
    -- set just selected, its rows carry the flow_id column DIRECTLY
    -- (the progress sibling has none), and wf_node_stream_flow_seq_idx
    -- (01.00.29_01) serves the delete. One pass, TWO batch-bounded
    -- deletes — the family's one-batch-per-pass law's documented
    -- allowance: each data-modifying CTE carries its own LIMIT $1, the
    -- pass's cost stays O(terminal flows + 2x batch). A LIVE node's ring
    -- is never touched here (its flow is not terminal-aged) — the ring
    -- trim owns the live side.
    SELECT o.seq
    FROM {schema}.wf_node_stream o
    JOIN doomed_flows df ON df.id = o.flow_id
    LIMIT $1
),
stream_deleted AS (
    DELETE FROM {schema}.wf_node_stream x USING stream_doomed d
    WHERE x.seq = d.seq
    RETURNING x.seq
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM stream_deleted) AS stream_deleted,
       (SELECT n FROM null_observed) AS null_observed
"""


NODE_PROGRESS_ROOTLESS_RETENTION_SQL = """\
WITH cur AS (
    -- THE PERSISTED CURSORS (01.00.36_01; R1's cure): see
    -- EDGE_ROOTLESS_RETENTION_SQL. The FOLD (R3) gives this statement
    -- TWO walks and TWO arm rows: the progress walk (the composite
    -- node_id/channel cursor) and the stream walk (the seq cursor).
    -- FOR UPDATE holds both rows against a concurrent pass.
    SELECT s.arm, s.cursor_a, s.cursor_b
    FROM {schema}.wf_rootless_sweep_state s
    WHERE s.arm IN ('wf_node_progress', 'wf_node_stream')
    FOR UPDATE
),
visited AS (
    -- THE PROGRESS WALK (H-B1's second pass): the PK-ordered scan
    -- visits AT MOST $1 rows, one probe set each, SEEKING strictly past
    -- the composite cursor (the nil-uuid / empty-channel sentinels on a
    -- fresh/reset cursor — no OR leg, the PK index walk stays the plan).
    SELECT wp.node_id, wp.channel, wp.updated_at
    FROM {schema}.wf_node_progress wp
    WHERE (wp.node_id, wp.channel) > (
        COALESCE((SELECT cursor_a FROM cur WHERE arm = 'wf_node_progress'),
                 '00000000-0000-0000-0000-000000000000')::uuid,
        COALESCE((SELECT cursor_b FROM cur WHERE arm = 'wf_node_progress'), '')
    )
    ORDER BY wp.node_id, wp.channel
    LIMIT $1
),
stream_visited AS (
    -- THE STREAM WALK (the R3 fold's second bounded walk): the seq
    -- PK-ordered scan, AT MOST $1 rows, SEEKING strictly past the seq
    -- cursor (0 = from the start).
    SELECT o.seq, o.node_id, o.flow_id, o.emitted_at
    FROM {schema}.wf_node_stream o
    WHERE o.seq > COALESCE(
        (SELECT cursor_a FROM cur WHERE arm = 'wf_node_stream'), '0'
    )::bigint
    ORDER BY o.seq
    LIMIT $1
),
doomed AS (
    -- THE NODE'S OWNER IDENTITY (H-B3): the progress row's owner is the
    -- NODE's flow link naming a FLOW ROOT. One NOT-EXISTS, three honest
    -- legs: the node GONE, the node WITHOUT the flow_id stamp (a
    -- vanilla node — the convicted unprunable class), or the pointee
    -- NOT a flow root (absent, or a live NON-root job). A node whose
    -- pointee IS a live root survives; a node whose pointee is a
    -- TERMINAL root's rows are the flows-side pass's own (aged by the
    -- root's finished_at there; the row's updated_at ages the
    -- rootless class here).
    SELECT v.node_id, v.channel
    FROM visited v
    WHERE v.updated_at <= statement_timestamp() - $2::interval
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.jobs n
          WHERE n.id = v.node_id
            AND n.metadata ? 'flow_id'
            AND EXISTS (
                SELECT 1 FROM {schema}.jobs f
                WHERE f.id = (n.metadata->>'flow_id')::uuid
                  AND f.step_key = '__flow__'
            )
      )
),
stream_doomed AS (
    -- THE STREAM'S ORPHAN CLASS (R3's node-join legs): the node GONE
    -- (a dead node's ring — nothing will ever trim or read it), or the
    -- CARRIED flow link naming a NON-root (absent, or a live NON-root
    -- job — the flow gone). A row whose node is live AND whose carried
    -- flow is a root (live or terminal) survives: the live side is the
    -- ring trim's, the terminal side the flows-side pass's.
    SELECT v.seq
    FROM stream_visited v
    WHERE v.emitted_at <= statement_timestamp() - $2::interval
      AND (
          NOT EXISTS (
              SELECT 1 FROM {schema}.jobs n WHERE n.id = v.node_id
          )
          OR NOT EXISTS (
              SELECT 1 FROM {schema}.jobs f
              WHERE f.id = v.flow_id
                AND f.step_key = '__flow__'
          )
      )
),
deleted AS (
    DELETE FROM {schema}.wf_node_progress o USING doomed d
    WHERE o.node_id = d.node_id
      AND o.channel = d.channel
    RETURNING o.node_id
),
stream_deleted AS (
    DELETE FROM {schema}.wf_node_stream o USING stream_doomed d
    WHERE o.seq = d.seq
    RETURNING o.seq
),
last_visit AS (
    SELECT v.node_id, v.channel
    FROM visited v
    ORDER BY v.node_id DESC, v.channel DESC
    LIMIT 1
),
stream_last_visit AS (
    SELECT max(v.seq) AS seq FROM stream_visited v
),
advanced AS (
    -- THE ADVANCE (R1): the last VISITED row per walk — a live front's
    -- rows ARE the advance. The same transaction as the deletes above.
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, cursor_b, updated_at)
    SELECT 'wf_node_progress',
           (SELECT node_id::text FROM last_visit),
           (SELECT channel FROM last_visit),
           statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a,
            cursor_b = EXCLUDED.cursor_b,
            updated_at = EXCLUDED.updated_at
),
stream_advanced AS (
    INSERT INTO {schema}.wf_rootless_sweep_state (arm, cursor_a, updated_at)
    SELECT 'wf_node_stream', (SELECT seq::text FROM stream_last_visit), statement_timestamp()
    WHERE EXISTS (SELECT 1 FROM stream_visited)
    ON CONFLICT (arm) DO UPDATE
        SET cursor_a = EXCLUDED.cursor_a, updated_at = EXCLUDED.updated_at
),
rewound AS (
    -- THE WRAPAROUND per walk: see EDGE_ROOTLESS_RETENTION_SQL.
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, cursor_b = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_node_progress'
      AND (SELECT cursor_a FROM cur WHERE arm = 'wf_node_progress') IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM visited)
    RETURNING s.arm
),
stream_rewound AS (
    UPDATE {schema}.wf_rootless_sweep_state s
    SET cursor_a = NULL, updated_at = statement_timestamp()
    WHERE s.arm = 'wf_node_stream'
      AND (SELECT cursor_a FROM cur WHERE arm = 'wf_node_stream') IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM stream_visited)
    RETURNING s.arm
)
SELECT (SELECT count(*) FROM deleted) AS deleted,
       (SELECT count(*) FROM stream_deleted) AS stream_deleted,
       (SELECT count(*) FROM visited) AS visited,
       (SELECT count(*) FROM stream_visited) AS stream_visited
"""


def render_rootless_retention_sql(template: str, schema: str) -> str:
    """The ROOTLESS-leg statements' renderer (H-B1's second pass).

    The flows-side statements ride the :class:`WorkflowSql` bundle's own
    fields (the bundle renders them in ``build``); the rootless
    statements render HERE rather than growing the bundle, so the
    two-pass law lives entirely in this module. The substitution is the
    bundle's own discipline — the schema identifier validated (and the
    ONLY interpolated value), ``$N`` binding for everything a caller
    controls. The rootless statements carry no ``{terminal}`` token (the
    root identity is a step-key check, never a status list), so no
    import of the bundle is needed — _sql.py imports THIS module's
    constants, the reverse import would be a cycle.

    Each rootless statement is SELF-CONTAINED on its cursor (01.00.36_01,
    the round-2 R1's cure): it seeks past the arm's persisted
    ``wf_rootless_sweep_state`` row, advances that row to the last
    VISITED key, and rewinds it on the exhausted pass — all in the
    delete's own statement/transaction, the job_events_prune_state
    watermark's own law. The caller binds (batch[, retention]) and
    nothing else.
    """
    require_schema(schema)
    return template.replace("{schema}", schema).replace("{{", "{").replace("}}", "}")
