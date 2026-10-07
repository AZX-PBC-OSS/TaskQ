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
    SELECT e.child_id,
           count(*) FILTER (WHERE p.id IS NULL) AS missing_parents,
           count(*) FILTER (
               WHERE p.id IS NOT NULL
                 AND p.status NOT IN {terminal}
           ) AS unterminal
    FROM {schema}.wf_edge e
    JOIN locked l ON l.id = e.child_id
    LEFT JOIN {schema}.jobs p ON p.id = e.parent_id
    GROUP BY e.child_id
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
      AND c.missing_parents > 0
      AND NOT j.metadata @> '{{"blocking_reason": "orphan_parent"}}'::jsonb
    RETURNING j.id
),
reconciled AS (
    UPDATE {schema}.jobs j
    SET deps_pending = c.unterminal::smallint
    FROM counts c
    WHERE j.id = c.child_id
      AND c.missing_parents = 0
      AND j.deps_pending <> c.unterminal::smallint
    RETURNING j.id
),
firable AS (
    SELECT l.id, l.flow_id, l.step_key
    FROM locked l
    JOIN counts c ON c.child_id = l.id
    WHERE c.missing_parents = 0
      AND c.unterminal = 0
)
SELECT
    (SELECT count(*) FROM blocked) AS blocked,
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
    SELECT e.child_id,
           count(*) FILTER (WHERE p.id IS NULL) AS missing_parents,
           count(*) FILTER (
               WHERE p.id IS NOT NULL
                 AND p.status NOT IN {terminal}
           ) AS unterminal
    FROM {schema}.wf_edge e
    JOIN locked l ON l.id = e.child_id
    LEFT JOIN {schema}.jobs p ON p.id = e.parent_id
    GROUP BY e.child_id
),
firable AS (
    SELECT f.id, f.flow_id, f.step_key,
           row_number() OVER (ORDER BY f.id) AS rn
    FROM (
        SELECT l.id, l.flow_id, l.step_key
        FROM locked l
        JOIN counts c ON c.child_id = l.id
        WHERE c.missing_parents = 0
          AND c.unterminal = 0
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
    RETURNING join_job_id, flow_id, step_key
)
SELECT w.join_job_id, w.step_key, w.flow_id, j.metadata->'consumers' AS consumers
FROM wins w
JOIN {schema}.jobs j ON j.id = w.join_job_id
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
RETURNING l.id
"""
