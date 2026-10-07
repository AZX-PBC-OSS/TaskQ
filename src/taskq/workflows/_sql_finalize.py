"""The finalize-path statement constants (T04): the terminal-mark fence, the guarded decrement, the guarded fire, the outbox insert, the node/fork writes.

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

TERMINAL_MARK_SQL = """\
UPDATE {schema}.jobs
SET status = $2::"{schema}".job_status,
    finished_at = clock_timestamp(),
    result = $3::jsonb,
    result_size_bytes = $4,
    error_class = $5,
    error_message = $6,
    error_traceback = $7
WHERE id = $1
  -- THE TERMINAL-MARK FENCE (P3 rule 6: the CAS IS the shape guard) —
  -- status + worker + ATTEMPT + claim_epoch. The attempt is the fencing
  -- token (hardening H8): a status-only CAS loses to the zombie on the
  -- next attempt's re-claim, corrupting the result AND the counter. A
  -- fenced-out write updates nothing, and tx2 never executes (the rowcount
  -- gate — 50 duplicate finalizes → 1 decrement).
  AND status = 'running'
  AND locked_by_worker = $8
  AND attempt = $9
  AND claim_epoch = $10
RETURNING id, status, attempt
"""


# tx2's guarded decrement: ONE atomic statement, the decrement owned
# exclusively by this statement (the eighth rule: derived values are written
# only by the statement that derives them). Guards, each load-bearing (the
# pytest-gremlins mutation surface):
#   * `j.deps_pending > 0` — the rowcount gate; flipping to `>= 0` produces
#     the deps = -1 fingerprint (the join strands).
#   * the flow-status EXISTS leg — the fire guard's leg (P3 rule 4); the
#     dispatch fence + the finalize fence + this leg — any two are
#     insufficient.
#   * the edge join — the edge ledger is the ONLY decrement authority
#     (COUNTER-AS-CACHE / LEDGER-AS-TRUTH).
#
# A promoted nested map's reduce node finalizes through this same statement:
# its edges point at the GRANDPARENT's join, so the grandparent's join
# decrements in this same transaction — nested maps, hence a depth-N DAG,
# stay expressible.
DECREMENT_SQL = """\
WITH flow_alive AS (
    SELECT 1 AS ok
    FROM {schema}.jobs f
    WHERE f.id = $2
      AND f.status NOT IN {terminal}
)
UPDATE {schema}.jobs j
SET deps_pending = j.deps_pending - 1
FROM {schema}.wf_edge e
WHERE e.parent_id = $1
  AND e.child_id = j.id
  AND j.deps_pending > 0
  AND j.status = 'pending'
  AND EXISTS (SELECT 1 FROM flow_alive)
RETURNING j.id, j.deps_pending
"""


# The guarded fire: exactly-once via UNIQUE(join_job_id); premature fires
# refused by `j.deps_pending = 0` (the counter hit zero) + the join-wait
# status + the flow-status EXISTS leg INSIDE the fire statement (pin 5: the
# sweep's fire carries the same leg — a post-cancel re-derive refuses, the
# unfenced variant stays RED forever).
FIRE_SQL = """\
WITH wins AS (
    INSERT INTO {schema}.wf_join_fire (id, join_job_id, flow_id, step_key, fired_by)
    SELECT $3, j.id, $2, j.step_key, $4
    FROM {schema}.jobs j
    WHERE j.id = $1
      AND j.deps_pending = 0
      AND j.status = 'pending'
      AND EXISTS (
          SELECT 1 FROM {schema}.jobs f
          WHERE f.id = $2
            AND f.status NOT IN {terminal}
      )
    ON CONFLICT (join_job_id) DO NOTHING
    RETURNING join_job_id, step_key
)
SELECT w.join_job_id, w.step_key, j.trace_id, j.metadata->'consumers' AS consumers
FROM wins w
JOIN {schema}.jobs j ON j.id = w.join_job_id
"""


OUTBOX_INSERT_SQL = """\
INSERT INTO {schema}.wf_outbox
    (id, join_job_id, flow_id, consumer_step_key, map_index, bindings)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)
"""


# The single-node insert (the workflow-row enqueue path's core write; T09's
# API wraps it). A joined node is born with deps_pending = <declared parent
# count> and blocking_reason='join' in metadata; vanilla enqueues never set
# it (DEFAULT 0 — a semantic no-op for them). trace_id is stamped on the
# same insert (§18.2's cheap survivor — one field, no extra write).
NODE_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, map_index, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14)
"""


# The fork's child INSERT: parallel-array binds (one statement per chunk —
# never one round trip per child, the 1000-child fan-out tx band's shape).
# status is omitted — every child is born 'pending' (the column DEFAULT);
# retry_kind rides the vanilla vocabulary.
FORK_CHILDREN_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::text[], $7::uuid[], $8::smallint[], $9::text[], $10::text[],
    $11::jsonb[], $12::text[], $13::text[]
)
"""


FORK_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[])
"""


FORK_JOIN_NODE_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
"""


FORK_JOIN_CONSUMERS_SQL = """\
UPDATE {schema}.jobs
SET metadata = jsonb_set(metadata, '{consumers}', $2::jsonb, true)
WHERE id = $1
"""


FLOW_STATUS_SQL = """\
SELECT id, status FROM {schema}.jobs WHERE id = $1
"""
