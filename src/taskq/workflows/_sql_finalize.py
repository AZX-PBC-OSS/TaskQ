"""The finalize-path statement constants (T04): the terminal-mark fence, the guarded decrement, the guarded fire, the outbox insert, the node/fork writes.

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

from typing import Final

#: THE FAN-IN'S BYTE CAP (T18's UNBOUNDED-JSONB policy, GAPS-ESTATE D5):
#: the collect's ``failures`` jsonb truncates here — over the cap the
#: join row COMPACTS to the bounded summary (the ``__truncated__`` marker
#: + the ledger pointer); the full detail stays on the ledger/attempts.
#: Sized to keep a 1000-child collect's row comfortably bounded while a
#: normal collect's items never compact.
FANIN_FAILURES_BYTE_CAP: Final[int] = 64 * 1024

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
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[])
"""


# ── T20: THE EMIT TX — the streaming source's per-page statement group ──
# The certified fork shapes RE-BOUND (the children INSERT + the edge rows
# are byte-shape FORK_CHILDREN_SQL / FORK_EDGES_SQL — named separately so
# the emit's call site and its pins grep THIS concern) + the ONE new leg:
# the CURSOR CHECKPOINT on the source row's own metadata (no new table,
# no new column), guarded by the FULL dispatch fence. ONE transaction
# (the fork-atomicity law at page granularity): a kill at ANY statement
# window — including AFTER the cursor write but before the commit — rolls
# the children, the edges AND the cursor back together; the reclaim
# re-pends the source (it never finalized), the re-claim re-emits exactly
# the lost page.
#
# THE REFUTED-CLAIM DISCIPLINE (the spike's 198 UniqueViolations — the
# design's proof the discriminator is load-bearing): the children's
# idempotency keys are PARENT-SCOPED at the EMIT scope
# (``wf:{flow}:emit:{map_index}:{step}``) and carry the per-child
# ``map_index`` — the fork key's discipline (pin 18) at chain level, plus
# the per-child trace_id the drill-down reads.
EMIT_CHILDREN_SQL = """\
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


EMIT_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[])
"""


# THE CURSOR CHECKPOINT — the fence carrier. A ZOMBIE source (its claim
# superseded by the reclaim + re-claim) updates NOTHING → the emit
# refuses (EmitFencedError) → the children above roll back with the tx.
EMIT_CURSOR_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
  AND status = 'running'
  AND locked_by_worker = $3
  AND attempt = $4
  AND claim_epoch = $5
RETURNING id
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


#: THE FORK'S CONSUMER EDGES (the map-join consumption cure — the
#: ecosystem mapper's defect): the join's DOWNSTREAM consumers are wired
#: like any node — the consumer's dep was RESERVED at create (the runner's
#: static insert counts the fork-spawned join's edge before the join's id
#: exists), so THIS edge row is the ledger's truth that releases it: the
#: consumer dispatches strictly after the JOIN'S OWN TERMINAL (the join
#: row's claim + default packer writes the collected result), and the
#: consumer's arg resolution reads the join's result through the SAME
#: typed door as any node result (the parent-results query on the edge
#: ledger). The outbox's consumer insert stays as the belt (the arbiter's
#: idempotency key conflicts with the static row — no double dispatch).
FORK_CONSUMER_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT c.id, $1, $2, p.policy
FROM unnest($3::text[], $4::text[]) AS p(key, policy)
JOIN {schema}.jobs c ON c.step_key = p.key
  AND (c.metadata->>'flow_id')::uuid = $2
"""


# The PUBLIC edge writer (the fork's FORK_EDGES_SQL is the fork-internal
# one; this is the single-edge insert the public join path uses): a joined
# node's incoming edges ARE the join counter's truth — a join declared
# without them is a stranded invisible join (the rederive arm diagnoses it
# metadata.blocking_reason='orphan_parent'; the declarative API refuses it
# at build time, definitions.validate_fork / validate_join_spec).
NODE_EDGE_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
VALUES ($1, $2, $3, $4)
"""


FLOW_STATUS_SQL = """\
SELECT id, status FROM {schema}.jobs WHERE id = $1
"""


# ── T06: the failed-parent propagation ──────────────────────────────────

# The FAIL-CLOSED peer-cascade: a parent's TERMINAL failure resolves every
# fail_closed join counting this parent by a FLOW-SCOPED transition set,
# ONE statement (one snapshot, one tx — the cascade is the linearization,
# the same shape cancel's flip is):
#
#   * the joined node → blocked: blocking_reason='failed_parent' + the
#     failed parent NAMED on the record (metadata.failed_parent) — the
#     joined node's side of the counter is resolved by THIS stamp (the
#     rederive arm locks blocking_reason='join' rows only, so a stamped
#     row is never re-reconciled, never fired — it rests visible-blocked,
#     never a hanging join);
#   * the running peers (the blocked joins' OTHER parents, still
#     non-terminal, NOT join-wait themselves — a nested join's own counter
#     is its ledger's truth and resolves through the sweep's recount once
#     ITS parents terminal) are PEER-CANCELLED with the record:
#     error_class = the cancel-origin marker (the `by` leg, the same
#     outcome reads the same way whichever path produced it) + the
#     structured record in metadata.peer_cancel
#     ({by: 'peer_failure', cascade_from: <the failed node>});
#   * the workflow → failed (§17.2's cascade): the flow root's flip, the
#     linearization point every flow-status leg then reads.
#
# Every write is guarded: the block only join-wait rows not already
# stamped; the cancel only pending/scheduled/running rows; the flow flip
# only a non-terminal root AND only when the cascade actually blocked a
# join (no fail_closed edge → no cascade → the collect-only failure never
# touches the flow). A re-run of this statement after a rolled-back tx is
# idempotent — a fenced/terminal state updates nothing.
FAIL_CLOSED_CASCADE_SQL = """\
WITH edges AS (
    SELECT e.child_id
    FROM {schema}.wf_edge e
    WHERE e.parent_id = $1::uuid
      AND e.failure_policy = 'fail_closed'
),
blocked AS (
    UPDATE {schema}.jobs j
    SET metadata = j.metadata || $2::jsonb
    FROM edges
    WHERE j.id = edges.child_id
      AND j.status = 'pending'
      AND j.deps_pending > 0
      AND j.metadata @> '{{"blocking_reason": "join"}}'::jsonb
      AND NOT j.metadata @> '{{"blocking_reason": "failed_parent"}}'::jsonb
    RETURNING j.id
),
peers AS (
    UPDATE {schema}.jobs p
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        error_class = $3::text,
        metadata = p.metadata || $4::jsonb
    FROM edges e
    JOIN {schema}.wf_edge sib ON sib.child_id = e.child_id
    WHERE p.id = sib.parent_id
      AND p.id <> $1::uuid
      AND p.status IN ('pending', 'scheduled', 'running')
      AND NOT (p.deps_pending > 0
               AND p.metadata @> '{{"blocking_reason": "join"}}'::jsonb)
    RETURNING p.id
),
flow_failed AS (
    UPDATE {schema}.jobs f
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = $3::text
    WHERE f.id = $5::uuid
      AND f.status NOT IN {terminal}
      -- The flow fails only when the cascade actually RESOLVED a
      -- fail_closed join of this parent: no fail_closed edge → no
      -- cascade → the collect-only failure never touches the flow.
      AND EXISTS (SELECT 1 FROM blocked)
    RETURNING f.id
)
SELECT (SELECT count(*) FROM blocked) AS blocked,
       (SELECT count(*) FROM peers) AS peers_cancelled,
       (SELECT count(*) FROM flow_failed) AS flow_failed
"""


# The ABSORBED fan-in (T06's collect / T07's maybe): a failed child whose
# edge declared an ABSORBING policy does NOT cascade — its failure fans in
# as the typed FailureInfo item APPENDED to each absorbing join's
# ``metadata.failures`` array (the full attempt history rides the item —
# the ledger rows are read in THIS statement; the detail never leaves the
# ledger/attempts). The join row's counters still resolve through the
# decrement (the failed child IS resolved): the join fires when the LAST
# child terminalizes, strictly after every ladder attempt (the fan-in
# lands in the same tx, before the fire's decrement reads 0). The
# statement returns the HISTORY + THE EDGE'S OWN POLICY (the item names
# the policy that absorbed it — the envelope must not lie about which
# policy ran, T07's C); the engine builds the typed item through the
# FailureInfo door — one wire shape, never a re-spelled envelope.
COLLECT_FAN_IN_SQL = """\
WITH history AS (
    SELECT jsonb_agg(
               jsonb_build_object(
                   'attempt', l.attempt,
                   'error_class', l.error_class,
                   'error_message', l.error_message
               ) ORDER BY l.attempt
           ) AS attempts
    FROM {schema}.wf_step_ledger l
    WHERE l.flow_id = $2::uuid
      AND l.step_key = $3::text
      AND COALESCE(l.map_index, -1) = COALESCE($4::smallint, -1)
      AND l.status IN ('succeeded', 'failed')
)
SELECT j.id AS join_job_id,
       e.failure_policy AS policy,
       COALESCE((SELECT history.attempts FROM history), '[]'::jsonb) AS attempts
FROM {schema}.jobs j
JOIN {schema}.wf_edge e ON e.child_id = j.id
WHERE e.parent_id = $1::uuid
  AND e.failure_policy IN ('collect', 'maybe')
  AND j.status = 'pending'
  AND j.deps_pending > 0
"""


# The fan-in's append (the absorbing join row's ``failures`` array grows
# by THIS child's item) — keyed per join row (the fan-in read above
# returns the joins; this write lands the item). Keyed single row; the
# append is jsonb concat on the array.
# The fan-in's append, BOUNDED (T18's UNBOUNDED-JSONB policy, GAPS-ESTATE
# D5 — the ONE home): the collect's ``failures`` array truncates at the
# byte cap — over the cap, the row COMPACTS: the array is replaced by the
# bounded summary (the ``"__truncated__": N`` marker counting the items no
# longer spelled on the row + the ledger pointer), and the FULL detail
# stays on the ledger/attempts (the record never loses it — the ledger is
# the collector's source of truth, the join row the bounded summary).
# Keyed per join row; the append is jsonb concat on the array. The three
# arms, in order: (1) the row already compacted (a non-array summary) →
# the marker increments, the row stays bounded forever; (2) the append
# fits → the array grows by the item; (3) the append would exceed the cap
# → the compaction (marker = every item the summary no longer spells).
COLLECT_FAN_IN_APPEND_SQL = """\
UPDATE {schema}.jobs j
SET metadata = jsonb_set(
        j.metadata,
        '{{failures}}',
        CASE
            WHEN jsonb_typeof(COALESCE(j.metadata->'failures', '[]'::jsonb)) <> 'array'
                THEN jsonb_set(
                        j.metadata->'failures',
                        '{{__truncated__}}',
                        to_jsonb(
                            COALESCE(
                                (j.metadata->'failures'->>'__truncated__')::int, 0
                            ) + 1
                        )
                    )
            WHEN octet_length(
                     (
                         COALESCE(j.metadata->'failures', '[]'::jsonb) || $2::jsonb
                     )::text
                 ) <= $3::int
                THEN COALESCE(j.metadata->'failures', '[]'::jsonb) || $2::jsonb
            ELSE jsonb_build_object(
                     '__truncated__',
                     jsonb_array_length(COALESCE(j.metadata->'failures', '[]'::jsonb))
                         + 1,
                     'detail_home',
                     'the FailureInfo details live in wf_step_ledger (the '
                     'ledger/attempts); this row carries the bounded summary',
                     'flow_id',
                     j.metadata->>'flow_id'
                 )
        END,
        true
    )
WHERE j.id = $1
RETURNING j.id
"""


# The failed child's ABSORBED-side decrement (T06/T07): DECREMENT_SQL's
# shape scoped to the ABSORBING policies (collect | maybe — the edges
# whose declared policy absorbs the failure) — a fail_closed edge's side
# is resolved by the cascade's block stamp (never by a decrement: the
# join must not become firable over a failed parent).
#
# THE FLOW-ALIVE GUARD IS THE FENCE, NOT THE POLICY (the phase-2 attack's
# H2): when the flow is ALREADY terminal (the fail_closed leg flipped it
# in this same tx), the refusing decrement is a FENCED resolution — and a
# fenced decrement is NOT absorption. The edge's declared POLICY alone
# absorbs nothing: the sweep's flow-fenced arm (REDERIVE_SWEEP_SQL) gives
# the join the blocked-with-reason terminal state, and the absorption
# record (WORKFLOW_NODES_SQL / the maintenance leg's has_failed) reads
# the join row's terminal-block stamp — never the edge's declaration —
# so the derivation can say 'failed' for the failed-closed run (the
# envelope never lies, T07's C).
DECREMENT_ABSORBED_SQL = """\
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
  AND e.failure_policy IN ('collect', 'maybe')
  AND j.deps_pending > 0
  AND j.status = 'pending'
  AND EXISTS (SELECT 1 FROM flow_alive)
RETURNING j.id, j.deps_pending
"""
