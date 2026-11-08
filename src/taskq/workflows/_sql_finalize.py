"""The finalize-path statement constants: the terminal-mark fence, the guarded decrement, the guarded fire, the outbox insert, the node/fork writes.

The schema identifier is the only caller-controlled interpolated value
(validated at WorkflowSql.build); the one further token
(``{min_deferral_interval}`` — the deferral floor fragment, a constant
rendered from ``backend/_sql_fragments.MIN_DEFERRAL_INTERVAL_SQL`` so the
SQL and the in-memory twin cannot drift) is fixed vocabulary, never
caller input. Every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

from typing import Final

#: the collect's ``failures`` jsonb truncates here — over the cap the join row
#: compacts to the bounded summary (the ``__truncated__`` marker + the ledger
#: pointer); the full detail stays on the ledger/attempts. Sized to keep a
#: 1000-child collect's row comfortably bounded while a normal collect's items
#: never compact.
FANIN_FAILURES_BYTE_CAP: Final[int] = 64 * 1024

#: the bounded collect's join row carries the ``__truncated__`` count in its
#: metadata (``wf_collect_truncated``) alongside the envelope's own markers —
#: the decode unwraps the envelope at the consumer's typed boundary (the
#: declared ``list[...]`` arrives), so the count must surface where the reads
#: decide, never be swallowed by the unwrap.
COLLECT_TRUNCATION_RECEIPT_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
"""

TERMINAL_MARK_SQL = """\
WITH upd AS (
    UPDATE {schema}.jobs
    SET status = $2::"{schema}".job_status,
        finished_at = clock_timestamp(),
        result = $3::jsonb,
        result_size_bytes = $4,
        error_class = $5,
        error_message = $6,
        error_traceback = $7,
        -- THE RESULT'S EXPIRY (the vanilla mark_succeeded's
        -- lateral equivalent): the SUCCEEDED terminal stamps
        -- result_expires_at from the ROW'S OWN COHORT's stored
        -- actor_config.result_ttl (the projection seeded it — the
        -- operator owns the row thereafter), priced from the completion
        -- instant (clock_timestamp()), so a long-queued step does not
        -- complete already expired. A FAILED terminal stamps NO expiry
        -- (the vanilla mark_failed's parity — there is no result to
        -- expire), and a cohort with no TTL writes NULL (the COALESCE's
        -- fallback keeps the row's own value).
        result_expires_at = CASE WHEN $2::text = 'succeeded' THEN COALESCE(
            (SELECT clock_timestamp() + ac.result_ttl * interval '1 second'
             FROM {schema}.actor_config ac WHERE ac.actor = {schema}.jobs.actor),
            result_expires_at
        ) ELSE result_expires_at END
    WHERE id = $1
      -- THE TERMINAL-MARK FENCE (P3 rule 6: the CAS IS the shape guard) —
      -- status + worker + ATTEMPT + claim_epoch. The attempt is the fencing
      -- token (the attempt-fencing law): a status-only CAS loses to the zombie on the
      -- next attempt's re-claim, corrupting the result AND the counter. A
      -- fenced-out write updates nothing, and tx2 never executes (the rowcount
      -- gate — 50 duplicate finalizes → 1 decrement).
      AND status = 'running'
      AND locked_by_worker = $8
      AND attempt = $9
      AND claim_epoch = $10
    RETURNING id, status, attempt, started_at, scheduled_at, payload, trace_id
),
-- THE ATTEMPT LEDGER'S TERMINAL ROW (the deploy matrix's audit fix):
-- the workflow node's terminal write records the attempt row the SAME
-- way every vanilla terminal write does (mark_succeeded / mark_failed
-- shape): the shared tier invariant reads a terminal row's OWN attempt
-- here — a wf row terminal without it is "a mutation with no audit
-- row", the conservation's false alarm and the audit trail's real hole.
-- THE HOLDER CTE (the FK idiom every job_attempts insert shares): the
-- worker id rides the row ONLY when the worker row EXISTS (a reaped
-- worker's terminal write records a NULL worker_id — the FK's ON
-- DELETE SET NULL face; a raw $8 would violate the FK on a reaped
-- holder).
holder AS (
    SELECT id FROM {schema}.workers WHERE id = $8 FOR KEY SHARE
),
att AS (
    INSERT INTO {schema}.job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata, due_at)
    SELECT upd.id, upd.attempt,
           -- A row claimed before started_at existed (a hand-built test
           -- row, a legacy shape) records the terminal instant as its
           -- start — the attempt LEDGER is the record of the attempt,
           -- and NOT NULL is the ledger's own law (the sweep's shape).
           COALESCE(upd.started_at, clock_timestamp()),
           clock_timestamp(), $2::text,
           $5, $6, $7,
           trunc(EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(upd.started_at, clock_timestamp()))) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb,
           upd.scheduled_at
    FROM upd
    ON CONFLICT (job_id, attempt) DO NOTHING
),
-- THE STATE_CHANGE EVENT rides the same transaction (the audit trail's
-- law: a terminal transition an events reader cannot see never happened).
evt AS (
    INSERT INTO {schema}.job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object('from_state', 'running',
                                                'to_state', $2::text,
                                                'worker_id', $8::text,
                                                'error_class', $5::text))
    FROM upd
)
SELECT id, status, attempt, payload, trace_id FROM upd
"""


# tx2's guarded decrement: one atomic statement, the decrement owned
# exclusively by this statement (the eighth rule: derived values are written
# only by the statement that derives them). Guards, each load-bearing (the
# pytest-gremlins mutation surface):
#   * `j.deps_pending > 0` — the rowcount gate; flipping to `>= 0` produces
#     the deps = -1 fingerprint (the join strands).
#   * the flow-status EXISTS leg — the fire guard's leg (P3 rule 4); the
#     dispatch fence + the finalize fence + this leg — any two are
#     insufficient.
#   * the edge join — the edge ledger is the only decrement authority
#     (counter-as-cache / ledger-as-truth).
#
# A promoted nested map's reduce node finalizes through this same statement:
# its edges point at the grandparent's join, so the grandparent's join
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
# status + the flow-status EXISTS leg inside the fire statement (pin 5: the
# sweep's fire carries the same leg — a post-cancel re-derive refuses, the
# unfenced variant stays red forever).
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


# The single-node insert (the workflow-row enqueue path's core write; the
# API wraps it). A joined node is born with deps_pending = <declared parent
# count> and blocking_reason='join' in metadata; vanilla enqueues never set
# it (default 0 — a semantic no-op for them). trace_id is stamped on the
# same insert (the cheap survivor — one field, no extra write).
# priority is the resolved value the caller computed (per-call > the
# declaration > the column default 0) — the priority surface's enqueue
# face; the unset case binds the column default's own value, so the
# pre-fix rows are byte-identical.
# start_to_close is the row's declared deadline: the resolved value the
# caller computed, stamped into the
# column the vanilla rows always had (01.00.00_01:85 — nullable, no
# default); the unset case binds NULL — the pre-fix row byte-identical
# (the enforcement falls to the worker's default_start_to_close at claim
# time, the resolution chain's own tail).
# fairness_key is the row's dispatch cohort (the wf fairness stamp):
# every claimable wf row is born with its run's flow_id as the value, so
# the round-robin dispatch partitions the run's rows into one cohort per
# run — a deep run's backlog cannot occupy the whole round against a
# shallow run's. The flow root stays unstamped (it is never work — the
# dispatch fence's root-marker leg excludes it).
NODE_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, map_index, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key, priority, start_to_close, fairness_key,
     required_worker_schema, retry_base_seconds, retry_cap_seconds,
     retry_backoff, retry_jitter)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
"""


# The fork's child INSERT: parallel-array binds (one statement per chunk —
# never one round trip per child, the 1000-child fan-out tx band's shape).
# status is omitted — every child is born 'pending' (the column default);
# retry_kind rides the vanilla vocabulary. priority is the child's resolved
# value (the builder's precedence arithmetic — the priority surface); the
# unset case binds 0, the column default's own value (pre-fix identical).
# start_to_close is the child's declared deadline (the column the
# vanilla rows always had); the unset case binds
# NULL (pre-fix identical).
# fairness_key is the child's dispatch cohort (the wf fairness stamp):
# one cohort per run (the flow_id), so the round-robin dispatch
# interleaves runs instead of draining them as one FIFO stream.
FORK_CHILDREN_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key, priority, start_to_close, fairness_key,
     retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter,
     required_worker_schema)
SELECT fanout.*, parent.required_worker_schema
FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::text[], $7::uuid[], $8::smallint[], $9::text[], $10::text[],
    $11::jsonb[], $12::text[], $13::text[], $14::smallint[], $15::interval[],
    $16::text[], $17::double precision[], $18::double precision[],
    $19::text[], $20::double precision[]
) AS fanout(
    id, actor, queue, payload, max_attempts, retry_kind,
    parent_id, map_index, step_key, trace_id, metadata,
    idempotency_scope, idempotency_key, priority, start_to_close, fairness_key,
    retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter
)
JOIN {schema}.jobs AS parent ON parent.id = fanout.parent_id
"""


FORK_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[])
"""


# ── the emit tx — the streaming source's per-page statement group ──
# The certified fork shapes re-bound (the children INSERT + the edge rows
# are byte-shape FORK_CHILDREN_SQL / FORK_EDGES_SQL — named separately so
# the emit's call site and its pins grep this concern) + the one new leg:
# the cursor checkpoint on the source row's own metadata (no new table,
# no new column), guarded by the full dispatch fence. one transaction
# (the fork-atomicity law at page granularity): a kill at any statement
# window — including after the cursor write but before the commit — rolls
# the children, the edges and the cursor back together; the reclaim
# re-pends the source (it never finalized), the re-claim re-emits exactly
# the lost page.
#
# the refuted-claim discipline (the measured 198 UniqueViolations — the
# design's proof the discriminator is load-bearing): the children's
# idempotency keys are parent-scoped at the emit scope
# (``wf:{flow}:emit:{map_index}:{step}``) and carry the per-child
# ``map_index`` — the fork key's discipline (pin 18) at chain level, plus
# the per-child trace_id the recorded failure-down reads.
# fairness_key is the child's dispatch cohort (the wf fairness stamp) —
# one cohort per run, the fork's own law at the emit's page inserts.
EMIT_CHILDREN_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key, fairness_key, priority, start_to_close,
     retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter,
     required_worker_schema)
SELECT page.*, source.required_worker_schema
FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::text[], $7::uuid[], $8::smallint[], $9::text[], $10::text[],
    $11::jsonb[], $12::text[], $13::text[], $14::text[],
    $15::smallint[], $16::interval[], $17::double precision[],
    $18::double precision[], $19::text[], $20::double precision[]
) AS page(
    id, actor, queue, payload, max_attempts, retry_kind,
    parent_id, map_index, step_key, trace_id, metadata,
    idempotency_scope, idempotency_key, fairness_key, priority, start_to_close,
    retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter
)
JOIN {schema}.jobs AS source ON source.id = page.parent_id
"""


EMIT_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[])
"""


# the cursor checkpoint — the fence carrier. A zombie source (its claim
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


# the run's non-terminal row count (the ledger's truth), excluding the source's
# own claim row — the admission's arithmetic input, read inside the tx under
# the lock.
EMIT_IN_FLIGHT_SQL = """\
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
  AND id <> $2::uuid
  AND status IN ('pending', 'scheduled', 'running')
"""


# one run's emitters serialize on this transaction-scoped advisory lock — the
# concurrent emitters' count+insert is atomic per run.
EMIT_ADMISSION_LOCK_SQL = """\
SELECT pg_advisory_xact_lock(hashtext($1::text))
"""


FORK_JOIN_NODE_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key, priority, start_to_close, fairness_key,
     required_worker_schema)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16,
        (SELECT parent.required_worker_schema FROM {schema}.jobs AS parent WHERE parent.id = $7))
"""


FORK_JOIN_CONSUMERS_SQL = """\
UPDATE {schema}.jobs
SET metadata = jsonb_set(metadata, '{consumers}', $2::jsonb, true)
WHERE id = $1
"""


#: the join's downstream consumers are wired like any node — the consumer's dep
#: was reserved at create (the runner's static insert counts the fork-spawned
#: join's edge before the join's id exists), so this edge row is the ledger's
#: truth that releases it: the consumer dispatches strictly after the JOIN'S
#: own terminal (the join row's claim + default packer writes the collected
#: result), and the consumer's arg resolution reads the join's result through
#: the same typed door as any node result (the parent-results query on the edge
#: ledger). The outbox's consumer insert stays as the belt (the arbiter's
#: idempotency key conflicts with the static row — no double dispatch).
FORK_CONSUMER_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
SELECT c.id, $1, $2, p.policy
FROM unnest($3::text[], $4::text[]) AS p(key, policy)
JOIN {schema}.jobs c ON c.step_key = p.key
  AND (c.metadata->>'flow_id')::uuid = $2
"""


# The public edge writer (the fork's FORK_EDGES_SQL is the fork-internal
# one; this is the single-edge insert the public join path uses): a joined
# node's incoming edges are the join counter's truth — a join declared
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


# ── The failed-parent propagation ──────────────────────────────────

# The fail-closed peer-cascade: a parent's terminal failure resolves every
# fail_closed join counting this parent by a flow-scoped transition set,
# one statement (one snapshot, one tx — the cascade is the linearization,
# the same shape cancel's flip is):
#
#   * the joined node → blocked: blocking_reason='failed_parent' + the
#     failed parent named on the record (metadata.failed_parent) — the
#     joined node's side of the counter is resolved by this stamp (the
#     rederive arm locks blocking_reason='join' rows only, so a stamped
#     row is never re-reconciled, never fired — it rests visible-blocked,
#     never a hanging join);
#   * the running peers (the blocked joins' other parents, still
#     non-terminal, not join-wait themselves — a nested join's own counter
#     is its ledger's truth and resolves through the sweep's recount once
#     its parents terminal) are peer-cancelled with the record:
#     error_class = the cancel-origin marker (the `by` leg, the same
#     outcome reads the same way whichever path produced it) + the
#     structured record in metadata.peer_cancel
#     ({by: 'peer_failure', cascade_from: <the failed node>});
#   * the workflow → failed (§17.2's cascade): the flow root's flip, the
#     linearization point every flow-status leg then reads.
#
# Every write is guarded: the block only join-wait rows not already
# stamped; the cancel only pending/scheduled/running rows; the flow flip
# only a non-terminal root and only when the cascade actually blocked a
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


# The absorbed fan-in (the collect / maybe policies): a failed child whose
# edge declared an absorbing policy does not cascade — its failure fans in
# as the typed FailureInfo item appended to each absorbing join's
# ``metadata.failures`` array (the full attempt history rides the item —
# the ledger rows are read in this statement; the detail never leaves the
# ledger/attempts). The join row's counters still resolve through the
# decrement (the failed child is resolved): the join fires when the last
# child terminalizes, strictly after every ladder attempt (the fan-in
# lands in the same tx, before the fire's decrement reads 0). The
# statement returns the history + the edge's own policy (the item names
# the policy that absorbed it — the envelope must not lie about which
# policy ran); the engine builds the typed item through the
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
# by this child's item) — keyed per join row (the fan-in read above
# returns the joins; this write lands the item). Keyed single row; the
# append is jsonb concat on the array.
# The fan-in's append, bounded (the unbounded-jsonb policy): the collect's ``failures`` array truncates at the
# byte cap — over the cap, the row compacts: the array is replaced by the
# bounded summary (the ``"__truncated__": N`` marker counting the items no
# longer spelled on the row + the ledger pointer), and the full detail
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


# The failed child's absorbed-side decrement (the absorbing policies): DECREMENT_SQL's
# shape scoped to the absorbing policies (collect | maybe — the edges
# whose declared policy absorbs the failure) — a fail_closed edge's side
# is resolved by the cascade's block stamp (never by a decrement: the
# join must not become firable over a failed parent).
#
# the flow-alive guard is the fence, not the policy (the phase-2 attack's
# flow-fenced arm): when the flow is already terminal (the fail_closed leg flipped it
# in this same tx), the refusing decrement is a fenced resolution — and a
# fenced decrement is not absorption. The edge's declared policy alone
# absorbs nothing: the sweep's flow-fenced arm (REDERIVE_SWEEP_SQL) gives
# the join the blocked-with-reason terminal state, and the absorption
# record (WORKFLOW_NODES_SQL / the maintenance leg's has_failed) reads
# the join row's terminal-block stamp — never the edge's declaration —
# so the derivation can say 'failed' for the failed-closed run (the
# envelope never lies).
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


# ── the deferral disposition's statement (the wf face of the vanilla
#    consumer's catch — the Snooze / RetryAfter row dispositions) ────────
#
# A workflow body raising the vanilla surface's typed control-flow signals
# (``taskq.exceptions.Snooze`` / ``RetryAfter``) resolves to one of the
# named dispositions (``api/_runner_defer.DeferralDisposition``), and this
# statement is the disposition's whole row law — the vanilla
# ``mark_snoozed`` / ``mark_retry_after_consume_{true,false}`` arms
# (``backend/_sql_templates.py``) mirrored onto the wf finalize's own
# fence vocabulary:
#
#   * the row re-schedules: ``scheduled_at = clock_timestamp() + delay``,
#     status 'scheduled' ('pending' for a zero effective delay) — the same
#     live-row shape the ladder's own NODE_REPEND_SQL_TEMPLATE writes, so
#     both doors re-claim it (the dev tick's claimable scan directly; the
#     fleet through the leader's scheduled→pending promotion sweep 3);
#   * the attempt arm (the vanilla dispositions' refund law,
#     ``ATTEMPT_REFUND_SQL``): a non-consuming deferral did not execute —
#     the claim's increment is refunded (floored at 0), so ``max_attempts``
#     is never exhausted by deferrals alone and the re-claim re-mints the
#     same attempt ordinal (the ledger's arbiter row is re-used — the
#     record never grows per snooze). A consuming ``RetryAfter`` keeps the
#     increment — the burn is the budget's own charge (the vanilla
#     mark_retry_after_consume_true's shape), bounded by the budget gate
#     below;
#   * the non-consuming delay carries the vanilla's deferral floor
#     (``MIN_DEFERRAL_INTERVAL`` — the monopolisation guard: a zero/now
#     snooze must not park the node at the head of the dispatch order,
#     one claim/refund round trip per cycle). The consuming arm keeps the
#     raw delay: an immediate consuming retry is a real execution, bounded
#     by the budget it spends;
#   * the counters: ``snooze_count`` moves for the refunding arms only (a
#     consuming retry is not a snooze — the vanilla's own accounting);
#   * the deferral stamp (the run page's visibility — the operator's
#     "why is this step sitting here" answer): the signal, the budget
#     face, and the disposition's own post-attempt ordinal, derived in
#     this statement (the eighth rule: derived values are written only by
#     the statement that derives them). The stamp's ``attempt`` is the
#     row-disposition discriminator the run page reads: the label renders
#     only while the row still sits at this deferral's own reschedule
#     (status 'scheduled' and attempt == the stamp's), so a later ladder
#     repend — the one other 'scheduled' writer, at a strictly greater
#     attempt — never reads as a live deferral;
#   * the lease-clear trio + ``assignment_routed = true`` (the vanilla
#     deferral arms' own set): the claim is released, and the row routes
#     by the actor's current assignment from here on (the routing
#     contract in ``backend/_dispatch_sql.py``);
#   * the cancel fence (the vanilla deferral arms' own law — "an operator
#     cancel in flight wins over the deferral" — on either carrier: the wf
#     cancel's own mark vocabulary, ``CANCEL_NODES_SQL_TEMPLATE``'s
#     ``metadata.cancel_phase``, and the vanilla cancel's own column mark
#     — the admin job-page cancel's ``cancel_running`` arm,
#     ``cancel_requested_at = now, cancel_phase = 1`` on the running row —
#     a mark a wf node row can carry like any job): a row carrying either
#     mark must never match the deferral arm — the merge below would
#     otherwise read as never-cancelled and the operator's request would
#     be laundered mid-flight (and the vanilla arms' own
#     ``cancel_phase = 0 / cancel_requested_at = NULL`` resets, mirrored
#     into the set below, would wash the column mark off the record). The
#     marked row answers the CANCEL_FENCED verdict (the runner
#     terminalises it through the wf terminal door, the vanilla cancel
#     ladder's own face), never a deferral; the resets are therefore only
#     ever reachable on a clean row — never laundry;
#   * the claim fence (the finalize's own CAS vocabulary: status +
#     worker + attempt + claim_epoch): a zombie's deferral matches
#     NOTHING — no verdict row — and the runner records the attempt
#     'fenced' on the ledger (never a running row left forever);
#   * the budget gate (the consuming arm's only vanilla exit —
#     mark_retry_after_consume_true's max_attempts fence): a consuming
#     retry past the budget answers the exhausted verdict — the runner
#     routes it through the wf terminal door (the ledger's failed row +
#     the failed-parent cascade ride finalize_node; the vanilla's own class +
#     message). The refunding arms are unconditional here: a deferral
#     that never ran spends nothing.
#
# Every write is keyed to the one claimed node row (never a scan); the
# two read CTEs (exhausted / cancel_fenced) write nothing — the verdict
# is named, the wf terminal door owns every death.
NODE_DEFER_SQL = """\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::int AS attempt,
           $5::bigint AS claim_epoch,
           $6::text AS disposition,
           $7::text AS signal_name,
           -- THE NON-CONSUMING DEFERRAL FLOOR (the vanilla mark_snoozed's
           -- own guard): only the refunding arms carry it; the consuming
           -- arm keeps the raw delay.
           CASE WHEN $6::text = 'retry_after_consuming' THEN $3::interval
                ELSE GREATEST($3::interval, {min_deferral_interval}) END
               AS effective_delay,
           -- THE DISPOSITION'S OWN POST-ATTEMPT ORDINAL (the refund law
           -- above; the fence guarantees j.attempt = $4 at write time).
           CASE WHEN $6::text = 'retry_after_consuming' THEN $4::int
                ELSE GREATEST($4::int - 1, 0) END
               AS post_attempt
),
-- Only the policy-classified arm reads this metadata domain. Missing count is
-- zero; malformed count/budget deliberately refuses and rolls the whole row law
-- back instead of resetting a bound. The exact current count and increment are
-- evaluated under the same statement's claim fence as the job transition.
retry_input AS (
    SELECT j.id,
           CASE
             WHEN NOT (j.metadata->'wf_retry' ? 'non_consuming_count') THEN 0
             WHEN j.metadata #>> '{wf_retry,non_consuming_count}' ~
                  '^(0|[1-9][0-9]*)$'
                  AND (
                      length(j.metadata #>> '{wf_retry,non_consuming_count}') < 10
                      OR (
                          length(j.metadata #>> '{wf_retry,non_consuming_count}') = 10
                          AND (j.metadata #>> '{wf_retry,non_consuming_count}') <= '2147483647'
                      )
                  )
               THEN (j.metadata #>> '{wf_retry,non_consuming_count}')::int
             ELSE NULL::int
           END AS current_count,
           CASE
             WHEN j.metadata #>> '{wf_retry,non_consuming_limit}' ~
                  '^[1-9][0-9]*$'
                  AND (
                      length(j.metadata #>> '{wf_retry,non_consuming_limit}') < 10
                      OR (
                          length(j.metadata #>> '{wf_retry,non_consuming_limit}') = 10
                          AND (j.metadata #>> '{wf_retry,non_consuming_limit}') <= '2147483647'
                      )
                  )
               THEN (j.metadata #>> '{wf_retry,non_consuming_limit}')::int
             ELSE NULL::int
           END AS limit_count
    FROM {schema}.jobs AS j
    WHERE j.id = (SELECT job_id FROM params)
      AND (SELECT disposition FROM params) = 'retry_after_non_consuming'
),
deferred AS (
    UPDATE {schema}.jobs j
    SET status = CASE WHEN (SELECT effective_delay FROM params) > interval '0'
                      THEN 'scheduled'::"{schema}".job_status
                      ELSE 'pending'::"{schema}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT effective_delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        -- THE CANCEL COLUMNS' RESET (the vanilla mark_snoozed's own SET,
        -- mirrored): the deferral returns the row to the dispatch pool
        -- carrying NO cancel request — the columns reset with the claim.
        -- SAFE BECAUSE THE FENCE REFUSES THE MARKED ROW (below): the
        -- resets can only ever land on a row that carried NO cancel
        -- mark — a marked row is fenced, never laundered.
        cancel_phase = 0,
        cancel_requested_at = NULL,
        attempt = (SELECT post_attempt FROM params),
        snooze_count = CASE WHEN $6::text IN (
                                    'retry_after_consuming',
                                    'retry_after_non_consuming'
                                ) THEN j.snooze_count
                            ELSE j.snooze_count + 1 END,
        -- THE DEFERRAL STAMP (the run page's visibility — see the block
        -- comment): the signal + the budget face + the disposition's own
        -- post-attempt ordinal, one derived write.
        metadata = j.metadata || jsonb_build_object(
            'wf_deferral',
            jsonb_build_object(
                'signal', (SELECT signal_name FROM params),
                'consume_budget', $6::text = 'retry_after_consuming',
                'attempt', (SELECT post_attempt FROM params)
            )
        ) || CASE
            WHEN $6::text <> 'retry_after_non_consuming' THEN '{}'::jsonb
            ELSE jsonb_build_object(
                'wf_retry',
                COALESCE(j.metadata->'wf_retry', '{}'::jsonb) || jsonb_build_object(
                    'non_consuming_count',
                        (SELECT current_count FROM retry_input) + 1,
                    'non_consuming_limit', (SELECT limit_count FROM retry_input)
                )
            )
        END,
        -- A deferral returns a claimed row to the pending pool, so it
        -- routes by the actor's current assignment from here on (the
        -- routing contract in taskq/backend/_dispatch_sql.py; the
        -- vanilla deferral arms' own SET).
        assignment_routed = true
    WHERE j.id = (SELECT job_id FROM params)
      -- THE CLAIM FENCE (the finalize's own CAS vocabulary).
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      -- THE CANCEL FENCE (the wf cancel's own mark — the block
      -- comment), on EITHER carrier: the wf cancel's metadata stamp
      -- AND the vanilla cancel's own COLUMN mark (the admin job-page
      -- cancel's cancel_running arm — cancel_requested_at = now,
      -- cancel_phase = 1 on the running row). An operator cancel in
      -- flight WINS over the deferral on both faces; the resets above
      -- can therefore only ever land on a clean row (never laundry).
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
      -- THE BUDGET GATE (the consuming arm's only exit — the block
      -- comment); the refunding arms are unconditional.
      AND (
          (
              $6::text = 'retry_after_consuming'
              AND (
                  j.retry_kind = 'indefinite'
                  OR (j.retry_kind = 'transient' AND j.attempt < j.max_attempts)
              )
          )
          OR (
              $6::text = 'retry_after_non_consuming'
              AND EXISTS (
                  SELECT 1 FROM retry_input AS r
                  WHERE r.current_count + 1 < r.limit_count
              )
          )
          OR $6::text NOT IN (
              'retry_after_consuming', 'retry_after_non_consuming'
          )
      )
    RETURNING j.id
),
exhausted AS (
    -- THE BUDGET'S BOUND, read on the SAME fenced row the deferred arm
    -- keys (the vanilla mark_retry_after_consume_true's max_attempts
    -- arm, the arm order preserved: the cancel mark outranks it). This
    -- arm WRITES NOTHING — the wf terminal door owns every node death;
    -- the statement only names the verdict.
    SELECT j.id
    FROM {schema}.jobs j
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
      AND $6::text = 'retry_after_consuming'
      AND j.retry_kind <> 'indefinite'
      AND (j.retry_kind = 'non_retryable' OR j.attempt >= j.max_attempts)
      AND NOT EXISTS (SELECT 1 FROM deferred)
),
non_consuming_exhausted AS (
    -- Persist the exact terminal counter (saturated replays keep the budget)
    -- while leaving the running claim for the caller's ordinary terminal CAS.
    UPDATE {schema}.jobs AS j
    SET metadata = j.metadata || jsonb_build_object(
        'wf_retry',
        COALESCE(j.metadata->'wf_retry', '{}'::jsonb) || jsonb_build_object(
            'non_consuming_count', LEAST(r.current_count + 1, r.limit_count),
            'non_consuming_limit', r.limit_count
        )
    )
    FROM retry_input AS r
    WHERE j.id = r.id
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
      AND r.current_count + 1 >= r.limit_count
      AND NOT EXISTS (SELECT 1 FROM deferred)
    RETURNING j.id
),
malformed_non_consuming AS (
    SELECT j.id
    FROM {schema}.jobs AS j
    JOIN retry_input AS r ON r.id = j.id
    WHERE j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
      AND (r.current_count IS NULL OR r.limit_count IS NULL)
      AND NOT EXISTS (SELECT 1 FROM deferred)
),
cancel_fenced AS (
    -- THE CANCEL WINS (the vanilla deferral's own law — the block
    -- comment): a clean claimed row carrying the cancel mark on EITHER
    -- carrier — the wf cancel's metadata stamp OR the vanilla cancel's
    -- own column mark (the admin job-page cancel's arm). Also writes
    -- nothing; the runner terminalises the node 'cancelled' through the
    -- wf terminal door (the vanilla cancel ladder's face).
    SELECT j.id
    FROM {schema}.jobs j
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND (j.metadata ? 'cancel_phase' OR j.cancel_phase <> 0)
      AND NOT EXISTS (SELECT 1 FROM deferred)
)
SELECT 'deferred'::text AS verdict, id AS job_id FROM deferred
UNION ALL
SELECT 'exhausted'::text AS verdict, id AS job_id FROM exhausted
UNION ALL
SELECT 'exhausted'::text AS verdict, id AS job_id FROM non_consuming_exhausted
UNION ALL
SELECT 'malformed_counter'::text AS verdict, id AS job_id FROM malformed_non_consuming
UNION ALL
SELECT 'cancel_fenced'::text AS verdict, id AS job_id FROM cancel_fenced
"""
