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

from taskq.workflows._sql_claim import current_claim_join

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
def park_predicate(alias: str) -> str:
    """THE SHARED ``P(a)`` PREDICATE (§7's one authoritative spelling) — the
    public seam for the product's own read surfaces (the admin's runs list)
    that must key their park exclusions on the SAME predicate the engine
    uses, never a re-spelled copy (the drift the red team named). The
    private alias remains the bundle renderers' own name."""
    return _wf_emit_park_predicate(alias)


def _wf_emit_park_predicate(alias: str) -> str:
    """The one shared exact-park predicate ``P(a)`` (the representation's own
    identity): the marker, never a timestamp, is the discriminator.
    ``scheduled_at`` is deliberately absent from park identity — it stores
    the park instant and is never a due date. Every park-scoped statement
    renders this one definition with its own alias; there is no second
    spelling to drift. The jsonb literals carry the doubled-brace render
    convention (the bundle's collapse pass un-doubles them)."""
    return (
        "{a}.status = 'scheduled'\n"
        '  AND {a}.metadata @> \'{{"blocking_reason":"backpressure"}}\'::jsonb\n'
        '  AND {a}.metadata @> \'{{"wf_deferral":'
        '{{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}}}\'::jsonb\n'
        '  AND {a}.metadata @> \'{{"wf_emit_backpressure":{{"version":1}}}}\'::jsonb\n'
        "  AND {a}.step_key IS NOT NULL\n"
        "  AND {a}.step_key <> '__flow__'\n"
        "  AND {a}.metadata ? 'flow_id'"
    ).replace("{a}", alias)


def _wf_emit_validated_park_width(alias: str) -> str:
    """The shared inline scalar ``validated_park_width(a)``: NULL when the row
    is not an exact park; otherwise only the validated positive-or-zero
    ``wf_emit_backpressure.page_width``. Malformed values refuse (the
    row-dependent arm — a uuid-text cast cannot constant-fold) rather than
    coerce; constant raise arms such as ``1/0`` are forbidden because
    PostgreSQL folds them at plan time."""
    a = alias
    width_text = f"{a}.metadata->'wf_emit_backpressure'->>'page_width'"
    return (
        "CASE WHEN NOT (" + _wf_emit_park_predicate(a) + ")\n"
        "     THEN NULL\n"
        f"     WHEN jsonb_typeof({a}.metadata->'wf_emit_backpressure'->'page_width') = 'number'\n"
        f"          AND {width_text} ~ '^[0-9]+$'\n"
        f"     THEN ({width_text})::bigint\n"
        f"     ELSE ({a}.id::text)::integer\n"
        "END"
    )


def _wf_emit_validated_marker_bound(alias: str) -> str:
    """The shared inline scalar validator for the exact-park marker's own
    ``bound`` (a positive integer). Row-dependent fail-closed arm."""
    a = alias
    bound_text = f"{a}.metadata->'wf_emit_backpressure'->>'bound'"
    return (
        "CASE WHEN NOT (" + _wf_emit_park_predicate(a) + ")\n"
        "     THEN NULL\n"
        f"     WHEN jsonb_typeof({a}.metadata->'wf_emit_backpressure'->'bound') = 'number'\n"
        f"          AND {bound_text} ~ '^[1-9][0-9]*$'\n"
        f"     THEN ({bound_text})::bigint\n"
        f"     ELSE ({a}.id::text)::integer\n"
        "END"
    )


def _wf_emit_validated_reservation_width(alias: str) -> str:
    """The shared inline scalar ``validated_reservation_width(a)``: NULL when
    the reservation key is absent; otherwise exactly the two-key object
    ``{"version":1,"width":W}`` with validated positive-or-zero integer
    width. Unknown keys, wrong types, null, bool, float, negative widths, and
    version drift fail closed; no reservation is valid on an exact park."""
    a = alias
    res = f"{a}.metadata->'wf_emit_reservation'"
    return (
        f"CASE WHEN NOT ({a}.metadata ? 'wf_emit_reservation')\n"
        "     THEN NULL\n"
        "     WHEN (" + _wf_emit_park_predicate(a) + ")\n"
        "          -- No reservation is valid on an exact park.\n"
        f"          OR jsonb_typeof({res}) <> 'object'\n"
        f"          OR ({res}) - '{{\"version\",\"width\"}}'::text[] <> '{{}}'::jsonb\n"
        f"          OR jsonb_typeof({res}->'version') <> 'number'\n"
        f"          OR ({res}->>'version') <> '1'\n"
        f"          OR jsonb_typeof({res}->'width') <> 'number'\n"
        f"          OR ({res}->>'width') !~ '^[0-9]+$'\n"
        f"     THEN ({a}.id::text)::integer\n"
        f"     ELSE ({res}->>'width')::bigint\n"
        "END"
    )


COLLECT_TRUNCATION_RECEIPT_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
"""

TERMINAL_MARK_SQL = (
    """\
-- THE RESERVATION CAPTURE (§2.4): the winning row's OLD reservation fact is
-- captured in its own CTE before the terminal UPDATE removes the key — the
-- VALIDATED old width (the shared validated_reservation_width scalar: NULL
-- when absent, fail-closed on a malformed key), never a raw unvalidated
-- blob — and the diagnostics count only captured winners, never an
-- already-converted row and never a Python pre-read.
WITH target AS (
    SELECT j.id,
           (j.metadata ? 'wf_emit_reservation') AS had_reservation,
           """
    + _wf_emit_validated_reservation_width("j")
    + """ AS old_reservation
    FROM {schema}.jobs j
    WHERE j.id = $1
), upd AS (
    UPDATE {schema}.jobs
    SET status = $2::"{schema}".job_status,
        finished_at = clock_timestamp(),
        result = $3::jsonb,
        result_size_bytes = $4,
        error_class = $5,
        error_message = $6,
        error_traceback = $7,
        -- THE TERMINAL REMOVES THE RESERVATION KEY (§2.4): no key survives
        -- a terminal row for re-arm; a row without one is unchanged.
        metadata = metadata - 'wf_emit_reservation',
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
SELECT upd.id, upd.status, upd.attempt, upd.payload, upd.trace_id,
       t.had_reservation, t.old_reservation
FROM upd
JOIN target t ON t.id = upd.id
"""
)


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
# The commit also removes the page-width RESERVATION key (§2.4's reserved-
# replay disposition: a committed page converts the reservation — the next
# derived charge changes by W' - W; unreserved emits carry no key and are
# unchanged).
EMIT_CURSOR_SQL = """\
UPDATE {schema}.jobs
SET metadata = (metadata - 'wf_emit_reservation') || $2::jsonb
WHERE id = $1
  AND status = 'running'
  AND locked_by_worker = $3
  AND attempt = $4
  AND claim_epoch = $5
  AND cancel_phase = 0
  AND NOT metadata ? 'cancel_phase'
  AND NOT EXISTS (
      SELECT 1 FROM {schema}.jobs AS root
      WHERE root.id = ({schema}.jobs.metadata->>'flow_id')::uuid
        AND root.status IN ('succeeded','failed','cancelled','crashed','abandoned')
  )
RETURNING id
"""


#: The derived observation — the sole capacity authority and the folded home
#: of the legacy in-flight count. The status leg is HOISTED outside the
#: aggregate so the scan stays on ``jobs_wf_flow_nodes_idx`` (its required
#: key: the flow uuid, then the full pending/scheduled/running domain); the
#: examined rows are the live non-root rows plus exact parks admitted by
#: those predicates — never terminal history, so the observation is
#: ``Theta(live + parked)``. ``$2`` is the running source's own id for
#: unreserved admission (the single authoritative own-row exclusion) and
#: NULL for terminal, park, and drain observations — in particular the park
#: observation includes its own newly written row. No persisted charge
#: exists to drift: every counted leg derives from these real rows.
WF_EMIT_CAPACITY_OBSERVATION_SQL = (
    "WITH observed AS MATERIALIZED (\n"
    "    SELECT\n"
    "        (" + _wf_emit_park_predicate("j") + ") AS is_park,\n"
    "        " + _wf_emit_validated_park_width("j") + " AS park_width,\n"
    "        " + _wf_emit_validated_reservation_width("j") + " AS reservation_width\n"
    "    FROM {schema}.jobs AS j\n"
    "    WHERE (j.metadata->>'flow_id')::uuid = $1::uuid\n"
    "      AND j.status IN ('pending', 'scheduled', 'running')\n"
    "      AND j.metadata ? 'flow_id'\n"
    "      AND j.step_key IS NOT NULL\n"
    "      AND j.step_key <> '__flow__'\n"
    "      AND ($2::uuid IS NULL OR j.id <> $2::uuid)\n"
    ")\n"
    "SELECT count(*) FILTER (WHERE NOT is_park) AS live_rows,\n"
    "       coalesce(sum(reservation_width), 0)::bigint AS reservations,\n"
    "       min(park_width) FILTER (WHERE is_park) AS min_waiting_width\n"
    "FROM observed\n"
)

#: The per-terminal exact-marker existence probe (the event rule's step 1):
#: one indexed zero-row query through ``jobs_wf_emit_park_idx`` for a
#: non-crossing terminal; no count, no intent, no notify follows a zero-row
#: probe.
WF_EMIT_PARK_EXISTS_SQL = (
    "SELECT 1\n"
    "FROM {schema}.jobs AS j\n"
    "WHERE (j.metadata->>'flow_id')::uuid = $1::uuid\n"
    "  AND " + _wf_emit_park_predicate("j") + "\n"
    "LIMIT 1\n"
)

#: The single authoritative candidate comparison (the drain's own): the
#: oldest FITTING waiter on real rows — a non-fitting older row can never
#: block a smaller follower. The drain supplies the already-derived
#: ``$charge``; the fit comparison rides the ``jobs_wf_emit_park_idx``
#: (flow, created_at, id) partial index, so the scan is
#: ``Theta(parked) <= Theta(S)``.
WF_EMIT_CANDIDATE_SQL = (
    "SELECT j.id, " + _wf_emit_validated_park_width("j") + " AS width\n"
    "FROM {schema}.jobs AS j\n"
    "WHERE (j.metadata->>'flow_id')::uuid = $1::uuid\n"
    "  AND " + _wf_emit_park_predicate("j") + "\n"
    "  AND $2::bigint + " + _wf_emit_validated_park_width("j") + " <= $3::bigint\n"
    "ORDER BY j.created_at, j.id\n"
    "LIMIT 1\n"
)

#: The bounded run's capacity/intent record — bound and wake intents only.
#: There is no persisted live-row charge, waiter count, cached head, or
#: cached minimum width anywhere: capacity is derived from ``jobs`` rows.
WF_EMIT_INTENT_LOCK_SQL = """\
SELECT bound, wake_generation, handled_generation
FROM {schema}.wf_emit_capacity
WHERE flow_id = $1
FOR UPDATE
"""

#: The stamp at create/adoption: ``bound`` is immutable from the run's
#: compiled declaration. A conflicting existing bound is corruption and is
#: refused by the caller's read-back check.
WF_EMIT_INTENT_STAMP_SQL = """\
INSERT INTO {schema}.wf_emit_capacity (flow_id, bound, wake_generation, handled_generation)
VALUES ($1, $2, 0, 0)
ON CONFLICT (flow_id) DO NOTHING
RETURNING flow_id
"""

#: The adopted record's bound read-back (the missing/mismatched fail-closed
#: check before feature use).
WF_EMIT_INTENT_BOUND_SQL = """\
SELECT bound FROM {schema}.wf_emit_capacity WHERE flow_id = $1
"""

#: The threshold crossing: one durable generation bump, only when no intent
#: is already pending. The near-overflow refusal is named and fail-closed —
#: the bigint domains never wrap or reset.
WF_EMIT_INTENT_GENERATION_SQL = """\
UPDATE {schema}.wf_emit_capacity
SET wake_generation = wake_generation + 1
WHERE flow_id = $1
  AND wake_generation = handled_generation
  AND wake_generation < 9223372036854775806::bigint
RETURNING wake_generation
"""

#: Completion: the drain transaction that observed no fitting candidate marks
#: the generation handled. CAS-gated so a concurrent terminal's newer
#: generation survives.
WF_EMIT_INTENT_HANDLED_SQL = """\
UPDATE {schema}.wf_emit_capacity
SET handled_generation = wake_generation
WHERE flow_id = $1
  AND wake_generation > handled_generation
RETURNING flow_id
"""

#: The global pending-intent selection: at most 100 distinct flow ids in one
#: short indexed read (the pending-generation partial index), ordered,
#: rotated by the persistent ``after_flow_id`` cursor, and holding no row
#: lock past the read.
WF_EMIT_PENDING_INTENTS_SQL = """\
SELECT flow_id, bound, wake_generation, handled_generation
FROM {schema}.wf_emit_capacity
WHERE wake_generation > handled_generation
  AND flow_id > $1::uuid
ORDER BY flow_id
LIMIT 100
"""

#: The fairness rotation's singleton: one row, one ``after_flow_id``, never
#: growing with run cardinality.
WF_EMIT_ROTATION_READ_SQL = """\
SELECT after_flow_id FROM {schema}.wf_emit_intent_rotation WHERE id = true
"""

#: The rotation is committed independently of the per-flow work. The
#: write is an UPSERT: a schema whose singleton row was truncated (the
#: per-test reset's own law) re-seeds it on the first write, and the
#: read tolerates the missing row (NULL → the zero-UUID start).
WF_EMIT_ROTATION_WRITE_SQL = """\
INSERT INTO {schema}.wf_emit_intent_rotation (id, after_flow_id)
VALUES (true, $1::uuid)
ON CONFLICT (id) DO UPDATE
SET after_flow_id = $1::uuid
"""

#: The bounded exact-marker sweep's selection (D7): a KEYSET rotation over
#: the ``jobs_wf_emit_park_idx`` partial index — the distinct marked flow
#: ids that still lack their required intent record, ordered by flow id,
#: at most 100 per call, continuing after ``$1`` (the caller's persisted
#: cursor). It is never a parked-estate seq scan or a due-time wake. The
#: per-flow width/bound validation lives in the caller's ISOLATED per-flow
#: repair transaction (a malformed marker poisons only its own flow's
#: repair, with a bounded diagnostic, and the rest repair on) — the
#: selection itself aggregates nothing and can never abort on one row's
#: malformed width. THE SELECTION'S OWN LAW (N3): a marked flow is
#: selected when its intent record is ABSENT (the omitted seat) OR NOT
#: PENDING (``wake = handled`` — a vanilla-writer release freed a live
#: row and the parked waiter may now fit); an intent that is already
#: PENDING is the drain arms' own to observe (never double-selected).
WF_EMIT_MARKER_SWEEP_SQL = (
    "SELECT DISTINCT (j.metadata->>'flow_id')::uuid AS flow_id\n"
    "FROM {schema}.jobs AS j\n"
    "WHERE " + _wf_emit_park_predicate("j") + "\n"
    "  AND (j.metadata->>'flow_id')::uuid > $1::uuid\n"
    "  AND NOT EXISTS (\n"
    "      SELECT 1 FROM {schema}.wf_emit_capacity c\n"
    "      WHERE c.flow_id = (j.metadata->>'flow_id')::uuid\n"
    "        AND c.wake_generation > c.handled_generation\n"
    "  )\n"
    "ORDER BY flow_id\n"
    "LIMIT 100\n"
)

#: The isolated per-flow repair's own bound read (D7): the flow's marker
#: bound through the validated scalar — a malformed marker bound raises
#: (row-dependent) inside the flow's own repair transaction, quarantining
#: the poison to that flow's bounded diagnostic.
WF_EMIT_MARKER_FLOW_BOUND_SQL = (
    "SELECT " + _wf_emit_validated_marker_bound("j") + " AS bound\n"
    "FROM {schema}.jobs AS j\n"
    "WHERE (j.metadata->>'flow_id')::uuid = $1::uuid\n"
    "  AND " + _wf_emit_park_predicate("j") + "\n"
    "LIMIT 1\n"
)

#: The loud non-drainage refusal: on the committed terminal-root proof (a
#: genuine child failure exhausted its declared retry policy and the root
#: terminal ``failed`` committed, while an exact parked source still holds
#: un-emitted work — INCLUDING a first-page park with no committed
#: ``emit_cursor``: the absent key means zero pages have committed and the
#: default/initial cursor identifies the first page; it does not make the
#: park empty or abandonable), the same failure-cascade transaction fences
#: every such parked source. No logical attempt is spent, no ladder retry
#: is created, and the causal child's genuine error is never overwritten.
#: The ledger fence is the EXACT current-claim association (the shared
#: renderer: job + current epoch + step_key + null-safe map_index, status
#: ``awaited``) — older epochs, sibling ``ctx.substep`` rows, map siblings,
#: and terminal history are never rewritten.
WF_NOT_DRAINING_FENCE_SQL = (
    "WITH parks AS (\n"
    "    SELECT j.id\n"
    "    FROM {schema}.jobs AS j\n"
    "    WHERE (j.metadata->>'flow_id')::uuid = $1::uuid\n"
    "      AND "
    + _wf_emit_park_predicate("j")
    + "\n"  # Why: the shared predicate renderer is the module's own constant text; the schema token is the render's validated substitution.
    "), fenced AS (\n"
    "    UPDATE {schema}.jobs AS j\n"
    "    SET status = 'failed',\n"
    "        finished_at = clock_timestamp(),\n"
    "        error_class = $2,\n"
    "        error_message = $3,\n"
    "        metadata = (j.metadata - 'blocking_reason' - 'wf_emit_reservation')\n"
    "            || jsonb_build_object(\n"
    "                'wf_not_draining', jsonb_build_object(\n"
    "                    'reason', 'terminal_root_after_child_failure_exhaustion',\n"
    "                    'root_id', $1::uuid,\n"
    "                    'child_id', $4::uuid,\n"
    "                    'at', to_char(clock_timestamp() AT TIME ZONE 'UTC',\n"
    '                                  \'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\')\n'
    "                )\n"
    "            )\n"
    "    FROM parks\n"
    "    WHERE j.id = parks.id\n"
    "    RETURNING j.id, j.claim_epoch, j.step_key, j.map_index\n"
    "), ledger AS (\n"
    "    UPDATE {schema}.wf_step_ledger AS l\n"
    "    SET status = 'fenced',\n"
    "        error_class = $2,\n"
    "        updated_at = clock_timestamp()\n"
    "    FROM fenced\n"
    "    WHERE " + current_claim_join("fenced", "l") + "\n"
    "      AND l.status = 'awaited'\n"
    "    RETURNING l.id\n"
    "), evt AS (\n"
    "    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)\n"
    "    SELECT fenced.id, clock_timestamp(), 'state_change',\n"
    "           jsonb_build_object('from_state', 'scheduled',\n"
    "                              'to_state', 'failed',\n"
    "                              'error_class', $2::text,\n"
    "                              'reason',\n"
    "                              'terminal_root_after_child_failure_exhaustion')\n"
    "    FROM fenced\n"
    ")\n"
    "SELECT (SELECT count(*) FROM fenced)::int AS fenced_count\n"
)

EMIT_ADMISSION_LOCK_SQL = """\
SELECT pg_advisory_xact_lock(hashtext($1::text))
"""

#: THE LIVE ROOT LOCK (§3.1's canonical order, D1): every drain chunk's
#: transaction locks the run's LIVE root row — ``step_key='__flow__'``
#: — before the intent row and any candidate CAS. The root-failure and
#: maintenance seats (the failure cascade's ``flow_failed`` update and
#: ``WORKFLOW_ROOT_MAINTAIN_SQL``'s ``FOR UPDATE`` roots scan) hold this
#: same row lock, so the drain and the root-failure fence can no longer
#: interleave: a maintenance pass either runs before the drain (the
#: §5.4 fence fences the still-parked source) or after it (the promoted
#: source is unresolved live work and the root legitimately does not
#: finalize). Without this lock the fence's park re-evaluation could
#: lose the race to the promotion CAS and strand a pending reserved
#: source under a terminal root forever.
EMIT_ROOT_LOCK_SQL = """\
SELECT id FROM {schema}.jobs
WHERE id = $1::uuid AND step_key = '__flow__'
FOR UPDATE
"""

# A dedicated ordinary-capacity park, intentionally separate from
# NODE_DEFER_SQL's delay/GREATEST/snooze/provider-retry machinery. The job and
# current ledger update commit or roll back together. The caller validates the
# descriptor before entry and fails the transaction if the exact current
# ledger id+claim instant does not take the single awaited transition.
EMIT_BACKPRESSURE_PARK_SQL = """\
WITH params AS (
    SELECT $1::uuid AS job_id, $2::uuid AS worker_id,
           $3::int AS physical_attempt, $4::bigint AS claim_epoch,
           $5::int AS page_width, $6::int AS bound,
           $7::int AS logical_attempt, $8::int AS outstanding,
           $9::uuid AS ledger_id, $10::timestamptz AS ledger_claimed_at
), parked AS (
    UPDATE {schema}.jobs AS j
    SET status = 'scheduled',
        -- THE PARK INSTANT, not a due date: scheduled_at stays NOT NULL and
        -- is deliberately absent from park identity and claim eligibility —
        -- the marker, never the timestamp, is the discriminator.
        scheduled_at = clock_timestamp(),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        attempt = 0,
        assignment_routed = true,
        -- A park strips any stale reservation key: no reservation is valid
        -- on an exact park (the derived observation's own invariant).
        metadata = (j.metadata - 'wf_emit_reservation') || jsonb_build_object(
            'blocking_reason', 'backpressure',
            'wf_source_logical_attempt', (SELECT logical_attempt FROM params),
            'wf_deferral', jsonb_build_object(
                'signal', 'EmitBackpressure',
                'consume_budget', false,
                'attempt', 0
            ),
            'wf_emit_backpressure',
                COALESCE(j.metadata->'wf_emit_backpressure', '{}'::jsonb)
                || jsonb_build_object(
                    'version', 1,
                    'page_width', (SELECT page_width FROM params),
                    'bound', (SELECT bound FROM params),
                    'outstanding', (SELECT outstanding FROM params),
                    'deferrals', COALESCE(NULLIF(
                        j.metadata #>> '{wf_emit_backpressure,deferrals}', ''
                    )::int, 0) + 1,
                    'wakes', COALESCE(NULLIF(
                        j.metadata #>> '{wf_emit_backpressure,wakes}', ''
                    )::int, 0),
                    'deferred_at', to_char(
                        clock_timestamp() AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    )
                )
        )
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT physical_attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
    RETURNING j.id
), current_ledger AS (
    UPDATE {schema}.wf_step_ledger AS l
    SET status = 'awaited'
    WHERE l.id = (SELECT ledger_id FROM params)
      AND l.job_id = (SELECT id FROM parked)
      AND l.updated_at = (SELECT ledger_claimed_at FROM params)
      AND l.status IN ('running', 'awaited', 'crashed')
    RETURNING l.id
)
SELECT 'parked'::text AS verdict, p.id,
       (SELECT count(*) FROM current_ledger)::int AS ledger_count
FROM parked AS p
UNION ALL
SELECT 'cancel_fenced'::text, j.id, 0
FROM {schema}.jobs AS j
WHERE j.id = (SELECT job_id FROM params)
  AND j.status = 'running'
  AND j.locked_by_worker = (SELECT worker_id FROM params)
  AND j.attempt = (SELECT physical_attempt FROM params)
  AND j.claim_epoch = (SELECT claim_epoch FROM params)
  AND (j.metadata ? 'cancel_phase' OR j.cancel_phase <> 0)
  AND NOT EXISTS (SELECT 1 FROM parked)
"""


#: The promotion CAS (the one commit-acknowledged, run-targeted promotion
#: primitive's row law): a fitted waiter's exact real row goes ``scheduled ->
#: pending`` and carries a durable page-width RESERVATION — a woken source
#: cannot be barged into another no-fit re-park. The CAS requires the
#: candidate id, ``P(j)``, absence of an existing reservation, and a
#: nonterminal root; it emits the effective queue wake only on a winning
#: row. ``$2`` is the candidate's validated page width (read from the same
#: row by the candidate comparison). ``attempt = 0`` and ``claim_epoch`` are
#: untouched. The reservation is not a dispatch slot, worker reservation,
#: lease, or execution permission: ordinary queue claim remains the sole
#: execution grant.
# Why S608-safe: the schema token is the render's validated substitution
# (require_schema); every caller value is $N-bound.
EMIT_BACKPRESSURE_WAKE_SQL = f"""\
UPDATE {{schema}}.jobs AS j
SET status = 'pending',
    scheduled_at = clock_timestamp(),
    assignment_routed = true,
    metadata = (
        (j.metadata - 'blocking_reason')
        || jsonb_build_object(
            'wf_emit_reservation',
            jsonb_build_object('version', 1, 'width', $2::int)
        )
    )
    || jsonb_build_object(
        'wf_emit_backpressure',
        (j.metadata->'wf_emit_backpressure')
        || jsonb_build_object(
            'wakes', COALESCE(NULLIF(
                j.metadata #>> '{{wf_emit_backpressure,wakes}}', ''
            )::int, 0) + 1,
            'last_wake_at', to_char(
                clock_timestamp() AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            )
        )
    )
WHERE j.id = $1
  AND NOT j.metadata ? 'wf_emit_reservation'
  AND {_wf_emit_park_predicate("j")}
  AND EXISTS (
      SELECT 1 FROM {{schema}}.jobs AS root
      WHERE root.id = (j.metadata->>'flow_id')::uuid
        AND root.step_key = '__flow__'
        AND root.status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
  )
RETURNING COALESCE(
    (SELECT ac.queue FROM {{schema}}.actor_config AS ac WHERE ac.actor = j.actor),
    j.queue
) AS effective_queue
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
        -- THE TERMINAL REMOVES THE RESERVATION KEY (§2.4): the peer
        -- cancel's terminal removes any held page-width reservation in
        -- the same winning statement (the cancel-origin marker still
        -- rides the same metadata write).
        metadata = (p.metadata - 'wf_emit_reservation') || $4::jsonb
    FROM edges e
    JOIN {schema}.wf_edge sib ON sib.child_id = e.child_id
    WHERE p.id = sib.parent_id
      AND p.id <> $1::uuid
      AND p.status IN ('pending', 'scheduled', 'running')
      AND NOT (p.deps_pending > 0
               AND p.metadata @> '{{"blocking_reason": "join"}}'::jsonb)
    RETURNING p.id
)
SELECT (SELECT count(*) FROM blocked) AS blocked,
       (SELECT count(*) FROM peers) AS peers_cancelled
"""

#: THE ROOT'S OWN GATED FAILURE (N1): its own statement — its own fresh
#: snapshot, taken after the root lock's grant — so the unresolved-live
#: gate decides from the ROWS AS THEY STAND NOW (a promotion the drain
#: committed while this transaction waited on the root is visible). The
#: maintenance seat's own law, applied at the cascade seat: a live
#: unresolved node (a promoted pending source, a running node) withholds
#: the root's failure; the maintenance's later pass owns the finalize once
#: the live work rests.
WF_FLOW_FAILED_GATED_SQL = """\
WITH flow_locked AS (
    SELECT f.id FROM {schema}.jobs f WHERE f.id = $1::uuid FOR UPDATE
),
failed AS (
    UPDATE {schema}.jobs f
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = $2::text
    WHERE f.id = (SELECT id FROM flow_locked)
      AND f.status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
      AND NOT EXISTS (
          SELECT 1 FROM {schema}.jobs n
          WHERE (n.metadata->>'flow_id')::uuid = f.id
            AND n.metadata ? 'flow_id'
            AND n.step_key <> '__flow__'
            AND n.id <> f.id
            AND n.status IN ('pending', 'scheduled', 'running')
            AND NOT (n.metadata ? 'blocking_reason')
      )
    RETURNING f.id
)
SELECT (SELECT count(*) FROM failed)::int AS flow_failed
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
               ) ORDER BY l.attempt, l.claim_epoch, l.created_at, l.id
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
