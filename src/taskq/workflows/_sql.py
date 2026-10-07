"""The workflows engine's named statement constants (GAPS-ESTATE F10).

Workflow SQL lives as named statement constants — the
``backend/_sql_templates.py`` / ``_dispatch_sql.py`` pattern, never inline
f-strings at call sites. The schema identifier is the ONLY interpolated
value: every constant carries a literal ``{schema}`` token substituted at
:meth:`WorkflowSql.build` time after the schema name validates against
``taskq.constants.require_schema``. Every caller-controlled value uses
asyncpg ``$N`` positional parameter binding.

THE JSONB LANDMINE (the fanout proof's cut #1, HIGH — the rule's test lives
in the engine pins): asyncpg parses ``$N`` as a parameter placeholder EVEN
INSIDE a quoted JSONB literal. An f-string that renders ``'{"peer":$3}'::jsonb``
fails with ``invalid input syntax for type json`` on every attempt. The rule:
parameterize all JSON (``$3::jsonb`` with a dict param), NEVER interpolate
into a JSONB literal. This module's literal jsonb shapes interpolate nothing.

Multi-row writes (fork fan-out, sweep fires, drain consumers) bind PARALLEL
ARRAYS through ``unnest`` — one statement per table per batch, never one
round trip per row (the 1000-child fan-out tx band's shape). Every ``id``
column is minted APP-SIDE through the ``taskq._ids`` seam (uuid7) and bound
as a parameter — ``gen_random_uuid()`` and ``uuid4`` are checker-banned
repo-wide, and the seam-only generation pin greps this package.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from taskq.constants import require_schema

__all__ = [
    "BLOCKING_REASON_JOIN",
    "BLOCKING_REASON_ORPHAN_PARENT",
    "TERMINAL_SQL_SET",
    "WorkflowSql",
]

#: The terminal-status SQL set — the statement-side twin of
#: :data:`taskq.backend.statemachine.TERMINAL_STATUSES`. A literal (not a
#: bind) because it is fixed vocabulary, never caller input; the equivalence
#: to the Python frozenset is pinned by test.
TERMINAL_SQL_SET: Final[str] = "('succeeded','failed','cancelled','crashed','abandoned')"

#: metadata.blocking_reason values (the blocked-row representation, T03:
#: carried on the row's metadata jsonb, never a new ENUM).
BLOCKING_REASON_JOIN: Final[str] = "join"
BLOCKING_REASON_ORPHAN_PARENT: Final[str] = "orphan_parent"

_TERMINAL_MARK_SQL = """\
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
_DECREMENT_SQL = """\
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
_FIRE_SQL = """\
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
"""

_OUTBOX_INSERT_SQL = """\
INSERT INTO {schema}.wf_outbox
    (id, join_job_id, flow_id, consumer_step_key, map_index, bindings)
VALUES ($1, $2, $3, $4, $5, $6::jsonb)
"""

# The fork's child INSERT: parallel-array binds (one statement per chunk —
# never one round trip per child, the 1000-child fan-out tx band's shape).
# status is omitted — every child is born 'pending' (the column DEFAULT);
# retry_kind rides the vanilla vocabulary.
_FORK_CHILDREN_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::smallint[], $7::text[], $8::uuid[], $9::smallint[], $10::text[],
    $11::text[], $12::jsonb[], $13::text[], $14::text[]
)
"""

_FORK_EDGES_SQL = """\
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[])
"""

_FORK_JOIN_NODE_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
"""

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
_REDERIVE_SWEEP_SQL = """\
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
# minted APP-SIDE (uuid7 via the seam, never gen_random_uuid) as a bound
# array, matched by the firable row's ordinal — a row beyond the minted
# pool (a parent terminalized between the count and this statement, whose
# tx2 blocks on the held child lock) stays firable next pass, never NULL-id
# fired. The flow-status leg rides INSIDE the fire statement (pin 5): a
# flow that cancelled refuses here, in the sweep's own fire statement.
_SWEEP_FIRE_SQL = """\
WITH locked AS (
    SELECT c.id, c.step_key, c.metadata->>'flow_id' AS flow_id
    FROM {schema}.jobs c
    WHERE c.status = 'pending'
      AND c.deps_pending > 0
      AND c.metadata @> '{{"blocking_reason": "join"}}'::jsonb
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
SELECT join_job_id, flow_id, step_key FROM wins
"""

# The outbox drain's consumer insert: IDEMPOTENT on the consumer step key —
# the composite (idempotency_scope, idempotency_key) arbiter
# (jobs_idempotency_scope_key_uniq, 01.00.03) is the dedup authority. A crash
# between fire-commit and consumer-insert re-drains; the arbiter makes the
# retry a no-op (pin 20: the drain completes the dispatch EXACTLY ONCE).
_OUTBOX_DRAIN_CONSUMERS_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, map_index, step_key, trace_id, metadata,
     idempotency_scope, idempotency_key)
SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::smallint[],
    $6::smallint[], $7::text[], $8::uuid[], $9::smallint[], $10::text[],
    $11::text[], $12::jsonb[], $13::text[], $14::text[]
)
ON CONFLICT (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id
"""

_OUTBOX_DRAIN_FLIP_SQL = """\
UPDATE {schema}.wf_outbox
SET delivered = true
WHERE id = ANY($1::uuid[])
  AND NOT delivered
RETURNING id
"""

_OUTBOX_FETCH_UNDELIVERED_SQL = """\
SELECT o.id, o.join_job_id, o.flow_id, o.consumer_step_key, o.map_index, o.bindings
FROM {schema}.wf_outbox o
WHERE NOT o.delivered
ORDER BY o.id
LIMIT $1
FOR UPDATE OF o SKIP LOCKED
"""

# The step-ledger claim: ONE round trip, P1 FINAL's idempotent-claim shape —
# INSERT ... ON CONFLICT DO UPDATE ... RETURNING, never check-then-insert
# (verified 30 reps x 10 concurrent). A fresh claim inserts status='running'
# (the attempt increments at claim, the only grant of work); a conflicting
# claim returns the EXISTING row — the UNIQUE (flow_id, step_key, attempt)
# triple physically blocks double-recording (P3 rule 2), and the ledger
# terminal write rides the finalize's own transaction.
_LEDGER_CLAIM_SQL = """\
INSERT INTO {schema}.wf_step_ledger
    (id, flow_id, job_id, step_key, map_index, attempt, status)
VALUES ($1, $2, $3, $4, $5, $6, 'running')
ON CONFLICT (flow_id, step_key, attempt) DO UPDATE
    SET updated_at = clock_timestamp()
RETURNING id, flow_id, job_id, step_key, map_index, attempt, status,
          result, error_class, error_message
"""

# The memoized-result read (map-child retries, ctx.step replay): the latest
# TERMINAL ledger row for (flow, step[, map_index]) — the ON CONFLICT path
# returns the recorded result rather than re-executing. attempt is NOT in
# this key: a retried map child claims a NEW attempt row, but the replay
# consults any prior terminal outcome first (the retry's side effect is the
# recorded result's, never a fresh execution).
_LEDGER_MEMOIZED_SQL = """\
SELECT attempt, status, result, error_class, error_message, job_id
FROM {schema}.wf_step_ledger
WHERE flow_id = $1
  AND step_key = $2
  AND ((map_index = $3::smallint) OR ($3::smallint IS NULL AND map_index IS NULL))
  AND status IN ('succeeded', 'failed')
ORDER BY attempt DESC
LIMIT 1
"""

# The ledger's terminal-outcome write — rides the finalize's OWN transaction
# (the ledger-terminal-atomic rule, hardening H9): a split write leaves
# node=succeeded with ledger=running, the phantom the fence prevents.
_LEDGER_TERMINAL_SQL = """\
UPDATE {schema}.wf_step_ledger
SET status = $4,
    result = $5::jsonb,
    error_class = $6,
    error_message = $7,
    capture = $8::jsonb,
    updated_at = clock_timestamp()
WHERE flow_id = $1
  AND step_key = $2
  AND attempt = $3
RETURNING id
"""

# A fenced-out attempt is recorded outcome='fenced' — never a running ledger
# row left forever on a terminal flow (hardening H1: "neither landed nor
# refused" is the state the linearization doctrine forbids).
_LEDGER_FENCE_ATTEMPT_SQL = """\
UPDATE {schema}.wf_step_ledger
SET status = 'fenced',
    error_class = $4,
    updated_at = clock_timestamp()
WHERE flow_id = $1
  AND step_key = $2
  AND attempt = $3
  AND status = 'running'
RETURNING id
"""

# The fenced-attempt sweep arm (hardening H1-H3): reap phantom 'running'
# ledger rows on terminal flows so the rows-alone reconstruction reconciles
# (a terminal flow is reconstructible from rows alone; pin 15).
_PHANTOM_REAP_SQL = """\
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

# The single-node insert (the workflow-row enqueue path's core write; T09's
# API wraps it). A joined node is born with deps_pending = <declared parent
# count> and blocking_reason='join' in metadata; vanilla enqueues never set
# it (DEFAULT 0 — a semantic no-op for them). trace_id is stamped on the
# same insert (§18.2's cheap survivor — one field, no extra write).
_NODE_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     parent_id, map_index, step_key, deps_pending, trace_id, metadata,
     idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $14)
"""


_FLOW_STATUS_SQL = """\
SELECT id, status FROM {schema}.jobs WHERE id = $1
"""

# The RUN-KEY claim (G2): the flow's root row inserted under the
# 'workflow-run' scope with the caller's key — the composite
# (idempotency_scope, idempotency_key) arbiter is the rememberer. The root
# row IS the run: its status is the run's status (the linearization point
# every flow-status leg checks), step_key names the entry step.
_FLOW_RUN_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     step_key, trace_id, metadata, idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, $7, $8, $9::jsonb, $10, $11)
ON CONFLICT (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id, status::text
"""

_FLOW_RUN_READ_SQL = """\
SELECT id, status::text
FROM {schema}.jobs
WHERE idempotency_scope = $1 AND idempotency_key = $2
"""


@dataclass(frozen=True, slots=True)
class WorkflowSql:
    """The workflow statement bundle, rendered for one validated schema."""

    schema: str
    terminal_mark: str
    decrement: str
    fire: str
    outbox_insert: str
    outbox_fetch_undelivered: str
    outbox_drain_consumers: str
    outbox_drain_flip: str
    fork_children: str
    fork_edges: str
    fork_join_node: str
    rederive_sweep: str
    sweep_fire: str
    ledger_claim: str
    ledger_memoized: str
    ledger_terminal: str
    ledger_fence_attempt: str
    phantom_reap: str
    node_insert: str
    flow_status: str
    flow_run_insert: str
    flow_run_read: str

    @staticmethod
    def build(schema: str) -> WorkflowSql:
        """Render every constant for *schema* (validated via
        ``taskq.constants.require_schema``).

        All user-supplied values use ``$N`` parameter binding — only the
        schema identifier is interpolated, and it is validated here before
        any statement renders (the S608 rationale, the same discipline as
        ``backend/_sql_templates.render``).
        """
        require_schema(schema)
        subs: Final[tuple[tuple[str, str], ...]] = (
            ("{schema}", schema),
            ("{terminal}", TERMINAL_SQL_SET),
        )

        def render(template: str) -> str:
            out = template
            for token, value in subs:
                out = out.replace(token, value)
            # The doubled braces in the jsonb shape literals are the
            # migration-runner convention; substitution needs them single.
            return out.replace("{{", "{").replace("}}", "}")

        return WorkflowSql(
            schema=schema,
            terminal_mark=render(_TERMINAL_MARK_SQL),
            decrement=render(_DECREMENT_SQL),
            fire=render(_FIRE_SQL),
            outbox_insert=render(_OUTBOX_INSERT_SQL),
            outbox_fetch_undelivered=render(_OUTBOX_FETCH_UNDELIVERED_SQL),
            outbox_drain_consumers=render(_OUTBOX_DRAIN_CONSUMERS_SQL),
            outbox_drain_flip=render(_OUTBOX_DRAIN_FLIP_SQL),
            fork_children=render(_FORK_CHILDREN_SQL),
            fork_edges=render(_FORK_EDGES_SQL),
            fork_join_node=render(_FORK_JOIN_NODE_SQL),
            rederive_sweep=render(_REDERIVE_SWEEP_SQL),
            sweep_fire=render(_SWEEP_FIRE_SQL),
            ledger_claim=render(_LEDGER_CLAIM_SQL),
            ledger_memoized=render(_LEDGER_MEMOIZED_SQL),
            ledger_terminal=render(_LEDGER_TERMINAL_SQL),
            ledger_fence_attempt=render(_LEDGER_FENCE_ATTEMPT_SQL),
            phantom_reap=render(_PHANTOM_REAP_SQL),
            node_insert=render(_NODE_INSERT_SQL),
            flow_status=render(_FLOW_STATUS_SQL),
            flow_run_insert=render(_FLOW_RUN_INSERT_SQL),
            flow_run_read=render(_FLOW_RUN_READ_SQL),
        )
