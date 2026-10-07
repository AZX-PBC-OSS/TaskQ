"""The ledger statement constants (T04/T05): the step-ledger claim (one round trip), the memoized-result read, the terminal + fence writes, the run-key arbiter's insert/read.

The schema identifier is the ONLY interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

# The step-ledger claim: ONE round trip, P1 FINAL's idempotent-claim shape —
# INSERT ... ON CONFLICT DO UPDATE ... RETURNING, never check-then-insert
# (verified 30 reps x 10 concurrent). A fresh claim inserts status='running'
# (the attempt increments at claim, the only grant of work); a conflicting
# claim returns the EXISTING row — the UNIQUE (flow_id, step_key, attempt)
# triple physically blocks double-recording (P3 rule 2), and the ledger
# terminal write rides the finalize's own transaction.
LEDGER_CLAIM_SQL = """\
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
LEDGER_MEMOIZED_SQL = """\
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
LEDGER_TERMINAL_SQL = """\
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
LEDGER_FENCE_ATTEMPT_SQL = """\
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


# The RUN-KEY claim (G2): the flow's root row inserted under the
# 'workflow-run' scope with the caller's key — the composite
# (idempotency_scope, idempotency_key) arbiter is the rememberer. The root
# row IS the run: its status is the run's status (the linearization point
# every flow-status leg checks), step_key names the entry step.
FLOW_RUN_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     step_key, trace_id, metadata, idempotency_scope, idempotency_key)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, '__flow__', $7, $8::jsonb, $9, $10)
ON CONFLICT (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id, status::text
"""


FLOW_RUN_READ_SQL = """\
SELECT id, status::text
FROM {schema}.jobs
WHERE idempotency_scope = $1 AND idempotency_key = $2
"""
