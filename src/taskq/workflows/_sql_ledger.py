"""The ledger statement constants : the step-ledger claim (one round trip), the memoized-result read, the terminal + fence writes, the run-key arbiter's insert/read.

The schema identifier is the only interpolated value (validated at
WorkflowSql.build); every caller-controlled value uses ``$N`` parameter
binding. See _sql.py for the bundle and the JSONB-landmine rule.
"""

from __future__ import annotations

# The step-ledger claim: one round trip, P1 final's idempotent-claim shape —
# INSERT ... ON CONFLICT DO UPDATE ... RETURNING, never check-then-insert
# (verified 30 reps x 10 concurrent). A fresh claim inserts status='running'
# (the attempt increments at claim, the only grant of work); a conflicting
# claim returns the existing row — the claim arbiter
# (wf_step_ledger_claim_uniq, 01.00.24_03 + 01.00.42_01: (flow_id, step_key,
# COALESCE(map_index, -1), attempt, claim_epoch)) physically blocks
# double-recording (P3 rule 2), and the ledger terminal write rides the
# finalize's own transaction. claim_epoch joins the arbiter because the
# physical attempt ordinal is refunded and reminted by the capacity park's
# 1 -> 0 -> 1 cycle: every dispatch claim records its OWN ledger row, so the
# parked attempt's 'awaited' record survives beside the resumed attempt's
# terminal. The in-process driver's own claim binds the CAS-granted epoch
# (the same pair every context/finalize fence carries); a synthetic
# caller with no dispatch claim behind it stamps 0.
#
# map_index, -1. map children of one step key are different claims (the run-key
# contract) — a conflict target omitting map_index collapses them onto one row
# (child 1's claim returns child 0's row, the terminal write overwrites, the
# memoized replay returns the wrong child's result). The expression spells the
# index verbatim — an ON CONFLICT target infers only an arbiter carrying the
# identical expression.
LEDGER_CLAIM_SQL = """\
INSERT INTO {schema}.wf_step_ledger
    (id, flow_id, job_id, step_key, map_index, attempt, claim_epoch, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'running')
ON CONFLICT (flow_id, step_key, COALESCE(map_index, -1), attempt, claim_epoch) DO UPDATE
    SET updated_at = clock_timestamp()
RETURNING id, flow_id, job_id, step_key, map_index, attempt, status,
          result, error_class, error_message, updated_at
"""


# The memoized-result read (map-child retries, ctx.step replay): the latest
# TERMINAL ledger row for (flow, step[, map_index]) — the ON CONFLICT path
# returns the recorded result rather than re-executing. attempt is not in
# this key: a retried map child claims a new attempt row, but the replay
# consults any prior terminal outcome first (the retry's side effect is the
# recorded result's, never a fresh execution).
#
# The ordering is the CLAIM HISTORY's own (attempt, claim_epoch, created_at,
# id): equivalent physical attempts resolve by their actual claim epoch, so
# the latest CURRENT claimant is deterministic — never buffer/UUID arrival
# order.
LEDGER_MEMOIZED_SQL = """\
SELECT attempt, status, result, error_class, error_message, job_id
FROM {schema}.wf_step_ledger
WHERE flow_id = $1
  AND step_key = $2
  AND ((map_index = $3::bigint) OR ($3::bigint IS NULL AND map_index IS NULL))
  AND status IN ('succeeded', 'failed')
ORDER BY attempt DESC, claim_epoch DESC, created_at DESC, id DESC
LIMIT 1
"""


# The ledger's terminal-outcome write — rides the finalize's own transaction
# (the ledger-terminal-atomic rule, the terminal-atomicity rule): a split write leaves
# node=succeeded with ledger=running, the phantom the fence prevents.
#
# ONE keying — the write touches exactly the claimed row:
#   LEDGER_TERMINAL_BY_ID_SQL keys the claim's OWN RETURNING id AND its own
#   claim instant: the caller that holds the claim (claim_step_ledger →
#   LedgerClaim.ledger_id/claimed_at) pins the write to that row and that
#   stamp and nothing else. There is no four-expression fallback and no
#   instant-less bypass in current production claims: the epoch arbiter
#   (flow, step, COALESCE(map,-1), attempt, claim_epoch) can carry several
#   rows per (flow, step, attempt) — an update keyed only on those four
#   expressions would rewrite the parked attempt's 'awaited' history beside
#   the current row (the demonstrated UPDATE 2), and a status-only by-id
#   write could re-stamp a row a later re-mint owns.
LEDGER_TERMINAL_BY_ID_SQL = """\
UPDATE {schema}.wf_step_ledger
SET status = $2,
    result = $3::jsonb,
    error_class = $4,
    error_message = $5,
    capture = $6::jsonb,
    updated_at = clock_timestamp()
WHERE id = $1
  AND status IN ('running', 'awaited', 'crashed')
  -- The re-mint guard: the caller presents its OWN claim's RETURNING
  -- instant (``claim_step_ledger``'s updated_at — the claim's instance
  -- identity) and writes ONLY the row that claim actually wrote, at the
  -- stamp that claim left. The arbiter's re-use (the refund re-claims the
  -- SAME ordinal → the SAME arbiter row) re-stamps updated_at, and the
  -- stale instant REFUSES the write: a re-minted LIVE winner's ledger row
  -- can never inherit a stale claimant's stamp (the deadline's 'failed'
  -- burn included).
  AND updated_at = $7::timestamptz
RETURNING id
"""


# A fenced-out attempt is recorded outcome='fenced' — never a running ledger
# row left forever on a terminal flow ("neither landed nor
# refused" is the state the linearization doctrine forbids). The same exact
# claim identity as the terminal write (the claim's own id + instant);
# the guard covers the claim's nonterminal states; a terminal result is
# never re-fenced.
LEDGER_FENCE_BY_ID_SQL = """\
UPDATE {schema}.wf_step_ledger
SET status = 'fenced',
    error_class = $2,
    updated_at = clock_timestamp()
WHERE id = $1
  AND status IN ('running', 'awaited', 'crashed')
  -- The re-mint guard: a caller presenting its OWN claim's
  -- RETURNING instant (``claim_step_ledger``'s updated_at — the claim's
  -- instance identity) fences ONLY the row that claim actually wrote.
  -- The refund re-mints the ordinal — the arbiter re-use hands a LIVE
  -- winner the SAME ledger row (the DO UPDATE re-stamps updated_at) —
  -- and the stale instant REFUSES the fence: the winner's own terminal
  -- writes the row, the record never marks a live attempt 'fenced'.
  AND updated_at = $3::timestamptz
RETURNING id
"""


# The run-key claim: the flow's root row inserted under the
# 'workflow-run' scope with the caller's key — the composite
# (idempotency_scope, idempotency_key) arbiter is the rememberer. The root
# row is the run: its status is the run's status (the linearization point
# every flow-status leg checks), step_key names the entry step.
FLOW_RUN_INSERT_SQL = """\
INSERT INTO {schema}.jobs
    (id, actor, queue, payload, attempt, max_attempts, retry_kind,
     step_key, trace_id, metadata, idempotency_scope, idempotency_key,
     required_worker_schema, run_epoch)
VALUES ($1, $2, $3, $4::jsonb, 0, $5, $6, '__flow__', $7, $8::jsonb, $9, $10, $11, 1)
ON CONFLICT (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id, status::text, required_worker_schema
"""


FLOW_RUN_READ_SQL = """\
SELECT id, status::text, required_worker_schema
FROM {schema}.jobs
WHERE idempotency_scope = $1 AND idempotency_key = $2
"""
