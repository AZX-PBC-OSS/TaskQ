"""The flow RUNNER's named statements (the split of ``api/_runner`` —
§7b's concerns-separate law): every SQL template the runner drives, named
once and rendered through :func:`render_sql` (never inline SQL, never an
f-string — the S608 rule; the schema renders via the estate's validator).

Module homes: the driving loop lives in ``_runner``; the loop machinery
in ``_runner_loop``; the ladder in ``_runner_ladder``; the exit in
``_runner_exit``; the chain/router finalize in ``_runner_chain``; the
body's runtime context in ``_ctx``/``_ctx_wait``.
"""

from __future__ import annotations

from typing import Any

import asyncpg

__all__ = ["wf_conn_fetchval"]

#: The flow input key on the root row's payload (cut #7: the input rides
#: the ROW — a restart between stages reads it back).
INPUT_KEY = "wf_input"

#: The skip record's result marker (the v1 skip semantics: the node
#: succeeds WITH the record — the envelope never lies about what ran).
SKIPPED_RESULT: dict[str, object] = {"skipped": True}

#: The data-args key on the node row's payload (the wiring's plain data
#: rides the ROW — the restart reads it back; never a closure).
WF_ARGS_KEY = "wf_args"

#: The map child's single-arg key (the fork's per-item identity).
ITEM_KEY = "wf_item"

NODE_CLAIM_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'running', locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds',
    attempt = attempt + 1, claim_epoch = 0
WHERE id = $1
  AND (
    status = 'pending'
    OR (status = 'scheduled' AND scheduled_at <= now())
  )
  AND deps_pending = 0
  -- THE HOLD-STAMP LEG (the fence's last word — the two-driver race's
  -- wedged-hold cure, the same leg the worker's claimable fence carries
  -- since the D2 soak's wedge): the claimable SELECT's snapshot can
  -- PREDATE the hold's mint (the other driver's body minted the hold
  -- between this driver's SELECT and this WRITE) — a stamp that rides
  -- the mint's OWN transaction (the signal row + the held representation
  -- are ONE tx) means the row is HELD: re-claiming it strands the
  -- re-claimer's body parked on a delivery this driver can never see
  -- (the parked-forever review row the two-drivers pin convicted).
  AND NOT metadata ? 'hold'
RETURNING attempt
"""

NODE_REPEND_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'scheduled', locked_by_worker = NULL, scheduled_at = now() + ($2::double precision * interval '1 second')
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

PARENT_RESULTS_BY_KEY_SQL_TEMPLATE = """
SELECT p.step_key, p.result
FROM {schema}.wf_edge e
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE e.child_id = $1
ORDER BY p.id
"""

NODE_BY_STEP_KEY_SQL_TEMPLATE = """
SELECT id FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: The streaming source's checkpointed cursor read (T20): the key is the
#: bound EMIT_CURSOR_KEY constant — never an f-string SQL (the S608 rule);
#: the row is the SOURCE's own.
SOURCE_CURSOR_SQL_TEMPLATE = """
SELECT metadata -> $2 FROM {schema}.jobs WHERE id = $1
"""

EDGE_INSERT_SQL_TEMPLATE = """
INSERT INTO {schema}.wf_edge (child_id, parent_id, flow_id, failure_policy)
VALUES ($1, $2, $3, $4)
"""

INCREMENT_DEPS_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET deps_pending = deps_pending + 1,
    -- THE JOIN-WAIT MARK (the engine's own law, obeyed on the static
    -- create path too — attack-4 F-P4-WHYSTUCK-FALSE-REMEDY's root):
    -- "a JOINED node is born in join-wait: metadata carries
    -- blocking_reason='join'" (insert_node's contract). The static
    -- node pass inserted bare metadata and bumped the counter without
    -- the stamp, so the failed-parent cascade AND the sweep's re-derive
    -- (both keyed on the stamp) never saw the row — it stranded in
    -- deps_pending=1 with NO reason: the join never fired, the flow
    -- never terminalized, and the status read told the operator the
    -- join was waiting on parents that had ALREADY finalized.
    metadata = metadata || '{"blocking_reason": "join"}'::jsonb
WHERE id = $1
"""

CLAIMABLE_NODES_SQL_TEMPLATE = """
SELECT id, step_key, map_index, attempt, trace_id, payload FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND status IN ('pending', 'scheduled')
  AND (scheduled_at IS NULL OR scheduled_at <= now())
  AND deps_pending = 0
  AND step_key <> '__flow__'
  AND NOT metadata ? 'hold'
ORDER BY id
"""

#: THE NODE CENSUS (the create-seam's retry-completion read): the run's
#: node-row count — a nodeless root (the orphan's signature: pending +
#: zero nodes, the pre-cure debris shape) counts ZERO. The same
#: lineage-and-not-the-root predicate every node read spells.
FLOW_NODE_CENSUS_SQL_TEMPLATE = """
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
"""

FLOW_STATUS_SQL_TEMPLATE = """
SELECT status FROM {schema}.jobs WHERE id = $1
"""

FLOW_PAYLOAD_SQL_TEMPLATE = """
SELECT payload FROM {schema}.jobs WHERE id = $1
"""

SUCCEEDED_RESULTS_SQL_TEMPLATE = """
SELECT step_key, result FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'succeeded'
"""

HELD_COUNT_SQL_TEMPLATE = """
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'pending'
  AND metadata ? 'hold'
"""

ROOT_START_SQL_TEMPLATE = """
UPDATE {schema}.jobs SET status = 'running' WHERE id = $1 AND status = 'pending'
"""

CANCEL_ROOT_SQL_TEMPLATE = """
WITH flipped AS (
    UPDATE {schema}.jobs SET status = 'cancelled',
        error_class = 'WorkflowCancelled',
        error_message = $2,
        finished_at = clock_timestamp()
    WHERE id = $1
      AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
    RETURNING id, status::text AS to_state
), evt AS (
    -- THE AUDIT LEG (the operator's march's battery finding): the
    -- cancel's flip IS a mutation — the events reader gets the
    -- state_change that names it (the shared tier invariant reads a
    -- terminal row's OWN event here).
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', f.to_state,
                              'error_class', 'WorkflowCancelled')
    FROM flipped f
)
SELECT id FROM flipped
"""

CANCEL_NODES_SQL_TEMPLATE = """
WITH flipped AS (
    UPDATE {schema}.jobs
    SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
        finished_at = CASE WHEN status = 'running' THEN finished_at ELSE clock_timestamp() END,
        error_class = CASE WHEN status = 'running' THEN error_class ELSE 'WorkflowCancelled' END,
        metadata = metadata || '{"cancel_phase": "cooperative"}'::jsonb
    WHERE (metadata->>'flow_id')::uuid = $1
      AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
    RETURNING id, (status::text) AS to_state
), evt AS (
    -- THE AUDIT LEG: only the rows THIS statement actually
    -- terminalised (a running row's cooperative phase-1 keeps it
    -- running — its own terminal write carries its event).
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'pending', 'to_state', f.to_state,
                              'error_class', 'WorkflowCancelled')
    FROM flipped f
    WHERE f.to_state = 'cancelled'
)
SELECT count(*) FROM flipped
"""

TERMINAL_RESULT_SQL_TEMPLATE = """
SELECT result FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: THE EXIT'S DOWNSTREAM MARK (§17.1): the compiled descendants the exit
#: resolved — skipped WITH the record (the envelope never lies about the
#: nodes that didn't get to run), zero ledger rows. Terminal rows and
#: unspawned rows are untouched.
EXIT_SKIP_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'succeeded',
    result = $2::jsonb,
    finished_at = clock_timestamp()
WHERE (metadata->>'flow_id')::uuid = $1
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
  AND step_key = ANY($3)
"""

#: THE MANUAL RESUME'S NODE CAS (§17.2): terminal-FAILED → pending — ONE
#: statement, the only grant; the attempt ordinal untouched (CONTINUES —
#: the ladder's own count is the budget).
RETRY_NODE_CAS_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'pending',
    error_class = NULL,
    error_message = NULL,
    scheduled_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL
WHERE step_key = $2
  AND (metadata->>'flow_id')::uuid = $1
  AND status = 'failed'
RETURNING id
"""

#: THE CLOSURE RE-OPENS: the cascade's blocked rows return to join-wait —
#: the sweep's re-derive re-derives them from the edge ledger (a stamp is
#: the cache, never the truth).
RETRY_REOPEN_CLOSURE_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(metadata, '{blocking_reason}', '"join"'::jsonb, true)
WHERE (metadata->>'flow_id')::uuid = $1
  AND step_key = ANY($2)
  AND metadata @> '{"blocking_reason": "failed_parent"}'::jsonb
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
"""

#: THE FLOW RE-OPENS: a terminal-FAILED root returns to running (the
#: manual resume's own linearization; a CANCELLED root stays closed).
RETRY_FLOW_REOPEN_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'running', finished_at = NULL
WHERE id = $1
  AND status = 'failed'
"""


async def wf_conn_fetchval(pool: asyncpg.Pool, schema: str, query: str, *args: object) -> Any:
    """One rendered statement's single value (the runner's ad-hoc read
    seam — the schema rendered via the estate's validator)."""
    async with pool.acquire() as conn:
        return await conn.fetchval(render_sql(query, schema), *args)


def render_sql(template: str, schema: str) -> str:
    return template.replace("{schema}", schema)
