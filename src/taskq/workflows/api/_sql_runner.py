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

from typing import Any, Final

import asyncpg

from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import WF_LOOP_ESCALATION_STEP_KEY
from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS

__all__ = ["wf_conn_fetchval"]

#: THE TERMINAL-FLOW FENCE'S SET (the fleet's dispatch fence's own
#: derivation — ``statemachine.TERMINAL_STATUSES`` rendered once at
#: import, the engine's own vocabulary, never caller input): the two
#: claim doors may not disagree — the fleet's dispatch refuses a
#: terminal flow's rows, so the in-process door's claim refuses the
#: same. Rendered by the same token-replace the fleet's
#: ``_wf_dispatch_fence`` uses (never ``.format`` — the templates keep
#: ``{schema}`` for the call-site render).
_TERMINAL_FLOW_SQL_SET: str = "('" + "','".join(sorted(TERMINAL_STATUSES)) + "')"


def _render_fence_tokens(template: str) -> str:
    """The in-process fence's token substitution (the fleet's
    ``_wf_dispatch_fence``'s shape, mirrored): the terminal-status set
    and the escalation step key are the engine's own constants — never
    caller input; every user-controlled value in the statement is
    $n-bound by the templates this renders."""
    return template.replace("__WF_TERMINAL__", _TERMINAL_FLOW_SQL_SET).replace(
        "__WF_ESCALATION_STEP__", WF_LOOP_ESCALATION_STEP_KEY
    )


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

NODE_CLAIM_SQL_TEMPLATE = _render_fence_tokens("""
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
  -- THE TERMINAL-FLOW FENCE (the fleet's dispatch fence's parity —
  -- rv6-e1's cure): a node row on a TERMINAL flow is not work — the
  -- flow flip is the cancel's linearization point, and the claim CAS
  -- re-checks the flow status INSIDE its own statement (the same
  -- leg the dispatch fence carries at its own claim seat). A claimable
  -- SELECT's snapshot can PREDATE the cancel's commit (the drain's
  -- documented exemption window: the join fire won, the cascade landed
  -- after); the SELECT's fence closes the common window, THIS leg
  -- closes the race — the claim is the GRANT of work, and a granted
  -- body executes. The exemption rides EXACTLY as the fleet fence
  -- carries it (the loop's registered escalation consumer — the
  -- pages-a-human duty: the exhaust tx commits the escalation's outbox
  -- row and the flow's terminal TOGETHER, so the consumer row is BORN
  -- into a terminal flow; the two fences may not disagree).
  AND (
    step_key = '__WF_ESCALATION_STEP__'
    OR NOT EXISTS (
      SELECT 1 FROM {schema}.jobs wf_flow
      WHERE wf_flow.id = ({schema}.jobs.metadata->>'flow_id')::uuid
        AND wf_flow.status IN __WF_TERMINAL__
    )
  )
RETURNING attempt
""")

NODE_REPEND_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'scheduled', locked_by_worker = NULL, scheduled_at = now() + ($2::double precision * interval '1 second')
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

# THE DEADLINE'S RE-PEND (the wf start_to_close cure — the expiry's
# disposition's own write): the ladder's re-pend shape
# (NODE_REPEND_SQL_TEMPLATE) with THE CANCEL FENCE IN-STATEMENT and THE
# BUDGET GATE — the vanilla ``mark_retry``'s own arms mirrored (the same
# shape NODE_DEFER_SQL's deferral arms keep):
#
#   * 'repended' — the clean face: the row 'scheduled' at the backoff
#     (the burn's retry), the cancel columns untouched;
#   * 'exhausted' — THE BUDGET'S BOUND (the retry kind is ``permanent``,
#     or the ladder's failed count reached ``max_attempts`` — the caller
#     prices both into $3): the arm WRITES NOTHING (the wf terminal door
#     owns every death — the row must still be 'running' for the
#     terminal mark's fence), the verdict names the caller's route;
#   * 'cancel_fenced' — THE CANCEL WINS (the vanilla mark_retry's own
#     law: a phase-carrying row — the wf cancel's metadata mark OR the
#     vanilla cancel's own column mark — matches NO arm; the deadline's
#     backoff must never re-queue a row a cancel in flight owns): the
#     caller routes the node through the wf terminal door, the C4
#     deferral's cancel_faced shape;
#   * None — the ZOMBIE (the claim fence matched nothing, a newer
#     attempt owns the row): the caller records the ledger's 'fenced',
#     H1.
#
# THE CLAIM IDENTITY'S FENCE (the fence legs bind the caller's own claim
# — the NODE_DEFER_SQL's own shape): the deadline's disposition carries the
# claim's own identity legs — worker + attempt + claim_epoch, the
# terminal-mark CAS's full vocabulary. The pre-cure WHERE (id + status +
# the cancel marks) let a ZOMBIE W1's deadline disposition flip a
# re-minted LIVE W2 row (the 90s wf lease lapses → the reclaim +
# re-claim as W2 → W1's timer fires → W2's row to 'scheduled', its
# ledger phantom-runs, its finalize refuses): the row the fence matches
# must be the row the CALLER'S OWN CLAIM holds, or every arm refuses and
# the no-match routes to the zombie face (the ledger's 'fenced' record,
# keyed on the claim's own instant — the C5 shape).
#
# Keyed to the ONE claimed node row; the two read arms write nothing.
NODE_DEADLINE_REPEND_SQL = """\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::double precision AS backoff_s,
           $3::boolean AS exhausted,
           $4::uuid AS worker_id,
           $5::int AS attempt,
           $6::bigint AS claim_epoch
),
repended AS (
    UPDATE {schema}.jobs j
    SET status = 'scheduled',
        locked_by_worker = NULL,
        scheduled_at = now() + ((SELECT backoff_s FROM params) * interval '1 second')
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      -- THE CLAIM IDENTITY'S FENCE (the NODE_DEFER_SQL's own CAS
      -- vocabulary): a zombie's deadline disposition — its claim lapsed,
      -- the row re-claimed and LIVE under a newer claimant — matches
      -- NOTHING (the row is the winner's; its own deadline owns it).
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      -- THE BUDGET GATE (the vanilla mark_retry's own max_attempts
      -- fence — the exhausted face's row must stay 'running' for the
      -- terminal mark's fence).
      AND NOT (SELECT exhausted FROM params)
      -- THE CANCEL FENCE (the vanilla mark_retry's own law, mirrored —
      -- the C1 cure's shape): a phase-carrying row on EITHER carrier
      -- matches NO arm.
      AND j.cancel_phase = 0
      AND NOT j.metadata ? 'cancel_phase'
    RETURNING j.id
),
cancel_fenced AS (
    SELECT j.id
    FROM {schema}.jobs j
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      -- THE CLAIM IDENTITY'S FENCE (the repended arm's own legs): the
      -- cancel's verdict names the CALLER'S OWN claim-carrying row — a
      -- zombie's deadline never terminalises the winner's row
      -- 'cancelled' on its view.
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.claim_epoch = (SELECT claim_epoch FROM params)
      AND (j.metadata ? 'cancel_phase' OR j.cancel_phase <> 0)
      AND NOT EXISTS (SELECT 1 FROM repended)
)
SELECT 'repended'::text AS verdict, id FROM repended
UNION ALL
-- THE EXHAUSTED VERDICT (a pure read of the params + the two arms'
-- existence — the C4 NODE_DEFER_SQL's own arm order: the cancel mark
-- outranks the budget's bound, the repended face outranks both). THE
-- CLAIM IDENTITY'S FENCE rides this arm too: a budget-exhausted
-- ZOMBIE's deadline is the ZOMBIE's face (no arm matched — the caller
-- records 'fenced'), never the winner's terminal.
SELECT 'exhausted'::text AS verdict, NULL::uuid AS id
WHERE (SELECT exhausted FROM params)
  AND EXISTS (
      SELECT 1 FROM {schema}.jobs j
      WHERE j.id = (SELECT job_id FROM params)
        AND j.status = 'running'
        AND j.locked_by_worker = (SELECT worker_id FROM params)
        AND j.attempt = (SELECT attempt FROM params)
        AND j.claim_epoch = (SELECT claim_epoch FROM params)
  )
  AND NOT EXISTS (SELECT 1 FROM repended)
  AND NOT EXISTS (SELECT 1 FROM cancel_fenced)
UNION ALL
SELECT 'cancel_fenced'::text AS verdict, id FROM cancel_fenced
"""

PARENT_RESULTS_BY_KEY_SQL_TEMPLATE = """
SELECT p.id, p.step_key, p.result
FROM {schema}.wf_edge e
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE e.child_id = $1
  -- THE ABSORBED PARENT'S SLOT (T27's barrier cure — the envelope must
  -- not lie in the sum): on an ABSORBING edge (collect | maybe) a
  -- terminal-FAILED parent contributes NO result item to the packed
  -- join — its absence is ON THE RECORD (the join's metadata.failures
  -- array carries the typed FailureInfo + the policy that ran), never a
  -- junk {} slot the consumer's decode cannot honor. A fail_closed
  -- edge's consumer never runs over a failed parent (the cascade), so
  -- this filter changes no other face.
  AND NOT (p.status = 'failed' AND e.failure_policy IN ('collect', 'maybe'))
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

CLAIMABLE_NODES_SQL_TEMPLATE = _render_fence_tokens("""
SELECT id, step_key, map_index, attempt, trace_id, payload, start_to_close, metadata FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND status IN ('pending', 'scheduled')
  AND (scheduled_at IS NULL OR scheduled_at <= now())
  AND deps_pending = 0
  AND step_key <> '__flow__'
  AND NOT metadata ? 'hold'
  -- THE TERMINAL-FLOW FENCE (the fleet's dispatch fence's parity —
  -- rv6-e1's cure): a terminal/cancelled flow's nodes are UNCLAIMABLE
  -- through THIS door too — a bare tick() (the driver's public dispatch
  -- pass, never gated by drive()'s loop-top check) on a dead flow
  -- claims NOTHING and executes NOTHING. The fleet's dispatch carries
  -- the same leg at its own claim seat (the ESCALATION-KIND exemption
  -- included: the pages-a-human duty is the ONE thing that survives a
  -- flow's terminality — the two fences may not disagree, a probe that
  -- disagrees with its claim is the worker-sleeps-the-page-never-runs
  -- shape).
  AND (
    step_key = '__WF_ESCALATION_STEP__'
    OR NOT EXISTS (
      SELECT 1 FROM {schema}.jobs wf_flow
      WHERE wf_flow.id = ({schema}.jobs.metadata->>'flow_id')::uuid
        AND wf_flow.status IN __WF_TERMINAL__
    )
  )
ORDER BY id
""")

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

#: THE ROOT'S PRIORITY-STAMP READ (the wf-priority cure's ADOPTION leg):
#: ONE bounded primary-key read of the root's
#: metadata, the fleet door's own SELECT shape
#: (``_CLAIMED_NODE_READ_SQL_TEMPLATE``'s stamp leg). The arbiter adopted
#: an EXISTING run — its root stamp is its own truth (the row is the law,
#: never the echo): the adopting dev door resolves the run's per-call
#: override from HERE, so a re-drive forks its rows at the same priority
#: the create stamped (never the declarations — one run, one cohort).
ROOT_PRIORITY_STAMP_SQL_TEMPLATE = """
SELECT metadata->>'wf_priority' AS wf_priority
FROM {schema}.jobs WHERE id = $1
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
        error_class = '{wf_cancelled_class}',
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
                              'error_class', '{wf_cancelled_class}')
    FROM flipped f
)
SELECT id FROM flipped
"""

CANCEL_NODES_SQL_TEMPLATE = """
WITH flipped AS (
    UPDATE {schema}.jobs
    SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
        finished_at = CASE WHEN status = 'running' THEN finished_at ELSE clock_timestamp() END,
        error_class = CASE WHEN status = 'running' THEN error_class ELSE '{wf_cancelled_class}' END,
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
                              'error_class', '{wf_cancelled_class}')
    FROM flipped f
    WHERE f.to_state = 'cancelled'
)
SELECT count(*) FROM flipped
"""

TERMINAL_RESULT_SQL_TEMPLATE = """
SELECT id, step_key, result FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: THE FAILURE FACE (the teardown round's cure): the run's newest
#: terminal-FAILED NODE row (the root's own row carries the propagation
#: verdict — UnabsorbedNodeFailure — but the NODE row carries the body's
#: error class + message: the failed run's first question's ANSWER).
FLOW_FAILURE_SQL_TEMPLATE = """
SELECT step_key, error_class, error_message FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND status = 'failed'
  AND step_key <> '__flow__'
ORDER BY finished_at DESC NULLS LAST
LIMIT 1
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
#:
#: THE ROOT'S GUARD (H-C1): ``AND step_key <> '__flow__'`` — the run's
#: own ROOT row is not a node, and a "retry" that matched it flipped the
#: root failed→pending with the propagation verdict ERASED (the
#: maintenance rollup never saw the run again — the run bricked, live-
#: proven). The verb-level guard is the ONE seat: the admin route, the
#: CLI's ``flows retry``, and the compiled face all ride this statement,
#: and the root key's re-run is the RUN-rerun's own concern (a fresh run,
#: or the run-rerun's census — never the per-node CAS).
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
  AND step_key <> '__flow__'
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

#: THE SKIP ENVELOPE'S RE-ARM (H-C4): the run's exit-skipped rows (the
#: typed early-exit's downstream mark) return to pending with the
#: envelope CLEARED. A skip is not an attempt (zero ledger rows) — the
#: re-arm restores exactly the pre-skip shape: a claimable row whose
#: next claim is its FIRST attempt. The root's own row is out of the
#: write set (the H-C1 guard's law: the root is not a node).
#:
#: THE PREDICATE IS THE ENGINE'S OWN CONTRACT (the round-2 conviction):
#: ``result @> '{"skipped": true}'::jsonb`` — the shape the exit writer
#: (``SKIPPED_RESULT``/the EXIT_SKIP mark) actually lands, TRUE with
#: it. The old ``result ? 'skipped'`` was KEY-EXISTENCE: a foreign
#: writer's ``{"skipped": false}`` (or any JSON that merely CARRIES the
#: key) matched and had its result DELETED — the display's false-skip
#: lie, and a foreign writer's data destroyed. Containment matches the
#: TRUE envelope only: a false/"no" envelope survives untouched.
RETRY_SKIP_REARM_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'pending',
    result = NULL,
    finished_at = NULL,
    scheduled_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL
WHERE (metadata->>'flow_id')::uuid = $1
  AND status = 'succeeded'
  AND result @> '{"skipped": true}'::jsonb
  AND step_key <> '__flow__'
RETURNING id
"""


async def wf_conn_fetchval(pool: asyncpg.Pool, schema: str, query: str, *args: object) -> Any:
    """One rendered statement's single value (the runner's ad-hoc read
    seam — the schema rendered via the estate's validator)."""
    async with pool.acquire() as conn:
        return await conn.fetchval(render_sql(query, schema), *args)


#: The render's token substitutions (the house token-substitution pattern
#: — the ``subs`` list's shape, :meth:`WorkflowSql.build <taskq.workflows._sql.WorkflowSql.build>`
#: being where the other templates' tokens resolve): the templates stay
#: PLAIN strings — ``render_sql`` never ``.format``\s them (the jsonb
#: shape literals keep their braces, the ``{schema}`` token survives for
#: the call-site render), and each substitution is a plain ``str.replace``
#: of an exact token. The wf cancel's own error class is interpolated from
#: its one home (``api/_runner_defer``), never spelled per template.
_SQL_TOKEN_SUBS: Final[tuple[tuple[str, str], ...]] = (
    ("{wf_cancelled_class}", WORKFLOW_CANCELLED_CLASS),
)


def render_sql(template: str, schema: str) -> str:
    out = template.replace("{schema}", schema)
    for token, value in _SQL_TOKEN_SUBS:
        out = out.replace(token, value)
    return out
