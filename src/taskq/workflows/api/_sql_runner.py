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

from taskq._sql_render import render as shared_render
from taskq.backend._sql_fragments import emit_park_marker_core
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import WF_LOOP_ESCALATION_STEP_KEY
from taskq.workflows._sql_claim import current_claim_join
from taskq.workflows._sql_finalize import validated_reservation_width
from taskq.workflows._sql_status import absorbed_exists
from taskq.workflows.api._runner_defer import WORKFLOW_CANCELLED_CLASS

__all__ = ["wf_conn_fetchval"]

#: the two claim doors may not disagree — the fleet's dispatch refuses a
#: terminal flow's rows, so the in-process door's claim refuses the same.
#: Rendered by the same token-replace the fleet's ``_wf_dispatch_fence`` uses
#: (never ``.format`` — the templates keep ``{schema}`` for the call-site
#: render).
_TERMINAL_FLOW_SQL_SET: str = "('" + "','".join(sorted(TERMINAL_STATUSES)) + "')"


#: The root-existence half of the in-process fence, shared by the claim CAS
#: and the flow-scoped enumeration. Both doors must ask the same question —
#: the run's ``__flow__`` root must be PRESENT (the retired negative form
#: admitted a row whose root had been archived or deleted) — and the
#: escalation / after-terminal exemptions ride INSIDE the EXISTS so they pass
#: on a terminal root that is present and are refused once it is gone. Every
#: candidate-row column is qualified to the outer row: the inner scan's alias
#: shadows an unqualified ``step_key`` / ``metadata``.
_ROOT_EXISTENCE_FENCE_CORE = """\
      EXISTS (
        SELECT 1 FROM {schema}.jobs wf_flow
        WHERE wf_flow.id = ({schema}.jobs.metadata->>'flow_id')::uuid
          AND wf_flow.step_key = '__flow__'
          AND (
            wf_flow.status NOT IN __WF_TERMINAL__
            OR {schema}.jobs.step_key = '__WF_ESCALATION_STEP__'
            OR {schema}.jobs.metadata ? 'wf_after_terminal_effect'
          )
      )"""

#: The claim CAS is generic over ``jobs``: a vanilla row must stay claimable,
#: so the null-safe ``IS NULL`` arm stays (an equality/NOT wrapper would make
#: a NULL ``step_key`` evaluate to unknown and refuse every vanilla row).
_CLAIM_ROOT_FENCE = (
    """\
  AND (
    {schema}.jobs.step_key IS NULL
    OR (
      {schema}.jobs.step_key <> '__flow__'
      AND """
    + _ROOT_EXISTENCE_FENCE_CORE
    + """
    )
  )"""
)

#: The enumeration is flow-scoped (``metadata->>'flow_id' = $1``) and carries
#: an unconditional ``step_key <> '__flow__'`` pre-filter, so its rows can
#: never be vanilla: the ``IS NULL`` arm would be dead text here (``<>`` on a
#: NULL step_key yields unknown, excluding the row before the fence sees it).
#: The fragment therefore states only the flow-scoped conjuncts, and the
#: pre-filter's redundancy is deliberate: it is the enumeration's own
#: node-only contract, not a fence leg that may be deleted.
_ENUM_ROOT_FENCE = (
    """\
  AND (
    {schema}.jobs.step_key <> '__flow__'
    AND """
    + _ROOT_EXISTENCE_FENCE_CORE
    + """
  )"""
)


def _render_fence_tokens(template: str) -> str:
    """The in-process fence's token substitution (the fleet's
    ``_wf_dispatch_fence``'s shape, mirrored): the terminal-status set
    and the escalation step key are the engine's own constants — never
    caller input; every user-controlled value in the statement is
    $n-bound by the templates this renders."""
    return template.replace("__WF_TERMINAL__", _TERMINAL_FLOW_SQL_SET).replace(
        "__WF_ESCALATION_STEP__", WF_LOOP_ESCALATION_STEP_KEY
    )


#: The flow input key on the root row's payload (the input rides
#: the row — a restart between stages reads it back).
INPUT_KEY = "wf_input"

#: The skip record's result marker (the v1 skip semantics: the node
#: succeeds with the record — the envelope never lies about what ran).
SKIPPED_RESULT: dict[str, object] = {"skipped": True}

#: The data-args key on the node row's payload (the wiring's plain data
#: rides the row — the restart reads it back; never a closure).
WF_ARGS_KEY = "wf_args"

#: The map child's single-arg key (the fork's per-item identity).
ITEM_KEY = "wf_item"

NODE_CLAIM_SQL_TEMPLATE = _render_fence_tokens(
    """
UPDATE {schema}.jobs
SET status = 'running', locked_by_worker = $2, lock_expires_at = now() + interval '90 seconds',
    started_at = COALESCE(started_at, clock_timestamp()),
    last_heartbeat_at = COALESCE(last_heartbeat_at, clock_timestamp()),
    -- THE DIRECT CLAIM IS A REAL CLAIM SEQUENCE (the fleet's own rule):
    -- the epoch increments atomically with the attempt, and the returned
    -- pair fences this claim's ledger row and every context/finalize
    -- write. A parked-and-woken direct node resumes at a NEW epoch and
    -- mints its own ledger row — the previous 'awaited' row stays
    -- untouched, and no shared zero distinguishes (or collides) claims.
    attempt = attempt + 1, claim_epoch = claim_epoch + 1
WHERE id = $1
  AND (
    status = 'pending'
    OR (status = 'scheduled' AND scheduled_at <= now())
  )
  -- A parked source has no body and no claim of its own: the marker, never
  -- a timestamp, is the discriminator, and a row still carrying the live
  -- EmitBackpressure marker is the promotion's to move (the promotion CAS
  -- removes the marker atomically with the reservation it installs).
  AND NOT ({wf_emit_park_core})
  AND deps_pending = 0
  -- THE HOLD-STAMP LEG (the fence's last word — the two-driver race's
  -- wedged-hold fix, the same leg the fleet's dispatch fence carries,
  -- _WF_DISPATCH_FENCE_TEMPLATE): the claimable SELECT's snapshot can
  -- PREDATE the hold's mint (the other driver's body minted the hold
  -- between this driver's SELECT and this WRITE) — a stamp that rides
  -- the mint's OWN transaction (the signal row + the held representation
  -- are ONE tx) means the row is HELD: re-claiming it strands the
  -- re-claimer's body parked on a delivery this driver can never see
  -- (the parked-forever review row the two-drivers pin rejected).
  AND NOT metadata ? 'hold'
  -- THE TERMINAL-FLOW FENCE (the fleet's dispatch fence's parity —
  -- the terminal-flow fence): a node row on a TERMINAL flow is not work — the
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
  -- The ROOT-EXISTENCE leg is the fleet fence's own, fail-closed: the
  -- run's ``__flow__`` root must be in ``jobs``. The negative terminal
  -- form this replaces admitted a row whose root had been archived or
  -- deleted (the subquery found nothing, so the negation held) and the
  -- tick claimed and executed a body whose workflow stamp no longer
  -- existed anywhere. The exemptions ride INSIDE the EXISTS, so an
  -- escalation / after-terminal row stays claimable on a terminal root
  -- that is present and is refused with every other row once it is
  -- gone. The predicate is one constant shared with the enumeration
  -- (``_ROOT_EXISTENCE_FENCE_CORE``); every candidate-row column is
  -- qualified to the outer row.
  __WF_ROOT_FENCE__
RETURNING attempt, claim_epoch
""".replace("__WF_ROOT_FENCE__", _CLAIM_ROOT_FENCE)
)

# The ordinary re-pend statement lives with its claim/ledger transaction
# in ``api._attempt``; no weaker claimant-only statement remains here.

# the ladder's re-pend shape (NODE_REPEND_SQL_TEMPLATE) with the cancel fence
# in-statement and the budget gate — the vanilla ``mark_retry``'s own arms
# mirrored (the same shape NODE_DEFER_SQL's deferral arms keep): * 'repended' —
# the clean face: the row 'scheduled' at the backoff (the burn's retry), the
# cancel columns untouched; * 'exhausted' — the budget's bound (the retry kind
# is ``permanent``, or the ladder's failed count reached ``max_attempts`` — the
# caller prices both into $3): the arm writes NOTHING (the wf terminal door
# owns every death — the row must still be 'running' for the terminal mark's
# fence), the verdict names the caller's route; * 'cancel_fenced' — the cancel
# wins (the vanilla mark_retry's own law: a phase-carrying row — the wf
# cancel's metadata mark or the vanilla cancel's own column mark — matches no
# arm; the deadline's backoff must never re-queue a row a cancel in flight
# owns): the caller routes the node through the wf terminal door, the
# deferral's cancel_faced shape; * None — the zombie (the claim fence matched
# nothing, a newer attempt owns the row): the caller records the ledger's
# 'fenced', the claim identity's fence (the fence legs bind the caller's own
# claim — the NODE_DEFER_SQL's own shape): the deadline's disposition carries
# the claim's own identity legs — worker + attempt + claim_epoch, the
# terminal-mark CAS's full vocabulary. The pre-fix WHERE (id + status + the
# cancel marks) let a zombie W1's deadline disposition flip a re-minted live W2
# row (the 90s wf lease lapses → the reclaim + re-claim as W2 → W1's timer
# fires → W2's row to 'scheduled', its ledger phantom-runs, its finalize
# refuses): the row the fence matches must be the row the caller's own claim
# holds, or every arm refuses and the no-match routes to the zombie face (the
# ledger's 'fenced' record, keyed on the claim's own instant — the
# claim-instant shape). Keyed to the one claimed node row; the two read arms
# write nothing.
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
      -- the phase-carry shape): a phase-carrying row on EITHER carrier
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
-- existence — the NODE_DEFER_SQL arm order: the cancel mark
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

# The detail re-read's PARENT ROWS (the bounded collect's ``detail_home``):
# ``metadata`` rides every level so the recursive re-read knows each parent's
# own node kind from the durable ``wf_node_kind`` stamp
# (``_runner_codec.WF_NODE_KIND_KEY``) rather than from a workflow registry a
# runner-less reader does not have. ``result_expired`` is the definite,
# two-valued "this row holds no durable value": a NULL result never is a
# collected ``None`` (a body returning ``None`` is stored as the empty
# document, never SQL NULL), so the flag is plain ``p.result IS NULL``. The
# TTL comparison is deliberately NOT part of the predicate: a row re-pended by
# the operator retry carries no expiry stamp at all, and ``NULL <
# clock_timestamp()`` is SQL NULL — a three-valued flag would read as "not
# expired" and let that row's lost value through the count check as a
# fabricated item.
PARENT_RESULTS_BY_KEY_SQL_TEMPLATE = """
SELECT p.id, p.step_key, p.result, p.metadata,
       (p.result IS NULL) AS result_expired
FROM {schema}.wf_edge e
JOIN {schema}.jobs p ON p.id = e.parent_id
WHERE e.child_id = $1
  -- THE ABSORBED PARENT'S SLOT (the barrier fix — the envelope must
  -- not lie in the sum): on an ABSORBING edge (collect | maybe) only a
  -- SUCCEEDED parent contributes a result item to the packed join. A
  -- terminal-FAILED parent's absence is ON THE RECORD (the join's
  -- metadata.failures array carries the typed FailureInfo + the policy
  -- that ran), never a junk {} slot the consumer's decode cannot honor —
  -- and a parent the per-node retry re-pends AFTER the join packed stays
  -- excluded too: the pack summed the succeeded set, so re-reading the
  -- parent's LIVE status must not re-open a count the marker already
  -- recorded. A fail_closed edge's consumer never runs over a failed
  -- parent (the cascade), so this filter changes no other face — and a
  -- re-pended parent behind that edge is read (and fails closed on its
  -- NULL result) exactly as the re-pend left it.
  AND NOT (p.status <> 'succeeded' AND e.failure_policy IN ('collect', 'maybe'))
ORDER BY p.id
"""

NODE_BY_STEP_KEY_SQL_TEMPLATE = """
SELECT id FROM {schema}.jobs WHERE step_key = $1
  AND (metadata->>'flow_id')::uuid = $2
"""

#: The streaming source's checkpointed cursor read: the key is the
#: bound EMIT_CURSOR_KEY constant — never an f-string SQL (the S608 rule);
#: the row is the source's own.
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
    -- create path too — the false-remedy defect's root):
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

CLAIMABLE_NODES_SQL_TEMPLATE = _render_fence_tokens(
    """
SELECT id, step_key, map_index, attempt, trace_id, payload, start_to_close, metadata,
       required_worker_schema, max_attempts, retry_kind,
       retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND (
    status = 'pending'
    OR (
      status = 'scheduled'
      -- THE CLAIM CAS'S OWN SCHEDULED SHAPE: a scheduled row is claimable
      -- only at a real due time that has passed — the same leg the claim
      -- CAS evaluates.
      AND scheduled_at <= now()
    )
  )
  -- The exact-park exclusion mirrors the claim CAS's own: a row still
  -- carrying the live EmitBackpressure marker is the promotion's to move,
  -- never a claim's to take — the promotion removes the marker atomically
  -- with the reservation it installs.
  AND NOT ({wf_emit_park_core})
  AND deps_pending = 0
  -- THE NODE-ONLY PRE-FILTER: this statement is the run's node enumeration,
  -- so the root row is excluded here unconditionally. The root-existence
  -- fence below repeats step_key <> '__flow__'; that repetition is
  -- deliberate (the fence reads as the fleet fence does) and this line is
  -- the enumeration's own node-only contract, not a removable duplicate:
  -- without it the fence would be the only thing keeping the root out.
  AND step_key <> '__flow__'
  AND NOT metadata ? 'hold'
  -- THE TERMINAL-FLOW FENCE (the fleet's dispatch fence's parity —
  -- the terminal-flow fence): a terminal/cancelled flow's nodes are UNCLAIMABLE
  -- through THIS door too — a bare tick() (the driver's public dispatch
  -- pass, never gated by drive()'s loop-top check) on a dead flow
  -- claims NOTHING and executes NOTHING. The fleet's dispatch carries
  -- the same leg at its own claim seat (the ESCALATION-KIND exemption
  -- included: the pages-a-human duty is the ONE thing that survives a
  -- flow's terminality — the two fences may not disagree, a probe that
  -- disagrees with its claim is the worker-sleeps-the-page-never-runs
  -- shape). The exemptions ride INSIDE the root-existence EXISTS (the
  -- fail-closed leg: a run whose ``__flow__`` root is gone has no
  -- workflow stamp left to resolve, so none of its rows is work).
  -- The fence is the flow-scoped variant: the enumeration cannot see a
  -- vanilla row (its WHERE pins the flow link and the pre-filter above
  -- excludes a NULL step_key), so the claim CAS's null-safe ``IS NULL``
  -- arm would be dead text here. See ``_ROOT_EXISTENCE_FENCE_CORE``.
  __WF_ROOT_FENCE__
ORDER BY id
""".replace("__WF_ROOT_FENCE__", _ENUM_ROOT_FENCE)
)

#: the run's node-row count — a nodeless root (the orphan's signature: pending
#: + zero nodes, the pre-fix debris shape) counts zero. The same
#: lineage-and-not-the-root predicate every node read spells.
FLOW_NODE_CENSUS_SQL_TEMPLATE = """
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
"""

#: one bounded primary-key read of the root's metadata, the fleet door's own
#: SELECT shape (``_CLAIMED_NODE_READ_SQL_TEMPLATE``'s stamp leg). The arbiter
#: adopted an existing run — its root stamp is its own truth (the row is the
#: law, never the echo): the adopting dev door resolves the run's per-call
#: override from here, so a re-drive forks its rows at the same priority the
#: create stamped (never the declarations — one run, one cohort).
ROOT_PRIORITY_STAMP_SQL_TEMPLATE = """
SELECT metadata->>'wf_priority' AS wf_priority
FROM {schema}.jobs WHERE id = $1
"""

FLOW_REQUIRED_WORKER_SCHEMA_SQL_TEMPLATE = """
SELECT required_worker_schema FROM {schema}.jobs
WHERE id = $1 AND step_key = '__flow__'
"""


FLOW_STATUS_SQL_TEMPLATE = """
SELECT status FROM {schema}.jobs WHERE id = $1
"""

FLOW_PAYLOAD_SQL_TEMPLATE = """
SELECT payload FROM {schema}.jobs WHERE id = $1
"""

FLOW_CANCELLED_SQL_TEMPLATE = """
SELECT error_class, error_message FROM {schema}.jobs
WHERE id = $1 AND status = 'cancelled'
"""

# The skip-predicate's view: every succeeded row's identity + result, with
# its ``metadata`` — the detail re-read
# (``_runner_codec.decode_result_with_detail``) needs the row id and the
# durable node-kind stamp when the entry is a bounded collect's truncated
# summary. ``result_expired`` carries the row's own "no durable value" fact
# (``result IS NULL``) so a swept entry row refuses at the read instead of
# decoding as a collected ``None``. The run's own ``__flow__`` root row is
# excluded from that flag: it is the run, not a node result, and it holds no
# ``result`` by construction.
SUCCEEDED_RESULTS_SQL_TEMPLATE = """
SELECT id, step_key, result, metadata,
       (result IS NULL AND step_key <> '__flow__') AS result_expired
FROM {schema}.jobs
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
-- THE PRE-UPDATE CAPTURE (A9.4's truthful old state): the target CTE
-- locks the root row (FOR UPDATE) and captures its ACTUAL pre-update
-- status before the flip — the event's from_state is the row's own
-- prior state (a pending root records 'pending', a running root
-- 'running'), never a hardcoded value. The lock is the old-state race's
-- own control (B-1): whichever writer locks first establishes the
-- transition; the other captures the committed winner state afterwards.
-- The root UPDATE itself remains the seat's first row lock and the
-- cancel's linearization point (root-before-nodes).
WITH target AS (
    SELECT j.id, j.status::text AS from_state
    FROM {schema}.jobs AS j
    WHERE j.id = $1
      AND j.status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
    FOR UPDATE
), flipped AS (
    UPDATE {schema}.jobs SET status = 'cancelled',
        error_class = '{wf_cancelled_class}',
        error_message = $2,
        finished_at = clock_timestamp()
    FROM target
    WHERE {schema}.jobs.id = target.id
    RETURNING {schema}.jobs.id, status::text AS to_state, target.from_state
), evt AS (
    -- THE AUDIT LEG (the operator's march's battery finding): the
    -- cancel's flip IS a mutation — the events reader gets the
    -- state_change that names it (the shared tier invariant reads a
    -- terminal row's OWN event here).
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', f.from_state, 'to_state', f.to_state,
                              'error_class', '{wf_cancelled_class}')
    FROM flipped f
)
SELECT id FROM flipped
"""


#: The runner templates' render path does not collapse braces (their own
#: jsonb literals are single-braced), while `_sql_finalize`'s shared
#: fragments are written in that module's doubled-brace convention (its
#: render collapses). A fragment moved across the seam is re-braced here,
#: once, with the convention named — the runner's own literals are
#: untouched.
def _rebrace(fragment: str) -> str:
    return fragment.replace("{{", "{").replace("}}", "}")


CANCEL_NODES_SQL_TEMPLATE = """
-- THE PRE-UPDATE CAPTURE (A9.4's truthful old state + B-1's old-state
-- race control): the target CTE locks each candidate row (FOR UPDATE —
-- whichever writer locks first establishes the transition; the other
-- captures the committed winner state) and captures its ACTUAL
-- pre-update status and its validated old page-width reservation (the
-- §2.4 semantics: NULL when absent, fail-closed on a malformed key) —
-- the event's from_state and the additive released-width detail are the
-- row's own facts, never post-update reads.
WITH targets AS (
    SELECT j.id,
           j.status::text AS from_state,
           {wf_emit_validated_reservation_width}
    FROM {schema}.jobs AS j
    WHERE (j.metadata->>'flow_id')::uuid = $1
      AND status NOT IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')
      -- A materialized terminal effect owns an independent ordinary policy.
      -- Its committed intent has already been granted this one physical job;
      -- later run cancellation must not revoke that durable obligation.
      AND NOT metadata ? 'wf_after_terminal_effect'
    FOR UPDATE
), flipped AS (
    UPDATE {schema}.jobs
    SET status = CASE WHEN status = 'running' THEN status ELSE 'cancelled' END,
        finished_at = CASE WHEN status = 'running' THEN finished_at ELSE clock_timestamp() END,
        error_class = CASE WHEN status = 'running' THEN error_class ELSE '{wf_cancelled_class}' END,
        -- THE §2.4 KEY FATE: the COOPERATIVE row (still running) retains
        -- its key; a row this statement actually TERMINALIZES removes it.
        metadata = CASE WHEN status = 'running'
                        THEN metadata || '{"cancel_phase": "cooperative"}'::jsonb
                        ELSE (metadata - 'wf_emit_reservation') || '{"cancel_phase": "cooperative"}'::jsonb END
    FROM targets AS t
    WHERE {schema}.jobs.id = t.id
    RETURNING {schema}.jobs.id, (status::text) AS to_state, t.from_state,
              t.released_emit_reservation_width
), current_claim AS (
    -- THE CURRENT-CLAIM RESOLUTION (the ONE shared association core —
    -- `_sql_claim.current_claim_join`: job_id + the CURRENT claim_epoch +
    -- the exact step_key + the null-safe map_index; NO attempt-equality
    -- leg — every non-consuming release (an ordinary Snooze, a legacy
    -- non-consuming RetryAfter, a hold, a worker refund, the capacity
    -- park) refunds the job's DISPLAYED ordinal below the recorded
    -- claim's attempt while the current epoch stands, so an attempt leg
    -- would resolve zero rows and leave the current claim unfenced — and
    -- no park-metadata test: the epoch already identifies the current
    -- claim across every refund representation). The renderer owns this
    -- disposition's own nonterminal status domain AND the strict
    -- single-row maximality: the ledger's arbiter makes
    -- same-job/same-epoch/same-step/same-map rows with different attempts
    -- distinct, so the maximality probe (highest attempt, then the latest
    -- updated_at, then the highest id) is what makes ONE cancelled winner
    -- unable to associate two rows — two fences and two events for one
    -- winner is the defect the probe closes. The captured ledger id +
    -- pre-update instant are the exact fence; the captured attempt is a
    -- NON-NARROWING recorded fact (the audit's optional claim_attempt
    -- detail), never an association leg. A never-claimed pending row has
    -- no current claim and needs none (LEFT-joined at the event).
    SELECT f.id AS job_id, l.id AS ledger_id, l.updated_at AS claimed_at,
           l.attempt AS claim_attempt
    FROM flipped AS f
    JOIN {schema}.jobs AS j ON j.id = f.id
    JOIN {schema}.wf_step_ledger AS l
      ON {current_claim_join}
    WHERE f.to_state = 'cancelled'
), fenced_ledger AS (
    -- The exact writer fence: ONLY the captured current row (its own id +
    -- its pre-update instant) changes; older epochs, sibling ctx.substep
    -- rows, map siblings, and terminal history are byte-for-byte
    -- untouched.
    UPDATE {schema}.wf_step_ledger AS l
    SET status = 'fenced', error_class = '{wf_cancelled_class}',
        updated_at = clock_timestamp()
    FROM current_claim AS c
    WHERE l.id = c.ledger_id
      AND l.updated_at = c.claimed_at
    RETURNING l.id
), evt AS (
    -- THE AUDIT LEG: only the rows THIS statement actually
    -- terminalised (a running row's cooperative phase-1 keeps it
    -- running — its own terminal write carries its event). The winner
    -- set owns the event cardinality INDEPENDENTLY of any ledger match
    -- (a never-claimed pending winner still records its event — the
    -- optional LEFT association adds the non-narrowing claim_attempt
    -- detail when a current claim exists and omits it through
    -- jsonb_strip_nulls when none does). Detail is the union of the
    -- captured old state and the additive F10/B-2 fields.
    INSERT INTO {schema}.job_events (job_id, occurred_at, kind, detail)
    SELECT f.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object(
               'from_state', f.from_state,
               'to_state', f.to_state,
               'error_class', '{wf_cancelled_class}',
               'claim_attempt', c.claim_attempt,
               'released_emit_reservation_width', f.released_emit_reservation_width
           ))
    FROM flipped AS f
    LEFT JOIN current_claim AS c ON c.job_id = f.id
    WHERE f.to_state = 'cancelled'
)
SELECT count(*) FROM flipped
""".replace(
    "{current_claim_join}",
    current_claim_join("j", "l", ("running", "awaited", "crashed")),
).replace(
    "{wf_emit_validated_reservation_width}",
    _rebrace(validated_reservation_width("j")) + " AS released_emit_reservation_width",
)

#: The terminal read: the run's status + the terminal node's row. The
#: node's own ``result_expired`` is the row's definite "no durable value"
#: fact (``result IS NULL``), so a swept terminal join row refuses at the
#: read instead of decoding as ``None`` — which is this reader's answer for
#: "not terminal yet". ``n.id IS NOT NULL`` keeps the LEFT JOIN's miss a
#: plain "no terminal row", never a stale expiry flag.
TERMINAL_RESULT_SQL_TEMPLATE = """
SELECT r.status AS run_status,
       n.id AS node_id, n.step_key AS node_key, n.result AS node_result,
       n.metadata AS node_metadata, n.status AS node_status,
       (n.id IS NOT NULL AND n.result IS NULL) AS result_expired
FROM {schema}.jobs r
LEFT JOIN {schema}.jobs n
  ON (n.metadata->>'flow_id')::uuid = r.id AND n.step_key = $1
WHERE r.id = $2 AND r.step_key = '__flow__'
"""

#: The failure face: the run's newest
#: terminal-failed node row (the root's own row carries the propagation
#: verdict — UnabsorbedNodeFailure — but the node row carries the body's
#: error class + message: the failed run's first question's answer).
#: The verdict names the failure that failed the run: the absorbed
#: failures are excluded — the same policy-vs-fence predicate the
#: derivation and the maintenance leg read (the three can never drift):
#: a branch whose failure its join's collect policy absorbed (the join
#: fired with the typed report) did not fail the run, and a newest-first
#: selection that read it sent the operator to a failure that was
#: already collected. When every node failure was absorbed the run did
#: not fail from a node — the no-row face speaks (the root carries the
#: verdict).
FLOW_FAILURE_SQL_TEMPLATE = (
    """
SELECT j.step_key, j.error_class, j.error_message FROM {schema}.jobs j
WHERE (j.metadata->>'flow_id')::uuid = $1
  AND j.status = 'failed'
  AND j.step_key <> '__flow__'
  AND NOT """
    + absorbed_exists("j")
    + """
ORDER BY j.finished_at DESC NULLS LAST
LIMIT 1
"""
)

#: the compiled descendants the exit resolved — skipped with the record (the
#: envelope never lies about the nodes that didn't get to run), zero ledger
#: rows. Terminal rows and unspawned rows are untouched.
EXIT_SKIP_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'succeeded',
    result = $2::jsonb,
    finished_at = clock_timestamp(),
    -- THE TERMINAL REMOVES THE RESERVATION KEY (§2.4): the early Exit's
    -- skip-terminal removes any held page-width reservation in the same
    -- winning statement — no key survives a terminal row for re-arm.
    metadata = metadata - 'wf_emit_reservation'
WHERE (metadata->>'flow_id')::uuid = $1
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
  AND step_key = ANY($3)
"""

#: terminal-failed → pending — one statement, the only grant; the attempt
#: ordinal untouched (continues — the ladder's own count is the budget). the
#: root's guard (H-C1): ``and step_key <> '__flow__'`` — the run's own root row
#: is not a node, and a "retry" that matched it flipped the root failed→pending
#: with the propagation verdict erased (the maintenance rollup never saw the
#: run again — the run bricked, live- proven). The verb-level guard is the one
#: seat: the admin route, the CLI's ``flows retry``, and the compiled face all
#: ride this statement, and the root key's re-run is the run-rerun's own
#: concern (a fresh run, or the run-rerun's census — never the per-node CAS).
RETRY_NODE_CAS_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'pending',
    error_class = NULL,
    error_message = NULL,
    scheduled_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    -- THE MANUAL RE-ARM'S DEFENSIVE STRIP (§2.4, N4): a manually
    -- re-armed terminal row carries no old episode's keys into its new
    -- one — the reservation key (a future promotion re-grants it; a
    -- stale key would charge the derived capacity and force the
    -- re-armed row's own honest next page through the deterministic
    -- grant-violation route), the park marker family, and any
    -- terminal-fence stamp.
    metadata = metadata - 'wf_emit_reservation'
        - 'blocking_reason' - 'wf_deferral' - 'wf_emit_backpressure'
        - 'wf_not_draining'
WHERE step_key = $2
  AND (metadata->>'flow_id')::uuid = $1
  AND status = 'failed'
  AND step_key <> '__flow__'
RETURNING id
"""

#: the cascade's blocked rows return to join-wait — the sweep's re-derive
#: re-derives them from the edge ledger (a stamp is the cache, never the
#: truth).
RETRY_REOPEN_CLOSURE_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET metadata = jsonb_set(metadata, '{blocking_reason}', '"join"'::jsonb, true)
WHERE (metadata->>'flow_id')::uuid = $1
  AND step_key = ANY($2)
  AND metadata @> '{"blocking_reason": "failed_parent"}'::jsonb
  AND status NOT IN ('succeeded','failed','cancelled','crashed','abandoned')
"""

#: a terminal-failed root returns to running (the manual resume's own
#: linearization; a cancelled root stays closed).
RETRY_FLOW_REOPEN_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'running', finished_at = NULL,
    run_epoch = COALESCE(run_epoch, 0) + 1
WHERE id = $1
  AND status = 'failed'
"""

#: the run's exit-skipped rows (the typed early-exit's downstream mark) return
#: to pending with the envelope cleared. A skip is not an attempt (zero ledger
#: rows) — the re-arm restores exactly the pre-skip shape: a claimable row
#: whose next claim is its first attempt. The root's own row is out of the
#: write set (the root guard's law: the root is not a node). the predicate is
#: the engine's own contract: ``result @> '{"skipped": true}'::jsonb`` — the
#: shape the exit writer (``SKIPPED_RESULT``/the EXIT_SKIP mark) actually
#: lands, true with it. The old ``result ? 'skipped'`` was key-existence: a
#: foreign writer's ``{"skipped": false}`` (or any JSON that merely carries the
#: key) matched and had its result deleted — the display's false-skip lie, and
#: a foreign writer's data destroyed. Containment matches the true envelope
#: only: a false/"no" envelope survives untouched.
RETRY_SKIP_REARM_SQL_TEMPLATE = """
UPDATE {schema}.jobs
SET status = 'pending',
    result = NULL,
    finished_at = NULL,
    scheduled_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    -- THE MANUAL RE-ARM'S DEFENSIVE STRIP (§2.4, N4): the same keys the
    -- node-retry re-arm strips — a re-armed row is an ordinary live row.
    metadata = metadata - 'wf_emit_reservation'
        - 'blocking_reason' - 'wf_deferral' - 'wf_emit_backpressure'
        - 'wf_not_draining'
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
#: plain strings — ``render_sql`` never ``.format``\s them (the jsonb
#: shape literals keep their braces, the ``{schema}`` token survives for
#: the call-site render), and each substitution is a plain ``str.replace``
#: of an exact token. The wf cancel's own error class is interpolated from
#: its one home (``api/_runner_defer``), never spelled per template.
_SQL_TOKEN_SUBS: Final[tuple[tuple[str, str], ...]] = (
    ("{wf_cancelled_class}", WORKFLOW_CANCELLED_CLASS),
    # The exact-park exclusion's shared MARKER CORE (the neutral renderer's
    # projection: complete minus its status leg — the only correct
    # exclusion for a WHERE that admits `pending` rows). Unqualified,
    # single-brace canonical text; the runner's render is a plain replace
    # and never collapses braces, so no re-brace is owed.
    ("{wf_emit_park_core}", emit_park_marker_core("")),
)


def render_sql(template: str, schema: str) -> str:
    """One runner statement's render (the shared render's substitution:
    the validated schema, then the token table above — the schema
    identifier is checked before anything renders). The runner templates'
    jsonb literals are single-braced (a doubled `}}` sequence inside a
    nested jsonb literal is REAL SQL text here, never an escape — the
    un-doubling pass would truncate it)."""
    return shared_render(template, schema, subs=_SQL_TOKEN_SUBS)
