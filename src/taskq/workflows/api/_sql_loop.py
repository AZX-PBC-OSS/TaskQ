"""The loop's statement constants (T19) — the budget columns' reads and
writes, the atomic carry-advance + cap guard, and the named-exhaustion
terminalization. The schema identifier is the ONLY interpolated value
(validated at :meth:`WorkflowSql.build`'s callers); every
caller-controlled value uses ``$N`` parameter binding. See ``_sql.py``
for the bundle law and the JSONB-landmine rule.

THE CLOCK DOCTRINE: every wall comparison is against PG's
``clock_timestamp()`` — the app clock is skewable (+1h under the skew
fixture), PG's clock is the truth (the BUDGET-DB-CLOCK pin's subject).
"""

from __future__ import annotations

from typing import Final

from taskq.constants import require_schema

__all__ = ["render_loop_sql"]


def render_loop_sql(template: str, schema: str) -> str:
    """Render one loop statement for *schema* — the SAME substitution
    the bundle's render does (the doubled braces are the
    migration-runner convention; substitution needs them single; the
    schema identifier is validated before anything renders; the
    ``{terminal}`` token is the terminal-status SQL set — the bundle's
    own constant). The loop statements live beside the bundle
    (``_sql_loop.py``) for the same no-god-file law; the RENDER is the
    bundle's."""
    from taskq.workflows._sql import TERMINAL_SQL_SET

    require_schema(schema)
    return (
        template.replace("{schema}", schema)
        .replace("{terminal}", TERMINAL_SQL_SET)
        .replace("{{", "{")
        .replace("}}", "}")
    )


#: The loop node's INIT (the claim-time write): the budget deadline is
#: computed FROM PG'S CLOCK (`now() + $2 seconds`) — never the app's.
#: The metadata carries the loop's kind marker (the sweep arm's target),
#: the iteration counter (the cap wall's truth) and the carry.
LOOP_INIT_SQL = """\
UPDATE {schema}.jobs
SET budget_deadline = CASE
        -- THE NO-WALL SHAPE (the deploy matrix's drain-cure): a loop
        -- that declares NO budget is NOT a loop with an already-expired
        -- budget. ``$2`` NULL → the deadline NULL (the sweep's arm
        -- reads a NULL deadline as NO wall); the old init coerced the
        -- absence to ``now() + 0`` — a deadline in the PAST the instant
        -- it landed, so the budget sweep's arm exhausted every
        -- budget-less loop that ever lost its holder (the drain cell's
        -- requeue was eaten by a wall the app never declared).
        WHEN $2::double precision IS NULL THEN NULL
        ELSE now() + ($2::double precision * interval '1 second')
    END,
    metadata = metadata || $3::jsonb
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

#: THE ADVANCE STATEMENT — the carry advanced EXACTLY ONCE per
#: iteration, ATOMIC with THE CAP GUARD: advancing TO iteration i+1
#: admits iteration i+1's spawn, so the guard refuses the advance that
#: would START spawn #max_iterations+2 (the cap bounds TOTAL SPAWNS —
#: the spike's cut 4): the guard lets the advance reach EXACTLY the cap
#: (``i+1 <= max``), so the FINAL iteration runs and the metadata's
#: counter reaches ``max`` — the state the crash window (a worker death
#: after the final advance, before the driver's top-of-loop cap check)
#: leaves on a RUNNING row, which is what makes the SWEEP's
#: ``iteration >= max`` predicate REACHABLE in production (the vacuous
#: arm's cure: the shipped ``i+1 < max`` guard topped the counter at
#: ``max-1`` FOREVER — the sweep's trigger state was unconstructible).
#: The driver's top-of-loop cap check exhausts the live path; a refused
#: advance (a concurrent terminal) stays the backstop exhaustion.
#: This one-statement atomicity is the CARRY-OPTIMISTIC dragon's cure: a
#: carry advanced at hold/retry time (outside this statement) is the
#: double-apply/lost-apply variant, kept RED forever.
#:
#: THE CLAIM IDENTITY'S FENCE (the one-tx-finalize doctrine's
#: terminal-write law, on the advance — the unguarded statement was the
#: doctrine's BACK DOOR): worker + attempt + claim_epoch, the SAME legs
#: the terminal-mark CAS carries. A zombie driver (its claim lapsed, the
#: loop reclaimed at a fresh attempt + epoch) is REFUSED — its stale
#: payload can never move the iteration counter BACKWARD, and the
#: reclaimed loop's own driver advances exactly once. The
#: NULL-tolerant form on the worker leg (``IS NOT DISTINCT FROM``): a
#: reclaimed/never-claimed row (``locked_by_worker IS NULL``) is
#: refuse-all for a live identity, and the sweep's under-lock identity
#: read matches it exactly; attempt/claim_epoch are NOT NULL columns, so
#: the form is plain equality for them.
LOOP_ADVANCE_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
  AND status = 'running'
  AND (metadata->>'iteration')::int + 1 <= $3::int
  AND locked_by_worker IS NOT DISTINCT FROM $4::uuid
  AND attempt IS NOT DISTINCT FROM $5::int
  AND claim_epoch IS NOT DISTINCT FROM $6::bigint
RETURNING (metadata->>'iteration')::int AS iteration
"""

#: THE NAMED EXHAUSTION + THE FLOW TERMINALIZES IN THE SAME TRANSACTION
#: (STRANDED-FLOW): the loop node gets the ``iteration_cap_exhausted``
#: state (the metadata's ``iteration_state`` + the typed failure class on
#: the row) and the flow root FLIPS TERMINAL in the same statement — a
#: wedged ``running`` flow that ticks forever (the spike's cut 5) is the
#: convicted variant, kept RED forever. Idempotent: a terminal loop row
#: (or a terminal flow) updates nothing.
#:
#: THE CLAIM IDENTITY'S FENCE (the same legs the advance and the
#: terminal-mark carry — the one-tx-finalize doctrine's BACK DOOR
#: closed): the exhaust lands only on the row the caller's own claim
#: identity holds. A zombie driver's exhaust (its claim lapsed, the loop
#: reclaimed and healthy under a new driver) is REFUSED — it can no
#: longer kill the reclaimed loop, write its escalation row, or fire the
#: named state from a stale view. The sweep's arm binds the identity it
#: read under its own row lock (``FOR UPDATE`` — stable to statement
#: end); the worker leg's NULL-tolerant form matches a
#: never-claimed/reclaimed row exactly (see LOOP_ADVANCE_SQL's note).
LOOP_EXHAUST_SQL = """\
WITH loop_row AS (
    UPDATE {schema}.jobs
    SET status = 'failed',
        error_class = $2,
        error_message = $4,
        finished_at = clock_timestamp(),
        metadata = metadata || $3::jsonb
    WHERE id = $1
      AND status = 'running'
      AND locked_by_worker IS NOT DISTINCT FROM $5::uuid
      AND attempt IS NOT DISTINCT FROM $6::int
      AND claim_epoch IS NOT DISTINCT FROM $7::bigint
    RETURNING id, (metadata->>'flow_id')::uuid AS flow_id
),
flow_terminal AS (
    UPDATE {schema}.jobs f
    SET status = 'failed',
        error_class = $2,
        finished_at = clock_timestamp()
    WHERE (SELECT flow_id FROM loop_row) IS NOT NULL
      AND f.id = (SELECT flow_id FROM loop_row)
      AND f.status NOT IN {terminal}
    RETURNING f.id
)
SELECT (SELECT count(*) FROM loop_row) AS loop_exhausted,
       (SELECT count(*) FROM flow_terminal) AS flow_terminalized
"""

#: THE ESCALATION OUTBOX ROW (the ``on_exhausted="escalate"`` arm): the
#: named state is the record; the escalation actor's enqueue rides the
#: SAME outbox the fired joins use (no second delivery mechanism).
LOOP_ESCALATION_OUTBOX_SQL = """\
INSERT INTO {schema}.wf_outbox
    (id, join_job_id, flow_id, consumer_step_key, bindings)
VALUES ($1, $2, $3, $4, $5::jsonb)
"""

#: THE BUDGET SWEEP ARM (the sweep's exclusivity law + P3 rule 1's
#: held-row inertness): the arm's HEART is ``AND NOT budget_paused`` —
#: a loop holding on a human is INVISIBLE to the wall even when its
#: deadline is forced into the past (the CONSUME-BUDGET dragon's cure;
#: the arm missing the leg is the mutation the pin drills). The cap wall
#: rides the same arm (TWO walls, one sweep, different predicates): a
#: running loop at/over its iteration cap is exhausted by the SWEEP —
#: an ``if`` in the body would red the crash pin (the worker dies, the
#: body never runs, the cap never fires). The clock comparison is PG's
#: ``clock_timestamp()`` (the DB-clock doctrine). The CLAIM IDENTITY
#: columns ride the select: the sweep's exhaust binds the identity it
#: read under this lock (the fence's under-lock read — the exhaust
#: statement's legs).
LOOP_BUDGET_SWEEP_SQL = """\
WITH loops AS (
    SELECT j.id, (j.metadata->>'flow_id')::uuid AS flow_id, j.step_key,
           j.locked_by_worker, j.attempt, j.claim_epoch
    FROM {schema}.jobs j
    WHERE j.metadata @> '{{"kind": "loop"}}'::jsonb
      AND j.status = 'running'
      AND NOT j.budget_paused
      AND (
        (j.budget_deadline IS NOT NULL AND j.budget_deadline <= clock_timestamp())
        OR (
            (j.lock_expires_at IS NULL OR j.lock_expires_at < clock_timestamp())
            AND j.metadata ? 'max_iterations'
            AND COALESCE((j.metadata->>'iteration')::int, 0)
                >= (j.metadata->>'max_iterations')::int
        )
      )
    ORDER BY j.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
SELECT l.id, l.flow_id, l.step_key, l.locked_by_worker, l.attempt, l.claim_epoch
FROM loops l
"""

#: THE ON-WAKE REMAINING (holds are FREE): computed FROM PG'S CLOCK on
#: every resume — the deadline is not burning while the loop waits on a
#: human; the remaining is a READ of the wall, never a carried snapshot.
LOOP_REMAINING_SQL = """\
UPDATE {schema}.jobs
SET budget_remaining_ms = GREATEST(
        0,
        (EXTRACT(epoch FROM (budget_deadline - clock_timestamp())) * 1000)::bigint
    )
WHERE id = $1
  AND budget_deadline IS NOT NULL
RETURNING budget_remaining_ms
"""

#: The loop node's WALL FACE (the sweep's which-wall read): the iteration
#: counter, the cap, the budget columns.
LOOP_NODE_WALL_SQL = """\
SELECT (metadata->>'iteration')::int AS iteration,
       (metadata->>'max_iterations')::int AS max_iterations,
       budget_deadline,
       budget_paused
FROM {schema}.jobs
WHERE id = $1
"""

#: The loop node's row read (the driver's iteration counter + carry):
#: jsonb decoded ONCE by the caller (the estate's `_json` seam).
LOOP_NODE_STATE_SQL = """\
SELECT status, metadata, budget_deadline, budget_paused
FROM {schema}.jobs
WHERE id = $1
"""

#: THE POLICY'S SOURCE (attack-3 H1's cure): the flow root's stamped
#: workflow name — the sweep's arm resolves the loop's REGISTERED
#: DEFINITION from it (D1: the policy is read from the definition, never
#: from the row's metadata cache).
LOOP_WORKFLOW_NAME_SQL = """\
SELECT metadata->>'workflow' FROM {schema}.jobs WHERE id = $1
"""

#: The iteration's carried state update is the ADVANCE statement's job —
#: this read is the memoized iteration timeline (the §13.3 trace shape:
#: the iteration index + the Done/Refine kind per iteration).
LOOP_TIMELINE_SQL = """\
SELECT step_key, map_index, attempt, status, result
FROM {schema}.wf_step_ledger
WHERE flow_id = $1
  AND step_key LIKE $2 || '.iter%'
ORDER BY id
"""

#: The index-driven plan's assert target (the ≤ 5 ms-class band — T08's
#: gate class): the arm's plan must walk the budget predicate from an
#: index, never a seq scan of the jobs table (the EXPLAIN pin's subject).
LOOP_BUDGET_EXPLAIN_SQL = """\
EXPLAIN (FORMAT JSON)
SELECT j.id FROM {schema}.jobs j
WHERE j.metadata @> '{{"kind": "loop"}}'::jsonb
  AND j.status = 'running'
  AND NOT j.budget_paused
  AND j.budget_deadline IS NOT NULL
  AND j.budget_deadline <= clock_timestamp()
"""

#: The named states' vocabulary (the loop's error + events face — a
#: defined ledger record, never a silent stop).
ITERATION_STATE_CAP_EXHAUSTED: Final[str] = "iteration_cap_exhausted"
ITERATION_STATE_BUDGET_EXHAUSTED: Final[str] = "budget_exhausted"

#: The loop node's metadata kind marker (the sweep arm's target).
LOOP_KIND_MARKER: Final[str] = "loop"

#: The loop's typed failure classes (the errors the pins name).
LOOP_ERROR_CAP: Final[str] = "IterationLimitExhausted"
LOOP_ERROR_BUDGET: Final[str] = "LoopBudgetExhausted"
LOOP_ERROR_BODY: Final[str] = "LoopBodyFailure"
#: THE SHAPE ERROR (the ledger's truth law): a body returning neither
#: Done nor Refine is the TYPED shape error — named at the iteration's
#: ledger terminal AND in the loop node's diagnosis; never a SUCCEEDED
#: row the memo replay would re-thread as a Refine.
LOOP_ERROR_SHAPE: Final[str] = "LoopBodyShapeError"
