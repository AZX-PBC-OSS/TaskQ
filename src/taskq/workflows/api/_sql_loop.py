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
SET budget_deadline = now() + ($2::double precision * interval '1 second'),
    metadata = metadata || $3::jsonb
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

#: THE ADVANCE STATEMENT — the carry advanced EXACTLY ONCE per
#: iteration, ATOMIC with THE CAP GUARD: advancing TO iteration i+1
#: admits iteration i+1's spawn, so the guard refuses the advance that
#: would start spawn #max_iterations+1 (the cap bounds TOTAL SPAWNS —
#: the spike's cut 4). A refused advance IS the cap exhaustion (the
#: caller terminalizes with the named state — never a silent stop).
#: This one-statement atomicity is the CARRY-OPTIMISTIC dragon's cure: a
#: carry advanced at hold/retry time (outside this statement) is the
#: double-apply/lost-apply variant, kept RED forever.
LOOP_ADVANCE_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata || $2::jsonb
WHERE id = $1
  AND status = 'running'
  AND (metadata->>'iteration')::int + 1 < $3::int
RETURNING (metadata->>'iteration')::int AS iteration
"""

#: THE NAMED EXHAUSTION + THE FLOW TERMINALIZES IN THE SAME TRANSACTION
#: (STRANDED-FLOW): the loop node gets the ``iteration_cap_exhausted``
#: state (the metadata's ``iteration_state`` + the typed failure class on
#: the row) and the flow root FLIPS TERMINAL in the same statement — a
#: wedged ``running`` flow that ticks forever (the spike's cut 5) is the
#: convicted variant, kept RED forever. Idempotent: a terminal loop row
#: (or a terminal flow) updates nothing.
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
#: ``clock_timestamp()`` (the DB-clock doctrine).
LOOP_BUDGET_SWEEP_SQL = """\
WITH loops AS (
    SELECT j.id, (j.metadata->>'flow_id')::uuid AS flow_id
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
SELECT l.id, l.flow_id
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
