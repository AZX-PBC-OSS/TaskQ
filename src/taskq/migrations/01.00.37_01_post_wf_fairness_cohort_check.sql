-- THE WF FAIRNESS COHORT'S STRUCTURAL BACKSTOP. Forward-only; there is
-- no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- PHASE: POST. Apply only after every worker in the fleet runs the
-- release that stamps the cohort — once this migration is applied, an
-- OLD worker cannot write workflow WORK: the flow ROOT row is exempt
-- (its insert passes — the root's own leg), but the STATIC NODE
-- INSERTS are refused, so the old worker's create_flow still FAILS,
-- and its forks and emits refuse the same way: the mixed old/new
-- worker wave is viable in the pre-phase window ONLY. The pre phase carries NO migration for
-- this feature: the stamp rides the EXISTING jobs.fairness_key column
-- (migrations/01.00.00_01:71 — nullable, no default), so the new code
-- rolls in against the old schema untouched.
--
-- THE TWO STATEMENTS:
--
-- 1. THE BACKFILL — THE FULL-HISTORY TOP-UP. The pre-stamp code wrote
--    every workflow row with the NULL fairness_key (nothing re-stamps
--    the column afterwards — write-once), so the whole pre-existing
--    workflow history is NULL-cohorted. The backfill rewrites EVERY
--    workflow row's cohort — TERMINAL ROWS INCLUDED: the stamp is the
--    row's own flow link, one statement over the flow-nodes expression
--    index (01.00.26_02), the write-once column taking its value once.
--    The dispatch never reads a terminal row's cohort, so the cost is
--    storage-only for the history; the live rows are what the
--    round-robin partition reads.
--
-- 2. THE CHECK ... NOT VALID — THE GUARD'S REAL OBLIGATION, ONE FACE:
--    a row carrying a WORKFLOW LINK must carry the link AS ITS STAMP.
--    The three shapes:
--
--    * a LINKED row (metadata ? 'flow_id'): fairness_key IS NOT DISTINCT
--      FROM metadata->>'flow_id' AND the link is not JSON-null — the
--      stamp IS the link; a NULL stamp or a wrong one REFUSES (the
--      regression's shape). A JSON-NULL link refuses with ANY stamp —
--      the key's existence is the discriminator, and a null link
--      cannot name a cohort (so such a row is the refused shape with a
--      stamp, without one, or with a user key alike).
--    * NO link (the metadata key absent): PASSES, inert — a NULL flow
--      link with a NULL stamp is the common vanilla shape, AND A NULL
--      FLOW LINK WITH A NON-NULL STAMP IS THE COLUMN'S OWN FEATURE (the
--      vanilla enqueue's USER-DERIVED fairness key — the producer's
--      cohort vocabulary, the dispatch CTE's partition identity). The
--      guard is deliberately ONE-DIRECTIONAL here: the workflow surface
--      stamps its rows' cohorts from their own flow links; the vanilla
--      surface's user keys are nobody's constraint. (The looser form is
--      the choice, stated: refusing a stamp-without-link would refuse
--      the column's own feature.)
--    The flow ROOT (step_key = '__flow__') is exempt outright: it is
--    never work (the dispatch fence's root-marker leg excludes it) and
--    stays unstamped. The root's leg uses IS NOT DISTINCT FROM — a
--    plain `step_key = '__flow__'` evaluates NULL on a NULL step_key
--    and a NULL poisons the OR (a refused shape would pass as
--    "unknown").
--
--    NOT VALID skips the initial scan (the backfill above already made
--    the estate consistent) and takes only a brief ACCESS EXCLUSIVE —
--    the same two-phase discipline as 01.00.08_01_pre_denial_counters
--    and 01.00.12_03_pre_reclaim_retry_policy. The constraint is
--    enforced on every subsequent INSERT and UPDATE; VALIDATE it
--    explicitly (VALIDATE CONSTRAINT ..., SHARE UPDATE EXCLUSIVE, does
--    not block reads or writes) once the estate is at ease.
--
-- THE STORAGE FACE (the operator's arithmetic): the stamp is a 36-char
-- uuid text on every workflow row (~40 B/row on the row itself, live
-- until archived/pruned) and, while the row is PENDING, the same value
-- in the probe indexes' key columns — TWO entries per pending wf row:
-- jobs_round_robin_probe_idx (the common pending probe, partial on
-- pending) carries EVERY pending row's stamp, and the population probe
-- adds the SECOND — jobs_unrouted_round_robin_probe_idx for the
-- producer-placed rows, jobs_assignment_routed_probe_idx for the
-- re-pended rows (the entries leave when the row leaves pending).
-- Vanilla rows carry NULL unless their producer declares a user cohort:
-- no forced storage, no index change from the workflow face.

-- 1. the backfill: the full workflow history takes its run's cohort —
--    terminal rows included (the write-once column's one-time top-up).
--    The root's exclusion is IS DISTINCT FROM, never <>: a NULL
--    step_key on a linked row would make <> evaluate NULL, the row
--    would stay unstamped-and-linked, and that shape is exactly what a
--    later VALIDATE CONSTRAINT refuses.
UPDATE "{schema}".jobs
SET fairness_key = metadata->>'flow_id'
WHERE fairness_key IS NULL
  AND metadata ? 'flow_id'
  AND step_key IS DISTINCT FROM '__flow__';

-- 2. the structural backstop: a linked row's stamp IS its flow link; no
--    link is the inert vanilla shape (the user-derived cohort feature
--    rides there — see the guard's own note above).
ALTER TABLE "{schema}".jobs ADD CONSTRAINT jobs_wf_fairness_cohort_check
    CHECK (
        step_key IS NOT DISTINCT FROM '__flow__'
        OR NOT (metadata ? 'flow_id')
        OR (
            metadata->>'flow_id' IS NOT NULL
            AND fairness_key IS NOT DISTINCT FROM metadata->>'flow_id'
        )
    ) NOT VALID;
