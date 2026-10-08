-- The loop node's budget columns — T19 (the loop machinery, promoted to
-- v1 by the maintainer's ruling: "you do not cut must haves").
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE METADATA-ONLY COLUMN ADDITIONS (ACCESS
-- EXCLUSIVE, held for milliseconds on the catalog — no table rewrite:
-- nullable columns / a CONSTANT default are filled lazily, Postgres 11+
-- skips the rewrite). No index builds, no CREATE TABLEs mixed in. ALL
-- WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1 (the
-- forever rule).
--
-- THE THREE COLUMNS (the loop node's budget state — the loop node IS a
-- `jobs` row, the same one-row truth as every node):
--
-- * budget_deadline timestamptz NULL — the wall the BUDGET SWEEP reads
--   (a loop without a budget leaves it NULL: the arm never fires). The
--   DB clock owns it: `now() + remaining` computed FROM PG (the
--   DB-clock doctrine — the app clock is skewable, PG's is the truth;
--   the skew fixture pins it).
-- * budget_paused boolean NOT NULL DEFAULT false — THE HELD-ROW
--   EXCLUSIVITY's budget face: a loop holding on a human is PAUSED and
--   INVISIBLE to the budget sweep — even when its deadline is forced
--   into the past. The arm carries `AND NOT budget_paused` (the
--   CONSUME-BUDGET dragon's cure: the consume variant killed a held
--   loop mid-hold and the operator's later approval was refused — work
--   silently lost, never again).
-- * budget_remaining_ms bigint NULL — the on-wake remaining (computed
--   from PG's clock on every resume — holds are FREE: the deadline is
--   not burning while the loop waits on a human).
--
-- NO NEW TABLE: the iteration records are the step ledger's rows under
-- the ITERATION-SCOPED step keys `(workflow, loop_key, iteration,
-- step)` — TEXT business keys (the T05 contract unchanged; iteration
-- records' ids are uuid7 via the seam, so `ORDER BY id` IS the
-- iteration timeline). The iteration timeline needs no second store.

ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS budget_deadline timestamptz NULL;

ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS budget_paused boolean NOT NULL DEFAULT false;

ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS budget_remaining_ms bigint NULL;

COMMENT ON COLUMN "{schema}".jobs.budget_deadline IS
    'The loop node''s budget wall (T19): the BUDGET SWEEP fires exhaustion at this DB-clock deadline — NULL for a loop without a budget (the arm never fires). Holds PAUSE it (budget_paused), never burn it.';
COMMENT ON COLUMN "{schema}".jobs.budget_paused IS
    'The held-loop''s budget pause (T19): the budget sweep''s arm carries AND NOT budget_paused — a loop holding on a human is invisible to the wall even when its deadline is forced into the past (the CONSUME-BUDGET dragon''s cure).';
COMMENT ON COLUMN "{schema}".jobs.budget_remaining_ms IS
    'The loop node''s on-wake remaining budget (T19), computed from PG''s clock on every resume — holds are free: the deadline does not burn while waiting on a human.';
