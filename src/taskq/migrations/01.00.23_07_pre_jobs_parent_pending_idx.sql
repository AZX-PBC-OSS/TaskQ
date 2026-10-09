-- The fan-out ledger's pending-children index (LIB-2, issue #670) —
-- SPLIT from 01.00.23_01_pre_jobs_parent_id.sql by the consolidation
-- (the single-lock-class law, family 1: the ALTERs and the CREATE
-- INDEX are different lock classes and may not share a file's one
-- write-block window). Idempotent: deployments that applied 23_01
-- when it still carried the index already have it (IF NOT EXISTS),
-- the ledger row records the no-op.

-- The pending-children count's quals, repeated VERBATIM (the
-- 01.00.12_06 doctrine: a partial index is only a candidate when the
-- planner can prove its predicate from the query's own quals):
--   parent_id = $1 AND status IN ('pending', 'scheduled')
-- The IS NOT NULL term keeps every unparented row (the overwhelming
-- majority — only fan-out children are stamped) out of the index, so
-- the WRITE-path cost is index maintenance on stamped rows only.

CREATE INDEX IF NOT EXISTS jobs_parent_pending_idx
    ON "{schema}".jobs (parent_id)
    WHERE status IN ('pending', 'scheduled') AND parent_id IS NOT NULL;
