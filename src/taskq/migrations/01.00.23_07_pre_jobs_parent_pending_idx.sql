-- The fan-out ledger's pending-children index (LIB-2, issue #670) —
-- the IF NOT EXISTS record beside its byte-frozen home: the shipped
-- 01.00.23_01_pre_jobs_parent_id.sql carries the index and may never
-- be rewritten (the upgrade-path gate's ledger checksum law — the
-- d24f17b9 split attempt was convicted by exactly that). Idempotent
-- everywhere: a fresh schema gets the index from 23_01 and no-ops
-- here; a deployed schema that applied the shipped 23_01 has it and
-- records the no-op's ledger row.

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
