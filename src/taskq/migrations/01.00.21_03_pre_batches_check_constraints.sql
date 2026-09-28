-- Converges the batches table's column CHECK constraints across every
-- deployment vintage. The released 01.00.05_01 (38b328db, shipped in a
-- release) declares them in its CREATE TABLE, so databases that applied
-- the released file already have them; databases that applied the
-- ORIGINAL file (pre-release source pins) do not. This file adds them
-- where absent — a no-op where present — so every database ends in the
-- same enforced shape:
--
--   expected_size        >= 0
--   consecutive_failures >= 0
--   failure_threshold    IS NULL OR >= 1
--
-- Transactional and idempotent by guard: the constraint names are checked
-- against pg_constraint before each ADD (the released population's
-- auto-named constraints match these names exactly), so the file is safe
-- on every vintage and re-runnable after a rollback. ADD CONSTRAINT takes
-- an ACCESS EXCLUSIVE lock for its validation scan, bounded by the
-- runner's ddl_lock_timeout; batches tables are small (one row per
-- batch), so the scan is milliseconds. For a fleet-scale batches table,
-- pre-stage with the documented CONCURRENTLY swap pattern from
-- 01.00.19_01's OPS NOTE before upgrading.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_expected_size_check'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_expected_size_check
            CHECK (expected_size >= 0)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_consecutive_failures_check'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_consecutive_failures_check
            CHECK (consecutive_failures >= 0)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_failure_threshold_check'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_failure_threshold_check
            CHECK (failure_threshold IS NULL OR failure_threshold >= 1)';
    END IF;
END
$$;
