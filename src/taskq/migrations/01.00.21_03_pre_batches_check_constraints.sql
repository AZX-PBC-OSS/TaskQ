-- taskq:no-transaction -- the NOT VALID commits must release ACCESS
-- EXCLUSIVE before VALIDATE scans, so the two cannot share a transaction
-- Converges the batches table's column CHECK constraints on databases that
-- applied 01.00.05_01 as originally shipped. The original CREATE TABLE
-- carried no CHECKs; the constraints below were added to the file after
-- it shipped (28c665bc), which the checksum drift guard rightly refuses
-- on any database holding the original's checksum — the file is restored
-- byte-exact and the constraints instead ride here, so every database
-- (fresh or upgraded) converges to the same enforced shape:
--
--   expected_size        >= 0
--   consecutive_failures >= 0
--   failure_threshold    IS NULL OR >= 1
--
-- NOT VALID first, then VALIDATE: ADD CONSTRAINT (even NOT VALID) takes
-- an ACCESS EXCLUSIVE lock, and a plain ADD would hold it for the whole
-- validation scan — on a large batches table that wait outlives the
-- runner's ddl_lock_timeout budget and parks every writer behind it.
-- NOT VALID commits the exclusive lock in moments, enforces the CHECK
-- for all NEW writes immediately, and VALIDATE then scans under SHARE
-- UPDATE EXCLUSIVE (reads and writes proceed). If existing rows violate
-- the CHECK, VALIDATE fails the migration — fail-closed: the operator
-- sees the invalid rows rather than the ledger silently recording a
-- constraint the data never satisfied. New writes were already bounded
-- by the application layer, so a violation means pre-existing bad data.
--
-- Idempotent by guard: no-transaction files may re-run after a mid-file
-- crash, and a database built from the post-28c665bc file already holds
-- the constraints (auto-named or explicit) — the pg_constraint check
-- makes both cases no-ops.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_expected_size_check'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conrelid = t.oid
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%expected_size%'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_expected_size_check
            CHECK (expected_size >= 0) NOT VALID';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_consecutive_failures_check'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%consecutive_failures%'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_consecutive_failures_check
            CHECK (consecutive_failures >= 0) NOT VALID';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.conname = 'batches_failure_threshold_check'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'batches'
          AND c.contype = 'c'
          AND pg_get_constraintdef(c.oid) LIKE '%failure_threshold%'
    ) THEN
        EXECUTE 'ALTER TABLE "{schema}".batches
            ADD CONSTRAINT batches_failure_threshold_check
            CHECK (failure_threshold IS NULL OR failure_threshold >= 1) NOT VALID';
    END IF;
END
$$;
ALTER TABLE "{schema}".batches VALIDATE CONSTRAINT batches_expected_size_check;
ALTER TABLE "{schema}".batches VALIDATE CONSTRAINT batches_consecutive_failures_check;
ALTER TABLE "{schema}".batches VALIDATE CONSTRAINT batches_failure_threshold_check;
