-- Durable workflow run incarnation for guarded terminal effects. PRE.
-- Legacy rows remain NULL; newly created roots stamp 1, and only the existing
-- locked manual-reopen path increments it. No module/global counter exists.
-- Archive mirrors the immutable evidence. Forward-only; no down migration.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS run_epoch bigint NULL;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS run_epoch bigint NULL;
COMMENT ON COLUMN "{schema}".jobs.run_epoch IS
    'Workflow run incarnation; NULL on legacy rows. New roots begin at 1 and explicit successful reopen increments the root under its row lock. Terminal-effect identity uses this durable value.';
