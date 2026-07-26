-- Add idempotency_scope column and replace the single-column unique index on
-- idempotency_key with a composite (idempotency_scope, idempotency_key) index.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- The empty-string sentinel ('') is the default/global scope.  We use NOT NULL
-- deliberately: Postgres unique indexes treat NULL as distinct, so a nullable
-- idempotency_scope would let two unscoped idempotency_key values coexist
-- without colliding — silently breaking the prior global-dedupe guarantee.
-- NOT NULL DEFAULT '' preserves byte-for-byte behavior for callers who never
-- pass a scope.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql).
-- The archive-sweep INSERT names every column explicitly rather than relying
-- on `jobs` and `jobs_archive` sharing physical column order, so this ADD
-- COLUMN landing after archived_at/expire_at in jobs_archive's own order is
-- safe -- see the comment above _JOBS_COLUMNS_CSV in
-- src/taskq/worker/_leader_shared.py.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- Drop the old single-column unique index and replace it with the composite form.
DROP INDEX IF EXISTS "{schema}".jobs_idempotency_key_uniq;

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_scope_key_uniq
    ON "{schema}".jobs (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMENT ON COLUMN "{schema}".jobs.idempotency_scope IS
    'Namespacing scope for idempotency_key. The default empty string preserves '
    'the prior global-dedupe behavior exactly. NOT NULL (not nullable) because '
    'Postgres unique indexes treat NULL as distinct — a NULL scope would let two '
    'unscoped idempotency_key values coexist without colliding, breaking the '
    'global-dedupe guarantee. Use an explicit scope (e.g. run/batch/epoch id) to '
    'allow the same business key in different scopes to both succeed.';
