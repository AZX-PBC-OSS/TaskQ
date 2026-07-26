-- Add idempotency_scope column and the new composite (idempotency_scope,
-- idempotency_key) unique index, WITHOUT dropping the old single-column
-- index yet. Forward-only; there is no down migration. To revert, restore
-- from backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (why this is split into pre + a later post migration):
-- Postgres resolves `INSERT ... ON CONFLICT (col_list)` by finding a unique
-- index whose column set matches col_list EXACTLY (order-insensitive, but
-- not a subset/superset match). Pre-this-release code issues
-- `ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL`, which
-- only resolves against the single-column `jobs_idempotency_key_uniq`
-- index -- it does NOT match the new composite index. If this migration
-- dropped the old index, every enqueue with an idempotency_key issued by a
-- not-yet-upgraded worker during the rolling-deploy window would fail with
-- "there is no unique or exclusion constraint matching the ON CONFLICT
-- specification" -- a full outage of the idempotency-keyed enqueue path.
-- So this `pre` migration ADDS the composite index and leaves the old
-- index in place. Both indexes coexist during the overlap, and this keeps
-- PRE-THIS-RELEASE code (unscoped, unaware idempotency_scope exists)
-- working unmodified. It does NOT make USING idempotency_scope during the
-- overlap harmless: the old index still enforces "idempotency_key unique
-- across ALL scopes" (strictly stronger than the new composite
-- constraint), so enqueuing the SAME idempotency_key under TWO DIFFERENT
-- scopes during this window raises a Postgres UniqueViolationError on the
-- old index -- confirmed by cross-family review and covered by
-- tests/test_idempotency_scope_migrations.py::TestApplicationEnqueuePathDuringPreOnlyWindow.
-- THIS RELEASE's application code (src/taskq/backend/_enqueue.py) catches
-- that specific violation and raises
-- taskq.exceptions.ScopedIdempotencyMigrationPendingError instead of
-- letting the raw driver error crash the caller -- see that exception's
-- docstring for why it is a loud, typed error rather than a silent
-- cross-scope fallback. Unscoped calls and same-scope-repeated calls are
-- entirely unaffected during the overlap; only a caller who actively
-- reuses a key under a different scope before the post migration hits
-- this. Once the old index is dropped by
-- 01.00.03_01_post_idempotency_scope_drop_old_index.sql, that error stops
-- occurring and scoped dedupe activates for real. This is the same
-- forward-only ADD-only contract documented in docs/architecture.md
-- ("Schema Design Decisions" > "Forward-only migrations"), applied to an
-- index change instead of a column drop.
-- Deployment sequence:
--   1. `taskq migrate up --phase pre`  (this file) -- safe to run before,
--      during, or independent of the code rollout; old, unscoped code
--      keeps working unmodified against the still-present old index. Do
--      NOT start using idempotency_scope in application code until step 3
--      is complete, or expect ScopedIdempotencyMigrationPendingError on
--      any cross-scope reuse of a key in the meantime.
--   2. Roll out this release's code to every worker.
--   3. `taskq migrate up --phase post` (01.00.03_01) -- drops the old
--      index once step 2 is complete; only after this does
--      idempotency_scope actually decouple dedupe across scopes without
--      raising.
--
-- RESIDUAL RISK, CONFIRMED BY TWO INDEPENDENT REVIEWS -- this migration
-- BREAKS THE PRE-RELEASE ARCHIVE/PRUNE SWEEP (Sweep 5) FOR THE DURATION OF
-- THE ROLLOUT. This is not protected by the pre/post split above, because
-- the risk here is a column-position shift, not an index-resolution
-- shift, and the fix lives in code (this release explicit-columns the
-- archive-sweep INSERT; see src/taskq/worker/_leader_shared.py), not in
-- the migration. A worker still running the PREVIOUS (pre-this-release)
-- code base moves jobs to jobs_archive with a positional
-- `SELECT j.*` that assumes `jobs` and `jobs_archive` share physical
-- column order; adding idempotency_scope to `jobs` (which this migration
-- does, appended at the end of `jobs`'s own column order) breaks that
-- positional assumption for that OLD code the moment this migration
-- applies, regardless of the pre/post split above. If the elected
-- maintenance leader is still on pre-this-release code when the daily
-- prune/archive sweep fires after this migration is applied, that single
-- sweep invocation fails with a Postgres type error (confirmed: the
-- idempotency_scope text value lands in the `archived_at` timestamptz
-- column position). BOUNDED to that one daily sweep invocation on the
-- elected leader; NON-DESTRUCTIVE (the whole CTE transaction rolls back
-- cleanly, no rows lost or corrupted, dispatch/enqueue/dequeue unaffected);
-- SELF-HEALING as soon as the leader is running this release's code
-- (either because it was upgraded, or because leader re-election handed
-- the role to an already-upgraded worker). This CANNOT be fully closed within a single
-- release: the code fix that makes the archive sweep tolerate the new
-- column only exists in the release that also introduces the column.
-- Operators who need a zero-risk window for the sweep specifically should
-- ship the archive-sweep explicit-column fix alone in a prior release with
-- no schema change, let it fully roll out, and only then apply this
-- migration and this release's remaining code in a subsequent release.
-- Everyone else: apply this migration well clear of the scheduled prune
-- sweep window (TASKQ_PRUNE_SCHEDULE_UTC, default 03:00 UTC) relative to
-- your rollout, or force leader re-election onto an upgraded worker
-- immediately after deploying.

-- The empty-string sentinel ('') is the default/global scope.  We use NOT NULL
-- deliberately: Postgres unique indexes treat NULL as distinct, so a nullable
-- idempotency_scope would let two unscoped idempotency_key values coexist
-- without colliding — silently breaking the prior global-dedupe guarantee.
-- NOT NULL DEFAULT '' preserves byte-for-byte behavior for callers who never
-- pass a scope.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql).
-- This release's archive-sweep INSERT names every column explicitly rather
-- than relying on `jobs` and `jobs_archive` sharing physical column order,
-- so this ADD COLUMN landing after archived_at/expire_at in jobs_archive's
-- own order is safe for THIS release's code -- see the comment above
-- _JOBS_COLUMNS_CSV in src/taskq/worker/_leader_shared.py. It is not safe
-- for pre-this-release code; see the RESIDUAL RISK note above.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- OPS NOTE -- locking impact of this migration on `jobs`:
-- The migration runner (src/taskq/migrate.py) applies every migration file
-- inside a single transaction, so `CREATE INDEX CONCURRENTLY` is not
-- available here (Postgres forbids it inside a transaction block). The
-- CREATE UNIQUE INDEX below therefore builds the new index while holding
-- the ordinary index-build lock, which conflicts with writes: INSERT/
-- UPDATE/DELETE against "{schema}".jobs (i.e. enqueue and dequeue) block
-- for the duration of the index build, which scales with the current row
-- count of `jobs`. On a small/lightly-loaded table this is momentary; on a
-- large, busy production `jobs` table this can freeze the whole worker
-- fleet's enqueue/dequeue path for a noticeable window. Apply this
-- migration during a maintenance window (or when `jobs` is small/quiescent,
-- e.g. right after a prune sweep) on any deployment where `jobs` is large.
-- This is a limitation of the migration runner's transaction-per-file
-- design, not specific to this migration -- 01.00.01_01 has the same shape,
-- but against the tiny cron_schedules table, so its lock window is
-- negligible; this is the first migration to take that lock against `jobs`
-- itself. (01.00.03_01_post, which only drops an index, is comparatively
-- cheap -- DROP INDEX takes an exclusive lock too, but it is near-instant,
-- unlike a build.)
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_scope_key_uniq
    ON "{schema}".jobs (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMENT ON COLUMN "{schema}".jobs.idempotency_scope IS
    'Namespacing scope for idempotency_key. The default empty string preserves '
    'the prior global-dedupe behavior exactly. NOT NULL (not nullable) because '
    'Postgres unique indexes treat NULL as distinct — a NULL scope would let two '
    'unscoped idempotency_key values coexist without colliding, breaking the '
    'global-dedupe guarantee. Use an explicit scope (e.g. run/batch/epoch id) to '
    'allow the same business key in different scopes to both succeed. The old '
    'single-column jobs_idempotency_key_uniq index is dropped separately by '
    '01.00.03_01_post_idempotency_scope_drop_old_index.sql once all workers are '
    'on the release that introduced this column -- see that migration''s header '
    'and this migration''s header for the full phase rationale.';
