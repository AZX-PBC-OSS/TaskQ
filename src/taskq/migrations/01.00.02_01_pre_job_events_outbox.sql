-- Partial index to accelerate fleet-wide polling of crash-reclaim events.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- ── Maintenance-window caveat (CREATE INDEX, not CONCURRENTLY) ──────────
-- The CREATE INDEX below takes an EXCLUSIVE lock on job_events for the
-- duration of the index build, blocking all readers and writers to that
-- table.  job_events is written on essentially every lifecycle transition
-- and progress event, so the lock can cause observable write stalls in
-- production.  Build time is proportional to the current row count.
--
-- src/taskq/migrate.py's apply_pending wraps every migration in a
-- transaction (each file runs inside `async with conn.transaction()`),
-- and Postgres forbids CREATE INDEX CONCURRENTLY inside a transaction
-- block — so this migration cannot use CONCURRENTLY without changing
-- migrate.py's transactional-apply behaviour (out of scope).
--
-- Operators with a large or heavily-populated job_events table should run
-- the equivalent `CREATE INDEX CONCURRENTLY IF NOT EXISTS
-- job_events_reclaim_idx ON "{schema}".job_events (id) WHERE kind =
-- 'state_change' AND (detail->>'reason') = 'lock_expired'` manually
-- outside the migration runner during a maintenance window, then mark
-- this migration as already-applied (or let it no-op via IF NOT EXISTS).

-- The sweep_expired_locks code already writes job_events rows with
-- kind='state_change' and detail->>'reason'='lock_expired' in the same
-- transaction as the reclaim UPDATE.  This partial index makes the
-- cursor-based tailing query (poll_reclaim_events) efficient without
-- scanning the full job_events table.
CREATE INDEX IF NOT EXISTS job_events_reclaim_idx
    ON "{schema}".job_events (id)
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired';

-- xact_id records the inserting transaction's id (pg_current_xact_id()) so
-- poll_reclaim_events can filter out rows whose transaction is not yet
-- guaranteed-complete relative to a later snapshot — bigserial id order and
-- commit order diverge under concurrent sweep transactions, so id alone is
-- not a safe cursor boundary. Nullable, no backfill: existing rows predate
-- any concurrency concern (already long committed) and are always safe;
-- NULL is treated as "safe" by poll_reclaim_events. Column add + default
-- are both fast metadata-only operations (no table rewrite): adding a
-- nullable column with no default, then attaching the default separately,
-- avoids a full-table rewrite even though the default expression is
-- volatile.
ALTER TABLE "{schema}".job_events ADD COLUMN IF NOT EXISTS xact_id bigint;
ALTER TABLE "{schema}".job_events ALTER COLUMN xact_id SET DEFAULT (pg_current_xact_id()::text::bigint);
