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
-- scanning the full job_events table.  poll_reclaim_events also filters
-- on occurred_at (see src/taskq/backend/_sql_templates.py for why); that
-- predicate is evaluated against the small partial-index result set, so
-- no separate index is needed for it.
CREATE INDEX IF NOT EXISTS job_events_reclaim_idx
    ON "{schema}".job_events (id)
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired';
