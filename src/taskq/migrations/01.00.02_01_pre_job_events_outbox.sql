-- Partial index to accelerate fleet-wide polling of crash-reclaim events.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- The sweep_expired_locks code already writes job_events rows with
-- kind='state_change' and detail->>'reason'='lock_expired' in the same
-- transaction as the reclaim UPDATE.  This partial index makes the
-- cursor-based tailing query (poll_reclaim_events) efficient without
-- scanning the full job_events table.
CREATE INDEX IF NOT EXISTS job_events_reclaim_idx
    ON "{schema}".job_events (id)
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired';
