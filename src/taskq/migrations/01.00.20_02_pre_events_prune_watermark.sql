-- The event-prune watermark: the boundary every job_events trailing-watermark
-- consumer can test its cursor against.
--
-- Two writers delete job_events rows a consumer may not have read yet: the
-- event-retention sweep (age-keyed, with the crash-reclaim outbox carve-out
-- and its age cap) and the terminal-job prune (its DELETE FROM jobs cascades
-- the archived jobs' events away, job_events.job_id REFERENCES jobs ON DELETE
-- CASCADE, there is no job_events_archive). Before this table neither deleter
-- left a trace a poller could see: a watch_reclaims consumer resuming a cursor
-- that sat behind either deletion horizon simply polled the surviving rows and
-- continued -- the lost events left no signal, the consumer's `async for`
-- behaved exactly as a fleet that had simply been quiet.
--
-- Each deleter now advances `pruned_through_id` to the highest event id its
-- statement deleted, in the SAME statement/transaction as the delete, so the
-- watermark can never lag what is already gone. The consumer-side poll
-- (taskq.client._taskq watch_reclaims transports) compares its persisted
-- cursor against this row: a cursor STRICTLY BELOW the watermark means at
-- least one undelivered event id was deleted -- a hole no poll can ever
-- refill -- and the stream ends with EventRetentionGapError instead of
-- silently skipping to live. A cursor at or above the watermark is safe by
-- construction: every deleted id was at or below a position the consumer had
-- already been delivered.
--
-- Why a recorded bound and not gap detection on the ids themselves: event id
-- is a bigserial, and an aborted writer transaction consumes its nextval
-- without inserting a row, so id-space holes occur in ordinary operation with
-- nothing deleted. A watermark only ever advances on a committed DELETE, so it
-- never fires for a rollback gap. It is a UNION bound across event kinds (the
-- sweep deletes by age across kinds; the poll filters to the reclaim slice),
-- so the signal is conservative: it proves the feed is incomplete, not that
-- the specific slice a consumer filters for lost a row. Fail-visible beats
-- silently plausible.
--
-- One row, singleton-check'd like maintenance_leader. GREATEST on conflict so
-- a concurrent duplicate sweep (rolling deploy, leader-lock name convergence)
-- can never move the bound backwards.
--
-- ROLLING DEPLOY: additive and inert to old code. Pre-fix leaders never write
-- the row (it ships at 0) and pre-fix pollers never read it, so applying while
-- old code runs changes nothing; the gap signal only exists once the new
-- poller runs. Backfill honesty: rows deleted BEFORE this migration applied
-- are not in any watermark, the same trust every retention regime extends to
-- pre-existing cursors. Forward-only; there is no down migration.
CREATE TABLE IF NOT EXISTS "{schema}".job_events_prune_state (
    singleton          boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    pruned_through_id  bigint NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

INSERT INTO "{schema}".job_events_prune_state (singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

COMMENT ON TABLE "{schema}".job_events_prune_state IS
    'Event-prune watermark: the highest job_events id any retention deleter '
    'has committed a delete below-or-at. A trailing-watermark consumer whose '
    'persisted cursor sits strictly below this value has lost undelivered '
    'events to retention and must be failed visible, not silently skipped.';
