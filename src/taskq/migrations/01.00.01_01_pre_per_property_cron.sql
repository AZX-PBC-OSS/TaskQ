-- Per-property cron schedules: replace UNIQUE(actor) with UNIQUE(actor, name)
-- and add an identity_key column propagated to cron-fired jobs for dedup.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- Existing rows keep name='' (the column default), so each pre-migration
-- schedule maps to the (actor, '') uniqueness key and the one-schedule-per-actor
-- invariant is preserved. New schedules may set name to run several cron
-- schedules per actor (e.g. per-property syncs).
ALTER TABLE "{schema}".cron_schedules DROP CONSTRAINT IF EXISTS cron_schedules_actor_key;

ALTER TABLE "{schema}".cron_schedules
    ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';

-- When set, the cron loop passes identity_key to EnqueueArgs so cron-fired
-- jobs dedup against on-demand jobs for the same business key.
ALTER TABLE "{schema}".cron_schedules
    ADD COLUMN IF NOT EXISTS identity_key text;

ALTER TABLE "{schema}".cron_schedules
    ADD CONSTRAINT cron_schedules_actor_name_key UNIQUE (actor, name);
