-- Fleet-wide per-queue concurrency cap: add max_concurrent column to the queues table.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- Ops note (locks), per the interim guidance in issue #29:
--   * ADD COLUMN ... int (nullable, no default) is metadata-only: no table
--     rewrite; it takes ACCESS EXCLUSIVE on "{schema}".queues for the
--     catalog update only (sub-millisecond).
--   * ADD CONSTRAINT ... CHECK takes ACCESS EXCLUSIVE on "queues" while it
--     scans existing rows for validation (CHECK is not one of the reduced-
--     lock forms — only ADD FOREIGN KEY is). The scan blocks reads and
--     writes on "queues" for its duration, but "queues" is small and
--     low-churn (one row per declared queue), so this is effectively
--     instant; no maintenance window is warranted.
--   * No index is built here, so CREATE INDEX CONCURRENTLY is not needed;
--     note the migration runner cannot express CONCURRENTLY at all
--     (issue #29) — relevant only to future index-creating migrations on
--     hot tables (jobs, job_events), which should name a maintenance
--     window explicitly. This migration does not warrant one.

-- Unlike actor_config.max_concurrent (per-actor, per-worker) and
-- WorkerSettings.max_concurrency (per-worker), this column sets a
-- fleet-wide concurrency cap for a queue — enforced across all workers
-- sharing the schema by binding a ConcurrencyReservation to the queue
-- name. The reservation reuses the existing distributed leased-slot
-- machinery (reservation_slots table) rather than a new mechanism.
-- NULL means uncapped, matching the actor_config.max_concurrent convention.
ALTER TABLE "{schema}".queues ADD COLUMN IF NOT EXISTS max_concurrent int;
ALTER TABLE "{schema}".queues ADD CONSTRAINT queues_max_concurrent_check
    CHECK (max_concurrent IS NULL OR max_concurrent >= 1);
