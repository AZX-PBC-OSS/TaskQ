-- Fleet-wide per-queue concurrency cap: add max_concurrent column to the queues table.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

-- Unlike actor_config.max_concurrent (per-actor, per-worker) and
-- WorkerSettings.max_concurrency (per-worker), this column sets a
-- fleet-wide concurrency cap for a queue — enforced across all workers
-- sharing the schema by binding a ConcurrencyReservation to the queue
-- name. The reservation reuses the existing distributed leased-slot
-- machinery (reservation_slots table) rather than a new mechanism.
-- NULL means uncapped, matching the actor_config.max_concurrent convention.
ALTER TABLE "{schema}".queues ADD COLUMN IF NOT EXISTS max_concurrent int;
