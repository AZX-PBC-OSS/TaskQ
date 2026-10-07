-- TaskQ initial schema: jobs/dispatch core, archive tables, and rate-limit tables.
-- The literal ""attack2"" tokens are substituted at apply time by the migration runner.

CREATE SCHEMA IF NOT EXISTS "attack2";

-- ============================================================
-- Migration tracking
-- ============================================================
CREATE TABLE "attack2".schema_migrations (
    version       text PRIMARY KEY,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    checksum      text NOT NULL
);

-- ============================================================
-- Workers (liveness / membership)
-- ============================================================
CREATE TABLE "attack2".workers (
    id                  uuid PRIMARY KEY,
    hostname            text NOT NULL,
    pid                 int  NOT NULL,
    queues              text[] NOT NULL,
    started_at          timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    worker_label        text,
    workgroup_instance  uuid,
    metadata            jsonb NOT NULL DEFAULT '{{}}'::jsonb
);
CREATE INDEX workers_last_seen_idx ON "attack2".workers (last_seen_at);
CREATE INDEX workers_wg_lookup_idx ON "attack2".workers (workgroup_instance, worker_label)
    WHERE worker_label IS NOT NULL AND workgroup_instance IS NOT NULL;

COMMENT ON COLUMN "attack2".workers.worker_label IS
    'Human-readable label set by the workgroup supervisor or --worker-label CLI flag.';
COMMENT ON COLUMN "attack2".workers.workgroup_instance IS
    'UUIDv7 identifying the workgroup orchestrator that launched this worker. '
    'Used for cross-process correlation and health checking.';

-- ============================================================
-- Maintenance leader (queryable; advisory lock is the source of truth)
-- ============================================================
CREATE TABLE "attack2".maintenance_leader (
    singleton     boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    worker_id     uuid NOT NULL REFERENCES "attack2".workers(id) ON DELETE CASCADE,
    elected_at    timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- Jobs (the hot table)
-- NOTE: 'awaiting_resource' is intentionally NOT in this enum.
-- Reservation denial transitions the job to 'scheduled' with a
-- metadata annotation (metadata.awaiting='reservation:<bucket>').
-- ============================================================
CREATE TYPE "attack2".job_status AS ENUM (
    'pending',
    'scheduled',
    'running',
    'succeeded',
    'failed',
    'cancelled',
    'crashed',
    'abandoned'
);

CREATE TABLE "attack2".jobs (
    id                  uuid PRIMARY KEY,
    actor               text NOT NULL,
    queue               text NOT NULL,
    identity_key        text,
    fairness_key        text,
    payload             jsonb NOT NULL,
    -- TODO(future-migration): must ship a new migration:
    --   ALTER TABLE "attack2".jobs ALTER COLUMN payload_schema_ver TYPE text USING payload_schema_ver::text;
    --   ALTER TABLE "attack2".jobs ALTER COLUMN payload_schema_ver SET DEFAULT '1';
    -- The discriminated-union pattern requires text (string values like 'v1', 'v2').
    -- Until that migration lands, string discriminator storage will fail at the PG level.
    payload_schema_ver  int NOT NULL DEFAULT 1,
    status              "attack2".job_status NOT NULL DEFAULT 'pending',
    priority            smallint NOT NULL DEFAULT 0,
    attempt             smallint NOT NULL DEFAULT 0,
    max_attempts        smallint NOT NULL,
    retry_kind          text NOT NULL,
    schedule_to_close   timestamptz,
    start_to_close      interval,
    heartbeat_timeout   interval,
    created_at          timestamptz NOT NULL DEFAULT now(),
    scheduled_at        timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz,
    last_heartbeat_at   timestamptz,
    -- No FK to workers(id): the implicit FOR KEY SHARE on the parent row
    -- taken by every dispatch UPDATE serializes through MultiXact SLRU under
    -- concurrent dequeue.
    locked_by_worker    uuid,
    lock_expires_at     timestamptz,
    cancel_requested_at timestamptz,
    cancel_phase        smallint NOT NULL DEFAULT 0,
    error_class         text,
    error_message       text,
    error_traceback     text,
    progress_state      jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    progress_seq        int NOT NULL DEFAULT 0,
    result              jsonb,
    result_size_bytes   int,
    result_expires_at   timestamptz,
    idempotency_key     text,
    trace_id            text,
    span_id             text,
    metadata            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    tags                text[] NOT NULL DEFAULT '{{}}',

    CHECK (cancel_phase BETWEEN 0 AND 2),
    CHECK (retry_kind IN ('transient', 'indefinite', 'non_retryable'))
);

-- Dispatch index: the most important index in the system.
CREATE INDEX jobs_dispatch_idx
    ON "attack2".jobs (queue, priority DESC, scheduled_at)
    WHERE status = 'pending';

-- Per-actor dispatch index for bounded LATERAL range scans.
-- Supports per-(actor, queue) index seeks decoupled from backlog depth.
CREATE INDEX jobs_actor_dispatch_idx
    ON "attack2".jobs (actor, queue, priority DESC, scheduled_at, id)
    WHERE status = 'pending';

-- Round-robin fairness sampling: the per-fairness_key ROW_NUMBER window in
-- the round-robin dispatch CTE needs pre-sorted per-partition input so deep
-- backlogs don't force a full sort of every pending row on each dispatch tick.
CREATE INDEX jobs_actor_fairness_dispatch_idx
    ON "attack2".jobs (actor, queue, fairness_key, priority DESC, scheduled_at, id)
    WHERE status = 'pending';

CREATE INDEX jobs_scheduled_wake_idx
    ON "attack2".jobs (scheduled_at)
    WHERE status = 'scheduled';

CREATE INDEX jobs_running_lock_expires_idx
    ON "attack2".jobs (lock_expires_at)
    WHERE status = 'running';

CREATE INDEX jobs_schedule_to_close_idx
    ON "attack2".jobs (schedule_to_close)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX jobs_identity_active_idx
    ON "attack2".jobs (actor, identity_key)
    WHERE status IN ('pending', 'scheduled', 'running');

CREATE UNIQUE INDEX jobs_idempotency_key_uniq
    ON "attack2".jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX jobs_singleton_uniq
    ON "attack2".jobs (actor)
    WHERE status IN ('pending', 'scheduled', 'running')
      AND metadata @> '{{"singleton": true}}'::jsonb;

CREATE INDEX jobs_actor_running_idx
    ON "attack2".jobs (actor)
    WHERE status = 'running';

-- Per-actor pending+scheduled count for max_pending backpressure (§3.3).
CREATE INDEX jobs_actor_pending_idx
    ON "attack2".jobs (actor)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX jobs_finished_at_idx
    ON "attack2".jobs (finished_at)
    WHERE status IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned');

CREATE INDEX jobs_metadata_gin_idx
    ON "attack2".jobs USING gin (metadata jsonb_path_ops);

CREATE INDEX jobs_cancel_requested_idx
    ON "attack2".jobs (locked_by_worker, cancel_requested_at)
    WHERE cancel_requested_at IS NOT NULL AND status = 'running';

-- Hot-path: heartbeat tick extends lock_expires_at for every running job owned by
-- this worker (heartbeat.py:42).  Without this, PG scans all running rows.
-- Vendor parallel: pgqueuer (queue_manager_id) WHERE queue_manager_id IS NOT NULL.
CREATE INDEX jobs_locked_by_worker_running_idx
    ON "attack2".jobs (locked_by_worker)
    WHERE status = 'running';

-- Leader sweep: clear expired results (postgres.py:197).
-- Vendor parallel: River (state, finalized_at) WHERE finalized_at IS NOT NULL.
CREATE INDEX jobs_result_expires_at_idx
    ON "attack2".jobs (result_expires_at)
    WHERE result IS NOT NULL;

CREATE INDEX jobs_tags_gin_idx ON "attack2".jobs USING gin (tags);

COMMENT ON COLUMN "attack2".jobs.identity_key IS
    'User-derived logical work unit. Used for serialization and unique-for. NOT idempotency.';
COMMENT ON COLUMN "attack2".jobs.idempotency_key IS
    'Caller-provided. Used to make enqueue idempotent. Distinct from identity.';
COMMENT ON COLUMN "attack2".jobs.fairness_key IS
    'User-derived cohort key. NULL collapses to one cohort via COALESCE in dispatch.';
COMMENT ON COLUMN "attack2".jobs.error_traceback IS
    'Last attempt error only. Full per-attempt history in job_attempts table.';
COMMENT ON COLUMN "attack2".jobs.cancel_phase IS
    '0 = no cancellation; 1 = cooperative cancel requested; 2 = force cancel issued.';

-- ============================================================
-- Per-attempt history (full trace of every execution try)
-- ============================================================
CREATE TABLE "attack2".job_attempts (
    job_id          uuid NOT NULL REFERENCES "attack2".jobs(id) ON DELETE CASCADE,
    attempt         smallint NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    outcome         text,
    error_class     text,
    error_message   text,
    error_traceback text,
    duration_ms     int,
    worker_id       uuid REFERENCES "attack2".workers(id) ON DELETE SET NULL,
    metadata        jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    PRIMARY KEY (job_id, attempt)
);
CREATE INDEX job_attempts_started_idx
    ON "attack2".job_attempts (started_at);
CREATE INDEX job_attempts_outcome_idx
    ON "attack2".job_attempts (outcome, finished_at);

COMMENT ON TABLE "attack2".job_attempts IS
    'Full history of every execution attempt of every job. Pruned with parent job via ON DELETE CASCADE.';
COMMENT ON COLUMN "attack2".job_attempts.outcome IS
    'Valid values: succeeded, failed, snoozed, cancelled, crashed. Error class distinguishes sub-types (e.g. DeadlineExceeded, WorkerCrashed, MaxAttemptsExceeded).';

-- ============================================================
-- Per-actor concurrency caps (cached config)
-- ============================================================
CREATE TABLE "attack2".actor_config (
    actor               text PRIMARY KEY,
    max_concurrent      int,
    max_pending         int,
    queue               text NOT NULL,
    max_attempts        smallint NOT NULL DEFAULT 3,
    retry_kind          text NOT NULL DEFAULT 'transient',
    result_ttl          float,
    metadata            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE "attack2".queues (
    name       text PRIMARY KEY,
    mode       text NOT NULL DEFAULT 'strict_fifo',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (mode IN ('strict_fifo', 'round_robin'))
);

-- ============================================================
-- Cron schedules
-- ============================================================
CREATE TABLE "attack2".cron_schedules (
    id                   uuid PRIMARY KEY,
    actor                text NOT NULL UNIQUE,
    cron_expr            text NOT NULL,
    timezone             text NOT NULL DEFAULT 'UTC',
    dst_strategy         text NOT NULL DEFAULT 'skip',
    payload_factory      text,
    enabled              boolean NOT NULL DEFAULT true,
    last_fired_at        timestamptz,
    last_fire_error      text,
    consecutive_failures int NOT NULL DEFAULT 0,
    next_fire_at         timestamptz NOT NULL,
    metadata             jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    CHECK (dst_strategy IN ('skip', 'firstof', 'allof'))
);
CREATE INDEX cron_schedules_next_fire_idx
    ON "attack2".cron_schedules (next_fire_at)
    WHERE enabled = true;

-- ============================================================
-- Concurrency reservation slots
-- ============================================================
CREATE TABLE "attack2".reservation_slots (
    bucket_name       text NOT NULL,
    slot_index        int  NOT NULL,
    job_id            uuid,
    held_by_worker_id uuid,
    acquired_at       timestamptz,
    lease_expires_at  timestamptz,
    PRIMARY KEY (bucket_name, slot_index)
);
CREATE INDEX reservation_slots_free_idx
    ON "attack2".reservation_slots (bucket_name, slot_index)
    WHERE job_id IS NULL;
CREATE INDEX reservation_slots_lease_expires_idx
    ON "attack2".reservation_slots (lease_expires_at)
    WHERE job_id IS NOT NULL;

-- Hot-path: heartbeat extends reservation leases via subquery on job_id
-- (heartbeat.py:47).  Without this, PG scans all non-free slots.
CREATE INDEX reservation_slots_job_id_idx
    ON "attack2".reservation_slots (job_id)
    WHERE job_id IS NOT NULL;

-- ============================================================
-- Token bucket / sliding window PG fallback
-- ============================================================
CREATE TABLE "attack2".rate_limit_buckets (
    bucket_name     text PRIMARY KEY,
    kind            text NOT NULL,
    state           jsonb NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Sliding-window log-style PG fallback table (used when the rate-limit
-- backend is "postgres" instead of Redis).
CREATE TABLE "attack2".rate_limit_window_entries (
    bucket_name  text        NOT NULL,
    ts           timestamptz NOT NULL,
    request_id   uuid        NOT NULL,
    PRIMARY KEY (bucket_name, ts, request_id)
);
CREATE INDEX rate_limit_window_entries_lookup
    ON "attack2".rate_limit_window_entries (bucket_name, ts);

-- ============================================================
-- Events log (state transitions; admin UI / audit)
-- ============================================================
CREATE TABLE "attack2".job_events (
    id          bigserial PRIMARY KEY,
    job_id      uuid NOT NULL REFERENCES "attack2".jobs(id) ON DELETE CASCADE,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    kind        text NOT NULL,
    detail      jsonb NOT NULL DEFAULT '{{}}'::jsonb
);
CREATE INDEX job_events_job_id_idx ON "attack2".job_events (job_id, occurred_at);
COMMENT ON COLUMN "attack2".job_events.kind IS
    'Event type; one of: state_change | cancel_request | heartbeat_miss | progress';

-- ============================================================
-- Archive tables for terminal jobs pruned by the maintenance leader
-- ============================================================
CREATE TABLE "attack2".jobs_archive (
    id                  uuid PRIMARY KEY,
    actor               text NOT NULL,
    queue               text NOT NULL,
    identity_key        text,
    fairness_key        text,
    payload             jsonb NOT NULL,
    payload_schema_ver  int NOT NULL DEFAULT 1,
    status              "attack2".job_status NOT NULL,
    priority            smallint NOT NULL DEFAULT 0,
    attempt             smallint NOT NULL DEFAULT 0,
    max_attempts        smallint NOT NULL,
    retry_kind          text NOT NULL,
    schedule_to_close   timestamptz,
    start_to_close      interval,
    heartbeat_timeout   interval,
    created_at          timestamptz NOT NULL DEFAULT now(),
    scheduled_at        timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    finished_at         timestamptz,
    last_heartbeat_at   timestamptz,
    locked_by_worker    uuid,
    lock_expires_at     timestamptz,
    cancel_requested_at timestamptz,
    cancel_phase        smallint NOT NULL DEFAULT 0,
    error_class         text,
    error_message       text,
    error_traceback     text,
    progress_state      jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    progress_seq        int NOT NULL DEFAULT 0,
    result              jsonb,
    result_size_bytes   int,
    result_expires_at   timestamptz,
    idempotency_key     text,
    trace_id            text,
    span_id             text,
    metadata            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    tags                text[] NOT NULL DEFAULT '{{}}',
    archived_at         timestamptz NOT NULL DEFAULT now(),
    expire_at           timestamptz NOT NULL,

    CHECK (cancel_phase BETWEEN 0 AND 2),
    CHECK (retry_kind IN ('transient', 'indefinite', 'non_retryable'))
);

CREATE INDEX jobs_archive_expire_at_idx
    ON "attack2".jobs_archive (expire_at);

CREATE INDEX jobs_archive_finished_at_idx
    ON "attack2".jobs_archive (finished_at);

CREATE INDEX jobs_archive_tags_gin_idx ON "attack2".jobs_archive USING gin (tags);

CREATE TABLE "attack2".job_attempts_archive (
    job_id          uuid NOT NULL REFERENCES "attack2".jobs_archive(id) ON DELETE CASCADE,
    attempt         smallint NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    outcome         text,
    error_class     text,
    error_message   text,
    error_traceback text,
    duration_ms     int,
    worker_id       uuid,
    metadata        jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    PRIMARY KEY (job_id, attempt)
);

CREATE INDEX job_attempts_archive_job_id_idx
    ON "attack2".job_attempts_archive (job_id);

COMMENT ON TABLE "attack2".jobs_archive IS
    'Terminal jobs moved from "attack2".jobs by the prune sweep. Retained for '
    'archive_retention_period (default 1 year) then hard-deleted by the '
    'archive expiry sweep. Not involved in dispatch or heartbeat.';

COMMENT ON TABLE "attack2".job_attempts_archive IS
    'Per-attempt history for archived jobs. Pruned with parent via ON DELETE '
    'CASCADE when the archive expiry sweep hard-deletes jobs_archive rows.';

-- ============================================================
-- NOTIFY trigger on jobs INSERT (wakes idle workers waiting on the channel)
-- ============================================================
-- Fires pg_notify when a row is inserted with status='pending',
-- waking all workers subscribed to the wake channel.
-- The application-side pg_notify() in PostgresBackend._enqueue_on_conn
-- and _enqueue_batch_on_conn remains the primary path; this trigger
-- is defense-in-depth for direct SQL inserts.

CREATE OR REPLACE FUNCTION "attack2".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify('taskq_wake_' || TG_TABLE_SCHEMA, '');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_notify_job_insert
AFTER INSERT ON "attack2".jobs
FOR EACH ROW
WHEN (NEW.status = 'pending')
EXECUTE FUNCTION "attack2".notify_job_insert();

-- Per-property cron schedules: replace UNIQUE(actor) with UNIQUE(actor, name)
-- and add an identity_key column propagated to cron-fired jobs for dedup.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "attack2" token is substituted at apply time by the migration runner.

-- Existing rows keep name='' (the column default), so each pre-migration
-- schedule maps to the (actor, '') uniqueness key and the one-schedule-per-actor
-- invariant is preserved. New schedules may set name to run several cron
-- schedules per actor (e.g. per-property syncs).
ALTER TABLE "attack2".cron_schedules DROP CONSTRAINT IF EXISTS cron_schedules_actor_key;

ALTER TABLE "attack2".cron_schedules
    ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';

-- When set, the cron loop passes identity_key to EnqueueArgs so cron-fired
-- jobs dedup against on-demand jobs for the same business key.
ALTER TABLE "attack2".cron_schedules
    ADD COLUMN IF NOT EXISTS identity_key text;

ALTER TABLE "attack2".cron_schedules
    ADD CONSTRAINT cron_schedules_actor_name_key UNIQUE (actor, name);

-- Partial index to accelerate fleet-wide polling of crash-reclaim events.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "attack2" token is substituted at apply time by the migration runner.

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
-- job_events_reclaim_idx ON "attack2".job_events (id) WHERE kind =
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
    ON "attack2".job_events (id)
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired';

-- Drop the old single-column jobs_idempotency_key_uniq index now that the
-- composite jobs_idempotency_scope_key_uniq index (added by
-- 01.00.03_01_pre_idempotency_scope.sql) has taken over enforcing
-- idempotency-key uniqueness. Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "attack2" token is
-- substituted at apply time by the migration runner.
--
-- DO NOT apply this migration (`taskq migrate up --phase post`, or a plain
-- `taskq migrate up` once the pre phase is already applied) until every
-- worker in the fleet is confirmed running the release that shipped
-- 01.00.03_01_pre_idempotency_scope.sql. Pre-that-release code issues
-- `ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL`, which
-- can only resolve against the single-column index this file drops.
-- Postgres resolves the ON CONFLICT arbiter index statically, at plan
-- time -- it does not matter whether the row being inserted actually
-- carries an idempotency_key -- so dropping the old index while any
-- pre-that-release worker is still running turns EVERY enqueue from that
-- worker into a hard failure ("there is no unique or exclusion constraint
-- matching the ON CONFLICT specification", SQLSTATE 42P10), keyed or not.
-- The migration runner additionally refuses to apply a post migration
-- before its same-version pre counterpart (see apply_pending in
-- src/taskq/migrate.py), so `taskq migrate up --phase post` cannot be
-- used to trigger this state accidentally ahead of the pre phase. See
-- the "PHASE OBLIGATIONS" note at the top of
-- 01.00.03_01_pre_idempotency_scope.sql for the full rationale and the
-- three-step deployment sequence this migration is step 3 of.
--
-- Until this migration runs, idempotency_scope is present and read/written
-- correctly by upgraded code, but the OLD single-column index still
-- enforces "idempotency_key unique across ALL scopes" -- strictly stronger
-- than the new composite constraint -- so two enqueues with the same key
-- in different scopes will still collide against the old index. Only after
-- this migration drops that index does the same key in different scopes
-- actually both succeed.
DROP INDEX IF EXISTS "attack2".jobs_idempotency_key_uniq;

-- Add idempotency_scope column and the new composite (idempotency_scope,
-- idempotency_key) unique index, WITHOUT dropping the old single-column
-- index yet. Forward-only; there is no down migration. To revert, restore
-- from backup. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (why this is split into pre + a later post migration):
-- Postgres resolves `INSERT ... ON CONFLICT (col_list)` by finding a unique
-- index whose column set matches col_list EXACTLY (order-insensitive, but
-- not a subset/superset match). Pre-this-release code issues
-- `ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL`, which
-- only resolves against the single-column `jobs_idempotency_key_uniq`
-- index -- it does NOT match the new composite index. If this migration
-- dropped the old index, EVERY enqueue issued by a not-yet-upgraded worker
-- during the rolling-deploy window would fail with "there is no unique or
-- exclusion constraint matching the ON CONFLICT specification" (SQLSTATE
-- 42P10) -- Postgres resolves the ON CONFLICT arbiter index statically at
-- plan time, so this fires even for rows with no idempotency_key: a full
-- outage of the enqueue path, not just the idempotency-keyed one.
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
-- cross-scope fallback. The trigger is a key existing under a DIFFERENT
-- scope, in EITHER direction: an unscoped call that reuses a key first
-- written under a non-default scope hits this too (verified against live
-- PostgreSQL). Only brand-new keys and same-scope-repeated calls are
-- unaffected during the overlap. Once the old index is dropped by
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
--
-- SECOND RESIDUAL RISK, FOUND BY CONCURRENCY TESTING OF THIS WINDOW
-- (tests/test_idempotency_scope_migrations.py::TestConcurrentOverlapWindow):
-- an OLD-code worker and an UPGRADED worker inserting the SAME unscoped
-- idempotency_key at the SAME instant. Postgres reports in-flight
-- speculative-insertion conflicts against NON-arbiter unique indexes
-- unconditionally, so when the composite index (non-arbiter for the old
-- statement) happens to report the conflict, the OLD worker's enqueue
-- crashes with a raw UniqueViolationError where pre-migration code would
-- have deduped cleanly. This cannot be fixed from the library side -- the
-- failing statement is the old release's code -- but it is BOUNDED to the
-- overlap window, requires a mixed-version fleet plus a same-key
-- same-instant race, is NON-DESTRUCTIVE (the losing transaction rolls
-- back; exactly one row survives; a caller retry then dedupes against the
-- winner), and SELF-HEALING once the post phase drops the old index. The
-- symmetric case for UPGRADED code IS handled on the pool-owning enqueue
-- paths (enqueue / enqueue_batch): this release's backend retries once on
-- a fresh transaction and dedupes via the composite arbiter, so those
-- callers never see an error for a same-pair race (see
-- _LegacyIdempotencyKeyConflictError in src/taskq/backend/_enqueue.py).
-- Borrowed-connection callers (enqueue_with_conn, enqueue_batch with an
-- explicit connection) cannot retry -- their transaction is already
-- aborted by the violation -- and get ScopedIdempotencyMigrationPendingError
-- instead. The enqueue_batch_fast COPY path has no ON CONFLICT handling at
-- all (duplicate keys abort the batch, as before this feature) and no
-- retry (a COPY has no arbiter to dedupe against on a second attempt),
-- but it DOES translate the legacy-index cross-scope violation into
-- ScopedIdempotencyMigrationPendingError like every other enqueue path.

-- The empty-string sentinel ('') is the default/global scope.  We use NOT NULL
-- deliberately: Postgres unique indexes treat NULL as distinct, so a nullable
-- idempotency_scope would let two unscoped idempotency_key values coexist
-- without colliding — silently breaking the prior global-dedupe guarantee.
-- NOT NULL DEFAULT '' preserves byte-for-byte behavior for callers who never
-- pass a scope.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql).
-- This release's archive-sweep INSERT names every column explicitly rather
-- than relying on `jobs` and `jobs_archive` sharing physical column order,
-- so this ADD COLUMN landing after archived_at/expire_at in jobs_archive's
-- own order is safe for THIS release's code -- see the comment above
-- _JOBS_COLUMNS_CSV in src/taskq/worker/_leader_shared.py. It is not safe
-- for pre-this-release code; see the RESIDUAL RISK note above.
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS idempotency_scope text NOT NULL DEFAULT '';

-- OPS NOTE -- locking impact of this migration on `jobs`:
-- The migration runner (src/taskq/migrate.py) applies every migration file
-- inside a single transaction, so `CREATE INDEX CONCURRENTLY` is not
-- available here (Postgres forbids it inside a transaction block). The
-- CREATE UNIQUE INDEX below therefore builds the new index while holding
-- the ordinary index-build lock, which conflicts with writes: INSERT/
-- UPDATE/DELETE against "attack2".jobs (i.e. enqueue and dequeue) block
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
    ON "attack2".jobs (idempotency_scope, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

COMMENT ON COLUMN "attack2".jobs.idempotency_scope IS
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

-- Fleet-wide per-queue concurrency cap: add max_concurrent column to the queues table.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "attack2" token is substituted at apply time by the migration runner.

-- Ops note (locks), per the interim guidance in issue #29:
--   * ADD COLUMN ... int (nullable, no default) is metadata-only: no table
--     rewrite; it takes ACCESS EXCLUSIVE on "attack2".queues for the
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
ALTER TABLE "attack2".queues ADD COLUMN IF NOT EXISTS max_concurrent int;
ALTER TABLE "attack2".queues ADD CONSTRAINT queues_max_concurrent_check
    CHECK (max_concurrent IS NULL OR max_concurrent >= 1);

-- Batches table: tracks batch lifecycle for enqueue_batch / wait_for_batch.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "attack2" token is substituted at apply time by the migration runner.

CREATE TABLE "attack2".batches (
    id                      uuid PRIMARY KEY,
    queue                   text NOT NULL,
    status                  text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'complete', 'aborted')),
    expected_size           int NOT NULL DEFAULT 0
        CHECK (expected_size >= 0),
    consecutive_failures    int NOT NULL DEFAULT 0
        CHECK (consecutive_failures >= 0),
    failure_threshold       int
        CHECK (failure_threshold IS NULL OR failure_threshold >= 1),
    finalizer_job_id        uuid,
    originating_actor       text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    completed_at            timestamptz,
    metadata                jsonb NOT NULL DEFAULT '{{}}'::jsonb
);

CREATE INDEX batches_queue_status_idx
    ON "attack2".batches (queue, status)
    WHERE status = 'active';

CREATE INDEX batches_finalizer_idx
    ON "attack2".batches (finalizer_job_id)
    WHERE finalizer_job_id IS NOT NULL;

COMMENT ON TABLE "attack2".batches IS
    'Tracks batch lifecycle for enqueue_batch / wait_for_batch. '
    'A batch is created active, transitions to complete or aborted when all '
    'member jobs resolve (or the failure threshold is exceeded).';

COMMENT ON COLUMN "attack2".batches.expected_size IS
    'Number of jobs enqueued in the batch; set at creation time and used to '
    'detect completion when completed_count equals expected_size.';

COMMENT ON COLUMN "attack2".batches.consecutive_failures IS
    'Running count of consecutive member-job failures; reset to 0 on each '
    'success. When it reaches failure_threshold the batch is auto-aborted '
    '(NULL failure_threshold means never auto-abort).';

-- Indexes for the bounded maintenance/cancel paths audited against a
-- realistically-seeded schema (93k jobs / 123k job_attempts / 150k
-- job_events on PostgreSQL 18, EXPLAIN (ANALYZE, BUFFERS); see the
-- verdict table in the audit trail). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "attack2"
-- token is substituted at apply time by the migration runner.
--
-- What this closes (measured, pre-index):
--   * cancel_where(queue=...) / list-by-queue: the matching CTE
--     (`WHERE queue = $1 AND status IN ('pending','scheduled')
--     ORDER BY id LIMIT $n`) had NO usable index — every partial index
--     on jobs with a queue key column is partial on
--     `status = 'pending'` alone, which cannot serve
--     `status IN ('pending','scheduled')`. The planner fell back to a
--     whole-table walk of jobs' PK in id order: ~2k buffers per batch
--     while dense, a full 93k-entry walk (~30-40 ms) for the drain's
--     final empty call, and a growing re-walk of already-cancelled
--     prefix rows across the drain (O(batches²) probes).
--   * cancel_where(actor=...) and deregister_actor's force-cancel drain
--     had the same shape: jobs_actor_pending_idx (actor) cannot
--     provide the `ORDER BY id` the CTE pins, so at realistic volume
--     the planner PK-walked the whole table instead of seeking the
--     actor's rows.
--   * cleanup_stale_workers' DDL fan-out: job_attempts.worker_id
--     REFERENCES workers(id) ON DELETE SET NULL, and with no index on
--     job_attempts(worker_id) the RI trigger ran a full seq scan of
--     job_attempts PER DELETED WORKER — 688 ms of trigger time to
--     delete 100 stale workers at 123k attempts (the fleet-crash
--     cleanup drains that in 4 batches per tick). With the index the
--     same delete measures 167 ms, the residual being the intrinsic
--     SET NULL rewrite of the ~208 attempt rows each worker references.
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- The migration runner's no-transaction directive + CREATE INDEX
-- CONCURRENTLY template (src/taskq/migrate.py's module docstring) was
-- tried for this file and REVERTED: it deadlocks under the runner's own
-- startup discipline. apply_pending_locked serializes concurrent
-- migrators with pg_advisory_lock (the wait shape
-- tests/test_migrate_lock_integration.py::
-- test_concurrent_migrations_serialize_rather_than_racing pins: two
-- replicas starting at once serialize on that lock rather than race —
-- exactly the scenario the lock exists for); a second replica's blocking
-- lock wait is an open transaction, and CREATE INDEX CONCURRENTLY waits
-- for every transaction that started before it — so a CIC build here
-- would wait on the replica's advisory-lock wait, which waits on the
-- first migrator's advisory lock: a cycle the deadlock detector breaks
-- by failing the apply. The cycle follows from CIC's documented
-- snapshot-wait behavior against the serialized-wait shape the test
-- pins. So this file follows 01.00.02_01_pre_job_events_outbox.sql's
-- precedent instead: a transactional plain CREATE INDEX, whose
-- ordinary locks queue behind the advisory-lock waiter without a
-- snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.02_01: each CREATE INDEX
-- below takes a write-blocking lock on its table for the duration of
-- the build (jobs is the hottest table in the system — enqueue,
-- dispatch, and heartbeat all write it). Build time scales with the
-- current row count; on a large, busy production jobs table this can
-- stall the worker fleet's writes for a noticeable window. Apply
-- during a maintenance window (or when jobs is small/quiescent, e.g.
-- right after a prune sweep) on any deployment where jobs is large.
-- The default event_writer_batch_size drain keeps steady-state jobs
-- small, so most deployments see momentary builds.

-- Bulk-cancel / list by queue, active rows only: the (queue, id) key
-- serves the cancel_where(queue=...) matching CTE's
-- `queue = $1 AND status IN ('pending','scheduled') ORDER BY id LIMIT`
-- as an Index Cond seek with id-ordered early termination (measured
-- post-index: 100 rows in ~0.1 ms / ~100 buffers dense; the drained
-- final call stops at the queue's boundary instead of walking the
-- table). Partial on the active statuses for the same reason as every
-- dispatch index: terminal rows would only bloat the key range and
-- every terminal write would otherwise pay index maintenance for a
-- filter that can never match them again.
CREATE INDEX IF NOT EXISTS jobs_queue_active_idx
    ON "attack2".jobs (queue, id)
    WHERE status IN ('pending', 'scheduled');

-- Bulk-cancel / deregister by actor, active rows only: (actor, id)
-- serves the `actor = $1 AND status IN ('pending','scheduled')
-- ORDER BY id LIMIT` CTEs in cancel_where(actor=...) and
-- deregister_actor's force-cancel drain as an actor-prefix seek in id
-- order (measured post-index: 100 rows in ~0.08-0.12 ms / ~100
-- buffers, vs a 1.9k-buffer whole-table PK walk before).
-- DELIBERATE overlap: jobs_actor_pending_idx (actor) — partial on the
-- same statuses — already serves the max_pending count probe and is
-- plan-pinned by tests/test_postgres_max_pending.py; this initiative
-- never drops structures, so both coexist. A future initiative may
-- consolidate to this index alone by re-pointing that pin (an
-- (actor, id) key serves `WHERE actor = $1` counts identically).
CREATE INDEX IF NOT EXISTS jobs_actor_active_id_idx
    ON "attack2".jobs (actor, id)
    WHERE status IN ('pending', 'scheduled');

-- cleanup_stale_workers' ON DELETE SET NULL fan-out: the
-- job_attempts_worker_id_fkey RI trigger probes
-- `worker_id = $1` once per deleted worker row; with no index that
-- probe is a full seq scan of job_attempts (measured 688 ms per 100
-- deleted workers at 123k attempts). Partial on
-- `worker_id IS NOT NULL` following the reservation_slots_job_id_idx
-- convention: equality against a non-null key implies the predicate,
-- so the RI trigger's parameterized plan uses it (verified with
-- EXPLAIN: Bitmap Index Scan, Index Cond worker_id = $1), and rows
-- leave the index as the SET NULL rewrites them — the index only ever
-- contains rows the trigger can still match.
CREATE INDEX IF NOT EXISTS job_attempts_worker_id_idx
    ON "attack2".job_attempts (worker_id)
    WHERE worker_id IS NOT NULL;

-- Partial index serving the job_events retention sweep's windowing CTE
-- (taskq/backend/_sweeps.py's _SWEEP_EVENT_TTL_SQL): the sweep selects
-- `occurred_at < statement_timestamp() - retention`, ordered by
-- (occurred_at, id), LIMIT one batch, over the deletable set only, and
-- this index serves that shape as an ordered Index Scan whose Index Cond
-- stops at the age boundary. Forward-only; there is no down migration. To
-- revert, restore from backup. The literal "attack2" token is
-- substituted at apply time by the migration runner.
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- The migration runner's no-transaction directive + CREATE INDEX
-- CONCURRENTLY template (src/taskq/migrate.py's module docstring) cannot
-- ship here: it deadlocks under the runner's own startup discipline.
-- apply_pending_locked serializes concurrent migrators with
-- pg_advisory_lock (the wait shape
-- tests/test_migrate_lock_integration.py::
-- test_concurrent_migrations_serialize_rather_than_racing pins: two
-- replicas starting at once serialize on that lock rather than race —
-- exactly the scenario the lock exists for); a second replica's blocking
-- lock wait is an open transaction, and CREATE INDEX CONCURRENTLY waits
-- for every transaction that started before it — so a CIC build here
-- waits on the replica's advisory-lock wait, which waits on the first
-- migrator's advisory lock: a cycle the deadlock detector breaks by
-- failing the apply (reproduced against this very migration: the
-- concurrent-migrators test fails with "deadlock detected" under the CIC
-- form and passes under this one). This file therefore follows
-- 01.00.02_01_pre_job_events_outbox.sql's precedent: a transactional
-- plain CREATE INDEX, whose ordinary locks queue behind the
-- advisory-lock waiter without a snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.02_01: the CREATE INDEX below
-- takes a write-blocking lock on job_events for the duration of the
-- build. job_events is written on essentially every lifecycle transition
-- and progress event, so the lock can cause observable write stalls in
-- production. Build time is proportional to the current row count.
-- Operators with a large or heavily-populated job_events table should
-- run the equivalent `CREATE INDEX CONCURRENTLY IF NOT EXISTS
-- job_events_occurred_at_idx ON "attack2".job_events (occurred_at, id)
-- WHERE NOT (kind = 'state_change' AND COALESCE(detail->>'reason', '')
-- = 'lock_expired')` manually outside the migration runner during a
-- maintenance window, then let this migration no-op via IF NOT EXISTS.
--
-- ── Why partial ────────────────────────────────────────────────────
-- The index covers only the DELETABLE set — everything except the
-- crash-reclaim outbox slice (`kind = 'state_change' AND
-- COALESCE(detail->>'reason', '') = 'lock_expired'`), the rows
-- poll_reclaim_events tails under its trailing-watermark protocol and
-- which the retention sweep exempts at every age. Outbox rows are
-- immortal under the sweep, so a full index would carry them forever and
-- every sweep tick's ordered scan would pay for their accumulating
-- volume; the partial form keeps per-tick cost independent of it.
-- detail->> is jsonb_extract_path_text, IMMUTABLE, so the predicate is
-- legal in an index. COALESCE makes a missing reason key
-- not-'lock_expired' (deletable): a bare (detail->>'reason') =
-- 'lock_expired' under NOT evaluates to NULL for ordinary state_change
-- rows — the most common event kind — which would silently exempt nearly
-- the whole table from both the index and the sweep. The WHERE clause
-- below must stay VERBATIM-identical to the sweep SQL's carve-out
-- predicate (taskq/backend/_sweeps.py's _SWEEP_EVENT_TTL_SQL): a partial
-- index serves a query only when the planner can prove the query implies
-- the index predicate, and a verbatim repeat of the predicate is that
-- proof.
CREATE INDEX IF NOT EXISTS job_events_occurred_at_idx
    ON "attack2".job_events (occurred_at, id)
    WHERE NOT (kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired');

-- Denial/snooze counters on the job row, and a floor check on
-- max_attempts. Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "attack2" token is substituted at
-- apply time by the migration runner.
--
-- A reservation/rate-limit denial is admission control, not an
-- execution, and a Snooze / RetryAfter(consume_budget=False) is a
-- voluntary deferral, not an execution — neither consumes retry budget,
-- and neither writes a job_attempts/job_events row. The durable record
-- of "how many times was this job deferred or refused admission" is a
-- pair of coalesced counters on the job row: O(1) storage in the number
-- of denials, readable by the admin surface in one row read, and
-- reclaimed with the row itself when the prune sweep archives it.
-- int (not smallint): a denial counter is genuinely unbounded in a way
-- attempt is not — a job can be denied admission indefinitely without
-- ever consuming budget, and the smallint ceiling is exactly the domain
-- the removed max_attempts increment used to walk into.
--
-- max_attempts stays a fixed ceiling: nothing in the non-consuming
-- paths raises it, and the check constraint pins the floor the policy
-- layer already enforces (RetryPolicy refuses max_attempts < 1) at the
-- storage boundary, so no writer — present or hand-rolled — can persist
-- a budget of zero. Constraint name follows the {{table}}_{{column}}_check
-- convention Postgres auto-generates for the unnamed inline CHECKs in
-- 01.00.00_01_pre_initial.sql (jobs_cancel_phase_check,
-- jobs_retry_kind_check). Postgres has no ADD CONSTRAINT IF NOT EXISTS;
-- the DO block below makes re-running idempotent, the same
-- duplicate_object-swallowing shape a partial re-apply needs.
--
-- OPS NOTE (locks): ALTER TABLE ... ADD COLUMN with a non-volatile
-- default is metadata-only on PG >= 11 (no table rewrite; the default
-- is stored in pg_attribute and read from there), so the column adds
-- themselves do not rewrite anything. The CHECK constraint is added in
-- two phases: NOT VALID takes only a brief ACCESS EXCLUSIVE (the
-- constraint applies to every new write immediately) and VALIDATE then
-- scans the existing rows under the weaker SHARE UPDATE EXCLUSIVE lock,
-- which does not block concurrent reads or writes — on a large jobs
-- table the validation scan therefore does not stall the fleet the way
-- a validated ADD CONSTRAINT's exclusive scan would.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- previous release's snooze statements reference only columns that
-- still exist (they simply keep widening max_attempts and writing their
-- per-occurrence rows until the new code takes over); this release's
-- statements require these counters, which is why the columns ship in
-- the pre phase, before the code rollout.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS snooze_count int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rate_limit_blocked_count int NOT NULL DEFAULT 0;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and the explicit column lists in src/taskq/worker/_leader_shared.py,
-- which pick the new columns up from COPY_FROM_COLUMNS), so the prune
-- sweep's archive INSERT carries the counters into the archive without a
-- column-count break.
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS snooze_count int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rate_limit_blocked_count int NOT NULL DEFAULT 0;

DO $$
BEGIN
    ALTER TABLE "attack2".jobs
        ADD CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;  -- constraint already present; idempotent re-apply
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_max_attempts_check'
          AND conrelid = '"attack2".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "attack2".jobs VALIDATE CONSTRAINT jobs_max_attempts_check;
    END IF;
END
$$;

COMMENT ON COLUMN "attack2".jobs.snooze_count IS
    'Coalesced count of non-consuming deferrals (Snooze, RetryAfter(consume_budget=False)) '
    'since enqueue. A deferral consumes no retry budget and writes no job_attempts/'
    'job_events rows; this counter is its durable record.';

COMMENT ON COLUMN "attack2".jobs.rate_limit_blocked_count IS
    'Coalesced count of admission denials (reservation/rate-limit) since enqueue. '
    'A denial is backpressure, not an execution: no budget consumed, no per-occurrence '
    'rows; this counter plus OTEL carry it.';

-- Drop jobs_actor_fairness_dispatch_idx now that
-- 01.00.09_01_pre_round_robin_probe_index.sql has taken over serving the
-- round-robin dispatch path. Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "attack2" token is
-- substituted at apply time by the migration runner.
--
-- DO NOT apply this migration (`taskq migrate up --phase post`, or a
-- plain `taskq migrate up` once the pre phase is already applied) until
-- every worker in the fleet is confirmed running the release that
-- shipped 01.00.09_01_pre_round_robin_probe_index.sql. Pre-that-release
-- code still runs the shipped round-robin candidates lateral — the
-- ROW_NUMBER window over EVERY due pending row of the (actor, queue)
-- pair — and that window reads this index for its pre-sorted
-- per-partition input (01.00.00_01's index comment documents exactly
-- that contract). Dropping the index under the old code does not break
-- correctness (the window sorts its input either way) but removes the
-- pre-sorted plan from a query that is already O(backlog depth) per
-- round — the defect issue #130 filed — so an old-generation worker
-- would pay the depth cost AND a full sort on top during the overlap
-- window. The migration runner additionally refuses to apply a post
-- migration before its same-version pre counterpart (see apply_pending
-- in src/taskq/migrate.py), so `taskq migrate up --phase post` cannot
-- drop this index before its replacement exists.
--
-- The new release has no consumer for this index: the depth-bounded
-- round-robin lateral probes cohorts through the COALESCE expression
-- index (its probes cannot use a bare fairness_key column position for
-- the NULL cohort — see _dispatch_sql.py's
-- _ROUND_ROBIN_CANDIDATES_LATERAL comment), and no other statement in
-- src/ references jobs_actor_fairness_dispatch_idx. Keeping it would
-- be pure write amplification: every enqueue and every pending↔running
-- status flip on the jobs hot path maintains an index nothing reads.
DROP INDEX IF EXISTS "attack2".jobs_actor_fairness_dispatch_idx;

-- Round-robin dispatch probe index: the expression index serving the
-- per-cohort candidate probes and the rr_keys cohort enumeration in
-- taskq/backend/_dispatch_sql.py's round-robin CTE (issue #130's
-- depth-bounded dispatch geometry). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "attack2"
-- token is substituted at apply time by the migration runner.
--
-- What it serves (see _dispatch_sql.py's module docstring for the full
-- depth-bounding doctrine):
--   * the candidates lateral's per-cohort probe: actor/queue equality
--     plus COALESCE(fairness_key, '__null__') equality is a full
--     three-column Index Cond prefix, the index's own
--     (priority DESC, scheduled_at, id) order serves the probe's
--     ORDER BY without a sort, and the STABLE scheduled_at bound is an
--     index-level condition — so each probe is an ordered scan that
--     stops at its LIMIT (residual * oversample rows), regardless of
--     how deep the cohort behind that key is;
--   * the rr_keys recursive enumeration: each step's
--     (actor, queue, COALESCE(fairness_key, '__null__')) > (...)
--     row comparison is an Index Cond on exactly these leading
--     columns, one bounded seek per DISTINCT cohort — the loose index
--     scan this Postgres generation has no native skip scan for.
--
-- The COALESCE(fairness_key, '__null__') expression is IMMUTABLE and
-- must stay VERBATIM-identical to every use in the dispatch SQL (the
-- probe equality, the rr_keys walk, and the window PARTITION BY): an
-- expression index serves a query only when the query carries the
-- identical expression, and that shared expression is also what folds
-- every unkeyed job into ONE cohort with a job literally keyed
-- '__null__' — the partition identity the shipped round-robin window
-- already used (PARTITION BY COALESCE(fairness_key, '__null__')).
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- The migration runner's no-transaction directive + CREATE INDEX
-- CONCURRENTLY template (src/taskq/migrate.py's module docstring)
-- cannot ship here: it deadlocks under the runner's own startup
-- discipline. apply_pending_locked serializes concurrent migrators
-- with pg_advisory_lock, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it — so a CIC build here waits on
-- the second replica's advisory-lock wait, which waits on the first
-- migrator's advisory lock: a cycle the deadlock detector breaks by
-- failing the apply. This file follows the 01.00.02_01 /
-- 01.00.07_01 precedent: a transactional plain CREATE INDEX, whose
-- ordinary locks queue behind the advisory-lock waiter without a
-- snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.02_01/01.00.07_01: the
-- CREATE INDEX below takes a write-blocking lock on jobs for the
-- duration of the build, and build time is proportional to the
-- current pending-row count. Operators with a large or heavily
-- backlogged jobs table should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_round_robin_probe_idx
-- ON "attack2".jobs (actor, queue, COALESCE(fairness_key,
-- '__null__'), priority DESC, scheduled_at, id)
-- WHERE status = 'pending'` manually outside the migration runner
-- during a maintenance window, then let this migration no-op via
-- IF NOT EXISTS.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- index is purely additive — the previous release's round-robin CTE
-- never references it (its window scans the whole due set on
-- jobs_actor_fairness_dispatch_idx), and this release's strict-FIFO
-- CTE never references it either (its lateral rides
-- jobs_actor_dispatch_idx). Only the new release's round-robin CTE
-- depends on it, which is why the index ships in the pre phase,
-- before the code rollout.
CREATE INDEX IF NOT EXISTS jobs_round_robin_probe_idx
    ON "attack2".jobs (actor, queue, COALESCE(fairness_key, '__null__'),
                        priority DESC, scheduled_at, id)
    WHERE status = 'pending';

-- Partial index serving the reclaim sweep's heartbeat arm (the per-job
-- heartbeat_timeout disjunct in taskq/backend/_sweeps.py's _SWEEP_1_SQL):
-- the arm selects running, heartbeat-configured rows whose holder has
-- been silent past the row's deadline, ordered by last_heartbeat_at,
-- LIMIT one batch, and this index serves that shape as an ordered scan
-- over ONLY the heartbeat-configured running set. Forward-only; there is
-- no down migration. To revert, restore from backup. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- Same deadlock shape as 01.00.07_01_pre_event_retention_index.sql (see
-- that file's full derivation): the migration runner serializes
-- concurrent migrators with pg_advisory_lock, a second replica's
-- blocking lock wait is an open transaction, and CREATE INDEX
-- CONCURRENTLY waits for every transaction that started before it — a
-- cycle the deadlock detector breaks by failing the apply. This file
-- therefore follows 01.00.02_01 / 01.00.07_01's precedent: a
-- transactional plain CREATE INDEX, whose ordinary locks queue behind
-- the advisory-lock waiter without a snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.02_01 / 01.00.07_01: the
-- CREATE INDEX below takes a write-blocking lock on jobs for the
-- duration of the build; build time is proportional to the current row
-- count. Operators with a large jobs table should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS
-- jobs_running_heartbeat_deadline_idx ON "attack2".jobs
-- (last_heartbeat_at) WHERE status = 'running' AND heartbeat_timeout IS
-- NOT NULL` manually outside the migration runner during a maintenance
-- window, then let this migration no-op via IF NOT EXISTS.
--
-- ── Why partial ────────────────────────────────────────────────────
-- The index covers only the rows the heartbeat arm can ever visit:
-- running jobs that carry a heartbeat_timeout. A fleet that never sets
-- the knob has an empty index (every sweep tick touches it at
-- buffer-scale, the same empty steady state the lease arm's index
-- has); a fleet that does set it pays a per-tick scan proportional to
-- its heartbeat-configured running set, never to the running set at
-- large. The index key is last_heartbeat_at because the row-exact
-- deadline (last_heartbeat_at + heartbeat_timeout) cannot be an index
-- condition at all: the bound is row-dependent (per-job heartbeat_timeout
-- varies per row), and timestamptz + interval is STABLE in Postgres, so
-- no expression index may exist on it. The arm's SQL therefore states
-- the necessary condition (last_heartbeat_at < statement_timestamp())
-- explicitly as the range bound, ORDER BY last_heartbeat_at pins the
-- scan to this index's key order, and the row-exact deadline rides as
-- a filter. The WHERE clause below must stay VERBATIM-identical to the
-- arm's `status = 'running' AND heartbeat_timeout IS NOT NULL`
-- conjuncts (taskq/backend/_sweeps.py's _SWEEP_1_SQL): a partial index
-- serves a query only when the planner can prove the query implies the
-- index predicate, and a verbatim repeat of the predicate is that
-- proof.
CREATE INDEX IF NOT EXISTS jobs_running_heartbeat_deadline_idx
    ON "attack2".jobs (last_heartbeat_at)
    WHERE status = 'running' AND heartbeat_timeout IS NOT NULL;

-- Fleet-reclaim marking for keyed rate-limit / reservation rows (the
-- #139 residual). Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "attack2" token is substituted at
-- apply time by the migration runner.
--
-- THE RESIDUAL: keyed reservation_slots rows (materialised by
-- KeyedReservationRef via ensure_slots) and keyed rate_limit_buckets
-- rows (published / preseeded by KeyedRateLimitRef materialisation)
-- orphan when the worker that created them DIES — the in-process
-- reclamation machinery (registry idle eviction + the pending-reclaim
-- drain) lives entirely inside the dead process, so no survivor can
-- name the rows, and they leak unless a live worker happens to
-- re-resolve the same concrete key. The key space is caller-controlled,
-- so steady-state cardinality after enough worker deaths is one bucket
-- per key ever materialised by a process that later died — unbounded.
--
-- THE SHAPE (vendor/solid_queue's Semaphore): per-key rows carry their
-- own staleness. Semaphore#attempt_decrement / attempt_increment
-- refresh expires_at inside the very UPDATE that changes the value, and
-- Dispatcher::Maintenance#expire_semaphores then runs a bounded
-- delete_all over the expired scope — refresh-on-use plus a bounded
-- expiry sweep, instead of registry bookkeeping that dies with the
-- process. This migration adds the two row-borne halves of that shape:
--
--   keyed         marks the rows the fleet sweep may delete. True ONLY
--                 on rows created by a keyed materialisation: a
--                 statically declared bucket's rows are born false and
--                 must NEVER be flipped true (a static reservation has
--                 no acquire-path heal, so deleted rows would deny
--                 forever). For rate_limit_buckets the mark is
--                 additionally restricted to PG-state-backed buckets
--                 (backend="postgres"): a redis-backend keyed bucket's
--                 healthy acquire path never touches PG, so its PG
--                 row's staleness cannot speak for Redis-side use, and
--                 sweeping it would reset the outage-fallback state of
--                 a fixed-quota bucket mid-outage. Redis-backend keyed
--                 rows therefore stay false — never swept.
--
--   last_used_at  the staleness stamp, refreshed by the acquire /
--                 release / upsert statements that already touch the
--                 row (no dedicated stamping round trip exists or is
--                 needed): reservation acquire's UPDATE arm and
--                 release, ensure_slots' INSERT, the token bucket's
--                 preseed / upsert / refund UPDATE, and the keyed
--                 materialisation publish. The maintenance leader's
--                 sweep_idle_keyed_rows deletes keyed rows whose stamp
--                 is older than the operator horizon
--                 (WorkerSettings.keyed_row_reclaim_period, default 1
--                 hour — the same idle threshold the in-process
--                 registry eviction uses), one bounded, committed batch
--                 per tick per table.
--
-- OPS NOTE (locks): every ALTER here is ADD COLUMN with a non-volatile
-- default — metadata-only on PG >= 11 (no table rewrite; the default is
-- stored in pg_attribute, and pre-existing rows read it as a fixed
-- value stamped at ALTER time) — and plain CREATE INDEX (the deliberate
-- non-CONCURRENTLY choice of 01.00.06: the CONCURRENTLY form deadlocks
-- with the migration advisory lock; see that migration's header).
-- reservation_slots and rate_limit_buckets are bounded tables (static
-- declarations plus the per-worker keyed caps), so both lock windows
-- are short. Pre-existing rows are irrelevant to the sweep either way:
-- they read keyed=false (the constant default) and are therefore never
-- deleted, whatever their last_used_at shows.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- previous release's statements reference only columns that still
-- exist; this release's acquire/release/upsert statements list the new
-- columns, which is why they ship in the pre phase, before the code
-- rollout. A pre-migration database makes the new leader-sweep block
-- raise UndefinedColumnError, which that block tolerates per tick (the
-- stale-batches block's pre-migration pattern) until this migration
-- lands.
ALTER TABLE "attack2".reservation_slots
    ADD COLUMN IF NOT EXISTS keyed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_used_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE "attack2".rate_limit_buckets
    ADD COLUMN IF NOT EXISTS keyed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_used_at timestamptz NOT NULL DEFAULT now();

-- The fleet sweep's window scans. Partial on keyed: the sweep's
-- candidate predicate is exactly (keyed AND last_used_at < horizon),
-- and the STABLE statement_timestamp() bound (the module docstring's
-- two-clock doctrine in taskq.backend._sweeps — write stamps are
-- VOLATILE clock_timestamp(), selection bounds are STABLE) lets the
-- planner serve the bound as an Index Cond instead of a post-scan
-- filter over every keyed row.
CREATE INDEX IF NOT EXISTS reservation_slots_keyed_last_used_idx
    ON "attack2".reservation_slots (last_used_at)
    WHERE keyed;
CREATE INDEX IF NOT EXISTS rate_limit_buckets_keyed_last_used_idx
    ON "attack2".rate_limit_buckets (last_used_at)
    WHERE keyed;

COMMENT ON COLUMN "attack2".reservation_slots.keyed IS
    'Fleet-reclaimable mark: the row was created by a keyed materialisation '
    '(KeyedReservationRef) whose owning registry entry may not outlive the '
    'process. The maintenance leader''s sweep_idle_keyed_rows may delete '
    'keyed rows unused past keyed_row_reclaim_period; statically declared '
    'buckets are born false and are never deleted by it.';
COMMENT ON COLUMN "attack2".reservation_slots.last_used_at IS
    'Staleness stamp for the fleet reclaim sweep, refreshed by the acquire / '
    'release / ensure_slots statements that already touch the row.';
COMMENT ON COLUMN "attack2".rate_limit_buckets.keyed IS
    'Fleet-reclaimable mark: keyed-materialised AND PG-state-backed '
    '(backend="postgres" — only then does the acquire path touch this row, '
    'keeping last_used_at truthful). Static buckets and redis-backend keyed '
    'buckets are born false and are never deleted by the fleet sweep.';
COMMENT ON COLUMN "attack2".rate_limit_buckets.last_used_at IS
    'Staleness stamp for the fleet reclaim sweep, refreshed by the preseed / '
    'upsert / refund statements that already touch the row.';

-- Row-borne lease for maintenance leadership. Leadership is held by the pod
-- whose worker_id is on this row while expires_at is in the future, renewed
-- by that pod on its heartbeat cadence and taken over by any pod once it has
-- lapsed -- on a horizon the leader itself wrote, never on the server's
-- connection-reaping schedule, and needing no privilege beyond UPDATE on this
-- row. Forward-only; there is no down migration. The literal "attack2" token
-- is substituted at apply time by the migration runner.
--
-- The expiry is stored rather than derived from last_seen_at because each
-- follower would otherwise compute the horizon from its OWN heartbeat
-- interval: a fleet whose new pods run a longer interval than the leader's
-- renewal cadence would take over from a live leader. Storing the instant the
-- holder chose leaves followers only a clock comparison.
--
-- Additive and rolling-safe: pre-lease code never reads or writes this column
-- (its upsert names singleton, worker_id, elected_at and last_seen_at, and its
-- heartbeat ping touches last_seen_at only), so it can be applied while such
-- pods are running. That also means a pre-lease pod taking the row leaves
-- whatever expiry it found in place rather than clearing it, so the column
-- cannot by itself say who wrote the row: NULL only until the first leasing
-- pod elects, and stale-but-non-NULL afterwards. The election predicate
-- therefore never judges a holder on this column alone -- it requires the
-- last_seen_at ping to have stopped as well, which is the one signal every
-- release writes.
ALTER TABLE "attack2".maintenance_leader
    ADD COLUMN IF NOT EXISTS expires_at timestamptz;

COMMENT ON COLUMN "attack2".maintenance_leader.expires_at IS
    'Server-clock instant after which any pod may take leadership. Written '
    'and renewed by the holder from its own leader_lease setting.';

-- Interruption counter for jobs whose running attempt was released by the
-- worker process going away (a graceful shutdown that outlasted its grace
-- windows). The same non-consuming-release family as snooze_count /
-- rate_limit_blocked_count (01.00.08_01): the claim's attempt increment is
-- refunded on release, and the durable record of "how often was this job
-- interrupted by infrastructure" is a coalesced counter on the job row —
-- O(1) storage, one row read for the admin surface, reclaimed with the row
-- itself when the prune sweep archives it. Forward-only; there is no down
-- migration. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- An interruption writes NO job_attempts row (it is not an execution
-- outcome) and exactly one job_events state_change with
-- detail.reason = 'interrupted'; the counter is the aggregate over those.
--
-- OPS NOTE (locks): ALTER TABLE ... ADD COLUMN with a non-volatile default
-- is metadata-only on PG >= 11 (no table rewrite; the default is stored in
-- pg_attribute and read from there), so these adds do not stall a fleet.
--
-- ROLLING DEPLOY: additive with a default, so it can be applied while
-- old pods run: the previous release's INSERT/UPDATE statements name their
-- columns explicitly and never read this one, and this release's prune
-- sweep archive INSERT picks it up through COPY_FROM_COLUMNS, which is why
-- jobs_archive gains it in the same file.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;

COMMENT ON COLUMN "attack2".jobs.interrupt_count IS
    'Times a running attempt of this job was released back to the queue by a '
    'worker shutdown, with the claim''s attempt increment refunded. Counted on '
    'the row; each release also writes one job_events state_change with '
    'detail.reason = ''interrupted''.';

-- Per-job retry-curve columns for crash/heartbeat reclaim. Forward-only;
-- there is no down migration. To revert, restore from backup. The
-- literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- Every other path that reschedules a job for another attempt derives
-- the delay from the actor's RetryPolicy (base, cap, backoff kind,
-- jitter) via taskq.retry.compute_backoff: the consumer's failure path
-- does, the Retry-After override does, the snooze arms do. Crash and
-- heartbeat reclaim (taskq/backend/_sweeps.py's _SWEEP_1_SQL, mirrored
-- in taskq/worker/heartbeat.py's isolate_self) could not, because
-- RetryPolicy lives in the worker process's actor registration and the
-- reclaim SQL runs entirely on a leader that need not host the crashed
-- job's actor at all. These columns give the leader a source for the
-- curve without a registry lookup: the enqueuing client (the one place
-- that always holds the actor's live ActorRef) stamps the policy's
-- scalars onto the row at enqueue time, exactly as it already stamps
-- max_attempts and retry_kind (taskq/client/_args.py).
--
-- Defaults reproduce taskq.retry.RetryPolicy's own field defaults, so a
-- column left at its default behaves exactly as an actor registered
-- with no explicit `retry=RetryPolicy(...)` literal would. The COPY-based
-- fast batch enqueue path (enqueue_batch_fast) does not stamp these
-- columns explicitly and rides the defaults — reclaim on a fast-batched
-- job spreads on the default curve rather than a divergent per-actor
-- one, a fallback no worse than the flat constant it replaces.
--
-- retry_backoff mirrors RetryPolicy.backoff's Literal domain
-- ('exponential', 'linear', 'fixed') under the same CHECK-constraint
-- convention retry_kind and cancel_phase already use in
-- 01.00.00_01_pre_initial.sql.
--
-- OPS NOTE (locks): ALTER TABLE ... ADD COLUMN with a non-volatile
-- default is metadata-only on PG >= 11 (no table rewrite; the default
-- is stored in pg_attribute and read from there), so the column adds
-- themselves do not rewrite anything. The CHECK constraint is added in
-- two phases, same discipline as 01.00.08_01_pre_denial_counters.sql:
-- NOT VALID takes only a brief ACCESS EXCLUSIVE and VALIDATE then scans
-- existing rows under SHARE UPDATE EXCLUSIVE, which does not block
-- concurrent reads or writes.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- previous release's reclaim statements reference only columns that
-- still exist and keep using the flat constant; this release's
-- statements read these columns, which is why they ship in the pre
-- phase, before the code rollout.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS retry_base_seconds double precision NOT NULL DEFAULT 5.0,
    ADD COLUMN IF NOT EXISTS retry_cap_seconds double precision NOT NULL DEFAULT 3600.0,
    ADD COLUMN IF NOT EXISTS retry_backoff text NOT NULL DEFAULT 'exponential',
    ADD COLUMN IF NOT EXISTS retry_jitter double precision NOT NULL DEFAULT 0.2;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and 01.00.08_01_pre_denial_counters.sql's precedent), so the prune
-- sweep's archive INSERT carries these columns into the archive without
-- a column-count break.
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS retry_base_seconds double precision NOT NULL DEFAULT 5.0,
    ADD COLUMN IF NOT EXISTS retry_cap_seconds double precision NOT NULL DEFAULT 3600.0,
    ADD COLUMN IF NOT EXISTS retry_backoff text NOT NULL DEFAULT 'exponential',
    ADD COLUMN IF NOT EXISTS retry_jitter double precision NOT NULL DEFAULT 0.2;

DO $$
BEGIN
    ALTER TABLE "attack2".jobs
        ADD CONSTRAINT jobs_retry_backoff_check
        CHECK (retry_backoff IN ('exponential', 'linear', 'fixed')) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;  -- constraint already present; idempotent re-apply
END
$$;

DO $$
BEGIN
    ALTER TABLE "attack2".jobs
        ADD CONSTRAINT jobs_retry_base_cap_check
        CHECK (retry_cap_seconds >= retry_base_seconds) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    ALTER TABLE "attack2".jobs
        ADD CONSTRAINT jobs_retry_jitter_check
        CHECK (retry_jitter >= 0.0 AND retry_jitter <= 1.0) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_retry_backoff_check'
          AND conrelid = '"attack2".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "attack2".jobs VALIDATE CONSTRAINT jobs_retry_backoff_check;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_retry_base_cap_check'
          AND conrelid = '"attack2".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "attack2".jobs VALIDATE CONSTRAINT jobs_retry_base_cap_check;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_retry_jitter_check'
          AND conrelid = '"attack2".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "attack2".jobs VALIDATE CONSTRAINT jobs_retry_jitter_check;
    END IF;
END
$$;

COMMENT ON COLUMN "attack2".jobs.retry_base_seconds IS
    'RetryPolicy.base (seconds) at enqueue time, stamped by the client from the '
    'actor''s live registration. The reclaim sweep and heartbeat isolate read this '
    'to compute the same backoff curve an application-level failure would, instead '
    'of a hardcoded flat interval.';

COMMENT ON COLUMN "attack2".jobs.retry_cap_seconds IS
    'RetryPolicy.cap (seconds) at enqueue time. Bounds the reclaim backoff the same '
    'way it bounds every other retry path''s delay.';

COMMENT ON COLUMN "attack2".jobs.retry_backoff IS
    'RetryPolicy.backoff (exponential/linear/fixed) at enqueue time, read by the '
    'reclaim sweep to select the same curve shape compute_backoff would apply.';

COMMENT ON COLUMN "attack2".jobs.retry_jitter IS
    'RetryPolicy.jitter at enqueue time. A fleet-wide reclaim event spreads the '
    'whole reclaimed cohort across this band instead of stamping one synchronized '
    'instant.';

-- The event-retention sweep's outbox age-cap arm. The crash-reclaim
-- outbox slice is exempt from ordinary retention so a lagging consumer's
-- watermark can still reach it, and that arm is the only thing that
-- bounds the slice. job_events_reclaim_idx is keyed on id alone (its
-- key order serves the poll's `id > cursor` cursor and must not change),
-- so the arm's occurred_at bound survives only as a post-scan Filter:
-- every tick walks the entire unconsumed outbox population and discards
-- all of it. In a fleet whose reclaim consumer lags or was never stood
-- up that population grows for the life of the deployment, so the tick
-- meant to bound the outbox is itself unbounded in the outbox's size —
-- and the steady-state tick that deletes nothing gets slower forever
-- until the statement timeout fires and event retention silently stops.
--
-- Keyed (occurred_at, id) under the SAME partial predicate: the age
-- bound becomes an Index Cond that stops at the boundary, and id trails
-- it so the arm's ordered drain stays index-served. The predicate is
-- VERBATIM the arm's carve-out (same literals, same parentheses) —
-- partial-index predicate matching requires the query to repeat the
-- index's predicate, and the verbatim repeat is the proof the planner
-- matches it.
--
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- Not CONCURRENTLY here: apply_pending wraps every migration in a
-- transaction and Postgres forbids CREATE INDEX CONCURRENTLY inside a
-- transaction block. Operators with a large job_events table should run
-- the equivalent CREATE INDEX CONCURRENTLY IF NOT EXISTS statement
-- manually during a maintenance window; the IF NOT EXISTS makes this
-- migration then no-op.
CREATE INDEX IF NOT EXISTS job_events_reclaim_age_idx
    ON "attack2".job_events (occurred_at, id)
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired';

-- The assignment-routed marker columns: the durable flag that tells a
-- re-pended row apart from a producer-placed one, replacing the started_at
-- proxy the dispatch CTE's assignment-routed arm used for the same
-- question. Forward-only; there is no down migration. To revert, restore
-- from backup. The literal "attack2" token is substituted at apply time
-- by the migration runner.
--
-- ── Why a marker column and not started_at ────────────────────────────
-- The routing question the dispatch arm actually asks is about ORIGIN:
-- was this pending row placed here by a producer, or handed back by a
-- re-pend? Producer placement governs its own routing (an explicit
-- enqueue(queue=...) keeps its queue, and a stale producer's post-move
-- enqueue to a retired source queue stays served by that queue's
-- consumers); a re-pend routes by the actor's CURRENT stored assignment,
-- so a move's left-behind tails are not stranded on a queue the operator
-- was told to stop consuming.
--
-- started_at answered that question only by proxy, "was claimed at least
-- once", and the proxy is wrong in one direction that matters: an
-- operator retry of a job that was terminalized BEFORE it was ever
-- claimed is a re-pend (an operator hands the row back deliberately) but
-- has started_at IS NULL, so it routed by its stale label and stranded
-- permanently: pending, due, and invisible to every running consumer.
-- Widening the arm to all pending rows is not the fix: that would route
-- producer-placed strays by the assignment too, collapsing the stray
-- contract the never-claimed arm exists to hold. The marker names the
-- distinction directly instead of inferring it, so each population is
-- exactly what its arm means.
--
-- started_at keeps its own meaning intact (the audit trail of whether the
-- job ever ran), which the proxy was quietly overloading.
--
-- ── Why the ALTERs live alone in this file ────────────────────────────
-- ADD COLUMN takes ACCESS EXCLUSIVE on the table: readers and writers
-- alike queue behind it, and every lock a transaction takes is held to
-- its COMMIT, so the lock's cost is the cost of everything else the
-- same transaction runs after the ALTER, not of the ALTER itself. On
-- this Postgres generation the ALTER is metadata-only (a non-volatile
-- DEFAULT is stored in the catalog rather than rewriting the table, so
-- the statement itself is milliseconds and its cost does not scale with
-- the jobs backlog). Keeping it that way is why this file contains
-- NOTHING else: the backfill UPDATE and the probe-index builds this
-- round originally shared the ALTER's transaction, holding ACCESS
-- EXCLUSIVE on jobs across a backlog-sized UPDATE and two full-table
-- index builds, long enough for every read, heartbeat and claim on the
-- table to queue behind it, outliving the workers' heartbeat budget and
-- self-terminating a live fleet mid-upgrade (issue #250). They now run
-- as their own migrations, each with the narrowest lock its work allows:
-- the backfill as 01.00.12_07 (ROW EXCLUSIVE: blocks neither readers
-- nor other writers) and the index builds as 01.00.12_08 (SHARE:
-- blocks writes, never reads; that file's builds share its ONE
-- transaction, so its write-block window is the sum of both builds and
-- the writes queued behind them drain when the FILE commits, before
-- the next file asks for the table).
-- The ACCESS EXCLUSIVE window here is exactly the two catalog writes
-- plus the commit.
--
-- The runner's ddl_lock_timeout (30 s by default) bounds how long these
-- ALTERs WAIT for the table lock if a concurrent session holds one; it
-- never interrupts a lock already held. The hold is what the split
-- above bounds.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS assignment_routed boolean NOT NULL DEFAULT false;

-- The archive mirrors jobs column-for-column. The archive move itself
-- does not carry this column: an archived row is terminal and never
-- dispatched again, so the routing marker is inert there and the DDL
-- default below is the correct value for every archived row.
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS assignment_routed boolean NOT NULL DEFAULT false;

-- Tag indexes restricted to the bulk-cancel drain's two live windows.
-- Forward-only; there is no down migration. To revert, DROP INDEX. The
-- literal "attack2" token is substituted at apply time by the migration
-- runner.
--
-- The drain (src/taskq/backend/_cancel_bulk.py) pages its match set with
-- a keyset cursor: "the next batch_size matching rows with id > cursor".
-- A tag filter can be answered from jobs_tags_gin_idx — but that index
-- holds every tagged row in EVERY status, so its posting list keeps
-- every row the drain has ever moved out of the window: a cancelled
-- match is still in the bitmap, fetched from the heap, and discarded by
-- the status qual on every batch of every later drain of the same tag.
-- The shape that pays is the drain's own resume contract: a re-run after
-- a mid-drain failure re-enters at cursor zero against a posting list
-- holding the entire first run's cancelled rows, and on a fleet-scale
-- tenant that per-batch re-fetch grows until the batch trips its
-- statement_timeout — on exactly the deep-history backlogs the re-run
-- exists to finish.
--
-- This is not visible at the suite's table sizes: measured on
-- postgres:18-alpine (EXPLAIN (ANALYZE, BUFFERS)), the keyset drain's
-- tag-filtered windows are already served dead-row-free at hundreds of
-- rows by the keyed (queue, id) / (actor, id) active-row indexes from
-- 01.00.06_01 and by jobs_tags_gin_idx itself. The indexes below earn
-- their place only where the same-tag dead population dwarfs the live
-- one AND the tag bitmap wins the plan race — the sparse-live,
-- deep-history resume — where their posting lists track the live match
-- set: a row leaves these indexes in the same transaction that moves it
-- out of the window, so no batch ever reads what an earlier batch
-- handled.
--
-- The predicates below repeat the drain's quals VERBATIM — a partial
-- index is only a candidate when the planner can prove its predicate
-- from the query's own quals:
--   * pending/scheduled arm: `status IN ('pending', 'scheduled')`
--   * cooperative arm: `status = 'running' AND cancel_phase = 0` — the
--     phase term is load-bearing: a requested row stays 'running' (the
--     worker owns the terminal write), so a status-only predicate would
--     keep every already-requested row in the key range and leave the
--     re-walk in place.
--
-- DELIBERATE overlap with jobs_tags_gin_idx, which serves tag filters
-- over ALL statuses (the admin list view, archive queries) and stays.
-- This initiative never drops structures.
--
-- A plain (id) partial index over the active rows is deliberately NOT
-- here, and this was measured rather than assumed: with one present, the
-- drain's queue- and actor-filtered windows (`queue = $1 AND status IN
-- (...) AND id > $cursor ORDER BY id LIMIT $n`) plan against it instead
-- of the keyed (queue, id) / (actor, id) indexes from 01.00.06_01 —
-- resolving the keyed column as a post-scan filter over every active row
-- rather than seeking one queue's batch (verified on an audit-shaped
-- corpus: both windows switch to the id-only index when it exists).
-- tests/test_index_audit.py pins those seeks and fails on exactly that
-- substitution, so the id partial cannot ship. The unfiltered drain's
-- one linear pass of terminal history per call (the keyset cursor bounds
-- it to once, not per batch) is accepted instead.
--
-- OPS NOTE (locks): a plain CREATE INDEX takes a SHARE lock that blocks
-- writes to jobs for the build. Build these CONCURRENTLY by hand during
-- a maintenance window on any deployment where jobs is large — the same
-- guidance 01.00.06_01 carries, and for the same reason: the migration
-- runner wraps each file in a transaction, and CREATE INDEX
-- CONCURRENTLY cannot run inside one.
CREATE INDEX IF NOT EXISTS jobs_tags_active_gin_idx
    ON "attack2".jobs USING gin (tags)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS jobs_tags_cancellable_running_gin_idx
    ON "attack2".jobs USING gin (tags)
    WHERE status = 'running' AND cancel_phase = 0;

-- The one-time backfill of the assignment-routed marker (the columns
-- arrive in 01.00.12_05_pre_assignment_routed_columns.sql). Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- ── What it paints, and why exactly this population ───────────────────
-- DEFAULT false with the column added NOT NULL is already correct for
-- every producer-placed row, so the backfill exists for exactly one
-- population: rows already claimed once that are dispatchable now
-- (pending) or become dispatchable when the scheduled-to-pending
-- promotion sweep flips them (scheduled). The promotion only re-dates a
-- row; it learns nothing about its origin and writes no marker, so a
-- delayed re-pend (a snooze, a retry-after, a reclaim with budget left)
-- still sleeping off its deferral at upgrade time must get the flag
-- HERE: its re-pend already happened, before the column existed. Every
-- row outside this population is either not dispatchable (running,
-- terminal) or producer-placed, and producer placement always has
-- started_at IS NULL (an enqueue is never a claim), including
-- future-dated scheduled enqueues, so the started_at conjunct keeps
-- the population exact. The assignment_routed = false conjunct keeps
-- the statement a no-op on rows a re-pend path has already flagged, so
-- a re-run never clobbers them.
--
-- Carry the old proxy's population forward, so a fleet upgrading
-- mid-flight keeps routing its in-flight re-pended tails by the
-- assignment exactly as before the upgrade: the pre-upgrade arm probed
-- pending rows with started_at IS NOT NULL, and a re-pended row still
-- in its deferral (scheduled, started_at set) joined that probe the
-- moment promotion flipped it to pending, both statuses name the
-- population the pre-upgrade fleet was serving, or was about to serve,
-- by assignment. Bounded by that population, not the backlog.
--
-- ── Why the backfill is its own migration ────────────────────────────
-- An UPDATE holds ROW EXCLUSIVE on jobs, not ACCESS EXCLUSIVE: it
-- blocks neither readers nor other writers (only writers of the same
-- rows, and no dispatch path writes a pending row it has not first
-- claimed through the row lock). A backfill over a deep re-pend backlog
-- can therefore take as long as it needs without parking the fleet's
-- reads, heartbeats or claims, which is exactly why it must NOT share
-- a transaction with the marker columns' ALTER, whose ACCESS EXCLUSIVE
-- would otherwise be held across it (issue #250). It also sequences
-- BEFORE the probe-index builds that read the flag (01.00.12_08 and
-- 01.00.12_09): building after the backfill means one build over the
-- final flag values, rather than an index the backfill then maintains
-- row-by-row through non-HOT updates.
UPDATE "attack2".jobs
   SET assignment_routed = true
 WHERE status IN ('pending', 'scheduled')
   AND started_at IS NOT NULL
   AND assignment_routed = false;

-- The assignment-routed population's two probe indexes (the marker
-- column arrives in 01.00.12_05, its backfill in 01.00.12_07).
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- The probe index follows the marker. Same geometry and same rationale
-- as the started_at-proxy index this round originally shipped and then
-- retired (the create/drop pair removed with it): the arm probes per
-- (actor, fairness cohort) with actor equality plus
-- COALESCE(fairness_key, '__null__') equality as a two-column Index
-- Cond prefix, and the index's own (priority DESC, scheduled_at, id)
-- order serves the probe's ORDER BY without a sort, so each probe stops
-- at its LIMIT regardless of cohort depth. The COALESCE expression is
-- IMMUTABLE and must stay VERBATIM-identical to every use in the
-- dispatch SQL, or the expression index stops serving the query.
--
-- The queue-move drain's window index. The drain re-selects "the next
-- batch_size of THIS actor's rows still carrying the SOURCE queue" on
-- every pass, so both actor and queue have to be Index Cond columns. On
-- the single-column partial indexes alone, whichever one the planner
-- picks leaves the other predicate as a post-scan Filter that walks the
-- population earlier batches already rewrote onto the target: batch N
-- pays for the (N-1) * batch_size rows already moved, and the drain's
-- total cost is quadratic in the backlog rather than linear. That only
-- bites on the deep backlogs where an operator most needs the move to
-- complete, and it shows up as a per-batch statement timeout that gets
-- worse the further the drain gets.
--
-- Leading (actor, queue) matches the drain's two equality predicates; the
-- trailing id serves its ORDER BY without a sort, so each batch is an
-- ordered scan that stops at its LIMIT. Partial on the dispatchable
-- statuses, which is the only population the drain moves: a terminal
-- row's queue label is inert.
--
-- ── Locks: one FILE per transaction, writers drain between files ────
-- The runner wraps each FILE in one transaction (the whole rendered
-- file is a single conn.execute, the same doctrine every sibling
-- states, and 01.00.12_06 carries four builds in one file), so both
-- builds below share this file's transaction. Each build takes a SHARE
-- lock on jobs: reads (dispatch probes, depth samplers, the admin UI)
-- keep flowing, while writes (claims, heartbeats, re-pends) queue for
-- the duration of ALL builds in this file: the write-block window per
-- file is the SUM of its builds, with no drain between builds inside a
-- file (measured: a writer INSERT blocked 1.04 s behind this file's two
-- builds vs 0.49 s behind a single-build file). The lock queue is FIFO,
-- so the writes that queued behind this file commit and run before the
-- NEXT file asks for the table: a fleet upgrading with old workers
-- still live sees one write-block window per file, never one continuous
-- window across the whole round, and reads never block at all (issue
-- #250: the original single-file form additionally held ACCESS
-- EXCLUSIVE, which blocks reads too, across the ALTERs, the backfill
-- and both builds in ONE window).
--
-- OPS NOTE (locks), same caveat as every sibling index migration
-- (01.00.06_01, 01.00.09_01, 01.00.13_02, 01.00.13_03): build time is
-- proportional to the jobs ROW COUNT (a partial index build still scans
-- the whole table). Most deployments see momentary builds: the bounded
-- maintenance sweeps keep steady-state jobs small. On a deployment
-- where a single build would outrun the workers' heartbeat budget,
-- pre-build this file's indexes by hand outside the runner during a
-- maintenance window and let this migration no-op via IF NOT EXISTS:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_assignment_routed_probe_idx
--       ON "attack2".jobs (actor, COALESCE(fairness_key, '__null__'),
--                           priority DESC, scheduled_at, id)
--       WHERE status = 'pending' AND assignment_routed;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_actor_queue_backlog_idx
--       ON "attack2".jobs (actor, queue, id)
--       WHERE status IN ('pending', 'scheduled');
--
-- Plain transactional CREATE INDEX here, not the no-transaction
-- CONCURRENTLY form, for the same deadlock shape as every sibling
-- above: the migration runner serializes concurrent migrators with
-- pg_advisory_lock, a second replica's blocking lock wait is an open
-- transaction, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it, a cycle the deadlock detector
-- breaks by failing the apply.
CREATE INDEX IF NOT EXISTS jobs_assignment_routed_probe_idx
    ON "attack2".jobs (actor, COALESCE(fairness_key, '__null__'),
                        priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND assignment_routed;

CREATE INDEX IF NOT EXISTS jobs_actor_queue_backlog_idx
    ON "attack2".jobs (actor, queue, id)
    WHERE status IN ('pending', 'scheduled');

-- The producer-placed population's two probe indexes: the label-routed
-- arms' mirrors of jobs_actor_dispatch_idx and
-- jobs_round_robin_probe_idx, partial on the assignment-routed marker
-- (the column arrives in 01.00.12_05, its backfill in 01.00.12_07).
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- ── Why the pending-only twins are not enough ──────────────────────────
-- Every label-routed probe filters NOT assignment_routed: the strict-
-- FIFO and round-robin candidates laterals, per_actor_capacity's
-- has_pending probe, and the claimable probe's first arm in
-- backend/_dispatch_sql.py, but jobs_actor_dispatch_idx and
-- jobs_round_robin_probe_idx are partial on status = 'pending' alone,
-- so a re-pended pending row sits in their ordered ranges and the
-- marker conjunct degrades to a post-scan Filter. Each probe walks
-- every re-pended row ahead of the producer-placed rows it can admit:
-- claim cost grows LINEARLY in re-pend depth (issue #243, measured on
-- PG 18 with 5k producer rows behind a same-(actor, queue) re-pend
-- tail: strict_fifo 2.3 ms -> 27.4 ms at 100k re-pended, round_robin
-- 2.7 ms -> 46.8 ms), the exact depth-proportional shape the depth
-- contract exists to remove, paid on the crash-reclaim re-pend tails
-- that follow every fleet-wide restart. The indexes below carry the
-- marker in their PREDICATES, so the re-pended population is not in
-- them at all: each probe starts at its first admissible row and stops
-- at its LIMIT, and the re-pended population is probed separately, by
-- the assignment-routed arm, on jobs_assignment_routed_probe_idx
-- (01.00.12_08), the two populations' probes are disjoint by the
-- marker, matching the routing contract's two disjoint arms.
--
-- jobs_queue_actor_dispatch_idx (01.00.13_02) already narrowed the
-- strict-FIFO pa_keys walk to this population; these two extend the
-- same narrowing to the probe geometries the candidates laterals and
-- the has_pending / claimable probes actually ride.
--
-- The COALESCE(fairness_key, '__null__') expression in the round-robin
-- twin is IMMUTABLE and must stay VERBATIM-identical to every use in
-- the dispatch SQL (the probe equality, the rr_keys walk, and the
-- window PARTITION BY), or the expression index stops serving the
-- query, the same doctrine as its pending-only twin.
--
-- ── Why no capped-arm index ────────────────────────────────────────────
-- The capped arm (capped_ranked -> top_ids -> locked) never probes the
-- backlog: top_ids cuts the MATERIALIZED ranked window (bounded by the
-- candidates laterals' residual * oversample probes), and `locked`
-- re-finds its rows by primary key. Every depth-coupled probe the
-- capped arm shares with the uncapped arm lives in the candidates
-- laterals these indexes serve, so a capped-actor-specific predicate
-- would duplicate this file's for no query the planner could reach.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- indexes are purely additive: no shipped statement references them
-- (the pending-only twins keep serving every pre-marker query), and
-- this release's probes run without them too (the marker conjunct
-- stays a Filter; only the depth bound is lost, never correctness),
-- which is why they ship in the pre phase, before the code rollout.
--
-- OPS NOTE (locks), same caveat as every sibling index migration
-- (01.00.06_01, 01.00.09_01, 01.00.13_02, 01.00.13_03): a plain
-- CREATE INDEX takes a SHARE lock that blocks writes to jobs for the
-- duration of the build, and build time is proportional to the jobs
-- row count (a partial index build still scans the whole table). The
-- runner wraps each FILE, not each statement, in one transaction, so
-- this file's two builds share ONE write-block window: writes queue
-- for the duration of both (their sum) and drain only when the file
-- commits, before the next file asks for the table. On a deployment
-- where that window would outrun the workers' heartbeat budget,
-- pre-build by hand outside the runner during a maintenance window and
-- let this migration no-op via IF NOT EXISTS:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_unrouted_actor_dispatch_idx
--       ON "attack2".jobs (actor, queue, priority DESC, scheduled_at, id)
--       WHERE status = 'pending' AND NOT assignment_routed;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_unrouted_round_robin_probe_idx
--       ON "attack2".jobs (actor, queue, COALESCE(fairness_key, '__null__'),
--                           priority DESC, scheduled_at, id)
--       WHERE status = 'pending' AND NOT assignment_routed;
--
-- Plain transactional CREATE INDEX here, not the no-transaction
-- CONCURRENTLY form, for the same deadlock shape as every sibling
-- above: the migration runner serializes concurrent migrators with
-- pg_advisory_lock, a second replica's blocking lock wait is an open
-- transaction, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it, a cycle the deadlock detector
-- breaks by failing the apply.
CREATE INDEX IF NOT EXISTS jobs_unrouted_actor_dispatch_idx
    ON "attack2".jobs (actor, queue, priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND NOT assignment_routed;

CREATE INDEX IF NOT EXISTS jobs_unrouted_round_robin_probe_idx
    ON "attack2".jobs (actor, queue, COALESCE(fairness_key, '__null__'),
                        priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND NOT assignment_routed;

-- The claim-recency stamp: the durable per-actor signal the dispatch
-- round's cross-actor tiebreak rotates on. Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "attack2" token
-- is substituted at apply time by the migration runner.
--
-- ── Why this column exists ────────────────────────────────────────────
-- A dispatch round cuts its admitted set at the round's limit over
-- ``ORDER BY pending_rank, priority DESC, scheduled_at, id``
-- (backend/_dispatch_sql.py). ``pending_rank`` is a per-actor row number,
-- so every actor holding due work offers its head job at rank 1, and once
-- the fleet holds more such actors than the round's limit the tie among
-- the rank-1 rows falls through to ``priority DESC, scheduled_at, id`` —
-- a STABLE total order: each round's winners immediately refill their own
-- rank-1 slot from their own backlog with the same relative key, so the
-- same prefix of actors wins every round and the rest are never claimed
-- at all (no cap, no denial, no failure — just pending forever, pinned by
-- tests/test_dispatch_actor_cohort_rotation.py and
-- tests/test_fleet_fairness_starvation.py).
--
-- No function of the candidate rows alone can break that tie differently
-- across rounds: the starved actor's head row is static, and the served
-- actor's refill is indistinguishable from the row it replaces. Rotation
-- therefore needs durable cross-round state, and this column is it: the
-- claim stamps every actor it admitted (statement_timestamp of the stamp
-- statement), and the next round's cut orders equal-priority rank-1 rows
-- by ``last_claimed_at ASC NULLS FIRST`` — never-claimed actors first,
-- then least-recently-claimed — which is round-robin over actors within a
-- priority tier. Priority still dominates the stamp, so an operator's
-- priority bias keeps its meaning; the stamp only removes the ACCIDENTAL
-- starvation the stable order produced among peers.
--
-- ── Shape ─────────────────────────────────────────────────────────────
-- Nullable, no default: NULL means "never claimed", which is exactly the
-- state of every pre-existing row and every freshly registered actor, and
-- NULLS FIRST puts those actors at the front of their first round. The
-- ALTER is metadata-only (no table rewrite) on this Postgres generation.
-- Old-release code never names the column, and every actor_config writer
-- in the tree writes explicit column lists, so the additive discipline
-- holds for the rolling-deploy window in both directions that the window
-- supports.
ALTER TABLE "attack2".actor_config
    ADD COLUMN IF NOT EXISTS last_claimed_at timestamptz;

-- Registry-scope walk index for the dispatch round: the (queue, actor)
-- loose-scan index on jobs. Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "attack2" token is
-- substituted at apply time by the migration runner.
--
-- ── Why this index exists ───────────────────────────────────────────
-- A dispatch round must be scoped to the (actor, queue) pairs it polls;
-- cost that grows with the fleet-wide REGISTERED-actor count couples
-- every queue's dispatch latency to a dimension an operator cannot see
-- in their own queue's metrics (the registry-scope oracle,
-- tests/test_dispatch_actor_registry_scope_bound.py, pins this).
-- actor_config carries one row per registered actor, sits far below
-- autovacuum's insert threshold, and is therefore usually never
-- analyzed — so before the registry-scope fix the planner had no option
-- but a full Seq Scan of the registry, repeated across the claim
-- statement's capacity CTEs, and the estimate (~440 rows even for a
-- one-actor fleet) cascaded through the candidate chain's nested loops.
-- The claim statement now drives its capacity CTEs from the round's own
-- pending-rows population, enumerated from jobs, and reads actor_config
-- back by primary key only for those actors.
--
-- jobs_queue_actor_dispatch_idx serves pa_keys, the strict-FIFO
-- variant's label-routed (queue, actor) enumeration in
-- backend/_dispatch_sql.py: a recursive loose index scan whose
-- ``queue = ANY($1)`` predicate is a ScalarArrayOp on this index's
-- LEADING column (one index range per round queue, zero entries visited
-- for unpolled queues) and whose per-step ``(queue, actor) > cur``
-- row-compare is an Index Cond within those ranges — one bounded seek
-- per distinct (queue, actor) pair the round polls, the skip-scan
-- emulation this Postgres generation has no native operator for. The
-- actor-leading dispatch indexes cannot serve that walk: with actor
-- first, the queue predicate degrades to a per-entry filter and the
-- walk reads every fleet actor's index entries between matches. The
-- partial predicate (status = 'pending' AND NOT assignment_routed) is
-- exactly the walk's own population, so re-pended rows (which route by
-- assignment, never by label) cost the walk nothing; the COALESCE-free
-- two-column key keeps the index narrow on the write hot path.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- index is purely additive — the previous release's dispatch CTE never
-- references it (its per_actor_capacity scans actor_config and probes
-- jobs_actor_dispatch_idx), and this release's statement runs without
-- it too (the walk degrades to the older indexes' filtered scans; only
-- the cost bound is lost, never correctness) — which is why it ships in
-- the pre phase, before the code rollout.
--
-- OPS NOTE (locks), same caveat as every sibling index migration: the
-- CREATE INDEX takes a write-blocking lock on jobs for the duration of
-- the build, and build time is proportional to the current pending-row
-- count. Operators with a large jobs backlog should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_queue_actor_dispatch_idx
-- ON "attack2".jobs (queue, actor) WHERE status = 'pending' AND NOT
-- assignment_routed` manually outside the migration runner during a
-- maintenance window, then let this migration no-op via IF NOT EXISTS.
CREATE INDEX IF NOT EXISTS jobs_queue_actor_dispatch_idx
    ON "attack2".jobs (queue, actor)
    WHERE status = 'pending' AND NOT assignment_routed;

-- Open-member probe index for batch completion: a partial B-tree over
-- the NON-TERMINAL members of every batch, keyed by batch id. Forward-only;
-- there is no down migration. To revert, DROP INDEX. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- ── Why this index exists ───────────────────────────────────────────
-- Every batched job's terminal write runs the batch hook
-- (taskq/batch.py), and the hook's completion attempt asks one question:
-- "does this batch still have a member that is not terminal?"
-- (_COMPLETE_BATCH_SQL's NOT EXISTS guard in backend/_batch_sql.py, the
-- same probe count_batch_non_terminal asks). Answered through
-- jobs_metadata_gin_idx's `metadata @>` bitmap, that question visits
-- EVERY member of the batch — terminal ones
-- included, because the GIN posting list holds them all and only the
-- heap recheck tells them apart — so a batch of N members paid O(N)
-- member visits per terminal write and O(N²) to complete. Measured on
-- postgres:18 with a 10 000-member batch: 323 buffers and ~1.1 ms per
-- probe; with this index, 5 buffers and ~0.05 ms.
--
-- The index body holds exactly the rows the probe can return: a member
-- row enters it when inserted (or re-pended by retry_job) and leaves it
-- in the same transaction that makes the row terminal, so the probe is
-- an equality seek on the batch id that stops at the first entry — or
-- at an empty range, which is the completion answer. Its size tracks
-- the in-flight member population, never the batch history.
--
-- ── Why the key and predicate take this shape ───────────────────────
--   * `(metadata->>'batch_id')` as text, not cast to uuid: metadata is
--     caller-supplied jsonb on every enqueue path, and a uuid cast in an
--     index expression would turn a non-uuid `batch_id` value on an
--     unrelated job into a failed INSERT.
--   * `(metadata->>'batch_id') IS NOT NULL`: keeps every non-batch job out
--     of the index (no entry, no maintenance on the write hot path), and
--     the planner proves it from the probe's own equality — a strict
--     operator on the same expression implies NOT NULL.
--   * `status IN ('pending', 'running', 'scheduled')`: the non-terminal
--     set, spelled positively so the predicate is exactly the text the
--     probe's own qual renders (backend/_batch_sql.py derives it from
--     ACTIVE_STATUSES); the partial index is only a candidate when the
--     planner can prove its predicate from the statement's quals, which
--     tests/test_batch_completion_cost_pg.py pins with EXPLAIN.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The index
-- is purely additive — the previous release's statements use the GIN
-- containment probe and never reference it, and this release's probe
-- runs without it too (it degrades to a filtered scan; only the cost
-- bound is lost, never correctness).
--
-- OPS NOTE (locks), same caveat as every sibling index migration: the
-- CREATE INDEX takes a write-blocking lock on jobs for the duration of
-- the build, and build time is proportional to the current row count
-- (the index body only ever holds the open batch members). Operators
-- with a large jobs table should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_batch_open_members_idx
-- ON "attack2".jobs ((metadata->>'batch_id')) WHERE
-- (metadata->>'batch_id') IS NOT NULL AND status IN ('pending',
-- 'running', 'scheduled')` manually outside the migration runner during
-- a maintenance window, then let this migration no-op via IF NOT EXISTS.
-- Plain CREATE INDEX here, not the no-transaction CONCURRENTLY form, for
-- the deadlock reason 01.00.11_01_pre_repended_probe_index.sql derives.
CREATE INDEX IF NOT EXISTS jobs_batch_open_members_idx
    ON "attack2".jobs ((metadata->>'batch_id'))
    WHERE (metadata->>'batch_id') IS NOT NULL
      AND status IN ('pending', 'running', 'scheduled');

-- The wake trigger's channel is derived from a schema TAG, not the schema
-- name. Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- ── Why the channel is hashed ──────────────────────────────────────────
-- NOTIFY channels are identifiers bounded by NAMEDATALEN-1 (63 bytes):
-- LISTEN silently truncates a longer one, pg_notify() rejects it. A
-- channel that interpolated the schema name ('taskq_wake_' || schema,
-- and the app's 'taskq_worker_' || schema || '_' || uuid) therefore had
-- a schema length past which the listener and the notifier no longer
-- named the same channel — from 14 characters for the per-worker cancel
-- channel, 53 for the wake channel — while the settings admit 63.
-- Every channel now embeds left(encode(sha224(schema), 'hex'), 10)
-- instead: fixed width, so no schema length breaks it. The Python twin
-- is taskq.constants.schema_channel_tag; the two are pinned equal end to
-- end by tests/test_notify_channel_length.py, and the prefix length is
-- SCHEMA_CHANNEL_TAG_HEX_LEN there. TG_TABLE_SCHEMA is the schema's
-- exact (case-preserved) name, the same text the application hashes.
--
-- ── The trigger is the sole wake source for inserts ────────────────────
-- Every enqueue path (single INSERT, batch INSERT, COPY) relies on this
-- trigger to wake dispatchers; the application issues no pg_notify of its
-- own after an INSERT. The WHEN clause is what keeps a future-dated row
-- from waking the fleet: every insert path decides status server-side
-- (scheduled_at in the future => 'scheduled'), so 'pending' means
-- dispatchable now. Postgres coalesces identical (channel, payload)
-- notifications within one transaction, so a batch costs one delivery.
--
-- ROLLING DEPLOY: the previous release's workers LISTEN on the old
-- 'taskq_wake_' || schema name and stop receiving wakes once this applies;
-- they keep claiming on their poll interval until restarted. Adopt by
-- restarting the fleet onto the new release (docs/guides/upgrading.md).

CREATE OR REPLACE FUNCTION "attack2".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify(
            'taskq_wake_' || left(encode(sha224(convert_to(TG_TABLE_SCHEMA::text, 'UTF8')), 'hex'), 10),
            ''
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Unique-preflight probe index over EVERY status, not just the active
-- ones. Forward-only; there is no down migration. To revert, DROP INDEX.
-- The literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- ── Why this index exists ───────────────────────────────────────────
-- The unique_for preflight (enqueue_unique_for_preflight in
-- backend/_sql_templates.py) asks: "the newest job for this (actor,
-- identity_key) within the window, in any of the CALLER's
-- unique_states". The state set is per-actor configuration
-- (@actor(unique_states=...)); DEFAULT_UNIQUE_STATES is the active
-- triple, but a caller may fold terminal states in (``succeeded`` is
-- the documented use: recent success is the dedup answer). The only
-- index serving the probe was jobs_identity_active_idx
-- (01.00.00_01), and it is PARTIAL on
-- status IN ('pending', 'scheduled', 'running'): the planner can prove
-- a partial index applies only when the query's own quals imply its
-- predicate, and a bound status array holding ANY terminal status
-- proves the opposite. Every probe whose unique_states leaves the
-- active triple therefore degrades from the index seek to a sequential
-- scan filtered on actor and identity_key, on the enqueue path, once
-- per unique enqueue.
--
-- This migration adds a NON-partial B-tree on (actor, identity_key,
-- status): equality, equality, and the probe's status array, for ANY
-- status set, so the probe's plan no longer depends on what the caller
-- configured. The status column rides as the third key (not a
-- predicate) precisely because the set varies per caller; the window's
-- created_at bound stays a post-scan filter (a VOLATILE
-- clock_timestamp() bound cannot be an index condition), and the
-- ORDER BY created_at DESC LIMIT 1 keeps its top-N shape over the
-- (tiny) per-identity candidate set the index hands it.
--
-- ── Deliberate overlap ──────────────────────────────────────────────
-- jobs_identity_active_idx stays: for the DEFAULT active-triple state
-- set it remains the better plan (its body holds only active rows, so
-- the seek touches a smaller index and never the terminal history),
-- and the planner keeps choosing it there. This index is the fallback
-- the partial index cannot serve; the redundancy is the same trade
-- 01.00.06_01's keyed indexes document, an enqueue-path probe is not
-- the place to make the planner prove a subset relation on a bound
-- array.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- index is purely additive: the previous release's probe never
-- references it, and this release's probe runs without it too (it
-- degrades to the sequential scan; only the cost bound is lost, never
-- correctness).
--
-- OPS NOTE (locks), same caveat as every sibling index migration: the
-- CREATE INDEX takes a write-blocking lock on jobs for the duration of
-- the build, and build time is proportional to the current row count
-- (a non-partial index body holds every job with a non-NULL identity
-- key, including the terminal history). Operators with a large jobs
-- table should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_identity_status_idx
-- ON "attack2".jobs (actor, identity_key, status)` manually outside
-- the migration runner during a maintenance window, then let this
-- migration no-op via IF NOT EXISTS. Plain CREATE INDEX here, not the
-- no-transaction CONCURRENTLY form, for the deadlock reason
-- 01.00.09_01_pre_round_robin_probe_index.sql derives.
CREATE INDEX IF NOT EXISTS jobs_identity_status_idx
    ON "attack2".jobs (actor, identity_key, status);

-- The insert-wake trigger carries the row's queue as its NOTIFY payload so
-- workers stop waking on inserts to queues they would never claim.
-- Forward-only; there is no down migration. To revert, re-apply the
-- previous function body (empty payload) from 01.00.14_01.
-- The literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- ── Why this exists ─────────────────────────────────────────────────
-- The wake channel is schema-wide (one channel per schema, by design:
-- the worker subscribes once and claims the queues it serves). Every
-- insert of a pending row therefore woke EVERY worker in the fleet, and
-- each wake costs a full multi-CTE claim round against the dispatcher
-- pool. At fifty workers the fleet answered each enqueue with up to
-- fifty claim statements, nearly all returning zero rows for workers
-- whose queues the insert never touched. The claim cooldown bounds the
-- waste per worker (about twenty rounds per second) but the fleet-wide
-- product still grows linearly with fleet size.
--
-- The fix keeps the single-channel design and moves the filtering into
-- the notification itself: the payload names the inserted row's queue,
-- and each worker's listener sets its wake event only when the payload
-- is a queue it serves (or the payload is empty, see the compatibility
-- contract below). A worker serving the queue claims ALL its queues in
-- the round, exactly as before; a worker serving nothing in the payload
-- skips the round and its fallback poll stays authoritative.
--
-- ── Compatibility contract (rolling deploys) ────────────────────────
-- The wake payload is a queue name, and the listener side treats an
-- EMPTY payload as "wake every subscriber": that keeps three shapes
-- working. Old listener + new trigger: the old listener ignores the
-- payload entirely and wakes as before. New listener + this trigger:
-- filtered. New listener + empty payload (the COPY fixup's bulk wake in
-- backend/_sql_templates.py, which cannot know one queue for a batch of
-- rows spanning queues, and any older trigger still live): everything
-- wakes, the historical contract. Workers refuse to start on pending
-- pre-phase migrations, so a new listener never runs against the old
-- trigger in the same schema.
--
-- Postgres coalesces identical (channel, payload) notifications within
-- one transaction, so a batch of inserts to ONE queue still costs one
-- delivery; a batch spanning queues now costs one delivery per distinct
-- queue (the pre-change cost was one delivery total, but every delivery
-- woke every worker; the filtered trade is strictly fewer spurious
-- claim rounds).

CREATE OR REPLACE FUNCTION "attack2".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify(
            'taskq_wake_' || left(encode(sha224(convert_to(TG_TABLE_SCHEMA::text, 'UTF8')), 'hex'), 10),
            NEW.queue
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- jobs_finished_at_idx becomes (status, finished_at) INCLUDE (id): the archive prune's
-- candidate window selects one terminal status over a finished_at range in
-- finished_at order with a LIMIT, and the composite serves all three in one
-- index (equality on the leading column, the range as the second, the order
-- preserved), which the (finished_at)-only shape cannot: measured on
-- PostgreSQL 18, the planner routes the prune's candidate window through a
-- skip scan on jobs_identity_status_idx (a bitmap over the ENTIRE terminal
-- population, a post-scan Filter on the range, a Sort), which is a
-- population scan wearing an index, exactly what the archive's own plan
-- pins forbid. The composite makes the population scan unchoosable.
--
-- The partial predicate the original index carried is DELIBERATELY
-- dropped. The plancache's generic plan (the form a long-lived leader
-- connection's daily prune settles into) cannot prove ``status = $1``
-- implies ``status IN (five)``, so a partial predicate EXCLUDES this
-- index from the generic plan entirely, leaving the skip-scan bitmap on
-- jobs_identity_status_idx as the planner's only status-serving option:
-- measured, that plan is a whole-population walk (bitmap the terminal
-- population, heap-fetch every candidate, filter the range, sort it) in
-- the very executions the daily prune lives in. The non-partial index is
-- provable for any bound parameter, so the generic plan keeps the
-- bounded index-only scan: Index Cond (status, finished_at), the range
-- as the seek, the LIMIT terminating the scan, measured correct (10,000
-- archived per batch) and bounded (no Sort, no population walk) in the
-- generic plan on a 70k-row terminal corpus. The size cost is the
-- running/scheduled rows (finished_at NULL, tiny); the history pages
-- filter multi-status sets ordered by status_priority first and never
-- used this index; their plans are unchanged.
--
-- ROLLING DEPLOY: drop and recreate is instant (one index, metadata-only
-- lock windows); a moment of concurrent prune-and-retry sees the old plan
-- shape, not an error. Forward-only; to revert, recreate the
-- (finished_at)-only shape from 01.00.00_01.

-- The INCLUDE (id) is not decoration: the prune's candidate window selects
-- only the id column, so with the id carried in the index the window is an
-- index-only scan (no heap fetches at all). Measured on PostgreSQL 18
-- (70k-row terminal corpus, the plancache's generic plan): without the
-- INCLUDE the generic plan costs the skip-scan bitmap cheaper and the
-- prune becomes a population scan; with it, the skip-scan alternative
-- cannot win at any row estimate and the window stays a bounded,
-- order-preserving index scan.

DROP INDEX IF EXISTS "attack2".jobs_finished_at_idx;

CREATE INDEX jobs_finished_at_idx
    ON "attack2".jobs (status, finished_at) INCLUDE (id);

-- The actor's declared retry curve reaches the server-side enqueue paths.
-- Forward-only; there is no down migration. To revert, DROP the four
-- columns (readers treat NULL as the enqueue default).
-- The literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- ── Why these columns exist ─────────────────────────────────────────
-- cron fires and the admin run-now build their EnqueueArgs from the
-- stored actor_config row. The row carried max_attempts and retry_kind
-- but NOT the curve scalars (retry_base, retry_cap, retry_backoff,
-- retry_jitter), so those took the EnqueueArgs dataclass defaults on
-- every server-side fire: an actor declaring base=600s/fixed/backoff
-- and jitter=0.0 was fired by cron with base=5s/exponential/jitter=0.2,
-- and a crash-reclaim sweep re-pended the two arms on different curves.
-- Measured: cron re-pend delay 5.2s against the declared 600.0s.
--
-- NULL means "the actor's declaration is unknown here": the enqueue
-- default stands, exactly as before this migration, so existing rows
-- (and rows for actors whose registry is absent at sync time) degrade
-- to the old behavior, not to an error.
--
-- The sync seeds these columns on FIRST CREATE only (the same seeding
-- semantics as max_attempts and retry_kind: the ON CONFLICT arm does
-- not touch them), so an operator correcting a stored curve is never
-- silently overwritten by a boot; a changed @actor literal surfaces
-- through the existing drift machinery.

ALTER TABLE "attack2".actor_config
    ADD COLUMN retry_base    interval,
    ADD COLUMN retry_cap     interval,
    ADD COLUMN retry_backoff text,
    ADD COLUMN retry_jitter  float8;

-- Non-saturating claim-epoch fence for the terminal/ownership writes.
--
-- The claim stamps the DISPLAYED attempt counter with a saturating
-- increment (``attempt = LEAST(j.attempt + 1, 32767)``,
-- backend/_dispatch_sql.py) so a row parked at the smallint ceiling
-- cannot turn a whole claim round into a smallint-out-of-range driver
-- error. At the ceiling the value stops advancing, which means the
-- terminal-write fences (``status = 'running' AND locked_by_worker = $n
-- AND attempt = $k``) can no longer tell a stale execution from the live
-- one: a reclaim plus a redispatch to the same worker leaves both the
-- stale handler and the live handler holding the same
-- (worker, attempt) pair, and the stale execution's mark_succeeded wins
-- while the live one is fenced out. The displayed counter must keep its
-- saturating, consumer-visible semantics, so the fix is a SECOND column:
--
-- claim_epoch is the row's non-saturating claim counter. Every
-- successful dispatch claim bumps it by exactly 1 (bigint: it does not
-- saturate, no reachable workload approaches 2^63 claims of one row).
-- Every terminal/ownership write that fences on ``attempt`` gains the
-- equality conjunct ``claim_epoch = $n`` and binds the epoch from its
-- own claim view (JobRow.claim_epoch, the value the dispatch round
-- returned). Because fences compare EQUALITY, never magnitude, the
-- epoch's only requirement is uniqueness across consecutive claims of
-- the same row, and +1 per claim provides that without bound.
--
-- INVARIANT: between two terminal/ownership writes that both fence on
-- claim_epoch, a successful claim of the same row MUST have bumped it.
-- The reclaim sweeps that clear locks (sweep 1's re-pend/crash/cancel
-- arms, the heartbeat isolate) deliberately leave claim_epoch
-- untouched: the reclaim itself hands the row back, and the NEXT claim
-- bumps it. A stale writer's epoch therefore goes stale at the moment a
-- new claim exists, on every path, including at the attempt ceiling
-- where attempt alone can no longer distinguish the two executions.
-- Writer order per row is: claim (bump) -> maybe reclaim (leave) ->
-- claim (bump) -> terminal write (fence on the epoch its own claim
-- returned). No writer except the dispatch claim ever assigns the
-- column.
--
-- The in-memory testing backend mirrors the semantics exactly
-- (taskq/testing/_dispatch.py bumps on claim, taskq/testing/_terminal.py
-- fences on equality), so the equivalence tier exercises the same
-- fence.
--
-- ROLLING DEPLOY: additive with a default, applied while old pods run:
-- the previous release's statements name their columns explicitly and
-- never read this one. This release's claim bumps it unconditionally,
-- which is safe against old workers too: an old worker's terminal write
-- never references the column, and the fence it does not carry is the
-- defect this migration ships the fix for. Forward-only; there is no
-- down migration.
--
-- OPS NOTE (locks): ADD COLUMN with a non-volatile default is
-- metadata-only on PG >= 11 (no table rewrite; the default is stored in
-- pg_attribute and read from there), so this add does not stall a
-- fleet. Existing rows read 0, which is correct rather than merely
-- harmless: fences compare equality, so epoch 0 is simply the one
-- epoch no claim of a pre-migration row can ever be stamped with (the
-- first post-migration claim stamps 1), and a pre-migration stale
-- writer (which binds no epoch at all) is fenced out by its missing
-- attempt proof exactly as before.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN "attack2".jobs.claim_epoch IS
    'Non-saturating claim-epoch fence. Bumped by exactly 1 on every '
    'successful dispatch claim; the reclaim sweeps that clear locks leave '
    'it untouched (the next claim bumps it). Every terminal/ownership '
    'write that fences on attempt also fences on this column equalling '
    'the epoch from its own claim view, so a reclaim plus redispatch can '
    'never leave a stale execution and the live one sharing a fence, '
    'including at the attempt ceiling where the displayed attempt counter '
    'saturates. Fences compare equality, never magnitude; writers other '
    'than the dispatch claim never assign this column.';

-- The fenced terminal/lease write's probe index: the job id as a trailing
-- KEY column of the running-holder partial index. Forward-only; there is
-- no down migration. To revert, recreate the two-key form
-- `(locked_by_worker) WHERE status = 'running'` by hand. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- ── Why this index exists ───────────────────────────────────────────
-- Every fenced single-row write (mark_succeeded / mark_failed /
-- mark_retry / mark_cancelled, the per-job lease checks) targets one row
-- by the fence
-- `id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $k
-- AND claim_epoch = $m`. The row is unique by `id = $1` (the primary
-- key), but the planner is free to drive the UPDATE from ANY index the
-- quals admit, and after bulk churn (a large enqueue burst lands before
-- autovacuum's next ANALYZE; `reltuples` goes stale and every equality
-- selectivity collapses to the same one-row estimate) the planner
-- measured on PostgreSQL 18 picks the running-holder partial index
-- `jobs_locked_by_worker_running_idx (locked_by_worker) WHERE status =
-- 'running'` and evaluates `id = $1` as a post-scan Filter: the write
-- walks EVERY running row the worker holds — measured at 1.3 ms and 91
-- buffers for one terminal write at a 2,000-row running population
-- (vs 0.06 ms and 18 buffers driven by the primary key), per terminal
-- write, for the whole drain.
--
-- With `id` as the trailing key column the same mispicked plan becomes
-- harmless: both equality quals are Index Conds (a non-leading key
-- column is still evaluated inside the index, never a heap-visiting
-- Filter), so the scan touches one index entry and one heap row
-- regardless of which index the planner picks or how stale the
-- statistics are. This is the hot-claim-path index discipline: the
-- column that filters every fenced probe rides in the key.
--
-- ── Why the trailing key costs (almost) nothing ────────────────────
-- The index is partial on `status = 'running'`, so its body holds only
-- in-flight rows and stays small by construction; entries leave the
-- index on every terminal/reclaim transition. Appending the id widens
-- each entry by ~16 bytes (uuid) on an index whose per-entry cost the
-- claim UPDATE's status transition already pays. Every existing reader
-- keeps its plan class: the heartbeat renewal
-- (`locked_by_worker = $1 AND status = 'running'`) and the web-admin
-- running count scan the leading-column prefix unchanged, and the
-- per-job lease check (`id = ANY($1::uuid[]) AND locked_by_worker =
-- $2`) gains the same trailing-key filtering this migration exists to
-- give the fence.
--
-- ── Why plain DROP + CREATE INDEX, not the no-transaction
--    CONCURRENTLY form ──
-- Same deadlock shape as 01.00.10_01 (see that file's full
-- derivation): the migration runner serializes concurrent migrators
-- with pg_advisory_lock, a second replica's blocking lock wait is an
-- open transaction, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it — a cycle the deadlock detector
-- breaks by failing the apply. This file therefore follows
-- 01.00.02_01 / 01.00.07_01 / 01.00.10_01's precedent: a transactional
-- plain DROP INDEX + CREATE INDEX, whose ordinary locks queue behind
-- the advisory-lock waiter without a snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.10_01: the pair below takes a
-- write-blocking lock on jobs for the duration of the drop+build;
-- build time is proportional to the current RUNNING row count (the
-- partial predicate), not the whole table. Operators with a large
-- in-flight population should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS
-- jobs_locked_by_worker_running_idx_new ON "attack2".jobs
-- (locked_by_worker, id) WHERE status = 'running'` (then swap the two
-- indexes by name in one transaction) manually outside the migration
-- runner during a maintenance window, and let this migration's
-- IF NOT EXISTS no-op.
DROP INDEX IF EXISTS "attack2".jobs_locked_by_worker_running_idx;
CREATE INDEX IF NOT EXISTS jobs_locked_by_worker_running_idx
    ON "attack2".jobs (locked_by_worker, id)
    WHERE status = 'running';

-- Ownership model for cron schedule disable state: WHO disabled a schedule.
--
-- Why: cron auto-disable (cron_auto_disable_threshold consecutive payload
-- factory failures) wrote only enabled=false, indistinguishable from an
-- operator's deliberate disable. Startup cron registration was create-only
-- (an operator's runtime disable must not be reverted by a redeploy), so the
-- two states shared one fate: a transient partial-DB blip that failed three
-- fires while the strike writes committed permanently halted critical
-- recurring work until a human re-enabled it.
--
-- disabled_by separates the two:
--
--   'auto'      the cron loop's auto-disable did it (recoverable: the code
--               re-declaring the schedule at startup reverts it, see the
--               registration pass in worker/_bootstrap.py).
--   'operator'  an operator disabled it (schedule handle disable(), the CLI,
--               the admin UI, actor deregistration). Never reverted by a
--               boot.
--   NULL        enabled (or a row disabled before this column existed:
--               pre-existing disabled rows predate ownership tracking, and
--               the safe reading of that ambiguity is operator intent, so
--               they keep the create-only semantics they were written under).
--
-- Forward-only; there is no down migration. To revert, DROP the column (all
-- readers treat NULL as "not auto-disabled", today's behavior). The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- OPS NOTE (locks): ADD COLUMN with no default is metadata-only (no table
-- rewrite), and the CHECK constraint is validated against a table that can
-- only hold NULL in the new column, so both alters are trivial on any size.
-- Additive, so it applies while old pods run: the previous release's
-- statements name their columns explicitly and never read this one.

ALTER TABLE "attack2".cron_schedules
    ADD COLUMN IF NOT EXISTS disabled_by text;

ALTER TABLE "attack2".cron_schedules
    ADD CONSTRAINT cron_schedules_disabled_by_check
    CHECK (disabled_by IN ('auto', 'operator'));

COMMENT ON COLUMN "attack2".cron_schedules.disabled_by IS
    'Who disabled this schedule: ''auto'' = the cron loop''s failure-count '
    'auto-disable (a code re-declaration at worker startup reverts it), '
    '''operator'' = a deliberate operator disable (handle, CLI, admin UI, '
    'actor deregistration; never reverted by a boot). NULL = enabled, or '
    'disabled before ownership was tracked (treated as operator intent).';

-- Admin audit trail: one row per operator mutation performed through the
-- admin UI (job cancel, job retry, schedule enable/disable/skip/run-now,
-- actor deregister). Before this table the admin UI's write endpoints
-- recorded NO principal anywhere: the auth dependency's IdentityClaims
-- return was discarded at the router boundary, and the only durable trace
-- of "who cancelled this job" was whatever the operator typed into the
-- reason box. This table is that trail.
--
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "attack2" token is substituted at apply time by the
-- migration runner.
--
-- ROLLING DEPLOY: purely additive. No existing statement references this
-- table, old pods neither read nor write it, and the new code tolerates
-- its absence (the audit write degrades to a logged warning on
-- backend-mediated mutations) while the migration lands.
--
-- Why there is NO foreign key to jobs: the audit's targets are exactly the
-- rows routine maintenance prunes, archives, and deregisters by design. An
-- FK (or a cascade) would let that maintenance erase the record of who did
-- what -- the one thing the table exists to prevent. (target_type,
-- target_id) is a deliberately loose reference; it stays readable after the
-- target is gone, which is when an audit question ("who cancelled the job
-- that vanished?") is asked.
--
-- RETENTION: no sweep touches this table. Entries accumulate for the life
-- of the schema; an operator who needs a bound should archive and truncate
-- deliberately (docs/guides/admin-ui.md, "Audit trail"), because a silent
-- retention window is a hole in the trail wearing a policy's clothes.

CREATE TABLE "attack2".admin_audit (
    id                bigserial PRIMARY KEY,
    occurred_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    principal_subject text NOT NULL,
    action            text NOT NULL,
    target_type       text NOT NULL,
    target_id         text NOT NULL,
    reason            text,
    detail            jsonb NOT NULL DEFAULT '{{}}'::jsonb
);

-- The job detail page renders the per-target trail, and an operator's
-- "what happened today" question scans time. Both orders are small,
-- additive, and index-only.
CREATE INDEX admin_audit_target_idx
    ON "attack2".admin_audit (target_type, target_id, occurred_at);

CREATE INDEX admin_audit_occurred_idx
    ON "attack2".admin_audit (occurred_at);

COMMENT ON TABLE "attack2".admin_audit IS
    'Audit trail of admin-UI operator mutations. One row per mutation: '
    'who (principal_subject), what (action), on what (target_type, '
    'target_id), why (reason), and any per-action extras (detail). No FK '
    'to jobs: targets are pruned/archived by design and the trail must '
    'outlive them.';

COMMENT ON COLUMN "attack2".admin_audit.principal_subject IS
    'The authenticated principal subject that performed the action, from '
    'the auth dependency IdentityClaims. The literal ''anonymous'' when '
    'the router runs without an auth dependency (dev deployments only).';

COMMENT ON COLUMN "attack2".admin_audit.action IS
    'One of: job.cancel | job.retry | schedule.enable | schedule.disable | '
    'schedule.skip | schedule.run | actor.deregister | rate_limit.reset';

-- Shared store for the SAML SSO replay and answered-AuthnRequest gates.
--
-- The SAML admin auth (taskq/web/admin/auth/saml.py) previously kept its
-- consumed-assertion records and answered-AuthnRequest records in
-- process-local dicts. In any deployment with more than one process (several
-- admin replicas behind a load balancer, or ``uvicorn --workers N``) a
-- captured, correctly-signed SAML response could be re-POSTed to a SIBLING
-- process and mint a second session: the replaying process saw none of the
-- first process's records, so both the assertion-replay gate and the
-- answered-request gate failed open across replicas. This table is the store
-- every replica shares; the consume is an atomic first-wins INSERT with
-- ``ON CONFLICT DO NOTHING`` semantics (see the upsert in saml.py), so
-- exactly one presentation of an assertion ID, and one answer per
-- AuthnRequest ID, wins fleet-wide.
--
-- Two kinds of record live here, keyed by ``kind``:
--
-- * ``assertion_replay``: a consumed assertion ID; expires at the
--   assertion's NotOnOrAfter (with a fixed fallback when the assertion
--   carries none). A row past its expiry neither blocks a later claim nor
--   matters: the assertion's own timestamp validation already refuses it.
-- * ``answered_request``: an AuthnRequest ID an accepted assertion has
--   already answered; expires with the correlation cookie's 300 s window,
--   past which the cookie can no longer authenticate a presentation.
--
-- TTL ENFORCEMENT: every read and every claim carries an
-- ``expires_at > <now>`` predicate, and each claim additionally sweeps
-- expired rows (``DELETE ... WHERE expires_at <= <now>``). ``now`` is the
-- APPLICATION clock passed as a parameter, not the database clock: the
-- expiry instants are application-clock values (NotOnOrAfter read from the
-- assertion, request TTLs counted from ``time.time()``), so comparing them
-- against a second clock would turn an application/database skew into
-- longer-or-shorter-than-requested TTLs.
--
-- GROWTH: only an ACCEPTED login writes here (every write is preceded by
-- full signature and timestamp validation), rows live at most one
-- NotOnOrAfter window (minutes; the answered kind exactly 300 s), and each
-- claim sweeps expired rows. That bounds the table to roughly the accepted
-- logins of one expiry window, so no index on ``expires_at`` is built: the
-- periodic sweep's sequential scan of a table that small is cheaper than
-- maintaining a second index on a hot insert path.
--
-- ROLLING DEPLOY: additive and inert to old code. Pre-fix processes never
-- read or write this table (their records stay process-local), so it can be
-- applied while they run; a mixed fleet's cross-replica replay protection is
-- as strong as its newest replica, which is strictly better than the
-- all-process-local behavior this migration ships the fix for. Forward-only;
-- there is no down migration.
CREATE TABLE IF NOT EXISTS "attack2".saml_replay_store (
    kind text NOT NULL,
    id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (kind, id)
);

COMMENT ON TABLE "attack2".saml_replay_store IS
    'Shared cross-replica store for the SAML admin auth ID gates. Rows are '
    '(kind, id) records with an expiry: kind ''assertion_replay'' consumes a '
    'SAML assertion ID at its NotOnOrAfter (single-use assertions), kind '
    '''answered_request'' records an AuthnRequest ID an accepted assertion '
    'has already answered (single-use AuthnRequest IDs on the cookie path). '
    'Claims are atomic first-wins inserts; every read and claim predicates '
    'on expires_at against the application clock, and each claim sweeps '
    'expired rows. Only accepted logins write here.';

-- Stamp the pre-ownership disabled population as operator intent, so the
-- residual NULL reading on a DISABLED row belongs only to the mixed-version
-- deploy window (issue #460).
--
-- Why: 01.00.19_02 added cron_schedules.disabled_by and read a NULL on a
-- disabled row as "disabled before the column existed" (operator intent,
-- never reverted by a boot). That reading is safe only for rows disabled
-- BEFORE the column existed. It is not safe for rows an OLD pod disables
-- AFTER it: the previous release's failure UPDATE writes enabled=false and
-- cannot name this column, so during a mixed-version rolling deploy (this
-- chain is additive and applies while old pods run, exactly as the
-- 01.00.19_02 header says) an old pod's transient-blip auto-disable lands
-- as enabled=false, disabled_by=NULL. The boot recovery predicate required
-- disabled_by='auto', never matched that row again, and the schedule stayed
-- disabled until a human re-enabled it. That is the unrecoverable cell of
-- issue #460.
--
-- This file closes the ambiguity by POPULATION instead of by value: every
-- disabled row that exists when this statement runs (the pre-ownership
-- population the 01.00.19_02 header already declared operator intent, plus
-- anything an old pod disabled before now) is stamped 'operator' and keeps
-- the create-only semantics it was written under. After it, a disabled row
-- with disabled_by=NULL can only be an old pod's write from the rest of the
-- deploy window. The boot recovery (worker/_bootstrap.py) reads that
-- residual NULL alongside 'auto' when the row carries the old failure arm's
-- fingerprint (consecutive_failures at or past the auto-disable threshold,
-- last_fire_error set): that is an old pod's auto-disable and the boot
-- reverts it. A NULL-disabled row WITHOUT the fingerprint reads as an old
-- pod's operator disable during the window and stays untouched.
--
-- Enabled rows are left alone: a marker on an enabled row is inert (the
-- revert requires enabled=false), and old pods re-enabling a row this
-- chain stamped cannot clear the column; the next real disable overwrites
-- the marker.
--
-- OPS NOTE (locks): one UPDATE over enabled=false AND disabled_by IS NULL.
-- Schedules are predominantly enabled, so the matched set is small, and the
-- row locks run under the same ddl_lock_timeout bound every migration runs
-- under. Old pods never read this column, so the stamp cannot change their
-- behavior; the statement is idempotent (a second run matches nothing).

UPDATE "attack2".cron_schedules
SET disabled_by = 'operator'
WHERE enabled = false AND disabled_by IS NULL;

COMMENT ON COLUMN "attack2".cron_schedules.disabled_by IS
    'Who disabled this schedule: ''auto'' = the cron loop''s failure-count '
    'auto-disable (a code re-declaration at worker startup reverts it), '
    '''operator'' = a deliberate operator disable (handle, CLI, admin UI, '
    'actor deregistration; never reverted by a boot). NULL = enabled, or a '
    'row an old pod disabled during a mixed-version deploy (01.00.19_05 '
    'stamped every disabled row that predates it ''operator''; the boot '
    'revert also recovers a NULL-disabled row carrying the old failure '
    'arm''s fingerprint).';

-- The admin /history page's seam-walk indexes. Forward-only; there is no
-- down migration. To revert, drop the two indexes by hand.
--
-- ── Why these indexes exist ─────────────────────────────────────────
-- The /history walk orders the union of jobs_archive and live terminal
-- jobs by the seam tuple `(COALESCE(finished_at, '9999-12-31
-- 23:59:59+00'::timestamptz), created_at, id) DESC`. The COALESCE in the
-- sort key is the walk's NULL handling: an unfinished (or archived
-- unfinished) row's seam position is the ceiling, so a still-running row
-- sorts at the top of the audit trail and a NULL-finished row can never
-- sit on the far side of a keyset cursor that compares the same
-- COALESCE. That expression is the point: NO index over bare
-- `finished_at` can serve a sort whose leading key is a function of it,
-- so before this migration every page turn of /history read and top-N
-- sorted EVERY matching row of the archive. Measured on PostgreSQL 18.6
-- at a 100,000-row archive: 18 ms per page (first page and cursor page
-- alike, the keyset predicate bounds nothing ahead of the sort), Seq
-- Scan + Sort over ~100k rows, growing linearly with retention.
--
-- The fix is the expression the walk already sorts by, indexed: an
-- index on `(COALESCE(finished_at, ceiling), created_at, id)` turns each
-- branch's ORDER BY into a backward index scan that starts at the
-- requested seam and stops at the page size. With the /history route
-- fetching each branch sorted-and-limited and merging the (at most two
-- pages') rows, the page cost stops depending on the archive's size:
-- measured on the same shape, 18.0 ms → 0.31 ms (first page) and
-- 0.38 ms (cursor page 2), and flat as the archive grows.
--
-- ── Why the expression must match byte-for-byte ──────────────────────
-- The planner matches the index to the query's ORDER BY only when the
-- indexed expression and the query's expression are structurally the
-- same constant fold. taskq/web/admin/history.py's
-- `_HISTORY_SEAM_PREDICATE` and `_HISTORY_ORDER_BY` spell the same
-- literal (`'9999-12-31 23:59:59+00'::timestamptz`); this file spells it
-- identically. A future change to either side must change all three
-- together, and the history seam tests (the walk-exactly-once and
-- never-replay pins) fail closed if the sort and the cursor ever
-- disagree.
--
-- ── Why the live jobs table gets one too ────────────────────────────
-- The walk's second branch reads live terminal rows. A deployment that
-- archives rarely (or a small fleet) keeps its terminal history in
-- `jobs` for a long time; the branch is the same shape and the same
-- growing sort without its own index. The live table also carries the
-- dispatch-path indexes; one more small expression index costs a few
-- bytes per terminal write and nothing on the hot path (no dispatch
-- statement orders by this tuple).
--
-- OPS NOTE (locks): plain CREATE INDEX inside the migration transaction,
-- the same precedent as 01.00.19_01 (the runner serializes migrators
-- with pg_advisory_lock; CONCURRENTLY cannot run in a transaction and a
-- bare wait for old snapshots deadlocks against the advisory waiter).
-- The build locks writes on each table for the build's duration; build
-- time is proportional to the current row count of each table. Operators
-- with a very large archive can pre-build the archive's index with
-- CREATE INDEX CONCURRENTLY before applying this migration; IF NOT
-- EXISTS keeps the migration a no-op then.

CREATE INDEX IF NOT EXISTS jobs_archive_seam_idx
    ON "attack2".jobs_archive (
        (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz)),
        created_at,
        id
    );

CREATE INDEX IF NOT EXISTS jobs_seam_idx
    ON "attack2".jobs (
        (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz)),
        created_at,
        id
    );

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
CREATE TABLE IF NOT EXISTS "attack2".job_events_prune_state (
    singleton          boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    pruned_through_id  bigint NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

INSERT INTO "attack2".job_events_prune_state (singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;

COMMENT ON TABLE "attack2".job_events_prune_state IS
    'Event-prune watermark: the highest job_events id any retention deleter '
    'has committed a delete below-or-at. A trailing-watermark consumer whose '
    'persisted cursor sits strictly below this value has lost undelivered '
    'events to retention and must be failed visible, not silently skipped.';

-- progress_seq: int -> bigint.
--
-- Why: the column is the job's strict monotone progress-write cursor, and
-- every writer ADVANCES it by arithmetic -- the flush's
-- ``progress_seq = j.progress_seq + f.seq_delta`` and the terminal writes'
-- absolute ``GREATEST(progress_seq, $n)`` SETs. On an int4 column the
-- arithmetic overflows at 2147483647 (SQLSTATE 22003, "integer out of
-- range"), and the failure mode is the crash-reclaim loop: the flush
-- UPDATE errors every tick, then the terminal write errors and the
-- terminal-write classification reads the 22003 (a PostgresError) as
-- TRANSIENT infrastructure failure, so the job never terminalises, the
-- lease sweep reclaims it, the actor re-executes (its committed side
-- effects included), produces more progress, and overflows again -- a
-- permanently stuck ``running`` row, reached by one chatty actor calling
-- ``ctx.progress()`` ~2^31 times (days at a few thousand calls/second).
-- No Python-side gate can close this: the overflow is the STORAGE
-- domain, so the honest bound is the column's.
--
-- The migration is a plain ALTER TYPE: int4 -> bigint REWRITES each table
-- under ACCESS EXCLUSIVE. That is a full table rewrite, not an in-place
-- widening: every row is copied into a fresh relfilenode and EVERY index
-- on the table is rebuilt, including the indexes that never touch
-- progress_seq (a Postgres behaviour of ALTER TYPE, not a choice), so
-- budget an ops window that scales with the live row count (measured
-- ~1.4 s at 2M jobs on the reference container). Both tables are bounded
-- (jobs by the retention sweep, jobs_archive by archive retention), so
-- the rewrite is an ops-window statement, not a live-traffic risk; run
-- with the same ddl lock timeout discipline every migration runs under.
-- A rolled-back or interrupted run is simply re-run (the runner records
-- completion only after the whole file succeeds).
--
-- The JS-side companion bound this creates (documented, not fixed): the
-- admin portal's progress driver compares progress_seq values as JS
-- numbers, which are exact only up to 2^53 -- above that, seq n and
-- n + 1 can compare equal and a tick is dropped as a duplicate. Reaching
-- 2^53 progress writes on ONE job is ~4 billion times this migration's
-- motivating overflow; at a sustained 1000 progress calls/second it is
-- ~285 years for a single job. The driver documents the bound where the
-- comparison happens (web/static/realtime.js, acceptProgress), and the
-- poll wire keeps the exact decimal in the ETag header for clients that
-- need exactness at any magnitude.

ALTER TABLE "attack2".jobs ALTER COLUMN progress_seq TYPE bigint;

ALTER TABLE "attack2".jobs_archive ALTER COLUMN progress_seq TYPE bigint;

-- The per-attempt due-time stamp: when each attempt was DUE, so a retry
-- chain's wait spans are reconstructable from the ledger alone. Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- ── Why this column exists ────────────────────────────────────────────
-- Every attempt of a retrying job is claimed against a due time: the
-- ``jobs.scheduled_at`` standing when the dispatch claim took the row. That
-- value is overwritten by the NEXT retry's reschedule
-- (``mark_retry``/``mark_retry_after``/the reclaim arms set
-- ``scheduled_at = clock_timestamp() + delay`` for the following attempt), so
-- after the fact only the FIRST attempt's wait is reconstructable
-- (``started_at - scheduled_at`` on the terminal row); attempts 2..N have no
-- surviving due time. The attempt ledger carries where each execution started
-- and ended, never what it was waiting FOR, which is exactly the span an
-- operational-insights read of retry chains needs (queue wait per attempt,
-- backlog vs. retry-backoff attribution).
--
-- ── Shape ─────────────────────────────────────────────────────────────
-- Nullable, no default, no backfill: the due time of a historical attempt no
-- longer exists anywhere to recover - ``jobs.scheduled_at`` was overwritten by
-- every subsequent reschedule, so a backfill would fabricate data, and none is
-- attempted. NULL is therefore the documented "pre-migration attempt (or a
-- writer that could not know the due time)" marker, the same NULL-semantics
-- discipline as 01.00.13_01's never-claimed stamp.
--
-- Stamping: the attempt row itself is born at its terminal write (the fused
-- ``mark_*`` statements, the reclaim sweeps' batched INSERTs, the isolate
-- write - the dispatch claim stamps ``jobs.started_at`` and the attempt rows
-- are written from that row's stamps). Every attempt-row writer sources
-- ``due_at`` from the job row's ``scheduled_at``:
--
-- - on the arms that do not reschedule, ``scheduled_at`` is untouched since
--   the claim, so the terminal write reads it straight off the updated row;
-- - on the reschedule arms that ALSO write an attempt row (``mark_retry``'s
--   retried arm, ``mark_retry_after_consume_true``'s snoozed arm), the
--   statement pre-reads ``scheduled_at`` in a same-snapshot CTE: all parts of
--   one statement share one snapshot, so the pre-read is the claim-time value
--   even though the RETURNING exposes the NEW reschedule. The next attempt's
--   terminal write then reads the rescheduled value, and the chain
--   reconstructs: ``due_at(k) -> started_at(k) -> due_at(k+1) -> ...``.
-- - the reclaim sweeps' snap CTEs and the isolate path's SELECT read
--   ``scheduled_at`` before their re-pend reschedules it, and carry it to the
--   batched INSERTs as an array parameter.
--
-- The ALTER is metadata-only (no table rewrite) on this Postgres generation.
-- Old-release code never names the column: every attempt writer in the tree
-- names its columns explicitly, so the additive discipline holds for the
-- rolling-deploy window in both directions that the window supports. Rows
-- written during the window by old workers read NULL until the new code
-- replaces them - the same documented NULL semantics, arrived at by deploy
-- order rather than by data loss.
--
-- No index: the column serves per-job chain reconstruction (reads keyed on
-- ``job_id``, served by the primary key) and archive analytics that already
-- scan by time; no shipped query filters or orders on ``due_at`` alone, and
-- the archive twin's hypertable conversion keeps ``started_at`` as its chunk
-- key untouched.
ALTER TABLE "attack2".job_attempts
    ADD COLUMN IF NOT EXISTS due_at timestamptz;
ALTER TABLE "attack2".job_attempts_archive
    ADD COLUMN IF NOT EXISTS due_at timestamptz;

COMMENT ON COLUMN "attack2".job_attempts.due_at IS
    'The jobs.scheduled_at this attempt was claimed against (the due time '
    'the dispatch claim took the row at), stamped by the attempt-row '
    'writers at the attempt''s terminal transition; on reschedule arms the '
    'statement pre-reads scheduled_at so the value is the claim-time due '
    'time, never the next attempt''s. NULL = a pre-migration attempt (or a '
    'writer that could not know it): historical due times are unrecoverable, '
    'so there is no backfill. A retry chain reconstructs as '
    'due_at(k) -> started_at(k) -> due_at(k+1).';
COMMENT ON COLUMN "attack2".job_attempts_archive.due_at IS
    'Mirror of job_attempts.due_at, carried by the prune sweep''s '
    'column-explicit archive INSERT. Same NULL semantics: pre-migration '
    'attempts are NULL, never backfilled.';

-- The admin archive tab's keyset page index: (finished_at DESC NULLS LAST,
-- id DESC). Forward-only; there is no down migration. To revert, drop the
-- index by hand.
--
-- ── Why this index exists ────────────────────────────────────────────
-- The archive tab's list walks the archive newest-first: its ordering is
-- `finished_at DESC NULLS LAST, id DESC` (taskq/web/admin/jobs.py's
-- `_SORTABLE_ARCHIVE`, the nullable flag rendering NULLS LAST), the
-- keyset cursor comparing the same tuple (backend/_cursor.py's
-- `_row_wise_sql`). The shipped indexes cannot serve that ordering in
-- ANY scan direction:
--
-- * `jobs_archive_finished_at_idx (finished_at)` — backward scan yields
--   `finished_at DESC NULLS FIRST` (a backward scan reverses the null
--   placement along with the values); the query needs NULLS LAST. The
--   planner therefore cannot use it for the ordering.
-- * the /history seam indexes (01.00.20_01) index the COALESCE tuple the
--   /history walk sorts by — a different expression with different
--   trailing keys, matched only by that walk's statements.
--
-- So EVERY archive-tab page — first page and cursor page alike, the
-- keyset predicate bounds nothing ahead of the sort — plans as a full
-- Sort of every matching row, and the page cost is linear in the
-- archive's depth: the walk is quadratic. Measured on the timescaledb
-- trade-off corpus (benchmarks/results/timescale-tradeoffs-sweep.json,
-- the sweep's plain-engine points): 3.81 ms per page at a 10,000-row
-- archive, 23.47 ms at 100k, 55.92 ms at 400k, 135.82 ms at 2M —
-- ~0.068 ms per 1,000 archive rows, every page, at every depth; a
-- 10,000-page back-scroll of a 10M-row archive would pay ~680 ms PER
-- PAGE. The campaign's red/green legs at the 10M scale
-- (benchmarks/results/archive-scale.json, pagination_pathology): the
-- red page costs 690/690/662/447 ms at the 0/51k/510k/5.1M-depth
-- checkpoints, the green page 0.47/0.48/0.48/0.39 ms — a 1,000x+
-- ratio, flat at every depth. At the campaign's 100k red probe
-- (benchmarks/results/archive-scale-red-probe.json): 16.61 ms per page
-- (Gather Merge over a full sort) → 0.12 ms with this index.
--
-- This is the archive tab's own instance of the pathology 01.00.20_01
-- fixed for /history: the walk sorts by an expression no index matched.
-- The fix is the same shape — index the tuple the walk actually sorts
-- by — applied to the archive tab's ordering. The live jobs tab's
-- default page (`created_at DESC, id DESC`, non-nullable) is served by a
-- backward scan of ANY (created_at)-leading index and is a separate
-- question this migration does not touch.
--
-- ── Why the id tiebreaker is in the key ──────────────────────────────
-- The keyset cursor is the two-tuple (finished_at, id); ordering id with
-- the leading column is what makes the seam a single row-wise
-- comparison (backend/_cursor.py's `_row_wise_sql` docstring). Without
-- id in the index the page walk re-filters equal-finished_at rows per
-- page — the tiebreak range has no index support and the page cost
-- degrades with the tie width.

CREATE INDEX IF NOT EXISTS jobs_archive_page_idx
    ON "attack2".jobs_archive (finished_at DESC NULLS LAST, id DESC);

-- OPS NOTE (locks): plain CREATE INDEX inside the migration transaction,
-- the same precedent as 01.00.19_01 and 01.00.20_01 (the runner
-- serializes migrators with pg_advisory_lock; CONCURRENTLY cannot run in
-- a transaction). The build locks writes on jobs_archive for the build's
-- duration; build time is proportional to the current archive row
-- count. Operators with a very large archive can pre-build with CREATE
-- INDEX CONCURRENTLY before applying this migration; IF NOT EXISTS keeps
-- the migration a no-op then.

-- Converges the fence-probe index to the two-key form on databases that
-- applied 01.00.19_01 as originally shipped. The original file's
-- definition-conditional drop read pg_attribute, which cannot tell a key
-- column from an INCLUDE payload column: an INCLUDE(id) form and INVALID
-- debris from an interrupted CREATE INDEX CONCURRENTLY were both spared,
-- and the trailing CREATE INDEX IF NOT EXISTS then no-oped — the canonical
-- name left owning an index whose trailing `id` rides as payload (never an
-- Index Cond: the plan pathology this index exists to fix) or is not valid
-- at all. Databases that ran the original therefore may hold any of the
-- six owner states; this file's condition reads pg_index — spares only a
-- valid, ready index whose KEY columns (indkey[:indnkeyatts]) carry `id`,
-- drops and rebuilds everything else. On a database already at the
-- finished two-key form this is a no-op; on a fresh database the restored
-- 01.00.19_01 lands the two-key form and this file no-ops behind it.
--
-- OPS NOTE (escape hatch), for operators with a large jobs table — same
-- shape as 01.00.19_01's: a bare CONCURRENTLY pre-build under the
-- canonical name would be destroyed by this file's own drop before its
-- IF NOT EXISTS could no-op, so pre-build under a scratch name and swap
-- by name in one transaction:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS
--   jobs_locked_by_worker_running_idx_new ON "attack2".jobs
--   (locked_by_worker, id) WHERE status = 'running';
--
--   BEGIN;
--   ALTER INDEX "attack2".jobs_locked_by_worker_running_idx
--       RENAME TO jobs_locked_by_worker_running_idx_old;
--   ALTER INDEX "attack2".jobs_locked_by_worker_running_idx_new
--       RENAME TO jobs_locked_by_worker_running_idx;
--   COMMIT;
--
-- then apply this migration normally: the conditional drop spares the
-- valid two-key form, `..._old` is dropped, the CREATE no-ops.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'attack2'
          AND c.relname = 'jobs_locked_by_worker_running_idx'
          AND NOT EXISTS (
              -- spare the drop only while a VALID, READY index whose KEY
              -- columns carry `id` owns the canonical name. The key columns
              -- come from pg_index, not pg_attribute: pg_attribute cannot
              -- tell a key column from an INCLUDE payload column, and an
              -- INCLUDE(id) form is not the two-key form this index exists
              -- to land (the trailing id must ride in the index as an Index
              -- Cond, never payload). indisvalid/indisready: an interrupted
              -- CREATE INDEX CONCURRENTLY leaves INVALID debris that the
              -- IF NOT EXISTS below would silently keep.
              SELECT 1
              FROM pg_catalog.pg_index i
              JOIN pg_catalog.pg_attribute a
                ON a.attrelid = i.indrelid
               AND a.attname = 'id'
               AND a.attisdropped = false
              WHERE i.indexrelid = c.oid
                AND i.indisvalid
                AND i.indisready
                AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1])
          )
    ) THEN
        EXECUTE 'DROP INDEX "attack2".jobs_locked_by_worker_running_idx';
    END IF;
END
$$;
DROP INDEX IF EXISTS "attack2".jobs_locked_by_worker_running_idx_old;
CREATE INDEX IF NOT EXISTS jobs_locked_by_worker_running_idx
    ON "attack2".jobs (locked_by_worker, id)
    WHERE status = 'running';

-- Converges the batches table's column CHECK constraints across every
-- deployment vintage. The released 01.00.05_01 (38b328db, shipped in a
-- release) declares them in its CREATE TABLE, so databases that applied
-- the released file already have them; databases that applied the
-- ORIGINAL file (pre-release source pins) do not. This file adds them
-- where absent — a no-op where present — so every database ends in the
-- same enforced shape:
--
--   expected_size        >= 0
--   consecutive_failures >= 0
--   failure_threshold    IS NULL OR >= 1
--
-- Transactional and idempotent by guard: the constraint names are checked
-- against pg_constraint before each ADD (the released population's
-- auto-named constraints match these names exactly), so the file is safe
-- on every vintage and re-runnable after a rollback. ADD CONSTRAINT takes
-- an ACCESS EXCLUSIVE lock for its validation scan, bounded by the
-- runner's ddl_lock_timeout; batches tables are small (one row per
-- batch), so the scan is milliseconds. For a fleet-scale batches table,
-- pre-stage with the documented CONCURRENTLY swap pattern from
-- 01.00.19_01's OPS NOTE before upgrading.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'attack2'
          AND t.relname = 'batches'
          AND c.conname = 'batches_expected_size_check'
    ) THEN
        EXECUTE 'ALTER TABLE "attack2".batches
            ADD CONSTRAINT batches_expected_size_check
            CHECK (expected_size >= 0)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'attack2'
          AND t.relname = 'batches'
          AND c.conname = 'batches_consecutive_failures_check'
    ) THEN
        EXECUTE 'ALTER TABLE "attack2".batches
            ADD CONSTRAINT batches_consecutive_failures_check
            CHECK (consecutive_failures >= 0)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint c
        JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'attack2'
          AND t.relname = 'batches'
          AND c.conname = 'batches_failure_threshold_check'
    ) THEN
        EXECUTE 'ALTER TABLE "attack2".batches
            ADD CONSTRAINT batches_failure_threshold_check
            CHECK (failure_threshold IS NULL OR failure_threshold >= 1)';
    END IF;
END
$$;

-- Expression statistics for the dispatch claim's cohort-key expression.
-- Forward-only; there is no down migration. To revert, drop the statistics
-- object by hand:
--
--   DROP STATISTICS "attack2".jobs_dispatch_cohort_stats;
--
-- ── Why this statistics object exists ─────────────────────────────────
-- The dispatch claim's candidates laterals probe their per-cohort
-- populations with an ORDER BY + LIMIT walk over the dispatch partial
-- indexes (backend/_dispatch_sql.py: the strict-FIFO lateral rides
-- jobs_unrouted_actor_dispatch_idx, the round-robin and re-pended
-- laterals ride jobs_unrouted_round_robin_probe_idx /
-- jobs_assignment_routed_probe_idx). Both re-pended arms and the
-- round-robin label-routed arm constrain their probe with the cohort
-- equality `COALESCE(fairness_key, '__null__') = <key>` — the VERBATIM
-- expression of the index's second key column, so the qual is an Index
-- Cond and the probe is an index-ordered walk that stops at its LIMIT.
--
-- Whether the walk stays a walk is the planner's per-path cost
-- comparison, and the comparison runs on estimates: the ordered walk is
-- priced at the probe window (limit_n * oversample heap fetches), while
-- the bitmap+sort alternative is priced at the probe's index RANGE — a
-- range whose row estimate is dominated by the selectivity of the
-- COALESCE expression. A bare column has column statistics; an
-- expression has NONE until a statistics object names it, and the
-- planner then prices the expression equality at the default eqsel
-- (0.005). On a deep re-pended backlog that under-prices the bitmap by
-- three orders of magnitude, and the bounded walk loses to a plan that
-- visits the whole cohort range:
--
--   Bitmap Heap Scan on jobs: est=49 cost=222 act=17,820 loops=5
--     Filter=(schedule_to_close IS NULL OR schedule_to_close > now())
--     Bitmap Index Scan: est=51 cost=27 act=18,764 loops=5
--       IndexCond=(actor = tk.actor AND COALESCE(fairness_key, '__null__') = tk.fkey
--                  AND scheduled_at <= statement_timestamp())
--
-- measured on PostgreSQL 18.6 (EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON))
-- against a 94,010-row pending, assignment-routed, NULL-fairness-key
-- backlog across 5 actors (the deep-backlog audit corpus): the claim
-- round visited 18,764 index entries + 28,114 heap blocks per probe
-- (94k row visits, 28k blocks, 57-75 ms) to claim 50 jobs — the
-- planner believed the range held 51 rows because
-- sel(COALESCE(...)) collapsed to the 0.005 default, while the honest
-- walk's estimate (~100 fetches) priced 20x higher. With the expression
-- statistics collected, the same corpus plans the SAME statement as the
-- bounded ordered walk: 3.8-4.6 ms per round, index entries visited at
-- the window bound, buffers at the row count. The estimate is the
-- defect: the flip is only reachable where the range estimate is
-- garbage-small, and statistics make it honest.
--
-- The same arithmetic protects the many-cohort regime: with per-cohort
-- MCV/ndistinct statistics the bitmap alternative is priced at the
-- cohort's REAL range and wins only where the range is genuinely small
-- (where its full-range visit is bounded anyway), so the depth contract
-- holds at shallow and deep backlog alike.
--
-- Out of scope: the sliding_locked re-join rides the materialized
-- `ranked` window via the one-shot id array + pkey probes and carries
-- no cohort-equality probe, so this flip mechanism doesn't reach it.
--
-- ── Why statistics, not a template restructure ───────────────────────
-- The probe's plan choice is an estimate comparison no SQL shape can
-- arbitrate from the template alone. Shapes evaluated against the audit
-- corpus, all preserving the selection semantics exactly:
--
-- * rewriting the window qual as
--   `COALESCE(schedule_to_close, 'infinity') > statement_timestamp()`
--   (one qual, no OR) — the ordered estimate is unchanged; the flip
--   persists;
-- * splitting the probe into per-arm subqueries — the cohort equality
--   is the index's own key expression; removing it from the Index Cond
--   degrades every probe to an actor-wide range walk;
-- * a two-step shape (index-only id walk, then pkey fetches for the
--   payload columns) — the pkey probes price at 100 x rpc and lose to
--   the same garbage-small bitmap estimate;
-- * unfolding the scan LIMIT (subquery bound) — re-opens the
--   depth-proportional estimate cascade the shipped folded bound exists
--   to close (docs/design/sql-hotpath-followups.md §1, the JIT oracle).
--
-- The ordered walk's estimate (~window fetches) and the bitmap's
-- estimate (range x selectivity) can only meet honestly when the
-- expression's selectivity is measured. This file is that measurement.
--
-- ── Why the ANALYZE runs in this migration ────────────────────────────
-- A statistics object is inert until collected: without a populated
-- entry the planner keeps the default eqsel and the flip persists until
-- autovacuum's next ANALYZE happens to run. The migration therefore
-- collects it. ANALYZE takes ShareUpdateExclusiveLock on jobs (blocks
-- other maintenance, never reads or writes), is sampling-bounded (~300
-- x statistics target rows, not table-size-linear), and its lock
-- acquisition is bounded by the runner's ddl_lock_timeout like every
-- other statement in the file. On a busy table autovacuum may already
-- hold the lock: the migration fails cleanly and re-runs, the same
-- doctrine as every sibling file's lock caveat.

CREATE STATISTICS IF NOT EXISTS "attack2".jobs_dispatch_cohort_stats
    ON (COALESCE(fairness_key, '__null__'))
    FROM "attack2".jobs;

ANALYZE "attack2".jobs;

-- Workflow columns on `jobs` (and the `jobs_archive` mirror) — T03, the
-- measured-tax migration (§16). Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "attack2" token is substituted
-- at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- This round is SPLIT into three single-lock-class files because the
-- migration runner (src/taskq/migrate.py) wraps each FILE in one
-- transaction: a file's statements share ONE write-block window, so lock
-- classes must not mix within a file (the estate's own structural pin,
-- tests/test_migration_lock_scope_dead_index.py family 1, convicts the
-- mixed form — the ACCESS EXCLUSIVE-across-everything shape).
--
--   01.00.23_01_pre_workflow_columns.sql  THIS FILE: ONLY the metadata-only
--                                         ALTER TABLE ... ADD COLUMN
--                                         statements (ACCESS EXCLUSIVE,
--                                         held for milliseconds on the
--                                         catalog — no table rewrite: every
--                                         column is NULL-able or NOT NULL
--                                         with a constant default, so
--                                         Postgres skips the rewrite).
--   01.00.23_02_pre_workflow_tables.sql   ONLY the CREATE TABLE statements
--                                         (wf_edge, wf_join_fire, wf_outbox,
--                                         wf_step_ledger).
--   01.00.23_03_pre_workflow_indexes.sql  ONLY the CREATE INDEX statements.
--
-- ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1 (a
-- forever rule): a pre-workflow worker reads the row shape unchanged (the
-- rollback rule, §22.3). Deployment sequence:
--   1. `taskq migrate up --phase pre` (this round) — safe before/during the
--      code rollout; old code keeps working unmodified (it never reads the
--      new columns; see the RED-FIRST dispatch-claim pin for the one
--      deliberate semantic no-op, `AND deps_pending = 0`).
--   2. Roll out the release's code.
--
-- DEPENDENCY NOTE — the #674 substrate: this round is the substrate for the
-- workflow graph columns and carries `parent_id` itself (the #674
-- parent-pointer column is NOT in this checkout at apply time — the
-- wave's numbers were consumed elsewhere; this file owns the column, its
-- NOT NULL-on-children doctrine is enforced by the engine, not the schema:
-- the graph is FK-less per the no-FK decision, parent truth is the COLUMN).
--
-- OPS NOTE — locking impact: five ADD COLUMNs against `jobs` + five against
-- `jobs_archive`, all metadata-only (no rewrite), each taking ACCESS
-- EXCLUSIVE for the duration of the catalog change only. Brief even on a
-- large table; apply clear of the busiest dispatch second if paranoid.

ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS parent_id uuid NULL;
-- The join counter (a CACHE of "un-terminal parents" — the wf_edge ledger
-- is TRUTH; see the engine's COUNTER-AS-CACHE / LEDGER-AS-TRUTH rule).
-- NOT NULL DEFAULT 0: vanilla rows are born join-free, and the dispatch
-- claim's `AND deps_pending = 0` exclusion is a semantic no-op for them.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS deps_pending smallint NOT NULL DEFAULT 0;
-- The fan-out slot. Retry-in-place preserves (parent_id, map_index).
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS map_index smallint NULL;
-- The positional node key. Never changes across holds/retries/reclaims.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS step_key text NULL;
-- The per-attempt code-version RECORD (§22.1: mixed-version joins legal
-- with hashes recorded). A RECORD, not a cache — no cache_key machinery.
-- Written at claim by the workflow claim path; computed via the tors
-- canonical content hash.
ALTER TABLE "attack2".jobs
    ADD COLUMN IF NOT EXISTS code_version text NULL;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and 01.00.03_01's same-shape mirror). The archive sweep's INSERT names
-- its columns explicitly (COPY_FROM_COLUMNS), so these stay NULL/0 on
-- archived rows until the workflow-aware pruner (T18) defines their
-- retention; the mirror keeps the row-shape doctrine intact.
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS parent_id uuid NULL;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS deps_pending smallint NOT NULL DEFAULT 0;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS map_index smallint NULL;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS step_key text NULL;
ALTER TABLE "attack2".jobs_archive
    ADD COLUMN IF NOT EXISTS code_version text NULL;

COMMENT ON COLUMN "attack2".jobs.parent_id IS
    'Workflow graph parent pointer (uuid, FK-less by the no-FK decision — the graph is enforced by the engine, never by database FKs). NOT NULL on workflow child rows; NULL only for roots. The parent COLUMN is the only parent truth: nothing may derive it from a node-id string shape.';
COMMENT ON COLUMN "attack2".jobs.deps_pending IS
    'Join counter: a CACHE of the row''s un-terminal parents. The wf_edge ledger is truth. Joined nodes wait as status=''pending'' + deps_pending > 0 (metadata.blocking_reason=''join'') — no new ENUM value. The dispatch claim excludes rows with deps_pending > 0; a semantic no-op for vanilla rows (DEFAULT 0).';
COMMENT ON COLUMN "attack2".jobs.map_index IS
    'Fan-out slot. Retry-in-place preserves (parent_id, map_index); a retried map child claims the same row and its ledger result.';
COMMENT ON COLUMN "attack2".jobs.step_key IS
    'The positional node key: never changes across holds/retries/reclaims. NULL on vanilla rows.';
COMMENT ON COLUMN "attack2".jobs.code_version IS
    'The per-attempt code-version RECORD (§22.1): computed at claim via the tors canonical content hash. A record, not a cache — there is deliberately no cache_key/content-addressed machinery here.';

-- The workflow ledger tables — T03 (the engine's exactly-once fire ledger,
-- the edge ledger, the delivery outbox, the step ledger). Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "attack2" token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENTS (each takes its own
-- relation lock on a NEW table — no ALTERs, no index builds mixed in).
-- See 01.00.23_01's header for the round's three-file split and the
-- deployment sequence. All DDL is additive (no post_ phase, the v1 forever
-- rule); the tables are new, so the lock class is CREATE TABLE's own.
--
-- ID GENERATION (the estate invariant, T03's one-line rule): every table's
-- `id uuid PRIMARY KEY` is generated APP-SIDE through the seam
-- `taskq._ids.new_uuid()` (uuid7, time-ordered) — NEVER DB-side
-- `gen_random_uuid()`, NEVER uuid4 (the TID251 discipline; a pin greps this
-- package's DDL). Time-ordered ids land at the right-hand edge of the PK
-- B-tree (page-stable appends) and `ORDER BY id` is a usable creation
-- order (the sweep's recount, the drain, the admin's keyset pagination).
--
-- FK-LESS: no foreign keys anywhere in this round, per the estate's no-FK
-- decision — the graph is enforced by the engine, never by the schema.

-- ── wf_edge: THE EDGE LEDGER — the join counter's truth ─────────────────
-- One row per (child, parent) dependency edge. The engine's
-- COUNTER-AS-CACHE / LEDGER-AS-TRUTH rule: `jobs.deps_pending` is a cache;
-- the ledger is the only count source (`remaining = join_target − committed
-- decrements` — NEVER child-row presence: nested joins' children do not
-- exist yet at count time, which is exactly where child-row counting fires
-- an empty join early). The sweep re-derives the cache from this ledger.
CREATE TABLE "attack2".wf_edge (
    child_id  uuid NOT NULL,
    parent_id uuid NOT NULL,
    flow_id   uuid NOT NULL,
    PRIMARY KEY (child_id, parent_id)
);

-- ── wf_join_fire: the exactly-once FIRE ledger ───────────────────────────
-- At most one fire row per joined node, ever: the UNIQUE constraint on
-- join_job_id is what rejects a double fire (the PK on id keeps the estate's
-- uuid7 invariant; the fire guard's correctness rides the UNIQUE). The
-- proto's `child_id PRIMARY KEY` shape maps to it at port time.
CREATE TABLE "attack2".wf_join_fire (
    id          uuid PRIMARY KEY,
    join_job_id uuid NOT NULL,
    flow_id     uuid NOT NULL,
    step_key    text NOT NULL,
    fired_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    fired_by    text NOT NULL DEFAULT 'finalize',
    UNIQUE (join_job_id)
);

-- ── wf_outbox: the fired join's delivery queue ───────────────────────────
-- A fired join's consumer bindings ride here; the drain (a sweep arm)
-- inserts the consumer rows IDEMPOTENTLY (ON CONFLICT on the consumer step
-- key, via the jobs composite idempotency arbiter) and flips the undelivered
-- flag in the insert's transaction — the delivery half of the exactly-once
-- fire. Shape follows the proven proto outbox + the job_events outbox
-- precedent (01.00.02_01).
CREATE TABLE "attack2".wf_outbox (
    id                uuid PRIMARY KEY,
    join_job_id       uuid NOT NULL,
    flow_id           uuid NOT NULL,
    consumer_step_key text NOT NULL,
    map_index         smallint NULL,
    bindings          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    delivered         boolean NOT NULL DEFAULT false,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- ── wf_step_ledger: the step ledger (the exactly-once attempt record) ────
-- The ledger `attempt` increments at claim, the only grant of work; the
-- claim arbiter (the CREATE UNIQUE INDEX in 01.00.23_03,
-- wf_step_ledger_claim_uniq) physically blocks double-recording (P3 rule 2).
-- THE ARBITER KEYS ON COALESCE(map_index, -1): map children of one step key
-- are DIFFERENT claims per T05's key contract — a bare
-- UNIQUE (flow_id, step_key, attempt) collapses them onto ONE row (the
-- terminal write overwrites, the memoized replay returns the wrong child's
-- result — the ATTACK-FIXED shape; this round is NOT landed anywhere, so the
-- constraint was amended in place, the unique index lives in _03 beside the
-- round's other index builds). The ledger's terminal-outcome write rides
-- the finalize's own transaction (the ledger-terminal-atomic rule). The
-- failure IO-capture (the `capture` jsonb) is written at failure-finalize
-- per the workflow's none|errors-only|all policy, AFTER the redact chain
-- (chain → hook, unconditional) — the capture row must never carry a
-- canary.
CREATE TABLE "attack2".wf_step_ledger (
    id            uuid PRIMARY KEY,
    flow_id       uuid NOT NULL,
    job_id        uuid NOT NULL,
    step_key      text NOT NULL,
    map_index     smallint NULL,
    attempt       integer NOT NULL,
    status        text NOT NULL,
    result        jsonb NULL,
    error_class   text NULL,
    error_message text NULL,
    capture       jsonb NULL,
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE "attack2".wf_edge IS
    'The workflow edge ledger: one row per dependency edge. The join counter''s ONLY truth source (deps_pending is a cache the sweep reconciles from here).';
COMMENT ON TABLE "attack2".wf_join_fire IS
    'The workflow exactly-once fire ledger: at most one fire per joined node, ever (UNIQUE(join_job_id) rejects the double fire; the rowcount gate prevents premature ones).';
COMMENT ON TABLE "attack2".wf_outbox IS
    'The workflow delivery outbox: a fired join''s consumer bindings; the drain inserts consumer rows idempotently and flips the flag in the insert''s transaction.';
COMMENT ON TABLE "attack2".wf_step_ledger IS
    'The workflow step ledger: one row per (flow, step, map_index, attempt). The claim arbiter UNIQUE(flow_id, step_key, COALESCE(map_index,-1), attempt) physically blocks double-recording; terminal writes ride the finalize''s own transaction.';

-- The workflow indexes — T03. Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "attack2" token is substituted
-- at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENTS (SHARE lock class —
-- blocks writes for each build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed into the window). See 01.00.23_01's header for the
-- round's three-file split and the deployment sequence.
--
-- THE PARTIAL-INDEX DOCTRINE (§16.4, the measured truth): every workflow
-- index is PARTIAL — scoped to workflow rows only — so the vanilla
-- maintenance exemption is the point: the measured partial is 0.5% of the
-- same-column full index (194×; the pin asserts ≤ 1%).
--
-- Workflow rows are identified by `step_key IS NOT NULL` (vanilla rows never
-- set it). Join-wait rows are `status='pending' AND deps_pending > 0`.

-- The sweep's lock-first scan target: join-wait rows only. Tiny by
-- construction (a healthy fleet's join-wait population is in-flight joins);
-- the sweep's ordered walk rides it.
CREATE INDEX IF NOT EXISTS jobs_wf_join_wait_idx
    ON "attack2".jobs (id)
    WHERE status = 'pending' AND deps_pending > 0;

-- The sweep's FIRE-arm probe (SWEEP_FIRE_SQL's locked CTE): join-wait rows
-- REGARDLESS of the counter. deps_pending > 0 must NOT be in this
-- predicate's definition (the SWEEP_FIRE_SQL index-served question): the
-- fire arm identifies its rows by the LEDGER's count, never by the cache —
-- the rederive statement in the same transaction just reconciled the
-- firable rows' cache to 0, so a counter-carrying predicate would hide
-- exactly the rows the arm exists to fire. The two partials split the
-- population by which arm's WHERE each serves: the rederive's lock-first
-- scan keeps jobs_wf_join_wait_idx (its WHERE implies deps_pending > 0),
-- the fire's scan rides this one.
CREATE INDEX IF NOT EXISTS jobs_wf_join_fire_probe_idx
    ON "attack2".jobs (id)
    WHERE status = 'pending' AND metadata @> '{{"blocking_reason": "join"}}'::jsonb;

-- Children by parent: the fork-debt reconcile, the map's retry-in-place
-- lookup, and the status rollup's per-parent reads. Partial on workflow
-- child rows only.
CREATE INDEX IF NOT EXISTS jobs_wf_children_idx
    ON "attack2".jobs (parent_id, id)
    WHERE parent_id IS NOT NULL;

-- The edge ledger's parent probe: the sweep's set-based re-derive counts
-- un-terminal parents per child by walking this index (the child side is
-- wf_edge's PK prefix). A Seq Scan here is the 83.7 ms unscoped monster
-- shape — the scope pin convicts it.
CREATE INDEX IF NOT EXISTS wf_edge_parent_idx
    ON "attack2".wf_edge (parent_id);

-- The drain's undelivered scan: the proto's outbox_undelivered_idx shape.
CREATE INDEX IF NOT EXISTS wf_outbox_undelivered_idx
    ON "attack2".wf_outbox (id)
    WHERE NOT delivered;

-- The ledger's per-flow reconstruction read (rows-only status rebuild, the
-- admin timeline, the drain of phantom 'running' rows). The claim arbiter
-- (wf_step_ledger_claim_uniq, below) covers the (flow_id, …) prefix; this
-- covers the per-job lookup the finalize fence and the claim path use.
CREATE INDEX IF NOT EXISTS wf_step_ledger_job_idx
    ON "attack2".wf_step_ledger (job_id);

-- THE STEP LEDGER'S CLAIM ARBITER (T05's key contract, attack-hardened):
-- one row per (flow, step, map child, attempt) — map children of one step
-- key are DIFFERENT claims, so the arbiter keys on COALESCE(map_index, -1)
-- (the non-map step's NULL folds to -1; a bare
-- UNIQUE (flow_id, step_key, attempt) collapses map children onto ONE row:
-- the terminal write overwrites, the memoized replay returns the wrong
-- child's result). The expression matches LEDGER_CLAIM_SQL's
-- `ON CONFLICT (flow_id, step_key, COALESCE(map_index, -1), attempt)`
-- verbatim — an ON CONFLICT target infers only an arbiter spelled with the
-- identical expression.
CREATE UNIQUE INDEX IF NOT EXISTS wf_step_ledger_claim_uniq
    ON "attack2".wf_step_ledger (flow_id, step_key, COALESCE(map_index, -1), attempt);

-- The phantom reaper's scan target (PHANTOM_REAP_SQL): 'running' rows only
-- — the arm's whole population, tiny by construction (in-flight attempts),
-- so the reap's EXISTS over terminal flows probes this partial instead of
-- scanning the ledger.
CREATE INDEX IF NOT EXISTS wf_step_ledger_running_idx
    ON "attack2".wf_step_ledger (flow_id)
    WHERE status = 'running';

-- OPS NOTE — locking impact: each build takes SHARE on its table (blocks
-- writes, never reads) for the build's duration. A build scans the whole
-- table to evaluate the partial predicate per row, so the two `jobs` builds
-- scale with the jobs row count like any full index — the PARTIAL savings
-- are in steady-state size and maintenance (the measured 194×), not in the
-- one-time build. Apply clear of the busiest dispatch window on a large
-- `jobs` table, the same guidance 01.00.03_01 gave its unique-index build.

-- The failure-policy column on the edge ledger — T06 (the failed-parent
-- propagation). Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "attack2" token is substituted at
-- apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE METADATA-ONLY ALTER (ACCESS EXCLUSIVE, held for
-- milliseconds on the catalog — no table rewrite: a NOT NULL column with a
-- CONSTANT default is filled lazily, Postgres skips the rewrite). No index
-- builds, no CREATE TABLEs mixed in.
--
-- WHY A COLUMN ON wf_edge: the edge ledger is the join counter's ONLY
-- truth, and the propagation rule (T06) is a PER-EDGE fact — the same
-- failed child may feed a fail_closed join (cascade: the join blocks, the
-- flow fails, the peers peer-cancel) and a collect join (the failure fans
-- in as an Item failure; the join fires with the typed partial). The
-- policy is DECLARED at fork/declared-join time and RECORDED here — T08's
-- derivation reads the policy off this ledger ("the absorption is on the
-- record ... never a heuristic"); it is never inferred from the join's
-- shape.
--
-- DEFAULT 'fail_closed': the safe default (a failed parent must not
-- silently fire a join over a partial result). 'collect' is the declared
-- opt-in (T06's second semantics).
--
-- ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1 (the
-- forever rule); a pre-migration worker reads the ledger unchanged (it
-- never selects this column).

ALTER TABLE "attack2".wf_edge
    ADD COLUMN IF NOT EXISTS failure_policy text NOT NULL DEFAULT 'fail_closed';

COMMENT ON COLUMN "attack2".wf_edge.failure_policy IS
    'The declared failure policy of THIS edge (what a terminal failure of the parent does to the child join): ''fail_closed'' (the default — the join blocks, the flow fails, the running peers peer-cancel) or ''collect'' (the failure fans in as an Item failure; the join fires with the typed partial). Declared at fork/join time; recorded on the ledger — the derivation reads it, never infers it.';

-- The workflow status-rollup indexes — T08 (the grouped rollup must be
-- INDEX-DRIVEN at the fleet shape: the EXPLAIN pin asserts no seq scan).
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "attack2" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENTS (SHARE lock class —
-- blocks writes for each build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed in). ALL WORKFLOW DDL IS ADDITIVE — no `post_`
-- phase ever ships for v1 (the forever rule).
--
-- THE EXPRESSION INDEX: the workflow rollup's grouped read walks the
-- flow link (metadata->>'flow_id') — without the index the rollup is a
-- Seq Scan of the WHOLE jobs table per read (the monster class); with
-- it, the read is an index-only walk over ONE run's rows. PARTIAL on
-- workflow rows only (step_key IS NOT NULL — the vanilla population's
-- maintenance exemption, the measured partial-index doctrine): vanilla
-- rows never carry the flow link.

CREATE INDEX IF NOT EXISTS jobs_wf_flow_nodes_idx
    ON "attack2".jobs ((metadata->>'flow_id'), status)
    WHERE step_key IS NOT NULL;

