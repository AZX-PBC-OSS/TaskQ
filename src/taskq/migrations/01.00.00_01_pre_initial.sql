-- TaskQ initial schema: jobs/dispatch core, archive tables, and rate-limit tables.
-- The literal ""{schema}"" tokens are substituted at apply time by the migration runner.

CREATE SCHEMA IF NOT EXISTS "{schema}";

-- ============================================================
-- Migration tracking
-- ============================================================
CREATE TABLE "{schema}".schema_migrations (
    version       text PRIMARY KEY,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    checksum      text NOT NULL
);

-- ============================================================
-- Workers (liveness / membership)
-- ============================================================
CREATE TABLE "{schema}".workers (
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
CREATE INDEX workers_last_seen_idx ON "{schema}".workers (last_seen_at);
CREATE INDEX workers_wg_lookup_idx ON "{schema}".workers (workgroup_instance, worker_label)
    WHERE worker_label IS NOT NULL AND workgroup_instance IS NOT NULL;

COMMENT ON COLUMN "{schema}".workers.worker_label IS
    'Human-readable label set by the workgroup supervisor or --worker-label CLI flag.';
COMMENT ON COLUMN "{schema}".workers.workgroup_instance IS
    'UUIDv7 identifying the workgroup orchestrator that launched this worker. '
    'Used for cross-process correlation and health checking.';

-- ============================================================
-- Maintenance leader (queryable; advisory lock is the source of truth)
-- ============================================================
CREATE TABLE "{schema}".maintenance_leader (
    singleton     boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    worker_id     uuid NOT NULL REFERENCES "{schema}".workers(id) ON DELETE CASCADE,
    elected_at    timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- Jobs (the hot table)
-- NOTE: 'awaiting_resource' is intentionally NOT in this enum.
-- Reservation denial transitions the job to 'scheduled' with a
-- metadata annotation (metadata.awaiting='reservation:<bucket>').
-- ============================================================
CREATE TYPE "{schema}".job_status AS ENUM (
    'pending',
    'scheduled',
    'running',
    'succeeded',
    'failed',
    'cancelled',
    'crashed',
    'abandoned'
);

CREATE TABLE "{schema}".jobs (
    id                  uuid PRIMARY KEY,
    actor               text NOT NULL,
    queue               text NOT NULL,
    identity_key        text,
    fairness_key        text,
    payload             jsonb NOT NULL,
    -- TODO(future-migration): must ship a new migration:
    --   ALTER TABLE "{schema}".jobs ALTER COLUMN payload_schema_ver TYPE text USING payload_schema_ver::text;
    --   ALTER TABLE "{schema}".jobs ALTER COLUMN payload_schema_ver SET DEFAULT '1';
    -- The discriminated-union pattern requires text (string values like 'v1', 'v2').
    -- Until that migration lands, string discriminator storage will fail at the PG level.
    payload_schema_ver  int NOT NULL DEFAULT 1,
    status              "{schema}".job_status NOT NULL DEFAULT 'pending',
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
    ON "{schema}".jobs (queue, priority DESC, scheduled_at)
    WHERE status = 'pending';

-- Per-actor dispatch index for bounded LATERAL range scans.
-- Supports per-(actor, queue) index seeks decoupled from backlog depth.
CREATE INDEX jobs_actor_dispatch_idx
    ON "{schema}".jobs (actor, queue, priority DESC, scheduled_at, id)
    WHERE status = 'pending';

-- Round-robin fairness sampling: the per-fairness_key ROW_NUMBER window in
-- the round-robin dispatch CTE needs pre-sorted per-partition input so deep
-- backlogs don't force a full sort of every pending row on each dispatch tick.
CREATE INDEX jobs_actor_fairness_dispatch_idx
    ON "{schema}".jobs (actor, queue, fairness_key, priority DESC, scheduled_at, id)
    WHERE status = 'pending';

CREATE INDEX jobs_scheduled_wake_idx
    ON "{schema}".jobs (scheduled_at)
    WHERE status = 'scheduled';

CREATE INDEX jobs_running_lock_expires_idx
    ON "{schema}".jobs (lock_expires_at)
    WHERE status = 'running';

CREATE INDEX jobs_schedule_to_close_idx
    ON "{schema}".jobs (schedule_to_close)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX jobs_identity_active_idx
    ON "{schema}".jobs (actor, identity_key)
    WHERE status IN ('pending', 'scheduled', 'running');

CREATE UNIQUE INDEX jobs_idempotency_key_uniq
    ON "{schema}".jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX jobs_singleton_uniq
    ON "{schema}".jobs (actor)
    WHERE status IN ('pending', 'scheduled', 'running')
      AND metadata @> '{{"singleton": true}}'::jsonb;

CREATE INDEX jobs_actor_running_idx
    ON "{schema}".jobs (actor)
    WHERE status = 'running';

-- Per-actor pending+scheduled count for max_pending backpressure (§3.3).
CREATE INDEX jobs_actor_pending_idx
    ON "{schema}".jobs (actor)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX jobs_finished_at_idx
    ON "{schema}".jobs (finished_at)
    WHERE status IN ('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned');

CREATE INDEX jobs_metadata_gin_idx
    ON "{schema}".jobs USING gin (metadata jsonb_path_ops);

CREATE INDEX jobs_cancel_requested_idx
    ON "{schema}".jobs (locked_by_worker, cancel_requested_at)
    WHERE cancel_requested_at IS NOT NULL AND status = 'running';

-- Hot-path: heartbeat tick extends lock_expires_at for every running job owned by
-- this worker (heartbeat.py:42).  Without this, PG scans all running rows.
-- Vendor parallel: pgqueuer (queue_manager_id) WHERE queue_manager_id IS NOT NULL.
CREATE INDEX jobs_locked_by_worker_running_idx
    ON "{schema}".jobs (locked_by_worker)
    WHERE status = 'running';

-- Leader sweep: clear expired results (postgres.py:197).
-- Vendor parallel: River (state, finalized_at) WHERE finalized_at IS NOT NULL.
CREATE INDEX jobs_result_expires_at_idx
    ON "{schema}".jobs (result_expires_at)
    WHERE result IS NOT NULL;

CREATE INDEX jobs_tags_gin_idx ON "{schema}".jobs USING gin (tags);

COMMENT ON COLUMN "{schema}".jobs.identity_key IS
    'User-derived logical work unit. Used for serialization and unique-for. NOT idempotency.';
COMMENT ON COLUMN "{schema}".jobs.idempotency_key IS
    'Caller-provided. Used to make enqueue idempotent. Distinct from identity.';
COMMENT ON COLUMN "{schema}".jobs.fairness_key IS
    'User-derived cohort key. NULL collapses to one cohort via COALESCE in dispatch.';
COMMENT ON COLUMN "{schema}".jobs.error_traceback IS
    'Last attempt error only. Full per-attempt history in job_attempts table.';
COMMENT ON COLUMN "{schema}".jobs.cancel_phase IS
    '0 = no cancellation; 1 = cooperative cancel requested; 2 = force cancel issued.';

-- ============================================================
-- Per-attempt history (full trace of every execution try)
-- ============================================================
CREATE TABLE "{schema}".job_attempts (
    job_id          uuid NOT NULL REFERENCES "{schema}".jobs(id) ON DELETE CASCADE,
    attempt         smallint NOT NULL,
    started_at      timestamptz NOT NULL,
    finished_at     timestamptz,
    outcome         text,
    error_class     text,
    error_message   text,
    error_traceback text,
    duration_ms     int,
    worker_id       uuid REFERENCES "{schema}".workers(id) ON DELETE SET NULL,
    metadata        jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    PRIMARY KEY (job_id, attempt)
);
CREATE INDEX job_attempts_started_idx
    ON "{schema}".job_attempts (started_at);
CREATE INDEX job_attempts_outcome_idx
    ON "{schema}".job_attempts (outcome, finished_at);

COMMENT ON TABLE "{schema}".job_attempts IS
    'Full history of every execution attempt of every job. Pruned with parent job via ON DELETE CASCADE.';
COMMENT ON COLUMN "{schema}".job_attempts.outcome IS
    'Valid values: succeeded, failed, snoozed, cancelled, crashed. Error class distinguishes sub-types (e.g. DeadlineExceeded, WorkerCrashed, MaxAttemptsExceeded).';

-- ============================================================
-- Per-actor concurrency caps (cached config)
-- ============================================================
CREATE TABLE "{schema}".actor_config (
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

CREATE TABLE "{schema}".queues (
    name       text PRIMARY KEY,
    mode       text NOT NULL DEFAULT 'strict_fifo',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (mode IN ('strict_fifo', 'round_robin'))
);

-- ============================================================
-- Cron schedules
-- ============================================================
CREATE TABLE "{schema}".cron_schedules (
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
    ON "{schema}".cron_schedules (next_fire_at)
    WHERE enabled = true;

-- ============================================================
-- Concurrency reservation slots
-- ============================================================
CREATE TABLE "{schema}".reservation_slots (
    bucket_name       text NOT NULL,
    slot_index        int  NOT NULL,
    job_id            uuid,
    held_by_worker_id uuid,
    acquired_at       timestamptz,
    lease_expires_at  timestamptz,
    PRIMARY KEY (bucket_name, slot_index)
);
CREATE INDEX reservation_slots_free_idx
    ON "{schema}".reservation_slots (bucket_name, slot_index)
    WHERE job_id IS NULL;
CREATE INDEX reservation_slots_lease_expires_idx
    ON "{schema}".reservation_slots (lease_expires_at)
    WHERE job_id IS NOT NULL;

-- Hot-path: heartbeat extends reservation leases via subquery on job_id
-- (heartbeat.py:47).  Without this, PG scans all non-free slots.
CREATE INDEX reservation_slots_job_id_idx
    ON "{schema}".reservation_slots (job_id)
    WHERE job_id IS NOT NULL;

-- ============================================================
-- Token bucket / sliding window PG fallback
-- ============================================================
CREATE TABLE "{schema}".rate_limit_buckets (
    bucket_name     text PRIMARY KEY,
    kind            text NOT NULL,
    state           jsonb NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Sliding-window log-style PG fallback table (used when the rate-limit
-- backend is "postgres" instead of Redis).
CREATE TABLE "{schema}".rate_limit_window_entries (
    bucket_name  text        NOT NULL,
    ts           timestamptz NOT NULL,
    request_id   uuid        NOT NULL,
    PRIMARY KEY (bucket_name, ts, request_id)
);
CREATE INDEX rate_limit_window_entries_lookup
    ON "{schema}".rate_limit_window_entries (bucket_name, ts);

-- ============================================================
-- Events log (state transitions; admin UI / audit)
-- ============================================================
CREATE TABLE "{schema}".job_events (
    id          bigserial PRIMARY KEY,
    job_id      uuid NOT NULL REFERENCES "{schema}".jobs(id) ON DELETE CASCADE,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    kind        text NOT NULL,
    detail      jsonb NOT NULL DEFAULT '{{}}'::jsonb
);
CREATE INDEX job_events_job_id_idx ON "{schema}".job_events (job_id, occurred_at);
COMMENT ON COLUMN "{schema}".job_events.kind IS
    'Event type; one of: state_change | cancel_request | heartbeat_miss | progress';

-- ============================================================
-- Archive tables for terminal jobs pruned by the maintenance leader
-- ============================================================
CREATE TABLE "{schema}".jobs_archive (
    id                  uuid PRIMARY KEY,
    actor               text NOT NULL,
    queue               text NOT NULL,
    identity_key        text,
    fairness_key        text,
    payload             jsonb NOT NULL,
    payload_schema_ver  int NOT NULL DEFAULT 1,
    status              "{schema}".job_status NOT NULL,
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
    ON "{schema}".jobs_archive (expire_at);

CREATE INDEX jobs_archive_finished_at_idx
    ON "{schema}".jobs_archive (finished_at);

CREATE INDEX jobs_archive_tags_gin_idx ON "{schema}".jobs_archive USING gin (tags);

CREATE TABLE "{schema}".job_attempts_archive (
    job_id          uuid NOT NULL REFERENCES "{schema}".jobs_archive(id) ON DELETE CASCADE,
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
    ON "{schema}".job_attempts_archive (job_id);

COMMENT ON TABLE "{schema}".jobs_archive IS
    'Terminal jobs moved from "{schema}".jobs by the prune sweep. Retained for '
    'archive_retention_period (default 1 year) then hard-deleted by the '
    'archive expiry sweep. Not involved in dispatch or heartbeat.';

COMMENT ON TABLE "{schema}".job_attempts_archive IS
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

CREATE OR REPLACE FUNCTION "{schema}".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify('taskq_wake_' || TG_TABLE_SCHEMA, '');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_notify_job_insert
AFTER INSERT ON "{schema}".jobs
FOR EACH ROW
WHEN (NEW.status = 'pending')
EXECUTE FUNCTION "{schema}".notify_job_insert();
