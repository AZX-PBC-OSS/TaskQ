-- Batches table: tracks batch lifecycle for enqueue_batch / wait_for_batch.
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the migration runner.

CREATE TABLE "{schema}".batches (
    id                      uuid PRIMARY KEY,
    queue                   text NOT NULL,
    status                  text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'complete', 'aborted')),
    expected_size           int NOT NULL DEFAULT 0,
    consecutive_failures    int NOT NULL DEFAULT 0,
    failure_threshold       int,
    finalizer_job_id        uuid,
    originating_actor       text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    completed_at            timestamptz,
    metadata                jsonb NOT NULL DEFAULT '{{}}'::jsonb
);

CREATE INDEX batches_queue_status_idx
    ON "{schema}".batches (queue, status)
    WHERE status = 'active';

CREATE INDEX batches_finalizer_idx
    ON "{schema}".batches (finalizer_job_id)
    WHERE finalizer_job_id IS NOT NULL;

COMMENT ON TABLE "{schema}".batches IS
    'Tracks batch lifecycle for enqueue_batch / wait_for_batch. '
    'A batch is created active, transitions to complete or aborted when all '
    'member jobs resolve (or the failure threshold is exceeded).';

COMMENT ON COLUMN "{schema}".batches.expected_size IS
    'Number of jobs enqueued in the batch; set at creation time and used to '
    'detect completion when completed_count equals expected_size.';

COMMENT ON COLUMN "{schema}".batches.consecutive_failures IS
    'Running count of consecutive member-job failures; reset to 0 on each '
    'success. When it reaches failure_threshold the batch is auto-aborted '
    '(NULL failure_threshold means never auto-abort).';
