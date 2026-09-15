-- Per-job retry-curve columns for crash/heartbeat reclaim. Forward-only;
-- there is no down migration. To revert, restore from backup. The
-- literal "{schema}" token is substituted at apply time by the
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
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS retry_base_seconds double precision NOT NULL DEFAULT 5.0,
    ADD COLUMN IF NOT EXISTS retry_cap_seconds double precision NOT NULL DEFAULT 3600.0,
    ADD COLUMN IF NOT EXISTS retry_backoff text NOT NULL DEFAULT 'exponential',
    ADD COLUMN IF NOT EXISTS retry_jitter double precision NOT NULL DEFAULT 0.2;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and 01.00.08_01_pre_denial_counters.sql's precedent), so the prune
-- sweep's archive INSERT carries these columns into the archive without
-- a column-count break.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS retry_base_seconds double precision NOT NULL DEFAULT 5.0,
    ADD COLUMN IF NOT EXISTS retry_cap_seconds double precision NOT NULL DEFAULT 3600.0,
    ADD COLUMN IF NOT EXISTS retry_backoff text NOT NULL DEFAULT 'exponential',
    ADD COLUMN IF NOT EXISTS retry_jitter double precision NOT NULL DEFAULT 0.2;

DO $$
BEGIN
    ALTER TABLE "{schema}".jobs
        ADD CONSTRAINT jobs_retry_backoff_check
        CHECK (retry_backoff IN ('exponential', 'linear', 'fixed')) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;  -- constraint already present; idempotent re-apply
END
$$;

DO $$
BEGIN
    ALTER TABLE "{schema}".jobs
        ADD CONSTRAINT jobs_retry_base_cap_check
        CHECK (retry_cap_seconds >= retry_base_seconds) NOT VALID;
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    ALTER TABLE "{schema}".jobs
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
          AND conrelid = '"{schema}".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "{schema}".jobs VALIDATE CONSTRAINT jobs_retry_backoff_check;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_retry_base_cap_check'
          AND conrelid = '"{schema}".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "{schema}".jobs VALIDATE CONSTRAINT jobs_retry_base_cap_check;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_retry_jitter_check'
          AND conrelid = '"{schema}".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "{schema}".jobs VALIDATE CONSTRAINT jobs_retry_jitter_check;
    END IF;
END
$$;

COMMENT ON COLUMN "{schema}".jobs.retry_base_seconds IS
    'RetryPolicy.base (seconds) at enqueue time, stamped by the client from the '
    'actor''s live registration. The reclaim sweep and heartbeat isolate read this '
    'to compute the same backoff curve an application-level failure would, instead '
    'of a hardcoded flat interval.';

COMMENT ON COLUMN "{schema}".jobs.retry_cap_seconds IS
    'RetryPolicy.cap (seconds) at enqueue time. Bounds the reclaim backoff the same '
    'way it bounds every other retry path''s delay.';

COMMENT ON COLUMN "{schema}".jobs.retry_backoff IS
    'RetryPolicy.backoff (exponential/linear/fixed) at enqueue time, read by the '
    'reclaim sweep to select the same curve shape compute_backoff would apply.';

COMMENT ON COLUMN "{schema}".jobs.retry_jitter IS
    'RetryPolicy.jitter at enqueue time. A fleet-wide reclaim event spreads the '
    'whole reclaimed cohort across this band instead of stamping one synchronized '
    'instant.';
