-- Denial/snooze counters on the job row, and a floor check on
-- max_attempts. Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "{schema}" token is substituted at
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
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS snooze_count int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rate_limit_blocked_count int NOT NULL DEFAULT 0;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and the explicit column lists in src/taskq/worker/_leader_shared.py,
-- which pick the new columns up from COPY_FROM_COLUMNS), so the prune
-- sweep's archive INSERT carries the counters into the archive without a
-- column-count break.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS snooze_count int NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rate_limit_blocked_count int NOT NULL DEFAULT 0;

DO $$
BEGIN
    ALTER TABLE "{schema}".jobs
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
          AND conrelid = '"{schema}".jobs'::regclass
          AND NOT convalidated
    ) THEN
        ALTER TABLE "{schema}".jobs VALIDATE CONSTRAINT jobs_max_attempts_check;
    END IF;
END
$$;

COMMENT ON COLUMN "{schema}".jobs.snooze_count IS
    'Coalesced count of non-consuming deferrals (Snooze, RetryAfter(consume_budget=False)) '
    'since enqueue. A deferral consumes no retry budget and writes no job_attempts/'
    'job_events rows; this counter is its durable record.';

COMMENT ON COLUMN "{schema}".jobs.rate_limit_blocked_count IS
    'Coalesced count of admission denials (reservation/rate-limit) since enqueue. '
    'A denial is backpressure, not an execution: no budget consumed, no per-occurrence '
    'rows; this counter plus OTEL carry it.';
