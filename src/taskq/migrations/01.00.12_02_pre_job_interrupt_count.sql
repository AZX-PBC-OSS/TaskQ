-- Interruption counter on the job row. Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- A worker going away mid-attempt — a rolling deploy, a drain, a node
-- eviction — is infrastructure, not the job. The attempt it interrupts
-- is released rather than terminalised, and the claim's attempt
-- increment is refunded, exactly as the deferral and admission-denial
-- paths refund theirs. That makes the interruption invisible in
-- `attempt`, and it writes no job_attempts row because an interruption
-- is not an execution outcome. This counter is its durable record, in
-- the same shape and for the same reason as snooze_count and
-- rate_limit_blocked_count (01.00.08_01): O(1) storage however often it
-- happens, one row read for the admin surface, reclaimed with the row.
--
-- int, not smallint: a job whose runtime exceeds the deployment's
-- cancellation graces is interrupted on every deploy, indefinitely,
-- without ever consuming budget — the same genuinely-unbounded shape the
-- denial counters have, and exactly the domain a smallint ceiling would
-- walk into.
--
-- OPS NOTE (locks): ALTER TABLE ... ADD COLUMN with a non-volatile
-- default is metadata-only on PG >= 11 (the default is stored in
-- pg_attribute and read from there), so neither statement rewrites its
-- table however large jobs has grown.
--
-- ROLLING DEPLOY: pre-phase, safe for both code generations. The
-- previous release never reads or writes this column — it terminalises
-- its own in-flight work on shutdown, one last time per old pod — while
-- this release's release write requires the column, which is why it
-- ships before the code rollout. Rolling back leaves an unused column.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and COPY_FROM_COLUMNS, from which src/taskq/worker/_leader_shared.py
-- builds the prune sweep's archive INSERT), so the counter must exist on
-- both tables or that INSERT breaks on column count.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;

COMMENT ON COLUMN "{schema}".jobs.interrupt_count IS
    'Times a running attempt of this job was released back to the queue by a '
    'worker shutdown, with the claim''s attempt increment refunded. An '
    'interruption is not an execution: no job_attempts row, no retry budget '
    'spent. Each release also writes one job_events state_change carrying '
    'detail.reason = ''interrupted''.';
