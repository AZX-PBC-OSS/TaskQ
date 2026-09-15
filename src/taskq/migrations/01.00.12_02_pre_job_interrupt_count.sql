-- Interruption counter for jobs whose running attempt was released by the
-- worker process going away (a graceful shutdown that outlasted its grace
-- windows). The same non-consuming-release family as snooze_count /
-- rate_limit_blocked_count (01.00.08_01): the claim's attempt increment is
-- refunded on release, and the durable record of "how often was this job
-- interrupted by infrastructure" is a coalesced counter on the job row —
-- O(1) storage, one row read for the admin surface, reclaimed with the row
-- itself when the prune sweep archives it. Forward-only; there is no down
-- migration. The literal "{schema}" token is substituted at apply time by
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
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS interrupt_count int NOT NULL DEFAULT 0;

COMMENT ON COLUMN "{schema}".jobs.interrupt_count IS
    'Times a running attempt of this job was released back to the queue by a '
    'worker shutdown, with the claim''s attempt increment refunded. Counted on '
    'the row; each release also writes one job_events state_change with '
    'detail.reason = ''interrupted''.';
