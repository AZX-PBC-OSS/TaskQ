-- The per-attempt due-time stamp: when each attempt was DUE, so a retry
-- chain's wait spans are reconstructable from the ledger alone. Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
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
ALTER TABLE "{schema}".job_attempts
    ADD COLUMN IF NOT EXISTS due_at timestamptz;
ALTER TABLE "{schema}".job_attempts_archive
    ADD COLUMN IF NOT EXISTS due_at timestamptz;

COMMENT ON COLUMN "{schema}".job_attempts.due_at IS
    'The jobs.scheduled_at this attempt was claimed against (the due time '
    'the dispatch claim took the row at), stamped by the attempt-row '
    'writers at the attempt''s terminal transition; on reschedule arms the '
    'statement pre-reads scheduled_at so the value is the claim-time due '
    'time, never the next attempt''s. NULL = a pre-migration attempt (or a '
    'writer that could not know it): historical due times are unrecoverable, '
    'so there is no backfill. A retry chain reconstructs as '
    'due_at(k) -> started_at(k) -> due_at(k+1).';
COMMENT ON COLUMN "{schema}".job_attempts_archive.due_at IS
    'Mirror of job_attempts.due_at, carried by the prune sweep''s '
    'column-explicit archive INSERT. Same NULL semantics: pre-migration '
    'attempts are NULL, never backfilled.';
