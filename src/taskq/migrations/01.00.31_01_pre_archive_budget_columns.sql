-- The loop node's budget columns on `jobs_archive` — the archive mirror's
-- sync with 01.00.26_01 (the loop-budget round added the three columns to
-- `jobs` but missed the mirror; CERT2's F-CERT2-1, the archive/COPY drift).
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- METADATA-ONLY COLUMN ADDITIONS (ACCESS EXCLUSIVE, held for milliseconds
-- on the catalog — nullable columns / a CONSTANT default are filled
-- lazily, Postgres 11+ skips the rewrite). No index builds, no CREATE
-- TABLEs mixed in. ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever
-- ships for v1 (the forever rule).
--
-- The mirror law (01.00.00_01's shape, 01.00.03_01's and 01.00.23_01's
-- same-shape precedents): jobs_archive mirrors EVERY jobs column. The
-- archive sweep's INSERT names its columns via COPY_FROM_COLUMNS (the
-- explicit-column doctrine — the positional `SELECT j.*` died with
-- 01.00.03), so an unmirrored column stops the sweep from archiving it
-- (data loss, not drift): the guard
-- test_jobs_archive_columns_match_jobs_plus_archive_fields reds on
-- exactly that.

ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS budget_deadline timestamptz NULL;

ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS budget_paused boolean NOT NULL DEFAULT false;

ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS budget_remaining_ms bigint NULL;

COMMENT ON COLUMN "{schema}".jobs_archive.budget_deadline IS
    'The loop node''s budget wall, mirrored from jobs (T19) — see jobs.budget_deadline. NULL on vanilla rows; the workflow-aware pruner (T18) defines the archived rows'' retention.';
COMMENT ON COLUMN "{schema}".jobs_archive.budget_paused IS
    'The held-loop''s budget pause, mirrored from jobs (T19) — see jobs.budget_paused.';
COMMENT ON COLUMN "{schema}".jobs_archive.budget_remaining_ms IS
    'The loop node''s on-wake remaining budget, mirrored from jobs (T19) — see jobs.budget_remaining_ms.';
