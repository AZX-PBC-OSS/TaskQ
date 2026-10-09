-- Workflow columns on `jobs` (and the `jobs_archive` mirror) — T03, the
-- measured-tax migration (§16). Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "{schema}" token is substituted
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
--   01.00.23_04_pre_workflow_columns.sql  THIS FILE: ONLY the metadata-only
--                                         ALTER TABLE ... ADD COLUMN
--                                         statements (ACCESS EXCLUSIVE,
--                                         held for milliseconds on the
--                                         catalog — no table rewrite: every
--                                         column is NULL-able or NOT NULL
--                                         with a constant default, so
--                                         Postgres skips the rewrite).
--   01.00.23_05_pre_workflow_tables.sql   ONLY the CREATE TABLE statements
--                                         (wf_edge, wf_join_fire, wf_outbox,
--                                         wf_step_ledger).
--   01.00.23_06_pre_workflow_indexes.sql  ONLY the CREATE INDEX statements.
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

ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS parent_id uuid NULL;
-- The join counter (a CACHE of "un-terminal parents" — the wf_edge ledger
-- is TRUTH; see the engine's COUNTER-AS-CACHE / LEDGER-AS-TRUTH rule).
-- NOT NULL DEFAULT 0: vanilla rows are born join-free, and the dispatch
-- claim's `AND deps_pending = 0` exclusion is a semantic no-op for them.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS deps_pending smallint NOT NULL DEFAULT 0;
-- The fan-out slot. Retry-in-place preserves (parent_id, map_index).
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS map_index smallint NULL;
-- The positional node key. Never changes across holds/retries/reclaims.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS step_key text NULL;
-- The per-attempt code-version RECORD (§22.1: mixed-version joins legal
-- with hashes recorded). A RECORD, not a cache — no cache_key machinery.
-- Written at claim by the workflow claim path; computed via the tors
-- canonical content hash.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS code_version text NULL;

-- jobs_archive mirrors every jobs column (see 01.00.00_01_pre_initial.sql
-- and 01.00.03_01's same-shape mirror). The archive sweep's INSERT names
-- its columns explicitly (COPY_FROM_COLUMNS), so these stay NULL/0 on
-- archived rows until the workflow-aware pruner (T18) defines their
-- retention; the mirror keeps the row-shape doctrine intact.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS parent_id uuid NULL;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS deps_pending smallint NOT NULL DEFAULT 0;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS map_index smallint NULL;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS step_key text NULL;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS code_version text NULL;

COMMENT ON COLUMN "{schema}".jobs.parent_id IS
    'Workflow graph parent pointer (uuid, FK-less by the no-FK decision — the graph is enforced by the engine, never by database FKs). NOT NULL on workflow child rows; NULL only for roots. The parent COLUMN is the only parent truth: nothing may derive it from a node-id string shape.';
COMMENT ON COLUMN "{schema}".jobs.deps_pending IS
    'Join counter: a CACHE of the row''s un-terminal parents. The wf_edge ledger is truth. Joined nodes wait as status=''pending'' + deps_pending > 0 (metadata.blocking_reason=''join'') — no new ENUM value. The dispatch claim excludes rows with deps_pending > 0; a semantic no-op for vanilla rows (DEFAULT 0).';
COMMENT ON COLUMN "{schema}".jobs.map_index IS
    'Fan-out slot. Retry-in-place preserves (parent_id, map_index); a retried map child claims the same row and its ledger result.';
COMMENT ON COLUMN "{schema}".jobs.step_key IS
    'The positional node key: never changes across holds/retries/reclaims. NULL on vanilla rows.';
COMMENT ON COLUMN "{schema}".jobs.code_version IS
    'The per-attempt code-version RECORD (§22.1): computed at claim via the tors canonical content hash. A record, not a cache — there is deliberately no cache_key/content-addressed machinery here.';
