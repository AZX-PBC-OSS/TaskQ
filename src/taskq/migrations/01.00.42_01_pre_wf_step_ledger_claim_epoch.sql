-- The step ledger's per-claim identity (PRE phase). Forward-only; there is no
-- down migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS
-- -----------------
-- PRE: apply before deploying any binary that claims workflow rows with the
-- epoch-scoped ledger claim. The ledger's arbiter was
-- (flow_id, step_key, COALESCE(map_index, -1), attempt): a deferred attempt
-- whose physical attempt ordinal is refunded and reminted (the capacity
-- park's 1 -> 0 -> 1 cycle) re-claims the SAME arbiter row, overwriting the
-- parked attempt's 'awaited' record with the resumed attempt's terminal.
-- claim_epoch joins the arbiter so every dispatch claim records its own
-- ledger row: the parked attempt's deferral record survives beside the
-- resumed attempt's terminal. Existing rows carry DEFAULT 0 and stay valid
-- (persisted history, never a schema-version oracle). The post-phase
-- widening re-creates this arbiter without the epoch column
-- (01.00.42_02 is frozen); 01.00.43_01 re-creates the epoch form after
-- it in the same apply run, so every fully-migrated schema ends on the
-- epoch arbiter.
--
-- LOCK SCOPE: the ADD COLUMN is a metadata-only catalog change (no table
-- rewrite; NOT NULL with a constant default). DROP INDEX + CREATE UNIQUE
-- INDEX are catalog writes; the whole file runs in the migration runner's
-- single transaction. No backfill, validation scan, or destructive statement
-- exists beyond the replaced arbiter index it substitutes.
--
-- WORKFLOW-EXECUTOR CUTOVER (the binding release contract)
-- --------------------------------------------------------
-- The cutover files are 01.00.42_01, 01.00.42_02 and 01.00.43_01 — the
-- operator exclusion below spans all three. It replaces the workflow-ledger's
-- conflict arbiter, so it requires a workflow-EXECUTOR cutover — there is NO
-- supported old/new mixed execution across the index replacement:
--
-- * Before the cutover files apply, stop accepting workflow body work and
--   drain graceful in-flight executions in EVERY pre-correction workflow
--   process: fleet workers with workflow execution, all direct FlowRunner
--   processes, and any custom driver using the old ledger claim/finalize
--   statements. Stop/kill the processes after their bounded graceful
--   period (merely marking a queue stopped or reading a workers row is
--   not a drain).
-- * An OLD executor's workflow ledger claim binds the four-expression
--   ON CONFLICT target; against this schema there is no matching unique
--   constraint, so the claim fails with PostgreSQL 42P10
--   (InvalidColumnReferenceError) after the job row is already 'running' —
--   the old body never runs. That observed failure is the DOCUMENTED
--   UNSUPPORTED envelope, never a compatibility behavior; the same old
--   binary's four-expression TERMINAL/FENCE writes are equally
--   unsupported against the epoch arbiter. Recovery: a corrected worker
--   re-claims the row under real worker/attempt/epoch ownership after
--   process termination/lease reclaim; the transient running row is never
--   reported as a final success or a truthful body failure.
-- * Producers may keep writing pending root/work rows: durable
--   pending/scheduled/parked rows are storage, not executors.
-- * After 01.00.43_01:post has applied, restart only corrected binaries
--   (the ones that carry these statements). An old executor must never
--   rejoin.

ALTER TABLE "{schema}".wf_step_ledger
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS "{schema}".wf_step_ledger_claim_uniq;

CREATE UNIQUE INDEX IF NOT EXISTS wf_step_ledger_claim_uniq
    ON "{schema}".wf_step_ledger (flow_id, step_key, COALESCE(map_index, -1), attempt, claim_epoch);

COMMENT ON COLUMN "{schema}".wf_step_ledger.claim_epoch IS
    'The dispatch claim epoch of the execution this ledger row records. Joins the claim arbiter so a refunded-and-reminted physical attempt (the capacity park cycle) records one ledger row per claim: the parked attempt''s awaited record survives beside the resumed attempt''s terminal. Applying the cutover files (01.00.42_01, 01.00.42_02, 01.00.43_01) requires the workflow-executor cutover (see 01.00.42_01): an old binary''s four-expression claim/finalize is unsupported against this arbiter, and after 01.00.43_01:post has applied, restart only corrected binaries.';
