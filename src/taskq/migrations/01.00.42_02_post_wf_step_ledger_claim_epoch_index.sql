-- The step ledger's per-claim identity (POST phase). Forward-only; there is
-- no down migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS
-- -----------------
-- POST: the map-index widening (01.00.40_01, frozen) re-creates the ledger's
-- claim arbiter from its own frozen file with the pre-epoch four-expression
-- form. This file re-creates the epoch-scoped arbiter AFTER that rewrite in
-- the same apply run, so a fully-migrated schema ends on the five-expression
-- arbiter the current ledger claim's ON CONFLICT target infers. The
-- claim_epoch column itself is the PRE phase's (01.00.42_01); this file
-- touches the index only.
--
-- CUTOVER: this phase completes the workflow-executor cutover the PRE file
-- declares — the old binaries were drained and stopped before PRE, and only
-- corrected binaries (which present the actual positive claim epoch on every
-- claim) rejoin after POST. An old executor's four-expression claim has no
-- matching unique constraint here and never regains one; its 42P10 failure
-- is the documented unsupported envelope, not a compatibility behavior.
--
-- LOCK SCOPE: one DROP INDEX + one CREATE UNIQUE INDEX over the ledger table
-- are catalog writes inside the migration runner's single transaction; the
-- index build is a full-index build over the ledger's existing rows (no
-- CONCURRENTLY in a transactional migration — the same doctrine as
-- 01.00.40_01's own re-creation).

DROP INDEX IF EXISTS "{schema}".wf_step_ledger_claim_uniq;

CREATE UNIQUE INDEX IF NOT EXISTS wf_step_ledger_claim_uniq
    ON "{schema}".wf_step_ledger (flow_id, step_key, COALESCE(map_index, -1), attempt, claim_epoch);
