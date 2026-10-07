-- The workflow status-rollup indexes — T08 (the grouped rollup must be
-- INDEX-DRIVEN at the fleet shape: the EXPLAIN pin asserts no seq scan).
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENTS (SHARE lock class —
-- blocks writes for each build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed in). ALL WORKFLOW DDL IS ADDITIVE — no `post_`
-- phase ever ships for v1 (the forever rule).
--
-- THE EXPRESSION INDEX: the workflow rollup's grouped read walks the
-- flow link (metadata->>'flow_id') — without the index the rollup is a
-- Seq Scan of the WHOLE jobs table per read (the monster class); with
-- it, the read is an index-only walk over ONE run's rows. PARTIAL on
-- workflow rows only (step_key IS NOT NULL — the vanilla population's
-- maintenance exemption, the measured partial-index doctrine): vanilla
-- rows never carry the flow link.

CREATE INDEX IF NOT EXISTS jobs_wf_flow_nodes_idx
    ON "{schema}".jobs ((metadata->>'flow_id'), status)
    WHERE step_key IS NOT NULL;
