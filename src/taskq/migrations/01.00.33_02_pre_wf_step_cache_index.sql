-- The cross-run STEP CACHE's expiry index (T25). Forward-only; there is
-- no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENT (SHARE lock class —
-- blocks writes for the build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed into the window). See 01.00.33_01's header for
-- the round's split and the deployment sequence.
--
-- The retention arm's scan target: the EXPIRED rows first. The arm
-- deletes `WHERE expires_at <= clock_timestamp()`; without this index
-- the sweep's probe is a seq-scan over the whole cache (the unscoped
-- scan monster the sweep-cost curves convicted elsewhere). The PK
-- serves the lookup's address probe; this index serves the sweep's.
CREATE INDEX IF NOT EXISTS wf_step_cache_expiry_idx
    ON "{schema}".wf_step_cache (expires_at);
