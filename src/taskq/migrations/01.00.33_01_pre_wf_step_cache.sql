-- The cross-run STEP CACHE (T25) — the TEMPORAL dedup's table.
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENT (its own relation
-- lock on a NEW table — no ALTERs, no index builds mixed in; the
-- expiry index is 01.00.33_02's own SHARE-class file). All DDL is
-- additive (no post_ phase, the v1 forever rule): a pre-migration
-- worker reads the estate unchanged — nothing outside the runner's
-- opt-in cache face ever touches this table.
--
-- ID GENERATION DEVIATION (the estate invariant's OWN boundary — the
-- design's explicit ruling): this table has NO `id uuid` column and NO
-- app-side uuid7 — the CONTENT ADDRESS is the PRIMARY KEY. The uuid7
-- invariant exists for the LEDGER tables (time-ordered ids land at the
-- right-hand edge of the PK B-tree; `ORDER BY id` is a usable creation
-- order); a content-addressed cache is HASH-keyed — it has no creation
-- order to preserve, and its PK's entropy is the address itself. The
-- `created_at` column keeps the store instant for the receipt.
--
-- FK-LESS: no foreign keys anywhere in this round, per the estate's
-- no-FK decision — the producing run's `run_id` is a RECEIPT (the
-- provenance the hit's audit reads), never a lifecycle edge: the
-- producing run's rows may be pruned long before the cache entry
-- expires, and the cache entry must survive that pruning.
--
-- THE TWO-CLAIMS LAW (the design's spine, restated for the operator):
-- the run-key arbiter dedups CONCURRENT; THIS table dedups TEMPORAL —
-- a LATER run over the same body (its §22.1 code-version hash) + the
-- same input (the resolved args' canonical jsonb) reads the EARLIER
-- run's result and never executes the body. Rows are written on
-- TERMINAL-SUCCEEDED ONLY (a failure never squats an address), and the
-- runner's store CAS keeps ONE winner per address (a fresh winner never
-- loses its row; an expired corpse is re-filled). The TTL's
-- freshness leg compares against the DB clock; the sweep's retention
-- arm prunes the expired rows.
CREATE TABLE "{schema}".wf_step_cache (
    content_address text PRIMARY KEY,
    result          jsonb NOT NULL,
    run_id          uuid NOT NULL,
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE "{schema}".wf_step_cache IS
    'The cross-run step cache (T25): the TEMPORAL dedup — a later run over the same body (code-version hash) + input (canonical jsonb) reads the producing run''s result envelope without executing the body. Content-address is the PK (the Nix-style recursive hash); rows are success-only (a failed run never squats an address); expires_at is the in-DB TTL the lookup''s freshness leg and the sweep''s retention arm both read.';
