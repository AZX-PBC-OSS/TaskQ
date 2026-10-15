-- THE PROGRESS STATE CHANNEL'S PCT DOMAIN IS STORAGE-DOMAIN LAW (the
-- T20/T21 fixer's gate-hole cure). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}" token
-- is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONE ADD CONSTRAINT against wf_node_progress (an
-- existing table; the CHECK takes a brief ACCESS EXCLUSIVE lock — the
-- table is nodes x channels rows, the validation a scan of a tiny
-- relation). ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever
-- ships for v1.
--
-- THE PROMISE THIS SHIPS: 01.00.28's comment claims the column is
-- "the storage-domain twin" of the emission op's 0..100 bound — but
-- smallint's domain is ±32767, so the comment's promise had NO
-- constraint behind it: a ``pct=999`` (or ``-1``) landed in the state
-- row the moment a writer bypassed the Python gate (the convicted
-- hole: the emitter's buffer write was a PUBLIC ``submit`` that
-- validated nothing). The CHECK is the storage domain's own refusal —
-- the same teeth the stream table's class/kind CHECKs carry (DH3's
-- fence) — and the Python gate (``validate_emission``) stays the
-- contract's first door.
--
-- WHY NULL PASSES: the counters rows (channel '__stream__') and a
-- bare-progress emission carry no pct — the column is nullable by
-- 01.00.28's own shape, and this constraint bounds the VALUE domain,
-- not the presence.
--
-- IDEMPOTENCE: the DO block adds the constraint only when absent —
-- the fresh-schema replay and the deployed-ledger upgrade land the
-- same domain. (A pre-cure deployment whose state rows already carry
-- an out-of-domain pct fails VALIDATION loudly here: the rogue row is
-- named, never silently kept — the record must not look healthy while
-- the data is wrong.)

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}'
          AND t.relname = 'wf_node_progress'
          AND c.conname = 'wf_node_progress_pct_domain'
    ) THEN
        ALTER TABLE "{schema}".wf_node_progress
            ADD CONSTRAINT wf_node_progress_pct_domain
            CHECK (pct IS NULL OR pct BETWEEN 0 AND 100);
    END IF;
END
$$;
