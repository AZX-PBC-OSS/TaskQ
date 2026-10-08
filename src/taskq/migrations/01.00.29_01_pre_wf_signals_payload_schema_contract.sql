-- THE HOLD'S CONTRACT IS MANDATORY (attack-4 F-P4-UNTYPED-COLD-DOOR's
-- cure, the upgrade-world half). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY ONE BACKFILL UPDATE against wf_signals (rows
-- already in the table; the statement takes no schema-lock beyond the
-- row locks it needs — a held row is unlocked instantly, the table is
-- never rebuilt). ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever
-- ships for v1.
--
-- THE LAW: an approval without a declared contract is the audit hole.
-- The typed door (``_boundary_refusal``'s runtime boundary) validates
-- every delivery against the hold's DECLARED models — and a fresh
-- process's only witness is the ROW's ``payload_schema``. A hold row
-- written before the typed door (or hand-migrated) carries NULL: under
-- the pre-cure boundary that row delivered to ANY payload — the door's
-- teeth depended on which process asked.
--
-- THE BACKFILL: NULL → the empty jsonb object (the EXPLICIT no-contract
-- marker; spelled as doubled braces in the statement below — the
-- format-engine law). The boundary REFUSES the marker loudly (the typed
-- refusal, the hold SURVIVES); no row is silently untyped. New holds
-- cannot be contract-less: the wait site (``ctx.wait_signal``) and the
-- mint (``register_hold``) both refuse to create one.
--
-- The column STAYS nullable (no NOT NULL): a NULL row remains
-- representable so the upgrade world can be NAMED — but nothing
-- delivers to it, and the marker rows say the same thing in the
-- column's own terms.

UPDATE "{schema}".wf_signals
SET payload_schema = '{{}}'::jsonb
WHERE payload_schema IS NULL;
