-- THE HOLD IDENTITY GAINS THE MAP INDEX (the LedgerWatch adoption
-- team's finding F2 — the map-sibling hold collision). Forward-only;
-- there is no down migration. To revert, restore from backup. The
-- literal "{schema}" token is substituted at apply time by the
-- migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONE ADD COLUMN + ONE DROP/CREATE INDEX PAIR against
-- wf_signals (an existing table; the ADD COLUMN is a catalog-only
-- metadata change — momentary ACCESS EXCLUSIVE, no table rewrite, on
-- a table whose population is the estate's OPEN HOLDS: tiny by
-- construction, the index rebuilds are its own statements). ALL
-- WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1.
--
-- THE COLLISION BEING CURED
-- -------------------------
-- The hold identity was (workflow_id, node_key, signal_name,
-- hold_epoch): map SIBLINGS of one source share workflow_id and
-- node_key, so two siblings holding CONCURRENTLY on one signal name
-- collided on ONE hold row. Observed both faces (the adoption team's
-- finding): in-process, the sibling's wait site found the OTHER
-- sibling's held row and never registered its own — the second
-- sibling wedged `running` forever; under the fleet, the second
-- sibling's hold INSERT raised UniqueViolationError
-- (wf_signals_hold_identity_uniq) INTO the ladder, and the retry
-- consumed the OTHER sibling's delivered answer. A resolve woke one
-- arbitrary sibling (the resume's node lookup matched every sibling
-- carrying a hold stamp and took the first). The per-item approval —
-- the natural moderation/approval pipeline shape — wedged a run
-- silently.
--
-- THE CURE: the identity gains the map index — the same COALESCE
-- discipline the ledger claim arbiter carries
-- (wf_step_ledger_claim_uniq: (flow_id, step_key, COALESCE(map_index,
-- -1), attempt)). A map sibling's hold is addressable and resolvable
-- INDIVIDUALLY: sibling k's wait site reads only sibling k's rows
-- (the answer queue, the held representation, the timeout face —
-- every wait-site read gains the map_index filter), and the resume's
-- node lookup addresses the hold's OWN node by the hold id the
-- register stamped into that node's metadata (metadata->>'hold') —
-- one hold, one node, one resume. A plain step's hold carries
-- map_index NULL; COALESCE(map_index, -1) keeps the pre-cure
-- identity's uniqueness exactly for it.
--
-- THE REPLY HANDLE IS UNCHANGED: `id` (uuid7) stays THE reference —
-- HitlClient.resolve(id, …) addresses ONE hold. The identity tuple is
-- the UNRESOLVED-uniqueness ARBITER, never the addressing.
--
-- NO BACKFILL: the column is nullable; every pre-cure row carries
-- map_index NULL — a pre-cure hold was necessarily a PLAIN step's (a
-- map sibling could never hold: the collision forbade it), so NULL is
-- the truthful historical value, not a guess.

ALTER TABLE "{schema}".wf_signals ADD COLUMN map_index smallint;

DROP INDEX IF EXISTS "{schema}".wf_signals_hold_identity_uniq;

CREATE UNIQUE INDEX wf_signals_hold_identity_uniq
    ON "{schema}".wf_signals (workflow_id, node_key, signal_name, COALESCE(map_index, -1), hold_epoch)
    WHERE resolved_at IS NULL;

COMMENT ON TABLE "{schema}".wf_signals IS
    'The HITL hold rows (T10): the row IS the truth (the notification is a knock). The hold identity is (workflow_id, node_key, signal_name, COALESCE(map_index, -1), hold_epoch) — partial-unique over UNRESOLVED holds (multi-hold: a second hold mints a NEW epoch; map siblings hold CONCURRENTLY as distinct rows — the LedgerWatch F2 cure); the id (uuid7) is THE reply handle; the payload rides the row (deliver-no-drop); the epoch + call_id kill the stale-payload dragon (the wait site answering a call it never made).';
