-- The failure-policy column on the edge ledger — T06 (the failed-parent
-- propagation). Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "{schema}" token is substituted at
-- apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE METADATA-ONLY ALTER (ACCESS EXCLUSIVE, held for
-- milliseconds on the catalog — no table rewrite: a NOT NULL column with a
-- CONSTANT default is filled lazily, Postgres skips the rewrite). No index
-- builds, no CREATE TABLEs mixed in.
--
-- WHY A COLUMN ON wf_edge: the edge ledger is the join counter's ONLY
-- truth, and the propagation rule (T06) is a PER-EDGE fact — the same
-- failed child may feed a fail_closed join (cascade: the join blocks, the
-- flow fails, the peers peer-cancel) and a collect join (the failure fans
-- in as an Item failure; the join fires with the typed partial). The
-- policy is DECLARED at fork/declared-join time and RECORDED here — T08's
-- derivation reads the policy off this ledger ("the absorption is on the
-- record ... never a heuristic"); it is never inferred from the join's
-- shape.
--
-- DEFAULT 'fail_closed': the safe default (a failed parent must not
-- silently fire a join over a partial result). 'collect' is the declared
-- opt-in (T06's second semantics).
--
-- ALL WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1 (the
-- forever rule); a pre-migration worker reads the ledger unchanged (it
-- never selects this column).

ALTER TABLE "{schema}".wf_edge
    ADD COLUMN IF NOT EXISTS failure_policy text NOT NULL DEFAULT 'fail_closed';

COMMENT ON COLUMN "{schema}".wf_edge.failure_policy IS
    'The declared failure policy of THIS edge (what a terminal failure of the parent does to the child join): ''fail_closed'' (the default — the join blocks, the flow fails, the running peers peer-cancel) or ''collect'' (the failure fans in as an Item failure; the join fires with the typed partial). Declared at fork/join time; recorded on the ledger — the derivation reads it, never infers it.';
