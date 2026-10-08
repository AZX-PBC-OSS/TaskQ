-- The workflow flow-link index, REBUILT ON THE READ SHAPE (the phase-2
-- attack's H3 cure). Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "{schema}" token is substituted at
-- apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE INDEX STATEMENTS (SHARE lock class — blocks
-- writes for each build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed in). ALL WORKFLOW DDL IS ADDITIVE — no `post_`
-- phase ever ships for v1 (the forever rule).
--
-- THE REPRESENTATION, STATED ONCE: the flow_id linkage is the uuid
-- STRING in metadata.flow_id (the stamp's own shape — the node insert,
-- the fork writer, and the seed helpers all write the uuid's text);
-- every shipped READ compares the uuid it names —
-- ``(metadata->>'flow_id')::uuid = $1::uuid`` — because jobs.id IS uuid.
-- 01.00.25_01 built the expression index on the RAW TEXT key
-- ((metadata->>'flow_id'), status): the CAST between the index
-- expression and every read's comparison breaks the index match, and
-- every flow-scoped read became a Seq Scan of the fleet table at fleet
-- shapes (measured: the grouped rollup read ONE 500-node run in p50
-- 15.2 ms at 221k rows, LINEAR in the fleet table, not the run).
--
-- THE CURE: the index expression carries the same uuid cast the reads
-- carry — the expression and the comparison now agree letter for letter —
-- and the partial predicate names the INDEX'S TRUE POPULATION: a
-- WORKFLOW row (the flow link PRESENT — `metadata ? 'flow_id'` — and a
-- step key). 01.00.25_01's partial (step_key IS NOT NULL alone) was a
-- no-filter in production: VANILLA rows carry step keys too, so the
-- "partial" index held the whole fleet and the planner priced the
-- fleet-wide reads off it as a seq scan. With the true partial, every
-- flow-scoped read (the grouped rollup, the per-node variant, the
-- maintenance leg's per-flow join, the wf-progress gauge's sampler) is
-- served by an index over ONE run's rows — O(the run's node count),
-- never O(the fleet table) — and the reads carry the exact clauses
-- (step_key <> '__flow__' + metadata ? 'flow_id') that imply the
-- predicate, so the planner can prove the partial serves them.
--
-- The reads and the stamp are UNCHANGED in their representation: the
-- text key in metadata IS the one representation (the uuid's text); the
-- reads cast it to uuid for comparison and now ALSO name the flow link's
-- presence — semantically exact (a row without the link can never match
-- the uuid comparison), and it is what makes the partial provable.
--
-- The partial predicate is unchanged in its first conjunct (workflow
-- rows only — step_key IS NOT NULL; the vanilla population's maintenance
-- exemption, the measured partial-index doctrine).

DROP INDEX IF EXISTS "{schema}".jobs_wf_flow_nodes_idx;

CREATE INDEX IF NOT EXISTS jobs_wf_flow_nodes_idx
    ON "{schema}".jobs (((metadata->>'flow_id')::uuid), status)
    WHERE step_key IS NOT NULL AND metadata ? 'flow_id';
