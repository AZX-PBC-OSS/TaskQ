-- The workflow indexes — T03. Forward-only; there is no down migration.
-- To revert, restore from backup. The literal "{schema}" token is substituted
-- at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENTS (SHARE lock class —
-- blocks writes for each build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed into the window). See 01.00.24_01's header for the
-- round's three-file split and the deployment sequence.
--
-- THE PARTIAL-INDEX DOCTRINE (§16.4, the measured truth): every workflow
-- index is PARTIAL — scoped to workflow rows only — so the vanilla
-- maintenance exemption is the point: the measured partial is 0.5% of the
-- same-column full index (194×; the pin asserts ≤ 1%).
--
-- Workflow rows are identified by `step_key IS NOT NULL` (vanilla rows never
-- set it). Join-wait rows are `status='pending' AND deps_pending > 0`.

-- The sweep's lock-first scan target: join-wait rows only. Tiny by
-- construction (a healthy fleet's join-wait population is in-flight joins);
-- the sweep's ordered walk rides it.
CREATE INDEX IF NOT EXISTS jobs_wf_join_wait_idx
    ON "{schema}".jobs (id)
    WHERE status = 'pending' AND deps_pending > 0;

-- The sweep's FIRE-arm probe (SWEEP_FIRE_SQL's locked CTE): join-wait rows
-- REGARDLESS of the counter. deps_pending > 0 must NOT be in this
-- predicate's definition (the SWEEP_FIRE_SQL index-served question): the
-- fire arm identifies its rows by the LEDGER's count, never by the cache —
-- the rederive statement in the same transaction just reconciled the
-- firable rows' cache to 0, so a counter-carrying predicate would hide
-- exactly the rows the arm exists to fire. The two partials split the
-- population by which arm's WHERE each serves: the rederive's lock-first
-- scan keeps jobs_wf_join_wait_idx (its WHERE implies deps_pending > 0),
-- the fire's scan rides this one.
CREATE INDEX IF NOT EXISTS jobs_wf_join_fire_probe_idx
    ON "{schema}".jobs (id)
    WHERE status = 'pending' AND metadata @> '{{"blocking_reason": "join"}}'::jsonb;

-- Children by parent: the fork-debt reconcile, the map's retry-in-place
-- lookup, and the status rollup's per-parent reads. Partial on workflow
-- child rows only.
CREATE INDEX IF NOT EXISTS jobs_wf_children_idx
    ON "{schema}".jobs (parent_id, id)
    WHERE parent_id IS NOT NULL;

-- The edge ledger's parent probe: the sweep's set-based re-derive counts
-- un-terminal parents per child by walking this index (the child side is
-- wf_edge's PK prefix). A Seq Scan here is the 83.7 ms unscoped monster
-- shape — the scope pin convicts it.
CREATE INDEX IF NOT EXISTS wf_edge_parent_idx
    ON "{schema}".wf_edge (parent_id);

-- The drain's undelivered scan: the proto's outbox_undelivered_idx shape.
CREATE INDEX IF NOT EXISTS wf_outbox_undelivered_idx
    ON "{schema}".wf_outbox (id)
    WHERE NOT delivered;

-- The ledger's per-flow reconstruction read (rows-only status rebuild, the
-- admin timeline, the drain of phantom 'running' rows). The claim arbiter
-- (wf_step_ledger_claim_uniq, below) covers the (flow_id, …) prefix; this
-- covers the per-job lookup the finalize fence and the claim path use.
CREATE INDEX IF NOT EXISTS wf_step_ledger_job_idx
    ON "{schema}".wf_step_ledger (job_id);

-- THE STEP LEDGER'S CLAIM ARBITER (T05's key contract, attack-hardened):
-- one row per (flow, step, map child, attempt) — map children of one step
-- key are DIFFERENT claims, so the arbiter keys on COALESCE(map_index, -1)
-- (the non-map step's NULL folds to -1; a bare
-- UNIQUE (flow_id, step_key, attempt) collapses map children onto ONE row:
-- the terminal write overwrites, the memoized replay returns the wrong
-- child's result). The expression matches LEDGER_CLAIM_SQL's
-- `ON CONFLICT (flow_id, step_key, COALESCE(map_index, -1), attempt)`
-- verbatim — an ON CONFLICT target infers only an arbiter spelled with the
-- identical expression.
CREATE UNIQUE INDEX IF NOT EXISTS wf_step_ledger_claim_uniq
    ON "{schema}".wf_step_ledger (flow_id, step_key, COALESCE(map_index, -1), attempt);

-- The phantom reaper's scan target (PHANTOM_REAP_SQL): 'running' rows only
-- — the arm's whole population, tiny by construction (in-flight attempts),
-- so the reap's EXISTS over terminal flows probes this partial instead of
-- scanning the ledger.
CREATE INDEX IF NOT EXISTS wf_step_ledger_running_idx
    ON "{schema}".wf_step_ledger (flow_id)
    WHERE status = 'running';

-- OPS NOTE — locking impact: each build takes SHARE on its table (blocks
-- writes, never reads) for the build's duration. A build scans the whole
-- table to evaluate the partial predicate per row, so the two `jobs` builds
-- scale with the jobs row count like any full index — the PARTIAL savings
-- are in steady-state size and maintenance (the measured 194×), not in the
-- one-time build. Apply clear of the busiest dispatch window on a large
-- `jobs` table, the same guidance 01.00.03_01 gave its unique-index build.
