-- THE FLOW-HISTORY RETENTION ARMS' INDEXES (five workflow tables grew
-- monotone-forever: no retention arm, no FK for the job prune to
-- cascade). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- The no-transaction + CREATE INDEX CONCURRENTLY shape (the template
-- src/taskq/migrate.py's module docstring documents) was tried for this
-- file and REVERTED — the 01.00.06_01 law, re-proven here by the
-- 8-applier boot race (tests/test_attack_lifecycle_edges.py::
-- test_boot_race_8_appliers_fresh_schema, red before this file went
-- plain; measured on PG 17: the winner's CIC parked on a
-- ``virtualxid`` ShareLock held by a replica WAITING on the migration
-- advisory lock, and the deadlock detector broke the cycle by failing
-- the apply — 7 of 8 appliers died in turn, each new winner deadlocking
-- against the still-parked queue, leaving INVALID index debris each
-- time). apply_pending_locked serializes concurrent migrators with
-- pg_advisory_lock — exactly the scenario the lock exists for — and a
-- replica's blocking advisory-lock wait is an open transaction a
-- concurrent CIC's blocker scan waits on; the cycle is structural, not
-- bad luck. A transactional plain CREATE INDEX cannot cycle: its
-- ordinary locks queue behind the advisory-lock waiter without any
-- snapshot-wait scan, and a failed apply rolls back WHOLE (no INVALID
-- debris — the runner's drop-first no-transaction template is never
-- needed here). tests/test_migrations_unit.py::
-- test_bundled_migrations_are_all_transactional pins the fleet-wide law
-- this file had briefly broken.
--
-- OPS NOTE (locks), same caveat as 01.00.06_01: each CREATE INDEX below
-- takes a SHARE lock on its table — the lock BLOCKS ALL WRITES (the
-- inserts/deletes above; reads proceed) for the duration of the build.
-- THE MEASURED THRESHOLD: the build span is ~1.0 s @ 3M rows on warm
-- NVMe (the round-2's data; re-measured here at 0.6 s warm-cache /
-- 1.8 s cold, PG 18, the 266 MB wf_edge shape) — a CLOUD VOLUME is a
-- multiple of any local-NVMe number (network latency × the build's
-- random I/O; state your own multiple honestly by measuring a staging
-- restore). THE BY-HAND THRESHOLD'S SHAPE: beyond ~1M rows on cloud
-- volumes, build by hand in a maintenance window instead —
--   DROP INDEX CONCURRENTLY IF EXISTS "{schema}".wf_edge_flow_idx;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS wf_edge_flow_idx
--       ON "{schema}".wf_edge (flow_id);
-- and the runner's IF NOT EXISTS makes the boot a no-op (the by-hand
-- DROP-first shape is also the cure for an interrupted HAND build's
-- INVALID index — and the boot's own DROP-then-CREATE below rebuilds
-- such debris transactionally when the window is missed).
--
-- THE SCAN THEY SERVE: the flow-history retention arms (five
-- statements serving SIX tables — the stream's fold rides the
-- node_progress arm's) are TWO-PASS (H-B1's law): the flows-side pass
-- selects the TERMINAL-AGED ROOTS from the JOBS side
-- (jobs_finished_at_idx — a jobs index, not this file's), then deletes
-- each selected flow's rows through the table's flow column — THESE
-- indexes carry that delete's join leg (and the rows-alone status
-- reconstruction's and the per-flow admin reads' every (flow_id)-led
-- probe on tables that had none). The arms' rootless pass (the orphan
-- class) walks the table's own PK, batch-bounded, seeking past its
-- persisted cursor (01.00.36_01) — it needs no index from this file.
--
-- WHICH INDEXES (checked against the existing set — 01.00.24_03,
-- 01.00.28_01, 01.00.34_02):
--
-- * wf_edge had NO flow_id-led index at all (only wf_edge_parent_idx's
--   parent probe + the (child_id, parent_id) PK) — BUILD IT.
-- * wf_join_fire had NO flow_id-led index (only the UNIQUE(join_job_id)
--   arbiter + the id PK) — BUILD IT.
-- * wf_step_ledger's only flow_id-led index is the claim ARBITER
--   (flow_id, step_key, COALESCE(map_index,-1), attempt), whose
--   LEADING COLUMN serves the arms' flow probe — a separate plain
--   (flow_id) index is REDUNDANT write amplification on the family's
--   hottest table (measured 1.39ms with vs 2.25ms without for the
--   arbiter's own read — the arbiter stays; the plain index is NOT
--   built, H-B6).
-- * wf_signals needs NONE: wf_signals_run_status_idx
--   (workflow_id, status, id) already leads with the flow key.
-- * wf_node_progress needs NONE: its arm keys off the node's jobs row
--   (the PK probe on (node_id, channel) bounds the join leg, and the
--   table's population is nodes x channels — DH1's fence — so the
--   scan is bounded by construction).
DROP INDEX IF EXISTS "{schema}".wf_edge_flow_idx;

CREATE INDEX IF NOT EXISTS wf_edge_flow_idx
    ON "{schema}".wf_edge (flow_id);

DROP INDEX IF EXISTS "{schema}".wf_join_fire_flow_idx;

CREATE INDEX IF NOT EXISTS wf_join_fire_flow_idx
    ON "{schema}".wf_join_fire (flow_id);
