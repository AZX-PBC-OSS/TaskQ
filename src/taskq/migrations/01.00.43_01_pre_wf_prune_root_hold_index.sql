-- The prune root-hold index: the finished-run roots the retention hold-back
-- inspects, served as their own tiny index. Forward-only; there is no down
-- migration. To revert, drop the index. The literal "{schema}" token is
-- substituted at apply time by the migration runner.
--
-- WHY. The prune's root hold-back (worker/_leader_shared.py,
-- _WORKFLOW_LIVENESS_GUARD_SQL and _workflow_root_hold_cte) withholds a
-- finished run's __flow__ root while any non-terminal child of that run
-- remains, or while an undelivered wf_outbox intent awaits materialization.
-- The held set is materialized once per candidate statement and excluded
-- from the candidate window, so the child probe runs per ROOT, never per
-- terminal row.
--
-- Finding those roots from the jobs side needs a root-only path. The
-- existing jobs_finished_at_idx is (status, finished_at) INCLUDE (id) over
-- the whole terminal population; filtering it on step_key = '__flow__'
-- would heap-fetch every terminal row of the status to discover the handful
-- of roots. This partial index keys on the same (status, finished_at)
-- selection and carries id, but only over the root population: one row per
-- run, so the held-set scan is O(roots), and the planner can prove the
-- partial predicate from the hold's own step_key = '__flow__' clause.
--
-- The held-set scan is the only consumer: it reads (status, finished_at)
-- as index conditions, projects id, and needs no heap access (the partial
-- predicate makes the scan index-only). Nothing else reads this index, and
-- it never replaces jobs_finished_at_idx's role as the candidate window's
-- ordered scan.
--
-- SHARE lock class (a CREATE INDEX build blocks writes for its duration,
-- never reads), the same phase obligation as the other index-only workflow
-- migrations. Forward-only; to revert, DROP INDEX.

CREATE INDEX IF NOT EXISTS jobs_wf_root_retention_idx
    ON "{schema}".jobs (status, finished_at) INCLUDE (id)
    WHERE step_key = '__flow__';
