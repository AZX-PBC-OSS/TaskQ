-- Indexes for the bounded maintenance/cancel paths audited against a
-- realistically-seeded schema (93k jobs / 123k job_attempts / 150k
-- job_events on PostgreSQL 18, EXPLAIN (ANALYZE, BUFFERS); see the
-- verdict table in the audit trail). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- What this closes (measured, pre-index):
--   * cancel_where(queue=...) / list-by-queue: the matching CTE
--     (`WHERE queue = $1 AND status IN ('pending','scheduled')
--     ORDER BY id LIMIT $n`) had NO usable index — every partial index
--     on jobs with a queue key column is partial on
--     `status = 'pending'` alone, which cannot serve
--     `status IN ('pending','scheduled')`. The planner fell back to a
--     whole-table walk of jobs' PK in id order: ~2k buffers per batch
--     while dense, a full 93k-entry walk (~30-40 ms) for the drain's
--     final empty call, and a growing re-walk of already-cancelled
--     prefix rows across the drain (O(batches²) probes).
--   * cancel_where(actor=...) and deregister_actor's force-cancel drain
--     had the same shape: jobs_actor_pending_idx (actor) cannot
--     provide the `ORDER BY id` the CTE pins, so at realistic volume
--     the planner PK-walked the whole table instead of seeking the
--     actor's rows.
--   * cleanup_stale_workers' DDL fan-out: job_attempts.worker_id
--     REFERENCES workers(id) ON DELETE SET NULL, and with no index on
--     job_attempts(worker_id) the RI trigger ran a full seq scan of
--     job_attempts PER DELETED WORKER — 688 ms of trigger time to
--     delete 100 stale workers at 123k attempts (the fleet-crash
--     cleanup drains that in 4 batches per tick). With the index the
--     same delete measures 167 ms, the residual being the intrinsic
--     SET NULL rewrite of the ~208 attempt rows each worker references.
--
-- ── Why plain CREATE INDEX, not the no-transaction CONCURRENTLY form ──
-- The migration runner's no-transaction directive + CREATE INDEX
-- CONCURRENTLY template (src/taskq/migrate.py's module docstring) was
-- tried for this file and REVERTED: it deadlocks under the runner's own
-- startup discipline. apply_pending_locked serializes concurrent
-- migrators with pg_advisory_lock (the wait shape
-- tests/test_migrate_lock_integration.py::
-- test_concurrent_migrations_serialize_rather_than_racing pins: two
-- replicas starting at once serialize on that lock rather than race —
-- exactly the scenario the lock exists for); a second replica's blocking
-- lock wait is an open transaction, and CREATE INDEX CONCURRENTLY waits
-- for every transaction that started before it — so a CIC build here
-- would wait on the replica's advisory-lock wait, which waits on the
-- first migrator's advisory lock: a cycle the deadlock detector breaks
-- by failing the apply. The cycle follows from CIC's documented
-- snapshot-wait behavior against the serialized-wait shape the test
-- pins. So this file follows 01.00.02_01_pre_job_events_outbox.sql's
-- precedent instead: a transactional plain CREATE INDEX, whose
-- ordinary locks queue behind the advisory-lock waiter without a
-- snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.02_01: each CREATE INDEX
-- below takes a write-blocking lock on its table for the duration of
-- the build (jobs is the hottest table in the system — enqueue,
-- dispatch, and heartbeat all write it). Build time scales with the
-- current row count; on a large, busy production jobs table this can
-- stall the worker fleet's writes for a noticeable window. Apply
-- during a maintenance window (or when jobs is small/quiescent, e.g.
-- right after a prune sweep) on any deployment where jobs is large.
-- The default event_writer_batch_size drain keeps steady-state jobs
-- small, so most deployments see momentary builds.

-- Bulk-cancel / list by queue, active rows only: the (queue, id) key
-- serves the cancel_where(queue=...) matching CTE's
-- `queue = $1 AND status IN ('pending','scheduled') ORDER BY id LIMIT`
-- as an Index Cond seek with id-ordered early termination (measured
-- post-index: 100 rows in ~0.1 ms / ~100 buffers dense; the drained
-- final call stops at the queue's boundary instead of walking the
-- table). Partial on the active statuses for the same reason as every
-- dispatch index: terminal rows would only bloat the key range and
-- every terminal write would otherwise pay index maintenance for a
-- filter that can never match them again.
CREATE INDEX IF NOT EXISTS jobs_queue_active_idx
    ON "{schema}".jobs (queue, id)
    WHERE status IN ('pending', 'scheduled');

-- Bulk-cancel / deregister by actor, active rows only: (actor, id)
-- serves the `actor = $1 AND status IN ('pending','scheduled')
-- ORDER BY id LIMIT` CTEs in cancel_where(actor=...) and
-- deregister_actor's force-cancel drain as an actor-prefix seek in id
-- order (measured post-index: 100 rows in ~0.08-0.12 ms / ~100
-- buffers, vs a 1.9k-buffer whole-table PK walk before).
-- DELIBERATE overlap: jobs_actor_pending_idx (actor) — partial on the
-- same statuses — already serves the max_pending count probe and is
-- plan-pinned by tests/test_postgres_max_pending.py; this initiative
-- never drops structures, so both coexist. A future initiative may
-- consolidate to this index alone by re-pointing that pin (an
-- (actor, id) key serves `WHERE actor = $1` counts identically).
CREATE INDEX IF NOT EXISTS jobs_actor_active_id_idx
    ON "{schema}".jobs (actor, id)
    WHERE status IN ('pending', 'scheduled');

-- cleanup_stale_workers' ON DELETE SET NULL fan-out: the
-- job_attempts_worker_id_fkey RI trigger probes
-- `worker_id = $1` once per deleted worker row; with no index that
-- probe is a full seq scan of job_attempts (measured 688 ms per 100
-- deleted workers at 123k attempts). Partial on
-- `worker_id IS NOT NULL` following the reservation_slots_job_id_idx
-- convention: equality against a non-null key implies the predicate,
-- so the RI trigger's parameterized plan uses it (verified with
-- EXPLAIN: Bitmap Index Scan, Index Cond worker_id = $1), and rows
-- leave the index as the SET NULL rewrites them — the index only ever
-- contains rows the trigger can still match.
CREATE INDEX IF NOT EXISTS job_attempts_worker_id_idx
    ON "{schema}".job_attempts (worker_id)
    WHERE worker_id IS NOT NULL;
