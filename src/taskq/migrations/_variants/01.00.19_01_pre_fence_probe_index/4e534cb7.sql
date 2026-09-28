-- The fenced terminal/lease write's probe index: the job id as a trailing
-- KEY column of the running-holder partial index. Forward-only; there is
-- no down migration. To revert, recreate the two-key form
-- `(locked_by_worker) WHERE status = 'running'` by hand. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- ── Why this index exists ───────────────────────────────────────────
-- Every fenced single-row write (mark_succeeded / mark_failed /
-- mark_retry / mark_cancelled, the per-job lease checks) targets one row
-- by the fence
-- `id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $k
-- AND claim_epoch = $m`. The row is unique by `id = $1` (the primary
-- key), but the planner is free to drive the UPDATE from ANY index the
-- quals admit, and after bulk churn (a large enqueue burst lands before
-- autovacuum's next ANALYZE; `reltuples` goes stale and every equality
-- selectivity collapses to the same one-row estimate) the planner
-- measured on PostgreSQL 18 picks the running-holder partial index
-- `jobs_locked_by_worker_running_idx (locked_by_worker) WHERE status =
-- 'running'` and evaluates `id = $1` as a post-scan Filter: the write
-- walks EVERY running row the worker holds — measured at 1.3 ms and 91
-- buffers for one terminal write at a 2,000-row running population
-- (vs 0.06 ms and 18 buffers driven by the primary key), per terminal
-- write, for the whole drain.
--
-- With `id` as the trailing key column the same mispicked plan becomes
-- harmless: both equality quals are Index Conds (a non-leading key
-- column is still evaluated inside the index, never a heap-visiting
-- Filter), so the scan touches one index entry and one heap row
-- regardless of which index the planner picks or how stale the
-- statistics are. This is the hot-claim-path index discipline: the
-- column that filters every fenced probe rides in the key.
--
-- ── Why the trailing key costs (almost) nothing ────────────────────
-- The index is partial on `status = 'running'`, so its body holds only
-- in-flight rows and stays small by construction; entries leave the
-- index on every terminal/reclaim transition. Appending the id widens
-- each entry by ~16 bytes (uuid) on an index whose per-entry cost the
-- claim UPDATE's status transition already pays. Every existing reader
-- keeps its plan class: the heartbeat renewal
-- (`locked_by_worker = $1 AND status = 'running'`) and the web-admin
-- running count scan the leading-column prefix unchanged, and the
-- per-job lease check (`id = ANY($1::uuid[]) AND locked_by_worker =
-- $2`) gains the same trailing-key filtering this migration exists to
-- give the fence.
--
-- ── Why plain DROP + CREATE INDEX, not the no-transaction
--    CONCURRENTLY form ──
-- Same deadlock shape as 01.00.10_01 (see that file's full
-- derivation): the migration runner serializes concurrent migrators
-- with pg_advisory_lock, a second replica's blocking lock wait is an
-- open transaction, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it — a cycle the deadlock detector
-- breaks by failing the apply. This file therefore follows
-- 01.00.02_01 / 01.00.07_01 / 01.00.10_01's precedent: a transactional
-- plain DROP INDEX + CREATE INDEX, whose ordinary locks queue behind
-- the advisory-lock waiter without a snapshot-wait cycle.
--
-- OPS NOTE (locks), same caveat as 01.00.17_01: the DROP INDEX below
-- takes ACCESS EXCLUSIVE on jobs and shares this migration's
-- transaction with the CREATE INDEX that follows, so the lock is held
-- until COMMIT — for the whole build — and it blocks reads and writes
-- alike, not writes only. Build time is proportional to the whole
-- table: a non-concurrent CREATE INDEX heap-scans every row; the
-- partial predicate only decides which tuples are written into the
-- index, not how much of the table is read.
--
-- OPS NOTE (escape hatch), for operators with a large jobs table.
-- UNLIKE the precedent files, which contain no DROP INDEX at all, this
-- file must drop the legacy one-key form that currently owns the
-- canonical name — so a bare CONCURRENTLY pre-build under the canonical
-- name would be destroyed by this migration's own drop before its
-- IF NOT EXISTS could no-op. The swap below works around that by
-- parking the legacy form under a known name, and the drop in this
-- file is definition-conditional: it fires on every canonical owner
-- that is not the finished article — the legacy one-key form, an
-- INCLUDE(id) form (payload, never an Index Cond), INVALID debris from
-- an interrupted build — and spares only a valid, ready two-key form.
-- Run, outside the migration runner during a maintenance window:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS
--   jobs_locked_by_worker_running_idx_new ON "{schema}".jobs
--   (locked_by_worker, id) WHERE status = 'running';
--
-- then swap by name in ONE transaction (both statements are
-- metadata-only; the locks are momentary):
--
--   BEGIN;
--   ALTER INDEX "{schema}".jobs_locked_by_worker_running_idx
--       RENAME TO jobs_locked_by_worker_running_idx_old;
--   ALTER INDEX "{schema}".jobs_locked_by_worker_running_idx_new
--       RENAME TO jobs_locked_by_worker_running_idx;
--   COMMIT;
--
-- then apply this migration normally: the conditional drop sees the
-- valid two-key form already owning the canonical name and leaves it
-- alone, the parked `..._old` is dropped, and the CREATE INDEX IF NOT
-- EXISTS below no-ops. Final state is identical to the plain path: the
-- canonical name carries the two-key index, valid and in place.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relname = 'jobs_locked_by_worker_running_idx'
          AND NOT EXISTS (
              -- spare the drop only while a VALID, READY index whose KEY
              -- columns carry `id` owns the canonical name. The key columns
              -- come from pg_index, not pg_attribute: pg_attribute cannot
              -- tell a key column from an INCLUDE payload column, and an
              -- INCLUDE(id) form is not the two-key form this migration
              -- exists to land (the trailing id must ride in the index as
              -- an Index Cond, never payload). indisvalid/indisready:
              -- an interrupted CREATE INDEX CONCURRENTLY leaves INVALID
              -- debris that the IF NOT EXISTS below would silently keep
              -- (the runner's drop-the-debris discipline).
              SELECT 1
              FROM pg_catalog.pg_index i
              JOIN pg_catalog.pg_attribute a
                ON a.attrelid = i.indrelid
               AND a.attname = 'id'
               AND a.attisdropped = false
              WHERE i.indexrelid = c.oid
                AND i.indisvalid
                AND i.indisready
                AND a.attnum = ANY ((i.indkey::int2[])[0:i.indnkeyatts - 1])
          )
    ) THEN
        EXECUTE 'DROP INDEX "{schema}".jobs_locked_by_worker_running_idx';
    END IF;
END
$$;
DROP INDEX IF EXISTS "{schema}".jobs_locked_by_worker_running_idx_old;
CREATE INDEX IF NOT EXISTS jobs_locked_by_worker_running_idx
    ON "{schema}".jobs (locked_by_worker, id)
    WHERE status = 'running';
