-- Converges the fence-probe index to the two-key form on databases that
-- applied 01.00.19_01 as originally shipped. The original file's
-- definition-conditional drop read pg_attribute, which cannot tell a key
-- column from an INCLUDE payload column: an INCLUDE(id) form and INVALID
-- debris from an interrupted CREATE INDEX CONCURRENTLY were both spared,
-- and the trailing CREATE INDEX IF NOT EXISTS then no-oped — the canonical
-- name left owning an index whose trailing `id` rides as payload (never an
-- Index Cond: the plan pathology this index exists to fix) or is not valid
-- at all. Databases that ran the original therefore may hold any of the
-- six owner states; this file's condition reads pg_index — spares only a
-- valid, ready index whose KEY columns (indkey[:indnkeyatts]) carry `id`,
-- drops and rebuilds everything else. On a database already at the
-- finished two-key form this is a no-op; on a fresh database the restored
-- 01.00.19_01 lands the two-key form and this file no-ops behind it.
--
-- OPS NOTE (escape hatch), for operators with a large jobs table — same
-- shape as 01.00.19_01's: a bare CONCURRENTLY pre-build under the
-- canonical name would be destroyed by this file's own drop before its
-- IF NOT EXISTS could no-op, so pre-build under a scratch name and swap
-- by name in one transaction:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS
--   jobs_locked_by_worker_running_idx_new ON "{schema}".jobs
--   (locked_by_worker, id) WHERE status = 'running';
--
--   BEGIN;
--   ALTER INDEX "{schema}".jobs_locked_by_worker_running_idx
--       RENAME TO jobs_locked_by_worker_running_idx_old;
--   ALTER INDEX "{schema}".jobs_locked_by_worker_running_idx_new
--       RENAME TO jobs_locked_by_worker_running_idx;
--   COMMIT;
--
-- then apply this migration normally: the conditional drop spares the
-- valid two-key form, `..._old` is dropped, the CREATE no-ops.
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
              -- INCLUDE(id) form is not the two-key form this index exists
              -- to land (the trailing id must ride in the index as an Index
              -- Cond, never payload). indisvalid/indisready: an interrupted
              -- CREATE INDEX CONCURRENTLY leaves INVALID debris that the
              -- IF NOT EXISTS below would silently keep.
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
