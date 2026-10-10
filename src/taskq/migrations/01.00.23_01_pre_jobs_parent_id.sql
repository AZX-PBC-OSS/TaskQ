-- The fan-out ledger: jobs.parent_id (LIB-2, issue #670).
-- Forward-only; there is no down migration. To revert, drop by hand:
--
--   DROP INDEX "{schema}".jobs_parent_pending_idx;
--   ALTER TABLE "{schema}".jobs DROP COLUMN parent_id;
--   ALTER TABLE "{schema}".jobs_archive DROP COLUMN parent_id;
--
-- ── Why this column exists ───────────────────────────────────────────
-- A fan-out parent (a dispatcher actor that enqueues thousands of
-- children) previously had NO parent linkage in the data model:
-- parentage existed only as tag inheritance via a contextvar
-- (taskq/client/_enqueuer.py set_parent_tags/parent_tags), so pending-
-- children depth was at best a tag-convention approximation with
-- user-controlled collisions. This column is the exact ledger: every
-- child enqueue under a parent's context stamps the parent's job id
-- (the same contextvar flow, set at worker entry in worker/run.py and
-- worker/_consumer.py), and the client's backpressure read counts
-- pending children WHERE parent_id = $1 exactly.
--
-- ── Why NO foreign key ───────────────────────────────────────────────
-- parent_id is a PLAIN column, deliberately:
--   1. An FK takes a key-share lock on the parent row per child
--      insert — unacceptable serialization + deadlock surface on the
--      hottest table at fan-out rates.
--   2. Retention purges delete by age/status in bulk: an FK would
--      block a parent's purge while children pend, or cascade-delete
--      pending children. Dangling parent_id (parent purged, children
--      pending) is a DEFINED, harmless state — the count counts
--      children by parent_id and never joins to the parent row.
--   3. Self-referencing FKs on a TimescaleDB hypertable are a
--      restriction minefield, not entered for an advisory signal.
--   4. Linkage integrity comes from the stamping logic; the snapshot's
--      fail-open unknown posture covers absence.
--
-- ── The index ────────────────────────────────────────────────────────
-- The pending-children count's quals, repeated VERBATIM (the
-- 01.00.12_06 doctrine: a partial index is only a candidate when the
-- planner can prove its predicate from the query's own quals):
--   parent_id = $1 AND status IN ('pending', 'scheduled')
-- The IS NOT NULL term keeps every unparented row (the overwhelming
-- majority — only fan-out children are stamped) out of the index, so
-- the WRITE-path cost is index maintenance on stamped rows only: zero
-- for the plain enqueue traffic. COUNT of the enqueue_max_pending form
-- (pending + scheduled) matches the admission predicate exactly — a
-- scheduled child holds a pending slot.
--
-- OPS NOTE (locks): ALTER TABLE ADD COLUMN (nullable, no default) is a
-- catalog-only metadata change in PostgreSQL — momentary ACCESS
-- EXCLUSIVE, no table rewrite, on jobs and jobs_archive alike,
-- hypertable or plain. The plain CREATE INDEX takes a SHARE lock that
-- blocks writes to jobs for the build. Build it CONCURRENTLY by hand
-- during a maintenance window on any deployment where jobs is large —
-- the same guidance 01.00.06_01 and 01.00.12_06 carry, and for the
-- same reason: the migration runner wraps each file in a transaction,
-- and CREATE INDEX CONCURRENTLY cannot run inside one.

ALTER TABLE "{schema}".jobs ADD COLUMN parent_id uuid;

ALTER TABLE "{schema}".jobs_archive ADD COLUMN parent_id uuid;

CREATE INDEX IF NOT EXISTS jobs_parent_pending_idx
    ON "{schema}".jobs (parent_id)
    WHERE status IN ('pending', 'scheduled') AND parent_id IS NOT NULL;
