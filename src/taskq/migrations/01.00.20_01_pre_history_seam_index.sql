-- The admin /history page's seam-walk indexes. Forward-only; there is no
-- down migration. To revert, drop the two indexes by hand.
--
-- ── Why these indexes exist ─────────────────────────────────────────
-- The /history walk orders the union of jobs_archive and live terminal
-- jobs by the seam tuple `(COALESCE(finished_at, '9999-12-31
-- 23:59:59+00'::timestamptz), created_at, id) DESC`. The COALESCE in the
-- sort key is the walk's NULL handling: an unfinished (or archived
-- unfinished) row's seam position is the ceiling, so a still-running row
-- sorts at the top of the audit trail and a NULL-finished row can never
-- sit on the far side of a keyset cursor that compares the same
-- COALESCE. That expression is the point: NO index over bare
-- `finished_at` can serve a sort whose leading key is a function of it,
-- so before this migration every page turn of /history read and top-N
-- sorted EVERY matching row of the archive. Measured on PostgreSQL 18.6
-- at a 100,000-row archive: 18 ms per page (first page and cursor page
-- alike, the keyset predicate bounds nothing ahead of the sort), Seq
-- Scan + Sort over ~100k rows, growing linearly with retention.
--
-- The fix is the expression the walk already sorts by, indexed: an
-- index on `(COALESCE(finished_at, ceiling), created_at, id)` turns each
-- branch's ORDER BY into a backward index scan that starts at the
-- requested seam and stops at the page size. With the /history route
-- fetching each branch sorted-and-limited and merging the (at most two
-- pages') rows, the page cost stops depending on the archive's size:
-- measured on the same shape, 18.0 ms → 0.31 ms (first page) and
-- 0.38 ms (cursor page 2), and flat as the archive grows.
--
-- ── Why the expression must match byte-for-byte ──────────────────────
-- The planner matches the index to the query's ORDER BY only when the
-- indexed expression and the query's expression are structurally the
-- same constant fold. taskq/web/admin/history.py's
-- `_HISTORY_SEAM_PREDICATE` and `_HISTORY_ORDER_BY` spell the same
-- literal (`'9999-12-31 23:59:59+00'::timestamptz`); this file spells it
-- identically. A future change to either side must change all three
-- together, and the history seam tests (the walk-exactly-once and
-- never-replay pins) fail closed if the sort and the cursor ever
-- disagree.
--
-- ── Why the live jobs table gets one too ────────────────────────────
-- The walk's second branch reads live terminal rows. A deployment that
-- archives rarely (or a small fleet) keeps its terminal history in
-- `jobs` for a long time; the branch is the same shape and the same
-- growing sort without its own index. The live table also carries the
-- dispatch-path indexes; one more small expression index costs a few
-- bytes per terminal write and nothing on the hot path (no dispatch
-- statement orders by this tuple).
--
-- OPS NOTE (locks): plain CREATE INDEX inside the migration transaction,
-- the same precedent as 01.00.19_01 (the runner serializes migrators
-- with pg_advisory_lock; CONCURRENTLY cannot run in a transaction and a
-- bare wait for old snapshots deadlocks against the advisory waiter).
-- The build locks writes on each table for the build's duration; build
-- time is proportional to the current row count of each table. Operators
-- with a very large archive can pre-build the archive's index with
-- CREATE INDEX CONCURRENTLY before applying this migration; IF NOT
-- EXISTS keeps the migration a no-op then.

CREATE INDEX IF NOT EXISTS jobs_archive_seam_idx
    ON "{schema}".jobs_archive (
        (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz)),
        created_at,
        id
    );

CREATE INDEX IF NOT EXISTS jobs_seam_idx
    ON "{schema}".jobs (
        (COALESCE(finished_at, '9999-12-31 23:59:59+00'::timestamptz)),
        created_at,
        id
    );
