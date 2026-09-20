-- jobs_finished_at_idx becomes (status, finished_at) INCLUDE (id): the archive prune's
-- candidate window selects one terminal status over a finished_at range in
-- finished_at order with a LIMIT, and the composite serves all three in one
-- index (equality on the leading column, the range as the second, the order
-- preserved), which the (finished_at)-only shape cannot: measured on
-- PostgreSQL 18, the planner routes the prune's candidate window through a
-- skip scan on jobs_identity_status_idx (a bitmap over the ENTIRE terminal
-- population, a post-scan Filter on the range, a Sort), which is a
-- population scan wearing an index, exactly what the archive's own plan
-- pins forbid. The composite makes the population scan unchoosable.
--
-- The partial predicate the original index carried is DELIBERATELY
-- dropped. The plancache's generic plan (the form a long-lived leader
-- connection's daily prune settles into) cannot prove ``status = $1``
-- implies ``status IN (five)``, so a partial predicate EXCLUDES this
-- index from the generic plan entirely, leaving the skip-scan bitmap on
-- jobs_identity_status_idx as the planner's only status-serving option:
-- measured, that plan is a whole-population walk (bitmap the terminal
-- population, heap-fetch every candidate, filter the range, sort it) in
-- the very executions the daily prune lives in. The non-partial index is
-- provable for any bound parameter, so the generic plan keeps the
-- bounded index-only scan: Index Cond (status, finished_at), the range
-- as the seek, the LIMIT terminating the scan, measured correct (10,000
-- archived per batch) and bounded (no Sort, no population walk) in the
-- generic plan on a 70k-row terminal corpus. The size cost is the
-- running/scheduled rows (finished_at NULL, tiny); the history pages
-- filter multi-status sets ordered by status_priority first and never
-- used this index; their plans are unchanged.
--
-- ROLLING DEPLOY: drop and recreate is instant (one index, metadata-only
-- lock windows); a moment of concurrent prune-and-retry sees the old plan
-- shape, not an error. Forward-only; to revert, recreate the
-- (finished_at)-only shape from 01.00.00_01.

-- The INCLUDE (id) is not decoration: the prune's candidate window selects
-- only the id column, so with the id carried in the index the window is an
-- index-only scan (no heap fetches at all). Measured on PostgreSQL 18
-- (70k-row terminal corpus, the plancache's generic plan): without the
-- INCLUDE the generic plan costs the skip-scan bitmap cheaper and the
-- prune becomes a population scan; with it, the skip-scan alternative
-- cannot win at any row estimate and the window stays a bounded,
-- order-preserving index scan.

DROP INDEX IF EXISTS "{schema}".jobs_finished_at_idx;

CREATE INDEX jobs_finished_at_idx
    ON "{schema}".jobs (status, finished_at) INCLUDE (id);
