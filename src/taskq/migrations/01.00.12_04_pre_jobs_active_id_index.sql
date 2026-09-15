-- Active-row indexes for the bulk-cancel drain. Forward-only; there is
-- no down migration. To revert, DROP INDEX. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- Bulk cancel drains its match set as repeated bounded batches, each
-- selecting "the next batch_size matching pending/scheduled rows ORDER
-- BY id". Both indexes below exist to keep one batch's scan cost
-- proportional to the batch rather than to the table, and they cover the
-- two ways that fails:
--
--   * Without an id-ordered index over active rows, the planner walks
--     jobs_pkey and applies the status predicate as a post-scan filter.
--     jobs_pkey contains every row an earlier batch already cancelled,
--     so batch N walks past and discards the (N-1) * batch_size rows its
--     predecessors moved: drain cost is quadratic in backlog depth.
--   * Without a tags index restricted to active rows, a tag filter is
--     answered from the unpartial jobs_tags_gin_idx, which likewise
--     still contains every cancelled row — so one tenant's offboard
--     pays for every other tenant's already-cancelled backlog, and
--     slows as the fleet grows with nothing in that tenant's own
--     metrics to explain it.
--
-- The operational shape of either is an offboard that finishes promptly
-- on a small backlog and, on a large one, keeps tripping its own
-- per-batch statement_timeout the deeper it gets — stranding the tail on
-- exactly the backlogs where the command matters most. Nothing errors;
-- the drain simply stops finishing.
--
-- Both partial on the active statuses, the same shape and reasoning as
-- the (queue, id) and (actor, id) active-row indexes in 01.00.06_01:
-- cancelled rows can never match the drain's predicate again, so keeping
-- them in the key range buys nothing and charges every terminal write
-- for the maintenance.
--
-- DELIBERATE overlap with jobs_tags_gin_idx, which serves tag filters
-- over ALL statuses (the admin list view, archive queries) and stays.
-- This initiative never drops structures.
--
-- OPS NOTE (locks): a plain CREATE INDEX takes a SHARE lock that blocks
-- writes to jobs for the build. Build these CONCURRENTLY by hand during
-- a maintenance window on any deployment where jobs is large — the same
-- guidance 01.00.06_01 carries, and for the same reason: the migration
-- runner wraps each file in a transaction, and CREATE INDEX
-- CONCURRENTLY cannot run inside one.
CREATE INDEX IF NOT EXISTS jobs_active_id_idx
    ON "{schema}".jobs (id)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS jobs_tags_active_gin_idx
    ON "{schema}".jobs USING gin (tags)
    WHERE status IN ('pending', 'scheduled');
