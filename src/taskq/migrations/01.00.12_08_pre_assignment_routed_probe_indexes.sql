-- The assignment-routed population's two probe indexes (the marker
-- column arrives in 01.00.12_05, its backfill in 01.00.12_07).
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- The probe index follows the marker. Same geometry and same rationale
-- as the started_at-proxy index this round originally shipped and then
-- retired (the create/drop pair removed with it): the arm probes per
-- (actor, fairness cohort) with actor equality plus
-- COALESCE(fairness_key, '__null__') equality as a two-column Index
-- Cond prefix, and the index's own (priority DESC, scheduled_at, id)
-- order serves the probe's ORDER BY without a sort, so each probe stops
-- at its LIMIT regardless of cohort depth. The COALESCE expression is
-- IMMUTABLE and must stay VERBATIM-identical to every use in the
-- dispatch SQL, or the expression index stops serving the query.
--
-- The queue-move drain's window index. The drain re-selects "the next
-- batch_size of THIS actor's rows still carrying the SOURCE queue" on
-- every pass, so both actor and queue have to be Index Cond columns. On
-- the single-column partial indexes alone, whichever one the planner
-- picks leaves the other predicate as a post-scan Filter that walks the
-- population earlier batches already rewrote onto the target: batch N
-- pays for the (N-1) * batch_size rows already moved, and the drain's
-- total cost is quadratic in the backlog rather than linear. That only
-- bites on the deep backlogs where an operator most needs the move to
-- complete, and it shows up as a per-batch statement timeout that gets
-- worse the further the drain gets.
--
-- Leading (actor, queue) matches the drain's two equality predicates; the
-- trailing id serves its ORDER BY without a sort, so each batch is an
-- ordered scan that stops at its LIMIT. Partial on the dispatchable
-- statuses, which is the only population the drain moves — a terminal
-- row's queue label is inert.
--
-- ── Locks: one build per transaction, writers drain between them ────
-- Each CREATE INDEX below takes a SHARE lock on jobs for the duration
-- of its own build: reads (dispatch probes, depth samplers, the admin
-- UI) keep flowing, while writes (claims, heartbeats, re-pends) queue —
-- for THAT build only, because each build is its own transaction. The
-- lock queue is FIFO, so the writes that queued behind build N are
-- granted and run before build N+1 asks for the table: a fleet
-- upgrading with old workers still live sees one write-block window per
-- index, not one continuous window across the round (issue #250 — the
-- original single-transaction form held ACCESS EXCLUSIVE, which blocks
-- reads too, across both builds and the backfill).
--
-- OPS NOTE (locks), same caveat as every sibling index migration
-- (01.00.06_01, 01.00.09_01, 01.00.13_02, 01.00.13_03): build time is
-- proportional to the jobs ROW COUNT (a partial index build still scans
-- the whole table). Most deployments see momentary builds — the bounded
-- maintenance sweeps keep steady-state jobs small. On a deployment
-- where a single build would outrun the workers' heartbeat budget,
-- pre-build this file's indexes by hand outside the runner during a
-- maintenance window and let this migration no-op via IF NOT EXISTS:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_assignment_routed_probe_idx
--       ON "{schema}".jobs (actor, COALESCE(fairness_key, '__null__'),
--                           priority DESC, scheduled_at, id)
--       WHERE status = 'pending' AND assignment_routed;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_actor_queue_backlog_idx
--       ON "{schema}".jobs (actor, queue, id)
--       WHERE status IN ('pending', 'scheduled');
--
-- Plain transactional CREATE INDEX here, not the no-transaction
-- CONCURRENTLY form, for the same deadlock shape as every sibling
-- above: the migration runner serializes concurrent migrators with
-- pg_advisory_lock, a second replica's blocking lock wait is an open
-- transaction, and CREATE INDEX CONCURRENTLY waits for every
-- transaction that started before it — a cycle the deadlock detector
-- breaks by failing the apply.
CREATE INDEX IF NOT EXISTS jobs_assignment_routed_probe_idx
    ON "{schema}".jobs (actor, COALESCE(fairness_key, '__null__'),
                        priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND assignment_routed;

CREATE INDEX IF NOT EXISTS jobs_actor_queue_backlog_idx
    ON "{schema}".jobs (actor, queue, id)
    WHERE status IN ('pending', 'scheduled');
