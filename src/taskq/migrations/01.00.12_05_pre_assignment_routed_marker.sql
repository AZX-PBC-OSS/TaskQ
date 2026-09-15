-- The assignment-routed marker: the durable flag that tells a re-pended
-- row apart from a producer-placed one, replacing the started_at proxy
-- the dispatch CTE's assignment-routed arm used for the same question.
-- Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by
-- the migration runner.
--
-- ── Why a marker column and not started_at ────────────────────────────
-- The routing question the dispatch arm actually asks is about ORIGIN:
-- was this pending row placed here by a producer, or handed back by a
-- re-pend? Producer placement governs its own routing (an explicit
-- enqueue(queue=...) keeps its queue, and a stale producer's post-move
-- enqueue to a retired source queue stays served by that queue's
-- consumers); a re-pend routes by the actor's CURRENT stored assignment,
-- so a move's left-behind tails are not stranded on a queue the operator
-- was told to stop consuming.
--
-- started_at answered that question only by proxy — "was claimed at least
-- once" — and the proxy is wrong in one direction that matters: an
-- operator retry of a job that was terminalized BEFORE it was ever
-- claimed is a re-pend (an operator hands the row back deliberately) but
-- has started_at IS NULL, so it routed by its stale label and stranded
-- permanently: pending, due, and invisible to every running consumer.
-- Widening the arm to all pending rows is not the fix — that would route
-- producer-placed strays by the assignment too, collapsing the stray
-- contract the never-claimed arm exists to hold. The marker names the
-- distinction directly instead of inferring it, so each population is
-- exactly what its arm means.
--
-- started_at keeps its own meaning intact (the audit trail of whether the
-- job ever ran), which the proxy was quietly overloading.
--
-- ── Backfill ──────────────────────────────────────────────────────────
-- DEFAULT false with the column added NOT NULL: on this Postgres
-- generation a non-volatile default is stored in the catalog rather than
-- rewriting the table, so the ALTER is a metadata-only operation whose
-- cost does not scale with the jobs backlog. Existing rows then need the
-- one-time backfill below, which is bounded to the only population the
-- assignment-routed arm can ever visit — rows already claimed once that
-- are dispatchable now (pending) or become dispatchable when the
-- scheduled-to-pending promotion sweep flips them (scheduled) — rather
-- than the whole table. The promotion only re-dates a row; it learns
-- nothing about its origin and writes no marker, so a delayed re-pend
-- (a snooze, a retry-after, a reclaim with budget left) still sleeping
-- off its deferral at upgrade time must get the flag HERE — its re-pend
-- already happened, before the column existed. Every row outside this
-- population is either not dispatchable (running, terminal) or
-- producer-placed, and producer placement always has started_at IS NULL
-- (an enqueue is never a claim), including future-dated scheduled
-- enqueues — so the started_at conjunct keeps the population exact and
-- DEFAULT false is already correct for everything else. The
-- assignment_routed = false conjunct keeps the statement a no-op on
-- rows a re-pend path has already flagged, so a re-run never clobbers
-- them.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS assignment_routed boolean NOT NULL DEFAULT false;

-- The archive mirrors jobs column-for-column. The archive move itself
-- does not carry this column: an archived row is terminal and never
-- dispatched again, so the routing marker is inert there and the DDL
-- default below is the correct value for every archived row.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS assignment_routed boolean NOT NULL DEFAULT false;

-- Carry the old proxy's population forward, so a fleet upgrading mid-flight
-- keeps routing its in-flight re-pended tails by the assignment exactly as
-- before the upgrade. The pre-upgrade arm probed pending rows with
-- started_at IS NOT NULL, and a re-pended row still in its deferral
-- (scheduled, started_at set) joined that probe the moment promotion
-- flipped it to pending — so both statuses name the population the
-- pre-upgrade fleet was serving, or was about to serve, by assignment.
-- Bounded by that population, not the backlog.
UPDATE "{schema}".jobs
   SET assignment_routed = true
 WHERE status IN ('pending', 'scheduled')
   AND started_at IS NOT NULL
   AND assignment_routed = false;

-- The probe index follows the marker. Same geometry and same rationale as
-- the index it replaces (01.00.11_01_pre_repended_probe_index.sql): the
-- arm probes per (actor, fairness cohort) with actor equality plus
-- COALESCE(fairness_key, '__null__') equality as a two-column Index Cond
-- prefix, and the index's own (priority DESC, scheduled_at, id) order
-- serves the probe's ORDER BY without a sort, so each probe stops at its
-- LIMIT regardless of cohort depth. The COALESCE expression is IMMUTABLE
-- and must stay VERBATIM-identical to every use in the dispatch SQL, or
-- the expression index stops serving the query.
--
-- OPS NOTE (locks), same caveat as every sibling index migration: the
-- CREATE INDEX takes a write-blocking lock on jobs for the duration of the
-- build. Operators with a large jobs table should run the equivalent
-- `CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_assignment_routed_probe_idx
-- ON "{schema}".jobs (actor, COALESCE(fairness_key, '__null__'),
-- priority DESC, scheduled_at, id) WHERE status = 'pending' AND
-- assignment_routed` manually outside the migration runner during a
-- maintenance window, then let this migration no-op via IF NOT EXISTS.
CREATE INDEX IF NOT EXISTS jobs_assignment_routed_probe_idx
    ON "{schema}".jobs (actor, COALESCE(fairness_key, '__null__'),
                        priority DESC, scheduled_at, id)
    WHERE status = 'pending' AND assignment_routed;

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
-- OPS NOTE (locks): as with every sibling index migration, the build
-- takes a write-blocking lock on jobs. Operators with a large jobs table
-- should run the equivalent `CREATE INDEX CONCURRENTLY IF NOT EXISTS
-- jobs_actor_queue_backlog_idx ON "{schema}".jobs (actor, queue, id)
-- WHERE status IN ('pending', 'scheduled')` manually outside the
-- migration runner during a maintenance window, then let this migration
-- no-op via IF NOT EXISTS.
CREATE INDEX IF NOT EXISTS jobs_actor_queue_backlog_idx
    ON "{schema}".jobs (actor, queue, id)
    WHERE status IN ('pending', 'scheduled');
