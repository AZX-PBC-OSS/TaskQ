-- The one-time backfill of the assignment-routed marker (the columns
-- arrive in 01.00.12_05_pre_assignment_routed_columns.sql). Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- ── What it paints, and why exactly this population ───────────────────
-- DEFAULT false with the column added NOT NULL is already correct for
-- every producer-placed row, so the backfill exists for exactly one
-- population: rows already claimed once that are dispatchable now
-- (pending) or become dispatchable when the scheduled-to-pending
-- promotion sweep flips them (scheduled). The promotion only re-dates a
-- row; it learns nothing about its origin and writes no marker, so a
-- delayed re-pend (a snooze, a retry-after, a reclaim with budget left)
-- still sleeping off its deferral at upgrade time must get the flag
-- HERE: its re-pend already happened, before the column existed. Every
-- row outside this population is either not dispatchable (running,
-- terminal) or producer-placed, and producer placement always has
-- started_at IS NULL (an enqueue is never a claim), including
-- future-dated scheduled enqueues, so the started_at conjunct keeps
-- the population exact. The assignment_routed = false conjunct keeps
-- the statement a no-op on rows a re-pend path has already flagged, so
-- a re-run never clobbers them.
--
-- Carry the old proxy's population forward, so a fleet upgrading
-- mid-flight keeps routing its in-flight re-pended tails by the
-- assignment exactly as before the upgrade: the pre-upgrade arm probed
-- pending rows with started_at IS NOT NULL, and a re-pended row still
-- in its deferral (scheduled, started_at set) joined that probe the
-- moment promotion flipped it to pending, both statuses name the
-- population the pre-upgrade fleet was serving, or was about to serve,
-- by assignment. Bounded by that population, not the backlog.
--
-- ── Why the backfill is its own migration ────────────────────────────
-- An UPDATE holds ROW EXCLUSIVE on jobs, not ACCESS EXCLUSIVE: it
-- blocks neither readers nor other writers (only writers of the same
-- rows, and no dispatch path writes a pending row it has not first
-- claimed through the row lock). A backfill over a deep re-pend backlog
-- can therefore take as long as it needs without parking the fleet's
-- reads, heartbeats or claims, which is exactly why it must NOT share
-- a transaction with the marker columns' ALTER, whose ACCESS EXCLUSIVE
-- would otherwise be held across it (issue #250). It also sequences
-- BEFORE the probe-index builds that read the flag (01.00.12_08 and
-- 01.00.12_09): building after the backfill means one build over the
-- final flag values, rather than an index the backfill then maintains
-- row-by-row through non-HOT updates.
UPDATE "{schema}".jobs
   SET assignment_routed = true
 WHERE status IN ('pending', 'scheduled')
   AND started_at IS NOT NULL
   AND assignment_routed = false;
