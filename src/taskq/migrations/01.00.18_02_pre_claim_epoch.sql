-- Non-saturating claim-epoch fence for the terminal/ownership writes.
--
-- The claim stamps the DISPLAYED attempt counter with a saturating
-- increment (``attempt = LEAST(j.attempt + 1, 32767)``,
-- backend/_dispatch_sql.py) so a row parked at the smallint ceiling
-- cannot turn a whole claim round into a smallint-out-of-range driver
-- error. At the ceiling the value stops advancing, which means the
-- terminal-write fences (``status = 'running' AND locked_by_worker = $n
-- AND attempt = $k``) can no longer tell a stale execution from the live
-- one: a reclaim plus a redispatch to the same worker leaves both the
-- stale handler and the live handler holding the same
-- (worker, attempt) pair, and the stale execution's mark_succeeded wins
-- while the live one is fenced out. The displayed counter must keep its
-- saturating, consumer-visible semantics, so the fix is a SECOND column:
--
-- claim_epoch is the row's non-saturating claim counter. Every
-- successful dispatch claim bumps it by exactly 1 (bigint: it does not
-- saturate, no reachable workload approaches 2^63 claims of one row).
-- Every terminal/ownership write that fences on ``attempt`` gains the
-- equality conjunct ``claim_epoch = $n`` and binds the epoch from its
-- own claim view (JobRow.claim_epoch, the value the dispatch round
-- returned). Because fences compare EQUALITY, never magnitude, the
-- epoch's only requirement is uniqueness across consecutive claims of
-- the same row, and +1 per claim provides that without bound.
--
-- INVARIANT: between two terminal/ownership writes that both fence on
-- claim_epoch, a successful claim of the same row MUST have bumped it.
-- The reclaim sweeps that clear locks (sweep 1's re-pend/crash/cancel
-- arms, the heartbeat isolate) deliberately leave claim_epoch
-- untouched: the reclaim itself hands the row back, and the NEXT claim
-- bumps it. A stale writer's epoch therefore goes stale at the moment a
-- new claim exists, on every path, including at the attempt ceiling
-- where attempt alone can no longer distinguish the two executions.
-- Writer order per row is: claim (bump) -> maybe reclaim (leave) ->
-- claim (bump) -> terminal write (fence on the epoch its own claim
-- returned). No writer except the dispatch claim ever assigns the
-- column.
--
-- The in-memory testing backend mirrors the semantics exactly
-- (taskq/testing/_dispatch.py bumps on claim, taskq/testing/_terminal.py
-- fences on equality), so the equivalence tier exercises the same
-- fence.
--
-- ROLLING DEPLOY: additive with a default, applied while old pods run:
-- the previous release's statements name their columns explicitly and
-- never read this one. This release's claim bumps it unconditionally,
-- which is safe against old workers too: an old worker's terminal write
-- never references the column, and the fence it does not carry is the
-- defect this migration ships the fix for. Forward-only; there is no
-- down migration.
--
-- OPS NOTE (locks): ADD COLUMN with a non-volatile default is
-- metadata-only on PG >= 11 (no table rewrite; the default is stored in
-- pg_attribute and read from there), so this add does not stall a
-- fleet. Existing rows read 0, which is correct rather than merely
-- harmless: fences compare equality, so epoch 0 is simply the one
-- epoch no claim of a pre-migration row can ever be stamped with (the
-- first post-migration claim stamps 1), and a pre-migration stale
-- writer (which binds no epoch at all) is fenced out by its missing
-- attempt proof exactly as before.
ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;

COMMENT ON COLUMN "{schema}".jobs.claim_epoch IS
    'Non-saturating claim-epoch fence. Bumped by exactly 1 on every '
    'successful dispatch claim; the reclaim sweeps that clear locks leave '
    'it untouched (the next claim bumps it). Every terminal/ownership '
    'write that fences on attempt also fences on this column equalling '
    'the epoch from its own claim view, so a reclaim plus redispatch can '
    'never leave a stale execution and the live one sharing a fence, '
    'including at the attempt ceiling where the displayed attempt counter '
    'saturates. Fences compare equality, never magnitude; writers other '
    'than the dispatch claim never assign this column.';
