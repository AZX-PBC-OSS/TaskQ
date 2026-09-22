-- Stamp the pre-ownership disabled population as operator intent, so the
-- residual NULL reading on a DISABLED row belongs only to the mixed-version
-- deploy window (issue #460).
--
-- Why: 01.00.19_02 added cron_schedules.disabled_by and read a NULL on a
-- disabled row as "disabled before the column existed" (operator intent,
-- never reverted by a boot). That reading is safe only for rows disabled
-- BEFORE the column existed. It is not safe for rows an OLD pod disables
-- AFTER it: the previous release's failure UPDATE writes enabled=false and
-- cannot name this column, so during a mixed-version rolling deploy (this
-- chain is additive and applies while old pods run, exactly as the
-- 01.00.19_02 header says) an old pod's transient-blip auto-disable lands
-- as enabled=false, disabled_by=NULL. The boot recovery predicate required
-- disabled_by='auto', never matched that row again, and the schedule stayed
-- disabled until a human re-enabled it. That is the unrecoverable cell of
-- issue #460.
--
-- This file closes the ambiguity by POPULATION instead of by value: every
-- disabled row that exists when this statement runs (the pre-ownership
-- population the 01.00.19_02 header already declared operator intent, plus
-- anything an old pod disabled before now) is stamped 'operator' and keeps
-- the create-only semantics it was written under. After it, a disabled row
-- with disabled_by=NULL can only be an old pod's write from the rest of the
-- deploy window. The boot recovery (worker/_bootstrap.py) reads that
-- residual NULL alongside 'auto' when the row carries the old failure arm's
-- fingerprint (consecutive_failures at or past the auto-disable threshold,
-- last_fire_error set): that is an old pod's auto-disable and the boot
-- reverts it. A NULL-disabled row WITHOUT the fingerprint reads as an old
-- pod's operator disable during the window and stays untouched.
--
-- Enabled rows are left alone: a marker on an enabled row is inert (the
-- revert requires enabled=false), and old pods re-enabling a row this
-- chain stamped cannot clear the column; the next real disable overwrites
-- the marker.
--
-- OPS NOTE (locks): one UPDATE over enabled=false AND disabled_by IS NULL.
-- Schedules are predominantly enabled, so the matched set is small, and the
-- row locks run under the same ddl_lock_timeout bound every migration runs
-- under. Old pods never read this column, so the stamp cannot change their
-- behavior; the statement is idempotent (a second run matches nothing).

UPDATE "{schema}".cron_schedules
SET disabled_by = 'operator'
WHERE enabled = false AND disabled_by IS NULL;

COMMENT ON COLUMN "{schema}".cron_schedules.disabled_by IS
    'Who disabled this schedule: ''auto'' = the cron loop''s failure-count '
    'auto-disable (a code re-declaration at worker startup reverts it), '
    '''operator'' = a deliberate operator disable (handle, CLI, admin UI, '
    'actor deregistration; never reverted by a boot). NULL = enabled, or a '
    'row an old pod disabled during a mixed-version deploy (01.00.19_05 '
    'stamped every disabled row that predates it ''operator''; the boot '
    'revert also recovers a NULL-disabled row carrying the old failure '
    'arm''s fingerprint).';
