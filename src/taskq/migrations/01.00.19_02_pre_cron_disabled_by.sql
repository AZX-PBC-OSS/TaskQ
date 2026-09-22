-- Ownership model for cron schedule disable state: WHO disabled a schedule.
--
-- Why: cron auto-disable (cron_auto_disable_threshold consecutive payload
-- factory failures) wrote only enabled=false, indistinguishable from an
-- operator's deliberate disable. Startup cron registration was create-only
-- (an operator's runtime disable must not be reverted by a redeploy), so the
-- two states shared one fate: a transient partial-DB blip that failed three
-- fires while the strike writes committed permanently halted critical
-- recurring work until a human re-enabled it.
--
-- disabled_by separates the two:
--
--   'auto'      the cron loop's auto-disable did it (recoverable: the code
--               re-declaring the schedule at startup reverts it, see the
--               registration pass in worker/_bootstrap.py).
--   'operator'  an operator disabled it (schedule handle disable(), the CLI,
--               the admin UI, actor deregistration). Never reverted by a
--               boot.
--   NULL        enabled (or a row disabled before this column existed:
--               pre-existing disabled rows predate ownership tracking, and
--               the safe reading of that ambiguity is operator intent, so
--               they keep the create-only semantics they were written under).
--
-- Forward-only; there is no down migration. To revert, DROP the column (all
-- readers treat NULL as "not auto-disabled", today's behavior). The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- OPS NOTE (locks): ADD COLUMN with no default is metadata-only (no table
-- rewrite), and the CHECK constraint is validated against a table that can
-- only hold NULL in the new column, so both alters are trivial on any size.
-- Additive, so it applies while old pods run: the previous release's
-- statements name their columns explicitly and never read this one.

ALTER TABLE "{schema}".cron_schedules
    ADD COLUMN IF NOT EXISTS disabled_by text;

ALTER TABLE "{schema}".cron_schedules
    ADD CONSTRAINT cron_schedules_disabled_by_check
    CHECK (disabled_by IN ('auto', 'operator'));

COMMENT ON COLUMN "{schema}".cron_schedules.disabled_by IS
    'Who disabled this schedule: ''auto'' = the cron loop''s failure-count '
    'auto-disable (a code re-declaration at worker startup reverts it), '
    '''operator'' = a deliberate operator disable (handle, CLI, admin UI, '
    'actor deregistration; never reverted by a boot). NULL = enabled, or '
    'disabled before ownership was tracked (treated as operator intent).';
