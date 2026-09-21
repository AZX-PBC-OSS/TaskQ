-- Admin audit trail: one row per operator mutation performed through the
-- admin UI (job cancel, job retry, schedule enable/disable/skip/run-now,
-- actor deregister). Before this table the admin UI's write endpoints
-- recorded NO principal anywhere: the auth dependency's IdentityClaims
-- return was discarded at the router boundary, and the only durable trace
-- of "who cancelled this job" was whatever the operator typed into the
-- reason box. This table is that trail.
--
-- Forward-only; there is no down migration. To revert, restore from backup.
-- The literal "{schema}" token is substituted at apply time by the
-- migration runner.
--
-- ROLLING DEPLOY: purely additive. No existing statement references this
-- table, old pods neither read nor write it, and the new code tolerates
-- its absence (the audit write degrades to a logged warning on
-- backend-mediated mutations) while the migration lands.
--
-- Why there is NO foreign key to jobs: the audit's targets are exactly the
-- rows routine maintenance prunes, archives, and deregisters by design. An
-- FK (or a cascade) would let that maintenance erase the record of who did
-- what -- the one thing the table exists to prevent. (target_type,
-- target_id) is a deliberately loose reference; it stays readable after the
-- target is gone, which is when an audit question ("who cancelled the job
-- that vanished?") is asked.
--
-- RETENTION: no sweep touches this table. Entries accumulate for the life
-- of the schema; an operator who needs a bound should archive and truncate
-- deliberately (docs/guides/admin-ui.md, "Audit trail"), because a silent
-- retention window is a hole in the trail wearing a policy's clothes.

CREATE TABLE "{schema}".admin_audit (
    id                bigserial PRIMARY KEY,
    occurred_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    principal_subject text NOT NULL,
    action            text NOT NULL,
    target_type       text NOT NULL,
    target_id         text NOT NULL,
    reason            text,
    detail            jsonb NOT NULL DEFAULT '{{}}'::jsonb
);

-- The job detail page renders the per-target trail, and an operator's
-- "what happened today" question scans time. Both orders are small,
-- additive, and index-only.
CREATE INDEX admin_audit_target_idx
    ON "{schema}".admin_audit (target_type, target_id, occurred_at);

CREATE INDEX admin_audit_occurred_idx
    ON "{schema}".admin_audit (occurred_at);

COMMENT ON TABLE "{schema}".admin_audit IS
    'Audit trail of admin-UI operator mutations. One row per mutation: '
    'who (principal_subject), what (action), on what (target_type, '
    'target_id), why (reason), and any per-action extras (detail). No FK '
    'to jobs: targets are pruned/archived by design and the trail must '
    'outlive them.';

COMMENT ON COLUMN "{schema}".admin_audit.principal_subject IS
    'The authenticated principal subject that performed the action, from '
    'the auth dependency IdentityClaims. The literal ''anonymous'' when '
    'the router runs without an auth dependency (dev deployments only).';

COMMENT ON COLUMN "{schema}".admin_audit.action IS
    'One of: job.cancel | job.retry | schedule.enable | schedule.disable | '
    'schedule.skip | schedule.run | actor.deregister';
