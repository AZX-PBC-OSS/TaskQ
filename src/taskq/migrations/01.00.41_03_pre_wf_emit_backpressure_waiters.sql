-- Slot-free workflow emit backpressure waiter lookup. Forward-only; there is
-- no down migration. The literal "{schema}" token is substituted at apply time.
--
-- PRE: apply before deploying producers that raise/park EmitBackpressure.
-- Snapshot semantics still work without this index, but deploying code first
-- would turn bounded recovery into an unindexed jobs scan. This PRE ordering is
-- therefore mandatory rather than an optional performance hint.
--
-- LOCK SCOPE: this file runs in one transaction. DROP NOT NULL is a catalog-only
-- ACCESS EXCLUSIVE change (no table rewrite/validation scan); ordinary CREATE
-- INDEX then takes SHARE (blocks writes, not reads) while it scans existing rows.
-- No CONCURRENTLY/non-transactional partial application is introduced. There is
-- no trigger, deferred wake flag, status enum value, table, or due timestamp.
ALTER TABLE "{schema}".jobs
    ALTER COLUMN scheduled_at DROP NOT NULL;
--
-- The exact live representation is status=scheduled + scheduled_at IS NULL +
-- blocking_reason=backpressure + typed EmitBackpressure and version-1 record.
-- (validated flow_id expression, PK id) implements the fitted wake probe and
-- the estate-wide ordered recovery cursor. Wake removes blocking_reason in the
-- same UPDATE that promotes pending, so rows leave this index self-consumingly.
CREATE INDEX IF NOT EXISTS jobs_wf_emit_backpressure_waiter_idx
    ON "{schema}".jobs (((metadata->>'flow_id')::uuid), id)
    WHERE status = 'scheduled'
      AND scheduled_at IS NULL
      AND metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
      AND metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure"}}}}'::jsonb
      AND metadata @> '{{"wf_emit_backpressure":{{"version":1}}}}'::jsonb
      AND step_key IS NOT NULL
      AND step_key <> '__flow__'
      AND metadata ? 'flow_id';
