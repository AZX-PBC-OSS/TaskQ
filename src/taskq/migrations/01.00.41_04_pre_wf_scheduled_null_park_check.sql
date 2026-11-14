-- The scheduled-without-timer storage domain (PRE). Forward-only; there is no
-- down migration. The literal "{schema}" token is substituted at apply time.
--
-- PRE: apply in the same release train as 01.00.41_03, before any binary that
-- parks EmitBackpressure sources. 01.00.41_03 dropped jobs.scheduled_at's NOT
-- NULL so the capacity park can rest a source at status='scheduled' with no
-- timer. That opens one ambiguous storage state — a scheduled NULL row that is
-- NOT the exact park representation — which the dispatch claim's CAS can never
-- claim (NULL fails scheduled_at <= now()) and the direct claimable listing
-- used to admit: the busy-spin-on-an-unclaimable-row shape. This constraint
-- closes the domain at the storage layer: a scheduled row may have a NULL due
-- time ONLY as the exact live park representation (the marker + typed signal
-- + version-1 record + a real source node of a real run), so no other writer
-- and no corrupted row can mint an unclaimable scheduled NULL.
--
-- LOCK SCOPE: ADD CONSTRAINT ... NOT VALID is a catalog-only change (no table
-- scan; brief ACCESS EXCLUSIVE). VALIDATE CONSTRAINT then takes SHARE UPDATE
-- EXCLUSIVE — it scans existing rows for the constraint's truth but blocks
-- neither reads nor writes while doing so. Both statements run in the
-- migration runner's single transaction. Pre-41_03 rows cannot violate it
-- (scheduled_at was NOT NULL before 41_03), and the only NULL writer is the
-- park itself, so the validation scan is a formality on every legitimate
-- deployment.
ALTER TABLE "{schema}".jobs
    ADD CONSTRAINT jobs_scheduled_null_park_chk
    CHECK (
        status <> 'scheduled'
        OR scheduled_at IS NOT NULL
        OR (
            metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
            AND metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure"}}}}'::jsonb
            AND metadata @> '{{"wf_emit_backpressure":{{"version":1}}}}'::jsonb
            AND step_key IS NOT NULL
            AND step_key <> '__flow__'
            AND metadata ? 'flow_id'
        )
    ) NOT VALID;

ALTER TABLE "{schema}".jobs
    VALIDATE CONSTRAINT jobs_scheduled_null_park_chk;
