-- Physical workflow worker-capability claim fence. Forward-only; there is no
-- down migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS
-- -----------------
-- PRE: apply before deploying producers that stamp required_worker_schema.
-- Existing rows default to NULL and are deliberately inert. A guarded row
-- cannot be created through the supporting insert statements before this
-- column exists. The claim guard is physical: legacy fleet and direct claim
-- UPDATEs both enter running through this trigger; a CHECK, queue assignment,
-- worker-liveness declaration, or metadata-only marker is not a substitute.
--
-- LOCK SCOPE: the nullable ADD COLUMNs are metadata-only catalog changes (no
-- table rewrite). CREATE OR REPLACE FUNCTION and CREATE TRIGGER are catalog
-- writes. The whole file runs in the migration runner's single transaction.
-- No backfill, index build, validation scan, or destructive statement exists.
--
-- workers.metadata.workflow_execution_schema is a boot-stamped exact JSON
-- integer. JSON numbers with a fractional/exponent spelling, booleans,
-- strings, null, arrays/objects, missing workers, and values outside the
-- signed integer domain all fail closed. WF001 is the stable refusal SQLSTATE.

ALTER TABLE "{schema}".jobs
    ADD COLUMN IF NOT EXISTS required_worker_schema integer NULL;

-- Archive mirroring preserves the complete job record; archived rows never
-- participate in claims and need no trigger.
ALTER TABLE "{schema}".jobs_archive
    ADD COLUMN IF NOT EXISTS required_worker_schema integer NULL;

COMMENT ON COLUMN "{schema}".jobs.required_worker_schema IS
    'Nullable minimum worker metadata.workflow_execution_schema required to claim this workflow node. NULL is legacy/unguarded. Entry into running is physically checked by jobs_wf_worker_capability_guard; the value is inherited unchanged by retry, re-pend, wake, fork, emit, and outbox production.';

CREATE OR REPLACE FUNCTION "{schema}".enforce_wf_worker_capability()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    raw_capability jsonb;
    rendered_capability text;
    actual_capability integer;
BEGIN
    -- The overwhelmingly common legacy/vanilla shape pays one NULL check.
    IF NEW.required_worker_schema IS NULL THEN
        RETURN NEW;
    END IF;

    -- NEW carries the claimant even when an old claim statement knows
    -- nothing about workflow capability. No worker row is not permission.
    SELECT w.metadata -> 'workflow_execution_schema'
      INTO raw_capability
      FROM "{schema}".workers AS w
     WHERE w.id = NEW.locked_by_worker;

    IF raw_capability IS NULL
       OR jsonb_typeof(raw_capability) IS DISTINCT FROM 'number' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'WF001',
            MESSAGE = format(
                'workflow job %s requires worker workflow_execution_schema >= %s, '
                'but claiming worker %s is missing or has a non-integer capability; '
                'upgrade workers serving queue %s to a release implementing that '
                'workflow execution schema before claiming it; body execution was refused',
                NEW.id, NEW.required_worker_schema, NEW.locked_by_worker, NEW.queue
            );
    END IF;

    -- jsonb_typeof accepts every JSON number. Require the canonical integer
    -- spelling before casting so decimal/exponent ambiguity and integer
    -- overflow cannot escape as an incidental driver SQLSTATE.
    rendered_capability := raw_capability #>> '{{}}';
    IF rendered_capability !~ '^(0|[1-9][0-9]*)$'
       OR length(rendered_capability) > 10
       OR (length(rendered_capability) = 10
           AND rendered_capability > '2147483647') THEN
        RAISE EXCEPTION USING
            ERRCODE = 'WF001',
            MESSAGE = format(
                'workflow job %s requires worker workflow_execution_schema >= %s, '
                'but claiming worker %s has a malformed, fractional, or out-of-range '
                'capability; restore the worker boot registration instead of hand-writing '
                'metadata; body execution was refused',
                NEW.id, NEW.required_worker_schema, NEW.locked_by_worker
            );
    END IF;

    actual_capability := rendered_capability::integer;
    IF actual_capability < NEW.required_worker_schema THEN
        RAISE EXCEPTION USING
            ERRCODE = 'WF001',
            MESSAGE = format(
                'workflow job %s requires worker workflow_execution_schema >= %s, '
                'but claiming worker %s stamped %s; upgrade workers serving queue %s '
                'before retrying the claim; body execution was refused',
                NEW.id, NEW.required_worker_schema, NEW.locked_by_worker,
                actual_capability, NEW.queue
            );
    END IF;

    RETURN NEW;
END;
$function$;

-- Column-specific firing keeps heartbeat and ownership/metadata-only writes
-- outside this function entirely. The WHEN clause additionally excludes
-- running-to-running re-stamps, terminal transitions, roots, and vanilla rows.
CREATE TRIGGER jobs_wf_worker_capability_guard
BEFORE UPDATE OF status ON "{schema}".jobs
FOR EACH ROW
WHEN (
    OLD.status IS DISTINCT FROM 'running'
    AND NEW.status = 'running'
    AND NEW.step_key IS NOT NULL
    AND NEW.step_key <> '__flow__'
)
EXECUTE FUNCTION "{schema}".enforce_wf_worker_capability();
