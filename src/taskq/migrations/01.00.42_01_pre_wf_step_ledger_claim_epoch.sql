-- The workflow step ledger's per-claim identity AND the derived emit
-- -capacity object set, folded into ONE migration (PRE phase). Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- THIS FILE IS THE WHOLE POST-FOLD OBJECT SET'S DDL OWNER. It retains the
-- ledger claim_epoch hard executor cutover and installs, in the same one
-- migration transaction:
--
--   * wf_emit_capacity — the bounded run's capacity/intent record: the run's
--     immutable bound and its wake/handled generation pair. There is NO
--     persisted live-row charge, waiter count, cached head, or cached
--     minimum width anywhere in this schema: capacity is derived from real
--     jobs rows at read time. Approaching the bigint generation bound is a
--     named fail-closed refusal, never a wrap or reset.
--   * wf_emit_intent_rotation — the global fairness rotation's singleton
--     (one after_flow_id cursor, never growing with run cardinality).
--   * wf_emit_capacity_pending_idx — the pending-generation partial index
--     the global selection's ordered 100-flow read serves.
--   * the exact-park status guard (function + trigger) — an OLD due sweep's
--     scheduled -> pending transition on an exact parked row is refused
--     unless the winning promotion installs the versioned page-width
--     reservation in the same UPDATE. Terminal cancel/reclaim remains legal
--     and disappears from every derived predicate. The guard only rejects an
--     illegal promotion; it creates no wake work.
--   * jobs_wf_emit_park_idx — the exact-park partial index the per-terminal
--     existence probe, the candidate comparison, and the bounded marker
--     sweep serve. Evidence-backed: without it the hot per-terminal probe
--     rides jobs_wf_flow_nodes_idx and removes every ordinary scheduled row
--     it filters (measured 5,000 removed rows / 206 buffers versus one
--     partial-index search / one buffer on the same population).
--
-- The previously proposed separate 01.00.41_05 and 01.00.41_06 identities
-- are WITHDRAWN and must never be created: these objects' sole DDL owner is
-- THIS file. The pre-fold ledger-only bytes of this file (frozen in a
-- development branch at
-- ae56b483253ca951f346958d0c58266a10e52b92fd4a071ba4b0bf41f2d9ab22) were
-- never published on the binding main; a development database migrated with
-- them is not current — the migration runner's default fail-closed checksum
-- comparison refuses the next apply_pending against this folded file, and
-- such a schema must be rebuilt. The catalog/capability preflight still
-- fails closed before feature use (a whole-object-set test), never relying
-- on migration drift as its guard.
--
-- PHASE OBLIGATIONS
-- -----------------
-- PRE: apply before deploying any binary that claims workflow rows with the
-- epoch-scoped ledger claim, and before any binary that parks or promotes
-- EmitBackpressure sources. 42_01 fences workflow EXECUTORS only: it is not
-- claimed to fence an old maintenance leader, operator-cancel API, or direct
-- producer. Mixed-version behavior is explicit:
--
-- * Before applying, DRAIN THE PRODUCER FIRST (stop new bounded-flow
--   production and emission), then complete the full writer-role drain:
--   stop old maintenance leaders, direct producers/runners, executors, and
--   cancel-serving binaries; wait for every in-flight jobs/wf_step_ledger
--   application transaction to finish; close or prove idle the old writer
--   pools so no successor statement can begin. In particular require the
--   absence of any residual transaction that holds jobs and will next write
--   wf_step_ledger. pg_stat_activity/pg_locks inspection is useful evidence
--   but is only a time-of-check snapshot; it is not the authority for
--   safety.
-- * An OLD executor's workflow ledger claim binds the four-expression ON
--   CONFLICT target; against this schema there is no matching unique
--   constraint, so the claim fails with PostgreSQL 42P10 after the job row
--   is already 'running' — the old body never runs. That observed failure
--   is the DOCUMENTED UNSUPPORTED envelope, never a compatibility behavior.
--   Recovery: a corrected worker re-claims the row under real
--   worker/attempt/epoch ownership after process termination/lease
--   reclaim; the transient running row is never reported as a final
--   success or a truthful body failure.
-- * Producers may resume AFTER apply. DURING apply, vanilla producer WRITES
--   block for the one jobs write window below while producer READS proceed.
--   Executor-only cutover does not erase that migration effect, and no
--   operator text may overstate it as a jobs READ outage.
-- * After THIS file has applied, restart ONLY corrected binaries. An old
--   executor must never rejoin. No bounded run may be activated until old
--   leaders, direct producers, and cancel-serving binaries are drained:
--   before activation the new relations and the exact-park trigger are
--   inert; after activation an old non-executor writer is unsupported,
--   cannot corrupt a derived total, and any delayed wake is recovered by
--   the new leader's exact-marker sweep.
--
-- LOCK SCOPE (one transaction; the file's cross-table order is
-- wf_step_ledger BEFORE jobs)
-- ---------------------------------------------------------------
-- On wf_step_ledger:
--   * ALTER TABLE ... ADD COLUMN claim_epoch takes ACCESS EXCLUSIVE on
--     wf_step_ledger. It is a metadata-only catalog change (NOT NULL with a
--     constant default: no table rewrite, no validation scan) and the lock
--     is retained through COMMIT.
--   * DROP INDEX takes ACCESS EXCLUSIVE on wf_step_ledger (subsumed by the
--     already-held ADD COLUMN lock; no additional wait).
--   * the non-CONCURRENT CREATE UNIQUE INDEX takes SHARE on wf_step_ledger —
--     it blocks writes (not reads) while it scans existing rows to build
--     the replacement arbiter, so a populated ledger's rebuild is a
--     STOP-ACCEPTING-WORK window (the executor drain above owns it).
--   * COMMENT ON COLUMN takes SHARE UPDATE EXCLUSIVE on wf_step_ledger.
-- On jobs:
--   * the non-CONCURRENT jobs_wf_emit_park_idx build takes SHARE on jobs —
--     it scans the populated core table and BLOCKS jobs writes during that
--     build (reads proceed).
--   * the trigger-install statement is LAST and takes SHARE ROW EXCLUSIVE
--     on jobs (PostgreSQL 18 §13.3.1): it blocks jobs writes and is self-
--     exclusive, but READS PROCEED (only ACCESS EXCLUSIVE blocks an
--     ordinary SELECT). Its standalone acquisition could queue behind an
--     in-flight write transaction, with subsequent writers queueing behind
--     the waiting trigger. Because it follows the SHARE park-index build
--     in the same transaction, its grant is IMMEDIATE AGAINST WRITERS:
--     every mode conflicting with SHARE ROW EXCLUSIVE also conflicts with
--     the already-held SHARE. It is NOT immediate against a CONCURRENT
--     READER holding SHARE on jobs (for example LOCK TABLE ... IN SHARE
--     MODE): this migration's own SHARE is compatible with that reader,
--     while the trigger's SHARE ROW EXCLUSIVE conflicts with it, so the
--     trigger install queues behind a long-lived SHARE reader (measured on
--     PostgreSQL 18.6) and writers queue behind the waiting trigger. The
--     jobs-side apply outage is therefore ONE write-block window from
--     SHARE acquisition through COMMIT (including its acquisition queue
--     wait, which a concurrent SHARE reader can extend), with reads
--     proceeding throughout; there is NO second read-blocking trigger
--     window.
--
-- THE DEADLOCK RULE (both outcomes, victim-agnostic)
-- ---------------------------------------------------------------
-- A residual undrained writer using the OPPOSITE table order (holding or
-- acquiring jobs before wf_step_ledger) can deadlock against this file's
-- wf_step_ledger -> jobs order. PostgreSQL's choice of victim is
-- NONDETERMINISTIC: the waiting backend whose deadlock_timeout discovery
-- fires first can determine the victim. PostgreSQL may abort EITHER the
-- migration OR the residual writer with SQLSTATE 40P01 (deadlock detected):
--
--   * If the MIGRATION is the victim: its one-file transaction rolls back
--     fail closed, the pre-fold catalog shape remains intact, no
--     schema_migrations record for 42_01 is written, and the whole
--     post-fold object set is absent. Re-apply only after repeating and
--     completing the drain; never reorder locks or blindly retry.
--   * If the RESIDUAL WRITER is the victim: its application transaction
--     rolls back and the surviving migration normally completes and commits
--     the whole post-fold object set; there is no migration work to
--     re-apply. The asyncpg application sees
--     asyncpg.exceptions.DeadlockDetectedError / SQLSTATE 40P01, must roll
--     back and retry the entire aborted application transaction under its
--     own retry/idempotency policy, and receives no automatic retry from
--     the migration runner. This application-visible failure is why the
--     full-role drain is mandatory.
--
-- Neither outcome leaves a partial migration object set or partial work in
-- the aborted application transaction. An operator lock_timeout bound
-- (ddl_lock_timeout, default 30 s) is specified; it reduces the deadlock
-- opportunity but cannot eliminate it if a residual writer escapes the
-- drain.

ALTER TABLE "{schema}".wf_step_ledger
    ADD COLUMN IF NOT EXISTS claim_epoch bigint NOT NULL DEFAULT 0;

DROP INDEX IF EXISTS "{schema}".wf_step_ledger_claim_uniq;

CREATE UNIQUE INDEX IF NOT EXISTS wf_step_ledger_claim_uniq
    ON "{schema}".wf_step_ledger (flow_id, step_key, COALESCE(map_index, -1), attempt, claim_epoch);

COMMENT ON COLUMN "{schema}".wf_step_ledger.claim_epoch IS
    'The dispatch claim epoch of the execution this ledger row records. Joins the claim arbiter so a refunded-and-reminted physical attempt (the capacity park cycle) records one ledger row per claim: the parked attempt''s awaited record survives beside the resumed attempt''s terminal. Applying 01.00.42_01 requires the workflow-executor cutover (see 01.00.42_01): an old binary''s four-expression claim/finalize is unsupported against this arbiter, and after it has applied, restart only corrected binaries.';

CREATE TABLE IF NOT EXISTS "{schema}".wf_emit_capacity (
    flow_id uuid PRIMARY KEY,
    bound integer NOT NULL,
    wake_generation bigint NOT NULL DEFAULT 0,
    handled_generation bigint NOT NULL DEFAULT 0,
    CONSTRAINT wf_emit_capacity_bound_positive CHECK (bound > 0),
    CONSTRAINT wf_emit_capacity_generations_monotonic
        CHECK (wake_generation >= handled_generation)
);

COMMENT ON TABLE "{schema}".wf_emit_capacity IS
    'The bounded run''s immutable bound and its wake/handled promotion-intent generations. The table name denotes the run''s bound and wake intent; it stores NO capacity total: charge, waiter cardinality, candidate identity, and min_waiting_width are derived from real jobs rows at read time. At most one row per bounded run; a row contains no executable actor work, payload, cursor, child identity, timer, or per-event history.';

CREATE TABLE IF NOT EXISTS "{schema}".wf_emit_intent_rotation (
    id boolean PRIMARY KEY DEFAULT true,
    after_flow_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'::uuid,
    CONSTRAINT wf_emit_intent_rotation_singleton CHECK (id)
);

INSERT INTO "{schema}".wf_emit_intent_rotation (id) VALUES (true)
    ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE "{schema}".wf_emit_intent_rotation IS
    'The global fairness rotation''s single after_flow_id cursor (the pending-intent selection''s total order). One singleton row; no estate snapshot, flow list, or executable work; never grows with run cardinality.';

CREATE INDEX IF NOT EXISTS wf_emit_capacity_pending_idx
    ON "{schema}".wf_emit_capacity (flow_id)
    WHERE wake_generation > handled_generation;

COMMENT ON INDEX "{schema}".wf_emit_capacity_pending_idx IS
    'The global pending-intent selection''s ordered 100-flow read: at most 100 distinct flow ids in one short indexed read under the persistent rotating after_flow_id cursor, holding no row lock past the read.';

-- THE §5.4 NON-DRAINAGE REFUSAL FUNCTION (the root-maintenance failure
-- arm's server-side seat): on the committed terminal-root proof (the root
-- terminal 'failed' behind a genuine child failure that exhausted its
-- declared retry policy, while an exact parked source still holds an
-- un-emitted committed cursor), the same transaction fences every such
-- parked source with WORKFLOW_NOT_DRAINING_ERROR_CLASS, the named reason,
-- and the causal child/root ids. No logical attempt is spent, no ladder
-- retry is created, and the causal child's genuine error is never
-- overwritten. The cascade seat (the engine's fail-closed path) calls the
-- statement form directly with the child it knows; this function derives
-- the causal child from the rows (the most recently failed exhausted
-- child).
CREATE OR REPLACE FUNCTION "{schema}".fence_not_draining_parked(p_root uuid)
RETURNS integer
LANGUAGE plpgsql
AS $fence$
DECLARE
    v_child uuid;
    v_fenced integer;
BEGIN
    SELECT l.job_id INTO v_child
    FROM "{schema}".wf_step_ledger l
    WHERE l.flow_id = p_root
      AND l.status = 'failed'
      AND l.job_id IS NOT NULL
    ORDER BY l.updated_at DESC, l.id DESC
    LIMIT 1;

    WITH parks AS (
        SELECT j.id
        FROM "{schema}".jobs AS j
        WHERE (j.metadata->>'flow_id')::uuid = p_root
          AND j.status = 'scheduled'
          AND j.metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
          AND j.metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}}}'::jsonb
          AND j.metadata @> '{{"wf_emit_backpressure":{{"version":1}}}}'::jsonb
          AND j.step_key IS NOT NULL
          AND j.step_key <> '__flow__'
          AND j.metadata ? 'flow_id'
          AND j.metadata ? 'emit_cursor'
    ), fenced AS (
        UPDATE "{schema}".jobs AS j
        SET status = 'failed',
            finished_at = clock_timestamp(),
            error_class = 'WorkflowNotDrainingError',
            error_message = 'the run is provably not draining: the root terminal ''failed'' '
                            'committed after the causal child failure exhausted its retry '
                            'policy, while this source still parks on an un-emitted '
                            'committed cursor (reason: '
                            'terminal_root_after_child_failure_exhaustion)',
            metadata = (j.metadata - 'blocking_reason' - 'wf_emit_reservation')
                || jsonb_build_object(
                    'wf_not_draining', jsonb_build_object(
                        'reason', 'terminal_root_after_child_failure_exhaustion',
                        'root_id', p_root,
                        'child_id', v_child,
                        'at', to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                      'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                    )
                )
        FROM parks
        WHERE j.id = parks.id
        RETURNING j.id
    ), ledger AS (
        UPDATE "{schema}".wf_step_ledger AS l
        SET status = 'fenced',
            error_class = 'WorkflowNotDrainingError',
            updated_at = clock_timestamp()
        FROM fenced
        WHERE l.job_id = fenced.id
          AND l.status = 'awaited'
        RETURNING l.id
    ), evt AS (
        INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail)
        SELECT fenced.id, clock_timestamp(), 'state_change',
               jsonb_build_object('from_state', 'scheduled',
                                  'to_state', 'failed',
                                  'error_class', 'WorkflowNotDrainingError',
                                  'reason',
                                  'terminal_root_after_child_failure_exhaustion')
        FROM fenced
    )
    SELECT count(*) INTO v_fenced FROM fenced;
    RETURN v_fenced;
END;
$fence$;

CREATE INDEX IF NOT EXISTS jobs_wf_emit_park_idx
    ON "{schema}".jobs (((metadata->>'flow_id')::uuid), created_at, id)
    WHERE status = 'scheduled'
      AND metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
      AND metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}}}'::jsonb
      AND metadata @> '{{"wf_emit_backpressure":{{"version":1}}}}'::jsonb
      AND step_key IS NOT NULL
      AND step_key <> '__flow__'
      AND metadata ? 'flow_id';

COMMENT ON INDEX "{schema}".jobs_wf_emit_park_idx IS
    'The exact-park partial index (flow uuid, created_at, id) the per-terminal existence probe, the oldest-fitting candidate comparison, and the bounded marker sweep serve. The marker, never a timestamp, discriminates: scheduled_at is deliberately absent from park identity.';

CREATE OR REPLACE FUNCTION "{schema}".wf_emit_park_guard()
RETURNS pg_catalog.trigger
LANGUAGE plpgsql
AS $fn$
BEGIN
    -- The exact-park status guard (the representation's own invariant): a
    -- scheduled row carrying the live EmitBackpressure marker may move to
    -- pending ONLY through the winning reserved promotion — the same UPDATE
    -- that installs the valid two-key versioned reservation removes the
    -- marker. An old due sweep's bare scheduled -> pending is refused
    -- fail-closed (SQLSTATE WF002): a parked source is never claimable
    -- work, and the promotion CAS is the sole legal mover.
    IF OLD.metadata @> '{{"blocking_reason":"backpressure"}}'::jsonb
       AND OLD.metadata @> '{{"wf_deferral":{{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}}}'::jsonb
       AND OLD.metadata @> '{{"wf_emit_backpressure":{{"version":1}}}}'::jsonb
       AND OLD.step_key IS NOT NULL
       AND OLD.step_key <> '__flow__'
       AND OLD.metadata ? 'flow_id' THEN
        IF NEW.metadata ? 'wf_emit_reservation' IS NOT TRUE
           OR jsonb_typeof(NEW.metadata->'wf_emit_reservation') <> 'object'
           OR (NEW.metadata->'wf_emit_reservation') - '{{version,width}}'::text[] <> '{{}}'::jsonb
           OR jsonb_typeof(NEW.metadata->'wf_emit_reservation'->'version') <> 'number'
           OR (NEW.metadata->'wf_emit_reservation'->>'version') <> '1'
           OR jsonb_typeof(NEW.metadata->'wf_emit_reservation'->'width') <> 'number'
           OR (NEW.metadata->'wf_emit_reservation'->>'width') !~ '^[0-9]+$'
        THEN
            RAISE EXCEPTION
                'an exact parked source cannot move scheduled -> pending without a winning reserved promotion (old job %, marker version %)',
                OLD.id,
                OLD.metadata->'wf_emit_backpressure'->>'version'
                USING ERRCODE = 'WF002',
                      HINT = 'the emit-capacity promotion CAS is the sole legal mover; the scheduled->pending sweep must not touch exact parks';
        END IF;
    END IF;
    RETURN NEW;
END;
$fn$;

-- The trigger install is LAST (the jobs-side single write window's tail;
-- reads proceed throughout).
DROP TRIGGER IF EXISTS wf_emit_park_guard ON "{schema}".jobs;

CREATE TRIGGER wf_emit_park_guard
    BEFORE UPDATE OF status
    ON "{schema}".jobs
    FOR EACH ROW
    WHEN (OLD.status = 'scheduled' AND NEW.status = 'pending')
    EXECUTE FUNCTION "{schema}".wf_emit_park_guard();
