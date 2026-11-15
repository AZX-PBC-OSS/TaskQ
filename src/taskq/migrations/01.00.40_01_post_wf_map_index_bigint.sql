-- Workflow record identity: smallint -> bigint on all five carriers.
-- Forward-only; there is no down migration. The literal "{schema}" token is
-- substituted at apply time. To revert, restore from backup or fix forward.
--
-- PHASE: POST. Apply only after every process capable of emitting, finalizing,
-- directly driving, or leading workflow maintenance runs the release with the
-- bigint SQL binds and schema-derived identity gate. Once high identities
-- exist, an OLD binary's explicit smallint binds cannot finalize, memoize,
-- count, fence, collect, or drain them. In particular an old elected leader's
-- smallint[] outbox bind rejects the whole drain batch. The workers heartbeat
-- table is not a release oracle; this is an explicit operator removal gate.
-- The mixed old/new binary wave is supported while this POST remains pending:
-- the new gate still refuses identities above 32767 on the old schema. There
-- is deliberately no PRE migration for the widening.
--
-- The five ALTERs share the runner's one transaction with the migration-ledger
-- record, so no committed partially-widened state is supported or exposed.
-- Existing smallint values and NULLs need no backfill. Nullable stays nullable.
-- PostgreSQL 18 preserves the OLD int4 coercion in each expression index's
-- stored definition (COALESCE(map_index::integer,-1)) across ALTER TYPE. That
-- rebuilt index does not match the new int8 ON CONFLICT expression. The two
-- arbiters are therefore dropped before the ALTERs (avoiding a useless legacy
-- rebuild) and recreated after them under the already-held ACCESS EXCLUSIVE
-- locks. They remain in this same atomic file: same names, keys, uniqueness,
-- and partial-hold predicate, now with bigint expressions. No unrelated index
-- DDL or second transaction is introduced; ON CONFLICT inference is executed
-- by the real-PG proof rather than assumed.
--
-- LOCK SCOPE: each ALTER TYPE takes ACCESS EXCLUSIVE and rewrites its table,
-- rebuilding every index on that table (including unrelated indexes). This
-- transaction retains every acquired lock until COMMIT, so the five tables
-- block reads and writes during their rewrite and all subsequent statements
-- in this file. The runner's ddl_lock_timeout bounds lock ACQUISITION only;
-- statement_timeout is widened for the run and does not bound the outage once
-- a rewrite owns its lock. Plan an operator-approved maintenance window sized
-- from jobs, jobs_archive and wf_step_ledger. See the P3 evidence artifact for
-- the measured local row counts, elapsed time and storage; those measurements
-- are not a production duration guarantee.
--
-- map_index remains an exact, non-negative integer record ordinal. This file
-- does not widen jobs.deps_pending (smallint), change MAX_FAN_IN_PER_JOIN, make
-- one giant map legal, introduce opaque identities, or provide a rollback.

DROP INDEX "{schema}".wf_step_ledger_claim_uniq;
DROP INDEX "{schema}".wf_signals_hold_identity_uniq;

ALTER TABLE "{schema}".jobs
    ALTER COLUMN map_index TYPE bigint;

ALTER TABLE "{schema}".jobs_archive
    ALTER COLUMN map_index TYPE bigint;

ALTER TABLE "{schema}".wf_step_ledger
    ALTER COLUMN map_index TYPE bigint;

ALTER TABLE "{schema}".wf_signals
    ALTER COLUMN map_index TYPE bigint;

ALTER TABLE "{schema}".wf_outbox
    ALTER COLUMN map_index TYPE bigint;

CREATE UNIQUE INDEX wf_step_ledger_claim_uniq
    ON "{schema}".wf_step_ledger
    (flow_id, step_key, COALESCE(map_index, -1), attempt);

CREATE UNIQUE INDEX wf_signals_hold_identity_uniq
    ON "{schema}".wf_signals
    (workflow_id, node_key, signal_name, COALESCE(map_index, -1), hold_epoch)
    WHERE resolved_at IS NULL;
