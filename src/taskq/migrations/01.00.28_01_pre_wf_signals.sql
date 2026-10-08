-- The wf_signals table — T10 (HITL as rows: held nodes, typed signals,
-- the abandon policies). Forward-only; there is no down migration. To
-- revert, restore from backup. The literal "{schema}" token is
-- substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENT (its own relation
-- lock on a NEW table — no ALTERs, no index builds mixed in). ALL
-- WORKFLOW DDL IS ADDITIVE — no `post_` phase ever ships for v1.
--
-- ID GENERATION (the estate invariant): the `id uuid PRIMARY KEY` is
-- generated APP-SIDE through the seam `taskq._ids.new_uuid()` (uuid7,
-- time-ordered) — NEVER DB-side `gen_random_uuid()`, NEVER uuid4 (the
-- TID251 discipline). THE ID IS THE REPLY HANDLE: every hold's id is
-- THE reference the client/admin/CLI/pubsub use to resolve it (a
-- surface that cannot name the hold id reds the pin).
--
-- THE HOLD IDENTITY, ONE SENTENCE (the reconciliation every mention
-- agrees with): `call_id` is the per-attempt wait-call token; a new
-- hold registration mints a NEW epoch, so
-- `(workflow_id, node_key, signal_name, hold_epoch)` is the FULL
-- UNIQUE IDENTITY and `call_id` is its projection on the routing path
-- (the resume lookup + the deliver routing match on call_id; the
-- uniqueness never needs call_id — a new call = a new epoch).
--
-- THE STALE-PAYLOAD DRAGON (named, killed): pre-epoch, the wait site
-- found the PREVIOUS hold's delivered payload and ANSWERED A CALL IT
-- NEVER MADE; the epoch + call_id kill it (the pin).
--
-- MULTI-HOLD (A-CRITICAL-3, cut #3b; the agent-loop spike's verdict M
-- is the second confirmation): a step may hold on the SAME signal name
-- more than once across its attempt history — the unique constraint is
-- on the hold EPOCH, not the name alone (the spike's `(flow, name)` PK
-- raised UniqueViolation on the second hold — and because the register
-- died inside the exception handler, the iteration DEADLOCKED:
-- running↔reclaimed forever, the run never completed — worse than a
-- clean failure). The PARTIAL unique index (unresolved holds only)
-- carries the identity; the epoch's counter column rides the row.
--
-- THE PAYLOAD RIDES THE ROW (cut #3's cure): the spike's `deliver()`
-- accepted the human's answer and DROPPED it — the author hand-rolled
-- a `signal_payloads` side table. Never again: `payload` IS the
-- column, `payload_schema` the declared model's schema (what a UI
-- renders from), and `ctx.signal(name)` reads it on resume.
--
-- FK-LESS: no foreign keys anywhere, per the estate's no-FK decision
-- — the graph is enforced by the engine, never by the schema.

CREATE TABLE "{schema}".wf_signals (
    id             uuid PRIMARY KEY,
    workflow_id    uuid NOT NULL,
    node_key       text NOT NULL,
    signal_name    text NOT NULL,
    hold_epoch     integer NOT NULL,
    call_id        text NOT NULL,
    payload        jsonb NULL,
    payload_schema jsonb NULL,
    status         text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at     timestamptz NULL,
    resolved_at    timestamptz NULL
);

CREATE UNIQUE INDEX wf_signals_hold_identity_uniq
    ON "{schema}".wf_signals (workflow_id, node_key, signal_name, hold_epoch)
    WHERE resolved_at IS NULL;

-- The reply handle's lookup (the client/admin/CLI resolve by id).
CREATE INDEX wf_signals_id_status_idx
    ON "{schema}".wf_signals (id, status);

-- The enumeration's read (the client's list-by-run: the pending holds
-- for a run, newest first).
CREATE INDEX wf_signals_run_status_idx
    ON "{schema}".wf_signals (workflow_id, status, id);

COMMENT ON TABLE "{schema}".wf_signals IS
    'The HITL hold rows (T10): the row IS the truth (the notification is a knock). The hold identity is (workflow_id, node_key, signal_name, hold_epoch) — partial-unique over UNRESOLVED holds (multi-hold: a second hold mints a NEW epoch); the id (uuid7) is THE reply handle; the payload rides the row (deliver-no-drop); the epoch + call_id kill the stale-payload dragon (the wait site answering a call it never made).';
