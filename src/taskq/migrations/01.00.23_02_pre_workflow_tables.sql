-- The workflow ledger tables — T03 (the engine's exactly-once fire ledger,
-- the edge ledger, the delivery outbox, the step ledger). Forward-only;
-- there is no down migration. To revert, restore from backup. The literal
-- "{schema}" token is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENTS (each takes its own
-- relation lock on a NEW table — no ALTERs, no index builds mixed in).
-- See 01.00.23_01's header for the round's three-file split and the
-- deployment sequence. All DDL is additive (no post_ phase, the v1 forever
-- rule); the tables are new, so the lock class is CREATE TABLE's own.
--
-- ID GENERATION (the estate invariant, T03's one-line rule): every table's
-- `id uuid PRIMARY KEY` is generated APP-SIDE through the seam
-- `taskq._ids.new_uuid()` (uuid7, time-ordered) — NEVER DB-side
-- `gen_random_uuid()`, NEVER uuid4 (the TID251 discipline; a pin greps this
-- package's DDL). Time-ordered ids land at the right-hand edge of the PK
-- B-tree (page-stable appends) and `ORDER BY id` is a usable creation
-- order (the sweep's recount, the drain, the admin's keyset pagination).
--
-- FK-LESS: no foreign keys anywhere in this round, per the estate's no-FK
-- decision — the graph is enforced by the engine, never by the schema.

-- ── wf_edge: THE EDGE LEDGER — the join counter's truth ─────────────────
-- One row per (child, parent) dependency edge. The engine's
-- COUNTER-AS-CACHE / LEDGER-AS-TRUTH rule: `jobs.deps_pending` is a cache;
-- the ledger is the only count source (`remaining = join_target − committed
-- decrements` — NEVER child-row presence: nested joins' children do not
-- exist yet at count time, which is exactly where child-row counting fires
-- an empty join early). The sweep re-derives the cache from this ledger.
CREATE TABLE "{schema}".wf_edge (
    child_id  uuid NOT NULL,
    parent_id uuid NOT NULL,
    flow_id   uuid NOT NULL,
    PRIMARY KEY (child_id, parent_id)
);

-- ── wf_join_fire: the exactly-once FIRE ledger ───────────────────────────
-- At most one fire row per joined node, ever: the UNIQUE constraint on
-- join_job_id is what rejects a double fire (the PK on id keeps the estate's
-- uuid7 invariant; the fire guard's correctness rides the UNIQUE). The
-- proto's `child_id PRIMARY KEY` shape maps to it at port time.
CREATE TABLE "{schema}".wf_join_fire (
    id          uuid PRIMARY KEY,
    join_job_id uuid NOT NULL,
    flow_id     uuid NOT NULL,
    step_key    text NOT NULL,
    fired_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    fired_by    text NOT NULL DEFAULT 'finalize',
    UNIQUE (join_job_id)
);

-- ── wf_outbox: the fired join's delivery queue ───────────────────────────
-- A fired join's consumer bindings ride here; the drain (a sweep arm)
-- inserts the consumer rows IDEMPOTENTLY (ON CONFLICT on the consumer step
-- key, via the jobs composite idempotency arbiter) and flips the undelivered
-- flag in the insert's transaction — the delivery half of the exactly-once
-- fire. Shape follows the proven proto outbox + the job_events outbox
-- precedent (01.00.02_01).
CREATE TABLE "{schema}".wf_outbox (
    id                uuid PRIMARY KEY,
    join_job_id       uuid NOT NULL,
    flow_id           uuid NOT NULL,
    consumer_step_key text NOT NULL,
    map_index         smallint NULL,
    bindings          jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    delivered         boolean NOT NULL DEFAULT false,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- ── wf_step_ledger: the step ledger (the exactly-once attempt record) ────
-- The ledger `attempt` increments at claim, the only grant of work; the
-- claim arbiter (the CREATE UNIQUE INDEX in 01.00.23_03,
-- wf_step_ledger_claim_uniq) physically blocks double-recording (P3 rule 2).
-- THE ARBITER KEYS ON COALESCE(map_index, -1): map children of one step key
-- are DIFFERENT claims per T05's key contract — a bare
-- UNIQUE (flow_id, step_key, attempt) collapses them onto ONE row (the
-- terminal write overwrites, the memoized replay returns the wrong child's
-- result — the ATTACK-FIXED shape; this round is NOT landed anywhere, so the
-- constraint was amended in place, the unique index lives in _03 beside the
-- round's other index builds). The ledger's terminal-outcome write rides
-- the finalize's own transaction (the ledger-terminal-atomic rule). The
-- failure IO-capture (the `capture` jsonb) is written at failure-finalize
-- per the workflow's none|errors-only|all policy, AFTER the redact chain
-- (chain → hook, unconditional) — the capture row must never carry a
-- canary.
CREATE TABLE "{schema}".wf_step_ledger (
    id            uuid PRIMARY KEY,
    flow_id       uuid NOT NULL,
    job_id        uuid NOT NULL,
    step_key      text NOT NULL,
    map_index     smallint NULL,
    attempt       integer NOT NULL,
    status        text NOT NULL,
    result        jsonb NULL,
    error_class   text NULL,
    error_message text NULL,
    capture       jsonb NULL,
    created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE "{schema}".wf_edge IS
    'The workflow edge ledger: one row per dependency edge. The join counter''s ONLY truth source (deps_pending is a cache the sweep reconciles from here).';
COMMENT ON TABLE "{schema}".wf_join_fire IS
    'The workflow exactly-once fire ledger: at most one fire per joined node, ever (UNIQUE(join_job_id) rejects the double fire; the rowcount gate prevents premature ones).';
COMMENT ON TABLE "{schema}".wf_outbox IS
    'The workflow delivery outbox: a fired join''s consumer bindings; the drain inserts consumer rows idempotently and flips the flag in the insert''s transaction.';
COMMENT ON TABLE "{schema}".wf_step_ledger IS
    'The workflow step ledger: one row per (flow, step, map_index, attempt). The claim arbiter UNIQUE(flow_id, step_key, COALESCE(map_index,-1), attempt) physically blocks double-recording; terminal writes ride the finalize''s own transaction.';
