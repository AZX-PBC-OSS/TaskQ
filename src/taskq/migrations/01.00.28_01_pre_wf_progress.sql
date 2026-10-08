-- The workflow progress tables — T21 (the two-channel persistence: the
-- STATE channel + the STREAM channel). Forward-only; there is no down
-- migration. To revert, restore from backup. The literal "{schema}" token
-- is substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENTS AND THE NEW TABLES'
-- OWN INDEXES (each takes its own relation lock on a NEW table — no
-- ALTERs of existing tables, no backfills). ALL WORKFLOW DDL IS
-- ADDITIVE — no `post_` phase ever ships for v1.
--
-- THE TWO CHANNELS (T21 decision d — the PoC's PROVEN shape, ported):
--
-- * wf_node_progress — THE STATE CHANNEL. ONE row per (node_id,
--   channel), UPSERTED latest-wins + an honest occurrence counter. The
--   row count is nodes x channels, CONSTANT whatever the emission rate
--   (DH1's fence — the 6.5M-row incident's every-event-a-row shape is
--   structurally impossible here: there is no insert path, only the
--   upsert). `pct` is smallint — the op validates 0..100 and the column
--   is the storage-domain twin of that bound. `dropped` accumulates the
--   STREAM channel's drop-oldest casualties for this node (the honest
--   emitted-vs-delivered pair — DH2's fence, the counter ON THE RECORD).
--
-- * wf_node_stream — THE STREAM CHANNEL. APPEND rows in a bounded
--   per-node ring (the append+trim is ONE statement, the drop count
--   RETURNED; the retention sweep's prune arm is the backstop). The seq
--   is THE ONE SEQ SPACE (DH3's fence: ONE stream, ONE class
--   discriminator, ONE cursor serves both the user class and the auto
--   class — a second seq-cursor space would re-create the fleet's
--   SSE-vocabulary fragmentation).
--
-- THE SEQ IS DB-SIDE, AND THAT IS THE POINT: `bigserial` (the
-- 01.00.20_03 progress_seq precedent — the cursor is the replay's total
-- order). This does NOT relitigate the estate's id invariant: `seq` is
-- not an identity, it is a POSITION — app-side uuid7 cannot be a dense
-- gapless cursor, and the seq-cursor replay law (T11) is defined on
-- exactly this shape. No other column is DB-generated.
--
-- THE CLOSED VOCABULARY IS STORAGE-DOMAIN TOO (DH3's teeth): `class`
-- carries exactly the two discriminators and `kind` exactly the closed
-- kind set — CHECK constraints, so a third vocabulary cannot sprout by
-- accident (the Python emit path validates against the same frozenset
-- first; the constraint is the storage domain's own refusal).
--
-- FK-LESS: no foreign keys anywhere, per the estate's no-FK decision
-- — the graph is enforced by the engine, never by the schema. A node
-- row deleted by retention leaves its progress rows (bounded by
-- nodes x channels) and its ring tail (bounded by the ring bound).

CREATE TABLE "{schema}".wf_node_progress (
    node_id     uuid NOT NULL,
    channel     text NOT NULL,
    pct         smallint NULL,
    message     text NULL,
    data        jsonb NULL,
    occurrences bigint NOT NULL DEFAULT 0,
    dropped     bigint NOT NULL DEFAULT 0,
    last_seq    bigint NULL,
    updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id, channel)
);

CREATE TABLE "{schema}".wf_node_stream (
    seq        bigserial PRIMARY KEY,
    node_id    uuid NOT NULL,
    flow_id    uuid NOT NULL,
    class      text NOT NULL,
    kind       text NOT NULL,
    payload    jsonb NULL,
    emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT wf_node_stream_class_closed CHECK (class IN ('user', 'auto')),
    CONSTRAINT wf_node_stream_kind_closed
        CHECK (kind IN ('progress', 'wf.node.started', 'wf.node.terminal'))
);

-- The node-scoped replay read (the SSE face's node tail) and the
-- append-trim's per-node rank walk: newest-first within the node.
CREATE INDEX wf_node_stream_node_seq_idx
    ON "{schema}".wf_node_stream (node_id, seq DESC);

-- The run-scoped replay read (the SSE face's run tail): one cursor
-- serves both classes.
CREATE INDEX wf_node_stream_flow_seq_idx
    ON "{schema}".wf_node_stream (flow_id, seq);

COMMENT ON TABLE "{schema}".wf_node_progress IS
    'The workflow progress STATE channel (T21 decision d): one row per (node_id, channel), upserted latest-wins + the honest occurrence counter (every coalesced emission counts); `dropped` accumulates the stream ring''s drop-oldest casualties. Row count = nodes x channels, CONSTANT under any emission rate (DH1''s fence). Keeps NO history — the "what did the body report at 14:32" question is answered by the STREAM channel only where the author streams to it.';

COMMENT ON TABLE "{schema}".wf_node_stream IS
    'The workflow progress STREAM channel (T21 decision d): append rows in a bounded per-node ring, drop-oldest with the dropped count ON THE RECORD. seq is THE ONE SEQ SPACE (DH3''s fence) — the user class (ctx.progress) and the auto class (the engine''s node-start/terminal projection) interleave in one cursor range; the kind vocabulary is closed by CHECK.';
