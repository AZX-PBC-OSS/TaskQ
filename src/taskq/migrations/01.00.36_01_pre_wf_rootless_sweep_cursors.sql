-- THE ROOTLESS WALKS' PERSISTED CURSORS (the round-2 conviction R1's
-- cure). Forward-only; there is no down migration. To revert, restore
-- from backup. The literal "{schema}" token is substituted at apply time
-- by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE TABLE STATEMENT, ITS SEED ROWS, AND
-- ITS COMMENT (the table's own relation lock on a NEW table — no ALTERs
-- of existing tables, no index builds, no backfills). All DDL is
-- additive; a pre-migration worker never reads or writes this table.
--
-- THE CONVICTION (R1, proven): the flow-history arms' ROOTLESS passes —
-- the PK-ordered walks that prune the orphan class (the row's pointee
-- is NOT a flow root at all) — NEVER ADVANCED at a live PK front. The
-- walk was ``ORDER BY pk LIMIT $1``: when the table's first $1 rows all
-- answered their probe "live root" (a live front), every pass re-walked
-- the SAME first batch, deleted nothing, and the orphans beyond the
-- front were stranded forever (the red-team's proof: a 3,000-row live
-- front, batch 1,000, five passes, deleted=0, 50 orphans stranded).
--
-- THE CURE: the estate's own watermark precedent —
-- ``job_events_prune_state`` (01.00.20_02), a persisted cursor row the
-- deleter advances IN THE SAME STATEMENT/TRANSACTION as its delete, so
-- the cursor can never lag what is already gone nor claim what is not.
-- Each rootless walk now SEEKS STRICTLY PAST its arm's last VISITED
-- row — visited, not deleted: a live front's rows ARE the advance (the
-- walk probed them once; it never re-reads them). One row per arm; the
-- cursor is the arm's own PK as text:
--
--   wf_edge           (child_id, parent_id)  → cursor_a = child uuid,
--                                            cursor_b = parent uuid
--   wf_join_fire      (id)                   → cursor_a = id uuid
--   wf_step_ledger    (id)                   → cursor_a = id uuid
--   wf_signals        (id)                   → cursor_a = id uuid
--   wf_node_progress  (node_id, channel)     → cursor_a = node uuid,
--                                            cursor_b = channel text
--   wf_node_stream    (seq)                  → cursor_a = seq bigint
--
-- NULL cursor = seek from the very start (the walk's sentinel: the nil
-- uuid / seq 0 / the empty channel — each below every real key). A pass
-- that finds NOTHING above its cursor REWINDS it to NULL (the
-- wraparound: a row whose root died after the walk had passed is
-- re-probed on the next sweep; one empty pass per full sweep, the batch
-- still bounding every pass). The walk's statements own the advance and
-- the rewind — INSERT .. ON CONFLICT / UPDATE in the delete's own
-- transaction — so a crashed pass rolls the cursor back with its
-- deletes and re-walks them; no row is ever skipped by a cursor that
-- ran ahead of its deletes. The FOR UPDATE on the arm's row serializes
-- a concurrent pass of the SAME arm (a rolling deploy's two leaders for
-- a beat): the loser reads the winner's ADVANCED cursor.
--
-- ROLLING DEPLOY: additive and inert to old code. Pre-fix arms never
-- touch this table (the seed rows just sit at NULL = from-the-start);
-- post-fix arms re-seed a truncated/absent row through their own UPSERT.
-- The rows are MUTABLE TEST STATE (the cursors advanced by one test
-- would bias the next), so testing/pg.py's truncate list carries the
-- table — after a truncate the walks answer NULL-cursor = from-start
-- and re-seed on their first pass, exactly the job_events_prune_state
-- precedent's reset contract.
CREATE TABLE IF NOT EXISTS "{schema}".wf_rootless_sweep_state (
    arm        text PRIMARY KEY,
    cursor_a   text NULL,
    cursor_b   text NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO "{schema}".wf_rootless_sweep_state (arm)
VALUES ('wf_edge'),
       ('wf_join_fire'),
       ('wf_step_ledger'),
       ('wf_signals'),
       ('wf_node_progress'),
       ('wf_node_stream')
ON CONFLICT (arm) DO NOTHING;

COMMENT ON TABLE "{schema}".wf_rootless_sweep_state IS
    'The flow-history rootless walks'' persisted cursors (the round-2 R1 '
    'cure): one row per arm, the arm''s last VISITED PK as text (cursor_a '
    '+ optional cursor_b; NULL = from the start). The walks seek strictly '
    'past the cursor so a live PK front never strands the orphans behind '
    'it; the cursor advances in the same statement/transaction as the '
    'deletes (the job_events_prune_state watermark''s own law) and rewinds '
    'to NULL when the walk finds nothing above it (the wraparound).';
