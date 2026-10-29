-- THE DURABLE TIMER'S DUE-SCAN INDEX (the timer-cures lane's cure 4 —
-- the 10k-timer tail). Forward-only; there is no down migration. To
-- revert, restore from backup. The literal "{schema}" token is
-- substituted at apply time by the migration runner.
--
-- PHASE OBLIGATIONS (single-lock-class file — GAPS-ESTATE F2)
-- ------------------------------------------------------------
-- THIS FILE HOLDS ONLY THE CREATE INDEX STATEMENT (SHARE lock class —
-- blocks writes for the build's duration, never reads; no ALTERs and no
-- CREATE TABLEs mixed into the window).
--
-- THE SCAN IT SERVES: the sweep's TIMER arm's due-scan —
--     WHERE status = 'held'
--       AND starts_with(signal_name, '__timer__')
--       AND expires_at IS NOT NULL AND expires_at <= clock_timestamp()
--     ORDER BY expires_at, id  LIMIT n
-- Pre-index this was a SEQ SCAN + SORT over every wf_signals row on
-- every sweep tick (the drill's receipt: 10k due timers + 100k human
-- holds, Seq Scan + Sort Key (expires_at, id)).
--
-- WHY THE PARTIAL INDEX (the reads chose it — measured): a full
-- (status, expires_at) index planned as a Bitmap Heap Scan + Sort with
-- the starts_with filter residual; THIS partial index plans as an INDEX
-- ONLY SCAN — the predicate consumes the status equality AND the
-- prefix, the column order owns the ORDER BY (no sort), and the index
-- contains only the timer rows (the human holds' volume never touches
-- it). starts_with is IMMUTABLE (pg_catalog), so the predicate is
-- index-legal. The expiry arm (the HUMAN holds' sweep) keeps its own
-- shape — this index is the timer arm's, not a shared one: one scan,
-- one owner.
CREATE INDEX IF NOT EXISTS wf_signals_timer_due_idx
    ON "{schema}".wf_signals (expires_at, id)
    WHERE status = 'held'
      AND starts_with(signal_name, '__timer__');
