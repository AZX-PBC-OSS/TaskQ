# THE T21 BUILD REPORT (PR-6: the progress-observability BUILD)

Branch `feat/taskqflow-t21` off the certified phase-3 head (55faa1f2),
REBASED over the phase-3 fixer (3941c874, the attack-3 lane) mid-build —
the reconciliation is in the code, no merges. Five commits + two
measurement chores, each with its pins in the same commit. The dev loop
was LOCAL (the warm PG on :5705 via `TASKQ_TEST_PG_DSN`; every gate run
captured to a timestamped file in this directory; the numbers run writes
`t21-numbers.json`). The PoC gate: `PROVEN` (t21-20261008T033005Z,
DH1–DH8 closed, 3 clean rounds) — the PoC's enumerated engine needs is
the spec this build ported: **2 tables, 10 named statements, 1 sweep
arm, the emission op, ZERO finalize changes.**

## THE COMMIT MAP

| commit | lands |
| --- | --- |
| 6250a409 | T21 — the two-channel persistence: migration 01.00.28 (wf_node_progress + wf_node_stream), the ten named statements (`_sql_progress.py`), the map-progress read's state-channel extension, the ring-prune sweep arm + its `wf_progress_ring_prune` registration |
| 5f178ff4 | T21 — the emission op + the auto projection: `StepContext.progress`, the coalesce (`ProgressEmitter`), the typed gate (`validate_emission`), the declared schema (`step(..., progress_schema=)`), the claim/finalize seams' `class='auto'` projections |
| 50f2f87→9e615194 | T21 — the aggregation + the faces + the fence: `_progress_read.py` (the SSE face's named-partial replay, `rebuild_display`'s ledger-wins fold, the map line, the read-side `aggregate=`), `WorkflowDef.aggregates`, the W2 join-for-progress validate warning |
| b56f9086 | T21 — the HTTP face: `progress_stream_generator` + the admin route (`GET /api/flow/{flow_id}/progress/stream`); the aggregate's input excludes the map's own auto-join |
| 82a7034c | T21 — the docs: the guide's §5, the progress guide's workflow section, the shim note, the band rows |
| d1d068fa | the rebase reconciliation's fixes (see the unspecifications) |

## THE PIN MAP (25 pins + the numbers run)

| pin file | pins |
| --- | --- |
| `test_wf_progress_persistence.py` (11) | the two tables' exact shape (+ ONE seq generator — DH3's one-cursor law); the storage teeth (the class/kind CHECKs refuse a third vocabulary); the state upsert's latest-wins + the honest counter; the ring bound + the EXACT drop accounting (appended == retained + dropped); the one-seq-space interleave (auto/user/auto); the projection's vocabulary refusal; the prune arm (the leak observed first: 801 rows → 64 in one pass, drop-oldest); the map line's LEFT JOIN (the silent child reads NULL, the counts stay complete); the aggregate input read; the ZERO-FINALIZE-CHANGES probe (the terminal-mark statement touches no progress table — structural); DH1's every-emission-a-row variant KEPT RED |
| `test_wf_progress_emission.py` (9) | the chain end-to-end (both channels + the interleaved projections + occurrences == emissions); the declared-schema door's teeth (the lying body FAILS the node); the typed refusals (pct/data shapes); the D5 cap with the `__truncated__` marker; the 10k-emission coalesce (occurrences == 10,000 EXACTLY, the STATE channel at 2 rows); the broken-pool flush (counted, warned, never raised); the disabled emitter (500 counted, nothing written); the drop counter on the record; the failing body's terminal projection (pct 99 in the state row, the ledger says failed) |
| `test_wf_progress_faces.py` (8) | the mid-map reconnect reconstructs the display (full + partial + the pruned window's convergence from the state re-sync); the silent-gap variant structurally impossible; the liar's display (failed WITH pct=99 inside; the liar counterfactual RED kept forever); the crash window (freshness-only loss, the terminal heals, the state ledger-derived at every sample); the MID-FLIGHT read-side aggregate over the half-done children (writes nothing, `wf_join_fire` == 0, fn_ms on the record); the gauge's workflow-dimensioned cardinality (the per-child counterfactual red); the W2 sunk-join warning; the HTTP face (the route registered + the wire shape: display first, the seq-ordered frames) |

## THE REDS (observed, captured to `t21-pin-reds.json`)

1. **the every-emission-a-row log** (DH1): 2,000 emissions → 2,000 rows
   (the numbers run re-observed it: 2,000 rows; the built state channel:
   2 rows).
2. **the unpruned ring** (DH1): the leak seeded past the bound → the arm
   prunes to exactly the bound, drop-OLDEST (the node's own head seq
   survives).
3. **the liar counterfactual** (DH4): the display deriving the state
   from the emissions → `running` at pct 99 on the dead node — the BUILT
   display shows `failed` with pct=99 inside.
4. **the silent-gap face** (DH6): structurally impossible in the built
   mode logic — a pruned-past cursor NAMES `mode='partial'` + carries
   the state re-sync.
5. **the per-child-label gauge** (DH5): the counterfactual series count
   (= children) recorded; the built gauge's series are (workflow, state)
   — flat.
6. **the chatty-body storm** (DH2): the write-per-emission counterfactual
   measured at ~226 writes/s of pure insert churn (the numbers run); the
   built coalesce held ~20/s under a 92k emissions/s body.

## THE NUMBERS (`.measurements/t21-numbers.json`, on the built code)

- **10,000 emissions** (measured at ~92,000 emissions/s — 92× the PoC's
  storm shape): the STATE channel stayed at **2 rows** (CONSTANT);
  `occurrences == 10,000` EXACTLY; the ring at the bound.
- **the 200-child map**: the grouped progress line (one read,
  `total/done/running/avg_pct/freshest` per step key); the read-side
  `aggregate=` fn over the 200 decoded children: **fn 0.009 ms** (the
  PoC's 0.22 ms shape), the read wrote nothing.
- **the finalize's blindness**: structural (the probe) — the terminal-
  mark statement touches no progress table.
- **the crash window**: freshness-only loss (the pin: pct frozen at 99
  while the ledger says running; the terminal heals; the state
  ledger-derived at every sample).

## THE GATES (the captured rounds)

- **3 consecutive clean rounds** on the final head
  (`t21-gate-{1,2,3}-*.txt`): ruff clean on the touched surface, pyright
  0 errors, **257 passed** per round (the full wf family + the phase-3
  cure pins + the admin jobs suite + the CI lane, the CI's own lane
  split: main / slow / load_sensitive-serial). Round 1's load_sensitive
  lane reded once (the enqueue-latency band — declared load-fragile,
  pre-existing flake: it reds on the BASE branch under load too) and
  passed on the immediate re-run; rounds 2–3 green straight through.
- The numbers run captured (above).

## THE DOCS

- `docs/guides/workflows.md` §5 — the emission op, the two channels, the
  asymmetry, the auto projection, the progress-lie fence, the derived-
  at-read aggregation, the faces, the shims.
- `docs/guides/progress.md` — the workflow-node section + the state
  channel's no-history boundary + the shim note.
- `perf-evidence-workflows.md` — the T21 band rows.

## THE UNSPECIFICATIONS FED BACK (never silently decided)

1. **The migration number**: the tickets named none for the build; the
   landing order governs → `01.00.28_01_pre_wf_progress.sql` (the fixer
   lane took no number).
2. **The stream's seq is DB-side** (`bigserial`) — the estate's app-side
   uuid7 id invariant is about IDENTITIES; the replay cursor is a
   POSITION and cannot be a uuid (the 01.00.20_03 `progress_seq` bigint
   precedent cited in the migration header). The PoC's one-sequence pin
   carries over (exactly one generator backs the table).
3. **The STREAM ring coalesces too** — the flush appends ONE row per
   FLUSH (the latest snapshot), not one per emission; the occurrence
   counter keeps the emission count honest. The PoC's numbers implied
   this shape (159 appended for 10k emissions); the docs state it.
4. **The aggregate's registry key**: the declared `aggregate=` fn is
   keyed by the map SOURCE's step key (the children's parent — the read
   surfaces resolve by the parent row), not the join's; the durable leg
   is the flow root's stamped workflow name (the fired join's own
   doctrine). The registry's shadow check compares aggregates too.
5. **The aggregate's input excludes the map's auto-join row** (its
   result IS the collected list — doubly counted); the read statement
   returns `step_key` and the read filters by name. The fleet's own
   `result` ENVELOPE (`{"value": …}`) unwraps — the fn sees exactly what
   the body returned.
6. **W2's detectable shape**: "a join whose only consumer is the
   display" is undecidable statically; the SHIPPED rule warns on the
   SUNK join (the declared no-dataflow-consumer marker) — the advisory
   class, naming the `aggregate=` door.
7. **The loop node's ctx.progress rides ONE emitter for the whole loop
   node** (the node is the progress unit; the iterations are ledger
   rows); the emitter's close rides `_drive_loop`'s callers (the
   fixer's refactor added the inner drive — the emitter param threads
   through it).
8. **The rebase reconciliation's fixes were LOST once** (the stash dance
   during the enqueue-band flake investigation dropped uncommitted
   fixes; the battery's round-1 red caught it — 65 fails — and
   d1d068fa re-applies them). The lesson is the capture law's: uncommitted
   fixes are not work.
9. **The load_sensitive lane flakes under battery load** (the enqueue
   band, pre-existing, reds on the base branch too) — the battery runs
   it serially as CI does; a first-run warm-up flake re-runs immediately
   and both verdicts stay in the capture.
10. **The shim is a documented MAPPING, not shipped code** (the PoC's
    disposition kept): the two old vocabularies' mappings live in the
    docs (§5's table); no runtime shim object ships in v1.
