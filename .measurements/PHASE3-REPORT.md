# THE PHASE-3 BUILD REPORT (PR-5: T17 → T09 → T19 → T10)

Branch `feat/taskqflow-p3` off the certified core b4c0013d. Five commits,
each with its pins in the same commit, no merges, no pushes. The dev loop
was LOCAL (the warm PG on :5693 via `TASKQ_TEST_PG_DSN`; every gate run
captured to a timestamped file here, verdicts read back in minutes).

## THE COMMIT MAP

| commit | ticket | lands |
| --- | --- | --- |
| bf65171e | T17 | the disposition ledger (`.measurements/t17-dispositions.md`), the ergonomic contract (the guide), the abstraction-contract gate, the TASKQ_TEST_PG_DSN seam |
| 58366f7c | T09 | the flow API: the wiring verbs (step/map_source/gather/sink/build), @app.actor + @app.workflow, wf.validate() (E1-E6 + W1), the Mermaid emission, the runner (create/drive/result), the typeprobe corpus extension, the T17 contract probes' strict-xfail reds FLIPPED GREEN |
| 09841cbc | T19 | wf.loop (the control union Done/Refine), migration 01.00.26 (the budget columns), the budget sweep arm (AND NOT budget_paused), the loop driver (the carry frozen-at-spawn + the atomic advance+cap), the named exhaustion, the leader-sweep registration |
| cae64afd | T10 | migration 01.00.27 (wf_signals), the deliver CAS, the expiry arm, HitlClient (list/get/resolve + the audit rows + the knock), ctx.wait_signal/signal, cancel_workflow, SignalTimeoutError/SignalAbandonedError, the hold→resume band |
| 4eb6a0e8 | T19×T10 | the composition pin: the hold inside the loop pauses the budget and completes (the P3 1a matrix, 6 checks) |

## WHAT REDED FIRST (the red-first ledger)

- **T17's four contract probes**: captured RED as strict-xfail against
  the pre-API tree (`.measurements/t17-contract-first-run.txt`); the
  cure commit removed the markers (a strict xfail passing FAILS the
  suite — the flip was forced) and they green (t09-contracts-green.txt).
- **The type gates**: the T01 corpus + the new wf_api corpus — 6
  MUST_ERROR markers, each red on pyright 1.1.414 AND ty 0.0.85
  (t09-typegate.txt).
- **The mutation drills** (the observed reds, captured): the budget
  arm's `AND NOT budget_paused` flipped → the paused-row pin reds with
  the dragon's name (t19-mutation-drill-red.txt) → restored → green
  (t19-loop-restored.txt). The CONSUME-BUDGET dragon and the
  shared-attempt-counter variant are KEPT RED FOREVER (recorded in the
  redlogs + the pins).
- **The runner pins' reds discovered LIVE**: the ledger claim's absence
  stranded the success path's terminal write; the parent-results
  step_key dict collapsed map children; the ctx.step terminal never
  landed (the phase-2 defect); each was fixed with its pin in the same
  commit.

## THE NUMBERS (the bands, from the files)

- **The loop bands** (t19 pins): the cap wall = exactly `max_iterations`
  spawns + the named `iteration_cap_exhausted`; the budget wall fires on
  PG's clock (the +1h SkewedClock fixture green); the paused row with a
  FORCED-PAST deadline never fires.
- **The hold→resume band** (G11c, t10-hold-resume-band.json): the
  deliver's commit → the row claimable, measured then PINNED ≤ 50 ms in
  `perf-evidence-workflows.md`.
- **Coverage**: 94% branch on `taskq/workflows/api/` (every module
  ≥ 91%; the estate floor is 90) — t09-coverage-4.txt.
- **The battery**: 205 passed (the full wf family + the estate slice)
  on the T10 head; ×2 stability runs green (133 × 2, the pin families)
  on the final head. Pyright 0 errors across the new surface + pins.

## THE DOCS WRITTEN

- `docs/api-reference/workflows.md` (new; in the nav): the tour, the
  wiring verbs, the validate rules (the false-positive budget tracked),
  the Mermaid vocabulary, the runner.
- `docs/guides/workflows.md`: the ergonomic contract (T17) + §1 (the
  concept), §2 (the two type faces), §3 (fan-out & reduce), §9 (loops &
  back-edges — the carry, the two walls, the named exhaustion, the
  ladder classes, the NAIVE-MEMO guidance), §4 (HITL — the resume
  contract, the reply handle, the context contract, the knob, the
  timers, the cancel cascade, resume-not-retry).
- `perf-evidence-workflows.md`: the hold→resume band row.

## THE UNSPECIFICATIONS FED BACK (never silently decided)

1. **The migration numbers drifted from the tickets' plan**: T19's
   ticket says `01.00.24_*` and T10's says `01.00.25_*`, but phase 2
   landed 01.00.24 (the edge failure policy) and 01.00.25 (the status
   indexes). LANDING order governs (the tickets' own rule): T19 →
   `01.00.26_01_pre_loop_budget.sql`, T10 →
   `01.00.27_01_pre_wf_signals.sql`. The _variants matrix coverage was
   NOT needed (additive-only metadata ALTERs; no plan-shape variant).
2. **The `01.00.23` F2 rules inheritance**: both new migrations carry
   the single-lock-class + the phase-obligations headers, per the
   ticket's "both inherit T03's F2 rules".
3. **The wait-site consumption semantics were unspecified**: the
   re-execution doctrine + multi-hold + retry interact — the shipped
   answer is the ANSWER QUEUE (the delivered holds consumed in epoch
   order by a per-attempt cursor: a retry replays the answers — the
   operator never re-answers; a wait past the queue's end registers a
   new hold). The tickets never spelled what a RETRIED attempt's waits
   read; this is the v1 semantics, pinned.
4. **The v1 skip semantics were unspecified** (what a skipped child's
   downstream sees): shipped as succeed-WITH-the-record + the typed
   absorbed item on the absorbing joins (a skip is not an attempt).
5. **The loop body's HOLD was unspecified at the driver**: the hold
   inside an iteration is NOT a body failure — the driver catches the
   hold, the iteration rests 'awaited', the budget pauses. Pinned (the
   composition pin).
6. **The escalation actor's enqueue shape** was unspecified
   (`on_exhausted="escalate"`): v1 rides the SAME outbox the fired
   joins use (consumer step key `loop.escalation`) — no second delivery
   mechanism.
7. **The F3 worker-boot pin (T09's pin 9) is PARTIAL**: the projection
   carrier (the ActorConfig-compatible face) is pinned; the full
   worker-boot fail-fast exercise (TASKQ_QUEUES_STRICT with a
   workflow-actor queue) needs the worker wiring round — the red-team
   should attack the seam.
8. **The pytest-9.1.1 fixture bug (#14971) bit AGAIN** (the locally
   defined redlog fixtures dropped for the non-adjacent file) — the
   cure is the same shape as phase-2's (the fixtures live in
   `tests/_wf_fixtures.py`, registered from the root conftest); the
   upstream issue remains the root cause.
9. **The RedLog fixtures REWRITE their tracked evidence files on every
   run** (only the last entry survives) — the phase-2 evidence files'
   churn is a live defect in the measurement corpus's append-only law;
   found, NOT fixed here (the fixer should make RedLog append or
   key-per-run).
10. **`wf.validate()`'s fan-in rule**: the ticket's "~5 rules" became 6
    E-rules + 1 W (the fan-in bound is the engine's own E6 — the count
    difference is the mutation matrix's granularity, not a scope grow).
