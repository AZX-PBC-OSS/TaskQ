# THE TYPED-WORKFLOW LAYER — the consolidated landing (T01–T24's corpus)

## What this is

The consolidated `taskq[flows]` branch: the schema round, the engine, the ledger, the typed API, the admin/CLI surfaces, the deploy matrix, the marches — plus the phase-5 tail's cures. **The evidence body carries the whole corpus: the pins, the attacks, the certifications, the bands, the marches, the stranger test.**

The spec: [docs/design/workflows-spec.md](docs/design/workflows-spec.md) (T14 — the built truth, the kill list first, the adoption pitch, the LATER rows, the abstraction contract / wire-shim boundary).

## The gates, with their numbers

| Gate | The number | The capture |
|---|---|---|
| THE DEPLOY MATRIX (T15/§22): PG 15.19 / 16.15 / 17.11 / 18.6 × the six cells | **6/6 per version, 24/24** | `.measurements/p5/matrix-pg{15,16,17,18}-final.txt` + `matrix-pg18-local.txt` |
| THE DEEP-RESEARCH MARCH ×3 (the real cron kickoff → the map → the multi-HITL loop → the degraded pivot → the kill storm → the explorer) | **3 passed ×3** | `.measurements/p5/march-run{1,2,3}.txt` |
| THE STREAMING-EMIT + DOC-INGEST MARCHES (in the same file) | **green ×3** | same captures |
| THE OPERATOR'S MARCH ×3 (deploy → observe → intervene → debug → upgrade → rollback + THE UPGRADE PATH: the prior release's schema, populated, the full chain, the workflows LIVE) | **2 passed ×3** | `.measurements/p5/operator-run{1,2,3}.txt`, `operator-full-run{1,2}.txt` |
| THE FULL DEFAULT BATTERY | recorded below | `.measurements/p5/full-suite-final.txt` |
| THE SYSTEM TIER (`--system-e2e -m system`) | recorded below | `.measurements/p5/system-tier-final.txt` |
| pyright (the touched surfaces) | **0 errors** | `.measurements/p5/pyright-systemtier-final.txt` |
| ruff | clean | CI |
| The abstraction contract (the grep gate — zero case-study content in the shipped surfaces) | **0 hits** | `tests/test_doc_ingest_example.py` (the shipped gate) |
| The estate-hygiene cure (the cert-2's 11 structural reds) | red-first → **140 passed** | `.measurements/p5/cure-*.txt`, `cure-all-final.txt` |

## The phase-5 cures (each red-first, the captures committed)

1. **THE DISPATCH FENCE'S ROOT-MARKER LEG** — the flow ROOT's row was dispatchable; the door refused it loudly and the WORKER DIED (the fleet died with it). The claim + probe fences exclude the root.
2. **THE CREATE-PATH RACE** — the static-node pass ran statement-autocommit; the dispatch round claimed a not-yet-wired join-wait child and EXECUTED it (the arg resolution crashed the worker). The create is ONE transaction.
3. **THE LADDER'S BOUNDARY** — the args resolution ran outside the ladder's try; a resolution failure killed the worker. The boundary is the execution's boundary.
4. **THE TERMINAL MARK'S AUDIT LEGS** — the wf finalize wrote terminal status with no `job_attempts` row and no `state_change` event (the shared invariant's "a mutation with no audit row"). Both legs now ride the terminal-mark transaction (the vanilla mark shape: the holder CTE, the started-at COALESCE, ON CONFLICT DO NOTHING).
5. **THE CANCEL CASCADE'S AUDIT LEG** — the same hole class in all four cancel statements.
6. **THE LOOP'S MULTI-HITL CURSOR** — the answer queue's cursor was keyed per ATTEMPT: a hold-resume re-consumed earlier decisions into later iterations (the third iteration received the second refine; the loop exhausted without the operator's third decision ever landing). For a loop-kind node the queue position IS the row's iteration counter — the resume CONTINUES, the retry REPLAYS, both laws one mechanism.
7. **THE NO-WALL LOOP SHAPE** — a `budget_s=None` loop's init wrote `deadline = now() + 0` (an expired wall the instant it landed); the budget sweep ate every budget-less loop that lost its holder. NULL is the no-wall shape.
8. **THE UNRESOLVABLE STEP'S PARKING** — a version-skewed step key translates to the `WorkflowBodyUnresolvableError` parking (the defined snooze), never a crash.
9. **THE PER-ATTEMPT CODE-VERSION RECORD'S WRITE SITE** — T03's record had the hash and NO writer; the claim path stamps it (a record, not a gate).
10. **THE PRUNE TOLERANCE'S COLUMN HALF** — the budget round's trio broke the pre-workflow archive write; the fallback's write is the PRE-BUDGET mirror variant.
11. **THE MIGRATION-NUMBER COLLISION (the merge)** — main's `01.00.23_01_pre_jobs_parent_id` vs the branch's `01.00.23_01_pre_workflow_columns`: the workflow chain renumbered (01.00.24–31), every reference with it; the tolerance target re-anchored.

## THE STRANGER TEST

One fresh subagent (its own detached worktree + PG; docs + the demo stack as its ONLY input): ran the flagship demo end to end and explained the workflow API from the docs alone. **18 stumbles** (verbatim: `.measurements/p5/stranger-report.md`, dispositioned in `STRANGER-DISPOSITION.md`): the doc side cured (the discoverability pointers, the duality's two names, **the phantom `workflows.run` API the guide cited but the code never shipped**, the resolve verb in the runbook, the broken links, the demo's recovery line, the Act-4 digest bug); the API side reported (the `HitlClient` re-export, the one-call surface, the GateDecl contract, the demo runs no workflows).

## The review = the handover law

The fresh reviewers + ZEAZX26; the merge ONLY on the fresh approval + the green CI. The author does not merge.

## The unspecifications (stated honestly)

- The stranger's Run 1 died mid-demo with no diagnostic (external to the demo); the demo's re-runnability carried it.
- The flagship demo runs NO workflows (the demo act is its own rev) — the workflow demonstration lives in the marches + `docs/examples/doc-ingest.md`.
- The workflows guide's §-renumbering is NOT done (the anchors are load-bearing); the reader's key + the start-here pointer mitigate.
- The body-must-be-atomic sentence: `run_migrations`' loop… nothing half-stated — the honest list is maintained in `STRANGER-DISPOSITION.md`.

## The gate's final numbers (updated)

| Gate | The number |
|---|---|
| THE FULL DEFAULT BATTERY (serial, the merged head) | **13,073 passed / 69 failed / 9 skipped** (1:06:37) — the 69 are the rotating co-tenancy class: every sampled victim green solo (`test_worker_di_bootstrap` 35/35, `test_worker_main`, `test_batch_fast` (cured), `test_doc_ingest_example`, `test_cron_ownership_model`, `test_grace_boundary_timeline`, `test_rt_conservation_chaos` 49/49 together) — WITHIN the phase-4 clean-base control's own band (59–74 failures on a CLEAN base, `.measurements/PHASE4*`/the phase-4 fixer's report) |
| THE BATCH COPY'S ARITY (the gate's REAL catch) | the loop-budget trio rode the COPY's column list but not the record — 43 vs 40, every fast-path batch dead by `IndexError`; cured (omit-list exact membership); **54 passed** |
| pyright + ruff | 0 errors / all checks passed |
