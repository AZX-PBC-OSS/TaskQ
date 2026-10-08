# FINDINGS-VALIDATION — the two red-team reports vs the CURRENT consolidated head

**Validator:** the findings validator (subagent, alone), 2026-10-08.
**Audited head:** `9d584bcd` (`feat/taskqflow`). **NOTE:** the head moved
mid-audit — the session began on `b7774945`, the phase-5 builder landed
`63197a4b..9d584bcd` (the `EdgeFailurePolicy` typed door + the router
corpus) while the probes ran, and **every verdict below was re-verified
on `9d584bcd` itself** (the cancel/sweep/worker code was untouched by the
delta; the type gate re-ran: 17 markers green on both checkers).
**Probes:** my own detached worktree `/tmp/opencode/wt-fv-validate`
(probe scripts `/tmp/opencode/fv-probes/`), PG `:5715` (my own container,
`taskq-fv-pg`). `src/` untouched on the consolidated branch; two
UNCOMMITTED deliverable test files live in my worktree's `tests/` (the
next fix round lifts them from there, or re-derives them from this file).

**Counts: 12 convictions validated → 6 STILL-LIVE, 3 PARTIAL, 2
FIXED-ALREADY, 1 decisions-needed (with one live bug inside). Sub-items:
9 sub-findings still live.**

---

## The table (finding → verdict → evidence → cure owner for the still-live)

| # | Finding | Verdict | Evidence (current head unless noted) | Cure owner (still-live) |
|---|---|---|---|---|
| 1 | THE CANCEL ATOMICITY — the torn cancel | **STILL-LIVE** | `_hitl.py:849-856`: `cancel_run_signals(pool, …)` acquires its OWN connection (`pool.acquire()`), autocommit — called from inside `cancel_workflow`'s tx (`_runner.py:726-728`). My probe on :5715: forced rollback after the signal leg → **`root='running'`, `signal='cancelled'` — TORN=true**; `max_size=1` pool → the signal leg **deadlocks (TimeoutError)**, the two-connection architecture proven live. The phase-4 fixer's cure `32297d6f` exists but is **NOT an ancestor** of the head (it rides `feat/taskqflow-p4` only); the phase-4 fixer's claim ("the held-signal leg takes the CALLER'S conn") is true on THAT branch, false on the consolidated head | **Merge `32297d6f`** (re-land the caller's-conn cure on the consolidated head); the rollback + max_size=1 probes re-run green is the acceptance test |
| 2 | FABRICATED RED EVIDENCE — the hardcoded `.red()` entries | **STILL-LIVE** (the phase-3 round fixed the SINK MECHANICS only) | The sinks ARE append-only/run-scoped now (`_wf_fixtures.py` `RedLog.flush` → JSONL + run id). But the entries are still hand-written: `test_wf_loop_pins.py:476` (pin1 "the drill ran" — prose admitting no drill runs here; payload hardcoded), `:254` (pin2 records `paused_row_fired: True` at the end of a test whose GREEN assertions prove the opposite), `test_wf_hitl_pins.py:119` (pin1 records `zombie_wake: True` — the test asserts refusal), `:226` (pin7's ledger prose). My AST guard finds **11 literal-payload sites** (also `test_wf_engine_units.py:200,242`, `test_wf_finalize_pins.py:183,274`, `test_wf_fork_pins.py:91,177`, `test_wf_progress_faces.py:215`) — 7 MORE than the reports enumerated | **The fix round**: run each named mutation drill FOR REAL and record what it yielded, or delete the entry (a red that never ran doesn't exist). **THE GUARD IS WRITTEN** (my deliverable): `tests/test_fv_redlog_guard.py` in my worktree — rejects any `.red()` payload that is a literal-only collection; bites on all 11; its own bite test green |
| 3 | THE ORPHAN PERF FIGURE + the rewritten-in-place bands | **PARTIAL** | The quote "p50 477 ms / p95 1.73 s @ ~340 live joins" is **ACCURATE to its artifact**: `/tmp/opencode/fanout-proof/PAPER-CUTS.md` cut #4 + `READINESS.md:87` (the same numbers, the same 14.9 ms @ 200 contrast), and the citation NAMES the provenance ("the fanout proof's cut #4 crime", `perf-evidence-workflows.md:30`). So "orphan/unprovenanced" is **FALSE**. BUT the chain ends in prose: **no raw JSON in `fanout-proof/evidence/` carries 477/1.73** — the prototype's number is report-text only. AND the second half is **STILL-LIVE**: the band artifacts are newest-run-wins rewrites — my diff on the consolidated worktree: `.measurements/join-fire-latency.json` 8.479 → 15.293 (the old measurement DESTROYED, not appended); `pin4-enqueue-band.json` 206-line rewrite; the phase-2 cert itself admits "(wf-rollup-band.json, rewritten by my run)" (`cert2/PHASE2-CERTIFICATION.md:159-160`). The redlog sinks' append-only law was NOT extended to the bands | The fix round: extend the append-only/run-scoped pattern to every band artifact; re-capture the 477 figure into a raw JSON (or mark it prose-inherited) |
| 4 | THE SWEEP LOCK-FIRST TEETH | **STILL-LIVE — CONFIRMED BY RUNNING THE MUTATION** | I stripped `FOR UPDATE SKIP LOCKED` from `REDERIVE_SWEEP_SQL` + `SWEEP_FIRE_SQL` on a copy and ran the sweep+finalize pin set: **15/15 GREEN with the mutation** (`test_wf_sweep_pins.py` + `test_wf_finalize_pins.py`, 15 passed both before and after). No pin holds a child lock across a sweep pass — the lock-first claim had no teeth. **THE PIN IS WRITTEN** (my deliverable): `tests/test_fv_sweep_lock_teeth_pin.py` — a child's row lock held across a sweep pass: the shipped sweep SKIPS it (fast, counter untouched this pass), the released pass heals; **the no-SKIP-LOCKED mutant BLOCKS and the pin reds** (verified: mutant → failed, shipped → green, src restored) | The fix round: lift the teeth pin from my worktree (it carries the drill recipe in its docstring) |
| 5 | THE TYPEPROBE ESTATE | **PARTIAL — MOSTLY FIXED** | **FIXED-ALREADY:** the gate (`tests/typeprobe/_gate.py`) holds on the head — **17 MUST_ERROR markers red on pyright 1.1.414 AND ty 0.0.85 with DECLARED rule-ids**, the unmarked-error refusal live (the clean-site escape hatch removed); the span convention (whole-AST-statement assertion) kills the off-by-one; the dedicated `pyrightconfig.json` (`reportArgumentType` ON) kills the stale checker-invisible claim; no `error_traceback` clean sites remain (the corpus carries none). The "NOT a MUST_ERROR" line (`attack3_wf_negative_types.py:96`) is **FALSE** as a fabrication charge — it is an honest gap statement: I confirmed both checkers stay silent on the raw-body loop probe (BodyFn's object return still erases it). **STILL-LIVE sub-finding:** `attack2_wf_phase2_types.py` (9 markers) + `attack3_wf_negative_types.py` (6) are **in NO gate's corpus** (`_CORPUS` names 3 files) — and they DO produce errors (pyright: 27; ty: 17+4), so 15 marker-bearing files' worth of negative probes are unasserted by any gate | The type lane: add both files to `_CORPUS` (curation: the gate demands rule-ids per marker — name them) |
| 6 | CI RED AT HEAD + the stale certification | **STILL-LIVE** | The estate-hygiene cure `17914536` is **NOT an ancestor** of the head (it rides `feat/taskqflow-p5`). The violations stand on the head: stdlib `import json` at `workflows/_progress.py:42`, `workflows/_progress_read.py:35` and `:170` (the doctrine says `taskq._json`); the json doctrine has NO lint leg (ruff's TID251 covers uuid only, `pyproject.toml:275-296`), so nothing guards it; the cure's guard tests/battery are on p5, invisible here. Any certification claiming the head carries the cure is stale; any claim of "identical violations" should be read against THIS head: the json imports (at least 3 files, 7+ sites) are REAL here | **Merge `17914536`** (re-land p5's hygiene cure); then decide the enforcement leg (a TID251-style ban on stdlib json in `src/taskq/`) so it cannot regress silently |
| 7 | THE COVERAGE FLOOR | **STILL-LIVE** | My scoped run on the head (`test_wf_*` + `test_workflows_*`, branch coverage, 219 passed): **TOTAL 87%** (2713 stmts, 311 miss, 682 branch, 108 partial) — `_hitl.py` **67%**, `_worker_execution.py` **28%** (the new worker-hosted door is the biggest hole), `_ctx.py` 79%, `_ctx_wait.py` 85%, `_loop.py` 83%. The phase-4 attacker's claimed residuals (`_hitl 92/_loop 100/_ctx 94/_ctx_wait 99/engine 99`) are NOT reproducible from the wf pin suite alone — either measured on a broader lane or on a different head; either way the floor is not honestly met workflows-scoped. The gate is **global only**: `Makefile:79` and `ci.yaml:482` (`--cov=taskq --cov-fail-under=90`) — **no workflows-scoped coverage check exists**; the scoped deficit survives by dilution into the global number | The fix round: close `_worker_execution` (28%) + `_hitl` (67%) under the wf lanes, then ADD the scoped gate (a `--cov=taskq.workflows --cov-fail-under=90` CI leg) so the floor cannot dilute |
| 8 | THE @APP.ACTOR METADATA DROP | **STILL-LIVE** | My probe (the docs' own `doc_ingest` shape): a body handle `WorkflowActor(name='stage_a', queue='cpu', max_attempts=7)` wired via `step(handle, params)` compiles a node with **`actor='wf' queue='default' max_attempts=3 retry_kind=None`** — `step()` reads only its own kwargs (`_graph.py:318-325`), never the handle (`_graph.py:388-403` reads only `__name__`/`__annotations__`). The boot projection (`_worker_execution.py:178-192`) projects the graph's decls — so it projects the WRONG (`wf`, `default`) cohort, not the author's declared one. The queue-truth cure makes the ROW's label authoritative at execution, but the author's declared placement never REACHES the row: `@app.actor`'s queue/retry config is decorative, and the guide's own example (queue="cpu" × 4) routes to `default` | The fix round: `step()` derives actor/queue/max_attempts/retry_kind FROM a `WorkflowActor` handle (the handle's values as defaults, the step kwarg as the explicit override); the projection then sees the declared cohorts |
| 9 | THE EXECUTOR + THE DEFAULT-QUEUE COLLISION (the claim-snooze ping-pong) | **FIXED-ALREADY** | The intercept IS on the head: `worker/run.py:1062-1102` — a claimed row with `metadata.flow_id` routes to `_dispatch_flow_job` BEFORE the actor lookup; `dispatch-actor-not-found` is only the fall-through for non-flow rows (defined parking, the stranded-jobs detector the witness). The boot projection + `WorkflowActorQueueConflictError` (`_worker_execution.py:110-243`) are landed (`ea82ee36`, `5e857dc1` — ancestors of the head). **I re-ran the cure probe on MY OWN :5715** (`probe_cure.py`, real migrations, real worker subprocesses): PHASE A GREEN (claimed + executed ONCE, no snooze loop, the vanilla co-tenant never touched the flow rows), PHASE B GREEN (bodies in the worker process, the driver only orchestrating), PHASE S GREEN (the source on the default pool, the chain on the gpu pool) | — (no cure owed; keep the intercept's lazy-seam fallthrough documented) |
| 10 | THE STATIC-TYPE GAP: `step[R]`/`Promise[Never]` | **FIXED-ALREADY** | `086edf86` is an ancestor; `step[R]` (`_graph.py:318`) and `build(result, *residuals: Promise[Never])` (`_graph.py:568`) are shipped; the generic corpus's canonical wrong graphs — a real promise in the `Promise[Never]` residual slot, `Promise[Report]` vs `Promise[Config]`, `Promise[list[Report]]` vs `Promise[list[Config]]` (`wf_generic_step_negative_types.py:91,109,127`) — red on both checkers (the gate: 17 markers green); the two-faces section is in the guide (`7859aa96`) | — |
| 11 | THE ERGONOMIC SWEEP (9 sub-items) | **1 FIXED, 7 STILL-LIVE, 1 decision** | (a) **sync/async build contradiction: FIXED** — `@app.workflow` takes `Callable[[], object]` (`_app.py:271-299`), the docstring states the sync-pure law. (b) **the dead `channel=` param: STILL-LIVE** — `workflow(channel: SignalChannel \| None = None)` is accepted and never read (the closure stores only name/capture/redact). (c) **the join envelope vs `list[Report]`: STILL-LIVE — probed**: `gather([Promise[Report]])` → consumer with a `list[Report]` param → `validate()` reports **0 diagnostics** (E5 skips body-is-None joins both directions, `_validate.py:232-235`) AND the runtime codec returns lists RAW (`_runner.py:696` `if not isinstance(raw, dict): return raw`) — the `{"value": [...]}` unpack reaches a `list[Report]` param unvalidated at BOTH faces. (d) **the packaged `run()`: STILL-LIVE/absent** — `taskq/workflows/__init__.py` `__all__` (24 names) has `FlowRunner` but no run-over-the-raw-runner convenience. (e) **the public progress-stream export: STILL-LIVE/absent** — `progress_stream_generator` lives in the private `_progress_read.py:317`; the admin imports the private module directly. (f) **the two "step" nouns: STILL-LIVE** — the wiring verb `step()` (`_graph.py:318`) vs the memoized sub-step `ctx.step()` (`_ctx.py:204`), different semantics, one name. (g) **`loop(carry=)` type/value conflation: STILL-LIVE** — one param, two meanings resolved by distant isinstance branches: scalars → the initial VALUE (`_runner_loop.py:160-161`), BaseModel instances/classes → the CARRIER-TYPE check's subject (`_validate.py:375-381`); a dict carry silently gets NO type check. (h) **signal_name from `__name__`: STILL-LIVE** — `_ctx_wait.py:133-134` derives the signal identity from the model class name; a rename orphans live holds. (i) **the duplicate-error under hot reload: STILL-LIVE — probed**: my own probe hit `DuplicateWorkflowError` re-registering the same name in one process; a hot reload re-importing a module re-registers and dies | The fix round: (b) wire or delete `channel=`; (c) teach E5 the bodyless-join edge (the gather's element type IS declared — `Promise[list[R]]`) + item-validate list params in `_coerce`; (d)/(e) export a packaged `run()` + the progress stream (the event-intel prerequisite); (f)/(g)/(h)/(i) are DESIGN DECISIONS (name the sub-step verb, split `carry=` into `carry_type=`/`initial=`, pin signal names explicitly, make re-registration idempotent-per-import or keyed by source) |
| 12 | THE CAPABILITY DECISIONS | **DECISIONS NEEDED (one live bug)** | (a) **the provider rate-limit hook on the claim path: ABSENT** — the claim CTE's reservation machinery is queue-capacity-shaped only (`reservation_slots`/`reservation_headroom`); there is no provider-identity (rpm/tpm) hook a model-provider actor can bind. (b) **the 30-day cache recipe + the failed-run-squats-the-key bug: STILL-LIVE** — `FLOW_RUN_INSERT_SQL` is `ON CONFLICT (idempotency_scope, idempotency_key) DO NOTHING` (`_sql_ledger.py:128-138`) and the read-back returns the EXISTING row whatever its status: a **terminal-FAILED run squats the caller's key** — re-enqueue with the same key returns the failed run until retention (30-day default) prunes it. (c) **cost-shaped budgets: ABSENT** — `budget_s` is time-only (T19); no token/cost-shaped budget wall exists. (d) **the pct-vs-percent bridge: TWO VOCABULARIES LIVE** — `ctx.progress(pct: int \| None)` (`_ctx.py:172`), the publish path's `percent: float \| None` (`progress/_publish.py:180+`), the ring's `avg_pct` (`_sql_status.py:96-99`) — three spellings, no single conversion point | See the decision sketches below |

---

## THE DECISION SKETCHES (finding 12 — for the maintainer, not the fixer)

**12a — the provider rate-limit hook (claim path):**
- Option A (claim-path, honest): extend the dispatch CTE's reservation
  arm with a provider dimension — `ActorConfig.metadata` carries
  `{provider, rpm, tpm}`; the reservation CTE debits a per-provider
  token-bucket table in the same statement the claim fires (one
  round-trip, the same lease machinery). Cost: the claim CTE grows a
  third contention surface; the drift guards need a provider key.
- Option B (completion-path, cheap): a completion hook (the finalize's
  consumer side) records usage and the ADMIN/analytics surface enforces;
  the claim path stays pure capacity. Cost: a burst can oversubscribe
  the provider between ticks.
- RECOMMENDATION: B first (no new contention surface), A when a real
  fleet shows provider 429s as the binding constraint. The decision the
  estate needs NOW: name the carrier (`ProviderBudget` in ActorConfig
  metadata) so both options share the same config door.

**12b — the run-key squat:**
- Option A (release on terminal-fail): the flow-terminal write deletes
  (or re-arms) the idempotency key when the root terminalizes FAILED —
  a re-enqueue re-runs. Cost: the idempotency guarantee weakens for
  callers who WANT failed-run memoization (a re-run of a failed run is
  usually the caller's intent — the current squat is the surprising
  behavior).
- Option B (the recipe, documented): keep the squat, document the
  re-key recipe (the caller owns the key: append the attempt epoch).
  Cost: every caller learns the key-lifecycle rule the hard way.
- RECOMMENDATION: A — terminal-SUCCEEDED runs keep the key (the memo);
  terminal-FAILED/CRASHED runs release it. One CAS at the terminal
  write; the squat bug dies with the caller never learning a rule.

**12c — cost-shaped budgets:**
- Extend the T19 wall from `{budget_s}` to a carrier `{time_s, tokens,
  usd}` read by the SAME advance+cap statement (the loop's budget_paused
  machinery generalizes; the PG-side wall stays the PG-clock — token/cost
  counters are app-reported per iteration, the wall compares the SUM).
- RECOMMENDATION: ship `tokens` first (the LLM case is the only live
  one), `usd` derived at the config boundary (a price table), never a
  second runtime currency.

**12d — the pct/percent bridge:**
- One canonical spelling at the STORAGE boundary (`percent` float 0..100
  — `pct: int` becomes an accepted alias at the typed door, normalized
  on entry), one at the METRIC boundary (`avg_pct` stays — the SQL
  column is deployed; alias in the docs table).
- RECOMMENDATION: normalize at `_ctx.progress`'s door; do NOT rename the
  column. The bridge is one cast plus a docs table row — the decision is
  WHICH spelling the PUBLIC surface spells (recommend `percent`, the
  publish path's own word).

---

## THE STILL-LIVE LIST — the next fix round's work order (ranked)

1. **#1 the torn cancel** — data-destroying (a held operator's decision
   destroyed by a cancel that never landed); the cure exists (`32297d6f`)
   and merely needs re-landing. Probes: the rollback drill + the
   `max_size=1` deadlock, both captured this round.
2. **#2 the fabricated reds** — the evidence estate's integrity; the
   guard is WRITTEN (my worktree `tests/test_fv_redlog_guard.py`), 11
   sites to re-drill or delete.
3. **#6 the estate hygiene** — the cure exists (`17914536` on p5) and
   needs merging; then add the lint leg so the json doctrine cannot
   regress silently.
4. **#4 the sweep teeth** — the pin is WRITTEN (my worktree
   `tests/test_fv_sweep_lock_teeth_pin.py`, green/red verified); lift it.
5. **#8 the actor-metadata drop** — the guide's own example routes to the
   wrong queue today; `step()` should read the handle.
6. **#11c the join envelope** — a silent type-safety hole at BOTH faces
   (E5's body-is-None skip + the codec's raw-list passthrough).
7. **#7 the coverage floor** — scoped 87% with `_worker_execution` at
   28%; close the residuals then add the scoped gate.
8. **#3 the bands' append-only law** — extend the run-scoped pattern to
   the band artifacts; give the 477 figure a raw capture or mark it
   prose-inherited.
9. **#12b the run-key squat** — the decision sketch above (option A).
10. **#5 the ungated corpora** — add `attack2_`/`attack3_` to `_CORPUS`.
11. **#11 b/d/e/f/g/h/i the ergonomic batch** — one sweep, decisions
    attached per item above.

---

## The deliverables (uncommitted, in /tmp/opencode/wt-fv-validate/tests/)

- `tests/test_fv_redlog_guard.py` — THE REDLOG LAW (machine-generated or
  it doesn't exist): AST-guards every `.red()` payload against
  hand-constructed literals; bites on 11 live sites; self-bite test
  green.
- `tests/test_fv_sweep_lock_teeth_pin.py` — THE SWEEP LOCK-FIRST TEETH:
  a child lock held across a sweep pass is skipped-then-healed; red on
  the no-SKIP-LOCKED mutant (drill-verified both directions).
