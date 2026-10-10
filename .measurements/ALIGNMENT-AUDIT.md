# ALIGNMENT AUDIT — feat/taskqflow-p3: did the proofs and the specs reach the built tree?

**Auditor:** the ALIGNMENT AUDITOR (independent; read-only on the source).
**Head audited:** `55faa1f2` ("chore(measurements): the phase-3 corpus's working-run artifacts") —
**the phase-3 fixer was committing while I read.** The working tree at audit time carried 8
UNCOMMITTED files: `tests/attack3-{hitl,loop,validate}.py`, `tests/typeprobe/attack3_wf_negative_types.py`,
`.measurements/attack3/` (incl. `PHASE3-ATTACK-REPORT.md`, verdict **CERTIFICATION REFUSED**),
`.measurements/final-verify.txt` (55 passed), and 2 modified redlog/band jsons. The attack-3 corpus
is the red team's verdict on the SAME head I audited; where it convicts, I cite it — those findings
are being fixed in-flight by the fixer, and my counts mark them `MISSING (attack-found, fix in flight)`.

**Method:** static verification only — every mechanism/pin/number below was grepped in
`src/taskq/workflows/`, `src/taskq/migrations/`, the `tests/` pin files, the docs, and the captured
`.measurements/` corpus at the audited head. No test runs were made (the rules reserve those for my
own detached worktree; the in-tree captured battery + the attack report serve as the run evidence).
Numbers are read from the tree's own captured files, never asserted.

---

## 1. THE ALIGNMENT MATRIX — PROOF CORPUS BY PROOF CORPUS

Verdicts: **ALIGNED** (mechanism + pin present, number re-measured or band pinned) ·
**MISSING** (the proof proved it, the build lacks it — a finding) ·
**SUPERSEDED** (the build deliberately does it differently, on record) ·
**BACKLOG** (the proof's owner is a phase-4/5 ticket, verified not built — not a defect of phases 1-3).

### 1.1 P3 — the composition dragons (`/tmp/opencode/proto3/DRAGON-MATRIX.md`)

| # | Proven item | Verdict | Evidence in the built tree |
|---|---|---|---|
| 1a | **PAUSED** — a held iteration's budget is invisible to the sweep (`AND NOT budget_paused`); signal timeout strictly first; resume = `now()+remaining` | **ALIGNED** | migration `01.00.26_01` (the 3 budget columns, DB-clock comments); `api/_sql_loop.py:131` (`AND NOT j.budget_paused`, both sweep + remaining arms); `LOOP_REMAINING_SQL` computes from `clock_timestamp()`; pins `test_paused_loop_invisible_to_the_budget_sweep`, `test_hold_inside_a_loop_pauses_the_budget_and_completes` (T19×T10, 6 checks) |
| 1a′ | **CONSUME-BUDGET dragon kept red forever** | **ALIGNED** | `test_consume_budget_dragon_red_forever` + the captured mutation drill (`.measurements/t19-mutation-drill-red.txt` per the report; `t19-pin-reds.json`) |
| 1b | Step-key positional, attempt ordinal at claim, ledger PK = the exactly-once boundary | **ALIGNED** | `01.00.23_03` `wf_step_ledger_claim_uniq` keys `(flow_id, step_key, COALESCE(map_index,-1), attempt)`; `test_pin_1_double_run_red_and_memoized_green`, `test_pin_3_concurrent_step_claims_one_winner`, `test_pin_7_map_children_own_ledger_rows`, `attack_wf_map_ledger_pk.py` (the phase-1 revert drill) |
| 1c | Carry frozen at spawn, advanced exactly once, in the terminal tx | **ALIGNED** | `api/_loop.py` (frozen-at-spawn), `docs/guides/workflows.md` §9 ("advanced EXACTLY ONCE per iteration — in the ADVANCE STATEMENT"); `test_carry_advanced_exactly_once_per_iteration` |
| 2a | Cancel is flow-linearized, ONE transaction; the three legs (fire guard, dispatch fence, finalize fence) | **ALIGNED** | `test_cancel_workflow_one_transaction_idempotent`; `test_pin_2_dispatch_fence_refuses_post_cancel_claim`; `test_pin_5_sweep_fire_refuses_post_cancel`; the `_wf_dispatch_fence` fragment baked into the shipped claim template (phase-1 B2, revert-drilled) |
| 2b | Cancel vs a held signal — the late deliver REFUSED, no zombie wake | **ALIGNED** | `cancel_run_signals` (`_hitl.py`); `test_late_deliver_after_cancel_is_refused_no_zombie_wake` |
| 2c | Cancel vs a loop mid-iteration — no next iteration, the loop never advances | **ALIGNED** | the cancel TX's fenced finalize (`test_pin_24_finalize_on_cancelled_flow_never_decrements`); the advance lives in the finalize tx that loses the CAS |
| 2d | Decrement + fire + terminal = one transaction, both orders, no resurrection | **ALIGNED** | P1 FINAL's two-tx shape: `engine.py` tx1 (result-write + terminal-mark fence) / tx2 (decrement + fire + outbox), `_deadlock_retry` at both; `wf_join_fire UNIQUE(join_job_id)` (`01.00.23_02`); `test_pin_6_duplicate_finalizes_one_decrement`, `test_pin_23_tx2_decrement_guard_deps_never_negative` |
| 3 | Maybe + retry: ladder retries emit NO terminal; the collector fires strictly after exhaustion; skip fans in with zero ledger rows | **ALIGNED** | `test_t06_pin5_mid_ladder_emits_no_cascade_and_no_decrement`; `test_transient_ladder_emits_no_terminal_until_exhaustion`; `test_t06_pin4_skip_fans_in_zero_ledger_rows` |
| 4 | The finalize CAS is the shape guard — reclaim and finalize mutually exclusive on `status='running'` | **ALIGNED** | `_sql_finalize.py:38` (`AND attempt = $9` — H8's fencing token); `test_pin_13_attempt_fence` |
| 4′ | The split-finalize dragon kept red | **ALIGNED** | `_sql_finalize.py:83` ("the unfenced variant stays RED forever"); the redlog pins; the phase-1 certifier's own revert drills |
| rule 7 | The ladder's parent-visible terminal only at exhaustion | **ALIGNED** | see row 3; plus `test_retry_classifier_routes_by_kind` |

**P3: 12 ALIGNED / 0 MISSING / 0 SUPERSEDED.**

### 1.2 The hardening matrix (`/tmp/opencode/hardening/READINESS.md` — H1-H9, D1-D7)

| # | Proven item | Verdict | Evidence |
|---|---|---|---|
| H1 | Fenced attempts recorded `outcome='fenced'` — never a phantom running row | **ALIGNED** | `_sql_ledger.py:104,118` (`SET status='fenced'`); `test_pin_15_phantom_running_reaped` |
| H2/H2b | The cancel TX fences every in-flight ledger row of the flow (peers included) | **ALIGNED** | the cancel's ledger-fence statements (`_sql_ledger.py` — both guard `status='running'`); the 7-state cancel matrix's pins |
| H5 | `claimed`/`ladder_retry` ON the event stream — completeness is a tested property | **ALIGNED** | `test_wf_state_event_totality.py` (the totality generator; the 104-pair table's port) |
| H6 | Claim + ledger insert = ONE transaction | **ALIGNED** | `test_pin_5_ledger_claim_atomic` |
| H7 | Deadlock retry on both operators, linearization-preserving | **ALIGNED** | `engine.py::_deadlock_retry` (real `DeadlockDetectedError`, budget + loud failure); `test_pin_14_deadlock_retry_budget` |
| H8 | The attempt IS the fencing token in the finalize CAS | **ALIGNED** | `_sql_finalize.py:38`; `test_pin_13_attempt_fence` |
| H9 | The ledger terminal write is IN the finalize tx | **ALIGNED** | `test_pin_6_ledger_terminal_atomic`; tx1 in `engine.py` |
| H3/D7 | The budget-kill's child-kill + the iteration-finalize CAS unified in the sweep's tx when ported | **SUPERSEDED (under-documented)** | The tree fences the killed iteration's ledger row via T04 pin 15's PHANTOM-RUNNING reaper arm (eventual, a separate pass) — NOT the same-tx unification T19's ticket demanded ("the port unifies them rather than inheriting the seam"). The round-6 fold (`00-INDEX`) re-homed H1-H4 onto pin 15, so the divergence has a paper trail OUT of the tree; the in-tree record does not state the unification was declined. LOW: the capability (no phantom rows forever) is present; the timing guarantee differs silently. |
| D1 | Dispatch resolves bodies from the REGISTERED definition only; cross-process resolution via the stamped definition name | **ALIGNED** | `test_pin_16_body_from_definition`; `test_dispatch_resolves_bodies_from_the_definition_registry`; the round-2 cure (flow root stamped `metadata.workflow`, registry-as-truth/memo-as-cache, the dead `flow:{id}` fallback deleted); `test_attack_sweep_fire_skips_the_reducer_body` (fresh-interpreter subprocess) |
| D2 | The (ts, seq) display order deliberately picked + documented | **BACKLOG** | Home is T11 (the debug-view ticket) — not built; nothing in `docs/guides/workflows.md` picks the order yet. Correct deferral; verify it lands WITH T11. |
| D3 | Per-checker exhaustiveness recipes pinned; both checkers in CI | **ALIGNED** | `tests/typeprobe/_gate.py` (fails unless every MUST_ERROR reds on EACH checker); the `type-probes` CI job (verified by the phase-1 certifier's own F9 drill); `pyrightconfig.json` re-arms `reportMatchNotExhaustive`; `docs/guides/workflows.md:372` cites both pinned versions |
| D4 | The two-source reconstruction rule (ledger for attempted terminals; node error row for never-granted) | **ALIGNED** | `test_t08_reconstruction_two_sources_rows_only` |
| D5 | The JSONB size cap + compaction (the ONE home: T18) | **ALIGNED** | `test_t18_pin4_unbounded_jsonb_bounded_collect_row` |
| D6 | The peer-cascade fires only on TERMINAL failure — the docs sentence | **ALIGNED** | `docs/guides/workflows.md` "THE D6 SENTENCE" (:147) |

**Hardening: 13 ALIGNED / 0 MISSING / 1 SUPERSEDED (+2 deferred to backlog, on record).**

### 1.3 The HITL proof (`/tmp/opencode/hitl-proof/PROOF.md` — the four layers)

| # | Proven item | Verdict | Evidence |
|---|---|---|---|
| L1 type door | `TypedGate` binds the type ONCE; the wrong model not assignable; the classic `(type[P],P)` signature dead | **ALIGNED (type level)** | `_app.py::TypedGate`/`SignalChannel.gate` (the bound-door form, verbatim Package B); the MUST_ERROR corpus red on BOTH checkers (`.measurements/t09-typegate.txt`, attack-3's 4 new markers red on both) |
| L1(b) | **RUNTIME validation at the send boundary** — the smuggled `model_construct(verdict=42)` payload REFUSED with the named pydantic error, the hold SURVIVES, no ladder burn | **MISSING (HIGH — attack-found B2/H2, fix in flight)** | There is NO runtime payload validation anywhere on the deliver path: `HitlClient.resolve(hold_id, decision: dict)` and `deliver_payload` insert the JSONB raw; the CAS consumes the hold; the body's `_coerce` (`_runner.py:782`) raises inside the BODY → ladders the node — the operator's mistake kills the flow, the hold does NOT survive. The attack's `test_a3_union_wait_narrows_to_the_first_model` + `A3-H5` red this. The docstring promises exactly the proven shape ("the hold SURVIVES on the stale arm") — the promise is false at the audited head. |
| L1(c) | The resume receives the TYPED payload, narrows | **MISSING (HIGH — attack-found B2, fix in flight)** | `_coerce_signal` iterates the declared models IN ORDER and returns the first that validates — a lenient first member swallows a strict second member's delivery (wrong type, data dropped). The union-narrowing promise breaks at the answer queue. |
| L1(d) | A validating-but-semantic-breaking payload enters the LADDER, not a stranded hold | **ALIGNED** | the body's exception → ladder (the built behavior matches); `test_resume_does_not_burn_the_retry_ladder` covers the other half |
| L2 PAUSED | `budget_sweep` returns `[]` while held; forced-past deadline cannot fire | **ALIGNED** | `test_paused_loop_invisible_to_the_budget_sweep` + the T19×T10 composition pin |
| L2 timeout | The deadline passes → the NAMED typed failure (`SignalTimeoutError`) raises in the body; the node fails by POLICY | **MISSING (BLOCKER — attack-found B1, fix in flight)** | `SignalTimeoutError`/`SignalAbandonedError` exist as classes in `exceptions.py` (the glossary shape ✓) but have NO raise site: the wait site checks only `status='held'`, so after the expiry sweep marks `abandoned` the body re-executes and MINTS A NEW HOLD — hold→expire→re-hold→∞. `GateDecl.on_timeout` is read by NOTHING. The pin `test_signal_timeout_fires_on_db_clock` asserts only the abandonment; its docstring claims the face it never exercises (a vacuous claim, real assertion — the attack's verdict). |
| L2 cancel both orders | Late deliver refused; deliver→cancel fenced; the racy arm consistent | **ALIGNED** | `test_late_deliver_after_cancel_is_refused_no_zombie_wake`; the cancel one-tx pin |
| L2 abandon | The operator abandon = the DEFINED `abandoned` state, never a silent orphan | **ALIGNED (half)** | `sweep_expired_signals` → `abandoned` (`test` :404-420); the resume face is B1's missing raise site |
| L2 duplicate delivers | Two concurrent delivers → exactly ONE delivered + one `resumed` event | **ALIGNED (result level)** / **MISSING (audit leg — attack-found H4, fix in flight)** | the CAS is real and pinned; but the audit row + knock fire in the GUARD tx (still `'held'`) — two concurrent resolves write TWO `hitl.resolve` audit rows for ONE resolution (A3-H2: `2 == 1`). "Who resolved this is a ROW" holds; "exactly one row" does not. |
| L3 | Admin UI live: held node distinct, panel shows waiting-on/deadline/schema, refuse inline, drive the resume | **BACKLOG** | T11's page — verified absent (`src/taskq/web/admin/` has no workflow surface). Correct deferral. |
| L4 | One run = one trace from rows alone; rows == DOM; monotonic ts/seq | **ALIGNED (rows half)** | `test_t08_reconstruction_two_sources_rows_only`, `test_t08_one_run_one_trace_concurrent`, `test_t08_g7_teeth_the_lying_fixture_reds`; the DOM half rides T11 |
| — | The hold-id reply handle + the context contract + multi-workflow never-cross | **ALIGNED** | `test_hold_id_reply_handle_and_context_contract`; `HitlClient.{list,get,resolve}` (`_hitl.py:383-416`); uuid7 ids via the seam (`01.00.27` header) |
| — | **THE REDACT LAW extends to the enumeration surface** (canary in tool args reaches neither list nor knock) | **MISSING (HIGH — attack-found H3, fix in flight)** | `wait_signal(reason=, tool=, args=…)` persists the context RAW into `wf_signals.payload`; `HitlClient` redacts only at READ time and only when the constructor was HANDED a hook — nothing wires the workflow's own redact hook into the client. A3-H3: the canary `sk-canary-a3-4f9d` reached `client.list()` verbatim on the default surface. The in-tree test only proves the hook-constructed client — the default surface (the law's subject) is untested and leaks. |
| — | The pubsub knock: pointer-only, the row is the truth, a missed knock converges by polling | **ALIGNED** | `test_pubsub_knock_is_a_pointer_and_the_consumer_converges`; the attack's A3-H4 confirms pointer-only green |
| — | The timer-policy matrix — each `on_timeout` value's ledger record named (`fail` / `resume_with_default` / `escalate`) | **MISSING (MEDIUM)** | Only `on_timeout="fail"` exists, and (B1) it does not fire. `resume_with_default` / `escalate` appear in ONE docstring (`_hitl.py:333` — "the … policies' records land with their bodies' reads"), are implemented nowhere, pinned nowhere, and the phase-3 report's unspecifications do not record the deferral. An undocumented divergence. |

**HITL: 10 ALIGNED / 5 MISSING (4 attack-found, fix in flight; 1 auditor-found) / 0 SUPERSEDED (+1 backlog).**

### 1.4 The agent-loop proof (`/tmp/opencode/agent-loop-proof/VERDICT.md` — verdicts A-F/M + cuts 1-10)

| # | Proven item | Verdict | Evidence |
|---|---|---|---|
| verdict B/D | The carry frozen-then-advanced-once; the planner once per ITERATION; the cap = exactly `max_iterations` spawns + the NAMED `iteration_cap_exhausted`; PAUSED under skew | **ALIGNED (with one vacuous leg — attack-found)** | `test_iteration_cap_terminated_by_the_advance_guard_named_state`, `test_carry_advanced_exactly_once_per_iteration`, `test_budget_fires_on_db_clock_under_app_clock_skew`. The attack's vacuous-pin audit: the SWEEP's cap predicate is unreachable in production (the advance guard refuses at `i+1 >= max`, so the metadata's iteration tops out at `max-1` — the pin hand-crafts the trigger state). The driver-side guard owns the wall; the sweep arm is dead code as shipped. MEDIUM (attack-found, fix in flight). |
| verdict C | Crash durability: kills at 3 windows + the kill storm heal, zero double-executed steps | **ALIGNED (mechanism) / BACKLOG (the re-run at scale)** | the reclaim/fence machinery + pins are in; the storm re-run on the real implementation is T15's cell (not built — per plan) |
| verdict E | Per-tool payload models; the wrong-gate delivery reds on both checkers; the `(type[P],P)` dragon pinned GREEN-on-both as the deliberate control; `assert_never` for the carry union | **ALIGNED (type level)** | the typeprobe corpus + the attack's 4 new markers red on both; `Done`/`Refine` consumed with the residual machinery |
| verdict M | Multi-hold: same gate twice (epochs); the single-use variant red TWO ways; wrong-gate refusal with the hold surviving | **ALIGNED** | `test_second_hold_new_epoch_clean_and_stale_payload_refused`; `01.00.27`'s partial-unique-on-epoch identity + the stale-payload dragon named in the migration header |
| cut 1 | The ledger close rides the finalize's tx | **ALIGNED** | H9 / tx1 (above) |
| cut 2 | **LADDER-ROUTES-BY-FAILURE-CLASS — "infra fault ≠ body failure"** | **MISSING (HIGH — attack-found H5, fix in flight)** | The mechanism exists (`_is_infra_fault` → reclaim, `test_infra_fault_routes_to_reclaim_never_the_ladder`) — but the shipped classifier treats ANY body-raised `ConnectionError` as reclaim-eligible: a body-controlled `ConnectionError` wedges the loop forever (20 runs, 20 `crashed` rows, flow `running`, no ladder, no wall — A3-L3). The proof's red (storm kills cascade) is closed; a NEW dragon the proof never tested is open. |
| cut 3 | The hold EPOCH + `(gate, call_id)` identity; the stale-payload dragon killed | **ALIGNED** | see verdict M + the migration header's ONE-SENTENCE identity |
| cut 4 | `max_iterations` — the cap guard rides the advance statement, atomic | **ALIGNED** | `test_iteration_cap_terminated_by_the_advance_guard_named_state` |
| cut 5 | STRANDED-FLOW — a failed/exhausted loop terminalizes its flow in the same tx | **ALIGNED** | the exhaust arm's flow terminalization (`_sweep.py`, one tx); the pin asserts the root terminal |
| cut 6 | Parallel holds inside one iteration NOT expressible — SPECIFIED, not silent | **ALIGNED** | stated in T19's ticket AND in the shipped docs (`docs/guides/workflows.md` §4: "the chained gate works because the waits are sequential") |
| cut 7 | NAIVE-MEMO: the one-TX memo pinned; the docs say WHY the engine doesn't own the memo | **ALIGNED** | `test_iteration_memo_is_the_one_tx_shape`; `docs/guides/workflows.md` §9 "NAIVE-MEMO IS AUTHOR GUIDANCE, PINNED" with the negative example |
| cut 8 | The type story stops at the body boundary | **SUPERSEDED (on record)** | T17 cut #8's disposition: bodies are annotated, the compile reads the annotations, validate refuses the unannotated actor (`test_unannotated_step_mutation`) — the build went FURTHER than the spike's dict-typed boundary; recorded in `.measurements/t17-dispositions.md` |
| cut 9 | The operator's own latency consumes the budget between holds — named for the docs | **MISSING (LOW)** | Neither `docs/guides/workflows.md` §9 nor §4 states it. The PAUSED semantics are documented; the between-holds tick is not. |
| cut 10 | The vendored-Tailwind runtime-DOM gap | **BACKLOG** | P2/T11's architecture — not built |
| — | `on_exhausted="escalate"` — the escalation enqueue | **MISSING (HIGH — attack-found H1, fix in flight)** | Dead end-to-end at the audited head: (a) only the SWEEP's arm writes the outbox row — the DRIVER's exhaustion (the common live path) never enqueues; (b) `spec.on_exhausted` is read by NOTHING (`fail` ≡ `escalate`); (c) even the sweep's row is a dead letter (binding `{"actor": "loop_escalation"}` names an actor no author registers). A3-L1/A3-L2 red. The build report's unspec #6 claimed this was settled ("v1 rides the SAME outbox") — the claim was not verified by a pin. |

**Agent-loop: 8 ALIGNED / 4 MISSING (all attack-found except cut 9) / 1 SUPERSEDED (+2 backlog).**

### 1.5 The fanout proof (`/tmp/opencode/fanout-proof/READINESS.md` + `PAPER-CUTS.md`)

| # | Proven item | Verdict | Evidence |
|---|---|---|---|
| 1 | The nested heterogeneous fan-out at scale: 1002 nodes / 201 join fires, each exactly once, zero stranded joins | **ALIGNED (mechanism + band)** | the edge-ledger engine + `test_fan_out_tx_band_1000_children` (`.measurements/fanout-1000-tx-band.json` + the timestamped runs beside it — measured 65.8–120.1 ms across the captured runs / band ≤ 500 ms — the G12 gate, chunked parallel-array inserts in `_fork.py`) |
| 2 | COUNTER-AS-CACHE / LEDGER-AS-TRUTH — `remaining = join_target − committed decrements`, NEVER child-row presence | **ALIGNED** | `01.00.23_02`'s `wf_edge` header states the rule verbatim; `test_pin_17_empty_join_never_fires` (EMPTY-JOIN pin 17); the rederive arm re-derives the cache from the ledger |
| 3 | ID-COLLISION: graph-owned ids kill the hand-built-string key ceremony (cut #12) | **ALIGNED** | `test_pin_18_no_id_collision` |
| 4 | Fork atomicity: terminal UPDATE + fork INSERTs in ONE tx; the 3-window crash matrix heals exactly-once; the reconcile is a REPAIR (FORK-DEBT) | **ALIGNED** | `test_pin_19_fork_atomicity` (both the adversarial matrix and the red split-tx variant) |
| 5 | The outbox drain: idempotent `ON CONFLICT` on the step key, the flag flipped in the insert's tx | **ALIGNED** | `test_pin_20_outbox_drain_exactly_once`; `01.00.23_02`'s outbox header |
| 6 | The set-based sweep (the per-join round-trip N+1 shape is the convicted alternative; its MEASURED conviction is P1's sweep-cost curve: 14.9 ms @ 200 joins, the unscoped monster 83.7 ms @ 75k — the earlier "p50 477 ms / p95 1.73 s @ ~340 joins" figure had no capture anywhere and is DELETED — provenance or silence) | **ALIGNED** | the shipped arm IS set-based (`_sql_sweep.py` carries the P1 curve's citation); `perf-evidence-workflows.md` carries the honest re-measurement (the join-fire 9.0–23.0 ms across captures / band ≤ 50 ms; the scale curve 200→14.9, 1000→23.8, 5000→39.3) |
| 7 | Partial retry isolation / MAP-RETRY-BARRIER (N=8, K=3; five assertions; siblings never re-run; mid-ladder = zero decrements) | **ALIGNED** | `test_map_retry_barrier` (`test_wf_scenario_pins.py:220`) |
| 8 | JOIN-FAILURE-POLICY duality (terminality × required/maybe × timeout; the zero-collect; the lying-envelope dragon) | **ALIGNED** | `test_join_failure_policy_*` ×3; the `failure_policy` column on `wf_edge` (`01.00.24_01`) declared at fork time, recorded on the ledger |
| 9 | Failed-parent propagation: fail_closed join blocks + peer-cancel; collect fans in the typed partial | **ALIGNED** | `test_t06_pin1`…`test_t06_pin8` (8 pins, incl. the mixed-policy fence and the crash-heal arm) |
| 10 | The edge-scale bound + the scale curve pin (the honest refit, the named outlier) | **ALIGNED** | `test_edge_join_scale_curve_across_the_bound` + `.measurements/edge-scale-curve.json` + the fan-in bound E6 |
| 11 | Queue placement composes for free; a worker shortage DELAYS joins, never breaks them | **ALIGNED** | no placement concept in the workflow layer (the compile inherits the actor's queue); the docs sentence in T09's scope — verify it lands in T13's example prose (backlog) |
| cut #8 | The two reader code paths for one join concept → the uniform join-input read | **SUPERSEDED (record pending)** | the tickets fold it as a T14 LATER-row candidate; T14 is not built, so the in-tree record does not exist yet. Verify it lands WITH T14. |
| cut #11 | The schema-drift silent TRUNCATE → drop-not-truncate assert in the crash fixtures | **BACKLOG** | T15's fixtures — not built |

**Fanout: 12 ALIGNED / 0 MISSING / 1 SUPERSEDED (record pending) (+2 backlog).**

---

## 2. THE ALIGNMENT MATRIX — THE TICKETS (T03-T19)

| Ticket | Pin(s) | Verdict | Evidence / gap |
|---|---|---|---|
| T01 | The probe CI gate (pinned checkers + latest; MUST_ERROR red on EACH) | **ALIGNED** | `.github/workflows/ci.yaml` `type-probes` job → `tests/typeprobe/_gate.py`; the certifier's own can-fail drill (`.measurements/attack/F9-red-type-gate.txt`); 6+4 markers red on pyright 1.1.414 AND ty 0.0.85 (`t09-typegate.txt`, `typegate-attack3-*.txt`). The split/Router kill-evidence FREEZE rides T14 (backlog). |
| T02 | REPORTED — lands nothing; feeds T17 | **ALIGNED** | folded: `.measurements/t17-dispositions.md` triages the cuts — THE ONE HOME for the counts; read the totals there (its line is derived from its own table; no second arithmetic here) |
| T03 | The 5 forever-pins (row-width ≤ 8 B / partial ≤ 1 % / import law / enqueue band / dispatch band) + `code_version` + hold-via-`scheduled_at` + the dispatch exclusion + uuid7 seam | **ALIGNED** | `pin1-row-width.json` (7.37/8 B — re-measured, not assumed), `pin2-partial-index.json`, `test_import_law_taskq_never_imports_workflows`, `test_pin_4_enqueue_latency_band`, `test_pin_5_dispatch_claim_band`; `_version.py` + `test_pin_12_canonical_hash` (tors `content_hash`); the hold representation in `_sql_sweep.py`/`engine.py`; the fence fragment in the shipped claim template. R4 (no workflow env vars) honored — no settings rows owed. |
| T04 | Pins 1-24 (two-tx finalize, `join_fire`, lock-first sweep, the H4 restatement, ATTEMPT-FENCE 13, DEADLOCK-RETRY 14, PHANTOM-RUNNING 15, BODY-FROM-DEFINITION 16, EMPTY-JOIN 17, ID-COLLISION 18, FORK-DEBT 19, OUTBOX-FLUSH 20, sweep wiring 21, reaper 22, tx2 rowcount 23, cancel-no-decrement 24, REDACT-BEFORE-PERSIST 11, CANONICAL-HASH 12) | **ALIGNED** | each pin has a named test (see the per-corpus rows above); the trace_id stamp on node rows (K3's one survivor) at `engine.py:149,171` |
| T05 | The step ledger on the live composite index; `ctx.step`; RUN-KEY-REPLAY; LEDGER-CLAIM/TERMINAL-ATOMIC (pins 5-6, the two independent corroborations) | **ALIGNED** | `test_workflows_ledger_pins.py` (8 pins incl. `test_pin_4_run_key_replay_one_run`, `test_pin_5_ledger_claim_atomic`, `test_pin_6_ledger_terminal_atomic`); `context.py::step`; the run-key arbiter `workflow-run:<name>` |
| T06 | Failed-parent propagation — the 8 pins | **ALIGNED** | `test_wf_propagation_pins.py` (8 tests); `FailureInfo` embeds `ErrorInfo` (F9) |
| T07 | The fan-in bound; child-driven fallback; the scale curve; MAP-RETRY-BARRIER; JOIN-FAILURE-POLICY; empty-collect-of-zero | **ALIGNED** | `test_wf_fanin_bound_pins.py` ×4; `test_wf_scenario_pins.py` ×4 |
| T08 | One grouped query; rows-only reconstruction; `wf_progress_nodes_total`; the ABSORBED-FAILURE clause ahead of `failed`; the totality property; one-run-one-trace; the rollup cost gate | **ALIGNED** | `test_wf_status_pins.py` ×8; the metric in `obs/_otel.py:3721` + the prometheus rule (`rules.yaml:426`); the absorbed predicate's teeth (`test_t06_pin8`) |
| T09 | Promise/gather/build/sink; `@wf.actor` (same registry, F3); `wf.validate()` (E1-E6 + W1); Mermaid byte-stable + the shape vocabulary; `client.hitl`; **`Exit[DoneT]` — "SHIPPED"**; **`wf.retry_node` — "SHIPPED"**; the strict-mode visibility pin 9 | **ALIGNED except 2 MISSING** | all of the first group verified (the API surface pins, the validate mutation matrix fully bifurcated, `test_mermaid_golden_byte_stable`, `HitlClient`). **MISSING (HIGH): `Exit` and `wf.retry_node` exist NOWHERE in the built tree** — `grep` over `src/` + `tests/` returns zero; the ticket's terminal-surface ownership section pins both as SHIPPED (F5's commitments), and T12's `flows retry` verb consumes the latter. No supersession record anywhere (the phase-3 report's unspecifications are silent on it). Pin 9 (TASKQ_QUEUES_STRICT boot fail-fast) is PARTIAL — recorded honestly as unspec #7 (the projection carrier pinned; the full worker-boot exercise deferred) — an on-record deferral. |
| T10 | The 13 pins (late-deliver 1, double-send 2, HELD-ROW-EXCLUSIVE 3, finalize-race 4, no-further-dispatch 5, sweep-fire-post-cancel 6, RESUME-NOT-RETRY 7, DELIVER-NO-DROP 8, MULTI-HOLD 9, REPLY-HANDLE 10, CONTEXT-REDACT 11, PUBSUB-CONVERGENCE 12, DB-CLOCK 13) | **ALIGNED except 3 MISSING** | pins 1-10, 12, 13 verified in `test_wf_hitl_pins.py` (9 tests) + the sweep pins. **MISSING:** pin 8's boundary refusal (attack H2/B2 — the smuggled payload is consumed, not refused); pin 11's default-surface law (attack H3 — the canary reaches `list()`); the timer-policy matrix (only `fail`, and it doesn't fire — attack B1). |
| T17 | The cuts dispositioned; the ergonomic contract; the abstraction gate | **ALIGNED with a RECORD-ROT finding (MEDIUM)** | the disposition ledger (`.measurements/t17-dispositions.md` — the ONE home for the counts; the ledger's own correction note carries the fix history) + `test_wf_ergonomics_contract.py` (the repo-wide abstraction grep runs in-suite). **The ledger's "Re-test" column cited ≥7 test node names that DID NOT EXIST in the tree** (`test_wf_api_pins.py`, `test_wf_hitl_client_pins.py`, `test_list_shows_pending_holds`, `test_node_key_dot_root_refused`, `test_skipped_child_fans_in_typed`, `test_delivered_payload_reaches_the_resume`, `test_child_max_attempts_respected`, `test_result_read_decodes_once`) — the underlying capabilities exist under OTHER names (`test_wf_api_surface_pins.py`, `test_wf_hitl_pins.py`, `test_t06_pin4`), but the record pointed agents at red files. The capture law says the record must not lie — the ledger's Re-test column was re-pointed at existing tests (the ledger's own record-rot note). Also note: the ROOT-WITH-DOT refusal (cut #6's naming pin) HAD no test under any name — the refusal is now implemented + pinned (`tests/test_wf_phase3_cure_pins.py::test_wiring_key_with_a_dot_is_refused`). |
| T18 | The 4 pins (expiry-eats-children; pruned-parent-of-a-live-run; over-hold; UNBOUNDED-JSONB) + the index-backed EXPLAIN gate | **ALIGNED** | `test_wf_pruner_pins.py` (pins 1, 2, 3-folded, 4); the retention-coupling docs section |
| T19 | The 9 pins (CONSUME-BUDGET 1, BUDGET-SWEEP-VS-HOLD 2, CARRY-OPTIMISTIC 3, ITERATION-CAP 4, CARRIER-TYPE 5, LADDER-ROUTES 6, STRANDED-FLOW 7, NAIVE-MEMO 8, BUDGET-DB-CLOCK 9) | **ALIGNED except 2 MISSING** | pins 1-4, 6-9 verified (`test_wf_loop_pins.py` ×9 + the mutation drills). **MISSING (MEDIUM): pin 5 CARRIER-TYPE** — `LoopSpec.carry_type` is recorded ("the CARRIER-TYPE check's subject", `_loop.py:103`) but NO validate-time check exists, no MUST_ERROR probe, no test: a body whose `Refine[Foo].feedback` mismatches the declared `carry=Bar` compiles clean. **MISSING (HIGH — attack H1): the escalation arm of pin 4** ("the escalation actor fires") is dead end-to-end (see §1.4). |
| T11, T12, T13, T14, T15, T16 | Admin page; CLI flows; doc-ingest example; the spec doc + errata + kill list; the deploy matrix; the demo workflow | **BACKLOG — verified absent** | no workflow surface in `src/taskq/web/admin/`; no `taskq flows *` in `cli.py`; no example tree in `examples/` (docs prose only); no `docs/design/workflows-spec.md`; no workflow tier in `tests/system_e2e/`; `examples/app.py` has no workflow. These are phases 4-5 by the serial map — verified against reality (the tickets' optimism is not counted as done). |

---

## 3. THE MISSING LIST — RANKED BY RISK

1. **`Exit[DoneT]` + `wf.retry_node` (T09, pinned "SHIPPED") — absent, undocumented.** The only two ticket pins claiming SHIPPED that simply do not exist in the tree. The typed early-exit sentinel is the §17.6 gate's subject; `wf.retry_node` is T12's `flows retry` verb's engine. Risk: a phase-4/5 builder discovers the hole mid-PR, or worse, re-derives a different exit semantics. Severity **HIGH** (auditor-found; not in the attack report's scope).
2. **The HITL runtime boundary is absent end-to-end (attack B1 + B2 + H2, fix in flight).** The proofs' single most load-bearing runtime claim — the smuggled payload REFUSED at the boundary, the hold SURVIVES, the union narrows to the RIGHT member, the timed-out hold raises the TYPED face — is not what ships: any-dict delivers, first-model mis-narrowing, hold→expire→re-hold→∞. Three proof-layer behaviors (L1(b), L1(c), L2-timeout) inverted at the audited head. The captured red probes exist; the fixer's `final-verify.txt` (55 passed) suggests the cure is landing — **the audit of the CURE is owed on the fixer's commit, not this head.** Severity **BLOCKER-class** until re-verified.
3. **`on_exhausted="escalate"` dead end-to-end (attack H1, fix in flight).** The phase-3 report claimed it settled; the attack proved the driver path never enqueues, the flag is read by nothing, and the binding names an unregistered actor. Severity **HIGH**.
4. **The REDACT-BEFORE-PERSIST law does not reach T10's context row (attack H3, fix in flight).** A canary in tool args reaches `client.list()` verbatim on the default surface. Breaks the composition verdict TORS-REV-0.16 §G1's extension pin (T10 pin 11). Severity **HIGH**.
5. **The resolve audit is not exactly-once (attack H4, fix in flight).** Two concurrent resolves → two audit rows + two knocks for one resolution. "Who resolved this is a ROW" holds; the count lies. Severity **MEDIUM-HIGH**.
6. **The ladder-routing classifier over-broad (attack H5, fix in flight).** A body-raised `ConnectionError` wedges the loop forever — cut 2's own dragon resurrected through the classifier's other side. Severity **MEDIUM-HIGH**.
7. **T19 pin 5 CARRIER-TYPE — no check, no probe (auditor-found).** `carry_type` recorded, never enforced. Severity **MEDIUM**.
8. **The timer-policy matrix arms (`resume_with_default` / `escalate`) unimplemented, unpinned, unrecorded (auditor-found).** Severity **MEDIUM**.
9. **The T17 disposition ledger's re-test pointers cite nonexistent test names (auditor-found).** The record-rot finding; the underlying capabilities exist under other names. Severity **MEDIUM** (the capture law: the record must not lie).
10. **The vacuous pin legs the attack convicted (T19's sweep-cap predicate unreachable in production; T10 pin 12's knock shape pinned by nothing; T10 pin 13's claimed face never exercised).** Severity **MEDIUM** — the pins pass; three of them cannot fail.
11. **Agent-loop cut #9 — the between-holds budget tick unnamed in the docs (auditor-found).** Severity **LOW**.
12. **H3/D7's same-tx sweep unification silently replaced by the phantom-reaper arm (auditor-found).** Superceded on paper (the round-6 fold), silent in-tree. Severity **LOW**.

## 4. THE PHASE-4/5 BACKLOG — AS VERIFIED AGAINST REALITY

Not the tickets' optimism — what is ACTUALLY absent from the tree at `55faa1f2`:

- **T11** (admin workflow page + the five dragons as pins + D2's (ts,seq) + the P2 DOM golden's port + cut #10's Tailwind gap): not started — `src/taskq/web/admin/` has no workflow surface.
- **T12** (`taskq flows status/signal/cancel/retry/list`, the one state-taxonomy enum, flows→taskq-explain): not started — `cli.py` has no flows verbs; depends on `Exit`/`retry_node` existing first (see Missing #1).
- **T13** (the doc-ingest example, nine shapes incl. the loop + the cron-fired nightly refresh, the zero-warning budget): docs PROSE exists (`docs/guides/workflows.md` uses `doc_ingest` throughout — the abstraction contract holds in-tree, zero `capex`/`cbre` hits); the EXECUTABLE example tree does not.
- **T14** (`docs/design/workflows-spec.md` + the errata + the kill list + the LATER rows incl. fanout cut #8 + the split-kill corpus freeze): not started — the kill list currently lives ONLY in `/tmp/opencode/dag-research/` (out of tree).
- **T15** (the §22 deploy matrix over in-flight runs, the chaos Mermaid golden at fleet scale, the kill-storm re-run, the drop-not-truncate fixtures): the estate's `tests/system_e2e/` exists; the workflow tier does not.
- **T16** (the demo workflow: the failing child + collect + the budget-capped loop with the held approval, live in `examples/app.py`): not started — `examples/app.py` is workflow-free.
- **T01's residue:** the split/Router kill-evidence freeze rides T14 (the probe gate itself is landed and certified).

## 5. THE COUNTS

Across **86 matrix items** (12 P3 + 16 hardening + 15 HITL + 14 agent-loop + 14 fanout + 15 ticket rows):

| Verdict | Count |
|---|---|
| **ALIGNED** | **63** |
| **MISSING** | **12** (7 attack-found with fixes in flight; 5 auditor-found) |
| **SUPERSEDED** | **3** (2 on record: cut #8 via the T17 ledger + the round-6 fold; 1 under-documented: H3/D7) |
| **BACKLOG** (verified absent, owned by phase-4/5 tickets — by plan) | **8** |

**TOP MISSING:** the HITL runtime boundary (B1/B2/H2 — the proofs' core runtime claims), the escalation arm (H1), the redact-law extension (H3), and `Exit`/`wf.retry_node` — the last being the only MISSING item NOBODY (builder, certifier, red team) had on the record before this audit.

**The honest summary:** the phase-1/2 core's proofs survived adversarial certification essentially intact — every P3 dragon, every hardening fence, every fanout pin has a live home in the tree with a named test. The phase-3 surface (the API, the loop, HITL) is where proven capability was dropped in porting: the type-level story landed completely, the runtime story landed partially, and four runtime behaviors the proofs demonstrated — the boundary refusal, the timeout face, the escalation enqueue, the redact extension — did not make the crossing. The red team caught four of them at this same head; the fixer's cure is in flight and must be re-audited on its own commit. One gap (`Exit`/`retry_node`) was on nobody's list.

— the ALIGNMENT AUDITOR, 2026-10-07, against `feat/taskqflow-p3` @ `55faa1f2` (+8 uncommitted fixer files noted above)
