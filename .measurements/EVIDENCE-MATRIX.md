# EVIDENCE-MATRIX — the maintainer's 18 capabilities × 6 evidence kinds

Auditor: the evidence-matrix auditor (alone; read + run, NO fixing).
Heads recorded (METHOD's trees):

| tree | branch | head |
| --- | --- | --- |
| `/tmp/opencode/wt-taskqflow` (the consolidated head) | `feat/taskqflow` | `b7774945` (`b77749455009b33f5b0d247b4806d9d006fe9998`) |
| `/tmp/opencode/wt-taskqflow-p4` (the admin/CLI/examples lane) | `feat/taskqflow-p4` | `7a082c15` (`7a082c1549e5d825cc9282cd8ec6247842b6d538`) |
| (observed en route) `/tmp/opencode/wt-taskqflow-p5` | `feat/taskqflow-p5` | `17914536` — **contains both heads** (`b7774945` and `7a082c15` are ancestors); the true tip is p5, one estate-hygiene cure beyond the consolidated head |

Column law: **TESTED** = a test exists and was RUN by this audit (green, captured below). **PROVEN** = a pin whose red was a REAL mutation with a machine-generated receipt in `.measurements/` (the receipts law). **VERIFIED** = an independent certifier/attacker/red-team round names it. **DEMOED** = a runnable demo shows it (run by this audit where cheap). **DOCS** = the guide section. **TYPED** = the signature + the negative probe.

---

## RUNS CAPTURED (this audit, own PG :5716 `taskq-evidence-matrix-pg`)

| run | tree | what | result |
| --- | --- | --- | --- |
| wf pins battery A (56 tests: runner, propagation, fork, fanin, hitl, loop, status, scenario) | wt-taskqflow | pytest, `TASKQ_TEST_PG_DSN=…:5716/taskq` | **56 passed** in 18.12s |
| wf pins battery B (109 tests: t20 emit/router/fence, progress ×3, engine units, validate, ctx annotations, api surface, ledger, state totality, sweep, finalize) | wt-taskqflow | same | **109 passed, 1 skipped** (pyright binary absent — the typeprobe dep group owns it) in 51.92s |
| wf pins battery C (48 tests: phase3 cure, tolerance, pruner, schema migration, status property, ergonomics, maintain-liveness) | wt-taskqflow | same | **48 passed** in 16.13s |
| phase-4 demo/e2e battery (doc_ingest example full-run ×3 rounds + cron idempotency + ctx contract + CLI pins + the admin workflows page) | wt-taskqflow-p4 | same DSN | **42 passed** in 10.07s |
| ecosystem probes 1–7 | wt-taskqflow | `PROBE_DSN=…:5701/taskq` against the standing `taskq-ecopatterns-pg` | **7/7 PASS** (records appended to `/tmp/opencode/ecopatterns-probes/run.jsonl`) |

Prior machine captures this audit READ (not re-run): `.measurements/int2-e2e-doc-ingest-20261008T051301Z.txt` (the composed demo e2e: report envelope, poisoned-doc row, progress line, 13-node explorer read, stream ring), `.measurements/attack4-fix/cert2-*.txt` (the phase-4 recertifier's captures, on p4), `.measurements/patterns/probe-run-ecosystem.txt`.

---

## THE GRID

Artifacts are relative to the tree named (`head` = the consolidated head unless `p4:`).

### 1. DI / dependency injection — the body params' resolution

**The mechanism, named honestly:** workflow bodies have TWO resolution paths and NEITHER is the estate's provider-scope DI. (a) the `ctx` first parameter — the `StepContext` built at the claim seam (`src/taskq/workflows/api/_ctx.py::build_step_context`, the ONE factory; both the step path and the loop path build the same field set through it); (b) declared data args — values wired into `step(fn, data, key=…)` at compile, carried on the node row's jsonb payload, decoded ONCE by the codec (`api/_runner_codec.py`), re-validated at the pydantic boundary (the STEP door coerces). Stringified annotations resolve through `api/_hints.py::body_hints` (module globals → closure cells → unresolvable = skip, zero false positives). The estate's provider scopes (`docs/guides/dependency-injection.md`: `register_value/factory/class`, loop/job scopes, `validate()`) remain the ACTOR surface only — no provider injection into workflow bodies; the honest boundary is the guide's "two faces" section.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_ctx_annotation_pins.py` (4, ran); `tests/test_wf_validate_pins.py::test_e4_does_not_convict_a_fully_annotated_body` / `::test_e4_still_convicts_the_genuinely_unannotated` (ran); `tests/test_wf_runner_pins.py::test_dispatch_resolves_bodies_from_the_definition_registry` (ran) |
| PROVEN | `p4:.measurements/pin-reds.json` entry `pin16-body-from-definition` (the per-call body map mutation → the CAS loser ran the wrong body, `{"loser_body": "MUTATED"}`) |
| VERIFIED | `.measurements/PHASE1-CERTIFICATION.md` (the certifier's vacuous-replacement drills incl. the definition-registry path; round 2 CERTIFIED); `p4:.measurements/attack4-fix/PHASE4-CERT2.md` |
| DEMOED | `p4:examples/workflows.py` (data-arg wiring across three queues), driven by `p4:tests/test_doc_ingest_example.py` full-run — ran green on :5716 |
| DOCS | `docs/guides/dependency-injection.md` (the actor surface); `docs/guides/workflows.md` §2 "The type contract" + "THE TWO FACES" |
| TYPED | `tests/typeprobe/wf_api_negative_types.py` (promise-where-data-wanted MUST_ERROR with asserted rule ids); `_hints.py` seam |

### 2. Flow INPUTS — the run's input typing/validation

The input rides the root row's payload (`create_flow(input=…)` → `_insert_root`, cut #7), read back as `ctx.input`.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_runner_pins.py::test_create_flow_carries_input_and_drives` (ran); `p4:tests/test_doc_ingest_example.py::test_full_run_the_cron_slot_key_is_run_level_idempotent` (ran) |
| PROVEN | `.measurements/ledger-pin-reds.json` `pin4-run-key-replay` (the arbiter's scope reverted → cross-flow key collision, per-run receipts ×many) |
| VERIFIED | `.measurements/patterns/probe-run-ecosystem.txt` probe_01 (`input_on_row: true`) — re-run by this audit, PASS |
| DEMOED | the doc-ingest demo's `IngestBatch` input (the e2e capture + the test run) |
| DOCS | `docs/guides/workflows.md` §1; `p4:` the context-contract table's `ctx.input` row |
| TYPED | **EMPTY (the honest gap):** `create_flow`'s signature is `input: object = None` (`api/_runner.py:154`) — the input is NOT validated against any model at the create boundary; typing is by-convention via the body's annotations. No negative probe exists that a wrong-typed input is refused. The fix lane owes either a typed input door or a docs sentence stating the convention |

### 3. Flow OUTPUTS — the run's result envelope

`FlowRunner.result(flow_id)` (`api/_runner.py:899`): the terminal node's result, decoded once, `None` before the terminal; `Exit[T]`/`Done` sentinels for early exits.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_runner_pins.py::test_flow_result_read_decodes_once` (ran); `p4:tests/test_doc_ingest_example.py` (the typed `PublishReport` terminal asserted + the root row must read `succeeded`) — ran |
| PROVEN | EMPTY — no red receipt in `.measurements/` names a mutation of the result codec/terminal read (`encode_result`/`decode_result`/`TERMINAL_RESULT_SQL_TEMPLATE` never mutated). The fixer lane owes one red (e.g. the `{"value": …}` wrapper dropped → the envelope-key collision unconvicted) |
| VERIFIED | probes 1 + 7 (`Done` payload decoded, terminal) — re-run, PASS |
| DEMOED | the demo's report envelope (`{'failed_chains': 1, 'total': 6}` + per-trace rows) in `.measurements/int2-e2e-doc-ingest-20261008T051301Z.txt` |
| DOCS | `docs/guides/workflows.md` §1–§2; "the partial-success run (the envelope must not lie)" (§831) |
| TYPED | `tests/typeprobe/wf_api_negative_types.py::probe_exit_bare_return` (the `Exit[T]` sentinel's return position — a bare return reds) + `tests/typeprobe/attack3_wf_negative_types.py` |

### 4. THE RUN CONTEXT — `ctx.step`'s fields (the runtime contract)

`StepContext` (`api/_ctx.py`): `flow_id, job_id, node_key, attempt, input, map_index, claim_epoch, hold_epoch, budget_remaining_ms, queue, flow_name, claimed_at` — one factory (`build_step_context`), the step + loop paths never drift.

| kind | artifact |
| --- | --- |
| TESTED | `p4:tests/test_wf_context_contract.py` (the contract pins — ran, green); `tests/test_wf_ctx_annotation_pins.py::test_the_typed_ctx_type_checks` (ran) |
| PROVEN | `p4:.measurements/pin-reds.json` `pin13-attempt-fence` (finalize CAS without the attempt/epoch conjunct → `{"zombie_attempt": 1, …}`) — the epoch field's teeth; the emit cursor-fence reds (t20-emit-zombie) pin `claim_epoch`'s use |
| VERIFIED | `p4:.measurements/attack4-fix/PHASE4-CERT2.md` (the recertifier's drills) |
| DEMOED | the demo bodies assert on ctx (`page 1` / `screening doc-N` progress from a live run — the e2e capture's explorer read) |
| DOCS | **EMPTY on the consolidated head** — the "The context contract (what a body asserts on)" field table exists ONLY in `p4:docs/guides/workflows.md` (post-`7a082c15`). The merge lane owes the section to the consolidated head |
| TYPED | `tests/test_wf_ctx_annotation_pins.py::test_no_ctx_any_in_the_workflow_docs` + `::test_docs_workflow_bodies_declare_the_typed_ctx` (ran) |

### 5. THE RUN ATTEMPT — the attempt ordinal/epoch semantics

`ctx.attempt` (the ladder's count; holds do NOT advance it — resume-not-retry), the ledger key `(flow_id, step_key, COALESCE(map_index,-1), attempt)`, the finalize CAS's `worker + attempt + claim-epoch` fencing token, `hold_epoch` (the second hold = a NEW clean epoch, stale payloads refused).

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_finalize_pins.py::pin_13_attempt_fence` (ran); `tests/test_wf_hitl_pins.py::test_second_hold_new_epoch_clean_and_stale_payload_refused` + `::test_resume_does_not_burn_the_retry_ladder` (ran); `tests/test_attempt_ceiling_full_cycle_pg.py`, `tests/test_attempt_due_at.py` |
| PROVEN | `p4:.measurements/pin-reds.json` `pin13-attempt-fence` (the epoch conjunct dropped → the zombie attempt finalizes); `.measurements/ledger-pin-reds.json` pin receipts key the attempt in the arbiter |
| VERIFIED | `.measurements/PHASE1-CERTIFICATION.md` (the certifier's B1 live-revert drill on the ledger PK/arbiter); `p4:PHASE4-CERT2.md` |
| DEMOED | the demo's `doc-doomed`: attempt 1 fails through the ladder, attempt 2 takes the success path, siblings never re-run (the e2e capture + test run) |
| DOCS | `p4:docs/guides/workflows.md` context-contract table (`ctx.attempt` — "holds do not advance it"); `docs/guides/retries.md` (the ladder); §4 (resume-not-retry) |
| TYPED | `attempt: int` / `claim_epoch: int` / `hold_epoch: int` on `StepContext` (`api/_ctx.py:96–124`); negative surface via the ctx annotation pins |

### 6. FAILURES — the taxonomy + the propagation

`ErrorInfo` (the estate envelope, `backend/_protocol`) → `FailureInfo` (the workflow extension: `node_key, map_index, error, attempts: tuple[per-attempt history], policy: AbsorbingPolicy`); the retry classifier (`transient`/`permanent`); `WorkflowRunError`; `RouterNotTotal`; the failed-parent cascade with peer-cancel records.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_propagation_pins.py` (8 pins — ran); `tests/test_wf_scenario_pins.py` (ran); `tests/test_wf_runner_pins.py::test_retry_classifier_routes_by_kind` (ran) |
| PROVEN | `.measurements/phase2/t06-pin-reds.txt` (RED1 cascade neutralized, RED2 peer record dropped, RED3 fan-in dropped, RED4 skip-as-attempt — real mutations, greens alongside); `.measurements/t20-pin-reds.json` `t20-router-not-total-row`/`-runner` (the silent-drop mutation → `{"status": "failed", "error_class": "RouterNotTotal"}`) |
| VERIFIED | `.measurements/attack2/` + `.measurements/attack3/PHASE3-ATTACK-REPORT.md` (the red-team rounds); PHASE1 cert |
| DEMOED | the demo's poisoned doc: the node row reads `{'status': 'failed', 'error_class': 'ValueError', …}` and the run's report carries the failure — e2e capture + test run |
| DOCS | `docs/guides/workflows.md` "The failed-parent propagation (T06)" (§119); §831 |
| TYPED | `tests/test_wf_propagation_pins.py::test_t06_pin7_unknown_failure_policy_refused_at_build` (the Literal refusal); `FailureInfo`'s absorbing-vocabulary door (`_types.py` — a fail_closed claim unconstructible at the marker) |

### 7. MAYBE TYPES — Ok/Failed/Maybe envelopes, the exhaustiveness

**Named honestly:** there are NO `Ok`/`Failed`/`Maybe` classes in `src/taskq/workflows/` (grepped — zero). The envelope is `FailureInfo` + the `AbsorbingPolicy` marker (`Literal["collect", "maybe"]`, `_types.py:60`) — the item TYPED to the absorbing vocabulary, the walk repairing out-of-vocabulary corruption to `'collect'` (never a lie). Exhaustiveness is the per-node union idiom: a union result consumed with `match` + `assert_never` (the docs' shape 7). **Docs-truth flag:** `p4:docs/examples/doc-ingest.md` shape 2 says "997 `Ok` + 3 `Failed` fan in" — prose vocabulary the types do not carry; the demo's report uses `PublishReport`/`FailureInfo`. The docs lane should either name the real shapes or the fixer lane should ship the `Ok`/`Failed` aliases.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_scenario_pins.py::test_join_failure_policy_maybe_surfaced_and_the_zero_collect` (ran); `tests/test_wf_propagation_pins.py::test_t06_pin8_mixed_policy_node_the_fence_is_not_absorption` (ran) |
| PROVEN | `.measurements/phase2/t07-scenario-pin-reds.txt` + `t06-pin-reds.txt` (the scenario pins' real-mutation reds) |
| VERIFIED | the attack rounds (attack2/attack3 cover t07's absorbing policies); PHASE1 cert |
| DEMOED | the demo's `gather([labels], on_failure="maybe")` leg (the classifiable summary) — runs in the demo full-run |
| DOCS | `docs/guides/workflows.md` §75 "The fan-in bound + the two join shapes (T07)"; the worked duality (`p4:docs/examples/doc-ingest.md` shape 5) |
| TYPED | `AbsorbingPolicy` + `FailurePolicy` Literals (`_types.py`); the envelope-must-not-lie door (a fail_closed policy claim unconstructible). Negative probe for the marker: EMPTY — the typeprobe corpus has no MUST_ERROR probe on an `on_failure=` wrong literal (the runtime/build refusal is tested; the checker-level probe is not) |

### 8. CANCELLATION — the cancel semantics per state

One transaction, the flow flip as the linearization point; THREE legs: (1) the fire guard's flow-status leg, (2) the dispatch fence (a cancelled flow's children unclaimable), (3) the finalize fence (worker+attempt+claim-epoch). Holds: late deliver after cancel refused (no zombie wake); cancel is idempotent.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_hitl_pins.py::test_cancel_workflow_one_transaction_idempotent` + `::test_late_deliver_after_cancel_is_refused_no_zombie_wake` (ran); `tests/test_wf_state_event_totality.py::pin_5_sweep_fire_refuses_post_cancel` + `::pin_2_dispatch_fence_refuses_post_cancel_claim` (ran); `tests/test_wf_finalize_pins.py::pin_24_finalize_on_cancelled_flow_never_decrements` (ran) |
| PROVEN | `p4:.measurements/pin-reds.json` `pin2-pin5-unfenced-sweep-fire` (`{"fired_post_cancel": 1}`) + `pin2-dispatch-fence-plan` (EXPLAIN BUFFERS plan receipts ×3) + `pin24-flow-status-leg`; the certifier's B2 live-revert drill (PHASE1 cert) |
| VERIFIED | `p4:.measurements/attack4-fix/PHASE4-CERT2.md` §1 — the TORN-CANCEL cure verified with the certifier's own cancel×resolve×expiry-sweep interleaving (24 rounds, every landing a named consistent state) |
| DEMOED | **EMPTY** — the doc-ingest demo never cancels; no demo path exercises a flow cancel. The demo lane owes a cancel leg (e.g. the operator cancel from the run page mid-run) |
| DOCS | `docs/guides/workflows.md` "The three cancel legs" (§193); `docs/guides/cancellation.md` (the queue-level semantics) |
| TYPED | partial — the cancel verbs' typed doors are the CLI's audit-row principals (`p4:docs/guides/cli.md:1298`); no negative type probe on cancel shapes |

### 9. PROGRESS — T21's emission/aggregation/faces

The two channels (state + log ring), the coalescing cadence with the honest occurrence counter, the drop-oldest bound + ring-prune sweep arm, the display reconstruction (full/partial), the workflow-dimensioned gauge, the HTTP SSE face.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_progress_emission.py` (9 — ran), `tests/test_wf_progress_faces.py` (8 — ran), `tests/test_wf_progress_persistence.py` (ran), `tests/test_context_progress_background.py` |
| PROVEN | `.measurements/t21-pin-reds.json` — 6 named receipts: `red_every_emission_a_row_grows_unbounded` (the 6.5M-row incident's shape), `chatty_body_coalesce_cadence`, `ring_prune_arm_prunes_leaked_ring`, `gauge_cardinality_stays_workflow_dimensioned` (1 vs 12 series), `display_shows_failure_with_pct_inside` (the liar counterfactual), `silent_gap_variant_structurally_impossible` (the variant cannot be built) |
| VERIFIED | `.measurements/t21-gate-{1,2,3}-*.txt` (three gates, several rounds each); the cert rounds ride T21-REPORT.md |
| DEMOED | the demo's progress line + stream ring (`auto/wf.node.started` → `user/progress` → `auto/wf.node.terminal`, coalesced) in the e2e capture; the admin page's live SSE face (the page tests ran) |
| DOCS | `docs/guides/workflows.md` §5 "Progress: the nodes report (T21)" (§978); `docs/guides/progress.md`; `p4:docs/guides/observability.md` the triad section |
| TYPED | the declared-schema door refuses wrong shapes (`ProgressRefusedError`, `_progress.py:123`) — `tests/test_wf_progress_emission.py::test_declared_schema_door_refuses_wrong_shape` (ran). Known owed cure (stated in p4 docs): the emit door refuses pydantic models (`model.model_dump()` required) — the contract is the refusal until the T20-lane cure lands |

### 10. FAN-OUT — the map/emit

`map_source`'s fork at the source's finalize (runtime cardinality); the streaming source's emit tx (children + edges + cursor, ONE tx per page), fenced on the claim epoch, `map_index` discriminating siblings.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_t20_emit_pins.py` (4 — ran); `tests/test_wf_runner_pins.py::test_sequenced_maps_depth3_one_flow` + `::test_map_children_ledger_identity_and_max_attempts` (ran); `tests/test_wf_fork_pins.py` (ran) |
| PROVEN | `.measurements/t20-pin-reds.json`: `t20-emit-window-1/2/3` (kill at each statement window → partial page), `t20-emit-zombie` (the cursor unfenced → the superseded attempt's children), `t20-emit-map-index` (the discriminator dropped → sibling collision), `t20-emit-between-pages-kill` (the resume off-cursor → duplicates/loss) |
| VERIFIED | `.measurements/patterns/probe-run-ecosystem.txt` probe_04 (runtime 7-item fork → 7 child rows + 7 ledger rows) — re-run, PASS |
| DEMOED | the demo's fork-of-N (heterogeneous placement) + per-item fork + collect at batch scale; `.measurements/fanout-1000-tx-band.json` (the 1000-edge band) |
| DOCS | `docs/guides/workflows.md` §3 (§545) + §10 (§703, "The emit tx — the one new primitive", "map_index is load-bearing") |
| TYPED | `map_source`/`step` wiring generics (`Promise` not-awaitable, promise-not-data — `tests/typeprobe/wf_api_negative_types.py`) |

### 11. FAN-IN — the join/gather

The join-wait rows; the 1000-edge bound (refusals name the alternative); the child-driven join firing EXACTLY ONCE (`wf_join_fire` UNIQUE + the outbox's idempotent consumer inserts); the tx2 decrement guard; the empty join never fires.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_fanin_bound_pins.py` (4 — ran); `tests/test_workflows_ledger_pins.py::pin_17_empty_join_never_fires` (ran); `tests/test_wf_finalize_pins.py` (pins 23/24 — ran) |
| PROVEN | `p4:.measurements/pin-reds.json`: `pin17-empty-join` (child-row recount → fires early), `pin20-outbox-flush-stranded` (crash between fire-commit and consumer-insert), `pin23-decrement-guard` (`deps_pending_fingerprint: -1`), `pin1-snapshot-write`, `pin8-misnamed-child` (a missing parent counts as complete → orphan fired) |
| VERIFIED | `.measurements/patterns/probe-run-ecosystem.txt` probe_05 (two redundant sweep+drain passes moved NOTHING) — re-run, PASS |
| DEMOED | the demo's barrier join fired exactly once across 3 consecutive rounds (`p4:tests/test_doc_ingest_example.py` — ran; the exactly-once pin asserted against `wf_join_fire`) |
| DOCS | `docs/guides/workflows.md` §75 (the bound + two shapes); "The two-transaction finalize" (§27) |
| TYPED | `gather(..., on_failure=...)` Literal; the bound refusals name the alternative (`test_wf_fanin_bound_pins`) |

### 12. FORK/BRANCH — the fork + the conditional router

The certified fork (per-fork id discrimination, fork-atomicity: children + terminal one tx); the T20 router — route declarations total or refused at build AND at run (the foreign outcome fails the row loudly, never silently dropped).

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_fork_pins.py` (pins 18/19/20 — ran); `tests/test_attack_fork_inherit.py` + `test_attack_fork_inherit_pg.py`; `tests/test_wf_t20_router_pins.py` (6 — ran) |
| PROVEN | `p4:.measurements/pin-reds.json` `pin18-id-collision` (string-shape id mint → UniqueViolation, the convicted collapse), `pin19-fork-debt` (split-tx fork killed in the window → terminal parent, 0 children); `.measurements/t20-pin-reds.json` `t20-router-fork` (a hand-wired child outside the parent's terminal) + `t20-router-not-total-row`/`-runner` (the silent drop → loud `RouterNotTotal`) |
| VERIFIED | `tests/attack_wf_dispatch_fence.py` (PHASE1 cert); `p4:PHASE4-CERT2.md` |
| DEMOED | **PARTIAL** — the demo routes through a plain step + union `match`/`assert_never` (shape 7, the portable idiom); the T20 ROUTER DECLARATION itself has no demo leg (its evidence is the pins + the docs' code sample §761). The demo lane owes a router-decl route in the demo graph |
| DOCS | `docs/guides/workflows.md` §761 "The router: conditional edges, total or refused" |
| TYPED | `test_wf_t20_router_pins.py::test_t20_route_not_total_is_refused_at_declaration` (ran) — the build-time refusal; checker-level probe: EMPTY (no typeprobe MUST_ERROR for a non-total route map) |

### 13. MID-FAN RETRIES — a child retrying while siblings run (the isolation)

The ladder is per-row: a child's retry emits no cascade, no decrement; skips are not attempts (zero ledger rows); the retry barrier holds the join until the retried child's terminal.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_scenario_pins.py::test_map_retry_barrier` (ran); `tests/test_wf_propagation_pins.py::test_t06_pin5_mid_ladder_emits_no_cascade_and_no_decrement` (ran); `tests/test_wf_runner_pins.py::test_transient_ladder_emits_no_terminal_until_exhaustion` (ran) |
| PROVEN | `.measurements/phase2/t06-pin-reds.txt` RED4 (skip-as-attempt) + the pin5 reds; `pin23-decrement-guard` (the guard flip → `-1`) |
| VERIFIED | the attack rounds (attack2's t06 cascade file rode PHASE1 cert) |
| DEMOED | the demo's `doc-doomed` isolation — the failed item re-runs ALONE, siblings' results intact (the e2e capture's per-trace rows: 5 succeeded traces untouched) |
| DOCS | `docs/guides/workflows.md` §119; `p4:docs/examples/doc-ingest.md` shape 4 |
| TYPED | `max_attempts`/`retry_kind` per item on the compiled node (the actor-config projection's fields, probe_01); checker probe: EMPTY |

### 14. PARTIAL SUCCESS — the report envelope / the degraded routes

`collect`/`maybe` edges absorb; the failure fans in as `FailureInfo` items; the join FIRES with the typed partial result; the envelope must not lie (the policy marker names which policy ran).

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_propagation_pins.py::test_t06_pin3_collect_exhaustion_fans_in_fires_partial` (ran); `tests/test_wf_scenario_pins.py::test_join_failure_policy_maybe_surfaced_and_the_zero_collect` (ran) |
| PROVEN | `.measurements/phase2/t06-pin-reds.txt` RED3 (the fan-in dropped) |
| VERIFIED | the attack rounds; probe_03/05 ride the same finalize path |
| DEMOED | the demo's report envelope: `{'failed_chains': 1, 'total': 6}`, the poisoned doc's row, the 12 good traces — the e2e capture + the test run |
| DOCS | `docs/guides/workflows.md` §831 "the partial-success run (the envelope must not lie)"; `p4:docs/examples/doc-ingest.md` (the failures REPORTED, never swallowed) |
| TYPED | `FailureInfo.policy: AbsorbingPolicy` (the typed door — a fail_closed claim unconstructible); `_failure_info_from_json`'s repair walk |

### 15. HITL — the holds, the gates, the resolve

Rows are the humans: the hold row (gate, node, epoch, deadline, declared payload schema), the TypedGate's deliver CAS (exactly one resume; double deliver = typed no-op), resume-not-retry (no ladder burn), the knock channel (`taskq_wf_holds`), the expiry sweep's timeout arm on DB clock, the escalation ladder, the audit rows.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_hitl_pins.py` (9 — ran); `tests/test_wf_loop_pins.py::test_hold_inside_a_loop_pauses_the_budget_and_completes` (ran); `tests/attack3-hitl.py` |
| PROVEN | `.measurements/phase2/t07-scenario-pin-reds.txt` (the scenario pins); the phase-3 attack round's HITL reds (`.measurements/attack3/hitl-run1.txt`, `PHASE3-ATTACK-REPORT.md`) |
| VERIFIED | `.measurements/patterns/probe-run-ecosystem.txt` probe_02 (slot RELEASED while held, `locked_by_worker` NULL; deliver → `delivered`; re-deliver → `no-op`) — **re-run by this audit, PASS**; `p4:PHASE4-CERT2.md` (the resolve verified on both surfaces, 3× cold) |
| DEMOED | the demo's hold → `HitlClient.resolve` → resume → publish, ×3 rounds on :5716 (ran); the admin Resolve form's typed door (the page tests ran) |
| DOCS | `docs/guides/workflows.md` §4 "HITL: humans are rows (T10)" (§879); `p4:docs/guides/cli.md` the `flows signal/resolve` typed door (§1298); the explorer's holds & decisions section |
| TYPED | the TypedGate wrong-model delivery door (the probe lands with the T10 runtime — `tests/typeprobe/attack2_wf_phase2_types.py`); the CLI validates resolve/signal JSON against the bound gate's pydantic models before any row moves (tested in `p4:tests/test_wf_cli_pins.py` — ran) |

### 16. RESUMABILITY — the checkpoint/resume (the cursor + the crash heals)

The rows ARE the checkpoint: `ctx.step`'s ledger memo (pre-wait side effects replay CHEAP), the emit cursor (resume re-emits exactly the lost page), the sweep arms healing wedged state (rederive/outbox-drain/phantom-reap), reclaim ≠ ladder burn.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_propagation_pins.py::test_t06_pin6_sweep_heals_fail_closed_join_after_crash` (ran); `tests/test_wf_t20_emit_pins.py::test_t20_emit_tx_zero_reemitted_zero_lost_across_pages` (ran); `tests/test_workflows_ledger_pins.py::pin_1_double_run_red_and_memoized_green` (ran); `tests/test_attack_liveness_resume.py`; `tests/attack_wf_engine_windows.py` |
| PROVEN | `.measurements/t20-pin-reds.json` `t20-emit-between-pages-kill` + `t20-emit-zombie`; `.measurements/ledger-pin-reds.json` `pin1-double-run` (`{"executions_after_redelivery": 1}` — the replay forgets), `pin2-lost-completion-window`, `pin5-ledger-claim-atomic` |
| VERIFIED | probe_03 (the LangGraph replay: memo fn executed ONCE across two attempts, one delivered signal, the replayed answer) — **re-run by this audit, PASS** |
| DEMOED | **EMPTY** — no demo path kills a worker mid-page and resumes. The demo lane owes the crash-and-resume drill (the pins prove it; the demo never shows it) |
| DOCS | `docs/guides/workflows.md` §206 (the step ledger + arbiter), §27 (the two-tx finalize), §49 (the reducer body's boundary — the durable resolution); `context.py`'s replay-contract docstring |
| TYPED | partial — the ledger claim's ON CONFLICT arbiter is schema-typed (the migration pins, `tests/test_wf_schema_migration.py` — ran); no checker-level probe |

### 17. THE ADMIN VIEW — the explorer

The runs list (newest first, capped 200) + the run page `/taskq/workflows/{run_id}`: rows-alone Mermaid (one collapsed hexagon per map), the node panel (attempts, trace, error, one upstream hop, the attempt ledger), the holds & decisions with the typed Resolve form, the audit trail, the SSE revisioned-snapshot transport with the seq-cursor, the no-JS story stated.

| kind | artifact |
| --- | --- |
| TESTED | `p4:tests/web_admin/test_workflows_page.py` (663 lines — ran, green); `tests/test_admin_*.py` (walk round-trips, audit trail — p4 tree) |
| PROVEN | **EMPTY (page-level)** — no red receipt names a mutation of the explorer's own read/render path. The nearest receipts: `p4:.measurements/attack4-fix/cert2-solo-red-sse-caps.txt` (the SSE caps' red) and the state-event totality pins (the derivation all three surfaces share). The fixer lane owes an explorer mutation pin (e.g. the map's hexagon collapse derived from the wrong rows → the page shows N child boxes) |
| VERIFIED | `p4:.measurements/attack4-fix/cert2-cold-stack-walkthrough-r3.txt` — the demo compose walked 3× from a `down -v` cold start, the central resolve performed on BOTH surfaces; `p4:.measurements/attack4-fix/PHASE4-CERT2.md` (CERTIFIED-FOR-PHASE-5) |
| DEMOED | the demo app runs the explorer attached; the e2e capture's "THE EXPLORER READ (13 nodes)" incl. the failed node and the held state; the page tests ran on :5716 |
| DOCS | **EMPTY on the consolidated head** — "Visualizing workflows: the admin's run explorer" exists ONLY in `p4:docs/guides/workflows.md` (§1098); `docs/guides/admin-ui.md` (both trees) has ZERO workflow mention. The merge lane owes the section (and a pointer from admin-ui.md) |
| TYPED | n/a (server-rendered rows; the typed doors are the resolve form's pydantic validation — covered under HITL) |

### 18. OBSERVABILITY — the metrics, the events, the traces

The triad (p4 docs): the metrics (`taskq.wf_progress_nodes_total{workflow, state}` + friends, promtool-gated), the events (the bounded per-node stream ring: `wf.node.started` / `wf.node.terminal` / `user/progress` — rows, never a metric series per child), the admin + CLI faces reading ONE derivation. Traces: the OTEL round-trip exists at the ACTOR level only.

| kind | artifact |
| --- | --- |
| TESTED | `tests/test_wf_state_event_totality.py` (ran); `tests/test_prometheus_metrics.py` + `test_prometheus_metrics_review.py` (the wf gauge — p4 tree); `tests/otel_validation/test_otlp_round_trip.py` (actors) |
| PROVEN | `.measurements/t21-pin-reds.json` `gauge_cardinality_stays_workflow_dimensioned` + `red_every_emission_a_row_grows_unbounded` |
| VERIFIED | the T21 gates (`.measurements/t21-gate-{1,2,3}-*.txt`); the cert rounds |
| DEMOED | the e2e capture's stream ring + progress line; the prometheus endpoint e2e (queue-level). The wf metrics' live scrape: **EMPTY** — no demo capture shows a workflow run's Prometheus scrape |
| DOCS | **EMPTY on the consolidated head** — `p4:docs/guides/observability.md` "The observability triad (workflows)" + the marking taxonomy (§1394+) are p4-only; `docs/guides/observability.md` on the head lacks them. The merge lane owes the section |
| TYPED | partial — the metric names are constants in `src/taskq/obs/_otel.py:3721`; the marking taxonomy is a docs table, not a type. **TRACES for workflow runs: EMPTY** — no OTEL spans on the workflow claim/finalize/emit paths; the otel_validation corpus is actor-level. The fixer/phase-5 lane owes either spans or an honest docs sentence that flows are metrics+events only |

---

## FILL RATE

108 cells: **87 filled · 21 empty-or-partial** → **80.6% filled** (counting the partials as filled-but-flagged: 80 clean, 7 partial).

## THE EMPTY-CELL LIST (ranked — the work order)

The ranking rule: a capability unTESTED or unPROVEN outranks unDOCS; within a tier, the cell that blocks the replacement claim outranks the cosmetic.

**Tier 1 — the fixer lane owns (the missing pins / the missing teeth):**
1. **Flow OUTPUTS — PROVEN: EMPTY.** The result codec (`encode_result`/`decode_result`, `TERMINAL_RESULT_SQL_TEMPLATE`) has no real-mutation red anywhere in `.measurements/`. The fixer lane owes one red (the `{"value": …}` wrapper dropped → the envelope-key collision unconvicted).
2. **THE ADMIN VIEW — PROVEN: EMPTY.** No mutation pin targets the explorer's read/render path. Owes: the hexagon-collapse-derived-from-wrong-rows red (the page must render FROM the rows).
3. **MAYBE TYPES — TYPED: EMPTY (checker-level).** The `on_failure=` wrong literal is runtime-refused but has no typeprobe MUST_ERROR probe; the `Ok`/`Failed` prose-vs-types naming mismatch (`p4:docs/examples/doc-ingest.md` shape 2) is a docs-truth debt for the same lane.
4. **FORK/BRANCH — TYPED: EMPTY.** No checker-level probe for a non-total route map (the build-time refusal is tested).

**Tier 2 — the phase-4 (demo) lane owns (the demos):**
5. **CANCELLATION — DEMOED: EMPTY.** The demo never cancels a run; the cancel story (three legs, idempotent, torn-cancel cure) is pin- and cert-proven but never SHOWN. Owes: an operator-cancel leg in the doc-ingest demo (cancel mid-run from the run page; the graph grays; the children refuse).
6. **RESUMABILITY — DEMOED: EMPTY.** No demo path kills a worker mid-page and resumes. Owes: the crash-and-resume drill in the demo compose (kill the worker between pages; the resume re-emits exactly the lost page; the explorer watches it heal).
7. **OBSERVABILITY — DEMOED: EMPTY (the wf scrape).** No capture shows a workflow run's Prometheus scrape. Cheap: one scrape line in the demo walkthrough.
8. **FORK/BRANCH — DEMOED: partial.** The T20 router declaration has no demo leg (the demo routes via a plain step + union match). Owes: one router-decl route in the demo graph.

**Tier 3 — the merge/docs lane owes the consolidated head (p4's sections not yet landed):**
9. **RUN CONTEXT — DOCS** on the head: the context-contract field table (p4-only).
10. **THE ADMIN VIEW — DOCS** on the head: the explorer section (p4-only), plus the admin-ui.md pointer (empty in BOTH trees).
11. **OBSERVABILITY — DOCS** on the head: the triad + marking-taxonomy sections (p4-only).

**Tier 4 — the phase-5 lane owns (the honest-gap typing):**
12. **Flow INPUTS — TYPED: EMPTY.** `create_flow(input: object)` — no validation at the boundary, no negative probe. Phase-5 owes either a typed input door (validate the root payload against the declared entry model) or the documented convention + a MUST_ERROR probe.

---

## REPLACEMENT-READY VERDICT — the maintainer's bar: "taskq actor + langgraph replacement ready"

Two migration sources named. Evidence per source:

### Source A — the ctx child-dispatch actors (the TaskQ-native ad-hoc pattern)

| the bar's test | verdict | artifact |
| --- | --- | --- |
| the migration doc exists? | **YES** | `docs/guides/workflows.md` §845 "THE MIGRATION PATH: from the actor's ad-hoc children to the chain" — the pattern-by-pattern table (child dispatch → `ctx.emit_batch`; the counter row → the rows-alone derivation; ad-hoc retry → the certified ladder; invisible dependency → the declared edge; payload re-marshalling → the verbatim record), the lineage law (`flow_id`/`trace_id`/`map_index`/`parent_id`), and the honest opt-in boundary (a child outside the fork is a foreign row; the migration is per-workload, not a fleet conversion) |
| the pattern probes prove the shapes? | **YES** | probes 4 (dynamic map), 5 (exactly-once outbox), 6 (run-key arbiter), 1 (the actor-config projection `{actor, queue, max_attempts, retry_kind}` — the Dagster op/job split, probe_01's `actor_config_projection`) — all re-run by this audit, 4/4 PASS |
| the demo shows the ported shape? | **YES** | the doc-ingest demo IS the ported shape (the paged source emits children+edges+cursor one-tx; the per-record lineage readable in the explorer's node panel) — ran green ×3 rounds on :5716 |
| missing for TRUE | **Nothing material.** The one named migration-adjacent defect — the ecosystem mapper's downstream-of-map consumption trip (the parent's join promise consumed before terminal) — was CURED post-map: `api/_runner.py::_insert_static_nodes` now inserts every non-map node upfront with the RESERVED dep ("the ecosystem mapper's consumption defect's structural half"), and `tests/test_wf_phase3_cure_pins.py::test_map_join_promise_consumed_downstream_sees_the_collected_results` pins it (ran, green). **Source A: REPLACEMENT-READY.** |

### Source B — the LangGraph apps (interrupt / checkpoint / replay / state channels)

| the bar's test | verdict | artifact |
| --- | --- | --- |
| the migration doc exists? | **NO — EMPTY.** No user-facing LangGraph migration section in `docs/` (grepped: zero `langgraph` hits in either tree's docs). What exists is the internal measurement artifact: `.measurements/patterns/ECOSYSTEM-MAP.md` patterns 22–24 (interrupt/checkpoint EXPRESSED, thread replay EXPRESSED, state channels AWKWARD — the single-channel carry covers the common case, multi-key reducers have no first-class shape). A user migrating from LangGraph has NO guide. The phase-4 lane owes a "from LangGraph" migration section (or a page in `docs/guides/workflows.md`) that names the mapping: `interrupt()` ≙ the hold (`_NodeHeld`, slot released, resume-not-retry), `checkpoint` ≙ the rows + the ledger memo, the answer queue's per-attempt cursor ≙ `hold_epoch`, and the AWKWARD trade for multi-key state channels |
| the pattern probes prove the shapes? | **YES** | probe_02 (signals/holds: slot released, exactly-one resume, no-op double-deliver), probe_03 (checkpoint replay: memo executed once across two attempts, the delivered answer replayed), probe_07 (the loop carry advanced exactly once — the single-channel reducer) — re-run, 3/3 PASS |
| the demo shows the ported shape? | **YES, for interrupt+replay** | the demo's held approval (the loop's budget pauses, the resolve resumes toward publish) is the interrupt/resume shape, live on the admin explorer |
| missing for TRUE | **The migration doc.** The shapes are probe-proven and demo-shown; the DOC is the empty cell. Secondary (honest, stated): multi-key state channels with per-key merge fns have no first-class shape (ECOSYSTEM-MAP #24 AWKWARD) — a LangGraph app leaning on multi-key reducers does NOT port cleanly today; the doc must say so rather than claim it |

### The verdict

**REPLACEMENT-READY: source A yes; source B NOT YET — one empty cell (the LangGraph migration doc) plus the stated AWKWARD trade.** The engine-side bar is met for both (the shapes are tested, mutation-proven, independently certified, and demoed); the claim fails only on the migration-document artifact for LangGraph, which is the phase-4/docs lane's to write — and on the empty-cell work order above for the columns that would make the claim survive an auditor who only reads this matrix.
