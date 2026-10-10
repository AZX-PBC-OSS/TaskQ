# THE PHASE-3 FIX REPORT (fixer round 1 — the attack's refusal cured)

Head: the two fix commits on feat/taskqflow-p3 (`aa6cecf7` + `3941c874`).
PG: the fixer's own warm container on :5698 (`taskqflow-p3fix-pg`),
`TASKQ_TEST_PG_DSN` — local-first; nothing else touched.

## THE GATES (all green on the fix commits)

- ruff check src tests — **0** (the per-file S608/N999 tolerances follow
  the house's own rationale shape; the remaining hits at report time are
  ANOTHER session's in-flight files — `_progress_read.py`,
  `admin/jobs.py` — not this round's).
- ruff format — **0** on the fix's files (the 4 pre-existing drifted
  files also re-formatted).
- pyright 1.1.414 — **0 errors, 0 warnings** on `src/taskq tests` (the
  same caveat: the concurrent session's WIP files carry their own).
- the type gate (`tests/typeprobe/_gate.py`) — every MUST_ERROR marker
  (10) reds on pyright 1.1.414 AND ty 0.0.85 (the Exit sentinels' 2
  new markers included).
- the ×3 captured runs: `cure-run1/2/3.txt` — 37 passed each (the 19
  attack probes + the 18 cure pins).
- the final gate battery: `final-gate-battery.txt` — 146 passed (the
  whole wf pin family + the attack probes).
- the certified core: the full battery (12,897 passed) shows NO new
  failure from this round — the 18 reds there decompose into 12+1
  PRE-EXISTING (verified red on the PRISTINE head in a separate
  worktree: the rule-file counts, the prometheus rules, the dev
  restarts, the migration columns, the memory fixture, the changelog
  pin), the load-sensitive pin_4 band (passes solo; load noise), and
  the typed-outcomes trio (passes solo — the xdist teardown flake).

## THE CURES — per finding, the red/green paths

| Finding | The cure | Red (the capture) | Green (the pin) |
| --- | --- | --- | --- |
| B1 the dead timeout face | the wait site RAISES the glossary `SignalTimeoutError` on an abandoned hold — never an automatic new epoch; a caught-then-rewait is a NEW body decision (the per-attempt face marker) | `hitl-run1.txt` (A3-H1) | `attack3-hitl.py::test_a3_expired_hold_reholds_forever_no_timeout_face` + pin 13 rewritten real |
| B2 the union mis-narrow | `_coerce_signal` by SHAPE: each declared model validated strictly; EXACTLY ONE fits; zero/>1 = the typed refusal; the small honest `discriminator=` on the wait site | A3-H6 | `test_a3_union_wait_narrows_to_the_first_model` + `test_ambiguous_payload_refused_unless_the_gate_discriminates` + `test_both_fit_without_discriminator_is_refused_hold_survives` |
| H1 the dead escalate | `spec.on_exhausted` read by the DRIVER and the SWEEP (D1: the policy resolves from the REGISTERED definition); escalate enqueues via the outbox IN THE SAME TX to the REGISTERED `loop.escalation` step (the author's `escalates_to=` or the framework default); fail enqueues nothing | `loop-run1.txt` (A3-L1/L2) | the attack pins + `test_escalation_enqueues_and_the_registered_body_runs` + `test_driver_fail_policy_enqueues_nothing` + `test_escalates_to_registers_a_custom_body` (the consumer RUNS — no dead letter) |
| H2 the runtime deliver boundary | `resolve` validates against the hold's declared models BEFORE the CAS (the catalog + the durable `payload_schema` fallback); the refused resolve: the hold SURVIVES, the refusal AUDITED | `hitl-run1.txt` (A3-H5) | the attack pin + the survival + audit leg in `test_both_fit_without_discriminator_is_refused_hold_survives` |
| H3 the hold-context leak | reason/tool/args pass chain-then-hook (the vetted masks + the token-head pass + the workflow's own hook, wired from the registered definition) IN THE SAME TX as the hold's insert; the default client chains at read | `hitl-run1.txt` (A3-H3) | `test_a3_hold_context_canary_leaks_through_list_by_default` |
| H4 the audit exactly-once | the audit row + the knock ride the CAS-WINNING tx; the loser writes nothing | A3-H2 | `test_a3_double_resolve_audit_not_exactly_once` (1 audit + 1 knock + 1 resolution) |
| H5 the body-controlled classifier | the classifier is consulted ONLY at the MACHINERY boundary — the body cannot forge an infra fault; 20 body ConnectionErrors → the ladder burns → terminal; a machinery kill → reclaim (the ledger `crashed`, the ladder untouched, the loop completes) | `loop-run1.txt` (A3-L3) | `test_a3_poison_body_escapes_both_walls` + pin 6 rewritten to the escape-point contract (both arms) |
| M1 the until=held blindness | `_any_held` reads the hold MARKER (`metadata ? 'hold'`), not the racy `scheduled_at` proxy | A3-H7 | `test_a3_drive_held_misses_the_eternal_hold` |
| M2 the dead E3 | the diagnostic's unreachable `continue`-shadow fixed — the rule owns the injected shape | `validate-run1.txt` (A3-V1) | `test_a3_e3_rule_is_dead_code` |
| M3 the cross-graph smuggle | the verbs RECORD a foreign promise (never raise mid-build); validate convicts (E7, error) | A3-V2 | `test_a3_cross_graph_promise_smuggle_builds_a_silently_wrong_edge` + the committed E7 pin |
| M4 E5's duck hole | an unannotated/`Any` consumer param consuming a model producer is the E5 conviction | A3-V5 | `test_a3_e5_blind_to_untyped_and_duck_shapes` |
| M5 the unknown-queue seam | W2-unknown-queue (the warning class; the app's queue universe = its actors + TASKQ_QUEUES + `default`) | A3-V4 | `test_a3_actor_on_a_nonexistent_queue_is_not_refused_at_build` |
| THE VACUOUS FIVE | the sweep-cap arm REACHABLE (the advance guard lets the FINAL iteration run — the counter reaches the cap; the driver's top-check exhausts; the crash window's state is the sweep's trigger); pin 12 actually LISTENS; pin 13 exercises the face | the attack's vacuous audit | `test_the_final_iteration_runs_and_the_counter_reaches_the_cap` + the rewritten pins 12/13 |
| THE REDLOG DEFECT | the evidence sinks APPEND-ONLY + RUN-SCOPED (one JSONL record per run) — a partial run adds its own record, history never truncated; the band sink joins the law | `redlog-scope.txt` | the flush law in `tests/_wf_fixtures.py`; the corpus files only grow |

## THE ADDENDUM ROUNDS (folded into this one)

1. THE ALIGNMENT AUDIT: `Exit[DoneT]` landed end-to-end (the runner
   unwraps, the ledger records, the downstream marked skipped-with-the-
   record, the derivation reads COMPLETE; the type surface's 2 new
   MUST_ERROR markers red on BOTH pinned checkers); `retry_node` landed
   (the audited CAS, the attempt ordinal CONTINUES — one more attempt
   per manual retry; the closure re-opens; the failed root re-opens;
   the audit row); T19 pin 5's CARRIER-TYPE ENFORCED (E8); the timer
   policies recorded LATER with the reason (the body-level composition
   is the sanctioned escape — the docs name it).
2. THE ECOSYSTEM MAPPER: the map-join consumption defect CURED — the
   downstream of a map's join promise dispatches through the SAME typed
   door as any node result (the static row + the RESERVED dep + the
   fork's consumer edge + the join's own terminal writing the collected
   result; the drain's arbiter key now the static row's own convention —
   no duplicate dispatch; the transitive downstream composes). M1 was
   already this round's fix. The E4 FALSE POSITIVE cured (the raw
   presence check — the miner's repro green; the teeth pinned).
3. THE PATTERN MINER's demand-proven list: routed to the ticket author's
   lane (the progress/custom-event surface, the token streaming, the
   adoption pitch) — NOT this worktree's; the fixer's folds are the
   ones above.
