# T17 — THE PAPER-CUT DISPOSITION LEDGER (PR-5 landing round)

Every cut from `/tmp/opencode/authoring/PAPER-CUTS.md` (session
ses_eeacd1f1dffe — 20 cuts: 2 BLOCKER / 5 CRITICAL / 6 friction / 7 nit),
re-verified against the certified core (b4c0013d), dispositioned, and
routed to its landing ticket. The triage rule stands: **the bar is
"first-try correct, no boilerplate, IDE autocompletion resolves the
wiring."**

Count: 20 cuts — **applied 15 · declined-with-reason 1 · recorded-no-action
2 · verify-absent 2 · deferred 0.**

| Cut | Sev (confirmed) | Disposition | Where it landed | Re-test (the proof it closed) |
|---|---|---|---|---|
| #1 no user join body | BLOCKER | applied | **T09** — a fan-in node's user body IS the reducer: it runs inside the join's finalize tx, its decoded parents arrive as typed args, and its consumers dispatch as NORMAL steps (the engine's NodeSpec parents + deps_pending; the fired-join consumer bindings for the map shape) | `test_wf_runner_pins.py::test_join_user_body_cascades_downstream` |
| #2 no sequencing / nested maps | BLOCKER | applied | **T09** — the promise DAG spells sequencing as dataflow (a consumer of a join's promise dispatches strictly after it); nested maps compile (a map's source is another join's promise; the engine's promoted-nested-map decrement carries it) | `test_wf_runner_pins.py::test_sequenced_maps_depth3_one_flow` |
| #3 deliver() drops the payload | CRITICAL | applied | **T10** — the `wf_signals.payload` column rides the signal row; `ctx.signal(name)` reads it on resume | `test_wf_hitl_pins.py::test_delivered_payload_reaches_the_resume` |
| #3b wait_for_signal single-use | CRITICAL | applied | **T10** — MULTI-HOLD: the hold epoch `(workflow_id, node_key, signal_name, hold_epoch)` partial unique; a second hold mints a NEW epoch, re-holds cleanly | `test_wf_hitl_pins.py::test_second_hold_new_epoch_clean` (+ the UniqueViolation red on the (flow,name) variant) |
| #4 Maybe guard decided at create | CRITICAL | applied | **T09** — `skip: bool | Callable[[FlowState], bool]` evaluated AT DISPATCH against the flow's state | `test_wf_runner_pins.py::test_skip_predicate_decided_at_dispatch` |
| #5 holds burn the retry budget | CRITICAL | applied | **T10** — RESUME-NOT-RETRY: the resume consumes NO ladder attempts (the ledger distinguishes `awaited` from `failed`; the ladder counts `failed` only) | `test_wf_hitl_pins.py::test_resume_does_not_burn_the_retry_ladder` |
| #6 parent by string split | CRITICAL | applied (verified in core) | **T03/T04 (landed)** — the engine carries `parent_id` as a COLUMN; no string derivation anywhere (`grep rsplit` over src/taskq/workflows = the naming pins' site); ROOT-WITH-DOT / MISNAMED-CHILD refused at the API compile | `test_wf_api_pins.py::test_node_key_dot_root_refused` |
| #7 create_flow takes no input | friction | applied | **T09** — `create_flow(spec, input)` + `ctx.input` | `test_wf_runner_pins.py::test_create_flow_carries_input` |
| #8 Body untyped at the boundary | friction | applied | **T09** — bodies are annotated (`params: P -> Out`); the compile reads the annotations, the promises carry the types, `wf.validate()` refuses an unannotated actor; the payload codec hook (dumps/loads through the estate's `_json` seam) decodes once | `test_wf_api_pins.py::test_unannotated_actor_refused` |
| #9 collect result raw rows / no skipped policy | friction | applied | **T09** — the fan-in body receives DECODED results (never raw rows); THE SKIPPED POLICY, stated: a skipped child fans into absorbing joins as a `FailureInfo` item (`policy` marker on the item) and is NOT an Item in the decoded sequence | `test_wf_runner_pins.py::test_skipped_child_fans_in_typed` |
| #10 no drive(until=…) | friction | applied | **T19** — `FlowRunner.drive(flow_id, until="held" \| "terminal")` | `test_wf_loop_pins.py::test_drive_until_held` |
| #11 no signals read API | friction | applied | **T10** — `client.hitl.list(run=…)` answers "what is held" programmatically (T12's CLI verb reads the same client) | `test_wf_hitl_client_pins.py::test_list_shows_pending_holds` |
| #12 ladder ladders everything, hard-coded backoff | friction | applied | **T09** — actor calls take `retry=` (the landed classifier seam, `src/taskq/retry.py`) + `max_attempts=` per node | `test_wf_runner_pins.py::test_retry_classifier_routes_by_kind` |
| #13 claim(lease_s=…) ignores its param | nit | verify-absent | **verified absent in the core** — `LEDGER_CLAIM_SQL` binds exactly its parameters; no lease constant shadows a bind | `test_wf_engine_units.py` (the ledger claim family, green pre-phase-3) |
| #14 jsonb decode idioms differ | nit | applied | **T09** — the runner's read path decodes jsonb ONCE through the `_json` seam (`_decode_jsonb`), never per-call-site | `test_wf_runner_pins.py::test_result_read_decodes_once` |
| #15 Maybe's max_attempts dead expr | nit | applied | **T09** — every node (maps' children included) takes `max_attempts` as a real parameter | `test_wf_runner_pins.py::test_child_max_attempts_respected` |
| #16 vestigial _run_join arg / mutable class attr | nit | recorded, no action | spike hygiene — the repo engine has no `_run_join`; the registry/state are instance-owned (`WorkflowRegistry.__init__`) | — |
| #17 SCHEMA a module constant | nit | recorded, no action | spike hygiene — the repo's schema is settings-driven (`TaskQSettings.schema_name` + `require_schema` validation) | — |
| #18 wait_for_signal naming / resume contract | nit | **declined-with-reason** (the disposition T17 owns) | the name STAYS `ctx.wait_signal` — the tuple form `(Approval, Escalate)` is the TYPED wait (Package B finding 2), and the name pairs with `TypedGate.deliver`/`client.send_signal` on the same door; the RESUME-FROM-TOP contract is DOCUMENTED ON THE METHOD (the re-execution doctrine: the body re-runs; pre-wait side effects are `ctx.step`-ledgered and replay cheap) | `test_wf_hitl_pins.py::test_resume_reexecutes_from_top_ledgered_side_effects_replay` |
| #19 no result-read API | nit | applied | **T09** — the flow result surface: the compiled workflow's terminal promise read back by id, decoded | `test_wf_runner_pins.py::test_flow_result_read` |
| #20 the docs that don't exist | nit | deferred-with-ticket | **T13/T14** own the per-combinator doc pages (§8.5's skeleton); THIS round's commits write `docs/api-reference/workflows.md` + `docs/guides/workflows.md` §1-3 — the combinator pages land with the examples round | the mkdocs build gate |

## The red-first evidence

- **BLOCKER-reproduction red**: cuts #1/#2/#4/#7/#9 reproduce as the
  strict-xfail contract probes in `tests/test_wf_ergonomics_contract.py`
  (each probe FAILS — xfail, strict — against the pre-API tree; the
  landing commit removes the marker and the probe greens: the cut IS the
  red, the green is the cure). Cuts #3/#3b/#5 reproduce red in T10's pin
  family (the row-side cures land there; the reds are captured in
  `.measurements/t10-pin-reds.json`).
- **Bar-walk red/green**: `test_wf_ergonomics_contract.py::test_bar_walk`
  compiles the authoring session's ORIGINAL graph shape (the
  `doc_ingest` abstract domain) with zero boilerplate — it reds (xfail)
  until T09's surface lands, then greens unchanged.
- **Regression red**: the certified core's wf family (the phase-2 pin
  suites) runs green on every PR-5 commit — `.measurements/p3-baseline-wf-full.txt`
  (the pre-change baseline) and the per-commit gate files.
- **The abstraction contract**: the repo-bound surfaces carry ZERO
  campaign-domain terms — `tests/test_wf_doc_domain.py` greps the shipped
  surfaces (`docs/`, `examples/`, `src/taskq/workflows/`, `tests/`) for
  the banned vocabulary and passes on the abstract `doc_ingest` domain.
