# PINMAP — the rv2 pin pack

File: `tests/test_wf_attack_rv2.py` (drop-in at the repo's `tests/`;
sha256 `8bad3f789a82c83273f64332ab67ae07f7f625df9b20aa9f9f53e79b58018c65`;
ruff-clean against the repo config).
Tree pinned at `d24f17b9638e` (feat/taskqflow re-review head).
House fixtures used: `wf_conn` / `wf_schema` / `wf_pool` / `wf_sql`
(tests/_wf_fixtures.py), `TASKQ_TEST_PG_DSN` for the PG lane. PG pins
are marked `integration` (the two admin-surface pins also `fastapi`);
the pure-static/pure-Python pins carry no marker. The async PG pins
pick up the estate's always-on G7 rows↔status teardown automatically
(file prefix `test_wf_`); every seeded state was checked G7-clean on
both the red and the (simulated) cured paths.

| Finding | Pin(s) | Status at d24f17b9 | The red observed (unmarked) | The SAFE behavior asserted (what flips it XPASS-strict) |
|---|---|---|---|---|
| F-RV2-1 (admin `error_class` unbounded) | `test_rv2_1_the_run_view_bounds_error_class` + `test_rv2_1_the_node_panel_route_bounds_error_class` | xfail-strict ×2 | 200007-char `error_class` served whole by `fetch_run_view` (root + both node siblings) and by the `/api/runs/{id}/nodes/{key}` route (detail + children census) | every `error_class` served by the view/route bounded (≤ the estate's 10k display ceiling) with the truncation NAMED (either house marker shape) |
| F-RV2-2 (reap count is uuid-as-int) | `test_rv2_2_the_nodeless_reap_returns_the_reaped_count` | xfail-strict | `reap_nodeless_roots` returned 2165906773833615665791907008062182344 for ONE reaped orphan | return == the rows actually reaped (== 1); the reaped row is `failed`/`NodelessRunReaped` (the arm's own contract, green leg) |
| F-RV2-3 (TypedGate crashes the validator) | `test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal` | xfail-strict | `AttributeError: 'TypedGate' object has no attribute 'timeout_s'` out of `_rule_eternal_wait` via `app.get` | the mistake is a NAMED refusal — `WorkflowValidationError` (E-rule) or `WorkflowBuildError` (the wiring door); never a traceback |
| F-RV2-4a (audit reason/detail verbatim) | `test_rv2_4a_the_audit_reason_and_detail_are_bounded` | xfail-strict | 5,000,000-char reason / 5,000,012-char detail landed whole in `admin_audit` | reason AND detail bounded (≤ 10k ceiling) with the truncation named on the row |
| F-RV2-4b (NUL reason aborts the wf cancel) | `test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel` | xfail-strict | `CharacterNotInRepertoireError: invalid byte sequence ... 0x00`; root stayed `running` (whole cancel rolled back) | the cancel LANDS (root `cancelled`, return ≥ 1) and the audit row carries the sanitized reason (no NUL, content kept) |
| F-RV2-5 (cursor probe fence loss) | `test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe` | xfail-strict | each isolated leg reads plain/cursor = (0, 1): `__flow__` root, terminal-flow node, deps_pending=1 node | the cursor probe answers 0 rows exactly where the plain probe answers 0 (fence agreement — the module's own invariant) |
| F-RV2-6 (mid-loop render arity) | `test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation` | xfail-strict | pairings `[(True, 6), (True, 5)]` → `asyncpg.InterfaceError: the server requires 6 parameters, 5 were given` | no raw driver `InterfaceError` escapes the round (the comment's flip-back made true, or a NAMED degradation), AND every presented (statement, argc) pairing is arity-legal. Driver: real `ClaimCursor` on a stepped clock; stub conn enforces asyncpg's arity contract; no PG needed |
| F-RV2-7 (claim-health deque race) | `test_rv2_7_concurrent_record_and_snapshot_never_raise` | xfail-strict | 90 `RuntimeError: deque mutated during iteration` in 1.5s (4 writer + 2 reader threads, otel gate on) | zero exceptions of any class under sustained concurrent record+snapshot |
| F-RV2-8 (E11 nesting hole) | `test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal` | xfail-strict | `app.get(...)` on `loop(initial={"seed": parent_promise})` (E2 silenced via `sink`) DID NOT RAISE; separately verified the carry dies `UnencodableValue` at the jsonb bind | a build-time named refusal: `WorkflowValidationError` (E11 walking containers), `WorkflowBuildError` (the wiring door), or the encoder's typed `UnencodableValue` raised at declaration |
| F-RV2-9 (E10 arity lie) | `test_rv2_9_e10_counts_the_actual_params` | xfail-strict | leg 1: runnable unannotated body wired correctly refused with "takes 0 param(s)"; leg 2's message names 0 for a real 2-param body | leg 1 validates CLEAN; leg 2 (a real mismatch) refuses with E10 AND the message names the real arity ("takes 2 param(s)") |
| F-RV2-10 (deploy-matrix header) | `test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test` | xfail-strict (static) | cells 4/6/9 name no test function; `test_wf_matrix_red_drills.py` absent; the "operator table" doc never names the cells | every header cell maps to a real test; every cited file exists; the claimed operator table names the cells (or the claim leaves the header — the pin walks the header's text) |
| F-RV2-11 (the battery's deterministic red) | `test_rv2_11_the_split_placement_declaration_resolved` + `test_rv2_11_the_bad_source_declaration_still_refuses_loudly` | GREEN ×2 (guards) | — | see the verdict below |
| F-RV2-12 (zombie typeprobe) | `test_rv2_12_every_typeprobe_file_is_wired_into_the_gate` | xfail-strict (static) | `wf_gather_negative_types.py` on disk, absent from `_CORPUS` | non-underscore probe files == `_CORPUS`, both directions (underscore files are the directory's helpers: `_gate.py`, `_positive_ctx_probe.py` — the latter's own header names its wiring in `test_wf_ctx_annotation_pins.py`) |
| F-RV2-13 (head-stamp law reds at HEAD) | `test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree` | xfail-strict (static) | verifier rc=1 on the committed estate: 6 unstamped + 9 stale live claims named | the verifier greens on the committed `.measurements/runs`. The pin extracts `HEAD:`'s files (`git ls-files`/`git show`) into a tmp dir and restores mtimes from the run-scoped filename timestamps — the verifier's live-claim pick is mtime-ordered, so the verdict is the TREE'S, never the checkout's |
| F-RV2-14 (emit range vs the captures) | `test_rv2_14_every_ranged_figure_covers_its_captures` | xfail-strict (static) | `emit_tx_per_page.max_ms` cited 10.4–14.5; the captures carry 20.569 | every ranged ms figure in the streaming doc's measured sections covers its metric family across EVERY capture (run-scoped + rolled), at the cited precision; sections wire to families explicitly (an unwired section's range is a pin failure, not a silent skip) |

## F-RV2-11 — the root cause and the verdict

The failing battery test
(`tests/test_wf_execution_py.py::test_projection_the_split_placement_cohorts`)
declares `chain_source(chain, _exec_body, key="doc_source")` where
`_exec_body(ctx, params: ExecIngest)` takes a param NO wiring feeds
(zero `*args`). The runner invokes a chain-source body with ctx ALONE
(`api/_runner.py`: `if not node.args: return
tuple(parent_results.values())` — empty for a source), so the
declaration is a genuine wiring lie that would TypeError at the first
claim — exactly the ladder discovery E10 exists to refuse. Every other
chain source in the estate is ctx-only (`t20_source`,
`triage_source`). THE DECLARATION IS WRONG; E10's conviction is
correct. The resolution is fixing the battery test's declaration to
the ctx-only source shape — so the pins are GREEN GUARDS, not xfails:

* the corrected declaration projects both cohorts
  (`("wf", "default")` and `("wf-exec-gpu", "gpu")`) — the assertion
  the battery's red test meant to make;
* the bad shape keeps refusing LOUDLY (the `workflow-projection-
  skipped` record names `E10-arity`; zero cohorts project) — a
  "cure" that instead silences E10 flips this guard red.

## Notes for the fixer

* rv2-1's bound asserts the CEILING of the house's display conventions
  (10k) and either house marker shape ("truncated…" / "…more
  characters"); a tighter cure (512, 2000) satisfies it.
* rv2-4a's ceiling is the same 10k for both fields; the audit module's
  own `_SUBJECT_CONTROL_ESCAPES` discipline is the marker-shaped cure
  for rv2-4b's sanitize.
* rv2-6's driver deliberately does NOT rely on rv2-5's live bug (the
  probe is stubbed), so curing rv2-5 cannot false-red it.
* rv2-13's red count differs from the front's memo (15 named here: 6
  unstamped + 9 stale) only because the verifier's live-claim pick is
  mtime-sensitive; with deterministic mtimes the committed estate
  convicts exactly those 15 stems.

## Unreproducible findings

NONE — all 14 findings reproduced live at d24f17b9 before pinning
(the pre-pin repro shapes are in RECEIPTS.md).
