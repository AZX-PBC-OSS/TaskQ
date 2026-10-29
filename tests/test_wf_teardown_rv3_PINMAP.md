# PINMAP — the rv3 teardown pin pack

File: `tests/test_wf_teardown_rv3.py` (drop-in at the repo's `tests/`;
the pin head is `512f7a91` — feat/taskqflow's LOCAL tip; ruff-clean
against the repo config). House fixtures used: `wf_conn` / `wf_schema` /
`wf_pool` (`tests/_wf_fixtures.py`), `TASKQ_TEST_PG_DSN` for the PG
lane. PG pins are marked `integration`; the pure-static pins carry no
marker. Two pins are GREEN GUARDS at the head (the zero-false-positive
bounds — rv3-4c, rv3-12b); the rest are `xfail(strict=True)` LIVE
FINDINGS.

The findings are the teardown review's pinnable subset (the reviewer's
numbering; no F-RV3-11 — the reviewer's #11 was not pinnable-at-this-
head and the cut carries the rest).

| Finding | Pin(s) | Status at 512f7a91 | The red observed (unmarked) | The SAFE behavior asserted (what flips it XPASS-strict) |
|---|---|---|---|---|
| F-RV3-1 (map_source's can-only-error `key=`) | `test_rv3_1_map_source_takes_no_key_param` + `test_rv3_1b_the_map_source_removal_is_documented` | xfail-strict ×1 + GREEN guard ×1 | the signature carries `key: str \| None = None` whose every value raises `WorkflowBuildError` (the accept-and-ignore class's cousin — a param that only errors is a trap, not API) | the param GONE from the signature; the removal DOCUMENTED (the changelog or the guide's map section names the deletion — the preserve law) |
| F-RV3-2 (the packaged run's raw first contact) | `test_rv3_2_the_packaged_run_names_the_unmigrated_schema` | xfail-strict (integration) | `workflows.run(...)` against a schema that exists but was never migrated raised the RAW `asyncpg.exceptions.UndefinedTableError` out of `create_flow` (`_run.py:92`) — the client arm's `SchemaNotMigratedError` translation exists and the packaged door does not use it | the TYPED `SchemaNotMigratedError` naming the schema, chained from the driver error — the actionable setup message the client arm already ships |
| F-RV3-3 (apply_pending's Pool AttributeError) | `test_rv3_3_apply_pending_names_the_connection_it_refuses` | xfail-strict | `apply_pending(pool, schema=…)` died the untyped `AttributeError` (the Pool carries no `transaction()`; the annotation promises a `Connection`, the runtime never checks) | the typed refusal whose message NAMES the Connection (one `asyncpg.Connection` — the caller's transaction scope; acquire from the pool) |
| F-RV3-4 (the E2-analog gate hole) | `test_rv3_4a_a_declared_gate_the_body_never_waits_is_a_build_refusal` + `test_rv3_4b_a_wait_with_no_declared_gate_is_a_build_refusal` + `test_rv3_4c_the_conditional_interior_wait_is_not_the_static_refusal` | xfail-strict ×2 + GREEN guard ×1 | BOTH directions validated CLEAN: a `GateDecl` whose body carries NO `wait_signal` reference at all; a body's `ctx.wait_signal` with NO declared gate — the compile-visible gate seat and the bodies' waits never walked against each other (E2 walks the promise wiring; the gate wiring had no walk) | the named E14 build refusal on BOTH provable faces; the conditional-interior wait NEVER convicted (the static walk cannot prove a branch never fires — the documented C9/W-face, F-RV3-12's rule) |
| F-RV3-5 (the -m path's dead verbs) | `test_rv3_5_the_dash_m_path_serves_the_whole_verb_inventory` | xfail-strict (subprocess) | `python -m taskq.cli --help` served a SHORTER verb inventory than the imported app's — the `__main__` block at `cli.py:2676` runs `main()` mid-module; the workgroup/flows tail's commands are unregistered when it fires | the -m path's help serves EVERY verb the imported app registers (the block at EOF — the module fully loads before `main()` runs) |
| F-RV3-6 (two schema conventions) | `test_rv3_6_the_schema_param_carries_one_convention` | xfail-strict (static) | `HitlClient(pool, *, schema)` keyword-only vs `FlowRunner(compiled, pool, schema)` + `run(flow, pool, schema)` positional — two conventions among the read-face siblings | ONE convention — POSITIONAL (the majority); `HitlClient` is unreleased, the fix is free; keyword call sites keep working (positional-tolerant) |
| F-RV3-7 (result()'s failure face) | `test_rv3_7_a_failed_run_s_result_read_names_the_failure` | xfail-strict (integration) | a FAILED run's `FlowRunner.result()` returned `{}` (the empty dict — failed and running indistinguishable at the read face); `WorkflowRunError` exists and the read never raises it | the read raises the typed `WorkflowRunError` carrying the failing row's error class + message (the failed run's first question — WHAT failed, WHY — answered at the face) |
| F-RV3-8 (the wrapper defeats the stamper) | `test_rv3_8_the_actor_wrapped_bodies_stamp_their_own_code` | xfail-strict (integration) | every `@app.actor` node logged `node.code-version-unstamped` (`inspect.getsource` refuses the wrapper: "module, class, method, function, traceback, frame, or code object was expected, got WorkflowActor") — `code_version` NULL on every node; §22.1's deploy-drift record dead where the docs teach the canonical path | the stamper unwraps the `WorkflowActor` to its INNER function — each node's stamp equals the body's own `compute_code_version`, distinct per body |
| F-RV3-9 (the hold's two reason homes) | `test_rv3_9_the_hold_s_reason_rides_its_own_field` | xfail-strict (integration) | the wait site's `reason="awaiting compliance sign-off"` landed in the row's `payload['reason']` (populated) while `HoldContext.reason` read `None` — hardcoded at `_context` (`_hitl.py:879`) | ONE home: `.reason` is the TYPED READ FACE fed from the insert's payload (the row is the truth; the field never lies again) |
| F-RV3-10 (the double-sourced gate timeout) | `test_rv3_10_the_gate_timeout_sources_agree_or_the_drift_is_named` + `test_rv3_10b_the_gate_decl_docstring_names_the_union_face` | xfail-strict ×2 (static) | `GateDecl(timeout_s=30.0)` + the body's literal `timeout_s=45.0` validated CLEAN — the two sources never cross-checked; the `GateDecl` docstring stated the runtime raise face but never the split's precedence (what the Decl feeds vs what arms the runtime) | the W4 rule names the drift (BOTH values in the message); the docstring states the UNION face — the wait site's value arms the runtime, the declaration feeds the compile surfaces (Mermaid, W1), the drift cross-checked where both are literals |
| F-RV3-12 (the loop shape law, docstring-enforced) | `test_rv3_12_a_conditional_loop_wait_is_a_named_warning` + `test_rv3_12b_the_unconditional_loop_wait_stays_clean` | xfail-strict ×1 + GREEN guard ×1 | a loop body whose `wait_signal` sits in a CONDITIONAL interior validated CLEAN — the "ONE wait per iteration" law lived only in the docstring; the conditional wait mis-indexes the answer cursor (the T26 review's C9 is the same question: the iteration counter IS the cursor) | the W5 rule names the conditional-interior wait on a loop node; the UNCONDITIONAL wait never convicted (the zero-false-positive bound) |

## Notes for the fixer

* E14 is the next free E-id at the head (the validator's table ends at
  E13-gate-door); the W-ids W4 (the timeout drift) and W5 (the loop
  shape) are the next free W-ids (W1–W3 shipped; W2 carries two faces).
* E14's conviction faces are exactly TWO (the reviewer's own rule): a
  declared gate with NO `wait_signal` reference in the node's bodies AT
  ALL (the provable case), and a `wait_signal` with NO declared gate
  (the other provable case). The conditional-interior case is NOT
  statically provable — it is the documented C9/W-face (F-RV3-12's W5
  for the loop-kind node, where the mis-index bites).
* The zero-false-positive doctrine holds throughout: an unresolvable
  body source (no source, a builtin, a partial) SKIPS the walk — a
  guess is never convicted.
* rv3-6's cure must keep the keyword call sites working (the estate's
  `HitlClient(pool, schema=…)` calls are keyword today — positional-
  tolerant accepts them unchanged).
* rv3-2's pin creates a BARE schema (`{module}_rv3_bare`) and drops it
  in a `finally` — the G7 teardown never sees it (it is not the
  module's own schema).

## THE OUTCOME AT THE CURE LANE (the flips)

Recorded per cure (each cure lands WITH its pin in the same commit —
the marker removed, the pin the green guard):

| Finding | The cure's root cause | The cure's SHA | The pin's flip |
|---|---|---|---|
| F-RV3-4 | the validator walked the gate SEAT (E13) but never the gate WIRING — the compile knew the GateDecls and never read the bodies' waits against them (E2's analogy unextended) | `a7560072` — E14-gate-wiring (the AST source walk, `_wait_signal_sites` + `_hints.inner_fn`, the two provable faces; the conditional-interior bound kept) | 4a/4b markers REMOVED (green guards); 4c stayed green throughout |
| F-RV3-7 | `result()` read the terminal row's jsonb without reading the RUN's status — the failure verdict lived on the rows (the node's error_class/message) unread by the face | `da7ac919` — the status check + `_failure_verdict` (the newest terminal-failed node row's class + message; the root's `UnabsorbedNodeFailure` named as the cascade's face) | rv3-7 marker REMOVED |
| F-RV3-5 | the `__main__` block sat at `cli.py:2676` — `main()` fired mid-module, the typer app ran on the half-built registry and SystemExit'ed before the tails registered | `0a0b328a` — the block at EOF (the module fully loads before `main()`; the -m path serves the console script's own face) | rv3-5 marker REMOVED |
| F-RV3-2 | the packaged door (`run`) never wrapped its first contact in the client arm's translation | `301a103e` — the UndefinedTableError → `SchemaNotMigratedError` translation at `create_flow` (chained, the remedy in the message) | rv3-2 marker REMOVED |
| F-RV3-3 | the annotation promised a `Connection`, the runtime never checked; a Pool died the `AttributeError` at the first `transaction()` | `301a103e` — the Connection-or-Pool union + `MigrationConnectionError` (the Pool refused BY NAME, the acquire-and-pass remedy in the message) | rv3-3 marker REMOVED |
| F-RV3-8 | the stamper read the node body's identity raw — the `@app.actor` handle IS the body at the node, and `getsource` refuses an instance | `5a35a1c8` — `_stamp_code_version` unwraps through `_hints.inner_fn` (the one-unwrap seam; each canonical node stamps its OWN body's hash) | rv3-8 marker REMOVED |
| F-RV3-6 | two conventions among the read-face siblings; the minority (keyword-only) sat on the unreleased class | `5a35a1c8` — `HitlClient(pool, schema, *, redact=None)` — POSITIONAL (the majority), keyword call sites keep working | rv3-6 marker REMOVED |
| F-RV3-9 | `_context` hardcoded `reason=None` — the reason's only home was the payload doc the INSERT wrote | `5a35a1c8` — `.reason` is the typed read face fed from the payload (the masked text the redact chain wrote) | rv3-9 marker REMOVED |
| F-RV3-10 | the timeout's two sources had no precedence statement and no cross-check; the declaration's docstring documented the raise face, not the split | `565b8bd7` — the union face on `GateDecl` (+ the runtime half on `wait_signal`) and `W4-gate-timeout-split` (both literals, disagreeing → the named drift; the match key is the payload model's NAME) | rv3-10/rv3-10b markers REMOVED |
| F-RV3-1 | the `key=` param was the accept-and-REFUSE seat — a param whose every value raises is a trap, not API | `565b8bd7` — the param DELETED; the removal documented (the changelog's Unreleased section + the guide's map section — the preserve law) | rv3-1 marker REMOVED; rv3-1b stayed green (the documentation face) |
| F-RV3-12 | the loop's shape law lived only in the docstring; the conditional-wait mis-index (the T26 review's C9) had no name | `565b8bd7` — `W5-loop-wait-shape` (the LOOP-kind conditional-interior wait is the named warning; the unconditional shape kept; the plain STEP's conditional wait is the per-attempt cursor's honest replay) | rv3-12 marker REMOVED; rv3-12b stayed green |

The E-state after the lane: E1–E14; the W-state: W1, W2 (two faces),
W3, W4, W5. The reference's validate table carries ALL of them (the
count sentence says TWENTY); the docs-numbers pin (DOCS-5) greens at
the final head.

## THE CAPABILITY RE-VERIFICATION (the review-head discrepancy)

The teardown reviewer's "rate limiting + DI verified absent" ran
against a STALE head. At THIS lane's head, BOTH capabilities are
LANDED and pinned green:

* **DI** — `WorkflowApp(deps=…)` (the authoring door,
  `api/_app.py`), the runner door's `deps=` override, the packaged
  door's compiled binding, and the E12-deps-contract rule's build-time
  convictions: `tests/test_wf_deps_pins.py` — 12 pins green (the happy
  path, the map child, the loop, the hold-wake — the SAME instance
  through every invocation face — plus the four refusal faces and both
  door-binding pins).
* **the flow rate-limit gate** — `tests/test_wf_flow_rate_limit_pins.py`
  — 5 pins green (the deny observable naming the bucket, the cadence
  one-slot-never-bursts, the no-caps fast path, the dependency failure
  fail-closed, the door releasing the slot while the flow holds).

The discrepancy is the census's honesty row: the review's ABSENT
verdict was true at ITS head and false at the lane's — the
capabilities landed between the two heads.
