# PINMAP — the rv4 pin pack (T27 typed-route hostile review)

File: `tests/test_wf_typed_route_rv4_pins.py` (drop-in at the repo's
`tests/`; sha256
`a5bb094cda52ec0ae64b614783458e7635226f9aca4e2b10785dbc48cd214eb3`;
ruff-clean and pyright-clean against the repo configs).
Tree pinned at `fab8dbe6` (feat/taskqflow — the T27 typed-route merge
head).
House fixtures used: `wf_conn` / `wf_schema` / `module_pg_pool` /
`wf_sql` (tests/_wf_fixtures.py via tests/conftest.py),
`fire_count` from tests/_wf_fixtures.py; `TASKQ_TEST_PG_DSN` for the PG
lane. The three PG pins are marked `integration`; the twelve
static/build pins carry no marker. The async PG pins pick up the
estate's always-on G7 rows↔status teardown automatically (file prefix
`test_wf_`): the wedged/failed states the red paths leave behind were
checked G7-quiet (a wedged root with a live pending/running row
reconstructs `running` — agreement, no false teeth; the red and marked
runs carried zero teardown errors).

Convention for the "red verbatim" column: the first two lines of the
pin's OWN failure signature in the unmarked run (`red-run-raw.txt`) —
the assertion/raising line and the first `E`-line(s) carrying the
conviction.

| Finding | Pin name | Red verbatim (first 2 lines) | Status |
|---|---|---|---|
| F-RV4-1a (empty route + consumer → eternal wedge) | `test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list` | `assert verdict == "terminal", (`<br>`E   AssertionError: F-RV4-1a: the empty route WEDGED the run (drive returned 'max_ticks') — the join row never spawns and the consumer's reserved dep never releases` | xfail-strict (integration) |
| F-RV4-1b (empty route, join terminal → SUCCEEDED, result()=None) | `test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum` | `assert join_row is not None, (`<br>`E   AssertionError: F-RV4-1b: the join row never spawned — the run terminalized SUCCEEDED having routed NOTHING (result() is None below)` | xfail-strict (integration) |
| F-RV4-2 (>1000 fan-in → raw ValueError out of drive, source wedges running) | `test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal` | `verdict = await runner.drive(flow_id, tick=0.02, max_ticks=120)  →  src/taskq/workflows/api/_runner.py:746: in _execute_claimed`<br>`E   ValueError: join 'media.join' fans in 1001 children — above the declared maximum fan-in per join (1000). Use JoinSpec(child_driven=True) …` | xfail-strict (integration) |
| F-RV4-3 (zero-param arm builds clean) | `test_rv4_3_a_zero_param_arm_is_a_build_refusal` | `with pytest.raises(WorkflowValidationError) as exc_info:`<br>`E   Failed: DID NOT RAISE WorkflowValidationError` | xfail-strict |
| F-RV4-3 (two-param arm builds clean) | `test_rv4_3_a_two_param_arm_is_a_build_refusal` | `with pytest.raises(WorkflowValidationError) as exc_info:`<br>`E   Failed: DID NOT RAISE WorkflowValidationError` | xfail-strict |
| F-RV4-4 (superclass arm param → silent truncation) | `test_rv4_4_a_superclass_arm_param_is_a_build_refusal` | `with pytest.raises(WorkflowValidationError) as exc_info:`<br>`E   Failed: DID NOT RAISE WorkflowValidationError` | xfail-strict |
| F-RV4-4 (bare BaseModel arm param → runtime PydanticUserError) | `test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal` | `with pytest.raises(WorkflowValidationError) as exc_info:`<br>`E   Failed: DID NOT RAISE WorkflowValidationError` | xfail-strict |
| F-RV4-5 (same-tag twins collapse to one arm) | `test_rv4_5_duplicate_type_tags_refuse_at_the_verb` | `with pytest.raises(WorkflowBuildError) as exc_info:`<br>`E   Failed: DID NOT RAISE WorkflowBuildError` | xfail-strict |
| F-RV4-6 (field-identical union members → consumer decodes B as A) | `test_rv4_6_field_identical_union_members_are_a_build_diagnostic` | `assert "Rv4SumA" in message and "Rv4SumB" in message, (`<br>`E   AssertionError: F-RV4-6: no build-time diagnostic named the field-identical members — the consumer's silent mispick ships. Build surfaces said: ''` | xfail-strict |
| F-RV4-7a (arm wait, no gate → no E14) | `test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned` | `assert any(d.severity == "warning" and "media" in d.message for d in e14), (`<br>`E   AssertionError: F-RV4-7a: the arm's wait_signal raised no E14 warning (the arm-held hold is compile-invisible) — E14 said: []` | xfail-strict |
| F-RV4-7b (gate on source, wait in arm → E14 false conviction) | `test_rv4_7b_the_source_gate_covers_the_arms_waits` | `compiled = app.get("rv4_7b_arm_wait_gated_source")`<br>`E   taskq.workflows.api._validate.WorkflowValidationError: [error] validate: 1 error(s) — E14-gate-wiring: node 'media' declares gate(s) 'Rv4Approval' but its body NEVER waits …` | xfail-strict |
| F-RV4-8 (map-after-route → accidental "declared twice") | `test_rv4_8_every_double_attach_direction_speaks_the_designed_message` | `assert "already carries a map" in message, (`<br>`E   AssertionError: F-RV4-8 (map-after-route): the ACCIDENTAL refusal fired instead of the designed double-attach message — got: node 'media.join' is declared twice in one workflow — a step key is the ledger's identity, never a shadow` | xfail-strict |
| F-RV4-9 (unresolvable-but-declared → "does not declare" lie) | `test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared` | `assert "does not declare" not in message, (`<br>`E   AssertionError: F-RV4-9: the message claims UNDECLARED for a DECLARED-but-unresolvable annotation — the lie: route's source 'docs' does not declare a list[...] return …` | xfail-strict |
| F-RV4-9 (the undeclared face stays honest) | `test_rv4_9_an_undeclared_return_refuses_named` | — (green at head: refused `WorkflowBuildError` matching `does not declare`) | **GREEN guard** |
| F-RV4-10 (typo'd arm queue → zero W2) | `test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues` | `assert any("gpuu-typo" in d.message for d in w2), (`<br>`E   AssertionError: F-RV4-10: the typo'd ARM queue raised no W2 warning — the queue-vocabulary walk is blind to arm queues: []` | xfail-strict |

## The SAFE behavior each pin asserts (what flips it XPASS-strict)

* **rv4-1a/1b** — the empty source list is a DEFINED outcome: the
  route's join FIRES exactly once with `[]` (one `wf_join_fire` row,
  the join row succeeded); with a consumer, the consumer receives `[]`
  and the run terminalizes SUCCEEDED with `result()` == the consumer's
  empty-list answer (`{"count": 0}`); with the join as terminal,
  `result()` == `[]` (never `None`, never a wedge).
* **rv4-2** — a route over `MAX_FAN_IN_PER_JOIN+1` elements is a TYPED,
  LADDERED, NAMED refusal: `drive()` never raises a raw error through
  the driver; the source row terminal-FAILS with a non-empty
  `error_class` whose message names the fan-in bound; the root row is
  `failed`; a re-drive answers `'terminal'` quietly (no re-crash loop).
* **rv4-3 ×2** — the zero-param and two-param arms are BUILD refusals
  (an E-rule's aggregated report naming the arm body) — they never
  reach the runtime's `TypeError` ladder burn.
* **rv4-4 ×2** — the superclass (`DocBase` over member `TextDoc`) and
  bare-`BaseModel` arm params are BUILD refusals under
  `E15-route-totality` naming the arm; the superclass leg's message
  names the TRUNCATION hazard (`truncat…`).
* **rv4-5** — duplicate `type_tag` keys refuse at the verb
  (`WorkflowBuildError`) naming the colliding tag (`Rv4Twin`) and the
  duplication.
* **rv4-6** — a routed sum whose members are field-identical (no
  discriminator) raises a build-time diagnostic — the verb's door OR
  the validator's re-proof — naming BOTH members and the decode hazard.
* **rv4-7a** — an arm-held `wait_signal` with no declared gate produces
  an `E14-gate-wiring` WARNING naming the route's source node.
* **rv4-7b** — a gate declared on the route's SOURCE covers the arms'
  waits: no E14 diagnostic either direction (the zero-false-positive
  doctrine applied to the arm walk).
* **rv4-8** — all four double-attach directions (route→route,
  map→route, route→map, map→map) refuse with the DESIGNED message
  ("already carries a map …"); the accidental "declared twice" message
  never fires.
* **rv4-9** — the verb's refusal for a DECLARED-but-unresolvable return
  never claims "does not declare"; it names the resolution failure
  ("…resolv…"). The GREEN guard keeps the truly-undeclared face's
  "does not declare a list" message.
* **rv4-10** — `W2-unknown-queue` covers the ARMS' queues: a typo'd
  `RouteArm(queue="gpuu-typo")` warns, naming the queue.

## Decision flags for the maintainer (the directions I chose where the
## finding left the rule open)

1. **F-RV4-1's semantics** — pinned per the doctrine's named precedent
   (the map's empty-join shape: the join FIRES with `[]`). NOTE: at
   this head the MAP face does not implement that shape either —
   `map_source` over `[]` dies with the raw `ValueError: an empty fork
   (zero children) is refused at build time` out of the finalize tx
   (the source wedges `running`; repro-runtime.txt, "PRECEDENT PROBE").
   `_map_fork`'s own docstring ("the map over nothing — the join fires
   empty, the collect states it") is aspirational. The route pins'
   acceptance is the finding's scope; the map face's same cure is
   flagged here as a same-class defect the fixer may want to fold in
   (or consciously refuse — a fork-join over zero elements could also
   be argued a build/flow error, but then BOTH faces need the typed,
   laddered refusal rather than today's raw escape / silent skip).
2. **F-RV4-2's cure shape** — the pin asserts the named-refusal shape
   from the finding. If the maintainers instead teach the route the
   `child_driven` join (make the >1000 fan-in LEGAL), rewrite the pin:
   source succeeded, 1001 child rows, the join fires exactly once. The
   pin's other assertions (no raw escape, no wedge, no re-crash) stand
   either way. The reclaim re-crash loop is the fleet/worker-hosted
   face (the dev-loop driver's tick carries no reclaim arm); the pin's
   red convicts the raw escape + the `running` wedge, and its green
   leg (the quiet re-drive) covers the loop's absence.
3. **F-RV4-3's rule seat** — the pin accepts `E10-arity` (the walk
   extended to `map_arms`), `E12-deps-contract` (the two-param face),
   or `E15-route-totality` (the route's own rule owning the arms'
   shape), and requires the arm's name in the message. The two-param
   arm currently collides with the deps shape's COUNT (`wired+1`), so
   an E10-extension alone would read it as the deps opt-in — the cure
   must decide whether arms may declare deps at all (E12's walk does
   not cover them today either: an arm declaring the deps shape with no
   app deps builds clean and TypeErrors at runtime).
4. **F-RV4-4's exact rule** — pinned: the arm param must be EXACTLY the
   union member; a superclass/BaseModel param refuses naming the
   truncation hazard. The finding's alternative ("or the member's
   subclass with the fields revalidated") is NOT pinned — if the
   maintainers admit member-subclasses with revalidation, the pins here
   stand unchanged (they convict the SUPERCLASS direction only).
5. **F-RV4-6's rule-vs-docs** — pinned as the build-time diagnostic
   (the finding's first direction). If the maintainers choose
   docs-law-only, rewrite this pin to pin the docs law's presence (the
   guide's route section carrying the discriminator law verbatim). The
   rule's SEAT is also open (the route verb at the arms' return types /
   the validator's re-proof / E5's consumer-compat walk): the pin
   accepts any build surface that names both members + the hazard.
6. **F-RV4-7's contract** — pinned: E14 walks `map_arms`; the arm wait
   with no gate is the WARNING face (the T26 ruling's
   waited-never-declared severity), and the source's declared gate
   covers the arms' waits (per-arm gate declarations do not exist — the
   source's seat is the arms' seat). If the maintainers instead add
   per-arm gate declarations, rv4-7b's clean-shape assertion needs the
   new wiring's shape.
7. **F-RV4-9's green guard** — if the cure rewords the UNDECLARED
   face's message, update the guard's `match="does not declare"` to
   the new wording (the guard exists to keep the two faces distinct,
   not to freeze the string).
8. **F-RV4-10's alternative** — the pin asserts the W2 coverage. The
   finding's documented alternative (the docstring names the deliberate
   exclusion) is NOT pinned; if chosen, rewrite as a docstring-content
   pin.

## Unreproducible findings

NONE — all 10 findings reproduced live at `fab8dbe6` before pinning
(the pre-pin repro shapes + outputs are in `repro/` and RECEIPTS.md).
Two repro legs hit MY OWN artifacts and were corrected before pinning
(the convictions stand on the corrected legs, both kept for
provenance): F-RV4-6's round-1 leg defined the union members
closure-locally (annotations unresolvable → the codec returned raw
dicts — itself a demonstration of the zero-false-positive skip); the
module-level models convicted the mispick. F-RV4-9's round-1 leg bound
the factory-built model at module level (the name resolvable → the
refusal never reached); keeping the name off the module namespace
convicted the message lie. Neither changes a severity.
