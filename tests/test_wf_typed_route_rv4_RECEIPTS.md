# RECEIPTS — the rv4 pin pack's red-first protocol

Pin pack: `test_wf_typed_route_rv4_pins.py` (the T27 typed-route
internals-review convictions, feat/taskqflow @ `fab8dbe6`). Doctrine:
every landed finding was run UNMARKED first (the red captured below),
then marked `pytest.mark.xfail(strict=True, reason="LIVE FINDING
rv4-<id>: …")` and re-run (the strict-xfail run captured below). The one
green pin (rv4-9's undeclared leg) is the behavior-already-correct guard
(see PINMAP.md).

## Environment

- Tree: scratch copy `/tmp/opencode/pinwork-rv4` of
  `/tmp/opencode/gatecheck` @ `fab8dbe6` (a synced review copy at the
  right head; the runs below never touched gatecheck, author-wt, or
  routeread). The synced uv venv's editable install pointed at
  gatecheck's tree; the scratch copy's
  `.venv/lib/python3.13/site-packages/_editable_impl_taskq_py.pth` was
  rewritten to `/tmp/opencode/pinwork-rv4/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-rv4/src/taskq/__init__.py`). Every run also
  carried `PYTHONPATH=/tmp/opencode/pinwork-rv4/src`.
- Python: `/tmp/opencode/pinwork-rv4/.venv/bin/python` (3.13.15,
  pytest 9.1.1, asyncpg).
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`; per-module databases/schemas are the fixture
  machinery's own (`TASKQ_TEST_RUN_TOKEN=rv4pin*` mixed into the hashed
  names); the pre-pin repro scripts used schema `rv4pin_repro`, dropped
  in-place. Post-run hygiene verified after every leg: zero `tq_db_%`
  databases and zero `tq_%`/`rv4%` schemas remain on the shared
  container.
- Unmarked variant: `tests/test_wf_typed_route_rv4_pins_unmarked.py` —
  the pin file with the 14 `pytest.mark.xfail` decorator blocks stripped
  (mechanical balanced-paren strip, AST-verified: ONLY xfail decorators
  removed, all 61 functions + 18 non-xfail decorators preserved —
  `repro/make_unmarked.py`), nothing else changed.
- Gates: `ruff check` clean on BOTH variants; `pyright` (the repo's
  `pyright src/taskq tests` gate's config) clean on the pin file
  (0 errors, 0 warnings).

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

Scripts + full outputs in `repro/` (`repro-build-time.txt`,
`repro-runtime.txt`, `repro-runtime2.txt`, `repro-static2.txt`). The
verbatim convictions:

- **F-RV4-1a** (consumer shape): `route()` over a source returning `[]`
  with a consumer of the join — `drive()` returned `max_ticks`; rows:
  `'media' succeeded`, `'consume' pending deps_pending=1`, `'__flow__'
  running`; `media.join` row NEVER SPAWNED. The eternal wedge.
- **F-RV4-1b** (terminal shape): the join as `build()`'s terminal —
  `drive()` returned `terminal`, root `succeeded`, `result()` → `None`.
  Succeeded-having-routed-nothing through the empty-corpus door.
- **PRECEDENT NOTE**: the MAP face over `[]` at this head ALSO dies —
  `map-over-[] drive RAISED: ValueError: an empty fork (zero children)
  is refused at build time: …` out of the finalize tx (the source
  wedged `running`). `_map_fork`'s docstring ("the map over nothing —
  the join fires empty") names the DESIGNED semantics; the code never
  implements it (the empty-children ForkSpec trips `validate_fork`
  inside tx1). The rv4-1 pins assert the designed empty-join semantics
  for the ROUTE; the map face's same cure is flagged in PINMAP.md.
- **F-RV4-2**: `route()` over 1001 elements — `drive()` RAISED raw
  `ValueError: join 'media.join' fans in 1001 children — above the
  declared maximum fan-in per join (1000). Use JoinSpec(child_driven=
  True) …` through `_execute_claimed` (api/_runner.py:746 — outside the
  ladder's try); rows: `'media' running` (wedged), `'consume' pending`,
  root `running`. The dev-loop driver's tick carries no reclaim arm (the
  reclaim re-crash is the fleet/worker-hosted face); the in-process
  conviction is the raw escape + the eternal `running` wedge.
- **F-RV4-3**: zero-param arm + two-param arm both BUILT CLEAN (no
  E-rule fires). Runtime face: the zero-param child terminal-failed
  `error_class='TypeError'`, message `arm_zero() takes 1 positional
  argument but 2 were given` — the ladder burning the wiring-time lie.
- **F-RV4-4**: `item: DocBase` (the member `TextDoc`'s superclass) BUILT
  CLEAN; at runtime the arm received `{'type': 'DocBase', 'has_text':
  False, 'dump': {'doc_id': 'd1'}}` — the subclass's `text` field
  SILENTLY TRUNCATED (pydantic extra='ignore'). `item: BaseModel` BUILT
  CLEAN; the child terminal-failed `error_class='PydanticUserError'`
  ("Pydantic models should inherit from BaseModel, BaseModel cannot be
  instantiated directly").
- **F-RV4-5**: `create_model("Twin", …)` twice → both tags
  `__main__.Twin`, distinct types; the route BUILT CLEAN with
  `map_arms` carrying ONE entry — `twin_arm_b` (the second arm)
  silently overwrote `twin_arm_a`.
- **F-RV4-6**: arms returning the field-identical `SumA`/`SumB`
  (`doc_id, note` both) — the fork dispatched element d2 to arm_b (the
  row `media.item:__main__.AudioItem` carried `note="from-b"`) but the
  consumer's `list[SumA | SumB]` decode delivered `['SumA', 'SumA']` —
  the SumB arrived AS SumA, silently. (Round-1 leg with closure-local
  models was inconclusive — annotations unresolvable → the codec
  returned raw dicts; round 2 with module-level models convicted.
  Both outputs kept in repro/.)
- **F-RV4-7a**: an arm body calling `ctx.wait_signal(Approval)` with no
  declared gate — `validate_compiled` returned ZERO E14 diagnostics.
- **F-RV4-7b**: the gate declared on the SOURCE with the wait in the
  ARM — `app.get` raised `WorkflowValidationError: … E14-gate-wiring:
  node 'media' declares gate(s) 'Approval' but its body NEVER waits …`
  — the false conviction (the arm DOES wait).
- **F-RV4-8**: `map_source` after `route` refused with the ACCIDENTAL
  `WorkflowBuildError: node 'media.join' is declared twice in one
  workflow — a step key is the ledger's identity, never a shadow`
  (the designed `already carries a map or a route` fires on the other
  three directions — route-after-route, route-after-map, map-after-map).
- **F-RV4-9**: factory-built model kept off the module namespace →
  `body_hints` `{}` for a source whose `__annotations__['return']`
  EXISTS (`'list[UnseenDoc]'`) → the verb refuses with `route's source
  'docs' does not declare a list[...] return — …` — the message claims
  UNDECLARED for a DECLARED-but-unresolvable annotation. (Round-1 leg
  bound the model at module level and the closure/global seam resolved
  it — the corrected leg keeps the name off the namespace.)
- **F-RV4-10**: `RouteArm(queue="gpuu-typo")` with the queue universe
  declared (`TASKQ_QUEUES=gpu,io`; `known_queues={'gpu','default',
  'io'}`) — `validate_compiled` returned ZERO W2 diagnostics.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
PYTHONPATH=/tmp/opencode/pinwork-rv4/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv4pin3 \
/tmp/opencode/pinwork-rv4/.venv/bin/python -m pytest \
  tests/test_wf_typed_route_rv4_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FFFFFFFFFFFFF.F                                                          [100%]
=================================== FAILURES ===================================
________ test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list ________
tests/test_wf_typed_route_rv4_pins_unmarked.py:203: in test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list
    assert verdict == "terminal", (
E   AssertionError: F-RV4-1a: the empty route WEDGED the run (drive returned 'max_ticks') — the join row never spawns and the consumer's reserved dep never releases
E   assert 'max_ticks' == 'terminal'
E     
E     - terminal
E     + max_ticks
_______ test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum ________
tests/test_wf_typed_route_rv4_pins_unmarked.py:268: in test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum
    assert join_row is not None, (
E   AssertionError: F-RV4-1b: the join row never spawned — the run terminalized SUCCEEDED having routed NOTHING (result() is None below)
E   assert None is not None
______ test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal _______
tests/test_wf_typed_route_rv4_pins_unmarked.py:325: in test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal
    verdict = await runner.drive(flow_id, tick=0.02, max_ticks=120)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_runner.py:381: in drive
    ran = await self.tick(flow_id, execute=execute)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_runner.py:396: in tick
    await self._run_node(flow_id, row)
src/taskq/workflows/api/_runner.py:446: in _run_node
    await self._execute_claimed(flow_id, row, attempt, ledger_id, node, body)
src/taskq/workflows/api/_runner.py:746: in _execute_claimed
    await self._finalize_success(
src/taskq/workflows/api/_runner.py:1027: in _finalize_success
    final = await finalize_node(
src/taskq/workflows/engine.py:691: in finalize_node
    applied = await _deadlock_retry(_tx1)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/engine.py:203: in _deadlock_retry
    return await coro_factory()
           ^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/engine.py:671: in _tx1
    return await _run_tx1(
src/taskq/workflows/engine.py:309: in _run_tx1
    await insert_fork(
src/taskq/workflows/_fork.py:48: in insert_fork
    validate_fork(fork)  # the empty fork is a build-time refusal, never a runtime dragon
    ^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/definitions.py:204: in validate_fork
    raise ValueError(
E   ValueError: join 'media.join' fans in 1001 children — above the declared maximum fan-in per join (1000). Use JoinSpec(child_driven=True) (the child-driven shape: the fire counts terminal children from the edge ledger, never a per-joined-row edge list) or partition the map.
________________ test_rv4_3_a_zero_param_arm_is_a_build_refusal ________________
tests/test_wf_typed_route_rv4_pins_unmarked.py:400: in test_rv4_3_a_zero_param_arm_is_a_build_refusal
    with pytest.raises(WorkflowValidationError) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE WorkflowValidationError
________________ test_rv4_3_a_two_param_arm_is_a_build_refusal _________________
tests/test_wf_typed_route_rv4_pins_unmarked.py:420: in test_rv4_3_a_two_param_arm_is_a_build_refusal
    with pytest.raises(WorkflowValidationError) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE WorkflowValidationError
_____________ test_rv4_4_a_superclass_arm_param_is_a_build_refusal _____________
tests/test_wf_typed_route_rv4_pins_unmarked.py:477: in test_rv4_4_a_superclass_arm_param_is_a_build_refusal
    with pytest.raises(WorkflowValidationError) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE WorkflowValidationError
___________ test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal ___________
tests/test_wf_typed_route_rv4_pins_unmarked.py:500: in test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal
    with pytest.raises(WorkflowValidationError) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE WorkflowValidationError
______________ test_rv4_5_duplicate_type_tags_refuse_at_the_verb _______________
tests/test_wf_typed_route_rv4_pins_unmarked.py:551: in test_rv4_5_duplicate_type_tags_refuse_at_the_verb
    with pytest.raises(WorkflowBuildError) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE WorkflowBuildError
_______ test_rv4_6_field_identical_union_members_are_a_build_diagnostic ________
tests/test_wf_typed_route_rv4_pins_unmarked.py:619: in test_rv4_6_field_identical_union_members_are_a_build_diagnostic
    assert "Rv4SumA" in message and "Rv4SumB" in message, (
E   AssertionError: F-RV4-6: no build-time diagnostic named the field-identical members — the consumer's silent mispick ships. Build surfaces said: ''
E   assert ('Rv4SumA' in '')
___________ test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned ____________
tests/test_wf_typed_route_rv4_pins_unmarked.py:666: in test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned
    assert any(d.severity == "warning" and "media" in d.message for d in e14), (
E   AssertionError: F-RV4-7a: the arm's wait_signal raised no E14 warning (the arm-held hold is compile-invisible) — E14 said: []
E   assert False
______________ test_rv4_7b_the_source_gate_covers_the_arms_waits _______________
tests/test_wf_typed_route_rv4_pins_unmarked.py:698: in test_rv4_7b_the_source_gate_covers_the_arms_waits
    compiled = app.get("rv4_7b_arm_wait_gated_source")
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_app.py:407: in get
    validate_compiled(compiled)
src/taskq/workflows/api/_validate.py:141: in validate_compiled
    raise WorkflowValidationError("validate", "error", f"{len(errors)} error(s) — {report}")
E   taskq.workflows.api._validate.WorkflowValidationError: [error] validate: 1 error(s) — E14-gate-wiring: node 'media' declares gate(s) 'Rv4Approval' but its body NEVER waits — no wait_signal reference anywhere in the body: the declared hold seat has no waiter (work that waits for nobody). Wire the body's ctx.wait_signal to the declared payload models, or drop the gate declaration
_____ test_rv4_8_every_double_attach_direction_speaks_the_designed_message _____
tests/test_wf_typed_route_rv4_pins_unmarked.py:760: in test_rv4_8_every_double_attach_direction_speaks_the_designed_message
    assert "already carries a map" in message, (
E   AssertionError: F-RV4-8 (map-after-route): the ACCIDENTAL refusal fired instead of the designed double-attach message — got: node 'media.join' is declared twice in one workflow — a step key is the ledger's identity, never a shadow
E   assert 'already carries a map' in "node 'media.join' is declared twice in one workflow — a step key is the ledger's identity, never a shadow"
_____ test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared ______
tests/test_wf_typed_route_rv4_pins_unmarked.py:808: in test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared
    assert "does not declare" not in message, (
E   AssertionError: F-RV4-9: the message claims UNDECLARED for a DECLARED-but-unresolvable annotation — the lie: route's source 'docs' does not declare a list[...] return — the route is a MAP face: the source produces the elements' list, the arms key its members
E   assert 'does not declare' not in "route's sou... its members"
E     
E     'does not declare' is contained here:
E     ?                       ^
E       route's source 'docs' does not declare a list[...] return — the route is a MAP face: the source produces the elements' list, the arms key its members
E     ?                       ^^^^^^^^^^^^^ ++++
__________ test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues __________
tests/test_wf_typed_route_rv4_pins_unmarked.py:865: in test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues
    assert any("gpuu-typo" in d.message for d in w2), (
E   AssertionError: F-RV4-10: the typo'd ARM queue raised no W2 warning — the queue-vocabulary walk is blind to arm queues: []
E   assert False
=========================== short test summary info ============================
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_3_a_zero_param_arm_is_a_build_refusal
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_3_a_two_param_arm_is_a_build_refusal
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_4_a_superclass_arm_param_is_a_build_refusal
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_5_duplicate_type_tags_refuse_at_the_verb
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_6_field_identical_union_members_are_a_build_diagnostic
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_7b_the_source_gate_covers_the_arms_waits
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_8_every_double_attach_direction_speaks_the_designed_message
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared
FAILED tests/test_wf_typed_route_rv4_pins_unmarked.py::test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues
14 failed, 1 passed in 4.38s
```

(The one pass is the F-RV4-9 green guard — the undeclared face's
message, correct today. The two generator-expression `where` lines
pytest renders for the `any(...)` assertions are trimmed above at the
`E   assert False` line's tail; `red-run-raw.txt` is the untrimmed
byte-exact capture.)

Determinism: the marked run was executed TWICE (rv4pin4, rv4pin5) with
identical results; the reds are structural (missing rules, wrong
messages, wedged runs), not timing — the wedge pins' `max_ticks=120`
bound only bounds the red's wall clock (~1s/pin).

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
PYTHONPATH=/tmp/opencode/pinwork-rv4/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv4pin4 \
/tmp/opencode/pinwork-rv4/.venv/bin/python -m pytest \
  tests/test_wf_typed_route_rv4_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xxxxxxxxxxxxx.x                                                          [100%]
=========================== short test summary info ============================
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list - LIVE FINDING rv4-1: _route_fork returns None over an empty source list (api/_runner.py:1212-1213) — with a CONSUMER of the route's join the join row never spawns, the consumer's reserved dep never releases, and the run wedges 'running' forever (observed: drive() hit max_ticks with 'media' succeeded, 'consume' pending deps_pending=1, no 'media.join' row)
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum - LIVE FINDING rv4-1: _route_fork returns None over an empty source list (api/_runner.py:1212-1213) — with the join as the TERMINAL the run terminalizes SUCCEEDED with result() == None (observed live): the succeeded-having-routed-nothing shape through the empty-corpus door
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal - LIVE FINDING rv4-2: _route_fork carries no MAX_FAN_IN_PER_JOIN check — validate_fork raises INSIDE the finalize tx, OUTSIDE the ladder's try (api/_runner.py:746): a raw ValueError escapes drive() (observed live), the source row wedges 'running', and the fleet's reclaim re-crashes it forever — the error's own remedy (JoinSpec(child_driven=True)) is unreachable from the route API
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_3_a_zero_param_arm_is_a_build_refusal - LIVE FINDING rv4-3: E15's comment claims 'E10/E12's faces own the arity' but neither walks map_arms (_validate.py:664-666) — a zero-param arm BUILDS CLEAN (observed) and dies TypeError at the child ('arm_zero() takes 1 positional argument but 2 were given')
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_3_a_two_param_arm_is_a_build_refusal - LIVE FINDING rv4-3: E15's comment claims 'E10/E12's faces own the arity' but neither walks map_arms (_validate.py:664-666) — a two-param arm BUILDS CLEAN (observed) and meets the same runtime TypeError (the runner invokes arms as body(ctx, item))
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_4_a_superclass_arm_param_is_a_build_refusal - LIVE FINDING rv4-4: E15's related-check admits SUPERCLASS item params (_validate.py:672-678 — issubclass(member, param) passes) — item: DocBase over the member TextDoc builds clean (observed) and the runtime SILENTLY TRUNCATES the subclass's fields (pydantic extra='ignore': the arm receives DocBase(doc_id=...), the text field GONE)
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal - LIVE FINDING rv4-4: E15's related-check admits item: BaseModel (_validate.py:672-678 — issubclass(member, BaseModel) is trivially true) — the arm builds clean (observed) and dies raw PydanticUserError at the child (observed: error_class='PydanticUserError')
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_5_duplicate_type_tags_refuse_at_the_verb - LIVE FINDING rv4-5: type_tag = module.qualname is not injective — two create_model('Rv4Twin') types share one tag; the second arm SILENTLY OVERWRITES the first in the normalized dict (_graph.py:663; observed: map_arms carries ONE entry, twin_arm_a gone) and the tag-set totality compare passes both
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_6_field_identical_union_members_are_a_build_diagnostic - LIVE FINDING rv4-6: the route join's consumer decodes list[A | B] through pydantic's smart union (_runner_codec.py:219) — field-identical members decode AS THE FIRST MEMBER silently (observed live: the arm_b/Rv4SumB element arrived at the consumer as Rv4SumA); the fork's exact-type dispatch is lost at the consumer's boundary
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned - LIVE FINDING rv4-7: E14's _node_bodies walks body/loop_body/map_item but NOT map_arms (_validate.py:815-826) — an arm-held wait_signal raises NO E14 signal (observed: zero diagnostics): no warning, no Mermaid hold, gates undeclarable per-arm
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_7b_the_source_gate_covers_the_arms_waits - LIVE FINDING rv4-7: E14's _node_bodies walks body/loop_body/map_item but NOT map_arms (_validate.py:815-826) — a gate declared on the route's SOURCE whose waits live in the ARMS convicts 'declared-never-waited' (observed: E14 ERROR 'declares gate(s) ... but its body NEVER waits') — a false conviction against the zero-false-positive doctrine
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_8_every_double_attach_direction_speaks_the_designed_message - LIVE FINDING rv4-8: map_source's double-attach check reads map_item only (_graph.py:572) — map-after-route refuses via the ACCIDENTAL "node 'media.join' is declared twice" (observed) instead of the designed "already carries a map or a route"
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared - LIVE FINDING rv4-9: the verb hard-refuses "does not declare a list[...] return" when the source DID declare one the compile could not resolve (_graph.py:714-719; observed live with a factory-built model — body_hints {} → the message claims UNDECLARED) — the refusal lies about the cause and points at the wrong fix
XFAIL tests/test_wf_typed_route_rv4_pins.py::test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues - LIVE FINDING rv4-10: W2 reads node.queue only (_validate.py:1097-1120) — a typo'd RouteArm(queue='gpuu-typo') yields ZERO diagnostics (observed) though the route's children dispatch onto it
1 passed, 14 xfailed in 4.39s
```

## Post-run hygiene

Verified after the repro legs and both pytest runs: zero `tq_db_%`
databases and zero `tq_%`/`rv4%` schemas remain on the shared container
(`postgres://taskq:taskq@localhost:5891`) — the fixture machinery
dropped its per-module databases at teardown, and the repro scripts'
`rv4pin_repro` schema was dropped in place. Other reviewers' artifacts,
if any, were not touched.
