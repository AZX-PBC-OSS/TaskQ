# RECEIPTS — the rv2 pin pack's red-first protocol

Pin pack: `test_wf_attack_rv2.py` (the re-review round's convictions,
feat/taskqflow @ d24f17b9). Doctrine: every landed finding was run
UNMARKED first (the red captured below), then marked
`pytest.mark.xfail(strict=True, reason="LIVE FINDING rv2-<id>: …")` and
re-run (the strict-xfail run captured below). The two green pins
(rv2-11's pair) are the RESOLVED-behavior guards — green by design at
the head (see PINMAP.md).

## Environment

- Tree: scratch copy `/tmp/opencode/pinwork-rv2` of
  `/tmp/opencode/review2-taskqflow` @ `d24f17b9638e` (working tree
  carries only the review's own `.measurements/` residue; the runs
  below never touched the source review tree).
- Python: `/tmp/opencode/rv2-venv/bin/python` (3.14.7, pytest 9.1.1,
  asyncpg 0.32.0). The venv's editable install is STALE (an older
  tree), so every run carries `PYTHONPATH=<scratch>/src`.
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`; per-module databases/schemas are the fixture
  machinery's own (`TASKQ_TEST_RUN_TOKEN=rv2pin*` mixed into the hashed
  names); the pre-pin repro scripts used schema `rv2pin_repro`, dropped
  in-place. Post-run hygiene verified: zero `tq_db_%` databases and
  zero `tq_%`/`rv2pin_%` schemas remain on the cluster.
- Unmarked variant: `tests/test_wf_attack_rv2_unmarked.py` — the pin
  file with the 15 `pytest.mark.xfail` decorator blocks stripped
  (mechanical strip, AST-verified), nothing else changed.
- The pin file passes the repo's own ruff gate
  (`ruff check tests/test_wf_attack_rv2.py` — clean).

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

- F-RV2-1 — seeded a failed root + node with a 200KB `error_class`;
  `fetch_run_view` served 200000 chars whole for BOTH the root and the
  node. (`error_message` on the same read is bounded — the partial
  cure's asymmetry is the finding.)
- F-RV2-2 — one nodeless root past grace:
  `reap_nodeless_roots(..., grace_s=0.0)` returned
  `37942159148651807009686853494333877446` while the
  `NodelessRunReaped` row count was 1.
- F-RV2-3 — `step(..., gates=(app.channel().gate(Model),))` →
  `app.get(...)` raised `AttributeError: 'TypedGate' object has no
  attribute 'timeout_s'` (out of `_rule_eternal_wait`).
- F-RV2-4a — `record_admin_action` with a 5MB reason + 5MB detail:
  stored 5000000 / 5000012 chars verbatim.
- F-RV2-4b — `cancel_workflow_run(..., reason="bad\x00reason")` raised
  `asyncpg.exceptions.CharacterNotInRepertoireError: invalid byte
  sequence for encoding "UTF8": 0x00`; the root stayed `running` (the
  whole cancel rolled back).
- F-RV2-5 — the seeded trio, each leg isolated: `__flow__` root →
  plain/cursor (0, 1); terminal-flow node → (0, 1); deps_pending=1
  node → (0, 1).
- F-RV2-6 — driver model (real `ClaimCursor` on a stepped clock, wire
  conn enforcing asyncpg's arity contract): iteration pairings
  `[(True, 6), (True, 5)]` → `InterfaceError: the server requires 6
  parameters, 5 were given`.
- F-RV2-7 — 4 writer threads + 2 reader threads, 3s: 175
  `RuntimeError: deque mutated during iteration`.
- F-RV2-8 — `loop("lp", body, initial={"seed": parent}, ...)` +
  `sink(parent)`: `app.get(...).validate()` CLEAN; and
  `dumps_jsonb_str({"carry": {"seed": <Promise>}})` →
  `taskq.exceptions.UnencodableValue: Type is not JSON serializable:
  Promise` (the first-claim death).
- F-RV2-9 — `async def body(ctx, params) -> Report` wired with 1 arg →
  `E10-arity: 'only''s body takes 0 param(s) () but the wiring wired 1
  argument(s)`.
- F-RV2-10 — header cells 4/6/9 name no test function;
  `test_wf_matrix_red_drills.py` absent from `tests/system_e2e/`;
  `docs/guides/deployment.md` (1145 lines) carries no operator table /
  cell names.
- F-RV2-11 — the battery's own test fails at the head:
  `E10-arity: 'doc_source''s body takes 1 param(s) (params) but the
  wiring wired 0 argument(s)` → `workflow-projection-skipped`, pairs
  `set()`. Root-caused below (PINMAP).
- F-RV2-12 — `wf_gather_negative_types.py` on disk, absent from
  `_gate.py`'s `_CORPUS`.
- F-RV2-13 — verifier on the committed `.measurements/runs`: rc=1,
  15 named live claims (6 unstamped + 9 stale) with deterministic
  filename-timestamp mtimes; a naive working-tree run reports only 5
  because the verifier's live-claim pick is mtime-ordered and a
  checkout's mtimes are extraction-order — the pin restores the
  semantic order.
- F-RV2-14 — the five captures' `emit_tx_per_page.max_ms`:
  `[10.357, 13.089, 20.569, 11.776, 14.485]`; the doc cites
  `10.4–14.5`.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
PYTHONPATH=/tmp/opencode/pinwork-rv2/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv2pin5 \
/tmp/opencode/rv2-venv/bin/python -m pytest \
  tests/test_wf_attack_rv2_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FFFFFFFFFFFF..FFF                                                        [100%]
=================================== FAILURES ===================================
__________________ test_rv2_1_the_run_view_bounds_error_class __________________
tests/test_wf_attack_rv2_unmarked.py:194: in test_rv2_1_the_run_view_bounds_error_class
    assert len(error_class) <= _BOUND_CEILING_CHARS, (
E   AssertionError: F-RV2-1 (root): the run view served error_class UNBOUNDED (200007 chars) — the panel/page bound covers error_message but the class field rides raw
E   assert 200007 <= 10000
E    +  where 200007 = len('HostileEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE...EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE')
______________ test_rv2_1_the_node_panel_route_bounds_error_class ______________
tests/test_wf_attack_rv2_unmarked.py:239: in test_rv2_1_the_node_panel_route_bounds_error_class
    assert len(error_class) <= _BOUND_CEILING_CHARS, (
E   AssertionError: F-RV2-1 (panel detail): error_class served UNBOUNDED (200007 chars)
E   assert 200007 <= 10000
E    +  where 200007 = len('HostileEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE...EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE')
----------------------------- Captured stdout call -----------------------------
2026-10-09 18:43:56 [warning  ] admin-ui-no-auth               detail='admin UI is being served with no authentication: TASKQ_ENVIRONMENT is a dev environment, so the fail-closed startup check did not run. Every admin route is reachable by anyone who can reach this port. This is unsafe if the process is actually serving production traffic: a production deployment mislabeled as dev disables this check and the health/metrics token check (TASKQ_HEALTH_REQUIRE_TOKEN) at the same time. Pass auth_dependency to create_router, or set TASKQ_ENVIRONMENT to the real environment so startup fails closed.' environment=dev
2026-10-09 18:43:56 [warning  ] progress-router-no-auth        detail='the progress router is being served with no authentication: TASKQ_ENVIRONMENT is a dev environment, so the fail-closed startup check did not run. The SSE stream and per-job state endpoints are reachable by anyone who can reach this port, and each stream holds a Redis pubsub subscription and an asyncio task for as long as the client stays connected. Pass auth_dependency to create_router, or set TASKQ_ENVIRONMENT to the real environment so startup fails closed.' environment=dev
____________ test_rv2_2_the_nodeless_reap_returns_the_reaped_count _____________
tests/test_wf_attack_rv2_unmarked.py:274: in test_rv2_2_the_nodeless_reap_returns_the_reaped_count
    assert reaped == actually_reaped, (
E   AssertionError: F-RV2-2: the arm reports 2165907432825164962719817111437338127 reaped but the rows say 1 — the metric carries int(first-reaped-uuid), not the count
E   assert 2165907432825164962719817111437338127 == 1
___________ test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal ___________
tests/test_wf_attack_rv2_unmarked.py:302: in test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal
    app.get("rv2_typed_gate_mistake")
src/taskq/workflows/api/_app.py:363: in get
    validate_compiled(compiled)
src/taskq/workflows/api/_validate.py:90: in validate_compiled
    diagnostics = _run_rules(compiled)
                  ^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_validate.py:108: in _run_rules
    diagnostics += _rule_eternal_wait(compiled)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_validate.py:468: in _rule_eternal_wait
    if gate.timeout_s is None:
       ^^^^^^^^^^^^^^
E   AttributeError: 'TypedGate' object has no attribute 'timeout_s'
_____________ test_rv2_4a_the_audit_reason_and_detail_are_bounded ______________
tests/test_wf_attack_rv2_unmarked.py:337: in test_rv2_4a_the_audit_reason_and_detail_are_bounded
    assert len(reason) <= _BOUND_CEILING_CHARS, (
E   AssertionError: F-RV2-4a: the audit reason landed UNBOUNDED (5000000 chars) in the never-pruned trail
E   assert 5000000 <= 10000
E    +  where 5000000 = len('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx...xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
_____________ test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel ______________
tests/test_wf_attack_rv2_unmarked.py:366: in test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel
    cancelled = await cancel_workflow_run(
src/taskq/workflows/api/_runner_exit.py:274: in cancel_workflow_run
    flipped = await conn.fetchval(
../rv2-venv/lib/python3.14/site-packages/asyncpg/connection.py:741: in fetchval
    data = await self._execute(query, args, 1, timeout)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../rv2-venv/lib/python3.14/site-packages/asyncpg/connection.py:1904: in _execute
    result, _ = await self.__execute(
../rv2-venv/lib/python3.14/site-packages/asyncpg/connection.py:2003: in __execute
    result, stmt = await self._do_execute(
../rv2-venv/lib/python3.14/site-packages/asyncpg/connection.py:2066: in _do_execute
    result = await executor(stmt, None)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
asyncpg/protocol/protocol.pyx:197: in bind_execute
    ???
E   asyncpg.exceptions.CharacterNotInRepertoireError: invalid byte sequence for encoding "UTF8": 0x00
______ test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe _______
tests/test_wf_attack_rv2_unmarked.py:476: in test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe
    assert not disagreements, (
E   AssertionError: F-RV2-5: the cursor probe's world disagrees with the plain probe's (each seeded shape must read 0 rows in BOTH — the claim's candidacy the probe arbitrates for):
E       - __flow__ root: plain/cursor = (0, 1)
E       - terminal-flow node: plain/cursor = (0, 1)
E       - deps_pending=1 node: plain/cursor = (0, 1)
E   assert not ['__flow__ root: plain/cursor = (0, 1)', 'terminal-flow node: plain/cursor = (0, 1)', 'deps_pending=1 node: plain/cursor = (0, 1)']
______ test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation ______
tests/test_wf_attack_rv2_unmarked.py:603: in test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation
    assert not isinstance(raised, asyncpg.InterfaceError), (
E   AssertionError: F-RV2-6: the expired-cursor round died on the RAW driver arity error (the server requires 6 parameters, 5 were given) — the loop presented the $6 cursor render with 5 args (pairings: [(True, 6), (True, 5)]); the comment at the loop head claims the plain shape runs, and nothing names the degradation
E   assert not True
E    +  where True = isinstance(InterfaceError('the server requires 6 parameters, 5 were given'), <class 'asyncpg.exceptions._base.InterfaceError'>)
E    +    where <class 'asyncpg.exceptions._base.InterfaceError'> = asyncpg.InterfaceError
----------------------------- Captured stdout call -----------------------------
2026-10-09 18:43:57 [debug    ] dispatch                       count=0 from_state=pending kind=dispatch limit_n=4 queues=['q1'] to_state=running worker_id=01a1237b-2e4a-72f2-9dbe-35a2a5146e3b
2026-10-09 18:43:57 [debug    ] dispatch-window-expansion      expansion=1 oversample=4 queues=['q1']
____________ test_rv2_7_concurrent_record_and_snapshot_never_raise _____________
tests/test_wf_attack_rv2_unmarked.py:661: in test_rv2_7_concurrent_record_and_snapshot_never_raise
    assert not errors, (
E   AssertionError: F-RV2-7: concurrent record+snapshot raised 88 times (first: RuntimeError: deque mutated during iteration) — the scrape-time read must never see the recorder's mutation
E   assert not [RuntimeError('deque mutated during iteration'), RuntimeError('deque mutated during iteration'), RuntimeError('deque m...uring iteration'), RuntimeError('deque mutated during iteration'), RuntimeError('deque mutated during iteration'), ...]
___ test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal ___
tests/test_wf_attack_rv2_unmarked.py:701: in test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal
    with pytest.raises((WorkflowValidationError, WorkflowBuildError, UnencodableValue)):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE any of (WorkflowValidationError, WorkflowBuildError, UnencodableValue)
___________________ test_rv2_9_e10_counts_the_actual_params ____________________
tests/test_wf_attack_rv2_unmarked.py:731: in test_rv2_9_e10_counts_the_actual_params
    app.get("rv2_e10_duck_ok")  # the doctrine: NO refusal
    ^^^^^^^^^^^^^^^^^^^^^^^^^^
src/taskq/workflows/api/_app.py:363: in get
    validate_compiled(compiled)
src/taskq/workflows/api/_validate.py:94: in validate_compiled
    raise WorkflowValidationError("validate", "error", f"{len(errors)} error(s) — {report}")
E   taskq.workflows.api._validate.WorkflowValidationError: [error] validate: 1 error(s) — E10-arity: 'only''s body takes 0 param(s) () but the wiring wired 1 argument(s) — the arity is the wiring's own promise: a body param with no wired source is a TypeError mid-flow (the ladder's discovery of a wiring-time lie)
___________ test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test ___________
tests/test_wf_attack_rv2_unmarked.py:814: in test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test
    assert not failures, "the deploy-matrix header walks:\n  - " + "\n  - ".join(failures)
E   AssertionError: the deploy-matrix header walks:
E       - cell 4 (OUTAGE): no test function carries 'outage' — the cell is header fiction
E       - cell 6 (REDISPATCH OWNERSHIP): no test function carries 'redispatch' — the cell is header fiction
E       - cell 9 (RETENTION MID-FLIGHT): no test function carries 'retention' — the cell is header fiction
E       - the header cites test_wf_matrix_red_drills.py — no such file
E       - cell 3 (ROLLBACK): the cited operator table (docs/guides/deployment.md) never names it — the 'rows' claim is fiction
E       - cell 4 (OUTAGE): the cited operator table (docs/guides/deployment.md) never names it — the 'rows' claim is fiction
E       - cell 6 (REDISPATCH OWNERSHIP): the cited operator table (docs/guides/deployment.md) never names it — the 'rows' claim is fiction
E       - cell 7 (CRON x WORKFLOW): the cited operator table (docs/guides/deployment.md) never names it — the 'rows' claim is fiction
E   assert not ["cell 4 (OUTAGE): no test function carries 'outage' — the cell is header fiction", "cell 6 (REDISPATCH OWNERSHIP): no...ll 4 (OUTAGE): the cited operator table (docs/guides/deployment.md) never names it — the 'rows' claim is fiction", ...]
___________ test_rv2_12_every_typeprobe_file_is_wired_into_the_gate ____________
tests/test_wf_attack_rv2_unmarked.py:969: in test_rv2_12_every_typeprobe_file_is_wired_into_the_gate
    assert not zombies and not phantoms, (
E   AssertionError: the typeprobe corpus and the directory disagree — zombies on disk the gate never runs: ['wf_gather_negative_types.py']; corpus entries naming absent files: []
E   assert (not {'wf_gather_negative_types.py'})
_________ test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree __________
tests/test_wf_attack_rv2_unmarked.py:1024: in test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree
    assert proc.returncode == 0, (
E   AssertionError: F-RV2-13: the head-stamp verifier reds on the committed estate:
E     THE HEAD-STAMP LAW REDS (head d24f17b9638e):
E       edge-scale-curve: the LIVE CLAIM edge-scale-curve-20261009T004229-886b.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       estate-slice: the LIVE CLAIM estate-slice-20261008T223923.txt is stale — recorded on 4bf53c869f87, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       evidence-heads-verify: the LIVE CLAIM evidence-heads-verify-20261008T224106.txt is stale — recorded on 4bf53c869f87, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       fanout-1000-tx-band: the LIVE CLAIM fanout-1000-tx-band-20261009T004212-e67f.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       join-fire-latency: the LIVE CLAIM join-fire-latency-20261009T004213-bf94.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       mkdocs-strict: the LIVE CLAIM mkdocs-strict-20261008T224032.txt is stale — recorded on 4bf53c869f87, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       pin1-red-pad-drift: the LIVE CLAIM pin1-red-pad-drift-20261009T134538-5348.json is stale — recorded on e09c236554d9, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       pin1-row-width: the LIVE CLAIM pin1-row-width-20261009T134538-9d2d.json is stale — recorded on e09c236554d9, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       pin2-partial-index: the LIVE CLAIM pin2-partial-index-20261009T134544-069d.json is stale — recorded on e09c236554d9, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       pin4-enqueue-band: the LIVE CLAIM pin4-enqueue-band-20261009T004213-5c0c.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       pin5-dispatch-band: the LIVE CLAIM pin5-dispatch-band-20261009T004226-8b4e.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       typeprobe-gate: the LIVE CLAIM typeprobe-gate-20261008T224023.txt is stale — recorded on 4bf53c869f87, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       wf-perf-bands: the LIVE CLAIM wf-perf-bands-20261008T223939.txt is stale — recorded on 4bf53c869f87, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       wf-rollup-band: the LIVE CLAIM wf-rollup-band-20261008T232346-ea0c.json is unstamped — no head_sha recorded. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E       wf-scoped-coverage: the LIVE CLAIM wf-scoped-coverage-20261009T045020.json is stale — recorded on 03e230430a79, the head is d24f17b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.
E     
E   assert 1 == 0
E    +  where 1 = CompletedProcess(args=['/tmp/opencode/rv2-venv/bin/python', '/tmp/opencode/pinwork-rv2/scripts/verify_evidence_heads.p...b9638e. Re-record it at the head, or mark it SUPERSEDED-BY with the link — never left as the live claim.\n', stderr='').returncode
_____________ test_rv2_14_every_ranged_figure_covers_its_captures ______________
tests/test_wf_attack_rv2_unmarked.py:1116: in test_rv2_14_every_ranged_figure_covers_its_captures
    assert not failures, (
E   AssertionError: the streaming doc's ranged figures do not cover the evidence:
E       - line 25: emit_tx_per_page.max_ms cited 10.4–14.5 ms but the captures carry [20.569] (all: [10.357, 13.089, 20.569, 11.776, 14.485])
E   assert not ['line 25: emit_tx_per_page.max_ms cited 10.4–14.5 ms but the captures carry [20.569] (all: [10.357, 13.089, 20.569, 11.776, 14.485])']
=========================== short test summary info ============================
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_1_the_run_view_bounds_error_class
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_1_the_node_panel_route_bounds_error_class
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_2_the_nodeless_reap_returns_the_reaped_count
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_4a_the_audit_reason_and_detail_are_bounded
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_7_concurrent_record_and_snapshot_never_raise
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_9_e10_counts_the_actual_params
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_12_every_typeprobe_file_is_wired_into_the_gate
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree
FAILED tests/test_wf_attack_rv2_unmarked.py::test_rv2_14_every_ranged_figure_covers_its_captures
15 failed, 2 passed in 4.16s
```

(The two passes are the F-RV2-11 resolved-behavior guards.)

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
PYTHONPATH=/tmp/opencode/pinwork-rv2/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv2pin6 \
/tmp/opencode/rv2-venv/bin/python -m pytest \
  tests/test_wf_attack_rv2.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xxxxxxxxxxxx..xxx                                                        [100%]
=========================== short test summary info ============================
XFAIL tests/test_wf_attack_rv2.py::test_rv2_1_the_run_view_bounds_error_class - LIVE FINDING rv2-1: _wf_rows.py's _run_view_from_rows serves error_class RAW (the root's and every node's) — a 200KB error_class round-trips whole to the run page/SSE while error_message is bounded (the Q5 partial cure's residue)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_1_the_node_panel_route_bounds_error_class - LIVE FINDING rv2-1: _wf_actions.py's node-panel route binds error_message/error_traceback/captured_error via _bound_for_panel but serves error_class raw — the detail AND the children census
XFAIL tests/test_wf_attack_rv2.py::test_rv2_2_the_nodeless_reap_returns_the_reaped_count - LIVE FINDING rv2-2: reap_nodeless_roots returns int(fetchval(RETURNING f.id)) — the first reaped row's UUID read as a 128-bit int — as the reaped-count metric (_sweep.py:373-376)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_3_a_typed_gate_in_step_gates_is_a_named_refusal - LIVE FINDING rv2-3: step(gates=(<TypedGate>,)) — the natural channel.gate(...) door-confusion — crashes validate_compiled with a raw AttributeError ('TypedGate' object has no attribute 'timeout_s') instead of the named E-rule refusal
XFAIL tests/test_wf_attack_rv2.py::test_rv2_4a_the_audit_reason_and_detail_are_bounded - LIVE FINDING rv2-4a: record_admin_action binds reason/detail VERBATIM into the never-pruned admin_audit — a 5MB reason lands whole (the principal_subject column carries the 512-char bound + control-escape discipline; the free-text fields carry none)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_4b_a_nul_reason_never_aborts_the_wf_cancel - LIVE FINDING rv2-4b: a NUL byte in the cancel reason rolls the WHOLE workflow cancel back with an opaque CharacterNotInRepertoireError — the jobs-cancel route carries the parse_text_filter NUL guard; api/_runner_exit.py's cancel_workflow_run carries none
XFAIL tests/test_wf_attack_rv2.py::test_rv2_5_the_cursor_probe_fences_identically_to_the_plain_probe - LIVE FINDING rv2-5: DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL (_dispatch_sql.py:2026-2068) drops the deps_pending=0 filter AND the workflow fence (__flow__ exclusion + terminal-flow exclusion) the plain probe carries — the module's own 'the two fences may not disagree' invariant broken (seeded trio: plain 0 rows, cursor 1 row)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_6_the_expired_cursor_mid_expansion_is_a_named_degradation - LIVE FINDING rv2-6: _dispatch.py:322-343 — the expansion loop flips sql_stmt to the cursor render while round_bound is live but never flips it back; a cursor entry the jitter reset expires BETWEEN iterations calls the $6 statement with 5 args (asyncpg InterfaceError) while the comment claims the plain shape runs
XFAIL tests/test_wf_attack_rv2.py::test_rv2_7_concurrent_record_and_snapshot_never_raise - LIVE FINDING rv2-7: obs/_claim_health.claim_health_snapshot iterates the module-global _window deque while record_claim_latency mutates it (append/popleft, unlocked) — concurrent record+scrape raises RuntimeError: deque mutated during iteration (hundreds per 3s at thread-level concurrency; the Prometheus/OTel observable-gauge callbacks read at scrape time on their own threads)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_8_a_nested_promise_in_the_initial_carry_is_a_build_time_refusal - LIVE FINDING rv2-8: loop(initial={'seed': <Promise>}) — a promise handle NESTED in a dict/list initial carry — passes validate clean (E11's isinstance reads only the top level) and dies at the first claim with UnencodableValue (the jsonb bind refusing the handle), classified infra-fault: a claim→crash→reclaim loop
XFAIL tests/test_wf_attack_rv2.py::test_rv2_9_e10_counts_the_actual_params - LIVE FINDING rv2-9: _validate.py:419-441 — E10 counts ANNOTATED params (typing.get_type_hints), not ACTUAL params: an unannotated-but-runnable body wired correctly is refused 'takes 0 param(s)' — the message lies about the real arity against the validator's zero-false-positive doctrine (E5's own text names the unannotated consumer a tolerated duck-shaped hole)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_10_every_deploy_matrix_cell_maps_to_a_real_test - LIVE FINDING rv2-10: the deploy-matrix header lists 9 cells but cells 4 (OUTAGE), 6 (REDISPATCH OWNERSHIP) and 9 (RETENTION MID-FLIGHT) have no test function anywhere; it cites test_wf_matrix_red_drills.py (nonexistent) and a docs/guides/deployment.md 'operator table' (nonexistent)
XFAIL tests/test_wf_attack_rv2.py::test_rv2_12_every_typeprobe_file_is_wired_into_the_gate - LIVE FINDING rv2-12: tests/typeprobe/wf_gather_negative_types.py is a zombie probe — on disk with live MUST_ERROR markers (pyright reportArgumentType :96, ty invalid-argument-type :96:23 both fire today), wired NOWHERE (not in _gate.py's _CORPUS, so not in the CI gate) — the gate's own docstring's law is wire-or-delete
XFAIL tests/test_wf_attack_rv2.py::test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree - LIVE FINDING rv2-13: scripts/verify_evidence_heads.py --runs-dir <HEAD's committed .measurements/runs> exits rc=1 at d24f17b9 (unstamped live claims + stems stale against their recorded heads) — the evidence-integrity gate reds on its own estate
XFAIL tests/test_wf_attack_rv2.py::test_rv2_14_every_ranged_figure_covers_its_captures - LIVE FINDING rv2-14: perf-evidence-workflows-streaming.md's emit paragraph claims 'max 10.4–14.5 ms across the five captured runs' but capture t20-streaming-bands-20261008-052519.json carries max 20.569 — the cited range does not cover the evidence (the doc's own header law: THE TABLES CITE THE RANGE — EVERY captured run)
2 passed, 15 xfailed in 4.20s
```

No XPASS (strict would have failed the run). Repeat lanes: default
plugin set (`2 passed, 15 xfailed`) and `-n 2` xdist (`2 passed, 15
xfailed`) — identical verdicts.
