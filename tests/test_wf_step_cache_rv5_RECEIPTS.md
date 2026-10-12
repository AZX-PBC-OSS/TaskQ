# RECEIPTS — the rv5-cache pin pack's red-first protocol

Pin pack: `test_wf_step_cache_rv5_pins.py` (the T25 cross-run step-cache
lane's hostile-review convictions, feat/taskqflow-cross-run-cache @
`0839019c1`, based on `37ab05b8`). Doctrine: every landed finding was run
UNMARKED first (the red captured below), then marked
`pytest.mark.xfail(strict=True, reason="LIVE FINDING rv5-k<id>: …")` and
re-run (the strict-xfail run captured below). The one green pin (rv5-k2)
is the behavior-already-correct guard — the pack's own hole closed (see
PINMAP.md); its teeth were proven by the mutation drill captured below.

## Environment

- Tree: the detached worktree `/tmp/opencode/pinwork-cache` of
  `/home/rich/src/TaskQ` @ `0839019c1` (created by
  `git worktree add --detach`). The shared gatecheck2 venv
  (`/tmp/opencode/gatecheck2/.venv`, Python 3.13.15, pytest 9.1.1,
  asyncpg 0.31.0, pydantic 2.13.5) carries an editable `taskq` install
  pointing at gatecheck2's tree; every run below therefore carried
  `PYTHONPATH=/tmp/opencode/pinwork-cache/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-cache/src/taskq/__init__.py`). For the pyright
  gate the worktree got its own `.venv` shim (symlinked site-packages
  with `_editable_impl_taskq_py.pth` rewritten to
  `/tmp/opencode/pinwork-cache/src` — the rv4 pack's own seam): pyright
  1.1.414 over the repo's `[tool.pyright]` config reports **0 errors, 0
  warnings** on the pin file; `ruff check` + `ruff format --check` clean
  on BOTH variants (the marked pin file and the unmarked strip).
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`; per-module databases are the fixture machinery's
  own (`TASKQ_TEST_RUN_TOKEN=rv5cap*` / `rv5mut*` mixed into the hashed
  `tq_db_%` names); the pre-pin repro scripts used schemas
  `cap_probe_rv5` / `cap_probe_rv5b` / `cap_probe_receipt`, each dropped
  in-place at the script's own end. Post-run hygiene verified after every
  leg: zero `cap_%`/`tmp_%` schemas and zero `tq_db_%` databases of this
  pack's tokens remain on the shared container (the three `tq_db_%`
  databases present predate this pack and are other reviewers' — the
  pack's token-hashes were enumerated and none match; untouched).
- Unmarked variant: `tests/test_wf_step_cache_rv5_pins_unmarked.py` — the
  pin file with the 6 `pytest.mark.xfail` decorator blocks stripped
  (AST-verified mechanical strip — ONLY xfail decorators removed, all 31
  functions + the 13 non-xfail decorators preserved —
  `repro/make_unmarked.py`), nothing else changed.

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

Scripts + full outputs in `repro/` (`probe_k1_k3_k5.py` /
`probe_k1_k3_k5.txt`, `probe_k7_k2_k6.py` / `probe_k7_k2_k6.txt`). The
verbatim convictions:

- **F-K1** (the route-source wedge): `step(..., cache=True)` feeding
  `route()` — wiring ACCEPTED (no refusal); run 1 succeeded and stored
  one `wf_step_cache` row; run 2 `drive()` returned `max_ticks` over a
  `running` root with the source row `succeeded` carrying the RECEIPT
  (`wf_cache_hit` = `{run_id: <run 1>, address: 6a083640…, stored_at: …}`),
  ZERO `docs.item:*` child rows, no `docs.join` row, the consumer `sum`
  pending forever. The hit path's `_finalize_success` carries no
  `prebuilt_route_fork` (api/_runner.py:666-681 vs the miss path's
  :750-770/:805-814).
- **F-K3** (the ctx.input exclusion): a cached body reading
  `ctx.input["tenant"]` — run 1 (`tenant=acme`) → `'tenant:acme'`,
  run 2 (`tenant=globex`) → `'tenant:acme'` with the body executed ONCE
  across both runs. The cross-tenant leak terminal-succeeds.
- **F-K5** (the exit dead-cache): an `Exit[KReport]`-returning body with
  `cache=True` — wiring ACCEPTED; two runs both execute the body
  (`calls=2`); `wf_step_cache` holds ZERO rows afterwards. The opt-in
  no-ops silently (the `_finalize_exit` return at api/_runner.py:727-733
  precedes the store at :822-824).
- **F-K7** (the classifier asymmetry): `ConnectionError` injected at
  `_finalize_success` — MISS path: `drive()` RAISES
  `ConnectionError`, the node stays `running` (ledger `running` — the
  reclaim class, the ladder unburned, the body DID run); HIT path:
  `drive()` returns `'terminal'` over the node `failed` with
  `error_class='ConnectionError'` + a `failed` ledger row and the run
  `failed` — with the body NEVER run in the hit run. Same fault, two
  semantics.
- **F-K2** (the input-sensitivity guard, green at head):
  `cache_address(v, [{"doc_id": "X"}]) != cache_address(v, [{"doc_id": "Y"}])`;
  the two-input run pair executed the body TWICE and stored TWO rows.
  GREEN at the head — pinned as the guard.
- **F-K6** (the eviction door): `taskq cache purge <addr>` → exit 2
  (`No such command 'cache'`); `--help` lists no cache verb.
- **F-K4** (the closure hole's law): the guide's cache section carries
  "module, qualname, the body's own source" but NEVER the word "helper"
  nor the operational law that a changed helper does not move the
  address; the api-reference's cache row is likewise silent (the word
  "helper" appears nowhere in either cache text).

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-cache && \
PYTHONPATH=/tmp/opencode/pinwork-cache/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv5cap2 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_step_cache_rv5_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
F.FFFFF                                                                  [100%]
=================================== FAILURES ===================================
_________ test_k1_a_cached_route_source_is_refused_or_the_hit_reforks __________
tests/test_wf_step_cache_rv5_pins_unmarked.py:284: in test_k1_a_cached_route_source_is_refused_or_the_hit_reforks
    assert outcome == "terminal", (
E   AssertionError: F-K1: the cached route source's HIT wedged the run (drive returned 'max_ticks') — the hit's finalize never rebuilt the route fork: the children never spawn, the consumer's dep never releases, the run is a 'running' corpse over a succeeded source
E   assert 'max_ticks' == 'terminal'
E     
E     - terminal
E     + max_ticks
________ test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic _________
tests/test_wf_step_cache_rv5_pins_unmarked.py:400: in test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic
    assert any("cache" in d.message and "input" in d.message for d in diagnostics), (
E   AssertionError: F-K3: no build-time diagnostic named the cache+ctx.input hazard — the cross-tenant leak ships silently. validate() said: []
E   assert False
_________________ test_k4_the_docs_carry_the_closure_holes_law _________________
tests/test_wf_step_cache_rv5_pins_unmarked.py:430: in test_k4_the_docs_carry_the_closure_holes_law
    assert "helper" in lowered, (
E   AssertionError: F-K4: the guide's cache section never names the closure hole — the law 'the address covers the body's own source; a changed HELPER does NOT move the address' is nowhere written
_______________ test_k5_cache_on_an_exit_returning_body_refuses ________________
tests/test_wf_step_cache_rv5_pins_unmarked.py:460: in test_k5_cache_on_an_exit_returning_body_refuses
    with pytest.raises((WorkflowBuildError, WorkflowValidationError)) as exc_info:
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   Failed: DID NOT RAISE any of (WorkflowBuildError, WorkflowValidationError)
_____________________ test_k6_the_cache_purge_verb_evicts ______________________
tests/test_wf_step_cache_rv5_pins_unmarked.py:511: in test_k6_the_cache_purge_verb_evicts
    assert result.exit_code == 0, (
E   AssertionError: F-K6: `taskq cache purge <address>` failed (exit=2) — the eviction door does not exist. output: "Usage: taskq [OPTIONS] COMMAND [ARGS]...\nTry 'taskq --help' for help.\n╰─ Error ────╮\n│ No such command 'cache'. │\n╰───"
E   assert 2 == 0
__________ test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class ___________
tests/test_wf_step_cache_rv5_pins_unmarked.py:570: in test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class
    assert node["status"] != "failed", (
E   AssertionError: F-K7: the hit-finalize's infra fault terminal-FAILED the node (status='failed') — the SAME fault on the miss path escapes as the reclaim class; here it was laddered as a BODY failure with the body never run (the classifier asymmetry: one fault, two semantics)
E   assert 'failed' != 'failed'
=========================== short test summary info ============================
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k1_a_cached_route_source_is_refused_or_the_hit_reforks
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k4_the_docs_carry_the_closure_holes_law
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k5_cache_on_an_exit_returning_body_refuses
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k6_the_cache_purge_verb_evicts
FAILED tests/test_wf_step_cache_rv5_pins_unmarked.py::test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class
6 failed, 1 passed in 7.79s
```

(The one pass is the F-K2 green guard — the input-sensitivity law,
correct at this head. The K4 assertion's long echo line and the K3
generator `where` line are trimmed above; `red-run-raw.txt` is the
untrimmed byte-exact capture.)

Determinism: the unmarked run was executed TWICE (rv5cap1, rv5cap2) with
identical results (6 failed + 1 passed, same failing assertions); the
reds are structural (missing refusals/rules/verbs, a wedged run, a
misclassified fault), not timing — the wedge pin's `max_ticks=120` bound
only bounds the red's wall clock (~2.4s).

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-cache && \
PYTHONPATH=/tmp/opencode/pinwork-cache/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv5cap3 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_step_cache_rv5_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
x.xxxxx                                                                  [100%]
=========================== short test summary info ============================
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k1_a_cached_route_source_is_refused_or_the_hit_reforks - LIVE FINDING rv5-k1: cache=True on a typed-route source is accepted at wiring and the hit path never rebuilds the route fork (api/_runner.py's hit _finalize_success carries no prebuilt_route_fork) — observed: run 2 wedged (drive=max_ticks), the source row succeeded WITH the wf_cache_hit receipt, zero route children, the consumer pending, the root running forever
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k3_a_cached_body_reading_ctx_input_is_a_build_diagnostic - LIVE FINDING rv5-k3: a cached body reading ctx.input serves the FIRST run's truth to any later run with a different input (observed: run 2 with tenant=globex received 'tenant:acme', the body never ran) — no build surface names the hazard (validate_compiled returned zero diagnostics for the shape)
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k4_the_docs_carry_the_closure_holes_law - LIVE FINDING rv5-k4: the address covers the body's OWN source only — a changed HELPER keeps the address (the closure hole) — and NO doc states it: the guide's cache section and the api-reference carry 'module, qualname, the body's own source' but never the operational consequence (a changed helper does NOT move the address)
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k5_cache_on_an_exit_returning_body_refuses - LIVE FINDING rv5-k5: cache=True on an Exit-returning body builds clean and silently never stores (api/_runner.py's _finalize_exit returns before the cache store) — observed: two runs, zero cache rows, the body executed both times, no diagnostic anywhere
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k6_the_cache_purge_verb_evicts - LIVE FINDING rv5-k6: no eviction door exists — `taskq cache purge <address>` exits 2 (no such command) and the admin surface carries no equivalent; the operator's only eviction tool is raw SQL against wf_step_cache
XFAIL tests/test_wf_step_cache_rv5_pins.py::test_k7_the_hit_finalize_infra_fault_is_the_reclaim_class - LIVE FINDING rv5-k7: a ConnectionError on a HIT's finalize is laddered as a BODY failure (the hit's _finalize_success sits INSIDE the ladder's try — observed: max_attempts=1 → the node terminal-failed error_class='ConnectionError', the run failed, the body NEVER ran); the SAME fault on the MISS path's finalize escapes as infra (the reclaim class — observed: drive() raised, the node left running/reclaim-eligible, the ladder unburned)
1 passed, 6 xfailed in 8.78s
```

## THE F-K2 MUTATION DRILL (the green guard's teeth)

The mutation that drops the address's input leg — applied to the
worktree's `src/taskq/workflows/_cache.py` (`cache_address`'s doc loses
`"input": json_input`), the guard run against the MUTANT (token
rv5mut1; capture `repro/k2-mutation-drill.txt`):

```text
tests/test_wf_step_cache_rv5_pins.py:362: in test_k2_two_inputs_to_one_cached_body_miss_each_other
    assert addr_x != addr_y, (
E   AssertionError: F-K2: one body over two inputs hashed to ONE address — the input leg is absent from the address (any run's input reads every run's entry)
E   assert '59125f6ad2…' != '59125f6ad2…'   (the two inputs hashed to ONE address)
1 failed in 1.68s
```

The mutant was then restored (`git checkout -- src/taskq/workflows/_cache.py`;
`git diff` empty) and the guard re-run GREEN (token rv5mut2: `1 passed in
1.78s`). Teeth proven: the guard reds exactly under the mutation the 11
shipped pins survive.

## Post-run hygiene

Verified after the repro legs and every pytest run: zero
`cap_%`/`tmp_%` schemas and zero `tq_db_%` databases of this pack's
tokens remain on the shared container
(`postgres://taskq:taskq@localhost:5891`). The three pre-existing
`tq_db_%` databases belong to other reviewers' tokens (enumerated,
untouched). The `pinwork-cache` worktree itself carries only the two pin
files (untracked) and the pyright `.venv` shim (untracked); no tracked
file was modified (the mutation drill was restored byte-exact).

---

# THE RESIDUAL-CURES LANE'S RECONCILED FLIPS (the adoption-with-flip-receipts rows)

Pack ported to the reconciliation ground `feat/taskqflow-residual-cures`
(the four cure lanes' merges + this lane's cures). The pre-reconciliation
state was captured at the lane head BEFORE the reconciling commit
(`/tmp/opencode/cache-pack-pre.txt`: K1 + K5 XPASS-strict, K3 + K4 + K6
+ K7 xfail, K2 the green guard).

| Pin | Pre-flip state at the reconciliation ground | The cure the flip adopts | Receipt |
| --- | --- | --- | --- |
| K1 (the route-source wedge) | XPASS-strict — the SAFE behavior holds (the cached route-source is refused / the hit re-forks) | the cache-cures lane's refusal | this row |
| K5 (the Exit dead-cache) | XPASS-strict — the Exit+cache combination is refused at the wiring | the cache-cures lane's wiring refusal | this row |
| K3 (the ctx.input exclusion) | XFAIL — the pin's harness caught only `WorkflowValidationError` from `app.get`; the cured face is the E18 RULE: a `WorkflowBuildError` raised by the WIRING VERB (rule `E18-cache-ctx-input`, the hazard + the remedy named) | the reconciled pin catches the refusal face (build OR validate) and asserts the hazard named — the cured face is STRICTER than the diagnostic asked | this row |
| K4 (the closure-hole's law) | XFAIL — the law SHIPS verbatim ('A changed HELPER is invisible to the address'); the pin's regex demanded not/never in one LINE-span (the verbatim spans a newline) | the matcher reads the cured spelling (helper…invisible…address across the sentence) | this row |
| K6 (the eviction door) | XFAIL — the verb ships as `taskq wf cache evict` (the wf sub-typer), DRY-RUN by default, `--yes` deletes; the pin invoked `cache purge <address>` (exit 2) | the reconciled pin drives the cured verb's whole face: the dry run reports + touches nothing, `--yes` evicts, the next run MISSES | this row |
| K7 (the classifier asymmetry) | XFAIL — NOT flipped: my own run CONFIRMED the front's verdict (the hit's `_finalize_success` still inside the ladder's try; max_attempts=1 → the node terminal-failed `error_class='ConnectionError'`, the body never ran). The cure touches `api/_runner.py`'s finalize/ladder path; the docsfaces lane (still in flight) may touch the adjacent absorb path — the single-writer-per-file law dispatches K7 when docsfaces closes | — (the marker stays) | this row is the lane's ledger note |
