# RECEIPTS — the rv6-engine pin pack's red-first protocol

Pin pack: `test_wf_engine_rv6_pins.py` (the rv6 round's engine
convictions, the pushed head @ `0e2f5cc09`). Doctrine: every landed
finding was run UNMARKED first (the red captured below), then marked
`pytest.mark.xfail(strict=True, reason="LIVE FINDING rv6-e<id>: …")` and
re-run (the strict-xfail run captured below). This pack carries NO green
guard — both pins are live convictions.

## Environment

- Tree: the detached worktree `/tmp/opencode/pinwork-rv6-head` of
  `/home/rich/src/TaskQ` @ `0e2f5cc096727adc5d373d1f7dd813d10b10d331`
  (created by `git worktree add --detach`). The shared gatecheck2 venv
  (`/tmp/opencode/gatecheck2/.venv`, Python 3.13.15, pytest 9.1.1,
  asyncpg 0.31.0, pydantic 2.13.5) carries an editable `taskq` install
  pointing at gatecheck2's tree; every run below therefore carried
  `PYTHONPATH=/tmp/opencode/pinwork-rv6-head/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-rv6-head/src/taskq/__init__.py`). For the
  pyright gate the worktree got its own `.venv` shim (symlinked
  site-packages with `_editable_impl_taskq_py.pth` rewritten to
  `/tmp/opencode/pinwork-rv6-head/src`): pyright 1.1.414 over the repo's
  `[tool.pyright]` config reports **0 errors, 0 warnings** on the pin
  file; `ruff check` + `ruff format --check` clean on BOTH variants.
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`; the per-module database is the fixture machinery's
  own (`TASKQ_TEST_RUN_TOKEN=rv6eng*` mixed into the hashed `tq_db_%`
  name, dropped at module teardown). The pre-pin repro script used
  schemas `r6_probe_e1` / `r6_probe_e2` (the round's `r6_` prefix), each
  dropped in-place at the script's own end.
- Unmarked variant: `tests/test_wf_engine_rv6_pins_unmarked.py` — the pin
  file with the 2 `pytest.mark.xfail` decorator blocks stripped
  (AST-verified mechanical strip — ONLY xfail decorators removed, all 4
  functions + the 3 non-xfail decorators preserved —
  `/tmp/opencode/pinpacks/rv6/make_unmarked.py`), nothing else changed.

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

Script + full output in `repro/` (`probe_engine.py` / `probe_engine.txt`).
The verbatim convictions:

- **F-E1** (the in-process post-terminal execution hole): a claimable
  child (`pending`, `deps_pending=0`, no hold, step_key `only`) staged
  on a CANCELLED root; ONE bare `FlowRunner.tick()` → `ran=True`, the
  body RAN (the sentinel `['e1-body-ran']`), the row finalized
  `{'status': 'succeeded', 'attempt': 1}` under `root=cancelled`.
  `drive()`'s loop-top check never entered — the pin calls `tick()`, the
  public dispatch pass the sweep-cron/embedders' seam drives directly.
- **F-E2** (the hold→node wake's missing map_index leg): three map
  siblings (`work.item`, map_index 0/1/2, each held on its OWN signal —
  the pre-cure hold identity `(workflow_id, node_key, signal_name,
  hold_epoch)` admits the distinct names) → `deliver_payload` on the
  MIDDLE sibling's hold returned `delivered`, but the wake cleared
  **sibling 0's** hold marker (`still_held=False` at map_index=0) while
  the ANSWERED sibling 1 stays `still_held=True` — the delivered
  payload's own node never resumes; a sibling that was never answered
  re-runs.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-rv6-head && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-head/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6eng1 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_engine_rv6_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FF                                                                       [100%]
=================================== FAILURES ===================================
________ test_e1_the_in_process_claim_never_executes_on_a_terminal_flow ________
tests/test_wf_engine_rv6_pins_unmarked.py:109: in test_e1_the_in_process_claim_never_executes_on_a_terminal_flow
    assert _E1_SENTINEL == [], (
E   AssertionError: F-E1: the body EXECUTED on the cancelled flow — the in-process claim carries no terminal-flow fence (the fleet's dispatch SQL has it). Sentinel: ['e1-body-ran']
E   assert ['e1-body-ran'] == []
____________ test_e2_a_middle_out_resolve_wakes_the_holds_own_node _____________
tests/test_wf_engine_rv6_pins_unmarked.py:210: in test_e2_a_middle_out_resolve_wakes_the_holds_own_node
    assert resumed == {own_node}, (
E   AssertionError: F-E2: the middle-out resolve woke the WRONG sibling — the wake matches step_key + flow + hold-marker with no map_index/ORDER BY leg. The resolved hold 01a12cf3-bfb… owns node 01a12cf3-bfb… (map_index=1); the resumed set is ['01a12cf3-bfb'] — the answered sibling's node stays held (its delivered payload unconsumable by its own wait site) while a sibling that was never answered re-runs
E   assert {'01a12cf3-bf...4b03d63c76aa'} == {'01a12cf3-bf...e0ebaf72e200'}
E     Extra items in the left set:
E     '01a12cf3-bfb0-7b21-a878-4b03d63c76aa'
E     Extra items in the right set:
E     '01a12cf3-bfb6-7350-bc1e-e0ebaf72e200'
----------------------------- Captured stdout call -----------------------------
2026-10-11 14:52:14 [info     ] signal.delivered               hold_id=01a12cf3-bfb6-7350-bc1e-e0dda9aab1e3 run_id=01a12cf3-bfa8-7061-85c0-0177655090cd signal=sig-1
=========================== short test summary info ============================
FAILED tests/test_wf_engine_rv6_pins_unmarked.py::test_e1_the_in_process_claim_never_executes_on_a_terminal_flow
FAILED tests/test_wf_engine_rv6_pins_unmarked.py::test_e2_a_middle_out_resolve_wakes_the_holds_own_node
2 failed in 2.41s
```

(The E1 diff-echo's trailing `Use -v` line is trimmed above;
`red-run-raw.txt` is the untrimmed byte-exact capture. In the E2 red the
LEFT set member `…bfb0…` is sibling 0 — resumed, wrongly; the RIGHT set
member `…bfb6…e0ebaf72e200` is the answered sibling 1's own node — never
resumed. The `signal.delivered` log line names `sig-1`: the CAS targeted
the RIGHT hold; the wake woke the WRONG node.)

Determinism: the unmarked run was executed TWICE (rv6eng1, rv6eng3) with
identical results (2 failed, same failing assertions). Both reds are
structural: E1 is a missing WHERE leg (no clock enters); E2's wake is a
`fetchval` with no ORDER BY over freshly-inserted rows — the heap's
physical order is insertion order (sibling 0 first; no UPDATE moves the
rows), so the wrong-sibling pick is stable in this staging, and the
assertion (the resumed set must be EXACTLY the hold's own node) convicts
ANY wrong pick, not just this one.

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-rv6-head && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-head/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6eng2 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_engine_rv6_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xx                                                                       [100%]
=========================== short test summary info ============================
XFAIL tests/test_wf_engine_rv6_pins.py::test_e1_the_in_process_claim_never_executes_on_a_terminal_flow - LIVE FINDING rv6-e1: the in-process driver's claim path (CLAIMABLE_NODES_SQL_TEMPLATE / NODE_CLAIM_SQL_TEMPLATE) carries NO flow-status leg — a bare FlowRunner.tick() claims and EXECUTES a claimable child on a CANCELLED flow (observed: the body ran, the row finalized 'succeeded' with attempt=1 under a cancelled root). The fleet's dispatch SQL carries the terminal-flow fence; this door doesn't
XFAIL tests/test_wf_engine_rv6_pins.py::test_e2_a_middle_out_resolve_wakes_the_holds_own_node - LIVE FINDING rv6-e2: deliver_payload's wake resolves the held node by step_key + flow + metadata ? 'hold' — no map_index leg, no ORDER BY — so resolving the MIDDLE of three held map siblings wakes the arbitrary first-matching row (observed: sibling 0's hold marker cleared; the ANSWERED sibling 1 stays held, its delivered payload unconsumable by its own node)
2 xfailed in 2.47s
```

## Post-run hygiene

Verified after the repro legs and every pytest run: zero `r6_%` schemas
remain on the shared container (the repro script drops its own), and the
pack's module databases (`rv6eng*` tokens) were dropped by the fixture
machinery's own teardown — the three pre-existing `tq_db_%` databases on
the container carry other reviewers' `probe`-shaped residue (hundreds of
rows; enumerated, verified to contain neither `only` nor `work.item`
rows, untouched). The `pinwork-rv6-head` worktree carries only the two
pin files (untracked) and the pyright `.venv` shim (untracked); no
tracked file was modified. Both async PG pins pick up the estate's
always-on G7 rows↔status teardown automatically (file prefix `test_wf_`):
the E1 red path's residue (a cancelled root over a pending child) is the
cancel arm's own lag-tolerant shape (nothing asserted against), and the
E2 residue (a running root over held/pending siblings) reconstructs
non-terminal — both runs carried zero teardown errors.

---

# THE RESIDUAL-CURES LANE'S RE-RUN + E2'S RECONCILED FLIP

The pack re-ran at the reconciliation ground `feat/taskqflow-residual-cures`
(the four cure lanes' merges + this lane's cures — the re-run is
MANDATORY there: the terminal-fail cure touches the claim machinery).

* E1 — the xfail STANDS (the in-process post-terminal execution hole is
  still live at this tree — the engine-cures lane's cure has not
  landed; unregressed, unflipped).
* E2 — RECONCILED FLIP: the pin was XPASS-strict at the ground — the
  holdmap lane's cure (1528719d2, the resume addressing the hold's OWN
  node by the metadata hold stamp + the map-sibling identity) IS this
  finding's cure, landed by the holdmap merge. The marker is removed
  with the cure (the adoption-with-flip-receipts law — never a
  deletion, the pin's assertions unchanged). Green x2 at the lane head.
