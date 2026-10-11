# RECEIPTS — the rv6-estate pin pack's red-first protocol

Pin pack: `test_evidence_door_rv6_pins.py` (the content-door lane's five
convictions against its own verifier, the evidence-integrity lane @
`3f69eaa32`; the verifier under pin is `scripts/verify_evidence_heads.py`
there). Doctrine: every landed finding was run UNMARKED first (the red
captured below), then marked `pytest.mark.xfail(strict=True, reason="LIVE
FINDING rv6-d<id>: …")` and re-run (the strict-xfail run captured below).
This pack carries NO green guard — all five pins are live convictions.

## Environment

- Tree: the detached worktree `/tmp/opencode/pinwork-rv6-door` of
  `/home/rich/src/TaskQ` @ `3f69eaa325d523866de8e4de2e2490e483fe3384`
  (created by `git worktree add --detach`). The shared gatecheck2 venv
  (`/tmp/opencode/gatecheck2/.venv`, Python 3.13.15, pytest 9.1.1)
  carries an editable `taskq` install pointing at gatecheck2's tree;
  every run below therefore carried
  `PYTHONPATH=/tmp/opencode/pinwork-rv6-door/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-rv6-door/src/taskq/__init__.py`), and the pins'
  own verifier subprocesses re-export the same PYTHONPATH (the venv's
  editable target must never leak into the door's import). For the
  pyright gate the worktree got its own `.venv` shim (symlinked
  site-packages with `_editable_impl_taskq_py.pth` rewritten to
  `/tmp/opencode/pinwork-rv6-door/src`): pyright 1.1.414 over the repo's
  `[tool.pyright]` config reports **0 errors, 0 warnings** on the pin
  file; `ruff check` + `ruff format --check` clean on BOTH variants (the
  unmarked variant additionally drops the then-unused `import pytest`).
- PG: NONE. The pack is static — every pin builds a synthetic estate (a
  `runs/` dir with a hand-written `CLAIMS.json` + the tampered capture)
  in pytest's own `tmp_path` and runs the verifier as a subprocess
  against it (`--runs-dir`), the rv2-13 pin's own seam. The verifier's
  `REPO` is its own parent (the door worktree); `head_sha` stamps are the
  worktree's live `git rev-parse HEAD`, never hardcoded. No container, no
  schema, no database is touched. `TASKQ_TEST_PG_DSN` was exported for
  the runs but never consumed by this pack.
- Unmarked variant: `tests/test_evidence_door_rv6_pins_unmarked.py` — the
  pin file with the 5 `pytest.mark.xfail` decorator blocks stripped
  (AST-verified mechanical strip — ONLY xfail decorators removed, all 9
  functions preserved verbatim — `/tmp/opencode/pinpacks/rv6/
  make_unmarked.py`), plus the then-unused `import pytest` dropped (ruff
  F401); nothing else changed.

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

Script + full output in `repro/` (`probe_door.py` / `probe_door.txt`).
Every leg's claim is FRESH (head_sha = the worktree's HEAD) so only the
leg under pin can judge. The verbatim convictions (every leg rc=0 — the
law "holds" on each hostile estate):

- **F-D1** (the scan window): a capture whose stamp says `rc=0` (line 1)
  with the TRUE red verdict `1 failed, 31 passed in 42.0s` at line 24 of
  44 (outside the first-3/last-12 window) → `rc=0`, "the head-stamp law
  holds".
- **F-D2** (the JSON launder): `pytest-summary-20261011T000000Z.json`
  carrying `{"rc": 1, "failed": 3, "passed": 28}` → `rc=0`.
- **F-D3** (the empty registry): `{"version": 1, "claims": []}` → `rc=0`,
  "the head-stamp law holds".
- **F-D4** (the future-dated capture): `captured_at:
  2030-01-01T00:00:00`, the same future stamp in the filename, fresh
  head_sha → `rc=0`.
- **F-D5** (the env-forged head): `GITHUB_EVENT_PATH` → a hand-written
  payload naming `f`×40 (a sha that exists nowhere in the repo) → `rc=0`,
  and the verifier itself prints `recorded at THIS head (ffffffffffff)` —
  the forgery IN the green's own words.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-rv6-door && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-door/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6est4 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_evidence_door_rv6_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FFFFF                                                                    [100%]
=================================== FAILURES ===================================
_______ test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture ________
tests/test_evidence_door_rv6_pins_unmarked.py:149: in test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture
    assert "d1-stem" in output and "the head-stamp law holds" not in output, (
E   AssertionError: F-D1: the red verdict mid-capture was never read — the door greens a capture whose own content records failure because the verdict sits outside the first-3/last-12 edge window. The output:
E     rc=0
E     the head-stamp law holds: every evidence kind's live claim in /tmp/pytest-of-rich/pytest-23569/test_d1_the_door_convicts_a_re0/runs is recorded at THIS head (3f69eaa325d5) or explicitly superseded
______________ test_d2_a_json_capture_verdict_is_never_laundered _______________
tests/test_evidence_door_rv6_pins_unmarked.py:180: in test_d2_a_json_capture_verdict_is_never_laundered
    assert "d2-stem" in output and "the head-stamp law holds" not in output, (
E   AssertionError: F-D2: the JSON capture's own verdict fields (rc=1, failed=3) were never read — a valid-JSON body greens whatever it records. The output:
E     rc=0
E     the head-stamp law holds: every evidence kind's live claim in /tmp/pytest-of-rich/pytest-23569/test_d2_a_json_capture_verdict0/runs is recorded at THIS head (3f69eaa325d5) or explicitly superseded
_________________ test_d3_the_empty_registry_is_a_loud_refusal _________________
tests/test_evidence_door_rv6_pins_unmarked.py:197: in test_d3_the_empty_registry_is_a_loud_refusal
    assert "rc=0" not in output, (
E   AssertionError: F-D3: the empty registry GREENS — zero claims is zero verdicts, and the estate passes vacuously. The output:
E     rc=0
E     the head-stamp law holds: every evidence kind's live claim in /tmp/pytest-of-rich/pytest-23569/test_d3_the_empty_registry_is_0/runs is recorded at THIS head (3f69eaa325d5) or explicitly superseded
__________________ test_d4_a_future_dated_capture_is_refused ___________________
tests/test_evidence_door_rv6_pins_unmarked.py:230: in test_d4_a_future_dated_capture_is_refused
    assert "d4-stem" in output and "the head-stamp law holds" not in output, (
E   AssertionError: F-D4: the capture dated 2030 greens — the registry's captured_at field (and the filename's embedded timestamp) is never parsed. The output:
E     rc=0
E     the head-stamp law holds: every evidence kind's live claim in /tmp/pytest-of-rich/pytest-23569/test_d4_a_future_dated_capture0/runs is recorded at THIS head (3f69eaa325d5) or explicitly superseded
__________________ test_d5_the_verified_head_is_the_repos_own __________________
tests/test_evidence_door_rv6_pins_unmarked.py:259: in test_d5_the_verified_head_is_the_repos_own
    assert "the head-stamp law holds" not in output, (
E   AssertionError: F-D5: the verifier judged the estate against the FORGED head ffffffffffff… (the env payload's say-so) and GREENED — the repo's own HEAD was never consulted. The output:
E     rc=0
E     the head-stamp law holds: every evidence kind's live claim in /tmp/pytest-of-rich/pytest-23569/test_d5_the_verified_head_is_t0/runs is recorded at THIS head (ffffffffffff) or explicitly superseded
=========================== short test summary info ============================
FAILED tests/test_evidence_door_rv6_pins_unmarked.py::test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture
FAILED tests/test_evidence_door_rv6_pins_unmarked.py::test_d2_a_json_capture_verdict_is_never_laundered
FAILED tests/test_evidence_door_rv6_pins_unmarked.py::test_d3_the_empty_registry_is_a_loud_refusal
FAILED tests/test_evidence_door_rv6_pins_unmarked.py::test_d4_a_future_dated_capture_is_refused
FAILED tests/test_evidence_door_rv6_pins_unmarked.py::test_d5_the_verified_head_is_the_repos_own
5 failed in 1.46s
```

(The `E`-level assert-unwind lines and trailing blank lines are trimmed
above; `red-run-raw.txt` is the untrimmed byte-exact capture.)

Determinism: the unmarked run was executed THREE times (rv6est1 —
pre-format file, rv6est3, rv6est4) with identical results (5 failed, same
failing assertions); the reds are structural (the window's line counts,
the suffix exemption, the empty loop, the never-parsed field, the env
read), not timing — no clock, no container, no concurrency enters any
pin. The pytest tmp paths differ per run; the verdicts do not.

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-rv6-door && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-door/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6est5 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_evidence_door_rv6_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xxxxx                                                                    [100%]
=========================== short test summary info ============================
XFAIL tests/test_evidence_door_rv6_pins.py::test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture - LIVE FINDING rv6-d1: _content_verdict reads only the first 3 + last 12 lines (_scan_window) — a capture whose stamp says rc=0 whose true red verdict ('1 failed, N passed') sits mid-file (pushed out of the tail window by trailing chatter) greens silently (observed: rc=0, 'the head-stamp law holds')
XFAIL tests/test_evidence_door_rv6_pins.py::test_d2_a_json_capture_verdict_is_never_laundered - LIVE FINDING rv6-d2: a capture named *.json carrying {"rc": 1, "failed": 3} parses as valid JSON and greens — the door never reads verdict content inside JSON; the suffix alone is the exemption (observed: rc=0)
XFAIL tests/test_evidence_door_rv6_pins.py::test_d3_the_empty_registry_is_a_loud_refusal - LIVE FINDING rv6-d3: a claims registry with "claims": [] passes ('the head-stamp law holds') — an estate with NO claims greens vacuously (observed: rc=0)
XFAIL tests/test_evidence_door_rv6_pins.py::test_d4_a_future_dated_capture_is_refused - LIVE FINDING rv6-d4: captured_at is never parsed — a capture dated 2030 with a fresh head_sha greens (observed: rc=0). Neither the registry field nor the filename's embedded timestamp is read
XFAIL tests/test_evidence_door_rv6_pins.py::test_d5_the_verified_head_is_the_repos_own - LIVE FINDING rv6-d5: GITHUB_EVENT_PATH pointing at a hand-written payload makes the verifier judge against a sha that isn't HEAD — observed: rc=0 with the verifier itself printing 'recorded at THIS head (ffffffffffff)' for a sha that exists nowhere in the repo
5 xfailed in 1.32s
```

## Post-run hygiene

The pack touches no PG (static pins; the shared container at
`localhost:5891` was verified untouched by this pack — the three
pre-existing `tq_db_%` databases carry other reviewers' `probe`-shaped
residue and were left alone). Every synthetic estate lives in pytest's
auto-collected `tmp_path`. The `pinwork-rv6-door` worktree carries only
the two pin files (untracked) and the pyright `.venv` shim (untracked);
no tracked file was modified.
