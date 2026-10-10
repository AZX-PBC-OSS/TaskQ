# THE CANCEL-PIN CARRY + THE FACE C CURE

The lane: alone, on `feat/taskqflow-p5` (the PR head @ `e11e5807` — the
final union). The dev loop: the fixer's own PG on :5727
(`/home/rich/.fix5-pg`, `TASKQ_TEST_PG_DSN`). Every run captured under
`.measurements/carry/` (the branch's convention: `.measurements` is
gitignored; the tracked receipts are force-added, as here).

## 1. THE CARRY (item 1)

* `attack/taskqflow-cancel-pins` @ `d1075dd7` — the pin pack,
  `tests/test_wf_attack_cancel.py` (167 lines, three drilled faces) —
  cherry-picked ONTO the PR head. The pin file landed AS-IS (no
  fixture shims needed: `wf_conn`/`wf_schema`/`wf_pool` already live in
  the p5 tree's `tests/_wf_fixtures.py`).
* **THE SHA: `c086a480`** ("test(flows): the torn-cancel attack pins —
  the two faces + the import law").
* The receipts verified, not re-drilled: Face A (the poisoned late leg
  → the WHOLE rollback) and Face B (the one-connection pool → the
  atomic completion) passed FIRST RUN on the PR head (the torn-cancel
  cure — the signal leg on the caller's connection — is already in the
  p5 union; capture `carry-pins-first-run-*.txt`: 2 passed, 1 xfailed).

## 2. FACE C — CURED (item 2)

**THE LIVE DEFECT**: the cancel path imported the FastAPI-bearing admin
package through FIVE lazy sites (`api/_runner.py:819`,
`api/_runner_exit.py:162,285,338`, `api/_hitl.py:750` — all
`from taskq.web.admin._audit import record_admin_action`). The package
`__init__` imports the `fastapi` extra → on a base `taskq[flows]`
install the cancel DIED at `ModuleNotFoundError` before any row moved.
§16.1's import law violated through the cancel seam.

**THE CURE — the machinery is CORE, so its home is core** (`659449f3`):

* `src/taskq/audit.py` — NEW, the deps-free canonical home: the WHOLE
  admin-audit writer moved here (the INSERT SQL, `principal_subject`
  with the #463 redaction chain, `record_admin_action`,
  `record_admin_action_safe`, the fold, the degradation counter).
  Deps: structlog + opentelemetry-api (a CORE dependency) + the taskq
  core; the web package's fastapi-bearing `BoundedPool` is a
  TYPE_CHECKING annotation only.
* `src/taskq/web/admin/_audit.py` — the COMPAT SHIM: every public name
  (and the private `_record_failed_counter`, the admin redteams'
  patch surface — explicit-reexport form) re-exported; the admin routes
  and the admin tests keep their import path.
* The five workflows sites: the lazy import names `taskq.audit` — the
  engine imports ZERO `taskq.web.admin` modules at ANY scope.

**THE CAPABILITY SEAM** (the one indirection; the load-state registry
IS the capability marker): `taskq.audit.record_admin_action` resolves
the row through the LOADED admin shim's CURRENT module attribute when
the shim is loaded — a `sys.modules` read, NEVER an import — with the
identity guard falling through to the canonical body when the shim's
attribute IS the canonical function (the re-export must not recurse).
This is what lets the pin pack land AS-IS: Face A's poison rebinds
`taskq.web.admin._audit.record_admin_action`, and the shim's attribute
surface stays the ONE seam — the rebinding bites every writer in the
process, the engine's included. On a base install the shim is never
loaded and the canonical body is the writer. The degradation counter
resolves through the same seam.

**THE FLIP (the pin's designed drill, observed + captured —
`faceC-XPASS-flip-*.txt`)**: with the cure in, Face C went
**XPASS(strict)** — the red that says REMOVE the marker. The marker
was removed WITH the cure (the pin's docstring records the flip and
the law it now stands as); the pin is GREEN as the standing law — any
re-introduction of an admin import in the engine, including the lazy
shape that convicted the pre-cure head, reds there.

**THE VERIFY**:
* **the base install**: a FRESH VENV, `taskq[flows]` ONLY
  (`import fastapi` → ModuleNotFoundError in that venv): the cancel
  executes END-TO-END — create → hold → `cancel_workflow` → root=
  `cancelled`, signal=`cancelled`, audit_rows=1 (captured:
  `base-install-cancel-*.txt`, re-run on the final head:
  `base-install-cancel-FINAL-*.txt`).
* the pin pack on the cured head: **3/3 green**
  (`carry-pins-green-*.txt`, re-run post-commit:
  `carry-pins-green-final-*.txt`).
* **pyright FULL: 0 errors, 0 warnings** (src + tests;
  `pyright-final-*.txt`).
* the import-law pin: GREEN (Face C).
* the compat: the admin audit redteams + the trail + the scrub suites
  green WITH the pins (57 passed), and the WHOLE `tests/web_admin`
  suite: **904 passed** (`web_admin-sweep-*.txt`); the touched engine
  lanes (runner/finalize/fork/propagation/cli/hitl pins): 53 passed.
* ruff check (src/taskq tests): clean.

## 3. THE SHAs

| commit | what |
| --- | --- |
| `c086a480` | the pin pack carry (the cherry-pick of `d1075dd7`) |
| `659449f3` | Face C's cure (the deps-free audit home + the compat shim + the five sites + the marker removal) |
| `3003acf6` | the carry's captures |

The pre-existing dirty `.measurements/*.json` appends (the p5 lane's
own battery receipts, dirty at the carry's start) were LEFT uncommitted
— not this lane's work. A foreign debug patch on
`src/taskq/worker/heartbeat.py` (a `_slept_*` instrumentation of the
heartbeat's sleep, appearing mid-session — not this lane's edit) was
also left UNCOMMITTED and untouched: single-writer discipline —
verify with its author before it lands anywhere.

— the cancel-pin carry lane, alone; the PG on :5727 left running.
