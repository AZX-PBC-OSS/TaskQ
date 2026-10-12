# RECEIPTS — the rv6-docs pin pack's red-first protocol

Pin pack: `test_docs_rv6_pins.py` (the rv6 round's docs-currency
convictions, fix/docs-currency @ `6e2297fe8`). Doctrine: every landed
finding was run UNMARKED first (the red captured below), then marked
`pytest.mark.xfail(strict=True, reason="LIVE FINDING rv6-docs<id>: …")`
and re-run. F-DOCS-4 is the exception, BY THE DOCTRINE'S OWN rv2-11
POSTURE: its cited text does not exist in this tree (the holdmap cure's
runbook paragraph lives on the unmerged `feat/taskqflow-holdmap-cures`
lane — see the unreproducible note), so its pin stands GREEN as the
anti-rot guard and carries NO marker. The pack is STATIC — the estate's
own text, never a container.

## Environment

- Tree: the detached worktree `/tmp/opencode/pinwork-rv6-docs` of
  `/home/rich/src/TaskQ` @ `6e2297fe87ae2b1ad273c537f30fe7d82aa7019a`
  (created by `git worktree add --detach`). The shared gatecheck2 venv
  (`/tmp/opencode/gatecheck2/.venv`, Python 3.13.15, pytest 9.1.1)
  carries an editable `taskq` install pointing at gatecheck2's tree;
  every run below therefore carried
  `PYTHONPATH=/tmp/opencode/pinwork-rv6-docs/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-rv6-docs/src/taskq/__init__.py` — the F-DOCS-1
  pin walks THIS tree's `__all__`, never the venv's). For the pyright
  gate the worktree got its own `.venv` shim (symlinked site-packages
  with `_editable_impl_taskq_py.pth` rewritten to
  `/tmp/opencode/pinwork-rv6-docs/src`): pyright 1.1.414 over the repo's
  `[tool.pyright]` config reports **0 errors, 0 warnings** on the pin
  file; `ruff check` + `ruff format --check` clean on BOTH variants (the
  unmarked variant additionally drops the then-unused `import pytest`).
- PG: NONE. `TASKQ_TEST_PG_DSN` was exported for the runs but never
  consumed by this pack.
- Unmarked variant: `tests/test_docs_rv6_pins_unmarked.py` — the pin file
  with the 3 `pytest.mark.xfail` decorator blocks stripped (AST-verified
  mechanical strip — ONLY xfail decorators removed, all 4 functions
  preserved verbatim — `/tmp/opencode/pinpacks/rv6/make_unmarked.py`),
  plus the then-unused `import pytest` dropped (ruff F401); nothing else
  changed.

## Pre-pin verifications (static findings — convicted by inspection, the
## greps ARE the repro)

- **F-DOCS-1**: `docs/api-reference/workflows.md`'s "The wiring verbs"
  table (lines 56–63) carries step, gather, map_source, sink, build,
  loop — `route` appears nowhere as a row (its single occurrence in the
  file is inside the `E15-route-totality` error-rule text, line 159)
  though `route` is a top-level export
  (`src/taskq/workflows/__init__.py:118`). The pin's mechanical walk
  (public functions in `__all__` homed in the wiring layer `_graph` /
  `_loop`) finds the full conviction set: `['chain_source', 'route']`.
- **F-DOCS-2**: `grep -rn FanInBoundExceededError docs/` → ZERO hits;
  the class exists at `src/taskq/workflows/definitions.py:176` (raised at
  :230 and :279, the ladder's deterministic fourth class at
  `api/_runner_ladder.py:85`). The guide's fan-in section
  (`docs/guides/workflows.md:189` "The fan-in bound + the two join
  shapes (T07)") teaches the bound (≤ 1000) and the child-driven escape
  without ever naming the class the operator meets as `error_class`.
- **F-DOCS-3**: `grep -rn "does not declare a\|single-value\|non-list"
  docs/` → the only hit is unrelated (`guides/rate-limiting.md`'s GCRA
  paragraph). The guide's route teaching (`docs/guides/workflows.md:656`,
  ":714") is per-element only ("each element runs ITS type's arm"); the
  build refusal for a non-list source (`api/_graph.py:779`: "the route
  is a MAP face: the source produces the elements' list") and the
  single-value alternative (the chain step's in-body `Route`, the same
  guide's §router at :1132) are nowhere connected or named.
- **F-DOCS-4**: the cited paragraph ("served by five `wf_*` tables …",
  listing six `wf_*` tables + `jobs`) is NOT IN THIS TREE — it exists at
  `1528719d2:docs/guides/workflows.md:1832` on the unmerged
  holdmap-cures lane; this tree's §7 (`docs/guides/workflows.md:1605`,
  the file ends at :1613) carries no table-count sentence. SEE THE
  UNREPRODUCIBLE NOTE — the pin is the guard for the incoming paragraph.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-rv6-docs && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-docs/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6doc4 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_docs_rv6_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FFF.                                                                     [100%]
=================================== FAILURES ===================================
___________ test_docs1_every_wiring_verb_in_all_appears_in_the_table ___________
tests/test_docs_rv6_pins_unmarked.py:93: in test_docs1_every_wiring_verb_in_all_appears_in_the_table
    assert not missing, (
E   AssertionError: F-DOCS-1: public wiring verbs missing from the api-reference table: ['chain_source', 'route'] — `route` is a top-level export (the T27 typed route the guide teaches at length); a reader of the reference can never find it
E   assert not {'chain_source', 'route'}
______ test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy _______
tests/test_docs_rv6_pins_unmarked.py:122: in test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy
    assert "FanInBoundExceededError" in section_text, (
E   AssertionError: F-DOCS-2: the fan-in bound's error CLASS is named nowhere in the guide (nor anywhere in docs/) — the operator who meets error_class='FanInBoundExceededError' finds no doc; the section teaches the bound (≤ 1000) and the child-driven escape but never names the refusal's own class
E   assert 'FanInBoundExceededError' in "\n**The bound: ≤ 1000 declared parents per join** (`MAX_FAN_IN_PER_JOIN`) —\nenforced at validate (`wf.validate()` / ...e counter's own widening before it can reconcile), so a genuinely\nhuge fan-in is a schema conversation, not a knob.\n"
____________ test_docs3_the_single_value_route_disposition_is_named ____________
tests/test_docs_rv6_pins_unmarked.py:156: in test_docs3_the_single_value_route_disposition_is_named
    assert disposition is not None, (
E   AssertionError: F-DOCS-3: the single-value Promise[A|B] disposition is unstated — route() is taught only over list sources ('each element runs ITS type's arm'), and nothing names where routing a SINGLE union value goes (the chain step's in-body Route) nor route()'s own build refusal of the non-list source
E   assert None is not None
=========================== short test summary info ============================
FAILED tests/test_docs_rv6_pins_unmarked.py::test_docs1_every_wiring_verb_in_all_appears_in_the_table
FAILED tests/test_docs_rv6_pins_unmarked.py::test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy
FAILED tests/test_docs_rv6_pins_unmarked.py::test_docs3_the_single_value_route_disposition_is_named
3 failed, 1 passed in 0.35s
```

The `1 passed` is the F-DOCS-4 guard — green BECAUSE the cited text is
absent at this tree (zero count-claims found; the §7 witness holds), not
because any behavior was fixed. Determinism: the unmarked run was
executed twice (rv6doc1 — pre-format file, rv6doc4) with identical
results; the reds are static-text convictions, deterministic by
construction.

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-rv6-docs && \
PYTHONPATH=/tmp/opencode/pinwork-rv6-docs/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv6doc5 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_docs_rv6_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xxx.                                                                     [100%]
=========================== short test summary info ============================
XFAIL tests/test_docs_rv6_pins.py::test_docs1_every_wiring_verb_in_all_appears_in_the_table - LIVE FINDING rv6-docs1: the api-reference's wiring-verbs table omits `route` (a top-level export, the T27 typed-route verb) — the __all__-walk convicts the omission (and chain_source beside it)
XFAIL tests/test_docs_rv6_pins.py::test_docs2_the_fan_in_bounds_error_class_is_named_with_its_remedy - LIVE FINDING rv6-docs2: FanInBoundExceededError exists in src (workflows/definitions.py — the typed bound refusal, the ladder's deterministic fourth class) but nowhere in docs/ — the guide's fan-in section teaches the bound and the child-driven escape without ever naming the class the operator meets
XFAIL tests/test_docs_rv6_pins.py::test_docs3_the_single_value_route_disposition_is_named - LIVE FINDING rv6-docs3: route() is documented only as the per-element (list-source) map face — the single-value Promise[A|B] disposition (the build refusal 'the route is a MAP face … the source produces the elements' list'; the chain step's in-body Route as the single-value routing face) is nowhere named in docs/
1 passed, 3 xfailed in 0.34s
```

## Post-run hygiene

The pack touches no PG (static pins; the shared container at
`localhost:5891` was verified untouched by this pack). The
`pinwork-rv6-docs` worktree carries only the two pin files (untracked)
and the pyright `.venv` shim (untracked); no tracked file was modified.

## THE FLIP (the engine-cures lane, feat/taskqflow-engine-cures)

The three xfail-strict pins were re-convicted at the base head
`0e2f5cc09` (the port's own run: 3 failed + 1 passed under `--runxfail`),
the doc cures landed, and the pins XPASS-strict'd — the markers REMOVED
WITH the cures; the pack stands 4/4 GREEN:

```
tests/test_docs_rv6_pins.py ....    [100%]   4 passed
```

* **rv6-docs1** — the api-reference's wiring-verbs table carries the
  `route` row (and the `chain_source` row the `__all__`-walk convicted
  beside it).
* **rv6-docs2** — the guide's fan-in bound section NAMES
  `FanInBoundExceededError` beside the remedy (the child-driven escape —
  now exposed at the authoring verbs as `child_driven=`, the F6 cure).
* **rv6-docs3** — the guide's route teaching states the single-value
  disposition by name (the MAP-face build refusal + the chain step's
  in-body `Route`).
* **rv6-docs4** — the anti-rot guard ported GREEN (the miscounted
  runbook paragraph lives on the unmerged holdmap lane; the guard holds
  this tree's docs to the count-and-list law).
