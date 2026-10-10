# RECEIPTS — the rv3 teardown pack's red-first protocol

Pin pack: `tests/test_wf_teardown_rv3.py` (the teardown review's
convictions, feat/taskqflow @ `512f7a91`). Doctrine: every landed
finding was run UNMARKED first (the red captured below), then marked
`pytest.mark.xfail(strict=True, reason="LIVE FINDING rv3-<id>: …")`
and re-run (the strict-xfail run captured below). The green pins
(rv3-4c, rv3-12b — plus rv3-1b, whose documentation face the head
already satisfied) are the RESOLVED-behavior / zero-false-positive
guards — green by design at the head (see PINMAP.md).

## Environment

- Tree: the lane's own worktree `/tmp/opencode/wt-rv3` (branch
  `feat/taskqflow-rv3`), cut from feat/taskqflow's LOCAL tip
  `512f7a91`. NOTE: the task's brief named `33c05d5c` as the tip; the
  pin-carry lane (which owns `wt-land`) landed `512f7a91` (the
  allowlist fold) before this lane spun up — the branch tracks the
  CURRENT local tip, whose format-clean `test_suite_hygiene.py` the
  lane's own proof requires.
- Python: the worktree's `.venv` (3.14.7; uv sync --all-groups
  --all-extras; the worktree installed editable).
- PG: `postgresql://postgres:taskq@localhost:5766/taskq` via
  `TASKQ_TEST_PG_DSN` — the lane's own container (`taskq-rv3-pg`,
  contract-true: `max_connections=1000`, `fsync=off`, the `taskq`
  role; destroyed at close). Per-module databases/schemas are the
  fixture machinery's own; the pre-pin probe used schema `rv3probe`,
  dropped in place.
- Unmarked variant: the pin file AS COMMITTED at `9ca7460d`'s parent
  state ran UNMARKED — the red runs below are
  `/tmp/opencode/rv3-receipts/red-run1-unmarked.txt` (the first cut;
  the loop pins' wiring-shape correction) and
  `red-run2-unmarked.txt` (the corrected cut — THE RECEIPT OF RECORD),
  then the marked run `xfail-run3.txt` (13 xfailed, 3 passed — the
  same tree, only the markers differ).

## Pre-pin live reproductions (each finding convicted before pinning)

- F-RV3-1 — `inspect.signature(map_source)` carries
  `key: str | None = None`; the body raises `WorkflowBuildError` for
  every value but the derived key (`_graph.py`'s accept-and-ignore
  guard). A param that only errors.
- F-RV3-2 — `run(compiled, pool, bare_schema)` raised the RAW
  `asyncpg.exceptions.UndefinedTableError` out of
  `FlowRunner.create_flow` (`_run.py:92` in the traceback) — the
  packaged door's first contact, untranslated.
- F-RV3-3 — `apply_pending(pool, schema="taskq")` raised
  `AttributeError` (the Pool's missing `transaction()`); the
  annotation promised a `Connection` and the runtime never checked.
- F-RV3-4a — a `GateDecl(name="Review", payload_models=(_Approval,),
  timeout_s=30.0)` on a step whose body returns without any
  `wait_signal` reference: `app.get(...)` (the registration door)
  validated CLEAN — no rule walked the gate against the body.
- F-RV3-4b — the mirror: a body `await ctx.wait_signal((_Approval,),
  timeout_s=60.0)` with NO `gates=` on the node: CLEAN (the hold the
  admin's resolve/deliver doors cannot see, compiled invisibly).
- F-RV3-5 — `python -m taskq.cli --help`'s output omits the
  tail-registered verbs the imported app registers (the __main__ block
  at `cli.py:2676` runs `main()` before the workgroup/flows tails
  register). The subprocess pin names the dead set.
- F-RV3-6 — `inspect.signature`: `HitlClient.__init__`'s `schema` is
  `KEYWORD_ONLY`; `FlowRunner.__init__`'s is `POSITIONAL_OR_KEYWORD`.
- F-RV3-7 — a failed run (`doomed` body raising `RuntimeError(
  "boom-the-teardown")`, `max_attempts=1`, `retry_kind="permanent"`):
  `drive` returned terminal, and `result(flow_id)` returned `{}`
  (dict) — the pre-pin probe's exact face:
  `result() on the FAILED run returned: {} dict`; the root row
  `status=failed error_class=UnabsorbedNodeFailure`, the node row
  `error_class=RuntimeError error_message=boom-the-teardown` (the
  typed read never raised; `WorkflowRunError` exists unused by the
  read).
- F-RV3-8 — the `@app.actor` bodies' nodes each logged
  `node.code-version-unstamped` with
  `error='module, class, method, function, traceback, frame, or code
  object was expected, got WorkflowActor'` — `code_version` NULL on
  every node (the wrapper's identity is not sourceable; the stamper's
  best-effort arm logged the loss). The canonical path the docs teach
  ships §22.1's deploy-drift record dead.
- F-RV3-9 — the held run's `HitlClient.list(...)`: the hold's
  `payload['reason'] == 'awaiting compliance sign-off'` (the insert's
  context doc) and `HoldContext.reason is None` (hardcoded at
  `_hitl.py:879`).
- F-RV3-10 — `GateDecl(timeout_s=30.0)` against the body's literal
  `timeout_s=45.0`: `validate_compiled` returned ZERO drift
  diagnostics; and `GateDecl.__doc__` states the runtime raise face
  without the split's precedence (no compile-face statement, no
  which-source-arms-the-runtime statement).
- F-RV3-12 — the loop body's conditional-interior
  `await ctx.wait_signal(...)` (inside `if carry.n > 0:`): CLEAN — no
  rule names the mis-index (the T26 review's C9).

## The strict-xfail run (the markers on, the same tree)

`3 passed, 13 xfailed in 1.92s` — the 13 LIVE FINDINGS xfailed
strictly (a cure that does not flip its pin fails the suite with an
unexpected XPASS), the 3 guards green.

## Notes

* The red runs never touched the source tree (the pins build their own
  throwaway apps; the probe's `rv3probe` schema was dropped in place).
* The lane's PG container is destroyed at close; the receipts live in
  `/tmp/opencode/rv3-receipts/` (the /tmp-ephemeral law: the durable
  record is THIS file + PINMAP.md beside the pins).
