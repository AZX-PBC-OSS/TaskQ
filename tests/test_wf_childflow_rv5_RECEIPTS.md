# RECEIPTS — the rv5-child pin pack's red-first protocol

Pin pack: `tests/test_wf_childflow_rv5_pins.py` (the rv5 hostile review
of the T28 child-flows lane, feat/taskqflow-child-flows @ `f8bfc5033`).
Doctrine: every landed finding ran UNMARKED first (the red captured
below), then marked `pytest.mark.xfail(strict=True, reason="LIVE FINDING
rv5-cN: …")` and re-run (the strict-xfail run captured below). The flip
drill on one pin (rv5-c7) proves the mechanics: a temporary in-scratch
cure flips the marked pin to XPASS-strict and the unmarked pin to green;
the patch was then reverted and the marked state re-verified.

## Environment

- Tree: scratch copy `/tmp/opencode/pinwork-child` of
  `/tmp/opencode/childcheck` @ `f8bfc5033` (a worktree at the pinned
  head). **`childcheck` itself was never touched** — every run below
  (and every pre-pin probe) executed with
  `PYTHONPATH=/tmp/opencode/pinwork-child/src`; verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-child/src/taskq/__init__.py`. The only writes
  into the scratch tree: the pin file, its unmarked variant, the
  stripper (all under `tests/`), and the TEMPORARY flip-drill patch
  (`src/taskq/workflows/api/_hitl.py`, +10 lines, reverted with
  `git checkout` — final `git status` clean, pin file sha256
  `0219db45a5e948db785c2dedeed15b14ee0f5efa8797e90aaad00b84c76a9bea`).
- Python: `/tmp/opencode/gatecheck2/.venv/bin/python` (3.13.15,
  pytest 9.1.1, asyncpg 0.31.0, pydantic 2).
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`. The pack's fixture stack mints ONE module-scoped
  schema per pytest invocation named `chp_rv5_<token>` (the lane's
  `chp_` prefix law), truncates + reseeds it per test (the house
  `clean_pg_conn` pattern), and drops it CASCADE at module teardown.
  Post-run hygiene verified after EVERY leg (pre-pin probes, red,
  marked ×3, flip ×2): zero `chp_*` / `tq_*` schemas remain on the
  cluster. The pre-pin probes used `chp_q1` / `chp_q1meta` / `chp_q4c` /
  `chp_q4d` / `chp_q57` / `chp_q6` / `chp_q8a`, each dropped in place;
  the q8b attack script's hardcoded `ch_atk8b` (it ignores `CH_SCHEMA`)
  was dropped too.
- Unmarked variant: `tests/test_wf_childflow_rv5_pins_unmarked.py` —
  the pin file with the 7 `pytest.mark.xfail` decorator blocks stripped
  (mechanical balanced-paren strip, AST-verified: ONLY xfail decorators
  removed, all 36 functions + all 15 non-xfail decorators preserved —
  `make_unmarked.py`), nothing else changed.
- Gates: `ruff check` clean on BOTH variants; `ruff format --check`
  clean on the pin file. `pyright`: THIS venv's pyright reds the whole
  tree pre-existing (`pyright src/taskq tests` → 20717 errors at the
  pin head, landed lane files included — the repo gate's own
  environment differs from this venv). Like-for-like, the pin file
  carries ONLY the two error classes the lane's own landed
  `tests/test_wf_childflow_pins.py` carries under the identical
  invocation (`reportUnknownParameterType`,
  `reportUntypedFunctionDecorator` — the unbound `app.actor` decorator /
  cross-file fixture seam), no new class.

## Pre-pin live reproductions (every finding re-convicted in the scratch copy before pinning)

The attack scripts (`/tmp/opencode/ch_attacks/`, read-only reuse; run
with `PYTHONPATH=/tmp/opencode/pinwork-child/src`):

- **F-C1** (`q1_gather.py`, schema `chp_q1`): `drive: terminal | result:
  {'verdicts': ['found:d1-0', 'found:d1-0', 'found:d1-0'], 'n_handles':
  3, 'distinct_children': 1}` — `child runs on the rows: 1`;
  `CONVICTED — 3 concurrent spawns produced 1 child run(s); inputs lost:
  ['found:d1-1', 'found:d1-2']`. A metadata probe (`chp_q1meta`)
  completed the mechanism: the wake-resume runs as a NEW ATTEMPT
  (`spawn_cursor_1` and `spawn_cursor_2` both `= 1`), so the second
  execution's concurrent spawns all re-read slot 0 and adopt the SAME
  child again — the parent terminalizes succeeded with tenant-0's
  verdict fabricated 3 times.
- **F-C2** (`q4c_respawn.py`, schema `chp_q4c`): `children after happy
  path: 1` → purge the waiting child's rows → `purged-resume drive:
  terminal | result: {'verdict': 'ok', ...} | total child runs now: 2` —
  `SILENT RESPAWN — the purged child was re-created (runs 1 → 2), never
  a ChildFailed fail-close`.
- **F-C3** (`q8a_self.py`, schema `chp_q8a`): `tick 0: 3 … tick 11:
  q8_self runs = 14` — `CONVICTED — direct self-recursion created 14
  runs in 12 ticks, no guard, no depth bound`.
- **F-C4** (`q8b_depth.py`'s staging, re-driven as `_q4d_probe.py`,
  schema `chp_q4d`, depth 1200): `RecursionError after 8.2s`;
  `cancelled rows: 984 / 1200` (216 left RUNNING). The finding's own
  attack observed 983/217 — the split point moves with the host's stack
  depth (under pytest's deeper baseline the pack's red read 952
  cancelled / 248 survivors); the class is the conviction, not the
  count.
- **F-C5** (staging probe `_q57_probe.py`, schema `chp_q57`): the
  orphan-window state staged through the spawn's OWN create path
  (`create_flow(run_key=…, root_metadata_extra={parent_flow_id, …})` —
  byte-identical rows, receipt never written) → `cancel returned: 1 |
  parent root: cancelled | CHILD ROOT: running`.
- **F-C6** (`_q6_probe.py`, schema `chp_q6`): a child sleeping 2.5s
  against `wait(timeout_s=0.2)` on the dev drive → `drive: terminal in
  2.7s | result: {'verdict': "ok:['late']"}` — the deadline dead on the
  dev path. (The pin stages the sharper outliving-child face — see
  PINMAP.md flag 6.)
- **F-C7** (`_q57_probe.py` second leg): `resolve -> delivered` on the
  child-join hold, then the node's re-run registered hold epoch 2
  (`[{'status': 'delivered', 'hold_epoch': 1}, {'status': 'held',
  'hold_epoch': 2}]`) — the no-op bounce, the operator's decision a dead
  letter.

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-child && \
PYTHONPATH=/tmp/opencode/pinwork-child/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_childflow_rv5_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. `7 failed in 11.53s`. Verbatim failure signatures (the
byte-exact full capture is `red-run-raw.txt`; the structlog `node.held`
info lines the engine logs to stdout are trimmed from the sections
below, and C4's expected red-path noise — one `Task was destroyed but
it is pending!` asyncio ERROR naming the RecursionError-destroyed
asyncpg holder-release task — is kept):

```text
_____ test_rv5_c1_concurrent_spawns_in_one_body_each_mint_their_own_child ______
tests/test_wf_childflow_rv5_pins_unmarked.py:291: in test_rv5_c1_concurrent_spawns_in_one_body_each_mint_their_own_child
    assert result.distinct_children == 3, (
E   AssertionError: F-C1: 3 concurrent spawns aliased 1 child flow id(s) — the inputs of the dropped spawns were silently discarded (the gather dragon)
E   assert 1 == 3
E    +  where 1 = GatherOut(verdicts=['found:d1-0', 'found:d1-0', 'found:d1-0'], distinct_children=1).distinct_children
______ test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close _______
tests/test_wf_childflow_rv5_pins_unmarked.py:373: in test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close
    assert result.verdict == "failed", (
E   AssertionError: F-C2: the purged child's join resolved 'ok' — the wait's ChildFailed fail-close arm is the LOUD answer to a missing child root (never a silent respawn-and-join)
E   assert 'ok' == 'failed'
________ test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded ________
tests/test_wf_childflow_rv5_pins_unmarked.py:435: in test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded
    assert census == 1, (
E   AssertionError: F-C3: a self-spawning flow minted 14 runs in 12 ticks — unbounded flow growth from ONE create_flow (the recursion bomb; no cycle check, no depth bound)
E   assert 14 == 1
________ test_rv5_c4_the_cancel_cascade_is_iterative_over_a_deep_chain _________
tests/test_wf_childflow_rv5_pins_unmarked.py:497: in test_rv5_c4_the_cancel_cascade_is_iterative_over_a_deep_chain
    assert cancelled is not None, (
E   AssertionError: F-C4: the cancel cascade blew the Python stack at depth 1200 (RecursionError) — 248 of 1200 runs left NON-terminal, the call raising instead of reporting a bounded partial failure
E   assert None is not None
------------------------------ Captured log call -------------------------------
ERROR    asyncio:base_events.py:1890 Task was destroyed but it is pending!
task: <Task pending name='Task-2503' coro=<PoolConnectionHolder.release() done, …/asyncpg/pool.py:195>>
________ test_rv5_c5_the_cascade_walks_both_directions_no_child_escapes ________
tests/test_wf_childflow_rv5_pins_unmarked.py:588: in test_rv5_c5_the_cascade_walks_both_directions_no_child_escapes
    assert child_root["status"] == "cancelled", (
E   AssertionError: F-C5: the unreceipted child escaped the parent's cancel (child root 'running') — the cascade walked the receipts only and never the parent_flow_id provenance it could see; the orphan runs on with its parent dead
E   assert 'running' == 'cancelled'
_______ test_rv5_c6_the_dev_drive_s_tick_runs_the_child_join_expiry_arm ________
tests/test_rv5_c6_the_dev_drive_s_tick_runs_the_child_join_expiry_arm
    assert outcome == "terminal", (
E   AssertionError: F-C6: the dev drive never delivered the join's Expired arm — the parent held past its own timeout_s=0.2 deadline for the whole drive ('max_ticks'); sweep_expired_signals is wired in the fleet leader sweeps only, never in FlowRunner.tick
E   assert 'max_ticks' == 'terminal'
_____ test_rv5_c7_resolving_a_child_join_hold_is_a_named_boundary_refusal ______
tests/test_wf_childflow_rv5_pins_unmarked.py:730: in test_rv5_c7_resolving_a_child_join_hold_is_a_named_boundary_refusal
    assert result.status == "refused", (
E   AssertionError: F-C7: resolving a child-join hold returned 'delivered' — the delivery resolves NOTHING (the wait site re-holds on the still-live child; the operator's decision is a dead letter). The boundary must refuse the machine-owned hold, naming its kind
E   assert 'delivered' == 'refused'
=========================== short test summary info ============================
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c1_concurrent_spawns_in_one_body_each_mint_their_own_child
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c2_a_missing_child_root_at_the_wake_is_a_loud_fail_close
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c3_a_self_spawning_flow_is_refused_loudly_and_bounded
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c4_the_cancel_cascade_is_iterative_over_a_deep_chain
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c5_the_cascade_walks_both_directions_no_child_escapes
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c6_the_dev_drive_s_tick_runs_the_child_join_expiry_arm
FAILED tests/test_wf_childflow_rv5_pins_unmarked.py::test_rv5_c7_resolving_a_child_join_hold_is_a_named_boundary_refusal
7 failed in 11.53s
```

Zero teardown errors: the always-on G7 rows↔status check (auto-registered
on every `test_wf_*` async test by `tests/conftest.py`, running against
the pack's `chp_rv5_*` schema via the module-local `wf_conn`/`wf_schema`/
`wf_sql` overrides) stayed QUIET on every red state (the wedged/held/
orphaned/live-root states all reconstruct running/blocked/pending —
agreement; audited per-pin before landing the file).

THE PACK'S OWN SEAM (found while pinning, fixed in the fixture stack,
never in the source): the F-C4 red's RecursionError destroys an asyncpg
`PoolConnectionHolder.release()` task mid-flight; a graceful
`pool.close()` then waits on the never-released holder FOREVER (the
first red run wedged the suite at C4's teardown — zero DB activity,
epoll_wait). The pack's `wf_pool` fixture TERMINATES
(`pool.terminate()`) instead — a red must never wedge the suite. The
standalone probe process wedged the same way at its own `pool.close()`
(hours-long zombie reaped during this pack's hygiene pass).

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-child && \
PYTHONPATH=/tmp/opencode/pinwork-child/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_childflow_rv5_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. `7 xfailed in 11.94s` (`marked-run-raw.txt`, byte-exact).
Determinism: executed THREE times on the final bytes — the initial
marked run (`marked-run-raw.txt`), a determinism re-run
(`marked-run-determinism-raw.txt`, `7 xfailed in 11.28s`), and the
post-revert verification after the flip drill (`marked-run-final-raw.txt`,
`7 xfailed in 11.92s`) — identical verdicts every time. The reds are
structural (the cursor's non-atomic claim, the arbiter's row-bound
memory, the missing cycle guard, the Python recursion, the
receipts-only walk, the unwired arm, the kind-blind boundary), not
timing: the only wall-clock-sensitive pin (C6) asserts a STATE (the
drive's verdict + the body's member), and its deadline is DB-clock.

## THE FLIP DRILL (the mechanics proof, rv5-c7)

A temporary cure was applied IN THE SCRATCH COPY ONLY
(`src/taskq/workflows/api/_hitl.py`, `_boundary_verdict`: +10 lines —
refuse `signal_name LIKE '__child__:%'` at the resolve boundary with a
reason naming the `child_join` kind, before any model fitting):

- Marked pin under the cure (`flip-c7-marked-raw.txt`): exit 1 —
  `FAILED … [XPASS(strict)] LIVE FINDING rv5-c7: …` — the strict
  marker's designed red: *remove the marker WITH the cure*.
- Unmarked pin under the cure (`flip-c7-unmarked-raw.txt`): exit 0 —
  `1 passed` — the pin asserts the safe behavior, not a caricature of
  the defect.

The patch was then reverted (`git checkout -- src/taskq/workflows/api/_hitl.py`;
`git status` clean; pin-file sha256 unchanged) and the marked state
re-verified (`marked-run-final-raw.txt`).

## Post-run hygiene

Verified after every leg: zero `chp_*` and zero `tq_*` schemas remain on
`postgres://taskq:taskq@localhost:5891` — the pack's module fixture drops
its `chp_rv5_<token>` schema at teardown, and the pre-pin probe schemas
(`chp_q1`, `chp_q1meta`, `chp_q4c`, `chp_q4d`, `chp_q57`, `chp_q6`,
`chp_q8a`, and the q8b script's hardcoded `ch_atk8b`) were dropped in
place. The two wedged probe processes from the pre-`terminate()` legs
were reaped. Other reviewers' artifacts were not touched;
`/tmp/opencode/childcheck` was never modified.

---

# THE RESIDUAL-CURES LANE'S RECONCILED FLIPS (the adoption-with-flip-receipts rows)

Pack ported to the reconciliation ground `feat/taskqflow-residual-cures`
(the four cure lanes' merges + this lane's cures). The pre-reconciliation
state was captured at the lane head BEFORE the reconciling commit
(`.measurements/residual-cures/child-pack-pre-reconciliation.txt`: C4-C7
XPASS-strict, C1-C3 xfail).

| Pin | Pre-flip state at the reconciliation ground | The cure the flip adopts | Receipt |
| --- | --- | --- | --- |
| C1 (the gather dragon) | XFAIL under the marker / PASS unmarked ×4 + ×3 under --runxfail — the atomic slot claim (the child-cures lane's C1 cure) landed; the markers' own accounting flaked on the timing-sensitive gather | the marker REMOVED WITH the cure (the K-distinct-children shape asserted unchanged — the cure's own shape) | this row |
| C2 (the retention-respawn) | XFAIL — the cured fail-close lives AT THE RESPAWN ATTEMPT: the resume re-runs the spawn site, the dangling-receipt walk raises the TYPED `ChildFlowLostError`, the deterministic route terminal-fails the node, T06's propagation fails the root | the reconciled pin reads the ROWS (the node's error class names the wedge, the root failed LOUDLY, the census stayed 0) — the original pin's wait-arm shape (`ChildFailed` through `handle.wait()`) is unreachable behind spawn-then-wait, exactly as the conviction's own words say | this row |
| C3 (the recursion bomb) | XFAIL — the cured shape is the DEPTH BOUND (the ancestor-chain walk, `DEFAULT_SPAWN_MAX_DEPTH` = 32, the typed `SpawnDepthExceededError`), not the first-spawn refusal (census == 1) the pin asserted | the reconciled pin asserts the cured shape's whole substance: the tree TERMINALIZES (drive returns terminal), the census is bounded at the depth arithmetic (1 + the bound), and the typed refusal FIRED on the record (the rows' parent-link walk scopes the family) | this row |
| C4 (the cascade's stack) | XPASS-strict — the iterative cascade landed | the marker REMOVED WITH the cure | this row |
| C5 (the orphan window) | XPASS-strict — the both-directions walk landed | the marker REMOVED WITH the cure | this row |
| C6 (the dev-tick expiry gap) | XPASS-strict — the dev drive's expiry arm landed | the marker REMOVED WITH the cure | this row |
| C7 (the no-op resolve bounce) | XPASS-strict — the typed resolve refusal landed | the marker REMOVED WITH the cure | this row |

| C1 (order-leg addendum) | the reconciled C1 flaked on the strict VERDICT ORDER leg ('found:d1-0, found:d1-2, found:d1-1'): the unkeyed fan-out's slot race re-associates the handles across the hold-resume — the spawn docstring's own contract ('must join STABLY across the resume passes key= — the slot's allocation order is the scheduler's') | the order leg reconciled to the LAW (the no-fabrication SET + the distinct children); the keyed stable-join face stays the child-cures lane's own pin | this row |
