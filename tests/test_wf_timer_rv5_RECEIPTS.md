# RECEIPTS — the rv5-timer pin pack's red-first protocol

Pin pack: `test_wf_timer_rv5_pins.py` (the T28 durable-timer lane's
hostile-review convictions + the T29 redeploy face's operator surfaces,
feat/taskqflow-durable-timers @ `08dccb92d`, based on `37ab05b8`).
Doctrine: every landed finding was run UNMARKED first (the red captured
below), then marked `pytest.mark.xfail(strict=True, reason="LIVE FINDING
rv5-t<id>: …")` and re-run (the strict-xfail run captured below). This
pack carries NO green guard — all seven pins are live convictions.

## Environment

- Tree: the detached worktree `/tmp/opencode/pinwork-timer` of
  `/home/rich/src/TaskQ` @ `08dccb92d` (created by
  `git worktree add --detach`). The shared gatecheck2 venv
  (`/tmp/opencode/gatecheck2/.venv`, Python 3.13.15, pytest 9.1.1,
  asyncpg 0.31.0, pydantic 2.13.5) carries an editable `taskq` install
  pointing at gatecheck2's tree; every run below therefore carried
  `PYTHONPATH=/tmp/opencode/pinwork-timer/src` (verified:
  `python -c "import taskq; print(taskq.__file__)"` →
  `/tmp/opencode/pinwork-timer/src/taskq/__init__.py`). For the pyright
  gate the worktree got its own `.venv` shim (symlinked site-packages
  with `_editable_impl_taskq_py.pth` rewritten to
  `/tmp/opencode/pinwork-timer/src`): pyright 1.1.414 over the repo's
  `[tool.pyright]` config reports **0 errors, 0 warnings** on the pin
  file; `ruff check` + `ruff format --check` clean on BOTH variants.
- PG: `postgres://taskq:taskq@localhost:5891/taskq` via
  `TASKQ_TEST_PG_DSN`; per-module databases are the fixture machinery's
  own (`TASKQ_TEST_RUN_TOKEN=rv5tmp*` mixed into the hashed `tq_db_%`
  names); the pre-pin repro script used schema `tmp_probe_rv5` (plus the
  throwaway `tmp_probe_dbg`), each dropped in-place at the script's own
  end. Post-run hygiene verified after every leg: zero `cap_%`/`tmp_%`
  schemas and zero `tq_db_%` databases of this pack's tokens remain on
  the shared container.
- Unmarked variant: `tests/test_wf_timer_rv5_pins_unmarked.py` — the pin
  file with the 7 `pytest.mark.xfail` decorator blocks stripped
  (AST-verified mechanical strip — ONLY xfail decorators removed, all 25
  functions + the 11 non-xfail decorators preserved —
  `repro/make_unmarked.py`), nothing else changed.

## Pre-pin live reproductions (ad hoc, each finding convicted before pinning)

Script + full output in `repro/` (`probe_timer.py` / `probe_timer.txt`).
The verbatim convictions:

- **F-T1** (the trojan prefix): `create_model("__timer__Approval", …)` at
  the wait site minted hold `01a129…` with `signal_name='__timer__Approval'`
  — `HitlClient.list()` returned **0** holds (invisible); the hand answer
  `{"verdict": "approve"}` was **refused** ("this hold is a DURABLE TIMER
  … no hand delivers the wake"); `sweep_expired_signals` fired **0** (the
  fail-close never comes); `sweep_due_timers` then fired **1** and the
  resume died: node `failed` with `error_class='SignalPayloadError'`
  ("the delivered payload validates against NONE of the wait's declared
  models (['__timer__Approval'])").
- **F-T2** (the broadcast leak): a `wait_until` hold with a LIVE listener
  delivered `HoldCreated(signal='__timer__leak', source='notify')`; a
  listener started AFTER the mint delivered
  `HoldCreated(signal='__timer__leak', source='backfill')` and its
  `Backfilled.open_hold_ids` carried the timer hold's id.
- **F-T3a** (the 10k tail): 1500 due timer rows seeded; ONE
  `sweep_due_timers` pass fired exactly **100** in 0.02s and left 1400
  held (~50 minutes for 10k at the 30s default cadence);
  `sweep_due_timers(..., drain=True)` → `TypeError: sweep_due_timers()
  got an unexpected keyword argument 'drain'`.
- **F-T3b** (the missing index): the `wf_signals` migrations carry only
  `wf_signals_hold_identity_uniq` (workflow_id, node_key, signal_name,
  hold_epoch WHERE resolved_at IS NULL), `wf_signals_id_status_idx`
  (id, status), `wf_signals_run_status_idx` (workflow_id, status, id) —
  nothing covers `expires_at`.
- **F-T4** (the false affordance): rendering `workflow_detail.html` (the
  admin factory's own jinja2 Environment construction, autoescape on)
  with one `__timer__leak` hold + one `Approval` hold, both `held` with
  `actions_enabled=True`, renders `data-wf-form="resolve"` for **BOTH**
  (`['h1timer', 'h2human']`).
- **F-T5** (the drift-invisible operator): the two-node flow with the
  registry swap mid-run completed `second:v2:first:v1!` with **2
  distinct** `code_version` stamps on the run's rows; `taskq flows
  status <run>` exits 0 over five lines naming NO version anywhere.
- **F-T6** (the structural-redeploy wedge): after the rename redeploy
  (registry + compiled graph carry `renamed`, never `second`), the
  new-process `drive()` of the in-flight run RAISED
  `WorkflowRunError: step 'second' is not in workflow
  'rv5t6_wedge_flow''s compiled graph nor in its registered definition —
  a foreign step key (the registry is the only body source, D1)`; rows:
  `__flow__` running, `first` succeeded, `second` pending — forever.
- **PROBE ARTIFACT (mine, documented)**: the probe's F-T5 CLI leg ran
  `CliRunner.invoke` inside the probe's own event loop — the CLI's
  `asyncio.run()` refused (`RuntimeError: asyncio.run() cannot be called
  from a running event loop`, the `RuntimeWarning` at the capture's
  tail). The PIN runs the CLI through `to_thread` (the house's cli-pins
  idiom) and convicts cleanly (the red below).

## RUN 1 — UNMARKED (the red-first run)

Command:

```
cd /tmp/opencode/pinwork-timer && \
PYTHONPATH=/tmp/opencode/pinwork-timer/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv5tmp2 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_timer_rv5_pins_unmarked.py -p no:randomly --tb=short -q
```

Exit code: 1. Verbatim output (also kept as `red-run-raw.txt`):

```text
FFFFFFF                                                                  [100%]
=================================== FAILURES ===================================
______________ test_t1_wait_signal_refuses_a_timer_prefixed_model ______________
tests/test_wf_timer_rv5_pins_unmarked.py:239: in test_t1_wait_signal_refuses_a_timer_prefixed_model
    assert node["error_class"] != "SignalPayloadError", (
E   AssertionError: F-T1: the run died the TROJAN death — the minted '__timer__Approval' hold was invisible to the client and unanswerable by hand, then the TIMER ARM fired it at the deadline and the wait site's revalidation killed the run (SignalPayloadError). The mint itself must be refused
E   assert 'SignalPayloadError' != 'SignalPayloadError'
_________ test_t2_the_broadcast_and_backfill_never_surface_timer_holds _________
tests/test_wf_timer_rv5_pins_unmarked.py:288: in test_t2_the_broadcast_and_backfill_never_surface_timer_holds
    assert timer_creations == [], (
E   AssertionError: F-T2 (notify leg): the listener announced a TIMER hold as a HUMAN hold — HoldCreated events for the clock's own rows: [('__timer__leak', 'notify')]
E   assert [HoldCreated(...rce='notify')] == []
___________ test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade ___________
tests/test_wf_timer_rv5_pins_unmarked.py:376: in test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade
    assert remaining == 0, (
E   AssertionError: F-T3a: one sweep cascade left 1400 of 1500 due timers HELD (the arm fires one bounded batch per pass — at the 30s default cadence a 10k backlog takes ~50 minutes to drain)
E   assert 1400 == 0
______________ test_t3b_a_due_scan_index_exists_in_the_migrations ______________
tests/test_wf_timer_rv5_pins_unmarked.py:409: in test_t3b_a_due_scan_index_exists_in_the_migrations
    assert due_scan, (
E   AssertionError: F-T3b: no wf_signals index covers expires_at — the timer due-scan (status='held' + the __timer__ prefix + expires_at <= now ORDER BY expires_at) seq-scans + sorts per tick. The shipped indexes: ['CREATE UNIQUE INDEX wf_signals_hold_identity_uniq', 'CREATE INDEX wf_signals_id_status_idx', 'CREATE INDEX wf_signals_run_status_idx']
E   assert []
____________ test_t4_timer_rows_never_render_the_human_resolve_form ____________
tests/test_wf_timer_rv5_pins_unmarked.py:492: in test_t4_timer_rows_never_render_the_human_resolve_form
    assert "rv5t4timer" not in forms, (
E   AssertionError: F-T4: the TIMER hold row renders the human resolve form — a control whose every submission is refused-as-forgery is a false affordance on the operator's page
E   assert 'rv5t4timer' not in ['rv5t4timer', 'rv5t4human']
_______________ test_t5_flows_status_surfaces_the_version_drift ________________
tests/test_wf_timer_rv5_pins_unmarked.py:557: in test_t5_flows_status_surfaces_the_version_drift
    assert drift_lines, (
E   AssertionError: F-T5: `taskq flows status` never mentions the version drift — the run carries 2 distinct code_versions and the operator sees nothing. The output: ['run 01a129e7-…', 'workflow: rv5t5_drift_flow', "status: complete  (derived from the node rows — §17.5; the root row caches 'succeeded')", 'nodes: 2 — 2 succeeded (source: jobs rows by metadata.flow_id)', 'next: nothing is stuck — the run is progressing or terminal']
E   assert []
______________ test_t6_a_vanished_node_key_terminal_fails_loudly _______________
tests/test_wf_timer_rv5_pins_unmarked.py:614: in test_t6_a_vanished_node_key_terminal_fails_loudly
    assert node["status"] == "failed", (
E   AssertionError: F-T6: the vanished node's run NEVER terminalizes — 'second' is 'pending' and the root follows it (the eternal zombie: budget-free parked, detector-logged at best, surfaced nowhere ON THE RUN). The loud law: terminal-fail with a NAMED error class
E   assert 'pending' == 'failed'
=========================== short test summary info ============================
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t1_wait_signal_refuses_a_timer_prefixed_model
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t2_the_broadcast_and_backfill_never_surface_timer_holds
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t3b_a_due_scan_index_exists_in_the_migrations
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t4_timer_rows_never_render_the_human_resolve_form
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t5_flows_status_surfaces_the_version_drift
FAILED tests/test_wf_timer_rv5_pins_unmarked.py::test_t6_a_vanished_node_key_terminal_fails_loudly
7 failed in 9.97s
```

(The T2 diff-echo's trailing `Use -v` lines and the T1 captured-stdout
block are trimmed above; `red-run-raw.txt` is the untrimmed byte-exact
capture.)

Determinism: the unmarked run was executed TWICE (rv5tmp2, rv5tmp4) with
identical results (7 failed, same failing assertions); the reds are
structural (missing refusals/filters/indexes/surfaces, the raw escape,
the un-drained batch), not timing — the listener pins' 1.5s collect
windows are safety nets (the notifies land in ms; the cured tree simply
has nothing to deliver), and the T1/T6 drives' `max_ticks` bounds only
bound the red's wall clock.

## RUN 2 — MARKED (the strict-xfail run)

Command:

```
cd /tmp/opencode/pinwork-timer && \
PYTHONPATH=/tmp/opencode/pinwork-timer/src \
TASKQ_TEST_PG_DSN=postgres://taskq:taskq@localhost:5891/taskq \
TASKQ_TEST_RUN_TOKEN=rv5tmp3 \
/tmp/opencode/gatecheck2/.venv/bin/python -m pytest \
  tests/test_wf_timer_rv5_pins.py -p no:randomly --tb=short -q
```

Exit code: 0. Verbatim output (also kept as `marked-run-raw.txt`):

```text
xxxxxxx                                                                  [100%]
=========================== short test summary info ============================
XFAIL tests/test_wf_timer_rv5_pins.py::test_t1_wait_signal_refuses_a_timer_prefixed_model - LIVE FINDING rv5-t1: wait_signal mints a hold under the RESERVED __timer__ prefix from a trojan model name (observed: the __timer__Approval hold invisible to HitlClient.list(), the hand answer refused as a forgery, the expiry arm's fail-close never fires on it, and the TIMER arm self-fires it at the deadline into error_class='SignalPayloadError' killing the run) — no mint-time refusal exists
XFAIL tests/test_wf_timer_rv5_pins.py::test_t2_the_broadcast_and_backfill_never_surface_timer_holds - LIVE FINDING rv5-t2: durable timers surface as pending HUMAN holds on the T26 faces — the hold-create notify is unconditional (register_hold's create leg) and the listener's backfill reads WHERE status='held' with no prefix filter (_hitl_listen.py) — observed: HoldCreated(signal='__timer__leak') on the notify leg AND the backfill leg, and Backfilled.open_hold_ids carrying the timer id
XFAIL tests/test_wf_timer_rv5_pins.py::test_t3a_the_timer_arm_drains_the_due_set_in_one_cascade - LIVE FINDING rv5-t3a: sweep_due_timers fires ONE bounded batch (batch_size=100, no drain) per pass — observed: 1500 seeded due timers, one pass fired exactly 100 and left 1400 held; at the 30s default cadence a 10k backlog drains in ~50 minutes; no drain= knob exists (TypeError)
XFAIL tests/test_wf_timer_rv5_pins.py::test_t3b_a_due_scan_index_exists_in_the_migrations - LIVE FINDING rv5-t3b: no index leads the timer due-scan — the wf_signals indexes are (id, status) and (workflow_id, status, id) plus the hold-identity unique partial; the sweep's WHERE status='held' AND starts_with(signal_name,'__timer__') AND expires_at <= now ORDER BY expires_at is a seq scan + sort per tick
XFAIL tests/test_wf_timer_rv5_pins.py::test_t4_timer_rows_never_render_the_human_resolve_form - LIVE FINDING rv5-t4: the admin run page renders the human RESOLVE form on a TIMER hold row — workflow_detail.html's guard is `h.status == 'held' and actions_enabled` (any held row); observed at render level: the __timer__ hold row renders data-wf-form="resolve" exactly like the human hold's
XFAIL tests/test_wf_timer_rv5_pins.py::test_t5_flows_status_surfaces_the_version_drift - LIVE FINDING rv5-t5: zero code_version references in the CLI/admin — the stamps exist (jobs.code_version per claim) but no surface reads them: the mixed-version run (observed: 2 distinct stamps on one run's rows) gets NO drift line in `taskq flows status`
XFAIL tests/test_wf_timer_rv5_pins.py::test_t6_a_vanished_node_key_terminal_fails_loudly - LIVE FINDING rv5-t6: rename a node in a redeploy and the in-flight run's pending row zombies FOREVER — observed: the new-process drive RAISES WorkflowRunError raw ('step \'second\' is not in workflow ... compiled graph nor in its registered definition'), the node stays 'pending', the root stays 'running', NEVER terminal (the fleet's face parks it budget-free at the snooze cadence; nothing surfaces ON THE RUN)
7 xfailed in 7.82s
```

## Post-run hygiene

Verified after the repro legs and every pytest run: zero
`cap_%`/`tmp_%` schemas and zero `tq_db_%` databases of this pack's
tokens remain on the shared container
(`postgres://taskq:taskq@localhost:5891`). The pre-existing `tq_db_%`
databases belong to other reviewers' tokens (enumerated, untouched).
The `pinwork-timer` worktree itself carries only the two pin files
(untracked) and the pyright `.venv` shim (untracked); no tracked file
was modified. The F-T2 pin cancels its own flow in a `finally` (the
red path's hygiene — the module schema is shared across the file), and
the F-T3a pin reads its counters baseline-relative for the same reason.

---

# THE RESIDUAL-CURES LANE'S RECONCILED FLIPS (the adoption-with-flip-receipts rows)

Pack ported to the reconciliation ground `feat/taskqflow-residual-cures`
(the four cure lanes' merges + this lane's cures). Every flip below is
green x2 at the lane's final head; the pre-flip reds were captured at
the lane head BEFORE the cure commits
(`.measurements/residual-cures/residual-reds-pre-cure.txt`).

| Pin | Pre-flip state at the reconciliation ground | The cure the flip adopts | Receipt |
| --- | --- | --- | --- |
| T1 (the trojan-prefix mint refusal) | XPASS-strict (the pin asserted the SAFE behavior and PASSED — the marker's own lie) | the timer-cures lane's E17 cure (`ReservedTimerNamespaceError` at the `wait_signal` mint; deterministic route) | this row; the cure's own red-first receipt: the timer-cures lane's estate |
| T2 (the broadcast/backfill leak) | XPASS-strict | the timer-cures lane's prefix filters (the create leg's notify + `_hitl_listen.py`'s backfill) | this row; as above |
| T3a (the drain cascade) | XPASS-strict | the timer-cures lane's `drain=True` sweep cascade | this row; as above |
| T3b (the due-scan index) | XFAIL — but for the WRONG reason: the index SHIPS (`01.00.34_01_pre_wf_timer_due_index.sql`, the partial index leading `expires_at` with the timer predicate — better than asked); the pin's regex choked on `IF NOT EXISTS` and read the shipped index as NO index | the matcher fixed to the honest shape (`(?:IF\s+NOT\s+EXISTS\s+)?`); the marker removed | this row; the red's verbatim: `.measurements/residual-cures/residual-reds-pre-cure.txt` (the xfail leg) |
| T4 (the false affordance) | XFAIL — the template cure SHIPS (the `is_timer` branch) but the render harness's raw dicts predate the enrichment field (`h.is_timer` undefined → the human leg's form) | the harness rides the PRODUCTION enrichment (`web/admin/_wf_rows.py`'s `_normalize_hold` — the row's own truth); the marker removed | this row; red as above |
| T5 (the CLI drift face) | XFAIL — zero `code_version` references in the status face (the stamps unread) | THIS LANE'S CURE: `flows status` reads the run's rows' §22.1 stamps; >1 distinct renders the drift line — the drifted nodes named with their hashes, ONE coherent section with the stranded face (`code_version_drift_lines` in `workflows/_cli.py` + the stamps' read in `cli.py::_flows_status`) | this row; red as above |
| T6 (the vanished-node wedge) | XFAIL — the new-process drive raised `WorkflowRunError` raw; the row stayed `pending`, the root `running` | THIS LANE'S CURE: the typed `VanishedNodeError` (the wedge named + the registry-is-truth law + the cancel-then-restart remedy); `_run_node`/`run_fleet_claimed_step` claim-then-ladder — the DETERMINISTIC route terminal-fails the node on the first attempt (the class + remedy on the record), T06's propagation fails the root; never a zombie | this row; red as above |

Flip runs (green x2 at the final head): see the lane's landing capture
(`.measurements/residual-cures/`).
