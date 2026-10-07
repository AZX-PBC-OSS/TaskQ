# PHASE-1 CERTIFICATION — TaskQflow core (feat/taskqflow @ 244abfb5)

Certifier: the PHASE-1 CERTIFIER (independent; verified against the code and the database, not the reports).
Date: 2026-10-07
Verdict: **REFUSED** — 2 blockers, 1 medium, 3 low. The engine's attack surface is genuinely fixed
(every drill below held), but the fix wave left a red estate gate on the branch and B3's cure is
half-vacuous outside a single process. The core may NOT carry phase 2 until the two blockers close.

---

## 1. The attack tests — GREEN, and the fixes are in the SHIPPED paths

All six attack tests green, four independent runs (×3 in one session plus the initial):

```
tests/attack_wf_map_ledger_pk.py            PASSED
tests/attack_wf_runkey_scope_collision.py   PASSED
tests/attack_wf_engine_windows.py           PASSED (3 attacks)
tests/attack_wf_dispatch_fence.py           PASSED
tests/typeprobe/attack_wf_negative_types.py PASSED
```

Live revert drills (I mutated the shipped source, ran the attack, restored — tree verified clean after):

* **B2 drill:** `git checkout 6b6e5b77~1 -- src/taskq/backend/_dispatch_sql.py` →
  `test_attack_dispatch_claims_workflow_child_of_cancelled_flow` **FAILED** (the dead flow's
  child was claimed). The fix is the `_wf_dispatch_fence` fragment baked into the shipped claim
  template at its admission sites — not a test fixture.
* **B1 drill:** reverted `_sql_ledger.py` + `01.00.23_02/_03` to 71436c99~1 →
  `test_attack_map_children_ledger_pk_collision` **FAILED** (child 1's claim returned child 0's
  row, `job_id` mismatch — the convicted collapse). The fix is the shipped arbiter expression,
  the shipped ON CONFLICT target, and the shipped DDL.

## 2. The 8 vacuous replacements (a1b4e934) — real mutations, spot-drilled

Read all eight replacements. The reds now target shipped artifacts (module statement constants,
module functions, the real DB) rather than local lambdas/own-data. Two spot-drills, mutating the
SHIPPED source (not the test's monkeypatch), both red the pin:

* **ledger 1:** shipped `LEDGER_MEMOIZED_SQL` mutated in
  `src/taskq/workflows/_sql_ledger.py` (`status IN ('succeeded','failed')` → `'no-such-status'`)
  → `test_pin_1_double_run_red_and_memoized_green` **FAILED**.
* **finalize 1:** shipped `REDERIVE_SWEEP_SQL` write-guard flipped in
  `src/taskq/workflows/_sql_sweep.py` (`<>` → `=`) →
  `test_pin_1_snapshot_write_reds_and_the_shipped_arm_is_exact` **FAILED**.

Weaker class, noted (§7b): engine-12 and engine-16's reds patch a module symbol and then invoke
the patched symbol directly — the shipped artifact is the mutation target, but the mutated
behavior is not driven through the shipped dispatch/finalize entry. Acceptable as comparator
drills; the green sides do drive shipped code.

## 3. The blockers' curses

* **B1 (ledger-PK):** `wf_step_ledger_claim_uniq` (01.00.23_03) keys
  `(flow_id, step_key, COALESCE(map_index, -1), attempt)` — verbatim the claim's ON CONFLICT
  expression. `finalize_node` threads `ledger_id` (the claim's RETURNING id) into
  `LEDGER_TERMINAL_BY_ID_SQL` / `LEDGER_FENCE_BY_ID_SQL`, with the map-aware arbiter tuple as
  fallback. Coherent.
* **B2 (dispatch fence):** my own `EXPLAIN (ANALYZE, BUFFERS)` on a seeded schema (300 vanilla
  pending rows + a cancelled flow's child + a live child): the candidates laterals ride
  `jobs_unrouted_actor_dispatch_idx`, both lock steps and the terminal race guard ride
  `jobs_dispatch_idx`; the fence appears as `(step_key IS NULL) OR (NOT (ANY (... = hashed
  SubPlan)))` with the subplan evaluated ONCE per statement (9 buffers, loops=1). Index-served.
  The dispatch band re-run by me: **p50 7.44 ms / p99 11.46 ms vs the 50 ms budget** (green).
  The refusal + no-over-rejection legs both hold (pin 2 green).
* **B3 (sweep arms):** the three arms are registered in the `_SweepSpec` tick table with lazy
  imports; a fresh interpreter importing `taskq`, `taskq.backend`, and
  `taskq.worker._leader_sweeps` leaves **no** `taskq.workflows*` in `sys.modules` (§16.1 holds).
  BUT see Blocker 2 below — the reducer-memo half of the cure does not survive process death.

## 4. The F-fixes

* **F4:** `run_idempotency_scope(flow_name)` → `workflow-run:<name>`; the collision attack green.
* **F5:** the drain binds the unwrapped `payload` + the trace chain (`trace_id or fire_id`);
  verified in `_sweep.py` and `_sql_sweep.py`; the envelope attack green.
* **F6:** my own probes: empty fork → `ValueError`; edge-less join (deps_pending=2, zero
  parents) → `ValueError`; counter/edge mismatch → `ValueError`. All refuse.
* **F8:** my own probe file (`entry=42` against `insert_flow_run`), checked under
  tests/typeprobe's config: **pyright 1.1.414** `reportArgumentType` ERROR ("Literal[42] cannot
  be assigned to parameter entry of type FlowEntry"); **ty 0.0.85** `invalid-argument-type`
  ERROR. Both checkers refuse.
* **F9:** `.github/workflows/ci.yaml` has a `type-probes` job running `tests/typeprobe/_gate.py`
  under the pinned `typeprobe` group; the gate parses both checkers' outputs and fails unless
  every MUST_ERROR marker reds on EACH checker. The gate holds green (5 markers). I verified
  `.measurements/attack/F9-red-type-gate.txt`: the drill (line 30 unflagged on both checkers)
  exits 1 — the gate can fail. Real.

## 5. The gates I ran myself

| Gate | Result |
|---|---|
| ruff check (new code, scoped) | clean |
| ruff format --check (new code) | clean |
| ruff check . (repo) | clean once the stray test artifact (low #3) is removed |
| pyright (all new/changed files + attacks + pins) | **0 errors, 0 warnings** |
| Full pin suite + attacks (53 tests) | **53 passed** |
| Attack files ×3 | 3 × 6 passed |
| Import discipline + CI-workflow pins | 18 passed |
| Perf bands (measured files vs budgets) | fanout-1000 39.9/500 ms; join-fire 14.6/50 ms; row-width 7.37/8 B; partial ratio 0.26 %/1 %; enqueue p50 353 µs; dispatch p50 7.44/50 ms — all green |
| **Estate regression slice** | **14 FAILED** — see Blocker 1 |

## 6. §7b on the new code

No god files (largest new module 511 lines; the big files predate the wave). Concerns are
separated (statements per concern module, arms in `_sweep.py`, the memo in `_reducers.py`, the
registration in `_leader_sweeps.py`). Comment density is high but consistent with the house
style. Findings below.

---

## THE FINDINGS

### BLOCKER 1 — the estate gate is RED: the sweep differential audit (14 failures)

`tests/test_audit_sweep_registry_differential.py` — **14 failed / 10 passed** on HEAD
(e.g. `test_audit_success_tick_event_streams_identical`: *"Right contains one more item:
('metric_duration', 'wf_join_rederive')"*).

The audit's contract: the vendored pre-refactor sweep module vs the working-tree `_leader_sweeps`
must produce EQUAL observable event streams. 8cea6a25 (B3) added the three wf arms to the
`_SweepSpec` table; the audit's streams now differ and the file reds permanently. I verified this
is the FIX WAVE's doing, not pre-existing: at T05 (145e3f0f, pre-fix-wave, source pinned via
PYTHONPATH) the file is **24/24 green**. CI runs the whole suite (`pytest -n 2 -m "not slow and
not load_sensitive"`), so the branch's CI is red.

**Why blocker-class:** the fixer's claim "gates green" is false for the estate slice, and a
permanently-red regression audit on the sweep loop masks exactly the regressions it exists to
catch — the next `_leader_sweeps` change ships blind. This is the "the record must not lie"
class at gate tier.

**Cure sketch:** fold the three arms into the audit's expected streams (or refresh the vendor
module to a wf-arm-aware baseline), or scope the equality to the legacy eight arms with an
explicit named exclusion of the `wf_*` arms plus their own dedicated expectations. Either way:
green AND still load-bearing.

### BLOCKER 2 — B3's reducer-body cure is half-vacuous across processes; the D1 fallback is dead code

`_flow_reducers` is a **process-local** memo (`src/taskq/workflows/_reducers.py`). The finalize
registers into the process that ran the step. But `sweep_join_rederive` heals **schema-wide** —
ANY leader heals ANY flow's join-wait rows; nothing in the SQL scopes healing to the process that
finalized. When the healer is not the finalizer's process (a hard worker kill — the very crash
B3 exists to heal — or any multi-worker fleet where another pod's leader wins the heal),
`resolve_flow_reducer` returns None and the sweep fires the join and dispatches its declared
consumers off an **UN-REDUCED output, silently** — `_sweep.py` even documents it:
"A fired join with no resolvable body delivers its declared consumers; nothing else runs."
That is the convicted B3 dragon (at-least-once body execution degraded to NEVER) surviving the
cross-process form of the same window.

The claimed safety net — "D1's registered definition is the resolver's fallback" — resolves
`resolve_step_body(f"flow:{flow_id}", step_key)`. Nothing anywhere registers a definition under
the name `flow:<uuid>` (grep: the only occurrence of that string is the fallback itself; users
register under their own names and the flow rows carry no workflow-name metadata). The fallback
is unreachable. The attack (in-process monkeypatched `_run_tx2`) only exercises the
in-process window; the shipped guarantee as claimed does not hold fleet-wide.

**Cure sketch:** stamp the workflow's registered definition name on the flow root's metadata at
`insert_flow_run` and resolve the fired join's body from the DEFINITION registry via that name at
fire time (the memo becomes an optimization, never the source of truth) — or make the fire arm
refuse-to-fire a join whose body is unresolvable (leave it firable, stamp
`metadata.blocking_reason='body_unavailable'`, pin the silent-dispatch shape red) instead of
dispatching consumers off an un-reduced join.

### MEDIUM 3 — the reducer memo leaks: `forget_flow_reducers` has zero callers

`_flow_reducers` grows one entry per flow run, per process, forever (`forget_flow_reducers` is
defined, exported, and called by nothing — src or tests). On long-lived workers this is unbounded
growth keyed by flow id. Cure: call it at the flow's terminal reap/finalize (the phantom reaper's
pass is a natural hook), and pin the memo's bound.

### LOW 4 — pin 2's "plan-shape record" is a one-line artifact

`test_pin_2` records the fenced claim's EXPLAIN via `fetchval`, which returns only the FIRST plan
row: the persisted "plan" is the single top-level `Update on jobs j (cost=…)` line and proves
nothing about plan shape. I generated the full plan myself (§3: index-served, hashed-once
subplan) so the SHIP claim stands, but the recorded evidence does not carry it. Cure: fetch all
EXPLAIN rows (`fetch`) before recording.

### LOW 5 — the import-law pin litters a scratch .py into the repo tree

`test_wf_schema_migration.py` writes `.measurements/fixture_mutated_init.py` and never removes
it — an untracked scratch module that turns a repo-wide `ruff check .` red (26 errors, 1 file
unformatted). Cure: write to `tmp_path`, or delete in teardown.

### NOTE 6 — engine-12 / engine-16 red drills are the weak class

See §2. The mutation targets are shipped module symbols, but the red invokes the patched symbol
directly rather than driving the shipped dispatch/finalize entry through it. The green sides
cover the shipped paths; tighten when those files are next touched.

---

## WHAT STANDS

The B1/B2/F4/F5/F6/F8/F9 cures are real, shipped-path, and adversarially confirmed (revert
drills red, my own probes red on both checkers, my own EXPLAIN and band runs green). The pin
suite is materially stronger than the vacuous eight. The refusal is NOT about those — it is
about the two blockers: a red estate gate on the branch, and B3's guarantee not surviving the
process boundary it claims to survive.

**Certification: REFUSED.** Close Blockers 1 and 2 (with the drills re-run: the differential
audit green and the cross-process body-resolution shape pinned red-on-mutation), then
recertify. Mediums/lows may ride phase 2 except as they block the re-drill.
