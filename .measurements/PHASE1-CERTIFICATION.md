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

---

# PHASE-1 RECERTIFICATION — ROUND 2 (feat/taskqflow @ d83d5e1c; the cure wave 56bd8f48/e9ae3793/74bb2512)

Certifier: the PHASE-1 RECERTIFIER, round 2 (independent, adversarial; verified against the code
and the database, plus THREE of my own live-revert/defeat drills).
Date: 2026-10-07
Verdict: **CERTIFIED** — both blockers closed with genuine teeth; phase 2 may build. Findings
below ride WITH the certification (one scheduling demand, one medium, two notes). I fixed nothing.

## Blocker 1 (the audit differential) — CLOSED, verified adversarially

* `tests/test_audit_sweep_registry_differential.py` — **26/26 green** on HEAD (24 legacy + 2 new
  pins). Diff vs 244abfb5: purely additive except the double's `fetchrow` widening
  (`object | None` → a zero-summary dict) — invisible to the legacy scenarios (the vendored
  module never calls `fetchrow`); the 24 legacy tests are UNMODIFIED and pass.
* The mechanism is as claimed: `workflow_sweeps_capable` is the arms' OWN admission seam — a
  `ClassVar` on `PostgresBackend` (src/taskq/backend/postgres.py), probed by the `_SweepSpec`
  tick table's plain `hasattr` gate (src/taskq/worker/_leader_sweeps.py:872); the three wf specs
  gate on `("workflow_sweeps_capable",)` and on NOTHING else — no borrowed marker anywhere.
* The two new pins have TEETH: (1) the off-pin runs the whole tick on a legacy-surface double and
  asserts ZERO `wf_*` events; (2) the composition pin declares the marker on a second double and
  asserts stripping the wf events yields the legacy stream exactly (conservative: a wf-prefixed
  LEGACY event would break its own equality — the pin cannot lie in the wave's favor).
* MY DEFEAT DRILL A: I re-armed a rogue admission by flipping the join-rederive arm's
  `gated_on` back to the borrowed `("sweep_leaked_reservation_slots",)` in the shipped
  `_leader_sweeps.py` → `test_audit_wf_arms_stay_off_without_the_capability_marker` **RED**
  (line 659). The audit convicts the borrowed-marker registration. Tree restored, verified clean.
* §16.1 import law: MY OWN fresh-interpreter probe — `import taskq`, `taskq.backend`,
  `taskq.worker._leader_sweeps` → **zero** `taskq.workflows*` in `sys.modules`. Held.

## Blocker 2 (the process-local memo) — CLOSED, verified adversarially

* Read the whole resolution chain: `SWEEP_FIRE_SQL` now JOINs the flow root and returns
  `root.metadata->>'workflow' AS workflow_name` (src/taskq/workflows/_sql_sweep.py); the fire arm
  resolves the body via `resolve_flow_reducer(..., workflow_name=...)` →
  `resolve_step_body` against the real default registry (`KeyError` on an unknown name);
  the process-local memo is strictly FALL-THROUGH (the definition wins when the stamped name
  resolves — it cannot shadow); the dead `flow:{flow_id}` fallback is **DELETED** (grep: zero
  occurrences in src/). The stamp is written at `insert_flow_run`
  (`metadata.workflow = entry.name`, src/taskq/workflows/ledger.py).
* The cross-process attack (`test_attack_sweep_fire_skips_the_reducer_body`) is the real thing:
  the finalize dies at the `_run_tx2` boundary, the memo is dropped, and the heal runs in a FRESH
  interpreter subprocess (`sys.executable`, own registration, no shared state) — **green**.
* MY DEFEAT DRILL B: I stripped the registry leg in the shipped resolver (`if workflow_name:` →
  `if False and workflow_name:` — memo-only) and re-ran the attack → **RED** with the exact
  convicted shape: the fresh process fired the join with the body run ZERO times
  (`verdict={'fired': 1, 'body_calls': 0}`). Recorded at
  `.measurements/attack/ROUND2-DRILL-memo-only.txt`; tree restored, verified clean.
* MY DEFEAT DRILL C (the unregistered name): a flow root stamped with a workflow name NO process
  registers, healed with an empty memo → `resolve_flow_reducer` returns `None`, the join FIRES,
  the declared consumers are delivered, and NOTHING loud happens (no exception, no warn, no
  `blocking_reason`). See MEDIUM R2-2 below.

## The round-1 mediums/lows — all real now

* MEDIUM 3 (the memo leak): `forget_flow_reducers` has its caller — the phantom reaper's pass
  (src/taskq/workflows/_sweep.py:233). Pin 22 is real: it warms the cache, drives the SHIPPED
  `reap_phantom_ledger` against the live DB, and convicts a reaper that stops forgetting.
* LOW 4: pin 2 records the FULL EXPLAIN — every row fetched (`fetch`), the whole plan persisted
  to the red sink (the subplan rows included).
* LOW 5: the import-law drill's scratch module rides `tmp_path`; no stray file in the tree.

## The docs

`docs/guides/workflows.md` + the upgrading note carry a real cross-process semantics section:
the stamp, the registry-as-truth/memo-as-cache discipline, the reaper's bound, the raising-body
rollback + re-fire — AND an honest degradation disclosure (the unregistered-definition case,
named in as many words). Real content, matches the code.

## MY GATES (×1, all mine)

| Gate | Result |
|---|---|
| Differential audit | 26/26 |
| Pins + attacks + type probes (wf suite: sweep/finalize/fork/ledger/engine/migration pins, 4 attack files, negative types) | 50 passed |
| wf perf bands | 4 passed |
| Import discipline + CI-workflow/suite-hygiene pins | 49 passed |
| ruff check + ruff format --check (the wave's files) | clean / 18 formatted |
| pyright (all 12 changed files) | **0 errors, 0 warnings** |
| Bands re-measured by my own suite run | fanout-1000 43.3/500 ms; join-fire 11.2/50 ms; dispatch p50 7.33/50 ms (red drill 2138 ms); enqueue p50 385 µs/25 ms; row-width 7.37/8 B; partial ratio 0.26 %/1 % — all green |
| **FULL estate** (`pytest -n 2 -m "not slow and not load_sensitive"`, 30 min) | **4 failed / 12647 passed / 8 skipped** — see FINDING R2-1 |

## NO REGRESSIONS

`git diff 244abfb5..HEAD` on the B1/B2/F-wave shipped files (`_sql_ledger.py`, `_dispatch_sql.py`,
the migrations, `workflows/_sql.py`, both attack files, the typeprobe tree, `ci.yaml`): **EMPTY**.
The wave's src diffs are confined to the cure files (postgres.py, _leader_sweeps.py, _reducers.py,
_sql_sweep.py, _sweep.py, engine.py, ledger.py) and are surgical (read each hunk).

## THE FINDINGS (ride with the certification)

* **HIGH-finding R2-1 (estate debt, PRE-EXISTING — not this wave's, verified):** the full estate
  suite carries 4 reds: `test_no_stdlib_json` (stdlib `import json` in
  `workflows/_sweep.py:34`, `_types.py:12`, `ledger.py:47`), `test_sweepaudit_bounded_writes`
  (the workflows `_sql` write statements unregistered in the bounded-writes registry),
  `test_sql_templates_smoke_pg` ×2 (`_WF_DISPATCH_FENCE_TEMPLATE` — B2's own fence fragment —
  unregistered in the SQL guard). I built a synced baseline worktree at 244abfb5: **the same 4
  fail there, with IDENTICAL violation lists** (diffed set-for-set), and one adjacent test
  (`test_exemption_registry_has_no_stale_entries`) went red→green — the branch is strictly
  BETTER than the round-1 certified HEAD. `src/taskq/workflows/` does not exist on main, so
  these guards have been red on this branch since T04/B2; the round-1 "estate slice" evidently
  never ran the full suite (its 14 were the audit file's). NOT a regression of this wave —
  certification stands — but the CI selection is red on both sides of it, and two of the four
  are guard blindness over the workflows SQL itself (the bounded-writes audit has never vetted
  the workflow statements; the smoke guard has never parse/plan-validated the fence template).
  **SCHEDULED DEMAND:** phase 2's opening must land the estate cure — route the three json
  imports through `taskq._json`, register the workflows SQL statements in the bounded-writes
  registry and `_COVERED_BY` (with rendered-product validation) — before phase 2's own gates
  stack on top of a red suite.
* **MEDIUM R2-2 (the loudness asymmetry, my drill C):** a flow root stamped with a workflow name
  the healer's process has not registered fires its join and dispatches its consumers off an
  UN-REDUCED join silently — no exception, no warn, no `blocking_reason` stamp (verified live).
  The docs disclose it ("keep the definitions imported in every worker") and the round-1 refusal's
  own cure sketch sanctioned the resolve-from-definition cure, so this does not re-block; but the
  asymmetry doctrine says loud. Recommended cure (phase 2): on a stamped name that fails
  registry resolution with an EMPTY memo, stamp `metadata.blocking_reason='body_unavailable'`
  and/or emit a warn event, keeping the legacy silent path for genuinely anonymous (unstamped)
  roots only.
* **NOTE R2-3:** the heal path runs the definition body as `definition_body(None)` — a body that
  dereferences ctx raises, which rolls the sweep tx back LOUDLY (the doctrine holds), but the
  re-fire loop is unbounded (a poison pass re-fires every tick). Acceptable under the stated
  at-least-once semantics; tighten when the sweep arms are next touched.
* **NOTE R2-4:** the tracked `.measurements/*.json` bands are re-measured by every suite run
  (they show modified after a run) — expected; my run's values are all within budget.

**Certification: CERTIFIED (round 2).** Both blockers closed with shipped-path mechanisms, each
independently convicted by my own red drill; the import law holds; the pins are load-bearing;
no regressions against the round-1 certified baseline. Phase 2 may build, under the scheduled
estate demand R2-1 and the loudness recommendation R2-2.
