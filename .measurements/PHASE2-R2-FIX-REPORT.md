# PHASE-2 FIX REPORT (round 2) — the clean full-suite verify's 14 cured

Fixer: the phase-2 fixer subagent (round 2), on feat/taskqflow @
b4c0013d (the settled tree) → b397fcb4 (the code cures) — the
certifying full-suite run on b397fcb4: **`.measurements/
phase2-R2-FINAL-verify-b397fcb4-180439.txt`** (grep THE FILE).

The red it answers: `.measurements/phase2-FINAL-verify-b4c0013d-233500.txt`
— 14 failed / 12833 passed on the clean verify. READ per failure, not
guessed; the real tracebacks moved three of the brief's guesses:

---

## 1. test_sql_templates_smoke_pg (2) — the R2-1 class AGAIN — REAL

- THE TRACEBACK (not the brief's list): the unregistered constant is
  `taskq.backend._sweeps:_SWEEP_RESULT_TTL_GUARD_SQL` — the T18
  expiry-eats-children NOT-EXISTS fragment round 1's M1 splice added
  (`_SWEEP_RESULT_TTL_BASE_SQL.replace(LIMIT, guard + LIMIT)`); it
  carries SQL keywords, is not a self-contained statement, and had no
  registry entry. Both tests red on the same assert.
- THE CURE (517d1d15): registered in `_COVERED_BY`, marker-None (the
  containment needle is the fragment body itself, inside the composed
  product `_SWEEP_RESULT_TTL_SQL`'s prepare). Plus the standing
  BUILD-PROTOCOL line, in the commit: every new statement joins the
  guard AT THE COMMIT THAT ADDS IT.
- PROVEN BOTH WAYS: red when unregistered — a probe fragment appended
  to _sweeps.py reds `test_the_guard_has_no_silent_gaps` naming it
  (`.measurements/r2-guard-teeth-red-174629.txt`); green when
  registered (`.measurements/r2-cure-smoke-guard-green-174623.txt`,
  3/3).

## 2. test_prometheus_metrics (3) + test_rt_worker_rule_files (2) — the estate's counts/citation follow the SANCTIONED addition

- THE TRACEBACKS: rules.yaml ships 24 rules; the pins assert 23; the
  docs cite 23; and the new alert's series
  `taskq_wf_progress_nodes_total` was not in `_NAME_MAP` (the
  registered-instrument pin's behavioral oracle: not in the map = no
  scrape serves it). T08's ticket REQUIRED the alert in BOTH rule files
  + the runbook row (2a836bcb) and round 1's H3a wired the gauge's real
  emission path — the addition is sanctioned; what never happened is
  the estate's pins/citations/map following it.
- THE CURE (af4d1994, the sanctioned path, ticket cited in the commit):
  `_NAME_MAP` gains the gauge's row (observable gauge, unit "1" — the
  OTel name already ends in "total" but a gauge renders the bare name)
  + `_populate_all_instruments` records an observation;
  `_EXPECTED_ALERT_NAMES` gains TaskQWorkflowBlockedStuck; the two
  count pins 23 → 24; observability.md + ops.md §8 cite 24 rules with
  the new family named in both enumerations. test_rt_worker_rule_files
  needed NO edit (its two tests derive from the map + the docs).
- THE PROMTOOL GATE STAYS GREEN: the alert still fires —
  `.measurements/r2-cure-promtool-174352.txt` (44 passed).
- Green: `.measurements/r2-cure-prometheus-174346.txt` (39 passed).

## 3. test_migration_lock_scope_dead_index — the pinned end-state updated through the sanctioned mechanism

- THE TRACEBACK: `unexpected: {'jobs_wf_flow_nodes_idx'}` — the pin's
  frozen inventory predates 01.00.25_02 (61f40297), the index estate's
  own sanctioned change.
- THE CURE (5ba70f49): `_PINNED_JOBS_INDEXES` gains the index, the
  pin's update IS the commit that changed the estate, with the measured
  justification on the record: the grouped rollup 21.2 ms → 0.12 ms at
  the 220k-row fleet (fix2/index-fleet-bands.json,
  index-before/after-text-index.txt).
- THE DISCIPLINE STAYS PROVEN: the file's other ten tests (the dead
  probe index absent by name, the marker predicates, the phase gate,
  both paths' full-schema equivalence) green —
  `.measurements/r2-cure-migration-pin-green-174641.txt` (11 passed).

## 4. test_timescaledb_hypertables::test_mid_population_conversion — a PRE-EXISTING wall-clock flake, not the wf tables

- THE TRACEBACK, READ: the plan is CORRECT — two bitmap-scanned day
  chunks beside a 25-row `Seq Scan` on the newest chunk. The seed's
  now()-anchored 2000-minute window makes the newest chunk the current
  day's SLIVER (minutes since UTC midnight worth of rows: 25 just past
  midnight, 36k late in the day); the planner is RIGHT to seq-scan a
  25-row chunk (bitmap ~4.0 vs seq ~1.3). The verify ran in the sliver
  hours. Measured ground truth on a live timescale container: 1-day
  midnight-aligned chunks, 12800/36000/1200 rows — the 1200-row sliver
  is the failing shape. The module and the test are untouched by phase
  2 (last touched pre-branch): the clean full run merely landed the
  flake.
- THE CURE (167b7bda): the seed anchors at
  `date_trunc('day', now()) - 1 minute` — every populated chunk a full
  day at every wall clock. The 1-minute margin is load-bearing, proven
  en route: the boundary-on anchor (residue 0 lands exactly ON the
  boundary) re-created the sliver deterministically and still red;
  boundary−1 minute greens.
- Green ×3: `.measurements/r2-cure-timescale-green-x3-175016.txt`.

## 5. test_dev (3) — ENVIRONMENT-CLASS; test_memory_jobs_fixture — REAL (round 1's own leak); test_breaking_change_markers — REAL (the law bites round 1's commit)

- **test_dev (3 RuntimeError)**: `Could not find the 'taskq' executable
  on PATH` — the venv's console script exists (`.venv/bin/taskq`); the
  verify ran without the venv's bin on PATH. The RuntimeError is the
  designed loud message; no code change. Cured by running the suite
  with `.venv/bin` on PATH: `.measurements/r2-cure-dev-path-green-
  175132.txt` (27 passed).
- **test_memory_jobs_fixture::test_testing_no_transitive_asyncpg —
  REAL**: `import taskq.testing` loads asyncpg. The leak is round 1's
  own: M1 (7159b72a) put `import asyncpg` at `_sweeps.py` MODULE level
  for the tolerance's except arm — and the module rides
  `taskq.actor`'s import path through `ratelimit.registry`, so every
  testing import pulled the PG driver. THE CURE (63218d39): the house
  lazy-import pattern — the name imports inside the one function that
  uses it. Green: `.measurements/r2-cure-asyncpg-seam-green-175119.txt`
  (15 passed, incl. M1's tolerance pins).
- **test_breaking_change_markers — REAL**: T08's commit hand-wrote a
  `## [Unreleased — TaskQflow phase 2]` section into CHANGELOG.md —
  invisible to release-please. THE CURE (1af61166): the section
  removed; its content already lives in the T06/T07/T08 commit
  messages (nothing breaking to relocate to upgrading.md). Green:
  `.measurements/r2-cure-changelog-green-175144.txt` (4 passed).

---

## THE GATES (all local, all captured)

- ruff check + format (src + tests): ALL CHECKS PASSED —
  `.measurements/r2-gate-lint2-175200.txt` (1353 files formatted).
- pyright: 0 errors on the whole src —
  `.measurements/r2-gate-pyright-full-175205.txt` (and the touched
  files: r2-gate-pyright-175202.txt).
- The type-probe gate: every MUST_ERROR marker (5) reds on pyright
  1.1.414 AND ty 0.0.85 — `.measurements/r2-gate-typeprobe-175257.txt`.
- The changed-pin files ×3: 134 passed + 1 tier-skip, THREE times —
  `.measurements/r2-pins-x3-run{1,2,3}-*.txt` (index:
  r2-pins-x3-175314.txt).
- The attack corpus + the estate slice (the wf pins, the T06/T07/T08
  families, the migration/index-audit/sweep-differential gates, the
  promtool gate): 701 passed, 1 pre-existing uvloop skip —
  `.measurements/r2-estate-attack-battery-175639.txt`.
- THE VERDICT (the certifying full-suite re-run on the settled tree
  b397fcb4): `.measurements/phase2-R2-FINAL-verify-b397fcb4-180439.txt`
  — **12847 passed, 8 skipped, 0 failed** in 3623.86s (EXIT=0): the
  same 12847-test population the b4c0013d capture reds 14 of is green
  to the last test on this tree.

## ENVIRONMENT (the run's own)

- venv synced (`uv sync --all-extras --all-groups`; the sync re-pins
  taskq-py editable + the typeprobe group).
- The agent's own PG: docker `taskqflow-p2fix-pg` on :5695 (the
  diagnostic replica ran on the agent's own timescale container
  `taskqflow-p2fix-ts` :5696); the suite's shared pair boots via its
  own testcontainers as always.
- The capture law held: every run above is a timestamped file, grepped
  in place; no sleeping.
