# The test-estate review: tier map, structural holes, and the hardening plan

A holistic pass over the estate after the ~38-PR campaign: what each tier
actually runs, where a bug can still hide, and what buys the most
protection next. Everything below was run or read on this commit
(`6eb0d1ff` plus this branch's pins); every claim carries its evidence.

## 1. The tier map: what actually runs, where

| tier | command (exact) | where it runs | count | can never run in |
|---|---|---|---|---|
| fast unit + integration (all extras) | `pytest -n 4 -m "not slow and not load_sensitive"` | CI `test` matrix, 3.12/3.13/3.14, timeout 30m | 10290 passed, 7 skipped (this run) | nothing; it is the base |
| the same with coverage | `pytest -n 2 -m "not slow and not load_sensitive" --cov=taskq --cov-fail-under=90` | CI `coverage` lane, 45m | same set; measured 95% | nothing |
| slow / chaos / fleet (wall-clock) | `pytest -n 2 -m "slow and not load_sensitive"` | CI `slow-suite` lane only | 49 tests | every fast lane; a bug whose repro is slow-marked cannot red in the matrix |
| load-sensitive timing guards | `pytest -m "load_sensitive"` serial | CI `load-sensitive` lane only | 57 tests | every other lane deselects it |
| extras spot-checks | `pytest -n 2 -m <extra>` per extra | CI `test-extras` (8 legs) | aad 27, saml 52, oidc 29, otel 80, vault 17, aws 16, redis 130, fastapi 690 | a leg with a missing extra skips by importorskip - 55 fastapi + 32 jinja2 guards are the safety net |
| cross-extra pairs | `pytest -n 2 -m "<pair>"` | CI `test-cross` (5 legs) | small | only the 5 declared pairs run; any other combination (aad+saml, vault+aws, ...) has no leg |
| TimescaleDB | `pytest tests/test_timescaledb_hypertables.py` serial + a skip-free parser | CI `test-timescale` lane | 11+ (floor 11 enforced) | everything else; only this one module |
| container e2e (wheel workers) | `pytest --e2e -m e2e tests/e2e --timeout=900 -q` serial | CI `e2e` lane | 79 test fns in `tests/e2e/` | no opt-in flag, no run: the `--e2e` root flag deselects by default (104 deselected in a bare collect) |
| system-e2e (subprocess workers) | `pytest --system-e2e -m system tests/system_e2e --timeout=900 -n 2 -q` | CI `system-e2e` lane | 7 test fns (each a minutes-long lifecycle) | same opt-in rule |
| OTLP round-trip | `make test-otel` (`pytest --otel-validation -m otel_validation tests/otel_validation`) | NOTHING in CI. No workflow references `otel_validation` | 1 test fn | CI entirely. A regression in the real OTLP export path cannot red in CI |
| examples smoke/wiring | marker `examples`, "excluded from default CI run" | NOTHING in CI | 28 test fns | CI entirely |
| perf benchmarks | `pytest tests/perf -m "slow and load_sensitive"` on demand | NOTHING in CI (by design, documented in the module) | 1 | CI by design; the plan oracles own the deterministic half |

Timeouts: every lane carries a job cap (15/30/45m) and pytest-timeout 300s
default (900s in e2e/system-e2e); the slow tier's per-trial bound is 2700s
by design (see the `test` job's comment in `ci.yaml`).

## 2. Structural holes, each with evidence

### 2.1 The seam-audit matrix was never merged (found first, fixed here)

`docs/design/seam-audit-matrix.md` and its five mutation-verified pins
(`tests/test_seam_audit_pins.py`) existed only on the unmerged branch
`fix/seam-audit-matrix` (commit `17221a63`). `git branch --contains
17221a63` lists no main ancestor; the file was absent from the `main`
tree. The estate the owner described as having a seam-audit matrix did
not: the matrix's four merged owner-fixes (#470/#467/#471/#472) landed,
but the audit document and the five ADDED pins stayed behind. One pin
(`test_the_abandon_drain_renews_detector2_liveness_per_entry`) had also
drifted: #471 changed the abandon deque's seed shape to
`(job_id, entry)`, so the pin as written threw
`TypeError: cannot unpack non-iterable UUID object` on current main.
This branch cherry-picks the matrix, repairs the pin's seed to the
`(job_id, entry)` shape, and re-proves the mutation (section 5).

### 2.2 The parity registry self-reports 22 unpinned backend seams

`tests/test_backend_semantic_parity_registry.py` pins
`dispatch_batch` and `cancel_where` for in-memory-vs-PG semantic parity
and lists 22 more seams in `_SEMANTIC_SEAMS_UNPINNED` where "the two
backends could answer differently here and nothing would notice"
(`list_jobs`, the three `enqueue_batch*` variants, `reclaim_expired_locks`,
`deadline_sweep`, `retry_job`, `abort_batch`, the three `count_*`
aggregates, `poll_cancel_flags`, `poll_reclaim_events`, the
reservation/heartbeat selection seams, the read/list family). Three of
those 22 already have both-backend parity files written since the
registry was compiled (`retry_job` ->
`tests/test_retry_job_source_states_parity.py`,
`scheduled_to_pending` -> `tests/test_sweep_backend_parity.py`,
`reclaim_expired_locks` -> `tests/test_rt_sweeps_parity.py`); their
registry rows were stale and are promoted to `_SEMANTIC_SEAMS` in this
branch. The remaining 19 are the honest gap: each is a selection or
ordering predicate where the twin could drift and the suite stays green.

Twin exposure in bulk: 182 test files touch `InMemoryBackend`; 100 of
them never enter the integration tier (no `integration` mark anywhere in
the file). Most are legitimately unit-tier, but the ratio is the
measure of how much of the suite's knowledge about behavior lives only
in the twin.

### 2.3 The coverage gate's missing lines are concentrated in error arms

Measured on this commit (`coverage report`, same filter as the CI gate):
TOTAL 95% (24268 stmts, 840 missed, 6864 branch, 510 partial) against
the 90% floor. Every src file below 90%:

| file | cov | what the missing lines are |
|---|---|---|
| `__main__.py` | 60% | the `python -m taskq` stub (1 line) |
| `obs/_exporter.py` | 62% | lines 271-382: the whole exporter-wiring block - preconfigured-provider detection, scrape-port-stays-unbound warning, the OTLP/prometheus arm selection. These are the observability failure paths an operator sees first in a broken install |
| `testing/asyncpg_chaos.py` | 81% | the chaos injector's own error branches (test infra) |
| `contrib/prometheus/_metrics.py` | 82% | lines 141-150, 183-194: provider-already-set and otel-disabled scrape arms |
| `timescale.py` | 83% | lines 274-288: the `InsufficientPrivilegeError` -> `TimescaleDBUnavailableError` translation, the exact operator-facing failure when hypertables are requested on a role that cannot create the extension |
| `web/admin/auth/saml.py` | 87% | 22 lines, all error arms: replayed-assertion expiry delete (177-180), no-AuthnRequest-ID, not-authenticated, response-does-not-answer-request, no-assertion-ID, missing-NameID, the group/email attr type alternates. The SAML tier runs (52 tests, own CI leg) but almost none of its FAILURE branches are asserted |
| `_di/solver.py`, `client/_jobs.py` | 89% | solver: the unsatisfiable-scope arm; `_jobs`: 1371-1400 batch-summary assembly and scattered error arms |
| `_lock_budget.py` | 88% | one line (the no-settings default arm) |

The pattern is uniform: the estate's happy paths and conservation
invariants are pinned hard; the un-executed residue is dominated by
error/typed-exception arms, exactly the lines that produce operator
incidents. The 90% gate is met by aggregate weight, not by these files.

### 2.4 The two weak EXISTS rows the matrix itself flagged

The seam-audit matrix marks two rows "EXISTS (weak)". One is now closed
by this branch (below). The other stands: the cancel ladder's
`cancel_observed_at` monotonic anchor has no pin that constructs a
wall-clock jump against the ladder - the field contract is documented
and the shutdown orchestrator pins the None arm, but nothing fails if
someone swaps the anchor to `time.time()`.

### 2.5 What the lanes skip silently

The 434 lesson held everywhere it was checked: no lane hides a silent
mass-skip (the timescale lane parses its own summary; the extras lanes
run under explicit selection where importorskip is dead code; a bare
collect deselects exactly 104 and the e2e/system lanes own them). The
residue, from this run's skip summary: 7 skips, all explained
(`PG backend requires integration mark`, `e2e dependency group not
installed`, `connection-loss test requires PostgresBackend`, and
statement-count boundary guards). The two CI-invisible families are
section 1's last rows: `otel_validation` (1 test, no CI lane) and
`examples` (28 test fns, no CI lane). A regression to the real OTLP
export round-trip or the example wiring cannot red anywhere automated.

### 2.6 Mock-shape assertions are rare; the residue is fine

20 `assert_called_*` sites across 9 files in a 10401-test suite, and
each guards a boundary where the call IS the contract (a signal to a
subprocess, a Vault credential call, a pool acquire timeout). No
systemic mock-shape disease. `call_args` appears 116 times, mostly to
assert on captured payloads (observable outcome), not call shape.

### 2.7 Randomization luck is confined and bounded

One unseeded `random.random()` use in `tests/test_terminal_writes_chaos.py:528-529`
(2ms jitter windows inside a chaos soak; the assertions there are
conservation-shaped, so the jitter biases detection power, not
correctness). Hypothesis is used in 40 files with default seeding. No
suite-order dependency survived #434's hermeticity gate
(`tests/test_ci_workflow.py`, `test_suite_hygiene.py`).

### 2.8 A live load-lottery the sweep itself hit

`tests/test_dispatch_pg.py::test_dispatch_duration_is_recorded_when_the_dispatch_query_fails`
failed ONCE in this review's own full `-n 4` run and passed 5/5 standalone
plus in the same day's full coverage run. The mechanism is documented in
its own file: the fixture arms a REAL 1ms server statement budget
(`statement_timeout_dispatcher_pool`), and the sibling class
`_FailingDispatcherConn` exists in the same file because, per its own
docstring, "a real 1ms budget is a race - when the server's cancel lands
after the claim statement committed, the round half-succeeds". Under
co-tenancy the race surfaced as `InternalClientError: cannot switch to
state 12; another operation (2) is in progress` out of asyncpg's
protocol, an exception type outside the `pytest.raises(_asyncpg.PostgresError)`
contract. This is the exact class the ledger calls a load lottery: a
pin whose red is runner timing, not the defect it guards. The fix shape
is in the file already: drive the deterministic failure injection for
the every-round-fails invariant and keep the real-timeout arm behind
`load_sensitive`. Not fixed on this branch - changing what that test
exercises deserves its own red/green run, and flagging it honestly is
the review's job.

## 3. Where a bug can hide in the current lanes

1. Any behavior whose only execution is in `slow` (49 tests), e2e (79),
   system-e2e (7), `load_sensitive` (57), or the extras tiers - the fast
   matrix and coverage lanes deselect them, so a regression reds in
   exactly one lane. That is by design and the lanes are required, but
   it means a lane-green board is not estate-green: the rerun economics
   the lanes exist for are the same fact a sneaky regression exploits.
2. The OTLP round-trip and examples: no lane at all.
3. The 19 unpinned parity seams: the twin is what most PRs develop
   against; a twin-side selection bug there ships green.
4. The error arms of `obs/_exporter.py`, `timescale.py`, and
   `web/admin/auth/saml.py`: the gate holds at 95% while those specific
   lines never execute in any lane.

## 4. The hardening plan (prioritized by bug-classes caught per unit of work)

1. Land the seam-audit matrix and its pins on main (DONE this branch;
   cost: one cherry-pick plus a one-line seed repair; benefit: five
   mutation-verified seam pins move from a dead branch to the gate, and
   the matrix becomes the review surface for the next seam).
2. Promote stale parity-registry rows (DONE this branch; cost: three
   dict lines; benefit: the registry stops under-reporting protection
   that exists, so the next reviewer reads a true gap list).
3. Pin the phase-transition counter's pair set exhaustively (DONE this
   branch; cost: one unit test; benefit: closes matrix row 9's weak
   EXISTS - cardinality growth or a new ladder arm fails on arrival).
4. Write the missing error-arm pins for the three sub-90% files with
   operator-facing failure paths (`obs/_exporter.py` 271-382,
   `timescale.py` 274-288, `web/admin/auth/saml.py` the 22 lines).
   Cost: one test session per file, existing fixtures suffice (the saml
   extra leg already installs python3-saml). Benefit: converts the
   coverage gate from aggregate-weight to arm-level on exactly the
   lines that page people.
5. Add a CI lane (or a leg in the coverage job) for
   `tests/otel_validation` behind the same opt-in pattern as e2e.
   Cost: one lane, ~minutes, a collector container. Benefit: the real
   OTLP export path stops being CI-invisible.
6. Drive the 19 unpinned parity seams down: start with the ones PRs
   actually touch (`enqueue_batch` dedup decisions across a batch,
   `poll_cancel_flags` selection, `deadline_sweep` predicate+LIMIT).
   Cost: one parity file each following the
   `test_in_memory_dispatch_parity.py` shape, integration-marked.
   Benefit: the twin stops being able to answer differently than PG at
   the seams development actually exercises.
7. Pin the ladder's monotonic clock anchor (matrix row 10's remaining
   weak EXISTS). Cost: a fake-clock injection against the cancel
   ladder's elapsed computation. Benefit: the last documented
   clock-domain hole.
8. Decide and document the `examples` family's fate (a lane, or delete
   the marker and its "excluded" note). Cost: minutes. Benefit: 28
   test fns stop being dead weight that green boards silently skip.
9. Deflake `test_dispatch_duration_is_recorded_when_the_dispatch_query_fails`
   (section 2.8): deterministic injection for the every-round-fails
   invariant, the real 1ms-timeout arm moved to `load_sensitive`.
   Cost: one session with its own red/green. Benefit: removes the one
   live load lottery the review observed reding.

## 5. What this branch implemented

1. The seam-audit matrix + `tests/test_seam_audit_pins.py`, cherry-picked
   from `17221a63`; the liveness pin's seed repaired to the
   `(job_id, entry)` deque shape #471 introduced; the matrix's tallies
   and MISSING rows updated to the post-merge state (EXISTS 34 /
   MISSING 4 / ADDED 5; #458's fix noted as still unmerged with its
   branch tip `7c75835a` and registration `76207569`).
2. `tests/test_cancel_obs.py::test_phase_transition_pair_set_is_exactly_the_documented_four`:
   drives all four documented pairs and asserts the counter's entire
   exported set (exactly 4 timeseries, the exact pair tuples, one
   increment each). Mutation-sharp: adding a fifth emitted pair fails
   exactly this pin (1 failed, 5 passed under the mutation).
3. The parity registry promotions (section 2.2): `retry_job`,
   `scheduled_to_pending`, `reclaim_expired_locks` moved from
   `_SEMANTIC_SEAMS_UNPINNED` to `_SEMANTIC_SEAMS` with their pin
   files; the unpinned list drops 22 -> 19.
4. This document.

Proof run on this branch: `tests/test_seam_audit_pins.py` 5 passed;
`tests/test_cancel_obs.py` 6 passed; the mutation checks (section 2.1's
`TypeError` repro and the fifth-pair mutation) each fail exactly the pin
they target, all sibling pins stay green. Full fast+integration tier with
coverage on this tree: 10290 passed, 7 skipped, 95% (section 2.3); the
same tier re-ran plain with all six new pins collected: 10296 passed,
7 skipped, 0 failed (489s, `-n 4`).
