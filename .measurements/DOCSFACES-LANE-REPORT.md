# THE DOCS-FACES CURES LANE — THE REPORT

*Branch `feat/taskqflow-docsfaces-cures` (base `0e2f5cc09`, the
feat/taskqflow tip read off wt-land). The lane's PG (:5816,
taskq-docsfaces-pg) is DESTROYED at close. Worktree
/tmp/opencode/wt-docsfaces. This report is the lane's own record; the
parent session's verdict is its own.*

## Per-item disposition

| Item | Disposition |
|---|---|
| F1 the `taskq[flows]` extra | **CONFIRMED + CURED** — `uv sync --extra flows` failed live (`Extra `flows` is not defined...`); `flows = []` moved under `[project.optional-dependencies]` (pyproject.toml:73); the sync then resolved. THE PIN: `tests/test_extras_documented.py::test_flows_extra_is_an_optional_dependency_not_a_project_key` (the table placement, both directions) — and the existing extras-documentation pin forced the extra's rows into README/installation.md/quick-start.md. |
| F2 the doors divide | **DOCS CURE + CODE-CURE FINDING** — the buckets bind at the FLEET's dispatch claim (`src/taskq/worker/_consumer.py:966` `_acquire_rate_limits_or_route_denial`); the in-process runner loop (`api/_runner_loop.py`) has no rate-limit wiring. The lift is NOT small (registry construction + the denial-snooze machinery live on the worker side; importing the worker's helper into `taskq.workflows` would cross the package's own import law). THE DOCS: the doors stated at four faces (rate-limiting.md the authoring face + the "WHICH DOOR" block; workflows.md the orchestration-only drive + the drive bullet in api-reference). |
| F3 `runner.wsql` / the display fence | **PARTIALLY REFUTED + CURED** — `FlowRunner.wsql` EXISTS at this head (`api/_runner.py:163`, `WorkflowSql.build(schema)`); the progress.md:454 fence EXECUTES GREEN (docs tier). The REAL defect: workflows.md:1686 named `taskq.workflows.run_display` — the root namespace does not export it. Fixed to the documented submodule path + `runner.wsql` named. EXECUTION RECEIPT: `runs/docsfaces-docs-tier-*.txt` (96 fences incl. progress.md:454). |
| F5 the collect crash-class gap | **DOCS CURE + CODE-CURE FINDING** — the crash-class child (crashed terminal via the lease reclaim) bypasses `engine.py`'s `_resolve_failed_parent` (routes `outcome == "failed"` ONLY): no FailureInfo append, the join fires with a `null` slot. The engine cure is NOT small (the sweep-fire arm + the reclaim write are SQL-side machinery; routing the crash class through the fan-in touches the fail_closed cascade's semantics too). THE DOCS: the collect contract's crash behavior stated exactly (workflows.md). |
| F6 the max_attempts=1 overshoot | **REPORTED — the holdmap lane's item ladder** — the failing child in the probe's measurement was a route ITEM child (`plan.item:<tag>`); fork-spawned `.item:` children have NO compiled NodeDecl, so `fail_node` (`api/_runner_ladder.py:159` `node.max_attempts if node is not None else 3`) falls to the default 3 while the fork stamped the row's declared ceiling (`_fork.py:61`). THE ROW LIES NOT — THE LADDER IGNORES IT. Same root as the item ladder; coordinated by report, not double-cured. |
| F7 FailureInfo's import face | **CURED (docs)** — `from taskq.workflows._types import FailureInfo` stated at the collect contract; verified importable. |
| F8 the wiring-verbs table | **CURED** — the `route`/`RouteArm` row added + `map_source`'s `aggregate=`/`rate_limits=` (read from the runtime signatures, `_graph.py:942/626`). |
| F9 the denial receipt's scope | **CURED (docs)** — the column is the vanilla pre-flight's; the flow rows carry no denial receipt (counter + `dispatch-flow-rate-limit-denied` are the face). Stated at the authoring face's bullet list. |
| F10 the ~2-minute crash cost | **CURED (docs)** — the runbook's crash page states the arithmetic + the measured 122 s. |
| F11 the drive's resume wedge | **CURED (docs)** — the return semantics (terminal/held/max_ticks — CHECK IT) + the resume law at BOTH faces (api-reference drive bullet + workflows.md the reads section + the runbook page). The in-process driver's missing lock-reclaim sweep is the machinery's shape (the maintenance leader owns it) — documented, not a code cure. |
| F12 the teaching union | **CURED** — `kind: Literal["image"]`/`Literal["audio"]` distinct required faces; the fence RUNS (17/17 rate-limiting fences green). |
| F13 ProgressUpdated's None fields | **CURED (docs)** — the fields are NOT dead (the BACKFILL arm populates them, `_progress_listen.py:376`) — the pointer-not-payload law stated for both event models in ONE paragraph (notify-arm None; backfill has paid the read). |
| Dash 1 singleton self-scheduling | **CURED** — the warning + the two working shapes (actors.md); the pre-flight cross-ref (jobs-clients.md step 3). |
| Dash 2 leader failover | **CURED** — the promotion claim cross-referenced to `TASKQ_LEADER_LEASE` (jobs-clients.md) + the "Leader failover after a leader death" paragraph (maintenance-sweeps.md). |
| Dash 3 the lock-lease invariants | **CURED** — the three invariants in one table (workers.md, the heartbeat section). |
| Dash 4 `job show` tags | **CURED (code)** — `_JOB_SHOW_COLUMNS` + the tags line (cli.py) + the cli.md field list + example output. |
| Dash 5 the display-identity recipe | **CURED** — ops.md §5's "The recurring run's display identity" recipe. |
| Import 1 the guides' reachability | **CURED** — the api-reference header's guides-are-the-tutorial block + the doc-ingest example's pointer. |
| Import 2+9 the channels | **CURED** — THE BODY'S THREE INPUT CHANNELS table (workflows.md, the deps seam's head): context / wired payload / deps, the ctx.input-vs-wired-payload confusion named, the cache-address note. |
| Import 5 the JoinSpec remedy | **CURED (code)** — E6's message + `FanInBoundExceededError`'s lead with the PUBLIC partitioning face; JoinSpec named as the ENGINE's fork door, not a graph-verb parameter. |
| Import 6 the route join's nesting | **CURED (code)** — the route docstring packs VERBATIM (a list-returning arm NESTS); the flatten-in-the-arm guidance. |
| Import 7 the inference promise | **CURED (code)** — the step docstring carries the honest boundary (@app.actor wrappers degrade to `Promise[object]`). |
| Import 8 the crash runbook | **CURED** — runbooks.md "A workflow run's mid-run hard crash (the operator's page)" — six steps, the operator's door order. |
| Import 10 | **CURED** — F13's paragraph covers it. |

## THE PROOF (the gates at the committed head)

Branch head at close: `1e6a5aada` (the source-final head `f3feba2a` +
the measurement chain `30f4c3e0f` → `48a2b4169` → `37da03f1e` →
`1e6a5aada` — the two-step shape: the claims re-recorded at the
source-final head, the receipts' commits measurement-only, the rv2-13
pin's truthful red at R flipping to green at M).

| Gate | Result | Receipt |
|---|---|---|
| `uv sync --extra flows` (F1's live face) | resolves (was: `Extra `flows` is not defined...`) | the packaging pin: `test_flows_extra_is_an_optional_dependency_not_a_project_key` |
| ruff check . + format --check . | pass, 1705 files, rc=0 ×2 | `runs/lint-format-20261011T030601-docsfaces.txt` |
| pyright FULL (src/taskq tests) | 0 errors / 0 warnings / 0 informations | `runs/pyright-full-20261011T030601-docsfaces.txt` |
| the type gate | 39 MUST_ERROR markers red on pyright 1.1.414 AND ty 0.0.85, no strays | `runs/typeprobe-gate-20261011T030601-docsfaces.txt` |
| mkdocs --strict | rc=0, 0 warnings | `runs/mkdocs-strict-20261011T030601-docsfaces.txt` |
| the docs-examples tier ×2 (F3's EXECUTION RECEIPT) | 96 fences × 2, rc=0 ×2 — incl. `progress.md:454` (the run_display fence) + the cured rate-limiting fence | `/tmp/opencode/docs-tier-leg{1,2}.txt` summaries in the lane's session record; the tier is green at the final head |
| the wf battery ×2 at -n 8 | 561 + rv2-13-truthful ×2 → green ×2 at M | `runs/wf-battery-rv4-20261011T030601-docsfaces.txt` |
| the bands (test_wf_perf_bands) | 5 passed, rc=0 — the band claims re-recorded at `f3feba2a` | `runs/wf-bands-rv4-20261011T030601-docsfaces.txt` + the JSON captures |
| wf-scoped coverage | 92.99% vs floor 90.0 — green | `runs/wf-scoped-coverage-20261011T030331.json` |
| the fast tier ×1 at -n 8 (at R) | 1 failed / 9654 passed — rv2-13's TRUTHFUL red (the committed estate stale until the receipts commit) | `runs/fast-tier-n8-20261011T030601-docsfaces.txt` |
| the fast tier ×1 at -n 8 (at M) | **9655 passed / 4 skipped / 0 failed, rc=0** | `runs/fast-tier-n8-20261011T101129Z-docsfaces.txt` |
| rv2-13 green (the pin, standalone) | 17/17 at the final head | `runs/rv2-13-green-2026-10-11T100659Z.txt` |
| the evidence-heads verifier | **rc=0** at the final head `1e6a5aada` | `runs/evidence-heads-verify-2026-10-11T100659Z.txt` |

## THE ESTATE'S HONEST STATE

- The fast tier at R red TRUTHFULLY (the head-stamp law on the
  pre-cure claims) and was cured by the receipts commits, not gamed —
  the flip's both captures are on the record.
- The leg2 kill-and-resume drill flaked ONCE under the loaded battery
  (its 240 s resume deadline under 8-way contention + real worker
  subprocesses; it passed solo, passed the ×2 re-runs, and passed
  under the final fast tier). The loaded-bar discipline's own class;
  nothing in the lane's diff touches the lease/reclaim path.
- The code-cure findings REPORTED (not buried): F2 (the in-process
  driver's rate-limit lift is not small — the worker's
  `_consumer.py` owns the acquire machinery), F5 (engine.py's
  failed-parent resolution routes `outcome=="failed"` only — the
  crash class bypasses the collect fan-in), F6 (the item ladder's
  `fail_node` ignores the ROW's declared max_attempts for
  fork-spawned `.item:` children — the holdmap lane's file).
- The lane's PG (:5816) is destroyed at close; the worktree
  `/tmp/opencode/wt-docsfaces` and the branch
  `feat/taskqflow-docsfaces-cures` remain.
