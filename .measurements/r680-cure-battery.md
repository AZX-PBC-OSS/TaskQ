# the PR-680 cure round's battery — the reviewer's full gate re-run on the cure head

head: see the final commit (the round's commits: F-R1 receipt → F-R2 pin → F-R4 drill → F-R5 lazy+widened → F-R3 ship → this capture)
pg: the cure round's own container, taskq-cure680-pg, port 5721 (TASKQ_TEST_PG_DSN), never the reviewer's :5720
base: feat/taskqflow's tip af1b8779 (the p4 lane's worktree re-based onto it — the p4-unique commits were already carried there re-landed)

## the gates

1. `ruff check .` — All checks passed! (the CI's exact step, ci.yaml 'Lint with ruff')
2. `ruff format --check .` — 1614 files already formatted (the CI's exact step, ci.yaml 'Check formatting')
3. `pyright` FULL (all extras) — **0 errors, 0 warnings, 0 informations**
4. the wf suite + the attacks (tests/test_wf_*.py, tests/attack_*.py, tests/test_workflows_*.py):
   **286 passed, 1 skipped, 1 failed** — the failure: test_wf_perf_bands.py::test_pin_4_enqueue_latency_band,
   the load_sensitive-marked perf band, red under the round's co-tenant load (the cold compose
   stack + the promtool probes running concurrently). Solo + rerun: green ×4 (the documented
   rotating xdist/load class, the same disposition the lane's own captures carry —
   int4-fixer-wfpins-20261008T145215Z.txt: "load_sensitive-marked, red under the 69-container
   co-tenant load, green standalone and in the rerun"). Quiet-machine rerun: the bands family
   5/5, the sweep+finalize pin families 21/21.
5. the estate slice (test_actor_config_sync + test_audit_sweep_registry_differential +
   test_migration_lock_scope_dead_index + test_terminal_drift_guards): **73 passed**
6. the type gate (tests/typeprobe/_gate.py): **the gate holds — every MUST_ERROR marker (17)
   reds on pyright 1.1.414 AND ty 0.0.85 with its DECLARED rule-ids, no error outside the markers**
7. mkdocs build --strict — clean (17.7s)
8. the promtool every-alert run (test_prometheus_metrics_review.py::test_every_alert_rule_fires_
   on_real_names_and_labels, docker prom/prometheus): **green** — F-R3's TaskQWfHoldExpired fires
   against the REAL emitted label sets and stays silent on the healthy shape (run ×2: the first
   caught the $value arithmetic, fixed, the rerun green); the harness-binding pin
   (test_harness_series_are_bound_to_the_served_exposition): green — every fed series is a series
   the real scrapes served
9. the F-R3 count pins: tests/test_prometheus_metrics.py 16/16 (25 alerts, the exact-name set,
   the lockstep CRD, the docs' rule-citation 25 + enumeration) · tests/test_rt_worker_rule_files.py
   (the runbooked-alerts' anchors + the emitted-series drift, now binding TaskQWfHoldExpired)

## the demo's cold-stack resolve (the walk, on the F-R3 cure head)

the stack: `docker compose -p taskq-cure680-demo up -d --build` from THIS tree (the cold build —
the pull policy's default `build`, the image freshly built from the cure head; app :8000, sidecar
admin :8001, postgres + redis healthy, worker-1 + worker-2 up):

- trigger1: POST /workflows/doc_ingest/run → 202, run 01a11df1-b191-7932-9b1a-bf4a3ea93250
- the APP surface's run page resolve (:8000, the typed door, CSRF-guarded POST):
  {"status":"delivered","reason":null} → the review node succeeded → **root1: succeeded**
- trigger2: run 01a11df3-0ce7-7671-b71f-6b450388d490
- the SIDECAR surface's admin page resolve (:8001/admin — the decoupled deployment's typed door):
  {"status":"delivered","reason":null} → **root2: succeeded**

(the walk exercises the pages whose module F-R5 de-lazied and the runbook rows F-R3 rewrote —
the resolve doors and the renders held.)
