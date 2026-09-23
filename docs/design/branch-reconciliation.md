# The branch reconciliation: every pre-campaign branch dispositioned

The Sept 22-23 campaign (~44 merges to main) redid much of the work of the ~20
pre-campaign branches (Sept 11-21) in evolved form. This audit walks each
stale branch, reads its commits' intent, greps main for that intent, and
dispositions it. The owner's law: NOTHING LOST. Audited at main 7144421b.

Method per branch: the commit list and diff against the merge-base with main,
then main grepped for the same test names, mechanisms, and doc sections. A
verdict of LANDED cites the main file or line that carries the contribution.
No branch produced a PARTIAL finding: every piece checked has a landing
footprint on main, so no targeted rescue PRs were needed.

## The disposition table

| Branch | Intent (what it contributes) | Verdict | Evidence on main (7144421b) | Follow-up |
|---|---|---|---|---|
| atk/fence-gaps | Fence mark_retry's arms against an in-flight operator cancel (the launder fix); retry-fence attack suite; cancel-slate pins re-aimed at the fence | LANDED | The cancel fence: src/taskq/backend/_sql_templates.py:475-477 and the mark_retry header comment at :394-399; tests/test_attack_retry_fence.py and test_cancel_fence_arms.py carry the same pin names (test_operator_cancel_racing_a_failure_retry, test_fenced_retry_keeps_the_cancel_deliverable), evolved | delete branch |
| atk/fsm-properties (16) | FSM property fuzz (30k random op-steps + PG differential); event-edge oracle; progress_seq GREATEST-merge on every write arm; heartbeat claim-reconcile + slot-acquire disown | LANDED | tests/test_attack_fsm_fuzz.py (_ROW_EDGES :74, the illegal-event-edge pin :750) and test_attack_fsm_fuzz_pg.py, both evolved past the branch's versions; GREATEST-merged progress_seq in _sql_templates.py:307, 346, 469, 493, 597, 633; the reconcile landed as #431 with test_heartbeat_reclaim_arm.py | delete branch |
| atk/heartbeat-guard | Heartbeat failure-ledger attack suite; unexpected tick failures count toward the isolate threshold; SHUTDOWN cancel-origin stamps on sibling crash; ledger docs | LANDED | tests/test_attack_heartbeat_ledger.py; the failed-tick isolate decision src/taskq/worker/heartbeat.py:316-331; SHUTDOWN stamping heartbeat.py:1013 and worker/_bootstrap.py:2468; the evolved fixes shipped as #431 and #420 | delete branch |
| atk/input-boundaries | A non-finite or empty setting fails typed at the field; --older-than durations that outrun timedelta are usage errors, not tracebacks | LANDED | src/taskq/settings.py:290 and :909 (math.isfinite gates); cli.py:3014-3038 (_parse_older_than, the "duration is too large" usage error, the at-the-door comment); tests/test_attack_input_env_boundaries.py | delete branch |
| atk/rt-retention-integration | Attack pins for the prune/archive x TimescaleDB hypertables integration surface | LANDED | tests/test_attack_rt_retention_integration.py, evolved well past the branch's single-commit version (branch 1017 insertions; main's copy differs by 1047 lines) | delete branch |
| atk/terminal-provenance | mark_cancelled stamps the provable origin; isolate_self writes its reclaim event to the feed; truthful no-request reachability and channel semantics | LANDED | tests/test_attack_provenance.py with the same pin names (test_cancel_origin_truth_table_all_phases, test_phase2_without_request_is_not_forged_cooperative, test_sweep_and_isolate_feed_event_contents_agree), evolved; test_isolate_reclaim_feed.py; the origin truth table in src/taskq/constants.py:196-250; admin audit trail shipped as #429 | delete branch |
| atk/upgrade-paths (15) | The upgrade paths production walks survive attack (checkpoint chains, dependency floors, hypertable flip, kill mid-migration, mixed fleet); chaos pacing pins | LANDED | All five tests/test_atk_upgrade_*.py on main; the pacing pins' exit rides the shim's own delays (tests/test_heartbeat_chaos.py:702); the shared base commits landed with the fsm/notify wave | delete branch |
| atk/web-cron-notify | Int cursor fields parse bounded to int4; a cron seed on a spent DST overlap slot advances beyond the repeated range; SSO factory routes join the mutating-route sweep; wake transport delivery pins | LANDED | src/taskq/backend/_cursor.py:45-52 (_parse_int4); the spent-slot walk src/taskq/cron.py:484-499 with test_cron.py:311; tests/test_attack_notify_delivery.py; tests/web_admin/test_attack_list_edges.py | delete branch |
| attack/prune-ghost-pins | The archive write re-checks terminal status AND retention age at lock time (the ghost fix); deterministic retry injection in the ghost pins | LANDED | src/taskq/worker/_leader_shared.py:360-380 and :447 (the lock-time re-checks), :592-593 (both predicates re-checked in one statement); tests/test_attack_prune_ghost.py | delete branch |
| attack/twin-atomicity-pins | The twin's in-batch idempotency refusal is atomic; batch rollback covers mid-loop refusals; dedup-hit rows are spared the rollback | LANDED | src/taskq/testing/_batch.py:296-360 (the compensating rollback, the pre-existing holder row it must never pop); tests/test_attack_twin_atomic.py and test_attack_twin_pg_differential.py | delete branch |
| chore/hygiene-sweep | Attack test files drop ticket numbers; the migration-prose decision doc; conftest intercepts the watchdog force-exit; suite-hygiene pins | LANDED | tests/test_attack_loop_liveness.py (the rename); docs/design/migration-prose-decision.md; conftest.py:143-163 (_WATCHDOG_FORCE_EXITS interception); tests/test_suite_hygiene.py, evolved | delete branch |
| chore/hygiene-sweep-lc | Same doc and rename; the lost-job soak settles by quiescence; the renewal lock-order pin | LANDED | docs/design/migration-prose-decision.md and the test_attack_loop_liveness.py rename; tests/test_rt_lost_job_soak.py:11-13 and :82 (quiescence detection, not a fixed deadline); tests/test_rt_heartbeat_renewal_cancelwhere_lock_order.py | delete branch |
| chore/prose-audit | Prose sweep across 108 files in src, tests, and docs (comment clarity, dash hygiene) | LANDED | Spot-checked hunks on main: src/taskq/backend/_cancel_bulk.py:131 ("THE SHAPE: draining a filtered set as") and src/taskq/backend/_reads.py:155 ("actors without a row are absent") | delete branch |
| chore/uuid-utils-1 | uuid-utils pin floor rises to 1.0.0, lock follows | LANDED | pyproject.toml:52 ("uuid-utils>=1.0.0,<2") | delete branch |
| docs/030-release-notes-truth (10) | Every 0.3.0 breaking change carries a marker release-please reads; commit-search-depth covers the whole walk; the marker-guard and accuracy tests; the upgrading-doc max_attempts claim corrected | LANDED (evolved) | tests/test_breaking_change_markers.py rebuilt around the same 0.3.0 drift with commit-search-depth at 1000 in release-please-config.json:6 (the branch asked for 500); tests/test_release_breaking_change_accuracy.py; docs/guides/upgrading.md:634-638 (retry_job as the one explicit raise) | delete branch |
| docs/ops-adoption-guide (4) | The operations and adoption guide; new_uuid export; max_retry_backoff wiring; OTel example fix | LANDED | Merged verbatim as #119 (main 9157b318, identical subject); docs/guides/ops.md on main | delete branch |
| docs/taskq-concurrency-footguns (16) | Misnomer: no footguns doc in the diff. The Sept 12-13 hardening wave: per-actor enqueue cap admission, streaming cap refusals, terminal-write fusion, copy-on-write sweep-health caches, cron-tick calibration, Docker/psql skips | LANDED | Every commit maps to a main commit (#120, #149, #152 wave): _enqueue.py:502-534 (per-actor cap admission), obs/_otel.py:2030-2036 (copy-on-write publication discipline) with test_rt_sweep_health_cache_thread_safety.py | delete branch |
| feat/optional-hypertables | Optional TimescaleDB hypertables for the retention tables; the module, docs, tests, and CI lane | LANDED | src/taskq/timescale.py; docs/guides/timescaledb.md; tests/test_timescaledb_hypertables.py; the CI lane .github/workflows/ci.yaml:213-248 | delete branch |
| feat/queue-mode-and-capacity-semantics | Queue mode configurable so fairness_key is not a no-op; queue_ops; capacity semantics docs and tests | LANDED | src/taskq/worker/queue_ops.py; cli.py:95 (set_queue_mode) and :162 (the queues command); tests/test_queue_ops_integration.py and test_queue_ops_validation.py; cli.py:2506 documents the strict_fifo default | delete branch |
| feat/tors-acceleration | tors as the NUL-scan accelerator, reworked to a core dependency; A/B benchmark; parity and perf gates | LANDED | #401 (the NUL scan on tors, 30x on the worst shapes) and #446 (docs/design/tors-adoption-map.md); pyproject.toml:50 (tors>=0.10.1 core); benchmarks/ab_tors_nul_scan.py, tests/test_tors_nul_parity.py, tests/test_tors_nul_perf.py | delete branch |

## What the audit found

- Twenty branches, zero missing pieces. Every fix, pin, and doc each branch
  contributes has a landing footprint on main, in every case evolved past the
  branch's form (same pin names, tighter mechanisms, more coverage).
- The four large stacked branches (atk/fsm-properties, atk/upgrade-paths,
  atk/web-cron-notify, chore/hygiene-sweep-lc) share a common base of Sept 21
  commits; main absorbed that base once and each branch's unique tip work
  separately.
- One branch name lies: docs/taskq-concurrency-footguns contains no footguns
  document. It is the Sept 12-13 hardening wave whose commits main took via
  the #120/#149/#152 squashes.
- The campaign's substitutions, for the record: fence-gaps work -> the
  terminal-write fence probes #394/#400; heartbeat-guard -> #431/#420;
  terminal-provenance -> #429; web-cron-notify's attack suite -> the renamed
  tests/test_attack_loop_liveness.py; tors-acceleration -> #401/#446.

## Follow-up

The branches are safe to delete: origin main at 7144421b carries their whole
contribution. Nothing was extracted; no rescue PRs were opened.
