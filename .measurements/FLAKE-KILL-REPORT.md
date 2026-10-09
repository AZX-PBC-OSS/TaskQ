# FLAKE-KILL REPORT — the flake classes killed at the root, the pins that hold

**Branch**: `cons/grand-9` (worktree `/tmp/opencode/wt-taskqflow`, single-writer).
**Own PG**: `taskq-flakekill-pg` on :5719 (tuned to the shared pair's profile —
`max_connections=1000`, `fsync=off`, `synchronous_commit=off`,
`full_page_writes=off`, `max_wal_size=4GB`, `checkpoint_timeout=3600` — reached
via `TASKQ_TEST_PG_DSN`, the conftest's external-cluster seam, BUILD-PROTOCOL
§7b's one-container-per-worktree law).
**Capture law**: every run → a timestamped file in `.measurements/`.

## THE EXIT BAR — MET

Three consecutive full-suite green rounds of the fast tier
(``-n 8 -m "not slow and not load_sensitive"`` — CI's own fast-lane filter)
each under 15:00, captured:

| round | capture file | result | wall |
|---|---|---|---|
| 1 | `.measurements/flakekill-fast-tier-round1-20261008T220820Z.txt` | 1 failed (the mutual-drop, cured) | **11:31** |
| 2 | `.measurements/flakekill-fast-tier-round2-20261008T222709Z.txt` | 3 failed (cured: the TSL retention race, the publisher's token, the hygiene scan) | **10:44** |
| 3 | `.measurements/flakekill-fast-tier-round3-20261008T224412Z.txt` | **12,906 passed — GREEN** | **10:16** |
| 4 | `.measurements/flakekill-fast-tier-round4-20261008T231055Z.txt` | 2 failed (the mutual-drop's TRUE root — cured) | **10:54** |
| 5 | `.measurements/flakekill-fast-tier-round5-20261008T233925Z.txt` | **12,907 passed — GREEN** | **10:32** |
| 6 | `.measurements/flakekill-fast-tier-round6-20261008T235145Z.txt` | **12,907 passed — GREEN** | **10:46** |
| 7 | `.measurements/flakekill-fast-tier-round7-20261009T000355Z.txt` | **12,907 passed — GREEN** | **10:27** |

**The bar: rounds 5 → 6 → 7 — three consecutive greens, 10:32 / 10:46 / 10:27, every one under 15:00.**

Recon (unofficial, the durations profile): `.measurements/flakekill-recon-durations-20261008T214636Z.txt` — its 3 failed + 3 errors were the SPLIT-DROP class's discovery.

## BUILD-PROTOCOL §7b — THE RESOURCE LAW

*Written as the law because the campaign ITSELF was the load generator: 10+
PG containers and multiple parallel batteries on one box made the
load-sensitive pins red on load the lanes created — the co-tenancy
self-doctrination.*

1. **ONE container per worktree**, destroyed at the round's end. A worktree's
   lane names its container (`taskq-<lane>-pg`), points its runs at it with
   `TASKQ_TEST_PG_DSN`, and `docker rm -f`s it in the same session that lands
   the round. No per-invocation container spawns while a warm one exists.
2. **The lanes' batteries never run concurrent full-suites.** The orchestrator
   serializes full-suite rounds across lanes; a lane with a round in flight
   holds the box. Check `uptime` against `nproc` before launching — a load
   average within one core of the count means WAIT.
3. **The `load_sensitive` lane runs EXCLUSIVE**: a dedicated window, never
   beside another battery, one invocation at a time. Timing assertions
   measured beside a neighbor measure the neighbor too.
4. **Container inventory is a deliverable.** Every lane's report names its
   containers, their ports, their owners, and their expiry (see the inventory
   section below). A container whose lane has landed is destroyed, not
   documented.

### Container inventory — the :56xx/:57xx sprawl, actioned 2026-10-08

**Destroyed (idle orphans, lanes landed, zero external connections — verified
via `pg_stat_activity`: only internal background workers remained):**
`taskq-consolidator-pg` (:5718), `taskq-fixerlane-pg` (:5717),
`taskq-evidence-matrix-pg` (:5716), `taskq-fv-pg` (:5715),
`taskq-hygiene-pg` (:5709), `taskq-exec-pg` (:5708, postgres:16),
`taskq-typefix-pg` (:5707), `taskq-ecopatterns-pg` (:5701),
`taskqflow-p2fix-pg` (:5695), `taskqflow-p3fix-pg` (:5698) — both fix rounds
landed, worktrees gone, and the six `taskq-compose-e2e-1494918/-1494921-*`
test-debris containers (3 days old, e2e run leftovers never swept).

**Survivors (named owners, named expiry):**

| container | port | owner | expiry |
|---|---|---|---|
| `taskq-flakekill-pg` | :5719 | THIS lane (flake-kill) | destroyed with this round's last capture |
| `taskq-t20-pg` | :5704 | T20 lane — warm-by-design (mandate) | T20 lane's own teardown |
| `taskq-t21-pg` | :5705 | T21 lane (`wt-taskqflow-t21`) | T21 lane's own teardown |
| `taskqflow-p3-pg` | :5693 | P3 lane (`wt-taskqflow-p3`) | P3 lane's own teardown |
| `taskqflow-p4-pg` | :5697 | P4 lane (`wt-taskqflow-p4`) | P4 lane's own teardown |
| `taskq-mvcc-pg` | :5599 | MVCC lane (`/tmp/opencode/mvcc`) | MVCC lane's own teardown |
| `taskq-redis` | :6379 | shared dev redis, 26h up | the developer's own compose stack — not ours to kill |

## THE FLAKE CLASSES — root cause → structural fix → the pin

### Class C1: THE COPY ARITY DESYNC (the dominant class — ~50 of the prior round's 57 failures)

- **Reproduce**: `.measurements/flakekill-baseline-*.txt` — `IndexError:
  tuple index out of range` from `asyncpg/protocol/protocol.pyx copy_in` on
  every `enqueue_batch_fast` COPY, solo, deterministic. The
  mirror-divergence pins (InMemory raising the typed
  `DuplicateIdempotencyKeyError` while PG raised bare `IndexError`) were its
  downstream victims, as were every `[pg]`/`[copy]` fast-tier batch test.
- **ROOT CAUSE**: the loop-budget migration (01.00.26, mirrored 01.00.30)
  added `budget_deadline` / `budget_paused` / `budget_remaining_ms` to
  `COPY_FROM_COLUMNS` (for the archive CTE's mirror parity) WITHOUT adding
  them to `_COPY_ENQUEUE_OMITTED` and WITHOUT extending the hand-built record
  tuples: 42 columns, 39-wide records. asyncpg's cython `copy_in` answers a
  short record with a bare `IndexError` that names nothing. Measured probe:
  `COPY ...: columns=42 record0=39 → RESULT: IndexError` (the bisect in this
  round's session log).
- **STRUCTURAL FIX**: `src/taskq/backend/_sql_templates.py` — the three
  budget columns join `_COPY_ENQUEUE_OMITTED` under the SAME law as the
  workflow columns (vanilla enqueues never set them; the DDL defaults apply;
  only the loop node's own writes set them). The omission restores the
  builder/columns arity (39 == 39).
- **THE PIN**: `tests/test_migrations.py::test_enqueue_copy_record_arity_
  matches_columns` — drives ONE real enqueue through the REAL builder (a
  class-level probe on `asyncpg.Connection.copy_records_to_table`) and
  demands `len(record) == len(columns)`, naming the unwritten columns on
  failure. The omission-set pin
  (`test_copy_enqueue_columns_are_copy_from_minus_server_stamped`) carries
  the three columns + the incident note on both sides.

### Class C2: THE LEAKED-PROJECTION CLASS (the rotating worker bootstrap/health victims)

- **Reproduce**: `test_worker_bootstrap.py::test_bootstrap_populates_actor_
  config` — `assert len(rows) == 2` saw **9** (the strangers' `wf-demo-*`
  rows beside the test's own two); `test_health_lifecycle` —
  `WorkflowActorQueueConflictError: workflow actor 'wf' is declared over
  queues ['classify', 'cpu']` at boot, before the test's own stubs ran.
  Order-dependent: pytest-randomly's seed picks which poison module shares
  the victim's xdist worker — every victim green solo, the exact
  "greens solo, flake" culture the mandate indicts.
- **ROOT CAUSE**: the boot projection (`project_workflow_actor_configs`)
  reads the PROCESS-GLOBAL app registry (`_apps` WeakSet in
  `taskq/workflows/_worker_execution.py`) at EVERY in-process worker boot.
  Two tests register long-lived apps into it as import/exec side effects:
  `tests/test_wf_demo_legs.py` imports `examples.workflows` (module-level
  `wf_app`, cohorts `wf-demo-*`), and `tests/test_doc_ingest_example.py`
  execs the doc fence (a module-scoped `WorkflowApp` whose `step` calls use
  the DEFAULT `actor="wf"`, landing on BOTH `classify` and `cpu` — the
  one-queue law's tripwire). Neither has any reason to appear in another
  module's boot; nothing ever cleared the registry between tests.
- **STRUCTURAL FIX**: the src seam `reset_app_registry_for_tests()` (clears
  `_apps` — the projection's only input) + the conftest autouse fixture
  `_isolate_workflow_app_registry` clearing at BOTH ends: setup clears
  another module's residue, teardown clears this test's constructions. A
  within-test app registered after setup survives until teardown — the
  projection sees it for the test that made it and nobody else. The same
  pattern the suite already runs for the log-once stamps
  (`_reset_warn_once_stamps`).
- **THE PIN**: the conftest fixture IS the pin — it runs under every test.
  The leaked-task guard (below) is the neighboring detector for the task
  form of the same disease.

### Class C3: THE LEAKED-ASYCIOTASK CLASS (the detector is already PREDEST — kept green)

- **State**: `tests/conftest.py::_fail_on_leaked_asyncio_tasks` is ALREADY a
  hard fail (`pytest.fail` at teardown, the call-window snapshot catching
  teardown-window reaps) — a leaked task = the test FAILS, not a warning.
  This round's runs: **zero leak failures across all captures** — the
  cap-sampler close-reap cure (F1-F5 of #675, landed in
  `/tmp/opencode/flakekill`'s commits `8985f736`/`60a3ec92`) and the
  fixtures' close-and-await discipline hold. The class cannot rotate: any
  fixture that stops reaping its worker names itself in red.
- **THE PIN**: the guard itself, autouse under every test; zero-leak state
  re-verified every capture round.

### Class C4: THE BOOT-RACE CLASS (fixed timeouts sized on a quiet box)

- **Reproduce**: `tests/test_worker_bootstrap.py` slept a FIXED 2-4s and then
  asserted the boot's durable effects (the actor_config sync, the worker
  registration) — under a loaded box the sleep expired before the sync and
  the assertion read the PRE-boot table. The generic form is the mandate's
  "never registered within 30s": a poll bound sized on a quiet box, red
  under exactly the co-tenancy the lanes themselves created.
- **STRUCTURAL FIX**: the CONDITION, not the clock —
  `tests/test_worker_bootstrap.py` gains `_run_until_ready(coro_factory,
  ready)`: boot `_main`, poll `ready()` at 0.05s cadence bounded by a 30s
  deadline (~15x the measured solo boot), cancel cleanly after. The three
  fixed-sleep sites now poll their real milestone: the actor_config row
  count (2 / 1), and the `{schema}.workers` registration row (the
  cancel-timing test's "fully parked" pre-state). A deadline expiry falls
  through to the caller's assertion, which names the missing state — a slow
  boot reads as a slow failure, never a wrong one. The timescale retention
  pin's separate deadlock is the same class's DDL form (below).
- **THE TIMESCALE DEADLOCK (class C4's sibling)**:
  `test_policy_floor_bounds_the_event_ttl_sweep...` deadlocked
  deterministically (3/3): its own `add_retention_policy` RE-ARMS the
  timescaledb background job at ~now, UNDOING the `ts_conn` fixture's
  far-future deferral; the bgw's `drop_chunks` (AccessExclusiveLock on the
  below-floor chunk) then crossed the test connection's row locks →
  `DeadlockDetectedError` on the GREEN-phase seed. Fix: re-defer the policy
  job after the policy calls (`_schedule_policies(..., next_start=+3650d)`)
  — the test reads the policy's REGISTERED CONFIG (the floor), never needs
  the bgw to fire mid-test. Verified green x3 back-to-back.

### Class C5: THE AMBIENT-ENVIRONMENT CLASS (test_dev's PATH read)

- **Reproduce**: the three watch-loop tests died in `_start_worker`'s
  `shutil.which("taskq")` RuntimeError — invoked as `.venv/bin/pytest`,
  PATH does not name the venv's bin dir.
- **STRUCTURAL FIX**: the module-level autouse fixture `_taskq_exe_on_path`
  prepends the RUNNING interpreter's bin directory to PATH — the test names
  its own dependency instead of renting the ambient shell's.
- **THE PIN**: the fixture, autouse for the module; the not-found pin still
  patches `shutil.which` itself and is unaffected.

### Class C6: THE SPLIT-DROP CLASS (the recon's 3 failed + 3 errors)

- **Reproduce**: `InvalidCatalogNameError: database "tq_db_..." does not
  exist` mid-run — the recon watched THREE workers (gw0/gw1/gw3) each run
  the SAME ungrouped module (`test_typed_outcomes_attacks` — a PG-fixture
  module with no `integration` mark) with its own module-db lifecycle, and
  each lifecycle's `DROP DATABASE … WITH (FORCE)` terminated live
  connections elsewhere (the PG log's "terminating connection due to
  administrator command" storm).
- **ROOT CAUSE**: the loadgroup hook grouped only `integration`/`e2e`
  modules; a fast-tier PG-fixture module split across workers, each
  lifecycle dropping at scope exit while the controller still held more of
  the module's tests. The repo's own grouping doctrine ("what grouping
  prevents is the waste and noise of that split") extended from names to
  LIFECYCLES.
- **STRUCTURAL FIX**: the conftest's `pytest_collection_modifyitems` joins
  every item whose fixture closure touches a module-scoped PG/Redis fixture
  (`pg_dsn`, `module_redis_url`) into its own module's group — one fixture
  lifecycle, one worker. Belt-and-suspenders: `module_pg_schema`'s teardown
  tolerates a vanished database (pytest-asyncio defers the async
  module-fixture finalizer to the module loop's teardown, which can land
  after the sync pg_dsn drop — cleanup already complete).
- **THE PIN**: the hook itself, under every collection; zero split-drop
  failures in rounds 3/5/6/7.

### Class C7: THE MUTUAL-DROP CLASS (round 1's red, round 4's red — THE TRUE ROOT)

- **Reproduce (round 1)**: `test_atk_stale_epoch_write_never_applies…` —
  `InvalidCatalogNameError` on the PARENT session's own module db,
  mid-module, green solo. Round 4 reproduced it on ANOTHER param (gw7's
  `ed9cd4e0901b`) WITH the first fix already landed.
- **ROOT CAUSE (the true one, found in round 4)**: the fence-drill tests
  copy the tree and run the standing nets in a SUBPROCESS pytest with
  `env={**os.environ}` — inheriting the cluster DSN (the nets need it) AND
  the parent's run token. The scratch child's OWN
  `_publish_run_isolation_token` then OVERWROTE any inherited token with
  `PYTEST_XDIST_WORKER` (inherited = the parent's `gw7`), so the scratch
  session hashed the SAME (token, module) pairs as the parent's live
  modules and its `pg_dsn` fixtures answered with `DROP DATABASE … WITH
  (FORCE)` on the parent's dbs mid-test. The publisher's own docstring
  asserted "under xdist the worker id IS the token — invocation-unique":
  FALSE — `gw7` is identical in every `-n 8` invocation on the box.
- **STRUCTURAL FIX (the root)**: the published token is now the FULL
  BASETEMP PATH — the invocation-unique numbered dir (`pytest-N`) PLUS the
  worker's own subdirectory (`popen-gwK`): invocation-unique AND
  worker-distinct AND — because a subprocess pytest mints its own fresh
  numbered root — scratch-distinct BY CONSTRUCTION, whatever it inherits.
  The drill-level unique token stays as defense-in-depth (now reading the
  sanctioned seam, not the banned literal the suite-hygiene scan caught).
- **THE PIN**: `test_session_publishes_run_isolation_token` holds the
  full-path contract; the suite-hygiene pin held the seam when the first
  fix used the banned literal (the pin worked — the fix was rerouted, not
  the pin relaxed).

### Class C8: THE CADENCE FINDING (the maintainer's live report — investigated, NOT reproduced, PINNED)

- **The finding**: "every task sits idle yet timers fire 7x late,
  ALTERNATING". The mandate's own rule: if the degradation is OUR OWN LOAD,
  the honest verdict is the load-sensitivity marker + the exclusive-lane
  law, NOT a code bug — say so with the numbers.
- **THE NUMBERS (five measured regimes, `.measurements/cadence-probe-*.json`
  + the in-process `cadence-*.json`)**:

| regime | HB max delta | bare-timer max lateness |
|---|---|---|
| quiet, independent process, idle tasks | — | 1ms |
| 32 CPU hogs (full co-tenancy), independent | — | 4ms |
| beside a real -n 8 PG-heavy battery | — | 2ms |
| in-process `_main`, solo | 1.006x interval | 1.7ms |
| in-process `_main`, during the battery | 1.006x interval | 1.4ms |
| contended single core (runner+PG+hog on core 0) | 1.01x interval | 1.7ms |

  **The pathology does NOT reproduce** in any reachable regime, including
  the reported precondition (idle tasks).
- **The suspects, each probed and cleared**: the tick path is every-await
  (no sync PG round trip exists on the loop); the OTel wiring is
  batch/off-loop; co-tenancy never reaches an asyncio timer's fire time;
  the scheduling math is ALREADY the drift-safe follow form
  (`remaining = interval - elapsed-since-tick-START`; an overrun tick waits
  zero and re-enters — no drift accumulation possible, no compensating
  double-fire).
- **The ALTERNATING shape, reproduced and understood**: the red drill (a
  0.45s block every 0.55s) produced EXACTLY the reported pattern — raw
  deltas × interval: `2.6, 1.01, 1.0, 2.0, 1.0, 1.0, 1.99, 1.01` — late,
  on-time, on-time, late: a PERIODIC LOOP BLOCKER's own signature under
  the follow-form anchor. If the finding's box saw this, the cause was a
  periodic blocker in that process, not the heartbeat's math.
- **THE PIN (the structural fix)**: `tests/test_heartbeat_cadence.py` —
  the HB-deltas instrument made permanent: the cadence bound (1.2x
  interval = 20% headroom over the WORST measured beat, 5.8x below the
  reported 7x), the no-drift law (after an injected 1s on-loop block the
  NEXT beat is back inside the bound — red-verified against the
  drift-accumulating mutant shape and the repeated-blocker pathology),
  and the alternating check (late deltas outside the pinned block's
  window red, the raw distribution in the failure message). Marked
  `load_sensitive` — its subject is a wall-clock cadence; it runs in the
  exclusive lane where its measurement is trustworthy.

## THE TIMING TABLE (before/after)

Before: the prior round's `.measurements/fast-tier-timed-210136.txt` —
57 failed / 12,915 passed in **11:42** at -n 8 (green-but-broken: the COPY
arity desync's failures rotate with the seed, failing FAST and masking the
true wall cost of a green run).

| run | scope | failed | wall | capture |
|---|---|---|---|---|
| prior round | fast tier -n 8 | 57 | 11:42 | `fast-tier-timed-210136.txt` |
| round 1 | fast tier -n 8 | 1 | 11:31 | `flakekill-fast-tier-round1-*.txt` |
| round 2 | fast tier -n 8 | 3 | 10:44 | `flakekill-fast-tier-round2-*.txt` |
| round 3 | fast tier -n 8 | **0** | **10:16** | `flakekill-fast-tier-round3-*.txt` |
| round 4 | fast tier -n 8 | 2 | 10:54 | `flakekill-fast-tier-round4-*.txt` |
| round 5 | fast tier -n 8 | **0** | **10:32** | `flakekill-fast-tier-round5-*.txt` |
| round 6 | fast tier -n 8 | **0** | **10:46** | `flakekill-fast-tier-round6-*.txt` |
| round 7 | fast tier -n 8 | **0** | **10:27** | `flakekill-fast-tier-round7-*.txt` |

## THE SLOWEST 10 (the durations profile — WHY each is slow)

From the recon/round-1 `--durations=30` captures. Verdict: every one is
REAL WORK, no sleeps to fix (the fixed-sleep boot-races were cured at
their own class, C4):

| test | time | why |
|---|---|---|
| `test_prometheus_metrics_review` setups (×6, 82–125s) | 82–125s | the shipped worker bootstrap run in a SUBPROCESS + promtool rule evaluation inside the prom/prometheus image — real containers, real rule engines |
| `test_ops_flow_walkthroughs::flow4` | 70s | the full metrics-scrape walkthrough: real bootstrap, real exposition |
| `grace_boundary_timeline::kill_mid_cancel_ladder` (×2) | 68s | the cancel ladder's REAL grace periods (cancellation_grace + cleanup_grace) — the timeline IS the subject |
| `rt_conservation_chaos::cancel_request_survives_leader_handover` | 67s | real leader election + handover timeline |
| `compose_stack_e2e` setups (×4, 41–51s) | 41–51s | docker-compose stack boots — real orchestration per setup |
| `migrations_populated::stepwise` | 47s | replays EVERY migration stepwise onto a populated db — real migration work |
| `wf_demo_legs::leg2_kill_and_resume` | 40s | real worker subprocesses, kill + resume on live PG |
| `ops_flow_walkthroughs::flow1` | 36s | the cap knob's live walkthrough |

Nothing load-sensitive hides in the fast tier: the `load_sensitive` markers
already carve the 94-test exclusive lane, and the cadence pin (C8) joined
it.

## THE LOAD LANE (exclusive window, x1 green)

**MET**: `.measurements/flakekill-load-lane-exclusive-20261008T231344Z.txt`
(the run opened in the box's quiet window — load average 3.29, nothing
else on it): **94 passed + 1 documented skip in 1:13**, serial (CI's own
shape), including the new cadence pin.
