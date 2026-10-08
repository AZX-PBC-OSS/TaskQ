# FLAKE-KILL REPORT — the flake classes killed at the root, the pins that hold

**Branch**: `cons/grand-9` (worktree `/tmp/opencode/wt-taskqflow`, single-writer).
**Own PG**: `taskq-flakekill-pg` on :5719 (tuned to the shared pair's profile —
`max_connections=1000`, `fsync=off`, `synchronous_commit=off`,
`full_page_writes=off`, `max_wal_size=4GB`, `checkpoint_timeout=3600` — reached
via `TASKQ_TEST_PG_DSN`, the conftest's external-cluster seam, BUILD-PROTOCOL
§7b's one-container-per-worktree law).
**Capture law**: every run → a timestamped file in `.measurements/`.

## THE EXIT BAR

Three consecutive full-suite green rounds of the fast tier
(`-n 8 -m "not slow and not load_sensitive"` — CI's own fast-lane filter)
each under 15:00, captured:

| round | capture file | result | wall |
|---|---|---|---|
| (pending) | | | |

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

## THE TIMING TABLE (before/after)

Filled from the capture files as rounds land. Before: the prior round's
`.measurements/fast-tier-timed-210136.txt` — 57 failed / 12,915 passed in
**11:42** at -n 8 (green-but-broken: the failures above rotate with the seed).

| run | scope | failed | wall | capture |
|---|---|---|---|---|
| prior round | fast tier -n 8 | 57 | 11:42 | `fast-tier-timed-210136.txt` |
| (pending) | | | | |

## THE LOAD LANE (exclusive window, x1 green)

(pending — runs alone, nothing else on the box.)
