# The system-e2e tier: stateful lifecycle simulation of the running system

What the tier is, what it asserts, and how to run it. The tier lives in
`tests/system_e2e/`; the scenarios are its first population, and the
seam-audit matrix (in flight) extends it.

Related docs: [architecture.md](../architecture.md), the debugging playbook
([design/debugging-playbook.md](debugging-playbook.md)), and the conservation
chaos suite (`tests/test_rt_conservation_chaos.py`), whose subprocess harness
pattern this tier is built from.

---

## The missing tier, stated precisely

The suite already has three failure-shape tiers, and each stops one layer short
of the running system:

- **Unit pins** prove a function's contract in isolation.
- **Mechanism chaos** (the `test_rt_*` / `test_attack_*` families) proves one
  mechanism's failure paths in-process, against real PG and real Dragonfly.
- **Per-fix repros** (`fix/457`-style branches, `atk`/`rt` regression files)
  prove the specific defect each fix closed, at the smallest surface that
  reproduces it.

None of them runs the SYSTEM: several real worker processes, each running the
production bootstrap, living through a lifecycle (a deploy, a leader's death,
a broker outage, a storm) while the fleet keeps taking work, with the
invariants asserted over the WHOLE population at every settle point rather
than over one call's return value. That is this tier.

## What the tier asserts: system invariants, not per-call pins

Every scenario drives its population through the lifecycle and then asserts
the same shared counter (`tests/system_e2e/_invariants.py`):

1. **Conservation.** Every enqueued job reaches exactly one terminal outcome.
   Across BOTH tables: exactly one row per job (`jobs` + `jobs_archive`;
   zero rows is a dropped job, two is a resurrected one), archived rows are
   terminal, terminal rows are whole (`finished_at` set), no `running` row
   with a lapsed lease and no live holder (limbo), every non-terminal
   non-running row claimable (`scheduled_at` set), and the attempt ledger
   reconciles (one `job_attempts` row per attempt counter tick, live or
   archived). No job is ever in limbo.
2. **Exactly-once effects.** The actor bodies append
   `(job_id, attempt, actor, kind)` rows to a per-module `sys_effects` table;
   the counter reconciles it against the attempt ledger: never two runs of
   one attempt, never a body run with no claim row behind it.
3. **Progress-loss accounting.** Where the broker is involved: every consumed
   seq is durable (`jobs.progress_seq`, the poll-state surface), every seq a
   subscriber received is at most the durable seq, and the loss is bounded to
   the outage window (the fanout resumes after it).
4. **No permanently-stuck state.** `settle_terminal` is the backstop: a
   population that cannot reach all-terminal inside the scenario's cap fails
   the scenario.
5. **Audit truthfulness.** For the live population: a terminal status carries
   the `state_change` event naming it, a `state_change('cancelled')` event
   belongs to a cancelled row (both directions of the lie), and a cancelled
   row keeps the operator's request columns (the honoured request's trail
   survives).

Scenario-specific assertions (a drain must exit 0, a killed leader must exit
by SIGKILL, a survivor must still serve work afterward) layer on top; the
shared counter is the floor every scenario stands on.

## The scenarios

| module | lifecycle | chaos |
|---|---|---|
| `test_rolling_deploy.py` | generation A runs while B boots, A is SIGTERMed and drains, B takes ownership | enqueues land during A's drain window; every job in flight at the SIGTERM must reach `succeeded` |
| `test_leader_loss.py` | leader loss + re-election mid-flight | the elected maintenance leader is SIGKILLed with slots full; the survivor must re-elect, reclaim after the lease, and finish everything |
| `test_redis_outage.py` | a broker outage window | `CLIENT PAUSE ALL` spans enqueues, dispatches and progress fanouts; durable seq must carry every consumed seq, and the fanout must resume on the same process |
| `test_cancel_storm.py` | cancel storm | bulk cancels, operator retries and the archive prune all race on the same rows for a window; conservation must hold across both tables |
| `test_mixed_fleet.py` | a mixed misbehaving fleet | sync + async actors, one panicking, one hung past its start_to_close; the worker must survive and serve afterward, the ledger must balance. The SystemExit member of the fleet is its own test (see the known red below) |
| `test_happy_path_load.py` | the full happy path at load | three overlapping waves of 32 jobs (fast, progress fanouts, retry ladders, deliberate failures) across two workers; the conservation counter is checked at the end |

## The shape

- **Real surfaces.** Real Postgres and Dragonfly through the main suite's
  shared-container fixtures (`pg_dsn`, `module_pg_schema`), real worker
  SUBPROCESSES (`sys.executable -m tests.system_e2e._worker_entry`) running
  the production `_main` bootstrap with `TASKQ_*` env like a pod, and the
  real `TaskQ` client. No in-process worker stubs, no mocked backend.
- **One actor module, both sides.** `tests/system_e2e/actors.py` is imported
  by the test process (for the `ActorRef` handles) and by every worker
  subprocess (for the registry), so the registry a generation serves is the
  registry the client enqueues against, by construction.
- **The harness** (`_harness.py`) is the subprocess pattern from
  `tests/test_rt_conservation_chaos.py`: fixed argv, schema-scoped DSN,
  unique health socket per spawn, readiness = the worker's own socket
  answering, teardown reaps unconditionally.
- **Opt-in, like the other container tiers.** Collection is gated by
  `--system-e2e` (root `conftest.py`'s `_OPT_IN_TIERS`), so no other lane
  pays for the tier's wall clock. Tests are marked `integration` + `system`.

## How to run it

```sh
# One scenario module
uv run --no-sync pytest --system-e2e tests/system_e2e/test_rolling_deploy.py

# The whole tier (what CI's system-e2e lane runs)
uv run --no-sync pytest --system-e2e -m system tests/system_e2e --timeout=900 -n 2 -q
```

Docker is required (testcontainers boots the shared PG + Dragonfly pair). A
full-tier run is a few minutes of wall clock; each scenario is xdist-safe by
construction (module-scoped database + schema, unique health sockets, per-run
tags). CI wires the tier as its own lane (`system-e2e` in
`.github/workflows/ci.yaml`), following the slow-suite lane's structure.

### Known red on main

`test_mixed_fleet.py::test_sync_systemexit_is_an_attempt_outcome_not_worker_death`
reds on main, by design and with its owner named in the failure message: a
sync actor body's SystemExit escapes the executor-thread task boundary bare
and kills the worker process (rc = the actor's exit code), stranding its
claimed rows. That is the defect class `fix/459-sync-actor-systemexit` owns;
the scenario was verified green against that branch unchanged, and it goes
green on main the moment the branch lands. The fleet's survivable failure
modes (panic, deadline hang, healthy neighbours) are covered by the sibling
test, which is green on main.

## Extending it

A new scenario is one module: mark it `pytestmark = [pytest.mark.integration,
pytest.mark.system]`, drive the lifecycle through `_harness.spawn_worker` and
the `sys_client`, and end on `assert_balanced` (plus `assert_effects_balance`
whenever the bodies record). If a scenario needs a new misbehaviour, add the
actor to `actors.py` with its payload model; do not invent a second
actor-registry path.
