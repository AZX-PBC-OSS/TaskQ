# The flows demo — the workflow feature, end to end

A runnable `WorkflowApp` that exercises, on ONE run, the branch's two
feature families — **the capacity surface** (`max_concurrent`,
`reservations`, `result_ttl`) and **the retention arms**
(the `TASKQ_WORKFLOW_*_RETENTION_PERIOD` settings) — beside the
workflow engine's own story: a map fan-out with one deliberately
failing item under the `collect` failure policy, a HITL gate, a durable
timer, and a typed terminal report.

The graph (`flows_demo` in `app.py`):

```
source ──▶ map enrich (one item doomed, on_failure="collect")
              │
              ▼
           review ──▶ narration (sink — fire-and-forget)      ← the HITL gate
              │                                                  (typed Approval hold)
              ▼
           cooldown                                           ← the durable timer
              │                                                  (ctx.wait_until)
              ▼
           report  (max_concurrent=1,                         ← the capped stage
                    reservations=[1 slot], result_ttl=1h)
```

What each piece demonstrates:

| Piece | The feature | Where to look |
|---|---|---|
| `source` + `map enrich` | per-item fork (each item its own job, its own ledger identity) | `map_source(source, enrich_item, ...)` |
| `on_failure="collect"` | the doomed item's failure fans in as a typed `FailureInfo` envelope on the map join row; **the flow still completes** | the join row's `metadata.failures` |
| `review` | the HITL gate: the node holds on a typed `Approval`; the resolve delivers the payload and the body resumes | `ctx.wait_signal` + `GateDecl` |
| `cooldown` | the durable timer: a hold whose resolver is the CLOCK (`fired_by=timer`), not a process-local sleep | `ctx.wait_until` |
| `report` | the capacity surface declared on the handle: `max_concurrent=1` guards a not-thread-safe resource (the module-level overlap guard raises if two bodies EVER overlap), a `ConcurrencyReservation` slot, a `result_ttl` | the `@flows_app.actor` handle + the `actor_config` row |
| the launch | the retention arms armed through the settings' env names (default 0 = OFF) | `run_demo._arm_env` |

## Prerequisites

A reachable Postgres 18 — the only service. The two-minute one:

```bash
docker run -d --name taskq-flows-demo -p 5432:5432 \
  -e POSTGRES_USER=taskq -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq postgres:18.6
```

## Run it

The story in-process (the dev door — `FlowRunner`/`drive`; the rows
are the same rows a worker process would drive):

```bash
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
TASKQ_SCHEMA_NAME=flows_demo \
TASKQ_ENVIRONMENT=dev \
  uv run python -m examples.flows_demo.run_demo
```

The script migrates the schema (`flows_demo` by default — set
`TASKQ_SCHEMA_NAME` for your own), seeds the `actor_config` rows,
creates ONE run, drives it to the HOLD, resolves the hold the way the
admin's Resolve form does (`HitlClient.resolve`), drives to terminal,
and prints the run's observations. The schema is left in place so the
webapp below can serve the run.

The admin UI over the SAME rows (plus a background drive loop so runs
continue past holds and timers while the UI is open; the webapp defaults
`TASKQ_ADMIN_UI_SECURE_COOKIES` off for its own process — this demo is
plain http, where a Secure CSRF cookie is dropped and the Resolve form
would 403; `TASKQ_ADMIN_ACTIONS_ENABLED` is the Resolve's own opt-in —
without it the form never renders):

```bash
TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
TASKQ_SCHEMA_NAME=flows_demo \
TASKQ_ENVIRONMENT=dev \
TASKQ_QUEUES=flows-src,flows-review,flows-timer,flows-pub \
TASKQ_MIGRATE_ON_START=true \
TASKQ_ADMIN_ACTIONS_ENABLED=true \
  uv run uvicorn examples.flows_demo.webapp:app --port 8003
```

## WHAT TO OBSERVE

1. **The run page** — <http://localhost:8003/admin/workflows/{run_id}>
   (`run_demo` prints the URL; the runs list is
   <http://localhost:8003/admin/workflows>). The graph's nodes with
   their live statuses: the map collapsed to its join, the doomed
   item's failure badge, the review node's held-then-resumed history.
2. **The Resolve flow** — while a run sits at the review gate, the run
   page's node panel shows the HOLD (the typed gate, the reason, the
   deadline); the Resolve form validates against the declared
   `Approval` model — a payload that fits none of the gate's models is
   the typed refusal (the hold SURVIVES), a fitting one delivers
   exactly once (the CAS) and the body resumes with it.
3. **The collected failure** — the `source.join` node's
   `metadata.failures` is the typed `FailureInfo` envelope:
   `node_key: "source.item"`, the doomed item's `map_index`,
   `policy: "collect"`, the error class + the attempt history. The
   flow's root reads `succeeded` anyway — the collect is the feature:
   a failing item is REPORTED, never a cascade.
4. **The actor_config rows** — <http://localhost:8003/admin/actors>
   (or `run_demo`'s printed table). The `flows-pub` cohort's row
   carries the declared capacity terms: `max_concurrent=1`,
   `result_ttl=1h`, the `workflow_actor` metadata stamp. The
   declaration SEEDS the row; the operator owns it
   thereafter (`taskq actor-config` / `ActorsClient.set_capacity`) —
   a later boot never overwrites an operator's values.
5. **The cap's tripwire** — `run_demo`'s tail prints the overlap
   guard's counters: every body that entered the capped stage, and the
   overlaps (always zero — `max_concurrent=1` and the one-slot
   reservation keep the not-thread-safe resource single-entry; on a
   fleet the claim SQL's per-actor arithmetic enforces the same bound
   the guard screams about).
6. **The reservation's slot rows** — `run_demo` prints the
   `reservation_slots` rows for the demo's bucket: ONE slot, held by
   nobody once the run is terminal (the lease's release face).
7. **The durable timer's receipt** — the terminal report's `cooldown`
   field reads `fired_by=timer`: the wake came from the sweep's timer
   arm, against the `wf_signals` hold row under the reserved
   `__timer__` prefix — the clock resolved it, not a sleeping process.
8. **The retention arms** — `run_demo`'s act 6 fires ONE pass of each
   arm (the same `taskq.workflows._sweep` functions the leader's
   sweep loop drives, reading the five settings) against TWO runs:
   the terminal one and a second run parked LIVE at its review hold.
   At the declared 7-day periods nothing is aged out; at a 1µs period
   the TERMINAL run's flow history sweeps (`wf_edge`, `wf_join_fire`,
   `wf_step_ledger`, `wf_signals`, `wf_node_progress` — the deletion
   counts print) while the LIVE run's rows are untouched — the
   flow-terminal-scoped guard, demonstrated. The settings themselves
   are the zero-means-off convention (the shipped sibling:
   `TASKQ_WORKFLOW_OUTBOX_RETENTION_PERIOD`): a missing or `0` value
   skips the arm entirely — no surprise deletes on upgrade.

## The launch surface: the retention settings

`run_demo` arms (via `os.environ.setdefault` — an operator's explicit
env always wins):

| Env var | The demo's value | The arm it feeds |
|---|---|---|
| `TASKQ_WORKFLOW_EDGE_RETENTION_PERIOD` | `7d` | `wf_edge` (the join counter's ledger) |
| `TASKQ_WORKFLOW_STEP_LEDGER_RETENTION_PERIOD` | `7d` | `wf_step_ledger` (the exactly-once attempt record) |
| `TASKQ_WORKFLOW_JOIN_FIRE_RETENTION_PERIOD` | `7d` | `wf_join_fire` (the exactly-once fire ledger) |
| `TASKQ_WORKFLOW_SIGNALS_RETENTION_PERIOD` | `7d` | `wf_signals` (the holds + the durable timers) |
| `TASKQ_WORKFLOW_NODE_PROGRESS_RETENTION_PERIOD` | `7d` | `wf_node_progress` (the state channel) |

The convention is the estate's own
(`TASKQ_WORKFLOW_OUTBOX_RETENTION_PERIOD` is the shipped sibling):
non-negative durations, and **`0` means OFF** — no surprise deletes on
upgrade. The leader's sweep loop reads the settings every pass; the
demo's act 6 fires the same arms directly so the effect is observable
in one command.

## The smoke test

```bash
uv run --no-sync pytest tests/test_wf_flows_demo.py -v
```

(The `examples` + `integration` markers, the example tests' own
convention — `tests/test_examples_smoke.py`'s shape. Needs
testcontainers PG; no Redis.)

## Notes

- The capacity terms are declared on the HANDLE face
  (`@flows_app.actor(max_concurrent=1, reservations=[…],
  result_ttl=…)`) and flow from the handle into the
  node at wiring time — the wiring repeats nothing (an explicit
  `step(max_concurrent=…)` param would OVERRIDE the handle's term,
  never the reverse). The reservation's schema binding lands at
  `configure(schema)` (the demo's launch seam): the declaration at
  import cannot know the deployment's schema, and the bootstrap's
  ensure-slots filter reads the instance's own schema against the
  worker's settings.
- The demo drives the runs IN-PROCESS (the dev door) — the rows are
  the same rows a worker process would drive; the capacity ENFORCEMENT
  (the claim SQL's per-actor arithmetic over `actor_config`) is the
  fleet door's, exercised by a real `taskq worker` against the same
  rows.
