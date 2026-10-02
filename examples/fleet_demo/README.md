# The Fleet Demo — TaskQ's flagship surfaces, end to end

One script, seven acts, real containers. Everything you see is the engine
running on Postgres 18 + Redis: no mocks, no stubs, no web framework. The
worker's own structlog events are the proof at every step.

```bash
uv run python -m examples.fleet_demo.run_demo
```

The script brings up its own isolated infrastructure (compose project
`taskq-fleet-demo`, Postgres on **:5433**, Redis on **:6380**), applies the
schema migrations, runs a real `taskq worker` subprocess, and walks the
story below. Re-runnable end to end; the final act runs `down -v` so no
container or volume is left behind.

---

## Act 1 — Enqueue → dispatch → succeed

Three `ship_order` jobs go in through the client, the worker's dispatch
loop claims them (`"event":"dispatch"`, `pending → running`), and the rows
terminalise (`state-change`, `running → succeeded`). This is the spine
every later act hangs off.

## Act 2 — Rate limiting: the denial + the Retry-After

Twelve jobs hit the two Redis-backed limiters registered in `actors.py`:

| Actor | Primitive | Quota |
|---|---|---|
| `token_metered` | **Token bucket** | capacity 2, refill 1/s |
| `window_metered` | **GCRA sliding window** | 3 per 15s |

Only a few dispatch immediately. Every denied dispatch emits the
registry's decision log — `"event":"rate-limit-decision", "allowed":false`
— and the line carries the **`retry_after_seconds`**: the exact time the
limiter computed before a token (or a GCRA TAT slot) frees up.

What the denial does to the JOB ROW is the part most systems get wrong:
the dispatch is rolled back, the row re-pends as `scheduled` with
`scheduled_at = now + retry_after` — the Retry-After literally becomes the
row's due time — and **no retry budget is consumed** (`rate_limit_blocked_count`
counts it, `attempt` does not move). The insights `wait` table at the end
of the act reads these rows as the `deferred` segment, kept out of the
clean queue-latency percentiles by construction.

## Act 3 — The operator cancel: the ownership verdict

A `long_haul` job (45s of work, polling `ctx.should_abort()`) goes
running, and the operator cancels it:

```bash
taskq job cancel <id> --reason "operator drill"
```

Watch the CLI's verdict honesty: a running job answers
`outcome: cooperative cancel requested (cancel_phase=1)` — *requested*,
not *done*, because **the worker running the job owns the transition**.
The actor's next cancel checkpoint observes the flag (`"event":"long-haul
unwinding at the cancel checkpoint"`), the row terminalises `cancelled`,
and `taskq job events` shows the trail: the `cancel_request` entry with
the operator's reason, then the `cancelled` state change.

The contract this act demonstrates: an operator cancel resolves to
**cancelled, never abandoned** — the row keeps its cancel bookkeeping
(`cancel_requested_at`, `cancel_phase`) and the verdict names who owns
it. The operator-vs-shutdown *origin* itself is the worker's in-memory
routing stamp: it decides who owns the terminal write, and the shutdown
act's `cancel_origin_counts` event is where it surfaces.

## Act 4 — The cron tick budget + the catch-up window

Three cron schedules register themselves at worker startup:

| Schedule | Cadence | Payload factory |
|---|---|---|
| `cron_digest` | every 15s | instant |
| `cron_greedy_one` | every 5s | sleeps **4.0s** |
| `cron_greedy_two` | every 5s | sleeps **4.0s** |

The leader's cron tick has a funded factory budget of ~4.5s per tick
(`dispatcher_command_timeout` minus the write reserve). It can fund ONE
greedy factory. The second one every greedy tick finds the leftover
(≈0.45s) below the minimum fundable grant (a quarter of the funded
budget) — and is **budget-DEFERRED**, not struck: `next_fire_at` advances
one cadence, no strike, no retry budget. Watch the
`cron-fire-budget-deferred` events. (Deferral is the strike-free outcome
the tick owes a schedule that did nothing wrong: the factory never ran,
so there is no evidence against it.)

**The catch-up window's budget skipping.** The operator then mutes the
greedies and replays `cron_digest`'s clock 65 minutes into the past —
*outside* the 1h `cron_catch_up_window`. The tick admits it lost the
race: `"event":"cron missed slots skipped"` with `skipped_slots` counting
every owed fire it drops, hopping straight to the next future occurrence.
The window IS a budget: it bounds how far back the fleet will pay.

**The catch-up crawl.** Replayed only 75s back — *inside* the window —
the same schedule owes 5 slots at a 15s cadence. Now the opposite
behavior: each tick fires the oldest owed slot and advances
`next_fire_at` by one cadence — a burst of `cron fired` events, one per
tick, until the schedule has caught up. No budget consumed, nothing
skipped.

## Act 5 — The deploy story: SIGTERM mid-job

A second `long_haul` goes running and the demo SIGTERMs the worker —
twice, like a real orchestrator's escalation probe. The shutdown
orchestration walks its phases (`DRAINING → CANCELLING → FORCING →
RELEASING`, each a `shutdown-phase` event), the in-flight job is
interrupted — and then the contract: **a deploy never terminalises a job
row.** The interrupted row is released back to the fleet as `pending`
with its spent attempt standing. Not failed, not abandoned: nobody lied
about what happened to it.

## Act 6 — `taskq doctor` on a deliberately sick config

The worker is now down (act 5's deploy). The demo stages six ways a
deployment can rot silently, then runs the read-only report:

| Staged sickness | Finding family |
|---|---|
| *(always)* | **storage mode** — the detected mode (vanilla) + its capability consequences |
| `TASKQ_NOT_A_REAL_KNOB=1` | **unknown env** — a typo'd variable silently applies its intended setting's default |
| `ghost_job` in the registry, never registered by a worker | **no stored row, NEVER DISPATCHES** |
| `offline_meter` in the doctor's registry only (its queue is never subscribed) | **no stored row, NEVER DISPATCHES** — second instance of the family |
| `legacy-ingest` queues row, no actor assigned | **stale queues row** — inert cap that silently binds the next actor moved there |
| `ship_order` tuned `max_pending=1 < max_concurrent=4` | **incoherent stored config** — the concurrency cap is unreachable |
| `--platform-grace-seconds 1` | **platform stop grace below the modelled worst-case shutdown** — SIGKILL mid-teardown, work re-runs |
| pending rows due, zero live workers (act 5's released row is one) | **stranded work — unserved queue, `!! no capacity`** |

Exit code 0 either way — every condition is one a worker keeps running
through; the report is the one surface that names them together.

## Act 7 — `taskq insights` reads the same run back

The terminal surface over the run's own ledger:

```bash
taskq insights   # wait · balance · drain · cron
```

- **wait** — the `deferred` segment is act 2's rate-limit denials; the
  `clean` segment is act 1's first-delivery claims.
- **balance** — the worker is down: `!! no capacity` on the fleet's
  pending rows.
- **drain** — the throughput extrapolation over the same window.
- **cron** — the digest schedule's fan-out ledger (fires vs cleared) for
  the schedule acts 4's budget and catch-up acts drove.

## What this demo does NOT cover

- **The observability plumbing** — structlog JSON is what you've been
  reading throughout; for OTEL tracing export see
  `examples/otel_setup.py` (Jaeger / any OTLP collector).
- **The admin UI** — the same run, browsable: `examples/app.py` /
  `examples/admin_app.py` (`docker compose up` in `examples/`).
- **Storage modes beyond vanilla** — doctor detects TimescaleDB modes
  from the server (`timescale-apache`, `timescale-tsl`); point
  `TASKQ_PG_DSN` at a TimescaleDB instance to see that report line
  change (see `docs/guides/timescaledb.md`).
