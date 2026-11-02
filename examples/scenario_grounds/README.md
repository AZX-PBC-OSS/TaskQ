# The Scenario Grounds

Seven runnable scenarios, one production behavior each. Every script
drives real workflows against a real Postgres (and a real Redis when a
worker joins), prints the states it exercised and the rows it verified,
and ends green only when the rows told the truth it promised. The
companion test (`tests/test_scenario_grounds.py`) runs each script and
asserts the printed evidence's row-truth.

## Running a scenario

Each scenario builds its own schema from scratch (`grounds_s1` …
`grounds_s7`), so any of them re-runs cleanly and none of them sees
another's rows. Point them at your Postgres (and Redis for the
scenarios that start a worker); the defaults match the repository's
compose stack (Postgres on 5432, Redis on 6379):

```bash
export TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq
export TASKQ_REDIS_URL=redis://localhost:6379/0

uv run python -m examples.scenario_grounds.s1_fanout_hold
```

The output is the story in plain lines, with one machine-readable
`EVIDENCE {…}` line per check — the rows each scenario verified. The
companion test parses those lines and asserts them.

## The seven scenarios

### 1. The fan-out with the approval hold — `s1_fanout_hold.py`

A batch of documents fans out to two queues: the light items take the
fast lane, the dense ones the heavy lane. When both lanes have landed,
the flow stops at an approval step — a human gate — and waits. A typed
resolve answers the hold; the flow publishes and finishes.

Evidence: both lanes' rows on their own queues and all finished; the
hold's lifecycle (held, then delivered); the run's terminal state with
its published result.

### 2. The three-tenant priority split — `s2_priority_split.py`

Three tenants share one actor. The paying tenant's rows go first
(priority 30), the standard tier next (10), the free tier last (0) —
fifty runs each, 150 rows on one queue, created round-robin across the
tenants so the row ids alternate too, then one worker drains the lot
claiming one row at a time.

Evidence: the claim order read off the rows themselves — every
high-priority row claimed before any standard row, every standard row
before any free-tier row. Because the ids interleave across the
tenants, a claim order that ignored priority would fragment the drain
across the tenants; the grouped order the rows record can only come
from the priority column. And all 150 runs finish succeeded.

### 3. The max_concurrent damper — `s3_max_concurrent.py`

Ten flows arrive at once for one actor that admits two at a time; each
body runs about 0.7 s, so the cap has to hold the line. The dispatch
claim reads the actor's stored cap and admits at most two of its rows
per round.

Evidence: the rows' own start and finish instants folded into one
running count — never more than two in flight, two genuinely reached
(the cap bound, not idled), and all ten flows succeeded.

### 4. The reservation guard — `s4_reservation_guard.py`

A rendering step and a competing plain actor share a concurrency
reservation of two seats. The plain actor takes both seats first and
holds them while it works; the step's turn comes and finds no seat
free.

Evidence: the step waits — its row parked with the denial's own stamp
read off it (the reason and the denied bucket), no failure and no
attempt spent; the seats' occupancy read off the reservation table; and
the step goes again when the denial's retry hint says the seats may be
free — the hint is derived from the seats' own lease expiries, so the
scenario's five-second lease keeps the wait short.

### 5. Retention, live — `s5_retention_live.py`

Three runs finish — two plain, one whose approval was held then
answered — and a fourth stays live at its own gate. The horizon is
five seconds. The maintenance leader's retention sweep retires a
terminal run's history past the horizon and leaves a live run's
history alone.

Evidence: the history rows counted before the sweep can exist (the
baseline is sampled before the worker starts — a terminal run's
history is inert until a sweeper appears, so a slow caller cannot lose
the baseline) and after it prunes, per table; the live run's rows
untouched; and the sweep's own prune record (each arm's tick naming
the rows it retired) read from the worker's event log.

### 6. The retry ladder — `s6_retry_ladder.py`

A flaky body fails twice, then succeeds on its third try, all on its
own: the step's declared three attempts and the ladder's backoff
between them.

Evidence: the attempts' ledger rows (failed, failed, succeeded), and
the curve read off the re-pend's own provenance stamp: the re-pend
statement stamps the row's metadata with a per-attempt record — the
backoff it applied, its own instant, and the due instant, all from one
clock read, written in the re-pend's own statement. The scenario reads
that stamp once after the run and reports the applied backoff per
attempt (due minus at). No sampling, no wall-clock reconstruction —
the applied value itself is the evidence, so a flattened curve stamps
its flat value and the pin reds on it. The run ends succeeded with its
decoded result.

### 7. The degraded faces — `s7_degraded_faces.py`

Three wrong deployments, three clean refusals — nothing hangs silently,
nothing crashes, and no page anywhere answers a 500:

- the uninstalled schema: the run refuses with the typed error naming
  the schema and the remedy, and the admin's runs page renders its
  notice (a 200 with the notice). The page face mounts under the dev
  label, forced (never defaulted) so an ambient production label
  cannot crash it — dev-only by design: production mounts the sidecar
  with its real auth dependency;
- the stale definition: the worker's definitions dropped a step the
  database's rows still carry — the row parks loudly (budget-free, the
  reason stamped on the row), and the run finishes the moment the
  definitions are whole again;
- the misconfigured queue: a worker that never consumes the workflow's
  queue refuses to boot, naming the queue and the remedy.

## Where the definitions live

Each scenario's workflow definitions sit beside it (`defs_fanout.py`,
`defs_priority.py`, `defs_damper.py`, `defs_reserved.py`,
`defs_retained.py`, `defs_ladder.py`, `defs_stale_v1.py` and
`defs_stale_v2.py`) — the same files the scenario's workers carry via
their `--actors` reference. The scenario guides link each script from
[the scenario grounds guide](https://AZX-PBC-OSS.github.io/TaskQ/guides/scenario-grounds/).
