# Maintenance Sweeps

## Overview

The maintenance sweeps are the leader's background repair loops: reclaiming
expired job locks, failing overdue `schedule_to_close` jobs, promoting due
`scheduled` jobs, expiring stored results, and cleaning up stale workers —
plus the on-demand bulk operations that share their machinery (bulk cancel,
actor deregistration, cron firing).

Every one of these paths writes rows in bounded, individually committed
batches. This guide is the reasoning home for that design: what failure the
bounds prevent, how to tune them, what is isolated when two schemas share a
database, how to upgrade across the advisory-lock rename, and what happens
when a bounded operation fails partway through.

For the alert-side companion (what fires, how to confirm, how to remediate),
see [runbooks.md](runbooks.md). For the raw knob rows,
[configuration.md](configuration.md#leader-sweep-intervals).

---

## 1. What the sweeps are

| Sweep | What it does | Loop / cadence | Bound |
|---|---|---|---|
| 1 — `reclaim_expired_locks` | Reclaims `running` jobs whose `lock_expires_at` passed: retryable ones → `pending` (5 s backoff), the rest → `crashed` (or `cancelled` if a cancel was in flight). Writes one `job_attempts` and one `job_events` row per job. | leader sweep loop, every `TASKQ_SWEEP_INTERVAL` (default 30 s) | `event_writer_batch_size` per batch, up to `TASKQ_SWEEP_DRAIN_BATCHES` batches per tick |
| 2 — `sweep_deadline_exceeded` | Fails overdue `schedule_to_close` jobs with `error_class='DeadlineExceeded'`. Writes one `job_attempts` and one `job_events` row per job. | leader sweep loop, every `sweep_interval` | same |
| 3 — `scheduled_to_pending` | Promotes due `scheduled` jobs to `pending` and fires a wake NOTIFY. Writes one `job_events` row per job. | scheduled-wake loop, every **1 second** | **one batch per tick** — a larger backlog drains across ticks |
| 4 — `sweep_leaked_reservation_slots` | Clears reservation slots whose lease expired. Writes no `job_events` rows. | leader sweep loop, every `sweep_interval` | single statement, not batched (no event writes) |
| Result TTL — `sweep_expired_results` | Nulls expired stored results. Writes no `job_events` rows. | leader sweep loop, every `sweep_interval` | `event_writer_batch_size` per batch, drained |
| `cleanup_stale_workers` | Deletes workers whose heartbeat went stale; the `ON DELETE SET NULL` fan-out into `job_attempts` is what the batch bound caps. | leader sweep loop, every `sweep_interval` | `event_writer_batch_size` per batch, drained |
| `complete_stale_batches` | Completes `active` batches whose completion hook was lost. | leader sweep loop, every `sweep_interval` | `event_writer_batch_size` per batch |
| 5 — prune, 6 — archive expiry | Move terminal jobs to `jobs_archive`, then hard-delete old archive rows. Daily; no `job_events` writes. | daily (default 03:00 / 04:00 UTC) | `TASKQ_PRUNE_BATCH_SIZE` (default 10 000) — a different risk class, see §2 |
| Cron tick | Fires due schedules. | cron loop, every **1 second** | `TASKQ_CRON_TICK_LIMIT` schedules per tick |
| Bulk cancel / force-deregistration | On-demand (client / CLI), not leader-gated. | on call | `event_writer_batch_size` per batch, drained to completion |

`sweep_interval` governs the **ticks**; `TASKQ_SWEEP_DRAIN_BATCHES` governs how
much each tick does. One sweep-loop iteration executes each sweep once and, if
that call returned rows, drains up to `sweep_drain_batches` batches total
before leaving the remainder to the next tick. The scheduled-wake and cron
loops are different: they tick every second and run exactly one bounded batch
per tick — their backlog drains across ticks by construction.

---

## 2. Why every event-writer is bounded

Everything in this initiative writes `job_events` rows — sweeps 1–3, bulk
cancel, deregistration — and `job_events` is not just an audit trail: it feeds
`poll_reclaim_events()`, the crash-reclaim feed consumers subscribe to. That
feed's correctness is what the bounds protect. Two distinct failures motivated
the design, and they fail in opposite directions:

### The ratchet: one transaction, one deadline, zero progress

Before bounding, a sweep did all of its work inside a single transaction —
including one `job_events` INSERT round trip per row. Once the eligible
backlog exceeded what fits inside the transaction's deadline, the sweep could
**never commit**: it timed out, rolled back entirely, retried, and timed out
again. Nothing progressed, the backlog kept growing, and the failure was
self-reinforcing. The fleet looked healthy the whole time — workers
heartbeating, dispatch reporting `count: 0` — because the stalled work was
leader-side, not worker-side. Do not scale out a fleet in this state: extra
workers consume `pending` jobs faster but promote and reclaim nothing.

### The watermark: a transaction that commits, but too late

`RECLAIM_EVENT_VISIBILITY_DELAY` (default 2 s, `TASKQ_RECLAIM_EVENT_VISIBILITY_DELAY`)
is a trailing-watermark margin: `poll_reclaim_events` only returns rows older
than the margin, so a sibling transaction holding a lower `event_id` has time
to commit first. That guarantee **assumes no `job_events` writer holds its
transaction open longer than the margin between its INSERT and its COMMIT**
(the full derivation is in
[architecture.md](../architecture.md#which-component-drives-each-transition)).
An unbounded sweep is exactly the "abnormally large batch inserted in one
transaction" that assumption names as a violation. A writer that exceeds the
margin causes a **silently missed event** — a lower-`id` row commits after the
cursor has advanced past its position, with no error raised anywhere. This is
worse than the ratchet: the ratchet is loud in the metrics; the watermark
violation is invisible by construction.

### The three-part defense

| Layer | What it does | What it is *not* |
|---|---|---|
| **Bounded batches** | One call transitions at most `batch_size` rows using a constant number of statements (a LIMIT-ed driving UPDATE, one batched `job_attempts` INSERT and one `pg_notify` where applicable, one batched `job_events` INSERT) inside one short transaction. Repeated calls drain the remainder. | Not an enforcement — a `LIMIT` alone is a hope about database speed. |
| **Server-side `statement_timeout` per batch — the enforcement** | Each batch transaction applies `TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS` (default 1750 ms = 7/8 of the 2 s watermark). A batch that cannot finish inside the watermark margin is **aborted by the server** — arriving as `QueryCanceledError` (SQLSTATE 57014), treated as transient — not silently slow. | Not a success path: the aborted batch is retried and counts against the breaker. |
| **Batch size** | Keeps a *healthy* database off the timeout in the first place (derivation in §3). | Merely a tuning value — the timeout remains the guard if the constant is wrong for a given deployment. |

The timeout is deliberately the enforcement and the batch size deliberately
isn't, because they fail differently: a wrong batch size degrades throughput
gracefully (smaller batches, more round trips), while nothing but a server-side
abort can stop a batch that is merely slow from blowing the watermark.

**A stopped drain is a pause, not a rollback.** Every batch commits before the
next begins, so an interrupted drain (shutdown, transient PG error, an aborted
batch) keeps every batch that completed. The next tick resumes where the drain
stopped — there is no keyset cursor because every row a batch snaps is
transitioned by that same batch, so the eligible set shrinks monotonically.
The same property is what makes bulk cancel and force-deregistration safely
re-runnable (§6).

Worked example, at defaults: a 50 000-row expired-lock backlog after a
fleet-wide crash drains through sweep 1 at ≤ 8 batches × 100 rows = 800 rows
per 30 s tick — roughly 31 minutes to fully reclaim, with every 100-row batch
durable the moment it commits and the fleet dispatching reclaimed jobs
continuously throughout. That steady, bounded recovery is the intended
behavior, not a bug: the alternative — one 50 000-row transaction — is the
ratchet and the watermark violation in one shape. If a known-large recovery is
coming (planned fleet restart), raise `TASKQ_SWEEP_DRAIN_BATCHES` for the
duration rather than removing the bound.

### Two supporting choices worth knowing

- **`SET LOCAL`, with capture and restore.** The batch timeout is applied via
  `set_config('statement_timeout', ..., true)` — `SET LOCAL` semantics with a
  bindable value. The previous value is captured first and restored on the
  success path *inside the still-open transaction*, because `SET LOCAL`'s
  scope is the whole transaction: a sweep nested in a caller's open
  transaction (asyncpg runs it as a savepoint) would otherwise leak its bound
  past the savepoint release onto the caller's subsequent statements. On the
  error path no restore is needed — the savepoint rollback restores the
  setting via the server's subtransaction stack. A session-level `SET` would
  be simpler and wrong: it outlives the pooled connection's checkout and
  silently caps unrelated borrowers (dispatch, archive) at a timeout they
  never asked for.
- **The two-clock split and the microsecond ladder.** Row-*selection* bounds
  use `statement_timestamp()` (STABLE, so the planner can use it as a btree
  index condition — a `clock_timestamp()` bound degrades every snap to a
  whole-population filter walk), while *written* timestamps stay
  `clock_timestamp()` so they agree with `job_events.occurred_at`. Batched
  `occurred_at` values carry a microsecond ladder on the row ordinal
  (own evaluation instant + (ordinal − 1) µs) because a bare volatile
  `clock_timestamp()` collapses tens of rows in one statement onto the same
  microsecond, destroying the per-row distinctness and ordering the watermark
  reads. Both choices are pinned by tests; operators do not tune them, but
  they are why the sweeps' SQL looks the way it does.

---

## 3. Tuning

All knobs live in
[configuration.md](configuration.md#leader-sweep-intervals); this section is
the *why* behind each default and when to move it.

### `TASKQ_EVENT_WRITER_BATCH_SIZE` (default 100)

**The question it answers:** how many rows may one committed batch touch, for
every writer of `job_events` rows?

**Why 100:** the measured cost of a two-statement reclaim batch is
~0.85 ms per row, so 100 rows is ~85 ms on loopback and ~300 ms at a 3 ms
managed-Postgres round trip — roughly a 6× margin under the 2 s watermark at
the RTT the derivation targets (loopback extrapolation, not a
managed-instance measurement). At 1 ms RTT the margin is wider still.

**When to lower it:** when `TaskQSweepTimeouts`
([runbooks.md](runbooks.md#taskqsweeptimeouts)) is firing — the database
genuinely cannot finish 100-row batches inside the timeout. Lower the batch
size (each batch gets smaller; the drain takes more batches) rather than
raising `TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS` past the watermark margin:
the margin is what keeps out-of-commit-order reclaim events from being
silently missed, and raising the timeout above it converts an aborted batch
(you know) into a watermark violation (you don't).

**When not to raise it blindly:** the ceiling is 10 000, but the watermark
margin is the real ceiling — a batch size whose healthy-case duration
approaches `event_writer_statement_timeout_ms` has no headroom for a slow
day. The prune sweep's 10 000-row default is a different risk class: prune
writes no `job_events` rows, so the visibility margin does not bind it.

### `TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS` (default 1750)

**The question it answers:** how long may the server let one batch run before
aborting it?

**Why 7/8 of the watermark (1750 of 2000 ms):** the batch must fit inside the
visibility margin, with an eighth of the margin left for the commit itself.
The value is a *derived* fraction, not an independent number — if you raise
`TASKQ_RECLAIM_EVENT_VISIBILITY_DELAY`, raise this with it, keeping it below
the margin.

**Why `SET LOCAL`-scoped:** see §2 — a session-level setting would leak onto
every later borrower of the pooled connection. The capture/restore discipline
keeps a nested call from leaking it onto its caller's transaction.

### The reduced tier and the breaker: `TASKQ_EVENT_WRITER_REDUCED_BATCH_DIVISOR` (default 4), `TASKQ_SWEEP_BREAKER_FAILURE_THRESHOLD` (default 3), `TASKQ_SWEEP_BREAKER_WINDOW_SECS` (default 600)

**The question they answer:** what does the worker do when the database keeps
aborting full-size batches?

Three consecutive aborted batches (a client `TimeoutError` or a server-side
`QueryCanceledError` — the two shapes an aborted batch produces) within the
rolling window latch the sweep's `SweepBatchSizer` to the reduced tier:
`max(1, event_writer_batch_size / divisor)` — a quarter of the configured
size at defaults. Any success between failures resets the consecutive count;
**a latched breaker never unlatches** for the rest of the process lifetime.

**Why the latch is one-way:** the control signal — a cancelled sweep batch —
is contaminated. A timeout caused by a brief lock pile-up or a checkpoint says
nothing about whether the next full-size batch will fit, so treating a quiet
period as evidence the database recovered means every oscillation back to the
full tier is a trial that can roll back a transaction holding row locks.
Under sustained pressure the batch duration would flap between tiers, bimodal
and unpredictable. Staying degraded trades a little throughput for stability —
the reduced tier is a **ceiling, not a floor**: the sweep still drains, in
smaller committed batches.

**Remediation is a restart, deliberately.** Once the database is healthy
again, recycle the worker (or let the orchestrator do it) — the fresh process
starts unlatched. `TaskQSweepDegraded`
([runbooks.md](runbooks.md#taskqsweepdegraded)) pages on exactly this state,
comparing the sweep's used batch size against the same worker's configured
size, so no threshold drifts with your configuration.

Scope note: the breaker wraps the three `job_events`-writing sweeps
(`expired_locks`, `deadline_exceeded`, `scheduled_to_pending`). The
result-TTL, stale-worker and stale-batch sweeps pass their batch size
explicitly and are not breaker-wrapped.

### `TASKQ_SWEEP_DRAIN_BATCHES` (default 8)

**The question it answers:** how much of a sweep's backlog may one
`sweep_interval` tick consume?

One iteration runs each sweep once and then drains up to this many batches
total before leaving the remainder for the next tick. The bound exists so a
huge backlog cannot monopolize the loop — the leader has other work in the
same iteration, and detector 2 (the stale-tick watchdog) has a staleness
budget the drain must respect. Every batch commits, so a drain cut short at
the bound (or by shutdown, or by a transient error) keeps its progress.

Raise it when a known-large recovery is in flight and you want it faster;
at defaults, sweep-loop sweeps drain ≤ 8 × `event_writer_batch_size` rows per
`sweep_interval` (see the worked example in §2).

### `TASKQ_SWEEP_INTERVAL` (default 30)

Period between leader sweep-loop iterations — it governs the ticks that
everything above drains across. Lower it to shrink crash-recovery latency at
the cost of more frequent PG queries. Note the /ready maintenance view (see
"What degradation looks like" above) calls a sweep stalled after three whole
intervals without a success, so this value also sets that detector's
sensitivity.

### `TASKQ_CRON_TICK_LIMIT` (default 100)

Maximum schedules one cron tick selects, plans and fires. A catch-up burst
larger than this drains across successive one-second ticks instead of one
oversized transaction; the remainder stays due and untouched until its tick.
The tick (BEGIN, bounded selects of due schedules and their actor configs,
one batched enqueue, one UPDATE per outcome branch, COMMIT) is wrapped in a
single `asyncio.timeout` of `dispatcher_command_timeout`, so a stalled PG
errors the tick instead of stretching it past the watchdog's staleness
budget.

### What degradation looks like

- `taskq.maintenance_leader.sweep_timeouts` (counter, by `sweep_name`) — any
  sustained rate means batches are being aborted, not completing slowly. The
  `cron` label on this counter is the cron tick's own deadline, not a batch
  timeout.
- `taskq.maintenance_leader.sweep_batch_size` vs
  `taskq.maintenance_leader.sweep_batch_size_configured` (label-matched pair,
  same worker) — used below configured is the reduced tier.
- `taskq.maintenance_leader.sweep_last_success_seconds` (by `sweep_name`) —
  staleness; a value that never moves while the process runs is a stalled
  sweep.
- The `/ready` body carries a `maintenance` object: `degraded: true` with
  reasons when any sweep's last success is more than three `sweep_interval`s
  old, or when any sweep is on the reduced tier. It is deliberately **not** a
  503 — the probe verdict stays driven by liveness/readiness; degraded means
  "up, but sweeps are unhealthy", an operator signal in the body.

A timed-out sweep records its duration but **no row sample** — the batch was
aborted, so no rows committed. Read `sweep_rows` with the `sweep_timeouts`
counter in hand
([observability.md](observability.md#sweep-samples-rows-and-duration-are-different-populations)).
A process that has never completed a sweep (a fresh worker, or one that has
not won an election) reports `degraded: false` with the informational reason
`"no sweep has completed yet"` — it has stalled nothing. On leadership
demotion the outgoing leader clears its sweep-health stamps so a demoted
process cannot report a frozen (permanently degraded) maintenance view after
an ordinary failover.

---

## 4. Multi-schema coexistence

One Postgres database can host two TaskQ schemas (e.g. `taskq` and
`taskq_staging`, each its own `TASKQ_SCHEMA_NAME`). The isolation boundary is
the schema name, and it is now enforced on **both** stores.

### What is isolated in Postgres

| Namespace | Format | Example |
|---|---|---|
| Advisory locks | `taskq:{purpose}:{schema}` | `taskq:maintenance_leader:taskq`, `taskq:cron:taskq`, `taskq:prune:taskq`, `taskq:archive_expiry:taskq`, `taskq:migrate:taskq` |
| NOTIFY channels | `taskq_wake_{schema}`, `taskq_events_{schema}`, `taskq_worker_{schema}_{worker_id}` | `taskq_wake_taskq` |
| Tables | every table lives inside the schema | `taskq.jobs`, `staging.jobs` |
| Keyed PG locks | schema-qualified like their Redis twins | `taskq:{schema}:sw:{name}` (sliding-window PG fallback), `taskq:unique_for:{schema}:{actor}:{identity_key}` (enqueue single-flight) |

Advisory locks live in a per-database namespace, so an *unqualified* lock name
is shared by every schema in the database — the pre-rename behavior serialized
two schemas' leaders against each other, and the loser of that serialization
never ran its sweeps while dispatch (not leader-gated) kept flowing: the fleet
reported healthy while scheduled work stopped moving. The schema-qualified
names give each schema its own election, its own cron/prune/archive-expiry
serialization, and its own migration lock.

### What is isolated in Redis

Every Redis key and pub/sub channel carries the schema:

| Key / channel | Format |
|---|---|
| Token bucket | `taskq:{schema}:rl:tb:{name}` |
| Sliding window log | `taskq:{schema}:sw:{name}` |
| Sliding window (GCRA) | `taskq:{schema}:sw_gcra:{name}` |
| Per-job progress | `taskq:{schema}:progress:{job_id}` |
| Schema-wide progress fanout | `taskq:{schema}:progress` |

So two schemas can share one Redis database safely — buckets, windows and
progress streams never collide.

### What is *not* isolated

Two deployments pointing at the **same schema name** share one logical queue
system — by design. Same `jobs` table, same `actor_config` rows, same queues,
same rate-limit buckets, same schedules: a job enqueued by either deployment
is dispatchable by both. The schema name *is* the deployment boundary; treat
two fleets on one schema as one fleet.

One naming trap: the in-process prefix `taskq:global:queue:` is a **registry
namespace** for internal queue-cap reservations, not shared state. It names
primitives inside each process's rate-limit registry and never reaches
Postgres or Redis, so it does not couple schemas (or processes) to each other.

---

## 5. Upgrade discipline for the lock rename

The unqualified advisory-lock names (`taskq:maintenance_leader`,
`taskq:prune`, …) are **gone** — replaced outright by the schema-qualified
forms in §4, not run alongside them. The two naming schemes are not
overlap-compatible, and this is the one operational footgun of the rename:

**During a rolling deploy, old and new workers hold different lock names and
can both act as leader of the same schema at once.**

- The maintenance sweeps stay **row-safe** in that window — every snap uses
  `FOR UPDATE SKIP LOCKED`, so two concurrent leaders step over each other's
  rows rather than double-transitioning them.
- Cron gains a **double-fire window** — its advisory lock is what serialises
  ticks, so an old and a new leader can each fire the same due schedule.
- Election itself is unaffected (each side acquires its own lock name), which
  is why the split state is silent at the process level: nothing errors, and
  each worker reports itself healthy. The fleet-level signal is
  `sum(taskq_maintenance_leader_is_leader) != 1` — the split-brain alert —
  not any per-worker error.

**Adopt by restarting the fleet onto the new release, not by rolling it.**
Stop the old workers, start the new ones. The exposure window is the deploy
itself, not the steady state — once no old-release process remains, exactly
one leader per schema exists again. `TaskQLeaderLockContention`
([runbooks.md](runbooks.md#taskqleaderlockcontention)) is the alert that fires
if contention persists beyond handover; its remediation steps include this
adopt-by-restart note. See also
[upgrading.md](upgrading.md#schema-qualified-advisory-locks-adopt-by-restart).

---

## 6. Failure semantics operators must know

### Bulk cancel and force-deregistration: bounded committed progress

`JobsClient.cancel_where()` and `deregister_actor(force=True)` drain their
match set in bounded committed batches — `event_writer_batch_size` driving
rows per transaction, each batch carrying the server-side
`statement_timeout`, with the batch's `job_events` rows written by the same
transaction as the state change they describe (an event can never commit
without the transition it documents). No breaker wraps these calls, unlike
the standing sweep loops: they are interactive operator calls, not loop
policy, so a timeout abort propagates to the caller instead of degrading a
loop — re-run the operation to continue the drain.

**A mid-operation failure leaves partial progress, and a re-run continues
where it stopped.** Already-cancelled rows are skipped on the re-run: the
driving UPDATE re-checks the status predicates (evaluated per row under READ
COMMITTED), so earlier committed batches fall out of the match set. Two
details make that safe:

- **Termination keys on the window count, never the affected-row count.** A
  dispatcher claiming a `pending` job between a batch's snapshot and its row
  lock drops that row from the affected count while matching rows remain —
  terminating on the affected count would abandon the tail of the match set
  while the window was still full. A full window means more rows may remain,
  so the drain keeps going.
- **Deadlocks are retried per batch** (3 attempts, exponential backoff with
  jitter), and a batch's IDs are counted only after its event writes succeed,
  so an aborted batch contributes no phantom results.

Force-deregistration additionally refuses up front if the actor has *running*
jobs, and finishes with one small transaction for the schedule disable, the
`actor_config` delete, the terminal-history count and the optional queue
purge.

### Cron: per-schedule isolation, tick-level abort

Within one tick, each schedule is planned independently — a payload-factory
exception or a missing `actor_config` row fails **that schedule only**,
bumping its `consecutive_failures` (auto-disable at
`TASKQ_CRON_AUTO_DISABLE_THRESHOLD`) and storing its error text, while the
rest of the batch still fires.

An **insert-level infrastructure failure** (the batched enqueue itself
failing — connection loss, constraint trouble) is different: one enqueue
statement covers every planned fire, so its failure fails them all at once,
and on a real connection the aborted INSERT has already invalidated the
tick's transaction. The whole tick rolls back — the schedules stay due
because their `next_fire_at` was never advanced — and **the next one-second
tick retries**. Cron failure bookkeeping (the auto-disable counters) also
rolls back with the tick; the per-schedule span and metric still record the
failure attempt.

### NUL bytes in stored error text: sanitized, not rejected

Exception text derived from actor code is uncontrolled, and a NUL codepoint
(U+0000) in it cannot be stored in a Postgres `text` column. TaskQ splits the
problem by *where the text comes from*:

- **Derived text** (an actor exception's message or traceback, a cron
  schedule's `last_fire_error`) is **sanitized**: NULs are replaced with the
  visible `\x00` escape before storage. Rejecting would strand the very work
  the text describes — the terminal write fails, the job never reaches a
  terminal state, and the crash-reclaim loop re-dispatches it into the same
  exception forever, re-executing already-committed side effects each time.
  The stored text shows exactly where the NUL was, so the defect stays
  diagnosable.
- **Caller-supplied values** (payloads, metadata, labels, keyed-ref names)
  are **rejected loudly** with a `ValueError` at the boundary. A NUL there is
  a permanent data defect, and letting it surface as a raw driver error would
  mis-classify it as transient infrastructure failure — the job would retry
  forever instead of failing.
- In cron specifically, the sanitized error text also protects the batched
  failures UPDATE: one raw NUL in one schedule's error would abort that whole
  `unnest` statement (SQLSTATE 22021) and lose the failure bookkeeping for
  **every** schedule in the tick.

---

**See also:**
- [configuration.md](configuration.md#leader-sweep-intervals) — the knob rows (types, defaults, ranges).
- [runbooks.md](runbooks.md) — the alerts that page on sweep timeouts, degradation and promotion stalls, with confirm/remediate steps.
- [observability.md](observability.md#sweep-samples-rows-and-duration-are-different-populations) — the sweep metrics and their sample-population rule.
- [workers.md](workers.md#leader-election) — which loop runs which sweep, and the leader's loop inventory.
- [architecture.md](../architecture.md#which-component-drives-each-transition) — the trailing-watermark derivation the batch bounds enforce.
- [upgrading.md](upgrading.md) — adopt-by-restart for the lock rename and the migration 01.00.06_01 maintenance-window note.
