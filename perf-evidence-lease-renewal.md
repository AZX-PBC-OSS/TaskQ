# Threshold-gated lease renewal — enforced bound, sizing math, and before/after measurement

Evidence for #227: the heartbeat's per-beat jobs-lock renewal rewrites
`lock_expires_at` — the key of `jobs_running_lock_expires_idx` — for
every running row the worker holds, so every beat is a non-HOT update
per running row: a new row version plus new entries in every index a
running row satisfies (PK, `jobs_actor_running_idx`,
`jobs_locked_by_worker_running_idx`, `jobs_identity_active_idx`,
`jobs_running_lock_expires_idx`, the heartbeat-deadline partial, GIN
tags/metadata), fleet-wide, for as long as the worker lives.

## The change

`UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE`
(`src/taskq/backend/_sql.py`) — the statement the heartbeat loop binds
— renews only rows that need it:

```sql
... AND (heartbeat_timeout IS NOT NULL
         OR lock_expires_at IS NULL
         OR lock_expires_at <= clock_timestamp() + $4::interval)
```

The public `PostgresBackend.heartbeat_jobs` / in-memory twin keep the
unconditional template (byte-identical semantics; `build_heartbeat_sql`
without a threshold renders exactly the old statement), so the Backend
protocol surface is unchanged.

## Fix round: the round-1 proof model was false, and the gate had a reproduced default-settings lapse window

The round-1 derivation sized the floor as `(F+1) * (interval +
command_timeout)` on the premise that a failed tick is bounded by
"acquire-block then a timed-out command". **That premise was false.**
The tick issues ≥ 3 commands (liveness, the jobs-lock renewal, the
reservation-leases write, plus the still-held probe and the cancel
hook's statements), each **separately** bounded by the heartbeat pool's
per-query `command_timeout`, and the transaction's teardown adds its
own round trip. A brownout tick — a contended 5-7 s pool acquire, two
statements at ~1.9 s each (under the 2 s per-query timeout, so they
SUCCEED), then one that times out — lasts ≈ `acquire + 3c`, not
`acquire + c`. The independent red-team attack mechanically reproduced
the consequence at the DEFAULT settings: a legal skip at 49.5 s of a
60 s lease (just above the 48 s round-1 floor), then four such failed
ticks of ~12.3-13.8 s each, and **the gated lease expired 3.2-9.2 s
before the isolate decision while the unconditional renewal survived**
— the healthy leader's sweep reclaims rows the worker still holds
(double-run class). The round-1 property test could not express the
shape: its cascade gap scale was hard-capped at `0.999 × (interval +
command_timeout)`.

## The remediation: an enforced per-tick bound, and a floor sized to it

The landed fix makes every term of the failed-tick bound **enforced**
rather than derived (the attacker's preferred path (a), red-teamed —
see below), and re-sizes the floor to what that enforcement actually
guarantees:

1. **One command budget per tick.** `asyncio.timeout(
   heartbeat_command_timeout)` wraps the tick's whole command sequence —
   BEGIN, the liveness write, the gated renewal, the reservation-lease
   write, the still-held probe, the cancel hook's statements, COMMIT.
   Each statement's own pool-level per-query timeout stays as the inner
   backstop; the budget bounds the SEQUENCE, because the lease model
   cares about the whole tick. A brownout tick is now cut at one
   command timeout instead of running its statements out.
2. **Bounded teardown, no un-cuttable round trip.** On failure the
   transaction is rolled back only if the rollback fits the budget's
   REMAINDER; otherwise the connection is CLOSED (server-side rollback
   on disconnect — a local Terminate write, bounded and
   terminate()-guaranteed by the repo's own `close_conn_bounded`, no
   awaited server round trip; the pool discards the closed conn on
   release — verified). This is deliberate: an await that STARTS inside
   an expired `asyncio.timeout` scope is **not re-cancelled** (measured
   — the scope fires once), so both the rollback and the post-tx drain
   carry their own explicit bounds instead of hiding inside the dead
   scope.
3. **The post-tx drain shares the budget.** It runs under whatever the
   deadline has left and is deferred to the next tick when nothing is
   left (the controller's deque persists — nothing is lost). A mid-drain
   cut re-queues the in-flight abandon (the drain's exception path now
   re-queues on `CancelledError` too: the old pop-and-lose was harmless
   at task teardown but would strand a phase-3 abandon at a budget cut;
   the shielded write's late landing is absorbed by the existing
   not-applied guard).
4. **asyncpg's task cancellation is prompt** (measured: cancelling a
   task mid-`pg_sleep(3)` under a 0.3 s budget delivered the
   cancellation at 0.301 s with the connection left usable) — so the
   budget's cut lands on time and does not wait on server-side cancel
   confirmation.

The enforced failed-tick bound is therefore **`interval + 2 ×
command_timeout`** — acquire ≤ interval (its own timeout), the command
sequence ≤ one command timeout (the budget), the teardown ≤ one more
(the bounded rollback-or-close) — and the floor is `(F+1)` of those.

### Red-teaming the attacker's own "acquire + one c" target

The attack's preferred landing ("a failed tick bounded by acquire +
one c, making the `(F+1)(h+c)` model TRUE and preserving the measured
2× at defaults") does not survive its own stated trade-off, and the
round-1 lesson applies to it too:

- A budget of `k × c` (k = the tick's statement count) changes nothing
  about the bound — the per-statement timeouts already allow exactly
  that — and pushes the floor past the default lease (`(F+1)(h+3c)` =
  64 > 60): dead code at the median deployment.
- A budget of one `c` **for the statements alone** still leaves the
  transaction teardown (a rollback round trip, ≤ c) outside it — the
  same class of un-counted round trip that falsified round 1 — unless
  the teardown is the bounded close, which is what landed.
- Counting honestly, `h + 2c` is the floor's bound, and at the defaults
  `(F+1)(h+2c)` = 56 sits at/above the harvestable slack (lease −
  interval = 50): **the 2×-at-defaults win was not recoverable without
  either lying about a term or cutting legitimate slow ticks**. The
  honest landing: the gate is inert at the default lease and saves from
  lease ≈ 70 s upward.

The narrowed tolerance band is real and documented: a tick whose
statements legitimately need more than one command timeout **in total**
now fails fast (previously each statement could take just under one).
Raise `TASKQ_HEARTBEAT_COMMAND_TIMEOUT` for that regime — the knob's
meaning is broadened accordingly (per-query inner backstop + the tick's
sequence budget, settings.py) — and the floor absorbs the raised value
automatically: the gate harvests only slack that actually exists.

## Why the naive "skip while more than half the lease remains" is unsafe (both findings stand, now against the enforced bound)

1. **It falsely crash-reclaims healthy `heartbeat_timeout` jobs.** The
   reclaim sweep's heartbeat arm (`_SWEEP_1_SQL`) reclaims a row whose
   `last_heartbeat_at + heartbeat_timeout < now` while the lease is
   still valid; those rows' beats must stay per-tick fresh. The gated
   statement renews every `heartbeat_timeout IS NOT NULL` row on every
   beat (their `last_heartbeat_at` update is non-HOT anyway through the
   heartbeat-deadline partial index). Pinned by
   `tests/test_heartbeat_integration.py::test_threshold_gated_renewal_selects_rows_by_remaining_lease`.
2. **It lapses the lease before the isolate decision at the default
   settings.** With the enforced worst gap of `interval + 2 ×
   command_timeout` = 14 s, a skip at remaining 30 s + ε followed by
   four failed beats consumes 56 s: the lease is gone ~26 s BEFORE the
   loop isolates. Pinned (with the shipped floor holding the same
   cascade) by
   `tests/test_heartbeat.py::test_naive_half_lease_threshold_lapses_before_isolation_at_defaults`.

## Sizing proof

`_lease_renewal_threshold` = `max(lock_lease / 2, (F+1) × (interval +
2 × command_timeout))`:

- **Healthy beats**: a skip happens only while remaining > threshold,
  so the next beat's remaining is > `threshold − worst_gap` = `F ×
  worst_gap` at the floor (the round-1 text's `(F−1)` was an arithmetic
  slip — the algebra gives F) — ≥ one full worst gap for F ≥ 1.
- **The failure cascade**: the worst case is a skip at `threshold + ε`
  followed by F+1 failed beats, each gap strictly under one worst gap
  (an acquire that consumed its whole timeout fails with no commands
  and no teardown — a gap of exactly the interval). The lease at the
  isolate decision is > 0 by construction; a worker recovering after F
  failures still holds a full worst gap of lease.
- **At the default lease the gate is inert** (floor 56 ≥ slack 50), so
  no skip ever happens there — the reproduced window closes outright —
  and the enforced budget independently makes the default-config
  cascade survivable with margin (4 gaps × 14 = 56 against the 60 s
  lease), which the un-enforced per-statement bound
  (statement-count-dependent, up to `interval + (k+1) × command_timeout`
  per tick ≈ 20 s at k=3) is not: main's own worst case lapses.
- **Savings resume from lease ≈ 70 s** (2× at 70, 4× at 90, ~6× at 120
  where the half-lease arm binds; the `lock_lease / 2` arm keeps MORE
  margin than the floor whenever it binds, by construction).
- The comparison is server-side (`lock_expires_at <= clock_timestamp() +
  $4`), on the clock that stamped the lease — worker-clock skew cannot
  move it.

Pre-existing invariant blind spot (documented for a follow-up, unchanged
by this round): the enforced `lock_lease >= 4 × heartbeat_interval`
sizes the cascade as `(F+1) × interval` and does not bound
`command_timeout`'s contribution at all; the enforced budget shrinks
the real exposure but the validator could tighten it.

## Measurement

Deterministic rowcount protocol (the statement's own behaviour; no wall
clock): N running rows held by one worker, all freshly leased, 30 beats
at the 10 s default cadence; each beat ages the rows' leases to their
simulated remaining (an equal simulation overhead on both sides), then
executes the renewal statement — OLD = the unconditional template, NEW
= the gated template with the shipped threshold for that lease. Engine:
the test suite's tuned `postgres:18-alpine` container.

| lease | threshold | held rows | side | rows rewritten over 300 s | whole-run WAL bytes |
|---|---|---|---|---|---|
| 60 s (defaults) | 56 s | 8 | OLD | 240 | not measurable in this container (0 both sides) |
| 60 s (defaults) | 56 s | 8 | NEW | **240** (inert) | — |
| 60 s (defaults) | 56 s | 200 | OLD | 6,000 | 12,357,472 |
| 60 s (defaults) | 56 s | 200 | NEW | **6,000** (inert) | 12,925,528 |
| 120 s | 60 s | 8 | OLD | 240 | not measurable (0 both sides) |
| 120 s | 60 s | 8 | NEW | **40** | — |
| 120 s | 60 s | 200 | OLD | 6,000 | 12,566,432 |
| 120 s | 60 s | 200 | NEW | **1,000** | **7,217,448** |

- **At the default lease the gate is inert by design** — identical
  rewrite counts, main-identical cadence — which is what closes the
  reproduced lapse window (nothing is ever skipped), and the enforced
  budget is the round's default-config win (the cascade bound above).
- **At a 120 s lease the gate delivers ~6×** (a 60 s renewal cadence:
  5 renewals over 30 beats vs 30), with the WAL delta matching the
  rewrite count at ~892 bytes per saved rewrite — the per-row cost of
  the non-HOT update's new index entries across the ~7 indexes a
  running row satisfies. At 200 running rows per pod that is ~18 KB/s
  of WAL and index churn per pod that stops being written.
- The per-renewal statement is byte-identical work when it does renew,
  so renewal-attributed WAL and index churn scale with the rewrite
  count by construction.

## The `fillfactor` alternative — considered, not taken

`ALTER TABLE jobs SET (fillfactor = N)` would let a larger share of
renewal updates become HOT, but it **rewrites the whole `jobs` table**
under an ACCESS EXCLUSIVE lock — a maintenance-window operation at best
on a live fleet, interacting with the sibling `migrations` worktree's
lock-scope work (#243/#250). The threshold gate delivers the write-rate
cut with no schema change and no lock; `fillfactor` remains a
complementary follow-up (its own migration file, an ops note, likely
opt-in) for fleets that want HOT renewals for the rows that still renew
every beat (the `heartbeat_timeout` population). Decision: not taken
here; the trade is documented for the maintainers.
