# Threshold-gated lease renewal — design math and before/after measurement

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

## Why the naive "skip while more than half the lease remains" is unsafe

Two independent red-team findings, both pinned by tests:

1. **It falsely crash-reclaims healthy `heartbeat_timeout` jobs.** The
   reclaim sweep's heartbeat arm (`_SWEEP_1_SQL`) reclaims a row whose
   `last_heartbeat_at + heartbeat_timeout < now` while the lease is
   still valid. Those rows' beats must stay per-tick fresh; skipping
   their renewal while the lease is fresh starves `last_heartbeat_at`
   and the sweep reclaims a healthy job. The gated statement renews
   every `heartbeat_timeout IS NOT NULL` row on every beat (their
   `last_heartbeat_at` update is non-HOT anyway through the
   heartbeat-deadline partial index, so folding the lease extension in
   costs nothing extra for them). Pinned by
   `tests/test_heartbeat_integration.py::test_threshold_gated_renewal_selects_rows_by_remaining_lease`.

2. **It lapses the lease before the isolate decision at the default
   settings.** Defaults are lease 60 s, interval 10 s, 3 tolerated
   failures (isolate on the 4th). The worst coherent beat gap is
   `heartbeat_interval + heartbeat_command_timeout` (a pool acquire
   bounded at the interval plus one command bounded at the command
   timeout) = 12 s. A skip at remaining 30 s + ε followed by four
   failed beats consumes 48 s: the lease is gone ~18 s BEFORE the loop
   isolates, and the leader's sweep — on healthy Postgres, while this
   worker is merely partitioned — can reclaim rows the worker still
   holds and may still be running. Pinned (with the shipped floor
   holding the same cascade) by
   `tests/test_heartbeat.py::test_naive_half_lease_threshold_lapses_before_isolation_at_defaults`
   and the property test
   `test_gated_renewal_never_lets_a_live_lease_lapse`.

## The shipped threshold

```
threshold = max(lock_lease / 2,
                (max_heartbeat_failures + 1)
                  * (heartbeat_interval + heartbeat_command_timeout))
```

Sizing proof (the property test executes it over randomized adversarial
timelines):

- **Healthy beats**: a skip happens only while remaining > threshold,
  so the next beat's remaining is > `threshold - worst_gap` ≥
  `F * worst_gap` ≥ one full worst gap — a healthy-but-slow worker never
  lets a lease lapse.
- **The failure cascade**: the worst case is a skip at
  `threshold + ε` followed by `F+1` failed beats (the loop isolates on
  the F+1-th), each gap strictly under one worst gap (a tick that
  consumed its full acquire budget waits zero). The lease at the isolate
  decision is then > 0 by construction; a worker that recovers after F
  failures still holds more than one worst gap of lease.
- **Degenerate configs self-protect**: whenever the floor meets or
  exceeds the lease (the 4× invariant's minimum lease, fast heartbeats
  with a large command timeout, failure-tolerant fleets), the gate
  renews on every beat — exactly the unconditional behaviour, because
  there is no slack that is safe to harvest. At the enforced minimum
  (`lock_lease = 4 * heartbeat_interval`) the unconditional renewal has
  always had zero cascade margin; this sizing refuses to make it
  negative.
- **Clock-skew immunity**: the comparison is server-side
  (`lock_expires_at <= clock_timestamp() + $4`), on the same clock that
  stamped the lease — a worker-clock skew cannot move the threshold the
  way a client-side remaining-lease computation would.

Honest margin statement: in every config whose lease can survive its
own worst cascade, the gated policy keeps it surviving — but with less
headroom than the unconditional renewal (unconditional margin
`lease - (F+1)*worst_gap`; gated margin `max(ε, lease/2 -
(F+1)*worst_gap)` when the half-lease arm binds). That headroom is the
price of the halved write rate; at the defaults it is 2 s beyond the
exact cascade bound (`L - h = 50 s` vs threshold 48 s).

Pre-existing finding (not introduced here, worth a follow-up): the
enforced `lock_lease >= 4 * heartbeat_interval` invariant sizes the
failure cascade as `(F+1) * heartbeat_interval`, but a failed beat can
cost `heartbeat_interval + heartbeat_command_timeout` — with a
non-trivial command timeout the minimum lease cannot survive the worst
cascade even on main. The property test pins the gated policy's
degenerate-equivalence guarantee in that regime; a follow-up could
tighten the validator.

## Measurement

Deterministic rowcount protocol (the statement's own behaviour; no wall
clock): N running rows held by one worker, all freshly leased, 30 beats
at the default 10 s cadence; each beat ages the rows' leases to their
simulated remaining (an equal simulation overhead on both sides), then
executes the renewal statement — OLD = the unconditional template,
NEW = the gated template with the shipped default threshold (48 s).
Engine: the test suite's tuned `postgres:18-alpine` container.

| held rows | side | rows rewritten over 300 s | whole-run WAL bytes |
|---|---|---|---|
| 8 (cennan-scale) | OLD | 240 | not measurable in this container (0 both sides) |
| 8 | NEW | **120** | — |
| 200 (TAStack pod-scale) | OLD | 6,000 | 12,509,312 |
| 200 | NEW | **3,000** | **9,540,344** |

- **Exactly 2.0× fewer renewal rewrites at the default settings** — the
  "halves+" target: after a renewal (60 s remaining) the next beat
  carries 50 s (above the 48 s threshold, skipped), the one after 40 s
  (renewed) — a 20 s renewal cadence instead of 10 s. Larger
  `lock_lease`, lower `max_heartbeat_failures`, or a shorter command
  timeout all widen the gap further (the half-lease arm: lease 300 s →
  150 s threshold → a ~150 s cadence, ~15×).
- **The WAL delta matches the rewrite count exactly**: 2.97 MB saved
  for 3,000 saved rewrites ≈ 989 bytes per rewrite — the per-row cost
  of a non-HOT update's new index entries across the ~7 indexes a
  running row satisfies. At 200 running rows per pod that is ~10 KB/s
  of WAL and index churn per pod that stops being written, before
  counting the bloat-pressure relief on `jobs` (the pages a fleet's
  renewals keep dirty starve autovacuum of truncation headroom).
- The per-renewal statement is byte-identical work when it does renew
  (same SET/WHERE core; only the selection predicate differs), so
  renewal-attributed WAL and index churn scale with the rewrite count
  by construction.

## The `fillfactor` alternative — considered, not taken

`ALTER TABLE jobs SET (fillfactor = N)` would let a larger share of
renewal updates become HOT (no new index entries at all when the update
fits the page), but it **rewrites the whole `jobs` table** under an
ACCESS EXCLUSIVE lock — on the hot table of a live fleet that is a
maintenance-window operation at best, and it interacts with the
sibling `migrations` worktree's lock-scope work (#243/#250 restructures
the same table's index migrations). The threshold gate delivers the
write-rate halving with no schema change and no lock; `fillfactor`
remains available as a complementary follow-up (its own migration file,
an ops note, and — given the rewrite — likely opt-in) for fleets that
want HOT renewals for the rows that still renew every beat (the
`heartbeat_timeout` population). Decision: not taken here; the trade is
documented for the maintainers.
