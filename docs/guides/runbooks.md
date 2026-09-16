# Alert Runbooks

One section per TaskQ Prometheus alert, in the shape an on-call reader needs
first: what fired, how to confirm it against the database (not just the
metric), how to remediate, and — for the backlog alerts — how to tell
*under-provisioned* (add workers) from *stalled* (fix the sweep/database;
scaling out makes it worse).

The shipped alert rules live in
[`src/taskq/contrib/prometheus/rules.yaml`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/src/taskq/contrib/prometheus/rules.yaml)
(plain) and
[`src/taskq/contrib/kubernetes/prometheus_rule.yaml`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/src/taskq/contrib/kubernetes/prometheus_rule.yaml)
(PrometheusRule CRD); the two carry identical alerts.

SQL examples assume the default schema (`TASKQ_SCHEMA_NAME=taskq`); substitute
your schema. Timestamps in the jobs table are written by the database clock,
which is why every age check below uses `clock_timestamp()` server-side
instead of subtracting in your own clock domain.

---

## TaskQScheduledBacklogGrowing

**What fired.** `taskq_jobs_oldest_due_age_seconds > 300 and taskq_jobs_scheduled_count > (taskq_jobs_scheduled_count offset 5m)` for 5 minutes: the oldest due job has been waiting more than 5 minutes AND the `scheduled` job count is HIGHER than it was 5 minutes ago. Together these mean promotion from `scheduled` to `pending` is not keeping up with arrivals — not merely behind on one slow-to-clear job.

An earlier form of this alert joined the oldest-due-age gauge against its own value 5 minutes back. That self-join is satisfied by a perfectly healthy, steadily draining backlog for the entire time its current straggler waits its turn — the age of "whichever job is currently oldest" climbs monotonically right up until that one job is promoted, regardless of how healthily everything behind it drains — so the join degenerated to a bare `age > 300` threshold and paged on healthy operation. Count, not the single oldest item's age, is what distinguishes "stalled" from "one slow straggler": a `scheduled` count that is flat or falling while jobs promote on schedule is healthy no matter how long the current straggler has waited.

`taskq_jobs_scheduled_count` is a label-less twin of `taskq_jobs_by_status{status="scheduled"}`, sampled by the same leader tick — it exists so the two `and` operands carry identical (empty) label sets. Prometheus pairs the two sides of a vector `and` (or comparison) only when their label sets are identical, with no `on`/`ignoring` modifier here to reconcile a mismatch, and it reports a non-matching join as an empty result rather than an error — so a version of this alert that joined the per-`status` depth gauge directly against the label-less age gauge could never fire, however bad the stall.

**How to confirm.**

- Metrics: `taskq_jobs_oldest_due_age_seconds` climbing; `taskq_jobs_scheduled_count` (equivalently `taskq_jobs_by_status{status="scheduled"}`) rising while `{status="pending"}` is flat.
- SQL — jobs that are due for promotion right now:

  ```sql
  SELECT count(*) FROM taskq.jobs
  WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp();
  ```

  A persistently non-zero count on the leader is the confirmation; a healthy
  promotion sweep drives it to zero (or near zero) every second.

**How to remediate.** This alert is the derivative *and* the age: depth alone
can be a legitimate burst, but due jobs aging past 5 minutes is not. Follow
the triage below before touching replica counts.

**Under-provisioned or stalled?**

- *Under-provisioned* → **add workers**: queue depth high,
  `taskq_jobs_oldest_due_age_seconds` low or falling, and
  `taskq_maintenance_leader_sweep_last_success_seconds{sweep_name="scheduled_to_pending"}`
  fresh (moving every second). Dispatch capacity is the bottleneck; scale out.
- *Stalled* → **fix the sweep/database, do NOT scale out**:
  `taskq_jobs_oldest_due_age_seconds` climbing while the sweep's
  `last_success` stamp is stale. No amount of extra workers promotes a
  scheduled job — only the leader's sweep does. Scaling a stalled engine adds
  database load without adding progress. See
  [TaskQPromotionStalled](#taskqpromotionstalled) and continue there.

---

## TaskQPromotionStalled

**What fired.** `time() - taskq_maintenance_leader_sweep_last_success_seconds{sweep_name="scheduled_to_pending"} > 120` for 2 minutes: the `scheduled_to_pending` sweep has not completed in 2 minutes. The sweep ticks every second; three missing `sweep_interval`s is a stall, not slowness. This is the same signature as the 12,732-job incident, where a second schema in one database silently held the maintenance lock and promotion stopped while everything else looked green.

**How to confirm.**

- Metric: the `last_success` stamp for `sweep_name="scheduled_to_pending"` frozen while the process is up; `taskq_jobs_by_status{status="scheduled"}` climbing.
- SQL — due jobs accumulating:

  ```sql
  SELECT count(*), min(scheduled_at) FROM taskq.jobs
  WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp();
  ```

- SQL — is anything holding this schema's maintenance advisory lock? The
  lock key is schema-qualified
  (`hashtextextended('taskq:maintenance_leader:<schema>', 0)`, built by
  `taskq.constants.schema_lock_name`), so it appears with `classid = 0`:

  ```sql
  SELECT pid, locktype, classid, objid, granted
  FROM pg_locks
  WHERE locktype = 'advisory' AND classid = 0;
  ```

  A `granted = false` waiter with an old `pid`, or a holder whose `pid`
  maps to a session of this schema that should not be leader (a stuck
  partitioned holder), is the finding. The classic *second-schema* cause —
  another schema's deployment silently holding the one shared lock — is
  fixed by the schema-qualified names (see
  [TaskQLeaderLockContention](#taskqleaderlockcontention)); a holder from
  another schema now takes a different key and cannot stall this one.

**How to remediate.**

1. Check `TaskQSweepTimeouts` and `TaskQSweepDegraded` — if either is firing,
   the database cannot finish the sweep's batches and that is the root cause.
2. Check `{schema}.maintenance_leader.expires_at`. A lease that has lapsed
   and that no pod takes over means the survivors cannot reach or write that
   table — check their `election-attempt-failed` logs and the application
   role's grants on it. Recovering the role needs nothing else: no privilege
   over other sessions, and no manual intervention in the database.
3. If no lock contention and no timeouts: check leader health
   (`sum(taskq_maintenance_leader_is_leader) == 1` — the
   `TaskQLeaderSplitBrainOrNoLeader` alert covers the zero-leader case) and
   Postgres connectivity/latency from the leader pod.
4. After the cause is fixed, confirm recovery: the `last_success` stamp moves
   again and the due-jobs count from the SQL above drains to zero.

**Do not scale out.** Promotion is a leader-side sweep, not worker capacity.
Extra workers consume `pending` jobs faster but promote nothing.

---

## TaskQSweepTimeouts

**What fired.** `rate(taskq_maintenance_leader_sweep_timeouts_total[5m]) > 0` for 5 minutes: sweep batches are being aborted by deadlines (`TimeoutError` on the client) or server-side cancels (`QueryCanceledError` from `statement_timeout`). The database cannot finish bounded batches.

**How to confirm.**

- Metric: `taskq_maintenance_leader_sweep_timeouts_total` rising, labeled by `sweep_name`; `TaskQSweepDegraded` often follows once the batch-size breaker latches.
- SQL — what the sweeping session is doing when it dies (run while the rate is non-zero):

  ```sql
  SELECT pid, state, wait_event_type, wait_event, now() - query_start AS age,
         left(query, 120) AS query_head
  FROM pg_stat_activity
  WHERE query ILIKE '%job_events%' OR query ILIKE '%taskq%';
  ```

  Long `age` with `wait_event_type = 'Lock'` points at a lock pile-up;
  `age` near `TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS` with no wait event
  points at plain slowness (I/O, bloat, plan regression).

**How to remediate.**

1. Identify the wait from the SQL above. Lock pile-up: find and clear the
   blocking session (`pg_blocking_pids()`). Slowness: check table bloat and
   indexes on the tables the sweep writes (`job_events`, `jobs`).
2. If the database is genuinely slower than the batch budget, lower
   `TASKQ_EVENT_WRITER_BATCH_SIZE` (each batch gets smaller; the sweep drains
   the remainder across more batches) rather than raising
   `TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS` past the
   `reclaim_event_visibility_delay` margin — the margin is what keeps
   out-of-commit-order reclaim events from being silently missed.
3. Remember the reduced tier is a *degradation ceiling*, not a fix: if
   `TaskQSweepDegraded` is firing alongside this alert, the worker has
   already latched to reduced batches and the database problem is still there.

---

## TaskQSweepDegraded

**What fired.** `taskq_maintenance_leader_sweep_batch_size < taskq_maintenance_leader_sweep_batch_size_configured` (for 0m — page immediately): a sweep is running at the reduced batch tier. The worker itself is reporting an unhealthy database — the batch-size breaker only latches after repeated batch cancellations, and it does not unlatch for the rest of the process lifetime. Both series are emitted by the same worker under the same `sweep_name` label, so the comparison always tracks that worker's own `TASKQ_EVENT_WRITER_BATCH_SIZE` configuration — no threshold to maintain.

**How to confirm.**

- Metric: `taskq_maintenance_leader_sweep_batch_size` per `sweep_name` below the same worker's `taskq_maintenance_leader_sweep_batch_size_configured` (the configured `TASKQ_EVENT_WRITER_BATCH_SIZE`).
- SQL — the sweep is still making progress, just slower:

  ```sql
  SELECT count(*) FROM taskq.jobs
  WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp();
  ```

  Combined with `TaskQSweepTimeouts` history: the breaker latched because
  batches kept being cancelled within `TASKQ_SWEEP_BREAKER_WINDOW_SECS`.

**How to remediate.**

1. Treat this as the worker's own verdict on the database: find and fix the
   underlying slowness (see [TaskQSweepTimeouts](#taskqsweeptimeouts)).
2. Restart the worker (or let the orchestrator recycle it) after the database
   is healthy again — the breaker does not unlatch, so the reduced tier
   persists until a fresh process.

---

## TaskQLeaderLockContention

**What fired.** `sum(rate(taskq_leader_lock_contention_total[10m])) > 0 and sum(taskq_maintenance_leader_is_leader) < 1` sustained for 10 minutes: maintenance-lock acquisitions are being lost AND no worker holds leadership. The counter is recorded by the *losing* side at every maintenance acquisition point, labeled by `lock`.

The leader-count operand is what makes this alertable at all. A healthy multi-worker fleet has exactly one winner per election round and every other worker records a loss, so a lost-acquire rate above zero is the **normal steady state** of any fleet larger than one worker — an alert on that rate alone pages on health and trains operators to silence it. Losses while the leader count has fallen below one is the genuine signature: nobody ever wins. Both operands are summed to fleet-wide scalars so the vector join pairs; an `and` between series carrying different label sets never matches, and Prometheus reports that as an empty result rather than an error. The lock key is schema-qualified — `taskq:maintenance_leader:<schema>`, built by `taskq.constants.schema_lock_name` — so contention is always between sessions of the **same schema**: a second schema in the same database takes a different key and cannot starve this one (the cross-schema silent starvation the unqualified lock name allowed is fixed). What sustained contention on a schema-qualified lock means: same-schema double-election attempts that keep losing (two pods of this deployment racing each other every heartbeat), or a stuck/long-held session sitting on this schema's key.

**How to confirm.**

- Metric: `taskq_leader_lock_contention_total` rising, labeled by `lock` — confirm the label is your schema's `taskq:maintenance_leader:<schema>`. Read it beside `sum(taskq_maintenance_leader_is_leader)`: a rising rate with that sum at 1 is an ordinary fleet electing a leader, not an incident. A rate that equals the election attempt rate (`taskq_leader_election_attempts_total`) fleet-wide, with the sum at 0, means no worker ever wins.
- SQL — advisory lock holders and waiters:

  ```sql
  SELECT l.pid, l.classid, l.objid, l.granted, a.application_name,
         now() - a.backend_start AS session_age
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.locktype = 'advisory';
  ```

  The lock is a single bigint key (`hashtextextended('taskq:maintenance_leader:<schema>', 0)`), so it appears with `classid = 0`; match `objid` against `SELECT hashtextextended('taskq:maintenance_leader:<your-schema>', 0)` to pick this deployment's row out of the set. Contention is recorded once per distinct holder a pod finds in its way, so a stable fleet — however many followers it has — records nothing after it settles, and a handover records one event per pod. A sustained rate therefore means the observed holder keeps changing (handover churn), or that this pod cannot even read the lease row to observe a stable holder — its probe fails every cycle; check its `leader-retry` lines and connectivity.

**How to remediate.**

1. Identify the winning session from the SQL above. Because the key is schema-qualified, a holder belonging to another schema or deployment sharing this database is **not** the cause — it holds a different key. The holder is a session of this same schema.
2. Cross-check it against `{schema}.maintenance_leader`. A pod that follows a
   live lease records no contention at all, and a pod that finds the row
   absent wins it outright on its next cycle, so a sustained rate means the
   counter's remaining shapes: the row's holder keeps changing (handover
   churn), or the counting pod cannot read the row at all. A courtesy lock
   held by a session no lease row accounts for never produces it: the lease
   is what confers the role, so nothing waits on that session — but a lock
   nobody owns still points at a pod that elected under an older release,
   or at a session one left behind.
3. **Upgrade discipline:** the schema-qualified names replaced the
   unqualified (`taskq:maintenance_leader`) ones outright, and the two are
   NOT overlap-compatible — a mixed old/new fleet holds different keys, so
   old and new releases can both act as leader of the same schema at once
   (sweeps stay row-safe under `FOR UPDATE SKIP LOCKED`; cron gains a
   double-fire window because its lock is what serialises ticks). Adopt a
   release that changes lock names by restarting the fleet onto it, not by
   rolling it — the window is the deploy, not the steady state (see
   `taskq.constants.schema_lock_name`).
4. Confirm recovery: `taskq_leader_lock_contention_total` stops rising and
   `sum(taskq_maintenance_leader_is_leader) == 1` again
   (see `TaskQLeaderSplitBrainOrNoLeader`).

---

## TaskQRateLimitDependencyOutage

**What fired.** `rate(taskq_ratelimit_acquire_dependency_failures_total[5m]) > 0` for 5 minutes: rate-limit acquires are failing because the limiter's store — Redis, or the PG fallback behind it — could not answer, and the worker failed the acquire closed as a denial. Every rate-limited dispatch is snoozing while the outage lasts: no work is lost, but nothing rate-limited moves either, and the queue looks calm while it piles up behind the limiter.

**How to confirm.**

- Metric: `taskq_ratelimit_acquire_dependency_failures_total` rising, labeled by `error_type` (the exception class name). Read it beside `taskq_reservation_denials_total{source="rate_limit"}`: denials with this counter flat are ordinary contention; denials with this counter rising are an outage masquerading as contention — the two must be told apart before anyone scales a bucket.
- The `error_type` label names the failure class (a Redis connection error, a timeout): it distinguishes "the store is unreachable" from "the store is slow".
- Check the store from a worker pod, not from your laptop: the outage is between the worker's network position and the store (DNS, NetworkPolicy, the store itself).

**How to remediate.**

1. Restore the store dependency: Redis connectivity from the worker pods first (the common cause), then the store itself. The PG fallback fails the same closed way when Postgres is the sick dependency — check `TaskQSweepTimeouts` / `TaskQDispatchLatencyHigh` before touching Redis.
2. Do NOT raise bucket limits or disable rate limiting during the outage: the denials are the limiter failing closed (the configured safe behavior), and widening limits cannot create store capacity.
3. Snoozed dispatches retry on their own once acquires succeed again — confirm recovery by watching `taskq_ratelimit_acquire_dependency_failures_total` flatten and the snoozed backlog drain (`taskq_jobs_by_status{status="scheduled"}` falling).

---

## TaskQCronLockContention

**What fired.** `rate(taskq_cron_lock_contention_total[10m]) > 0` for 10 minutes: cron ticks are returning without firing because another session holds the cron advisory lock, sustained. A brief low rate is the benign leader-handover overlap; a rate sustained at the tick cadence means cron is not running anywhere — the lock is transaction-scoped and releases on COMMIT/ROLLBACK, which never happens if the holding session was partitioned without a FIN. That is the fleet-wide cron stall: every schedule silently stops firing, and the signals an operator would check first (`taskq_cron_disabled_schedules`, `taskq_cron_consecutive_failures`) deliberately stay still in that mode.

**How to confirm.**

- Metric: `taskq_cron_lock_contention_total` rising at roughly the tick rate (one contention per tick attempt) — not brief bursts. `taskq_cron_disabled_schedules` staying 0 while no `cron fired` lines appear is the same stall seen from the other side.
- SQL — the cron lock holder (the lock name is `taskq:cron:<schema>`, a single bigint key via `hashtextextended`, so it appears with `classid = 0`):

  ```sql
  SELECT l.pid, l.granted, a.state, a.wait_event_type,
         now() - a.backend_start AS session_age,
         left(a.query, 120) AS query_head
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.locktype = 'advisory' AND l.classid = 0
    AND l.objid = (SELECT hashtextextended('taskq:cron:<your-schema>', 0));
  ```

  A long-lived `granted` session whose `session_age` far exceeds worker liveness (partitioned without a FIN) is the finding; a healthy leader's hold is transaction-scoped and momentary.

**How to remediate.**

1. Terminate the stale holder if it is dead weight
   (`SELECT pg_terminate_backend(<pid>);`) — the server reaps partitioned sessions only on its `tcp_keepalives_*` schedule, which can outlast a maintenance window.
2. If the holder is a healthy worker of THIS schema: two pods both running cron loops against the same schema points at a deployment/election misconfiguration — one cron loop per schema is the contract; fix the deployment, do not kill the session.
3. Confirm recovery: `taskq_cron_lock_contention_total` stops rising, `cron fired` lines resume, and missed schedules catch up (due schedules fire immediately once the lock is free — cron does not skip missed ticks by default; see the cron guide for `TASKQ_CRON_TICK_LIMIT` if the backlog is large).

---

## TaskQRunningLeaseExpired

**What fired.** `taskq_jobs_running_lease_expired > 0` for 5 minutes: running jobs whose lock lease is past expiry, sustained. A healthy fleet reads 0 — the leader's reclaim sweep (`sweep_name="expired_locks"`) drains expired leases within a tick or two of expiry — so a sustained non-zero count means reclaim is not draining. Work is claimed and stuck in `running` while health probes stay green: the zombie-running shape.

**How to confirm.**

- Metric: `taskq_jobs_running_lease_expired` (sampled by every worker, so one flapping series is a sampling artifact — the alert fires on the sustained value). Cross-check the reclaim sweep's health: `taskq_maintenance_leader_sweep_last_success_seconds{sweep_name="expired_locks"}` fresh means the sweep runs but rows regrow faster than it drains (workers dying or wedging mid-run); a stale stamp means the sweep itself is stopped (see [TaskQPromotionStalled](#taskqpromotionstalled) — the same signature, different sweep).
- SQL — the zombies and their holders:

  ```sql
  SELECT id, actor, locked_by_worker, lock_expires_at,
         now() - lock_expires_at AS overdue_by, attempt, max_attempts
  FROM taskq.jobs
  WHERE status = 'running' AND lock_expires_at < clock_timestamp()
  ORDER BY lock_expires_at;
  ```

- The admin `/jobs` page renders the same state per row (Lease column): a red `expired` badge with the holding worker.

**How to remediate.**

1. If `TaskQHeartbeatMisses` is firing or the holders' workers are gone: the jobs self-heal — the reclaim sweep transitions them (retryable → `pending` after a backoff; exhausted → `crashed`). The alert's value is that it stays non-zero when that does NOT happen.
2. If the reclaim sweep is stalled or timing out: follow [TaskQPromotionStalled](#taskqpromotionstalled) and [TaskQSweepTimeouts](#taskqsweeptimeouts) — fix the sweep/database; the zombies are a symptom.
3. If the sweep is healthy and the count still regrows: jobs are repeatedly outliving their lease — the lease (`lock_lease`-shaped settings) is shorter than the actor's real run time and heartbeats are not renewing fast enough. Check `TaskQLockExpiringSoon` (firing means renewals are barely keeping ahead) and widen the lease/heartbeat budget for those actors; do not restart workers to "clear" the gauge — the same jobs will zombie again.
4. Confirm recovery: `taskq_jobs_running_lease_expired` returns to 0 and stays there across several sweep intervals.

---

## Related documentation

- [Observability](observability.md) — the metrics these alerts evaluate,
  including the sweep sample-population rule.
- [Configuration](configuration.md) — `TASKQ_EVENT_WRITER_*`,
  `TASKQ_SWEEP_*` and `TASKQ_CRON_TICK_LIMIT` knobs referenced above.
- [Troubleshooting](troubleshooting.md) — symptom-first diagnosis paths.
