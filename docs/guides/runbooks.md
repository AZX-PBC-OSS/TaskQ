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

**What fired.** `deriv(taskq_jobs_by_status{status="scheduled"}[15m]) > 0 and taskq_jobs_oldest_due_age_seconds > 300` for 5 minutes: the scheduled backlog is growing AND the oldest due job has been waiting more than 5 minutes. Together these mean promotion from `scheduled` to `pending` is not keeping up — or not happening at all.

**How to confirm.**

- Metrics: `taskq_jobs_by_status{status="scheduled"}` rising while `{status="pending"}` is flat; `taskq_jobs_oldest_due_age_seconds` climbing.
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
2. If a stuck session of this schema holds the advisory lock (a partitioned
   former leader that never released): terminate that session
   (`SELECT pg_terminate_backend(<pid>);`). Schemas in one database now
   always use distinct lock keys — the keys are schema-qualified — so a
   foreign-schema holder is no longer a possible cause.
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

**What fired.** `rate(taskq_leader_lock_contention_total[10m]) > 0` sustained for 10 minutes: maintenance-lock acquisitions are being lost to another session. The counter is recorded by the *losing* side at every maintenance acquisition point, labeled by `lock`. The lock key is schema-qualified — `taskq:maintenance_leader:<schema>`, built by `taskq.constants.schema_lock_name` — so contention is always between sessions of the **same schema**: a second schema in the same database takes a different key and cannot starve this one (the cross-schema silent starvation the unqualified lock name allowed is fixed). What sustained contention on a schema-qualified lock means: same-schema double-election attempts that keep losing (two pods of this deployment racing each other every heartbeat), or a stuck/long-held session sitting on this schema's key.

**How to confirm.**

- Metric: `taskq_leader_lock_contention_total` rising, labeled by `lock` — confirm the label is your schema's `taskq:maintenance_leader:<schema>`. If the rate equals the election attempt rate (`taskq_leader_election_attempts_total`), this worker never wins.
- SQL — advisory lock holders and waiters:

  ```sql
  SELECT l.pid, l.classid, l.objid, l.granted, a.application_name,
         now() - a.backend_start AS session_age
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.locktype = 'advisory';
  ```

  The lock is a single bigint key (`hashtextextended('taskq:maintenance_leader:<schema>', 0)`), so it appears with `classid = 0`; match `objid` against `SELECT hashtextextended('taskq:maintenance_leader:<your-schema>', 0)` to pick this deployment's row out of the set. Healthy contention — leader handover overlap between pods of the *same* deployment — is brief and intermittent, which is why the alert requires a sustained rate. A long-lived `granted` session, or a holder whose `pid` maps to a session with `session_age` far beyond heartbeat liveness (partitioned without a FIN), is the finding.

**How to remediate.**

1. Identify the winning session from the SQL above. Because the key is schema-qualified, a holder belonging to another schema or deployment sharing this database is **not** the cause — it holds a different key. The holder is a session of this same schema: either the legitimately-elected leader during handover (benign, and filtered out by the sustained-rate alert) or a stuck/dead one.
2. Terminate the stale holder if it is dead weight
   (`SELECT pg_terminate_backend(<pid>);`). The maintenance lock is
   session-scoped, and a partitioned session that never sends a FIN holds
   it until the server reaps the backend — bounded by the server's
   `tcp_keepalives_*` settings (minutes, not forever).
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

## Related documentation

- [Observability](observability.md) — the metrics these alerts evaluate,
  including the sweep sample-population rule.
- [Configuration](configuration.md) — `TASKQ_EVENT_WRITER_*`,
  `TASKQ_SWEEP_*` and `TASKQ_CRON_TICK_LIMIT` knobs referenced above.
- [Troubleshooting](troubleshooting.md) — symptom-first diagnosis paths.
