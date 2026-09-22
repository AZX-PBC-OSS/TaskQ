# Alert Runbooks

One section per TaskQ Prometheus alert, in the shape an on-call reader needs
first: what fired, how to confirm it against the database (not just the
metric), how to remediate, and (for the backlog alerts) how to tell
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

## TaskQQueueDepthHigh

**What fired.** `max by (actor, queue) (taskq_jobs_oldest_pending_age_seconds) > 900` for 5 minutes: the oldest PENDING job of some (actor, queue) pair has been eligible for more than 15 minutes, sustained. The gauge is attributed per actor and per queue on purpose, not per queue: in a multi-worker fleet no worker refuses to start because an actor's declared queue has no consumer (no supervisor can know what consumes a queue), so a misrouted actor produces no refusal, no error and no failed job, and its rows pile up pending while every probe stays green. Age rather than depth, because a deep queue that drains is healthy throughput.

**How to confirm.**

- Metric: `taskq_jobs_oldest_pending_age_seconds{actor="...", queue="..."}` climbing while the same queue's other actors stay flat is the misroute-or-starvation shape; the whole queue rising together is shared capacity. Read `taskq_queue_live_workers{queue="..."}` beside it: a zero there is the never-consumed case, which [TaskQQueueUnserved](#taskqqueueunserved) owns.
- SQL: the pending head of every (actor, queue) pair right now:

  ```sql
  SELECT actor, queue, count(*) AS depth,
         extract(epoch FROM (clock_timestamp() - min(scheduled_at)))::int AS oldest_age_s
  FROM taskq.jobs
  WHERE status = 'pending' AND scheduled_at <= clock_timestamp()
  GROUP BY actor, queue
  ORDER BY oldest_age_s DESC;
  ```

- The gauge is fed by the leader's `actor_backlog` sampler. If the series went absent or frozen exactly when the incident got worst, the sampler read is what died; check [TaskQSweepTimeouts](#taskqsweeptimeouts) before trusting the (now silent) gauge.

**How to remediate.**

1. No live worker subscribes to the queue: add the queue to some worker's `TASKQ_QUEUES` and restart it, or run a worker that serves it. The pile dispatches on its own once a consumer exists; nothing was lost.
2. Live workers present and draining slowly: the queue is under-provisioned. Add workers or raise `TASKQ_MAX_CONCURRENCY` on the fleet that serves it.
3. One actor aging while its queue-mates drain: check for a damper holding its rows back, the actor's own `max_concurrent` or a queue cap from `taskq queues set-max-concurrent`; the alert shows the head's age, the cap is the cause.
4. Confirm recovery: the age falls under the threshold and the depth drains.

Do not clear the gauge by deleting or re-enqueueing the pending pile: the rows are eligible work that dispatches in order once capacity exists, and re-enqueueing duplicates them.

---

## TaskQScheduledBacklogGrowing

**What fired.** `taskq_jobs_oldest_due_age_seconds > 300 and (taskq_jobs_scheduled_count > (taskq_jobs_scheduled_count offset 5m) or changes(taskq_jobs_scheduled_count[5m]) == 0)` for 5 minutes: the oldest due job has been waiting more than 5 minutes AND the `scheduled` job count is demonstrably not draining: either it is HIGHER than it was 5 minutes ago (promotion from `scheduled` to `pending` is not keeping up with arrivals) or it has not moved at all across those 5 minutes (promotion has stopped and no arrivals are landing net, the stalled plateau). Either way, due work is waiting while the scheduled backlog fails to shrink, not merely one slow-to-clear job.

An earlier form of this alert joined the oldest-due-age gauge against its own value 5 minutes back. That self-join is satisfied by a perfectly healthy, steadily draining backlog for the entire time its current straggler waits its turn: the age of "whichever job is currently oldest" climbs monotonically right up until that one job is promoted, regardless of how healthily everything behind it drains, so the join degenerated to a bare `age > 300` threshold and paged on healthy operation. Count, not the single oldest item's age, is what distinguishes "stalled" from "one slow straggler": a `scheduled` count that is moving (falling as jobs promote, rising as arrivals land) is healthy flow no matter how long the current straggler has waited. What is never healthy is a due job aging past the threshold while the count never moves at all, and a strict growth comparison cannot see it: a flat count is never `>` itself 5 minutes back, so a promoter that has stopped completely while arrivals are absent (or throttled, or backpressured) would stay silent forever on the growth arm alone. That is the stalled plateau the `changes(...) == 0` arm catches.

`taskq_jobs_scheduled_count` is a label-less twin of `taskq_jobs_by_status{status="scheduled"}`, sampled by the same leader tick; it exists so every operand of the alert carries an identical (empty) label set. Prometheus pairs the two sides of a vector `and`/`or` (or comparison) only when their label sets are identical, with no `on`/`ignoring` modifier here to reconcile a mismatch, and it reports a non-matching join as an empty result rather than an error, so a version of this alert that joined the per-`status` depth gauge directly against the label-less age gauge could never fire, however bad the stall.

**How to confirm.**

- Metrics: `taskq_jobs_oldest_due_age_seconds` climbing; `taskq_jobs_scheduled_count` (equivalently `taskq_jobs_by_status{status="scheduled"}`) either rising while `{status="pending"}` is flat (outpaced by arrivals) or not moving at all (the stalled plateau).
- SQL: jobs that are due for promotion right now:

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
  `taskq_jobs_oldest_due_age_seconds` climbing while the scheduled count
  rises or never moves. The usual signature is the sweep's `last_success`
  stamp gone stale: promotion is not running at all. A FRESH stamp with a
  frozen count is the subtler shape: the sweep completes but promotes
  nothing, so check `TaskQSweepTimeouts` and `TaskQSweepDegraded` and
  confirm with the due-jobs SQL above. No amount of extra workers promotes a
  scheduled job; only the leader's sweep does. Scaling a stalled engine adds
  database load without adding progress. See
  [TaskQPromotionStalled](#taskqpromotionstalled) and continue there.

---

## TaskQPromotionStalled

**What fired.** `time() - taskq_maintenance_leader_sweep_last_success_seconds{sweep_name="scheduled_to_pending"} > 120` for 2 minutes: the `scheduled_to_pending` sweep has not completed in 2 minutes. The sweep ticks every second; three missing `sweep_interval`s is a stall, not slowness. This is the same signature as the 12,732-job incident, where a second schema in one database silently held the maintenance lock and promotion stopped while everything else looked green.

**How to confirm.**

- Metric: the `last_success` stamp for `sweep_name="scheduled_to_pending"` frozen while the process is up; `taskq_jobs_by_status{status="scheduled"}` climbing.
- SQL: due jobs accumulating:

  ```sql
  SELECT count(*), min(scheduled_at) FROM taskq.jobs
  WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp();
  ```

- SQL: is anything holding this schema's maintenance advisory lock? The
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
  partitioned holder), is the finding. The classic *second-schema* cause:
  another schema's deployment silently holding the one shared lock,
  fixed by the schema-qualified names (see
  [TaskQLeaderLockContention](#taskqleaderlockcontention)); a holder from
  another schema now takes a different key and cannot stall this one.

**How to remediate.**

1. Check `TaskQSweepTimeouts` and `TaskQSweepDegraded`; if either is firing,
   the database cannot finish the sweep's batches and that is the root cause.
2. Check `{schema}.maintenance_leader.expires_at`. A lease that has lapsed
   and that no pod takes over means the survivors cannot reach or write that
   table; check their `election-attempt-failed` logs and the application
   role's grants on it. Recovering the role needs nothing else: no privilege
   over other sessions, and no manual intervention in the database.
3. If no lock contention and no timeouts: check leader health
   (`sum(taskq_maintenance_leader_is_leader) == 1`; the
   `TaskQLeaderSplitBrainOrNoLeader` alert covers the zero-leader case) and
   Postgres connectivity/latency from the leader pod.
4. After the cause is fixed, confirm recovery: the `last_success` stamp moves
   again and the due-jobs count from the SQL above drains to zero.

**Do not scale out.** Promotion is a leader-side sweep, not worker capacity.
Extra workers consume `pending` jobs faster but promote nothing.

---

## TaskQSweepTimeouts

**What fired.** `rate(taskq_maintenance_leader_sweep_timeouts_total[5m]) > 0` for 5 minutes: sweep batches are being aborted by deadlines (`TimeoutError` on the client) or server-side cancels (`QueryCanceledError` from `statement_timeout`): the database cannot finish bounded batches, or a gauge sampler's read did not complete. The `sweep_name` label says which: sweep names are aborted batches; the sampler names (`queue_depth`, `backlog_detection`, `actor_backlog`, `reservation_slots`) are reads that did not happen, counted for every failure class; a dead sampler's gauges go stale or absent while nothing else names the loss, and the per-actor backlog read dying resolves `TaskQQueueDepthHigh` at the exact moment the incident it alerts on is killing the read, which is why that failure must land here.

**How to confirm.**

- Metric: `taskq_maintenance_leader_sweep_timeouts_total` rising, labeled by `sweep_name`; `TaskQSweepDegraded` often follows once the batch-size breaker latches. A sampler name rather than a batch name means a gauge read is failing; read that gauge beside the counter: an absent or frozen series while this rate rises is the dead-sampler signature, never a resolved alert.
- SQL: what the sweeping session is doing when it dies (run while the rate is non-zero):

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
   `reclaim_event_visibility_delay` margin; the margin is what keeps
   out-of-commit-order reclaim events from being silently missed.
3. Remember the reduced tier is a *degradation ceiling*, not a fix: if
   `TaskQSweepDegraded` is firing alongside this alert, the worker has
   already latched to reduced batches and the database problem is still there.

---

## TaskQSweepUnexpectedErrors

**What fired.** `rate(taskq_maintenance_leader_sweep_unexpected_errors_total[5m]) > 0` for 5 minutes: prune-family sweep batches (the `prune` and `archive_expiry` sweeps) are being aborted by an error outside the deadline family, a constraint violation, a connection reset, or any other fault that is not a statement cancel. `TaskQSweepTimeouts` covers the deadline family only; this counter is the rest, and without it a drain stopped by this class reads healthy on every success-path metric: the row counters and success stamps only move on success, so a prune that keeps failing looks like a quiet day.

**How to confirm.**

- Metric: `taskq_maintenance_leader_sweep_unexpected_errors_total` rising, labeled by `sweep_name` (`prune` or `archive_expiry`). The `prune-failed` / `archive-expiry-failed` error log lines carry the `error` repr next to each increment; the exception class is the diagnosis.
- Cadence: one increment per aborted batch, and the first aborted batch ends the sweep attempt, so the counter advances on the retry ladder (`TASKQ_SWEEP_BREAKER_*` backoff, 60s doubling to a 1800s cap) rather than per batch per second. A rate that stays positive over the 5m window is a persistent failure, not a busy drain.
- Latch: the batch-size breaker counts these failures the same as deadline failures, so `TaskQSweepDegraded` firing alongside means the worker has latched to the reduced tier and is still failing there.

**How to remediate.**

1. Read the exception class on the `prune-failed` log line. A `UniqueViolationError` against the archive tables usually means duplicate archive targets: check for a second writer inserting into `jobs_archive` (a sibling taskq deployment pointed at the same schema), or a partially-restored backup.
2. A connection-class error (`PostgresConnectionError`, `OSError`): treat as a database or network incident; the retry ladder resumes the drain when the connection is back.
3. Anything else: the error class is new, the reduced tier is the safety net that keeps the batches small while you look. If the sweep stays stopped, the retention backlog grows silently behind `TaskQSweepDegraded`; escalate before the backlog hits dispatch latency.

---

## TaskQSweepDegraded

**What fired.** `taskq_maintenance_leader_sweep_batch_size < taskq_maintenance_leader_sweep_batch_size_configured` (for 0m, page immediately): a sweep is running at the reduced batch tier. The worker itself is reporting an unhealthy database: the batch-size breaker only latches after repeated batch cancellations, and it does not unlatch for the rest of the process lifetime. Both series are emitted by the same worker under the same `sweep_name` label, so the comparison always tracks that worker's own `TASKQ_EVENT_WRITER_BATCH_SIZE` configuration; no threshold to maintain.

**How to confirm.**

- Metric: `taskq_maintenance_leader_sweep_batch_size` per `sweep_name` below the same worker's `taskq_maintenance_leader_sweep_batch_size_configured` (the configured `TASKQ_EVENT_WRITER_BATCH_SIZE`).
- SQL: the sweep is still making progress, just slower:

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
   is healthy again; the breaker does not unlatch, so the reduced tier
   persists until a fresh process.

---

## TaskQDispatchLatencyHigh

**What fired.** `histogram_quantile(0.99, rate(taskq_dispatch_duration_seconds_bucket[5m])) > 0.05` for 5 minutes, by `queue`: the dispatch query's p99 exceeds 50 ms. The histogram times the claim round trip's SQL execution only, not actor run time, so this is the database getting slow at handing out work. The threshold is set near where the claim's `FOR UPDATE SKIP LOCKED` plan stops being index-shaped: a healthy dispatch round is sub-millisecond, so a 50 ms p99 is a plan or contention problem, not a tuning nit.

**How to confirm.**

- Metric: `taskq_dispatch_duration_seconds` p99 by `queue`. One queue high while the rest are flat points at that queue's claim path; every queue high at once points at the database.
- SQL: what the dispatching sessions are waiting on while the alert fires:

  ```sql
  SELECT pid, state, wait_event_type, wait_event,
         now() - query_start AS age, left(query, 120) AS query_head
  FROM pg_stat_activity
  WHERE query ILIKE '%SKIP LOCKED%' OR query ILIKE '%taskq%';
  ```

  `wait_event_type = 'Lock'` with short ages is dispatcher-on-dispatcher contention; ages near the threshold with an I/O wait is heap or index pressure on `jobs`.
- Plan check: the claim reads the `pending` partial index (`jobs_dispatch_idx`). A bloated table or a dropped index shows as a sequential-scan plan; `EXPLAIN` the claim query and compare against the flat sub-millisecond expectation in `docs/design/sql-hotpath-followups.md`.

**How to remediate.**

1. Every queue slow: treat it as a database incident first. [TaskQSweepTimeouts](#taskqsweeptimeouts) and [TaskQRateLimitDependencyOutage](#taskqratelimitdependencyoutage) read the same Postgres; a latency problem there degrades dispatch with it, and fixing dispatch alone fixes nothing.
2. One queue slow: check the population the claim has to consider. A very deep `pending` backlog on that queue, a large `TASKQ_DISPATCH_OVERSAMPLE`, and table bloat all grow the candidate set the claim probes before it locks.
3. Confirm recovery: the p99 falls back under 50 ms.

Do not scale dispatchers out to fix this: more concurrent claim rounds multiply contention on the same rows and push the p99 up, not down. Capacity adds consumption rate; it does not make the claim faster.

---

## TaskQLeaderLockContention

**What fired.** `sum(rate(taskq_leader_lock_contention_total[10m])) > 0 and sum(taskq_maintenance_leader_is_leader) < 1` sustained for 10 minutes: maintenance-lock acquisitions are being lost AND no worker holds leadership. The counter is recorded by the *losing* side at every maintenance acquisition point, labeled by `lock`.

The leader-count operand is what makes this alertable at all. A healthy multi-worker fleet has exactly one winner per election round and every other worker records a loss, so a lost-acquire rate above zero is the **normal steady state** of any fleet larger than one worker; an alert on that rate alone pages on health and trains operators to silence it. Losses while the leader count has fallen below one is the genuine signature: nobody ever wins. Both operands are summed to fleet-wide scalars so the vector join pairs; an `and` between series carrying different label sets never matches, and Prometheus reports that as an empty result rather than an error. The lock key is schema-qualified (`taskq:maintenance_leader:<schema>`), built by `taskq.constants.schema_lock_name`, so contention is always between sessions of the **same schema**: a second schema in the same database takes a different key and cannot starve this one (the cross-schema silent starvation the unqualified lock name allowed is fixed). What sustained contention on a schema-qualified lock means: same-schema double-election attempts that keep losing (two pods of this deployment racing each other every heartbeat), or a stuck/long-held session sitting on this schema's key.

**How to confirm.**

- Metric: `taskq_leader_lock_contention_total` rising, labeled by `lock`; confirm the label is your schema's `taskq:maintenance_leader:<schema>`. Read it beside `sum(taskq_maintenance_leader_is_leader)`: a rising rate with that sum at 1 is an ordinary fleet electing a leader, not an incident. A rate that equals the election attempt rate (`taskq_leader_election_attempts_total`) fleet-wide, with the sum at 0, means no worker ever wins.
- SQL: advisory lock holders and waiters:

  ```sql
  SELECT l.pid, l.classid, l.objid, l.granted, a.application_name,
         now() - a.backend_start AS session_age
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.locktype = 'advisory';
  ```

  The lock is a single bigint key (`hashtextextended('taskq:maintenance_leader:<schema>', 0)`), so it appears with `classid = 0`; match `objid` against `SELECT hashtextextended('taskq:maintenance_leader:<your-schema>', 0)` to pick this deployment's row out of the set. Contention is recorded once per distinct holder a pod finds in its way, so a stable fleet (however many followers it has) records nothing after it settles, and a handover records one event per pod. A sustained rate therefore means the observed holder keeps changing (handover churn), or that this pod cannot even read the lease row to observe a stable holder; its probe fails every cycle; check its `leader-retry` lines and connectivity.

**How to remediate.**

1. Identify the winning session from the SQL above. Because the key is schema-qualified, a holder belonging to another schema or deployment sharing this database is **not** the cause: it holds a different key. The holder is a session of this same schema.
2. Cross-check it against `{schema}.maintenance_leader`. A pod that follows a
   live lease records no contention at all, and a pod that finds the row
   absent wins it outright on its next cycle, so a sustained rate means the
   counter's remaining shapes: the row's holder keeps changing (handover
   churn), or the counting pod cannot read the row at all. A courtesy lock
   held by a session no lease row accounts for never produces it: the lease
   is what confers the role, so nothing waits on that session, but a lock
   nobody owns still points at a pod that elected under an older release,
   or at a session one left behind.
3. **Upgrade discipline:** the schema-qualified names replaced the
   unqualified (`taskq:maintenance_leader`) ones outright, and the two are
   NOT overlap-compatible: a mixed old/new fleet holds different keys, so
   old and new releases can both act as leader of the same schema at once
   (sweeps stay row-safe under `FOR UPDATE SKIP LOCKED`; cron gains a
   double-fire window because its lock is what serialises ticks). Adopt a
   release that changes lock names by restarting the fleet onto it, not by
   rolling it; the window is the deploy, not the steady state (see
   `taskq.constants.schema_lock_name`).
4. Confirm recovery: `taskq_leader_lock_contention_total` stops rising and
   `sum(taskq_maintenance_leader_is_leader) == 1` again
   (see `TaskQLeaderSplitBrainOrNoLeader`).

---

## TaskQLeaderSplitBrainOrNoLeader

**What fired.** `sum(taskq_maintenance_leader_is_leader) != 1` for 2 minutes, severity critical. The gauge reads 1 on the elected leader pod and 0 on every follower, labeled by `worker_id`, so the fleet-wide sum is the leader count. 0 means no worker holds the maintenance role: promotion, cron, reclaim and prune are all stopped. Above 1 means two pods both believe they are leader. Both are split-brain conditions in the operational sense: the single-serialiser contract everything else assumes is not holding.

**How to confirm.**

- Metric: `taskq_maintenance_leader_is_leader` per `worker_id`. All zeros, or two 1s. The zero case with `taskq_leader_lock_contention_total` rising is no worker ever winning the election; see [TaskQLeaderLockContention](#taskqleaderlockcontention).
- SQL: who the database says holds the role, and who holds the advisory lock:

  ```sql
  SELECT worker_id, elected_at, expires_at FROM taskq.maintenance_leader;

  SELECT pid, granted, now() - backend_start AS session_age
  FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE locktype = 'advisory' AND classid = 0;
  ```

  A healthy read is exactly one lease row with `expires_at` in the future, held (via the schema-qualified bigint key) by that pod. Two lease rows with overlapping terms, or a pod still reporting `is_leader = 1` after its own row lapsed, is the finding.
- The above-one shape has a known mundane cause: a fleet mid-upgrade across releases whose lock names differ holds different keys and can both act as leader of the same schema at once. Rule the mixed-fleet case out first (see the upgrade discipline in [TaskQLeaderLockContention](#taskqleaderlockcontention)) before hunting anything exotic.

**How to remediate.**

1. Zero leaders: work the contention path, not the gauge. Follow [TaskQLeaderLockContention](#taskqleaderlockcontention) and [TaskQPromotionStalled](#taskqpromotionstalled). Everything leader-side is stopped while this reads 0, so pending and scheduled work piles up, but nothing is lost; workers keep consuming what is already `pending`.
2. Two leaders: identify the stale believer from the metric's `worker_id` and restart that pod. The usual shape is a pod partitioned from Postgres right after winning: its in-process `is_leader` event stays set until a renewal fails, while the lease row it left behind lapses and a successor takes over. The sweeps are row-safe under `FOR UPDATE SKIP LOCKED` either way; the one real double-fire window is cron, whose advisory lock is what serialises ticks, so check `cron_schedules`-driven jobs for duplicates on the overlap interval before declaring no damage.
3. Confirm recovery: the sum returns to exactly 1 and the per-sweep `last_success` stamps move again.

---

## TaskQRateLimitDependencyOutage

**What fired.** `rate(taskq_ratelimit_acquire_dependency_failures_total[5m]) > 0` for 5 minutes: rate-limit acquires are failing because the limiter's store (Redis, or the PG fallback behind it) could not answer, and the worker failed the acquire closed as a denial. Every rate-limited dispatch is snoozing while the outage lasts: no work is lost, but nothing rate-limited moves either, and the queue looks calm while it piles up behind the limiter.

**How to confirm.**

- Metric: `taskq_ratelimit_acquire_dependency_failures_total` rising, labeled by `error_type` (the exception class name). Read it beside `taskq_reservation_denials_total{source="rate_limit"}`: denials with this counter flat are ordinary contention; denials with this counter rising are an outage masquerading as contention; the two must be told apart before anyone scales a bucket.
- The `error_type` label names the failure class (a Redis connection error, a timeout): it distinguishes "the store is unreachable" from "the store is slow".
- Check the store from a worker pod, not from your laptop: the outage is between the worker's network position and the store (DNS, NetworkPolicy, the store itself).

**How to remediate.**

1. Restore the store dependency: Redis connectivity from the worker pods first (the common cause), then the store itself. The PG fallback fails the same closed way when Postgres is the sick dependency; check `TaskQSweepTimeouts` / `TaskQDispatchLatencyHigh` before touching Redis.
2. Do NOT raise bucket limits or disable rate limiting during the outage: the denials are the limiter failing closed (the configured safe behavior), and widening limits cannot create store capacity.
3. Snoozed dispatches retry on their own once acquires succeed again; confirm recovery by watching `taskq_ratelimit_acquire_dependency_failures_total` flatten and the snoozed backlog drain (`taskq_jobs_by_status{status="scheduled"}` falling).

---

## TaskQProgressPublishFailures

**What fired.** `rate(taskq_progress_publish_failures_total[5m]) > 0` for 5 minutes: Redis publish round trips for progress events are failing. The counter is bumped on every failed publish attempt, labeled by `channel` (`per_job` or `global`) and `error_type` (the exception class name). The `progress-publish-failure` WARNING beside it is window-gated to one line per channel per minute, because a sustained Redis death fails every publish of every job and a log line per attempt is a flood, not a signal; the counter is the per-attempt aggregate the alert reads.

**What it means.** Only the real-time fanout degrades: the admin UI's live progress views fall back to polling at `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS`, so pages stay correct and merely stale between polls. The progress rows themselves persist on a separate Postgres path, counted by `taskq_progress_flush_failures_total` (a `stage="per_job"` versus `stage="pool"` distinction, a different incident); a job's settled progress is not lost by a publish failure.

**How to confirm.**

- Metric: `taskq_progress_publish_failures_total` by `channel, error_type`. `per_job` failures drop live updates for individual jobs' viewers; `global` failures drop the fleet-wide progress channel. `error_type` distinguishes the shapes: connection classes are network between the worker and Redis, `ResponseError` is Redis answering with an error (read-only replica, maxmemory), timeout classes are saturation.
- The admin UI shows the degradation directly: a page whose SSE stream cannot deliver events still polls, so "the page updates, only late" is the published symptom of this alert, never of the flush counter.

**How to remediate.**

1. Restore Redis from the worker pods' position (the outage is between them and the store, so test from there, not from your laptop): connectivity first, then capacity (`maxmemory` evictions, a read-only replica after a failover).
2. Do not disable progress emission to quiet the alert: the publish is fire-and-forget and its failure mode (a stale admin page that still polls) is strictly better than flying without progress telemetry.
3. Confirm recovery: the rate returns to 0 and the live views resume updating between polls.

---

## TaskQCronLockContention

**What fired.** `rate(taskq_cron_lock_contention_total[10m]) > 0` for 10 minutes: cron ticks are returning without firing because another session holds the cron advisory lock, sustained. A brief low rate is the benign leader-handover overlap; a rate sustained at the tick cadence means cron is not running anywhere: the lock is transaction-scoped and releases on COMMIT/ROLLBACK, which never happens if the holding session was partitioned without a FIN. That is the fleet-wide cron stall: every schedule silently stops firing, and the signals an operator would check first (`taskq_cron_disabled_schedules`, `taskq_cron_consecutive_failures`) deliberately stay still in that mode.

**How to confirm.**

- Metric: `taskq_cron_lock_contention_total` rising at roughly the tick rate (one contention per tick attempt), not brief bursts. `taskq_cron_disabled_schedules` staying 0 while no `cron fired` lines appear is the same stall seen from the other side.
- SQL: the cron lock holder (the lock name is `taskq:cron:<schema>`, a single bigint key via `hashtextextended`, so it appears with `classid = 0`):

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
   (`SELECT pg_terminate_backend(<pid>);`); the server reaps partitioned sessions only on its `tcp_keepalives_*` schedule, which can outlast a maintenance window.
2. If the holder is a healthy worker of THIS schema: two pods both running cron loops against the same schema points at a deployment/election misconfiguration: one cron loop per schema is the contract; fix the deployment, do not kill the session.
3. Confirm recovery: `taskq_cron_lock_contention_total` stops rising, `cron fired` lines resume, and missed schedules catch up (due schedules fire immediately once the lock is free; cron does not skip missed ticks by default; see the cron guide for `TASKQ_CRON_TICK_LIMIT` if the backlog is large).

---

## TaskQCronBudgetDeferrals

**What fired.** A sustained rate of `taskq_cron_budget_deferrals_total` (one increment per cron fire the tick's funded factory budget could not fund a grant for). A brief burst is benign: catch-up backlogs drain in tick-sized batches and each draining tick defers whatever its factories' waits could not fit. A rate that persists for minutes means one schedule's payload factory is **monopolizing the tick budget every tick**: it is planned ahead of its peers (an older `next_fire_at`, or a catch-up crawl), consumes most of the funded factory budget, and still SUCCEEDS, so it never strikes, never auto-disables, and never frees the budget. Its peers then defer on every tick: delayed (the deferral advances `next_fire_at` one leader tick, the owed slot stays inside the catch-up window), never struck, never disabled: quiet starvation with no auto-disable rescue, which is exactly why the counter exists. The monopolizer itself looks perfectly healthy (`cron fired` lines, `consecutive_failures = 0`).

**How to confirm.**

- Metric: `rate(taskq_cron_budget_deferrals_total[5m])` non-zero and flat, not decaying: a decaying rate is a catch-up drain ending. The `actor` label names the STARVING schedule's actor (per-schedule attribution is on the log line, not the label).
- Logs: `cron-fire-budget-deferred` events with the same `schedule_id` every ~1s, while a NEIGHBOUR schedule's `cron fired` lines keep appearing; the neighbour whose fires sit immediately beside the deferrals in the timeline is the monopolizer. Its factory's duration is visible in the gap between consecutive `cron fired` lines.
- SQL: the order the tick plans in (the monopolizer is the enabled, due row at the front):

  ```sql
  SELECT id, actor, name, payload_factory, next_fire_at,
         now() - next_fire_at AS overdue_by, consecutive_failures
  FROM taskq.cron_schedules
  WHERE enabled AND next_fire_at <= now()
  ORDER BY next_fire_at
  LIMIT 10;
  ```

  The monopolizer is the due row with a `payload_factory` and the oldest `next_fire_at` (or one advancing by one catch-up slot per tick); the deferring schedules are the factory-backed rows behind it.

**How to remediate.**

1. Tighten `TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT` to below the monopolizing factory's real duration (read it off the `cron fired` timeline gaps). The factory then outruns its granted deadline, takes the strike it has earned, and auto-disables after `TASKQ_CRON_AUTO_DISABLE_THRESHOLD` ticks, freeing its peers. This is the intended consequence, not collateral damage: a payload factory slower than the operator-declared budget is a defect of that schedule.
2. If the monopolizer's duration is legitimate, raise `TASKQ_DISPATCHER_COMMAND_TIMEOUT` so the funded factory budget (90% of it) fits the monopolizer plus a fundable grant (a quarter of the funded budget) for its peers, which then fire in the same tick. Check the watchdog interplay documented on that setting before widening it.
3. Or move the slow work out of the factory: payload factories run inside the leader's tick, holding the cron advisory lock for the whole planning batch. Work slower than a fraction of a tick belongs in the job the schedule enqueues, not in the payload build.
4. Confirm recovery: `taskq_cron_budget_deferrals_total` stops rising, the deferring schedules' `cron fired` lines resume, and their owed slots fire (deferred slots are retried, not skipped; nothing was lost).

---

## TaskQCronScheduleDisabled

**What fired.** `taskq_cron_disabled_schedules > 0` (for 0m, immediately): one or more cron schedules have been auto-disabled after `TASKQ_CRON_AUTO_DISABLE_THRESHOLD` (default 3) consecutive `payload_factory` failures. The gauge is refreshed by the leader's cron tick from the database's own count of disabled rows, so it settles on what every process has left behind after each tick. A disabled schedule enqueues nothing: its jobs stop firing, silently, on the schedule's own cadence.

**How to confirm.**

- Metric: the gauge value itself. It does not decay; it moves when the database's set of disabled schedules changes, and it returns to 0 when the last disabled schedule is re-enabled.
- SQL: which schedules are disabled and why:

  ```sql
  SELECT actor, name, consecutive_failures, last_fire_error, next_fire_at
  FROM taskq.cron_schedules
  WHERE NOT enabled
  ORDER BY actor, name;
  ```

  `last_fire_error` carries the exception text of the strike that disabled the schedule; the `cron schedule auto-disabled` ERROR log lines and the per-tick `cron fire failed` lines name the same schedules.
- The strikes are consecutive: a single success resets the count, so a schedule that reached auto-disable failed three ticks in a row. A factory raising every tick is a defect of that factory's code or configuration, not a transient dependency; a rate-limit or database outage during the ticks instead shows up in the alerts that own those dependencies.

**How to remediate.**

1. Fix the `payload_factory` the `last_fire_error` names and deploy the correction before re-enabling. Re-enabling a still-broken schedule spends three more strikes on the same defect and lands back here.
2. Check whether a restart already resolved it. Since the ownership model, the schedule row carries `disabled_by`: `'auto'` (the cron loop's failure-count auto-disable) is reverted automatically the next time a worker boot re-declares a code-owned schedule (`enabled=true`, `consecutive_failures=0`, the marker cleared, and a `cron-schedule-auto-disable-reverted` log line at the boot). So if the failure was a transient blip (failover, saturation) and the fixed or still-declared code re-registers the schedule, the alert resolves on the next boot and this section's step 2 is unnecessary. A row with `disabled_by='operator'` is deliberate operator intent: no restart re-enables it, only the steps below do. A disabled row with `disabled_by=NULL` is a mixed-version deploy's shape (an old pod cannot write the marker): a boot re-enables it when it carries the old failure arm's fingerprint (`consecutive_failures` at or past the threshold, `last_fire_error` set) and leaves it alone otherwise; disabled rows that predate marker tracking were stamped `'operator'` by migration `01.00.19_05`.

   ```sql
   SELECT actor, name, enabled, disabled_by, consecutive_failures, last_fire_error
   FROM taskq.cron_schedules
   WHERE disabled_by IS NOT NULL OR NOT enabled
   ORDER BY actor, name;
   ```
3. Re-enable the schedule: the schedule handle's `enable()` resets `consecutive_failures` to 0, clears `last_fire_error` and `disabled_by`, and the admin UI's schedules page exposes the same action. Missed fires inside `TASKQ_CRON_CATCH_UP_WINDOW` catch up on the next tick after enabling; older misses are skipped by design.
4. Confirm recovery: the gauge returns to 0 and `cron fired` lines resume for the re-enabled schedule.

Do not raise `TASKQ_CRON_AUTO_DISABLE_THRESHOLD` to keep a broken schedule alive: the auto-disable exists so a permanently failing factory stops consuming tick budget every second, and a larger budget only delays the same silence.

---

## TaskQRunningLeaseExpired

**What fired.** `taskq_jobs_running_lease_expired > 0` for 5 minutes: running jobs whose lock lease is past expiry, sustained, with no cancel in flight (rows in a cancel phase (`cancel_phase != 0`) are carved out of the gauge, because the reclaim sweep deliberately waits cancel grace + cleanup grace + 60 s past expiry for a cancelling row before it pre-empts it), so an expired lease mid-cancel is the cancellation protocol working, not an incident (a cancel that never completes pages elsewhere: [TaskQAbandonedJobs](#taskqabandonedjobs) when its worker is alive to escalate through the phases, `TaskQHeartbeatMisses` when it died mid-cancel; the reclaim sweep honors the row to `cancelled` either way). A healthy fleet reads 0: the leader's reclaim sweep (`sweep_name="expired_locks"`) drains expired leases within a tick or two of expiry, so a sustained non-zero count means reclaim is not draining. Work is claimed and stuck in `running` while health probes stay green: the zombie-running shape.

**How to confirm.**

- Metric: `taskq_jobs_running_lease_expired` (sampled by every worker, so one flapping series is a sampling artifact; the alert fires on the sustained value). Cross-check the reclaim sweep's health: `taskq_maintenance_leader_sweep_last_success_seconds{sweep_name="expired_locks"}` fresh means the sweep runs but rows regrow faster than it drains (workers dying or wedging mid-run); a stale stamp means the sweep itself is stopped (see [TaskQPromotionStalled](#taskqpromotionstalled): the same signature, different sweep).
- SQL: the zombies and their holders (the gauge's own predicate, cancel phases excluded):

  ```sql
  SELECT id, actor, locked_by_worker, lock_expires_at,
         now() - lock_expires_at AS overdue_by, attempt, max_attempts
  FROM taskq.jobs
  WHERE status = 'running' AND lock_expires_at < clock_timestamp()
    AND cancel_phase = 0
  ORDER BY lock_expires_at;
  ```

  Cancelling rows the sweep is still waiting out (expected, not zombies: the same grace ladder the reclaim sweep applies):

  ```sql
  SELECT id, actor, cancel_phase, cancel_requested_at, lock_expires_at
  FROM taskq.jobs
  WHERE status = 'running' AND lock_expires_at < clock_timestamp()
    AND cancel_phase != 0
  ORDER BY lock_expires_at;
  ```

- The admin `/jobs` page renders the same state per row (Lease column): a red `expired` badge with the holding worker.

**How to remediate.**

1. If `TaskQHeartbeatMisses` is firing or the holders' workers are gone: the jobs self-heal: the reclaim sweep transitions them (retryable → `pending` after a backoff; exhausted → `crashed`). The alert's value is that it stays non-zero when that does NOT happen.
2. If the reclaim sweep is stalled or timing out: follow [TaskQPromotionStalled](#taskqpromotionstalled) and [TaskQSweepTimeouts](#taskqsweeptimeouts); fix the sweep/database; the zombies are a symptom.
3. If the sweep is healthy and the count still regrows: jobs are repeatedly outliving their lease: the lease (`lock_lease`-shaped settings) is shorter than the actor's real run time and heartbeats are not renewing fast enough. Check `TaskQLockExpiringSoon` (it reads the measured lease left at each renewal: `lock_lease` minus the gap since the previous one, so firing means renewals are landing late, from a slow heartbeat pool, failed ticks or a blocked event loop; `taskq_worker_event_loop_lag_seconds` and `taskq_heartbeat_misses_total` say which) and widen the lease/heartbeat budget for those actors; do not restart workers to "clear" the gauge: the same jobs will zombie again.
4. Confirm recovery: `taskq_jobs_running_lease_expired` returns to 0 and stays there across several sweep intervals.

---

## TaskQFailedJobRateHigh

**What fired.** `sum(rate(messaging_client_consumed_messages_total{outcome="failed"}[5m])) / sum(rate(messaging_client_consumed_messages_total[5m])) > 0.01` for 5 minutes: more than 1% of consumed attempts ended in a **terminal** failure: a non-retryable exception class, or the retry budget (`max_attempts` / `schedule_to_close`) exhausted. Retried failures are not in this share: they end as `outcome="scheduled"` and are counted by [TaskQRetryRateHigh](#taskqretryratehigh).

**How to confirm.**

- Metric: `messaging_client_consumed_messages_total{outcome="failed"}` by `actor`, and `taskq_jobs_attempt_failures_total{retryable="false"}` by `actor, error_type`; the second names the exception class per actor.
- Database, the terminal rows and their reason:

  ```sql
  SELECT actor, error_class, count(*)
  FROM taskq.jobs
  WHERE status = 'failed' AND finished_at > clock_timestamp() - interval '15 minutes'
  GROUP BY actor, error_class ORDER BY count(*) DESC;
  ```

  `error_class = 'DeadlineExceeded'` is a `schedule_to_close` budget too tight for the actor's retry curve (`taskq_jobs_timeouts_total{kind="schedule_to_close"}` rises with it); a `PayloadValidationError` is a producer shipping a payload the actor's schema rejects; anything else is the actor's own exception.

**How to remediate.**

1. One actor, one `error_class`: fix that actor or its dependency; failed rows can be retried from the admin UI's Retry button or `backend.retry_job()` once the cause is fixed.
2. `DeadlineExceeded` dominating: widen `schedule_to_close` (or the retry `base`/`cap`) for the actor; see [ops.md: Timeouts](ops.md#2-timeouts-start_to_close-and-schedule_to_close).
3. Many actors at once: a shared dependency (database, downstream API); check `TaskQRetryRateHigh` and `TaskQDispatchLatencyHigh` first; the terminal share is the tail end of the same incident once budgets run out.

---

## TaskQRetryRateHigh

**What fired.** `sum(rate(taskq_jobs_attempt_failures_total{retryable="true"}[5m])) / sum(rate(messaging_client_consumed_messages_total[5m])) > 0.1` for 10 minutes: more than 10% of consumed attempts raised and were rescheduled for another try. Each retry burns an attempt, a backoff delay and a worker slot, so a sustained rate is a dependency failing under retry cover: the jobs still complete, the terminal-failed share stays flat, and nothing else fires until budgets run out.

**How to confirm.**

- Metric: `taskq_jobs_attempt_failures_total{retryable="true"}` by `actor, error_type`; the exception class per actor is the diagnosis (`ConnectionError` / `TimeoutError` on one actor is its downstream; `asyncpg` classes across actors is the database).
- Database, jobs currently waiting on a retry and what they last raised:

  ```sql
  SELECT actor, error_class, count(*), min(scheduled_at) AS next_try
  FROM taskq.jobs
  WHERE status = 'scheduled' AND attempt > 1
  GROUP BY actor, error_class ORDER BY count(*) DESC;
  ```

**How to remediate.**

1. Fix or wait out the dependency the `error_type` names; retries recover on their own once it answers again. Do not raise `max_attempts` to "make it go away": that spends more slots on the same failure.
2. If the rate is one actor with a transient class that is really permanent (a bad payload that will never succeed), classify it: `non_retryable_exceptions` on the actor, or a `retry_classifier`; see [ops.md: Classifying failures](ops.md#6-classifying-failures-terminal-retryable-transient).
3. Confirm recovery: the retried share falls back under the threshold and `taskq_jobs_by_status{status="scheduled"}` drains.

---

## TaskQAbandonedJobs

**What fired.** `rate(taskq_jobs_abandoned_total[5m]) > 0` for 5 minutes: a job was **abandoned**: an operator-requested cancel outlasted both grace periods (the actor was asked to stop, then forced with `task.cancel()`, and still never exited), so the running attempt was taken from it. Shutdowns never produce this: a deploy releases (interrupts) in-flight jobs back to the fleet. A retry or snooze never reaches this series either; those are `outcome="scheduled"` on the consumed-messages counter, so any rate here is a real actor ignoring cancellation.

**How to confirm.**

- Metric: `taskq_jobs_abandoned_total` by `actor`; recorded by the abandon write itself, on both backends.
- Database, the abandoned rows and the attempts that were taken away:

  ```sql
  SELECT j.id, j.actor, j.finished_at, a.worker_id, a.duration_ms
  FROM taskq.jobs j
  JOIN taskq.job_attempts a ON a.job_id = j.id AND a.attempt = j.attempt
  WHERE j.status = 'abandoned' AND j.error_class = 'CancelAbandoned'
  ORDER BY j.finished_at DESC LIMIT 50;
  ```

**How to remediate.**

1. The named actor does not yield to cancellation: it is blocking the event loop (a sync call without `asyncio.to_thread`), swallowing `CancelledError`, or running a native call that cannot be interrupted. Make it cooperative: poll `ctx.cancellation_requested` (or await `ctx.cancel_event`) in long loops, keep blocking work off the loop; see [ops.md: Thread-unsafe native libraries](ops.md#thread-unsafe-native-libraries).
2. The abandoned row is terminal; the actor's coroutine may still be running in the worker until the process restarts. If it holds resources, restart that worker (`taskq_active_jobs` on its health socket shows the stuck slot).
3. If the graces are too short for a well-behaved actor's cleanup, widen `TASKQ_CANCELLATION_GRACE_PERIOD` / `TASKQ_CLEANUP_GRACE_PERIOD`, but only after (1) is ruled out.

---

## TaskQHeartbeatMisses

**What fired.** `rate(taskq_heartbeat_misses_total[5m]) > 0` for 5 minutes, severity critical: a worker's heartbeat renewal ticks are failing. The counter is bumped once per failed beat in the heartbeat loop, on every transient Postgres error class (connection resets, statement timeouts, server-side cancels). At `TASKQ_MAX_HEARTBEAT_FAILURES` (default 3) consecutive failures the worker does not limp on: it self-isolates, handing its running jobs back to the fleet (retryable rows re-pended, exhausted rows crashed) and terminating, so a sustained miss escalates from lost renewals to lost capacity within a few beats.

**How to confirm.**

- Metric: `rate(taskq_heartbeat_misses_total[5m])`. A blip that decays within a beat or two is one transient error; a flat sustained rate is a worker that cannot reach Postgres. The counter carries no worker label, so read it beside the logs of the replica that is reporting it.
- Logs: `heartbeat-tick-failure` WARNING lines with `error_class` naming the Postgres failure, then `heartbeat-failures-approaching-limit` at half the budget, then the self-isolation. At the defaults the whole escalation fits in about 30 seconds (3 beats x 10 s).
- SQL: which workers the database still considers alive:

  ```sql
  SELECT id, hostname, pid, worker_label, last_seen_at,
         now() - last_seen_at AS silent_for
  FROM taskq.workers
  ORDER BY last_seen_at DESC;
  ```

  A row going silent for more than a beat while its process is up is the confirmation.

**How to remediate.**

1. Read `error_class` on the `heartbeat-tick-failure` lines. Connection-refused classes are network or DNS between the pod and Postgres; server-shutdown classes are the database restarting under the fleet; timeout classes are latency, and [TaskQDispatchLatencyHigh](#taskqdispatchlatencyhigh) or [TaskQSweepTimeouts](#taskqsweeptimeouts) firing alongside points at a shared database incident.
2. A worker that crossed the budget has already isolated: it exits, the orchestrator restarts it, and its re-pended jobs dispatch elsewhere. The capacity dip is the design working; do not pin a whole fleet's queue set on one replica.
3. If misses cluster at deploys or failovers, check the lease arithmetic the settings loader enforces: `lock_lease` must cover `max(heartbeat_interval, heartbeat_command_timeout) + (max_heartbeat_failures + 1) * (heartbeat_interval + heartbeat_command_timeout)` (58 s at the defaults). A lease tighter than that turns a beat hiccup into [TaskQRunningLeaseExpired](#taskqrunningleaseexpired).
4. Confirm recovery: the rate returns to 0 and the affected `taskq.workers` row's `last_seen_at` is fresh again.

Do not raise `TASKQ_MAX_HEARTBEAT_FAILURES` to quiet this alert: the budget is the worker's deadline for handing its jobs back while their leases are still sane, and widening it widens the window in which a dead worker's jobs look running.

---

## TaskQLockExpiringSoon

**What fired.** `histogram_quantile(0.99, rate(taskq_lock_expires_in_seconds_bucket[5m])) < 30` for 5 minutes: the lease left on job locks at the moment the heartbeat renewed them has a p99 below 30 s. The histogram records, on every successful beat, `lock_lease` minus the measured gap since the previous beat's UPDATE, both stamps taken at the same point so network latency cancels; a sample near 0 means the beat landed just before the leases it renewed expired. The reclaim sweep takes a job the moment the gap reaches the lease, so this alert is the leading edge of [TaskQRunningLeaseExpired](#taskqrunningleaseexpired): renewals are landing late, and the budget is being spent.

**How to confirm.**

- Metric: the p99 of `taskq_lock_expires_in_seconds` against your configured `TASKQ_LOCK_LEASE` (default 60 s): a p99 of 30 s means half the lease is gone to lateness. Read `taskq_heartbeat_misses_total` (failed ticks) and `taskq_worker_event_loop_lag_seconds` (a blocked loop) beside it; those two say WHICH kind of lateness this is.
- The sample is stamped on every successful beat whether or not the renewal threshold renewed any rows, so the histogram measures the beat cadence itself: a late beat lowers it exactly as a failed one does.
- SQL: what the fleet's running locks have left:

  ```sql
  SELECT id, actor, locked_by_worker, lock_expires_at,
         extract(epoch FROM (lock_expires_at - clock_timestamp()))::int AS remaining_s
  FROM taskq.jobs
  WHERE status = 'running'
  ORDER BY lock_expires_at
  LIMIT 20;
  ```

**How to remediate.**

1. `taskq_heartbeat_misses_total` rising with it: the heartbeat pool cannot answer in time. Follow [TaskQHeartbeatMisses](#taskqheartbeatmisses) (connectivity first, then the heartbeat pool's `TASKQ_HEARTBEAT_POOL_SIZE` and `TASKQ_HEARTBEAT_COMMAND_TIMEOUT`).
2. Heartbeats clean but `taskq_worker_event_loop_lag_seconds` high: the beats are late because the loop is blocked. Find the blocking actor (the worker's `event-loop-stall-attributed` warnings name it and the admin UI's Stall hotspots column aggregates it); this is an actor defect, not a lease misconfiguration.
3. Neither, and Postgres latency is the story: widen the budget through the enforced arithmetic (`lock_lease >= max(heartbeat_interval, heartbeat_command_timeout) + (max_heartbeat_failures + 1) * (heartbeat_interval + heartbeat_command_timeout)`); a settings load that fails the invariant is the loader keeping the lease ahead of the isolation cascade, not a bug.
4. Confirm recovery: the p99 climbs back above the threshold, toward the full lease.

Do not fix this by raising `TASKQ_LOCK_LEASE` alone when the cause is a blocked event loop: a longer lease moves the reclaim deadline out over the same wedged beat, and the actor stays wedged twice as long before anything notices.

---

## TaskQQueueUnserved

**What fired.** `taskq_queue_depth{queue!="_other_"} > 0 unless on(queue) taskq_queue_live_workers > 0` for 2 minutes: a queue holds pending or scheduled jobs and **no live worker subscribes to it**: no worker row whose `last_seen_at` is inside `TASKQ_ADMIN_WORKER_LIVENESS_SECONDS` (default 30 s, three heartbeats) lists the queue in its `queues`. Nothing will consume the work. Both gauges come from the same leader sampler tick, so the join never compares two moments; a worker row that stopped heartbeating does not count even before the stale-worker sweep removes it.

**How to confirm.**

- Metric: `taskq_queue_depth{queue="<q>"}` beside `taskq_queue_live_workers{queue="<q>"}` (absent, or 0).
- Database: who last served the queue and when:

  ```sql
  SELECT id, hostname, pid, worker_label, last_seen_at,
         last_seen_at > clock_timestamp() - interval '30 seconds' AS live
  FROM taskq.workers
  WHERE '<q>' = ANY(queues)
  ORDER BY last_seen_at DESC;
  ```

  No rows: nothing ever subscribed (a producer enqueues onto a queue name nobody runs, or the queue was dropped from every `TASKQ_QUEUES` at the last deploy). Rows, none live: the replicas serving it are down or partitioned from Postgres; check their `/ready` and `TaskQHeartbeatMisses`.
- The admin UI's queues page shows the same condition as the "pending jobs but no alive worker" banner.

**How to remediate.**

1. Start (or scale up) a worker whose `TASKQ_QUEUES` includes the queue, or add the queue to an existing worker's subscription. The backlog drains on its own once a live worker subscribes; nothing was lost.
2. If the queue name is a producer mistake (a typo, a stale config), re-route the jobs: `taskq actor-config move-queue` for a whole actor, or re-enqueue. The stranded-jobs detector names the actors involved (`taskq_jobs_stranded{reason="unserved_queue"}`).
3. Confirm recovery: `taskq_queue_live_workers{queue="<q>"} > 0` and the depth falling.

---

## TaskQStrandedJobs

**What fired.** `taskq_jobs_stranded > 0` for 5 minutes: pending/scheduled jobs that can never be dispatched, with the reason on the label. `reason="no_actor_config"`: the actor has no `actor_config` row (deregistered with `taskq actor-config deregister`, or never registered by any worker), so the dispatch CTE, which derives its candidates from `actor_config`, never sees the rows. `reason="unserved_queue"`: the queue dispatch routes the actor on (the actor's current assignment for a re-pended row, the row's own queue otherwise) has no live worker subscribed. Neither dispatch nor the deadline sweep will ever touch these rows.

**How to confirm.**

- Metric: `taskq_jobs_stranded` by `actor, reason`; sampled by the leader every `TASKQ_STRANDED_JOBS_INTERVAL`; the `stranded-jobs-no-actor-config` / `stranded-jobs-unserved-queue` log events carry the same counts, and the second names the queues.
- Database:

  ```sql
  -- no_actor_config
  SELECT j.actor, count(*) FROM taskq.jobs j
  WHERE j.status IN ('pending', 'scheduled')
    AND NOT EXISTS (SELECT 1 FROM taskq.actor_config ac WHERE ac.actor = j.actor)
  GROUP BY j.actor;
  ```

**How to remediate.**

1. `no_actor_config`: run a worker that registers the actor (registration writes the row at boot), or, if the actor is gone for good, cancel the rows (`JobsClient.cancel_where(JobFilter(actor=...))`, or the admin UI's cancel action) so they stop counting.
2. `unserved_queue`: follow [TaskQQueueUnserved](#taskqqueueunserved): subscribe a live worker to the named queue, or move the actor's assignment with `taskq actor-config move-queue`.
3. Confirm recovery: the series clears on the next detector tick (an empty reading is published, not a frozen last value) and the `stranded-jobs-cleared` event logs.

---

## Event-loop stall attribution (worker warnings)

**What fires.** No alert: the worker's lag watchdog warns on its own. A warning named `event-loop-stall-attributed` logs once per stall at the warn tier and again at the terminal trip tier, alongside the existing `worker-watchdog-lag-warn` and `worker-watchdog-trip` events. Every attribution also bumps `taskq_worker_loop_stall_attributions_total{actor, kind}`, and the heartbeat merges the worker's rolling tally (top 20 actors by count, with per-kind counts) into its `workers` row metadata, so the hotspots are visible fleet-wide in `/admin/workers` and `taskq doctor` without scraping each worker.

**What it means.** The event loop could not schedule (a beat was late past the warn budget), and the watchdog attributes the stall to the actor whose frame sat under the work holding the interpreter. The `kind` label separates the two shapes:

- `blocking_call`: the actor's synchronous call RELEASED the GIL (`time.sleep`, a socket or HTTP wait, a subprocess). The sampling is exact: the watchdog thread kept running and caught the blocking frame.
- `gil_held`: the synchronous work HELD the GIL (a C extension that does not release it, such as a large document parse, or a hot pure-Python loop). The watchdog's own wakeups starved; the sample is approximate and points at (or just after) the C call.

The warning's `frame` field is `file:line:function` of the deepest non-taskq frame, and `actor`/`job_id` name the registered actor and (when exactly one running job matched) the job.

**How to remediate.**

1. `blocking_call`: move the blocking call off the event loop (`asyncio.to_thread` / `run_in_executor`) or make the actor async.
2. `gil_held`: the actor holds the GIL in a long synchronous computation: chunk it or move it off the loop.
3. Confirm recovery: the per-actor rate of `taskq_worker_loop_stall_attributions_total` flattens, and the worker's Stall hotspots column in `/admin/workers` stops growing.

---

## Related documentation

- [Observability](observability.md): the metrics these alerts evaluate,
  including the sweep sample-population rule.
- [Configuration](configuration.md): `TASKQ_EVENT_WRITER_*`,
  `TASKQ_SWEEP_*` and `TASKQ_CRON_TICK_LIMIT` knobs referenced above.
- [Troubleshooting](troubleshooting.md): symptom-first diagnosis paths.
