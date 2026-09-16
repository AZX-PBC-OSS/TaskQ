# Upgrading

TaskQ's schema migrations are **forward-only by design**. There is no `down`
migration mechanism and none is planned — this section explains why, and
what to do if you need to undo a change.

---

## Forward-only migration policy

The migration runner (`taskq.migrate`) discovers `*.sql` files bundled under
`taskq.migrations` in lexicographic order (`{ver}_{nn}_{pre|post}_{description}.sql`),
applies any not already recorded in `{schema}.schema_migrations`, and records
a SHA-256 checksum of the rendered SQL after each successful apply.

There is no `down` operation. **To revert a migration, restore the database
from a backup taken before it was applied.**

This is a deliberate tradeoff, not a missing feature:

- Down migrations are rarely exercised in practice and rot quietly until the
  one time they're needed — at which point they often don't work.
- A schema rollback that isn't paired with a data rollback (e.g., a dropped
  column that already lost data) is not actually safe to run automatically.
- Point-in-time recovery / backup restore is the operation you actually want
  for "undo a bad deploy" in a durable job queue, since job state itself
  needs to roll back together with the schema.

## Before upgrading TaskQ

1. **Take a backup.** Since there is no automated rollback, a recent backup
   (or PITR window) is your only revert path.
2. **Check the [Changelog](../changelog.md)** for the target version — TaskQ is
   pre-1.0 (see the Stability note in the [README](https://github.com/AZX-PBC-OSS/TaskQ#readme)),
   so breaking changes, including schema changes, may land in minor version
   bumps (`0.x.0`), not only majors.
3. **Review pending migrations before applying them:**

   ```shell
   taskq migrate status
   ```

   This lists every discovered migration and whether it has already been
   applied, without changing anything.

4. **Apply migrations explicitly** — from a pre-deploy job or init container,
   before any worker starts:

   ```shell
   taskq migrate up
   ```

   Workers never self-migrate. `TASKQ_MIGRATE_ON_START=true` is honoured only
   by `taskq ui serve`; a worker warns and ignores it, and refuses to boot
   while a pre-phase migration is pending, so relying on it there produces a
   crash-loop rather than a migrated schema. N replicas racing to migrate is
   the concurrent-migration hazard migrations exist to avoid.

   The command is idempotent — migrations already recorded in
   `{schema}.schema_migrations` are skipped. See [cli.md](cli.md#taskq-migrate-up)
   for the full option reference (`--phase`, `--target`, `--max-steps`).

   `TASKQ_MIGRATE_ON_START=true` is **not** a substitute here: it is honoured
   only by `taskq ui serve`, which runs as a single process. The worker
   ignores it (and warns when it is set) — N worker replicas racing to migrate
   is the hazard the migration advisory lock exists to prevent — and a worker
   started against a schema still missing a `pre`-phase migration refuses to
   boot rather than running against a schema behind its code.

## Non-transactional migrations

By default every migration file runs inside its own transaction, so a failure
rolls the whole file back. PostgreSQL forbids some statements inside a
transaction block — notably `CREATE INDEX CONCURRENTLY` and
`DROP INDEX CONCURRENTLY`, the only forms that build or drop an index without
blocking writes on the table. On hot tables (`jobs`, `job_events`) a plain
`CREATE INDEX` takes a `SHARE` lock that blocks `INSERT`/`UPDATE`/`DELETE`
for the duration of a full-table scan and stalls the worker fleet, so index
migrations on those tables should use the concurrent forms.

A migration opts out of the transaction wrapper with a header directive in its
leading comment block (`--` line comments only, before the first SQL token):

```sql
-- taskq:no-transaction
-- NOT redundant with IF NOT EXISTS below: an interrupted CREATE INDEX
-- CONCURRENTLY leaves an INVALID index that IF NOT EXISTS alone would
-- silently skip rebuilding, so drop the debris first.
DROP INDEX CONCURRENTLY IF EXISTS "{schema}".jobs_queue_idx;
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_queue_idx ON "{schema}".jobs (queue);
```

The runner then executes the file **statement by statement, each in its own
implicit transaction** (the same semantics as Alembic's `autocommit_block` or
Rails' `disable_ddl_transaction!`). This changes the failure contract, so
three rules apply:

- **The migration must be idempotent and re-runnable.** Nothing rolls back: if
  the third statement fails, the first two stay applied. The ledger records
  the migration only after *every* statement succeeds, so the next
  `migrate up` re-executes the whole file — every statement must tolerate
  being re-run (`IF NOT EXISTS`, guarded inserts, etc.).
- **An interrupted `CREATE INDEX CONCURRENTLY` leaves an `INVALID` index
  behind.** The standard remedy is drop-and-rebuild, written into the
  migration itself as shown above: the `DROP INDEX CONCURRENTLY IF EXISTS`
  line removes debris from an interrupted attempt before rebuilding. A plain
  `CREATE INDEX CONCURRENTLY IF NOT EXISTS` alone would silently skip the
  rebuild while the invalid index keeps its name. You never have to find
  these by hand: when a run fails, `taskq migrate up` lists any INVALID
  indexes in its failure report.
- **No transaction-control statements.** `BEGIN`/`COMMIT`/`ROLLBACK` (and
  aliases) are rejected before anything executes — they would silently
  re-open a transaction, defeating the directive. The statement splitter
  assumes the server default `standard_conforming_strings=on`.

Operators can see the distinction two ways: `taskq migrate status` annotates
non-transactional migrations with `(no transaction)`, and the
`{schema}.schema_migrations` ledger records how each migration ran in its
`use_transaction` column (`false` = ran outside a transaction). The runner
adds that column when recording the next migration, so deployments upgraded
from older TaskQ versions need no manual step; rows applied before the column
existed read `true`.

## If a migration goes wrong

`taskq migrate up` diagnoses its own failures: it tells you which migration
failed, what state the schema is in, and the one action to take. You never
need to inspect catalog state by hand.

### Transactional migration (the default)

The whole file rolled back automatically, so the schema is exactly as it was
before the attempt. Fix the cause of the error, then re-run `taskq migrate
up`.

### Non-transactional migration (`-- taskq:no-transaction`)

Nothing rolls back: statements before the failure remain applied, and the
migration is **not** recorded. Re-run `taskq migrate up` — the migration is
idempotent, and the command's failure report lists any INVALID indexes the
interrupted attempt left behind; the drop-and-rebuild already written into
the migration cleans them up on the re-run. Only pin `taskq-py` back to the
previous version if the migration SQL itself is wrong and you need time to
ship a correction.

### A migration applied successfully but broke older workers

Restoring from backup is for this scenario: the migration itself succeeded,
but not-yet-upgraded workers cannot run against the new schema. Stop the
workers pointed at the affected schema, restore the pre-migration backup,
and pin `taskq-py` back until every worker is upgraded.

- Stop workers pointed at the affected schema to avoid further writes.
- Restore the database from the pre-migration backup.
- Pin `taskq-py` back to the previous version until the issue is resolved,
  since the previous version's code may not be compatible with the new
  schema.

---

## Breaking import path changes

### `taskq.worker.actor_config` → `taskq.actor_config`

> **Released in v0.2.0–v0.2.2, moved in unreleased.** This is a breaking
> change for anyone importing `ActorConfig` from the old path.

The `ActorConfig` dataclass has moved from `taskq.worker.actor_config` to
the top-level `taskq.actor_config` module. It is a shared carrier used by
the client, CLI, and admin UI — not worker-internal.

**Old (v0.2.0–v0.2.2):**

```python
from taskq.worker.actor_config import ActorConfig
```

**New:**

```python
from taskq.actor_config import ActorConfig
```

The old import path raises `ImportError` — update your imports.

### `taskq.worker.actor_config_ops` → `taskq.actor_config_ops`

The `actor_config_ops` module — listing, inspecting, tuning, and
deregistering actors on a live deployment — has moved from
`taskq.worker.actor_config_ops` to the top-level
`taskq.actor_config_ops`. This module was introduced on the unreleased
branch; if you were importing it from the `worker.*` path during
development, update to the top-level path.

**Old (unreleased branch only):**

```python
from taskq.worker.actor_config_ops import (
    list_actor_configs,
    get_actor_config,
    set_actor_config_capacity,
    deregister_actor,
)
```

**New:**

```python
from taskq.actor_config_ops import (
    list_actor_configs,
    get_actor_config,
    set_actor_config_capacity,
    deregister_actor,
)
```

### State-machine constants: `taskq.backend.statemachine` → `taskq.backend`

> **Unreleased.** Not a breaking change — the old path still imports. This
> is guidance for embedders reviewing their imports against the public API
> surface.

`TERMINAL_STATUSES` (and `ACTIVE_STATUSES`, `VALID_TRANSITIONS`,
`assert_valid_transition`) are re-exported from the public
`taskq.backend` package. The defining module,
`taskq.backend.statemachine`, is an internal layout detail — `taskq.backend`
is the covered surface: `tests/test_backend_init_coverage.py` pins that
every `taskq.backend.__all__` entry resolves and that each re-export is
identical to its submodule source, so that is the path that stays stable.

**Old (internal defining module — works today, not the covered surface):**

```python
from taskq.backend.statemachine import TERMINAL_STATUSES
```

**New:**

```python
from taskq.backend import TERMINAL_STATUSES
```

The same one-line switch applies to `ACTIVE_STATUSES`,
`VALID_TRANSITIONS`, and `assert_valid_transition`.

`TERMINAL_STATUSES` and the `JobStatus` literal alias are additionally
re-exported from the top-level `taskq` package — the shortest spelling
for "is this job done" checks and `JobStatus` annotations:

```python
from taskq import TERMINAL_STATUSES, JobStatus
```

Both paths are the same objects: `tests/test_public_api_exports.py`
pins that the top-level `TERMINAL_STATUSES` **is** (identical to, not
merely equal to) the `taskq.backend` one, so the two import paths can
never diverge.

---

## Breaking API changes

### `validate_actor_payload`: `actor_name=` → `actor=`

> **Unreleased.** Breaking for anyone calling
> `taskq.validate_actor_payload` with the third argument passed **by
> keyword**. Passing it positionally is unaffected.

The public `taskq.validate_actor_payload` export previously resolved to a
duplicate implementation in `taskq.exceptions` that embedded the raw payload
in its exception message and attached pydantic errors with `include_input`.
That message is persisted to the job row's `error_message` and rendered in the
web admin, so attacker-controlled payload values could leak into both. The
export now resolves to the single sanitized implementation in
`taskq._validation`, whose third parameter is named `actor`.

**Old:**

```python
from taskq import validate_actor_payload

model = validate_actor_payload(MyPayload, raw_payload, actor_name="send_email")
```

**New:**

```python
from taskq import validate_actor_payload

model = validate_actor_payload(MyPayload, raw_payload, actor="send_email")
```

The positional form needs no change and works on both versions:

```python
model = validate_actor_payload(MyPayload, raw_payload, "send_email")
```

Three further changes to be aware of, none of which fail loudly:

- **`actor` is now optional** (defaults to `None`), so omitting it no longer
  raises `TypeError`.
- **The exception message is shorter and no longer contains payload values.**
  It is now `Payload validation failed for actor '<name>': <model title>` —
  the pydantic detail and the `Raw payload: {...}` dump are gone. Anything
  that parses `error_message` (log pipelines, alert rules, admin tooling)
  must be updated.
- **`PayloadValidationError.validation_errors` entries no longer carry `input`
  or `url` keys**, because the errors are collected with `include_url=False,
  include_input=False`. Code reading `err["input"]` will now `KeyError`.

`raw_payload` also now accepts an existing `BaseModel` in addition to a
`dict` — a widening, so no action is required.

### `consume_one_job`: payload validation moved before rate-limit acquisition

> **Unreleased.** No action is required for workers. This affects only code
> that calls `taskq.worker._consumer.consume_one_job` directly with
> `validated_payload=None`.

On the normal dispatch path nothing changes: `dispatch_one_job` already
validates the payload and passes `validated_payload`, so `consume_one_job`
never runs its own fallback validation there.

For a direct caller, the fallback `validate_actor_payload` call now runs
*before* the rate-limit acquire block, and `acquire_for_actor` receives the
validated model rather than the raw row dict. Two consequences:

1. **`PayloadValidationError` now escapes before any token is consumed.**
   Previously the consumer acquired first, so an invalid payload burned a
   **non-refunded** rate-limit token (`release_for_actor` sets
   `refund_on_release=False`) for an actor body that could never run. If your
   caller was relying on acquire/release being invoked on the
   invalid-payload path, it no longer is.
2. **Your caller owns the terminal write for the escaping error.** This was
   already true — the outer `try` has no `except` clauses, so the error
   propagated from the in-`try` fallback as well — but it now propagates
   earlier. `dispatch_one_job`'s outer handler and the in-memory test runner
   both already do this.

This also fixes a defect you may have been working around: the registry
previously re-validated the **raw dict** against a keyed ref's own
`payload_type`, discarding the actor model's defaults. An actor model
defaulting `tenant_id="unattributed"` against a ref model requiring
`tenant_id` failed the job non-retryably on a payload that was valid for the
actor. If you added a redundant default to a ref model to work around this,
you can now remove it.

### Keyed refs: `payload_type` is required, `key_fn` receives the model

> **Unreleased.** Breaking for every `KeyedRateLimitRef` /
> `KeyedReservationRef` declaration.

**Old (broken):**

```python
KeyedRateLimitRef(
    base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0
)
```

**New:**

```python
KeyedRateLimitRef.typed(
    MyPayload,
    base_name="api-per-tenant",
    key_fn=lambda p: p.tenant_id,
    capacity=10,
    refill_per_second=1.0,
)
```

`payload_type` is now required, and `key_fn` receives the validated Pydantic
model, not the raw dict. Use `.typed()` for compile-time type checking of
`key_fn` against the payload model.

**Deploy hazard:** if defaults or validators change a key-deriving field's
value vs the raw row, new concrete names materialize fresh full-capacity
buckets alongside old ones (a temporary over-admission window). Drain
affected queues before deploying payload model changes that affect key
derivation.

### Dispatch-path malformed payloads fail immediately

> **Unreleased.** Breaking for callers that relied on invalid payloads
> being retried.

Dispatch-path malformed payloads now fail immediately as
`PayloadValidationError` (non-retryable) instead of being retried as a
generic `ValidationError`. In-flight legacy rows with invalid payloads will
fail on first dispatch instead of exhausting the retry budget.

### `wait_for_batch` defaults to `on_empty="error"`

> **Unreleased.** Breaking for callers that relied on the silent empty
> return.

Previously, calling `wait_for_batch` on a batch_id with zero jobs and no
`batches` row returned an empty `BatchCompletionStatus` silently. The
default is now `on_empty="error"`, which raises `EmptyBatchError`. Pass
`on_empty="ok"` to preserve the old silent-return behaviour.

### Sub-enqueue failure events carry `error_class`/`error_message`

> **Unreleased.** Breaking for log pipelines.

`sub_enqueue_re_enqueue_error` and `sub_enqueue_flush_error` now carry
`error_class` + `error_message` instead of the single `message` field,
matching the `error_class`/`error_message` convention used by every other
error event (`job_timeout`, `job_exception`, `job_failed`,
`rate_limit_release_failed`, `savepoint_rollback_failed`,
`stranded_jobs_query_failed`, and the `failed_details` payload of
`sub_enqueue_flush_failed`). Log pipelines querying `fields.message` on
these two events must switch to `error_message`.

### dotenvmodel 1.x: environment-variable precedence flips

> **Unreleased.** Breaking for deployments that rely on `.env` files
> beating the process environment.

dotenvmodel is bumped to 1.x (`>=1.1.0,<2`), adopting its 1.0 defaults.
Environment-variable precedence flips: the process environment now beats
`.env` files by default (previously `.env` values overwrote `os.environ`);
restore the files-beat-env-vars behaviour with `DOTENV_OVERRIDE=true` or
`TaskQSettings.load(override=True)`. `load()` no longer mutates
`os.environ` — read `TASKQ_*` values from the settings instance, not the
process environment, after a load. `TaskQSettings.load()` now forwards
dotenvmodel's full parameter surface (`env`, `override`, `env_dir`,
`read_dotfiles`, `read_environ`, `load_local`). Subclass string-field
defaults containing `${VAR}` references are interpolated at load time
(unset references resolve to `""`).

### Time is unified on the database clock

> **Unreleased.** Breaking for `Backend` implementors and for callers
> passing absolute `schedule_to_close` datetimes.

`EnqueueArgs.scheduled_at` is now nullable: "immediate" enqueue passes
`None` and the server stamps it (no more client-side `now()` default), and
`Backend` implementations that require a non-`None` datetime fail loudly.
The raw `schedule_to_close` datetime form is deprecated in favour of
`schedule_to_close_interval` (or declaring `retry.time_budget` on the actor
— absolute datetimes cross clock domains and can misbehave under skew);
every enqueue arm writes the deadline from one domain (server clock +
interval). The rate-limit Redis Lua scripts derive `now` from
`redis.call('TIME')` — the caller-supplied `now` ARGV is removed.

### `firstof`/`allof` DST strategies become live

> **Unreleased.** Breaking for schedules that already exist and declare
> them.

The cron tick's `SELECT` never listed the `dst_strategy` column and
`fire_schedule` read it as `row.get("dst_strategy", "skip")`, so the stored
value was **always** `skip` in production whatever the schedule said — the
branch that enqueues the second job for a DST fall-back overlap was
unreachable. The column is now selected and read, so a schedule configured
`allof` or `firstof` years ago changes behaviour on upgrade, with no
configuration change and nothing raised: on the autumn fall-back night an
`allof` schedule enqueues **two** jobs for the repeated local hour where it
used to enqueue one. Audit for non-`skip` schedules before rolling out,
and make sure their actors are idempotent.

### `worker_id` is no longer a metric dimension

> **Unreleased.** Breaking for dashboards, queries and alert rules that
> group or filter by `worker_id`.

`taskq.lock.expires_in_seconds`, `taskq.heartbeat.misses`,
`taskq.leader.election_attempts`, `taskq.leader.election_failures`,
`taskq.cron.lock_contention` and the `taskq.heartbeat.consecutive_failures`
gauge now emit a single undimensioned series each — they return one series
where they used to return one per worker. `worker_id` is a fresh UUID per
worker *process*, so every deploy, restart and autoscale event minted new
time series without bound; Azure Monitor counts each unique (metric,
dimension key, dimension value) seen in 12 hours as an active series, caps
a subscription at 50,000 per region, and throttles ingestion for *every*
custom metric once the cap is passed, with no backfill of what was dropped.
Per-worker attribution is unchanged on the channels where cardinality is
free: `worker_id` is bound onto every log line via contextvars, and
`taskq.worker_id` is a cron-fire span attribute. The `record_*` helpers
still accept their `worker_id` argument — only the dimension is gone.
`taskq.cron.consecutive_failures` is labeled by `actor`, not `schedule_id`
— schedule rows accept any actor string at creation time, so the label is
capped at the emitter like `queue`. A dashboard grouped by `schedule_id`
loses its series; group by `actor` and read per-schedule attribution from
the cron-fire span and the log lines, where cardinality is free.
`cron_auto_disable_threshold` is still evaluated per schedule in the
database.

### `taskq._json.dumps()` requires `str` dict keys

> **Unreleased.** Breaking for raw (unvalidated) dicts with non-`str`
> keys.

`taskq._json.dumps()` no longer passes `OPT_NON_STR_KEYS` to orjson — dict
keys that are not `str` (e.g. int keys in job metadata, actor results, or
progress data) now raise `TypeError` instead of being silently coerced to
string keys. **Callers must stringify keys before enqueue.**

Pydantic-validated payloads are unaffected: `dict[str, ...]` model fields
reject non-`str` keys at validation (they do not coerce), so a payload that
reaches `EnqueueArgs` through `jobs.enqueue` already has string keys.
Non-`str` keys were always lossy on the wire — JSON objects and PG `jsonb`
can only carry string keys — so failing fast surfaces at the boundary what
used to surface as a silently rewritten key on read-back. Dropping the flag
is also 1.29–1.73x faster on str-keyed input (its only effect there). See
the `taskq._json.dumps` docstring for the full reasoning.

### `heartbeat_timeout` is enforced by the reclaim sweep

> **Unreleased.** New enforcement for a parameter that was previously
> accepted and silently ignored.

`heartbeat_timeout`, accepted by every enqueue API (`JobsClient.enqueue`,
the `TaskQ` facade, `SubJobEnqueuer.enqueue()` / `enqueue_batch()`), is now
**enforced**: the leader's reclaim sweep reclaims a running job whose holder
has been silent past the job's `heartbeat_timeout` — exactly as an expired
lock is reclaimed, through the same crash-recovery transitions and the same
`reason='lock_expired'` outbox channel (the event carries
`cause='heartbeat_timeout'`) — even while the global `TASKQ_LOCK_LEASE`
lease is still valid. Previously the value was stored and read by nothing.
A non-positive value now raises `ValueError` at the enqueue boundary
(mirroring `start_to_close`'s rule: a zero-or-negative timeout anchors the
deadline in the past and would reclaim a healthy job on the first sweep
tick). Size it `>= 2x` the fleet's `TASKQ_HEARTBEAT_INTERVAL`. The
supporting partial index ships as migration `01.00.10_01`; fleets that set
no `heartbeat_timeout` keep an empty index and unchanged sweep cost.

### Reservation and rate-limit denials are counters, not event rows

> **Unreleased.** Breaking for anything that consumed per-denial event
> rows.

Reservation/rate-limit denials no longer persist per-occurrence rows: a
denial increments the bounded `snooze_count`/`rate_limit_blocked_count`
counter columns on the job row and emits an OTEL counter; it writes no
`job_attempts`/`job_events` row and no longer raises `max_attempts`.
Anything that consumed per-denial event rows (e.g. dashboards over
`job_events`) must read the counters or OTEL instead.

### Snoozing and denials no longer raise `max_attempts`; both refund the claim's attempt

> **Unreleased.** Breaking for anything that read `max_attempts` as a
> counter that deferrals inflate.

`max_attempts` is now immutable — no code path raises it (the ceiling is
a bound, not a counter). Two behaviours follow from that:

* **Actor-requested deferrals are unbounded, and never spend budget.**
  A `Snooze`, or a `RetryAfter(consume_budget=False)` honouring a
  server 429, refunds the dispatch claim's `attempt` increment
  (`attempt - 1`, floored at 0) — when work is deferred without executing,
  the attempt reservation is released so the job can wait out an unready
  downstream indefinitely: `attempt` oscillates and never walks toward the
  smallint ceiling, and `max_attempts` never moves. Backoff keys off real
  executions only.
* **Admission denials have HTTP-429 semantics.** A reservation or
  rate-limit denial means "come back later": the actor body never ran,
  so the denial refunds the claim's `attempt` increment, spends no retry
  budget, never writes a `job_events` or `job_attempts` row, and never
  by itself terminally fails the job. A denied job is rescheduled with
  backoff for as long as the bucket stays saturated. The single bound on
  a job that is never admitted is its `schedule_to_close` deadline,
  which fails it through the ordinary deadline path — a queue or limiter
  misconfiguration cannot kill a job whose only offence is that the
  fleet was busy more times than its `max_attempts`.

  Sustained contention stays visible on the aggregated counters the job
  row already carries — `rate_limit_blocked_count` and `snooze_count` —
  and on the denial metrics, rather than as one durable row per denial.

  A consuming `RetryAfter` (the default, `consume_budget=True`) is
  unaffected: it is a real execution asking for a known delay, so its
  budget exhaustion still terminally fails the job, deadline or no
  deadline.

### Graceful shutdown interrupts — it no longer terminalises in-flight work

> **Unreleased.** Behaviour change with one additive pre migration
> (`01.00.12_02_pre_job_interrupt_count.sql` — apply it before rolling
> the code, as with every `pre` file).

When a deploy's grace windows expire with a job still running, the worker
no longer writes a terminal state for it. The job is *interrupted*:
released back to the fleet as `pending` (actor unwound on the cancel) or
`scheduled` behind the remaining `TASKQ_TERMINATION_GRACE_PERIOD` budget
(actor never unwound — the row stays unclaimable until the exiting process
is provably gone; with `TASKQ_WATCHDOG_ENABLED=false` the hold is
`TASKQ_LOCK_LEASE`). The claim's `attempt` increment is refunded — the
same idiom the snooze/denial arms use — so a deploy no longer spends a
job's retry budget, and a job interrupted on every deploy is rescheduled
until it finishes or its `schedule_to_close` fails it with
`DeadlineExceeded`. The release writes one `job_events` transition with
`reason = 'interrupted'` and bumps the new `interrupt_count` column on the
row; `taskq.jobs.interrupted{actor,hold}` and
`taskq.jobs.interrupted_noop` are the OTEL counters.

What to audit:

* **`abandoned` now means "operator cancel".** A graceful shutdown never
  produces `cancelled` or `abandoned` for infrastructure reasons; any
  alert or dashboard that reads those statuses as deploy noise should now
  treat them as operator intent. (The abandoned-jobs alert is purely an
  operator-cancel signal now.)
* **Actors that return early on cancel persist that result.** A cancel
  request — operator or deploy — no longer overrides an actor that
  returns a value: returning records `succeeded` and keeps the result.
  Actors that must not keep a partial result on a deploy should read
  `ctx.cancel_origin` and re-raise on `SHUTDOWN` (see
  [cancellation.md](cancellation.md#shutdown-is-not-an-operator-cancel-ctxcancel_origin)).
* **The phase-4 name changed.** `ShutdownPhase.ABANDONING` is now
  `ShutdownPhase.RELEASING` — the integer value `4` is unchanged, so
  `/health` JSON and the CLI keep their numbers, but the `phase="RELEASING"`
  log string and any code importing the old enum member must move.
* **Long actors re-run from scratch on every deploy.** Anything longer
  than `cancellation_grace_period + cleanup_grace_period` is interrupted
  and re-claimed repeatedly; bound such actors with `schedule_to_close`
  or checkpoint via progress state (the released row carries the last
  checkpoint).

---

## Silent behaviour changes

These change what your code *does* without changing what it *accepts*. Nothing
raises, so nothing points you at the call site — audit for them explicitly.

* **`taskq_leader_lock_contention_total` stops rising in a healthy fleet.** It
  previously incremented on every follower's every heartbeat, so the
  sustained-rate alert fired permanently wherever more than one pod ran. It
  now records one event per distinct holder a pod finds in its way.
  Dashboards that graphed the old always-rising counter will go flat.

### `unique_for`'s default `unique_states` now includes `succeeded`

> **Unreleased.** Breaking for actors using `unique_for` without an
> explicit `unique_states`.

The default `unique_states` was `("pending", "scheduled", "running")`; it is
now `("pending", "scheduled", "running", "succeeded")`. `unique_for` reads as
"at most one job for this identity in this period" — leaving `succeeded` out
freed the identity the instant the first job completed, so a re-delivered
webhook or a double-clicked button inside a still-open window could run the
work a second time. The failure states (`failed`, `cancelled`, `crashed`,
`abandoned`) remain excluded: they mean the work did not happen, so matching
them would let one transient failure suppress every later attempt for the
rest of the window. This matches the default every comparable job queue
ships (the completed state is included in the uniqueness check by default).

To keep the old "block only concurrent execution" behaviour, pass
`unique_states=("pending", "scheduled", "running")` explicitly on the actor.

A dedup onto a job that already finished is now surfaced: the enqueue logs a
`WARN`-level `enqueue_deduplicated` line naming the matched status, and
`JobHandle.deduplicated_onto_terminal` is `True`.

### `migrate.apply_pending_locked` defaults to the `pre` phase only

`apply_pending_locked(...)` previously applied **every** pending migration when
called without a `phase` argument; it now applies only `pre`-phase migrations.
The entry point exists to fire on process lifecycle events — a pod restart, a
rollout, an autoscale event — that nobody sequences, and a `post`-phase
migration exists precisely to be withheld until the whole fleet is confirmed
upgraded. Letting a restart apply one would close a rolling-deploy overlap
window mid-rollout (for example, dropping the old single-column idempotency
index while half the fleet still issues `ON CONFLICT (idempotency_key)` takes
that half's entire enqueue path down).

If you call `apply_pending_locked` from your own deploy tooling and relied on
the old all-phases default, pass `phase=None` explicitly to restore it — from a
context that knows the fleet is fully upgraded. The operator-sequenced path is
unchanged: `taskq migrate up --phase post` remains the way to close out a
phased migration.

### Rate-limit refunds now credit the store that paid

If you run `backend="redis"` rate limits with `rate_limit_pg_fallback_enabled`
(the default), refunds were previously credited to Redis even when a Redis
outage had caused the acquire to fall through to Postgres and spend the token
there. Postgres was never repaid — permanently, for a fixed-quota bucket with
`refill_per_second == 0` — and Redis gained a token it never spent.

Refunds now dispatch on the store the acquire actually used. **Expect your
effective quotas to shift after upgrading**: Postgres-side buckets that had
silently drained will recover, and Redis-side buckets that had been inflated
will return to their configured capacity. If you had raised a `capacity` to
compensate for the drift, re-check it against the corrected behaviour rather
than leaving the compensation in place.

Two further refund defects in the same area are fixed: the in-memory and
Postgres log-style sliding-window `refund()` was a silent no-op (it now
properly frees slots), and the Postgres token-bucket `refund()` was likewise
a no-op (it now properly refunds tokens, capped at capacity, via `FOR
UPDATE` on `rate_limit_buckets`). Both were released behaviour — a
release-and-retry cycle never gave the slot back — so fixed-quota buckets
may again admit work that had been permanently locked out.

### `@actor(...)` capacity literals no longer win over the stored row

`max_concurrent`, `max_pending` and `result_ttl` are now **operator-owned**.
`sync_actor_config` seeds them on an actor's first registration and never
writes them again: the UPSERT omits them from its `SET` clause, and a
difference between the code literal and the stored row is logged at INFO as
`actor-config-capacity-override` instead of raising `ActorConfigDriftList`.

This is deliberate — it is what lets an operator retune a live fleet through
`taskq actor set-capacity` (or `taskq.actor_config_ops`) without a redeploy,
and all three fields take effect without a worker restart.

**The upgrade hazard is on schemas that have already run a worker.** There, a
row already exists, so changing an `@actor(max_concurrent=...)` literal and
redeploying now has *no effect* — the stored value continues to win, silently.
On 0.2.2 that same mismatch aborted worker startup, so the failure was loud and
you could not miss it.

Two consequences worth auditing before you upgrade:

- If you tune capacity **in code** and rely on redeploys to apply it, that
  workflow no longer works. Move the value to the operator surface, or clear
  the override to fall back to the literal.
- If an environment variable feeds an `@actor(...)` capacity argument, it stops
  being the effective value on any existing schema.

To see where code and stored rows disagree, check for
`actor-config-capacity-override` in your worker logs — it names every field
whose literal is being ignored. To hand a field back to the code literal, clear
the override: `--clear-max-pending` and `--clear-result-ttl` write NULL, which
their enforcement paths read as *use the `@actor(...)` value*. Note that
`--clear-max-concurrent` does **not** do this — the dispatch SQL reads NULL as
*unlimited*, because it cannot see the code literal once the row exists.

### Schema-qualified advisory locks: adopt by restart

> **Unreleased.** Silent for correctly-deployed fleets; a rolling deploy
> across the rename has a split-leader window.

The advisory-lock names are now schema-qualified —
`taskq:maintenance_leader:<schema>`, `taskq:cron:<schema>`,
`taskq:prune:<schema>`, `taskq:archive_expiry:<schema>`,
`taskq:migrate:<schema>` — replacing the unqualified (`taskq:maintenance_leader`,
…) forms outright. Advisory locks live in a per-database namespace, so the
unqualified names serialized every schema in the database against each other:
two schemas sharing one PG instance meant one schema's leader could silently
starve the other's. The qualified names give each schema its own locks.

**The upgrade hazard is a rolling deploy.** Old and new workers hold different
lock names, so during the roll both an old and a new worker can act as leader
of the same schema at once. The maintenance sweeps stay row-safe in that
window (every snap uses `FOR UPDATE SKIP LOCKED`), but cron gains a
**double-fire window** — its advisory lock is what serialises ticks. Nothing
errors anywhere; the fleet-level signal is `sum(taskq_maintenance_leader_is_leader) != 1`.

**Adopt by restarting the fleet onto the new release, not by rolling it.**
Stop the old workers, start the new ones. The exposure window is the deploy
itself, not the steady state. See
[maintenance-sweeps.md](maintenance-sweeps.md) §5 for the full reasoning and
[runbooks.md](runbooks.md#taskqleaderlockcontention) for the alert whose
remediation carries this note.

### Migration `01.00.06_01` takes write-blocking index locks

> **Unreleased.** Operational note for the `jobs` / `job_attempts` index
> migration; nothing breaks, but the apply can stall fleet writes.

Migration `01.00.06_01_pre_cancel_and_cascade_indexes.sql` adds the indexes
that serve the bounded bulk-cancel/deregistration drains and the stale-worker
cleanup fan-out. It uses plain transactional `CREATE INDEX` — each build takes
a write-blocking lock on its table for the duration, and `jobs` is the hottest
table in the system (enqueue, dispatch and heartbeat all write it). Build
time scales with the current row count: on a large, busy production `jobs`
table the apply can stall the worker fleet's writes for a noticeable window.

- **Apply during a maintenance window**, or when `jobs` is small/quiescent
  (e.g. right after a prune sweep), on any deployment where `jobs` is large.
  Most deployments see momentary builds — the bounded maintenance sweeps keep
  steady-state `jobs` small.
- It is transactional *deliberately*: the `CREATE INDEX CONCURRENTLY` form
  deadlocks under the migration runner's own serialized-migrator advisory
  lock (the concurrent build waits on every transaction that started before
  it, including a second replica's blocking lock wait — a cycle the deadlock
  detector breaks by failing the apply). This follows the
  `01.00.02_01` precedent; the migration file's header carries the full
  derivation.

### Bulk cancel and force-deregistration now make bounded committed progress

> **Unreleased.** Changes the failure semantics of `JobsClient.cancel_where()`
> and `deregister_actor(force=True)`; both remain correct to re-run.

These operations previously did all of their work in one unbounded
transaction; they now drain their match set in bounded committed batches
(`event_writer_batch_size` rows per transaction, each with a server-side
`statement_timeout`). A mid-operation failure therefore leaves the batches
that already committed **as durable partial progress** instead of rolling
everything back — and a re-run continues where the stopped one left off,
because already-cancelled rows fall out of the match set. If you relied on
all-or-nothing semantics (e.g. aborting a tenant offboard on any error and
expecting zero cancels), re-check the returned counts before retrying: they
now reflect only what the completed batches did. See
[maintenance-sweeps.md](maintenance-sweeps.md) for the full semantics,
including why the drain terminates on the window count rather than the
affected-row count.

### Sub-jobs inherit parent tags by default

> **Unreleased.** Silent for code that did not rely on sub-job tags being
> empty.

Every `ctx.jobs.enqueue()` call inside an actor body now propagates the
parent job's tags to the sub-job, making sub-jobs findable by
`JobFilter(tags=...)` and cancellable by `cancel_where`. Pass
`inherit_tags=False` per-call to opt out. This is a behaviour change for any
code that relied on sub-job tags being empty — inherited tags make sub-jobs
visible to tag-based filters and bulk cancels.

### `WorkerSettings` post-load validation runs on every load path

> **Unreleased.** Silent unless a `reload()` or a `validate=False` load
> produces values an earlier load would have accepted.

dotenvmodel is bumped 0.3.0 → 0.5.0 and `WorkerSettings` uses dotenvmodel's
native `post_load()` hook instead of manual `load()`/`load_from_dict()`
overrides. The base `DotEnvConfig._load_fields` invokes `post_load`
automatically on every load path — `load()`, `load_from_dict()`, and
`reload()` — including under `validate=False`, so a `reload()` that produces
invariant-violating values now fails instead of silently succeeding.
`log_format` validation also moved from dotenvmodel's `choices=` constraint
(which `load_from_dict(..., validate=False)` skipped, so an invalid
`TASKQ_LOG_FORMAT` could previously load silently) to a `validator` hook
that runs regardless of `validate=`; its error message is now ``log_format
must be one of ['console', 'json'], got <value>``.

### Every mixed-clock decision is single-arbiter on the store's clock

> **Unreleased.** Silent; behaviour under clock skew changes.

The application process and the database server keep separate clocks that
can diverge or step (VM pause/resume, NTP drift); every place that mixed the
two domains in one decision is anchored to the database clock: workgroup
supervisor freshness is computed server-side (a skewed supervisor host can
no longer kill healthy children); cron ticks read the server clock inside
the leader transaction, with the catch-up cutoff and beyond-window
recompute server-anchored (no fire-loops or silently skipped backlog under
leader-clock skew); rate limiting runs on the store's clock (PG window
predicates and GCRA/token-bucket epoch math are server-side; peeks measure
against the store clock too); prune/archive cutoffs and enqueue-pinned
result TTLs are stamped server-side; the batch COPY path is server-stamped
via an in-transaction fixup (`status`, `created_at`, `scheduled_at`,
`schedule_to_close`, `result_expires_at`), so dedup windows hold under skew.

### The admin session cookie is scoped to the admin mount path

> **Unreleased.** Silent; expect one session lifetime of cookie overlap on
> upgrade.

`taskq_session` carried no `path=`, so it defaulted to `/` and the browser
attached it to every request to the host application that mounts the admin
UI — including routes with no reason to see an admin session. Both SSO
backends now set `path` to their `base_path`, and logout clears it on the
same path (a delete on a different path clears nothing, which would have
left a live session behind). **A stale `path=/` cookie written by a previous
version is not replaced by the new one** — the browser keeps both and sends
both, and the broader one can shadow the narrower until it expires.
Operators upgrading should clear the `taskq_session` cookie, or expect one
session lifetime (`session_max_age_seconds`, default 8h) of overlap.

### `ctx.progress()` no longer blocks the actor

> **Unreleased.** Silent; removes a synchronous Redis round-trip from the
> actor body.

Progress publishing was fire-and-forget in name only — `ctx.progress()`
blocked the actor on a synchronous Redis round-trip. It now publishes via
background tasks with a drain on shutdown.

### Worker failure diagnostics are no longer swallowed

> **Unreleased.** Silent; log volume and log fields change.

Timeout and generic-exception attempts log `job_timeout` / `job_exception`
WARNING events carrying `error_class` / `error_message` /
`error_traceback`; every terminal (non-retryable) failure across all five
handlers emits exactly one `job_failed` ERROR event (`job_id`, `actor`,
`attempt`, `cause`, `error_class`, plus handler context such as
`snooze_count` / `consume_budget` / `bucket_name`) — one alertable event per
dead job, and per-attempt diagnostics at WARNING so retryable attempts
produce zero ERROR noise. Tracebacks are formatted from the explicit
exception object rather than the ambient `sys.exception()`, so handler
invocations outside an `except` block no longer record `'NoneType: None'`.
The `terminal-write-failed` event now includes `job_error_traceback` and
`infra_error_traceback`. Timeout spans (`lifecycle.scheduled` /
`lifecycle.failed`) now report the concrete exception class instead of
hardcoded `TimeoutError`, agreeing with the log fields. Snooze / RetryAfter
/ ReservationUnavailable terminal outcomes and the stranded-jobs leader
sweep also log their failure details instead of continuing silently.

### `on_retry_exhausted` awaits any Awaitable

> **Unreleased.** Silent; non-coroutine Awaitable callbacks now actually
> run.

`on_retry_exhausted` now uses `inspect.isawaitable()` instead of
`inspect.iscoroutine()`, so a callback returning a non-coroutine Awaitable
(e.g. a `Task` or a custom awaitable) is awaited instead of silently
skipped.

### `TaskQ(redis_url=...)` is validated at construction

> **Unreleased.** Silent; invalid URLs fail earlier and with a different
> exception.

The URL routes through `load_from_dict`, so the `RedisDsn` field type
coerces and validates it — an invalid URL now raises `TypeCoercionError`
fail-fast at `open()` (previously a late `ValueError` from redis-py), and an
empty or whitespace-only `redis_url` raises `ValueError` at construction
instead of silently disabling Redis.

### The `.env`-not-found warning filter is narrowed

> **Unreleased.** Silent; real dotenvmodel warnings are visible again.

The `.env`-not-found warning suppression is narrowed to exactly that one
warning — a `logging.Filter` matched on message prefix, instead of raising
the whole `dotenvmodel` logger to ERROR — so real misconfiguration warnings
(e.g. an invalid `DOTENV_*` value) stay visible.

### `start_to_close` now cancels the running actor

> **Unreleased.** Silent unless an actor runs past its deadline.

`start_to_close` now actually cancels the running actor on the transactional
path. `_run_actor_in_tx` wrapped the actor in `asyncio.shield()` *inside*
the `wait_for` enforcing the deadline, and a shield keeps the shielded
awaitable running when its waiter is cancelled — so the timeout applied to
the wait and never to the actor. The attempt was marked timed out and became
eligible for retry on another worker while the original body kept executing,
running every side effect past the timeout point twice. **An actor that
previously ran past its `start_to_close` deadline will now see
`CancelledError` at that deadline**, so any cleanup it needs on interruption
belongs in a `finally`. The autonomous path already used a bare `wait_for`
and is unchanged; the outer `shield(_run_actor_in_tx())`, which decouples
*external* cancellation from an in-flight commit, is untouched.

### Cancel state is cleared on every retry arm

> **Unreleased.** Silent; cancels of jobs that retry mid-cancel now work.

A cancel that escalated to `cancel_phase=2` in the same instant the actor
raised an ordinary retryable exception survived the retry write: `mark_retry`,
`mark_snoozed` and both `mark_retry_after` variants rewrote
`status`/`scheduled_at` but left `cancel_phase` and `cancel_requested_at` on
the row, and a retry reuses the *same* row. The next attempt was therefore
dispatched already at FORCED, so the cancel controller's PG-observation
fast-advance jumped straight to FORCED without ever calling `task.cancel()`
— the job could never be cancelled again, only abandoned while its
coroutine kept running. Both backends now clear the cancel columns on every
retry arm. Terminal arms still keep both columns: they are the audit trail,
and `mark_abandoned`'s `cancel_phase=2` guard reads them.

### Keyed rate-limit `key_fn` errors no longer embed the payload

> **Unreleased.** Silent; error text changes.

The `RateLimitRegistry` "key_fn returned an empty key" `ValueError`
interpolated the whole payload (`for payload {payload!r}`); that exception
propagates into the persisted `error_message` and the web admin through
generic exception handling, and payload values are attacker-controlled. The
message now names only the ref, matching the sanitization contract
`PayloadValidationError` follows in `taskq._validation`.

### Packaging, dependencies, and documentation corrections

> **Unreleased.** Silent; install behaviour changes.

- `humanize` moved from the core install to the `[fastapi]` extra (it was
  bloating core installs).
- `starlette` and `prometheus_client` are declared as direct dependencies
  (they were transitive-reliance).
- Dependency upper bounds are added to `asyncpg`, `redis`, `pydantic`,
  `fastapi`, `typer`, `dotenvmodel`, `uuid-utils`, `uvicorn`, `structlog`,
  `opentelemetry-instrumentation`, `prometheus-client` — a deployment
  pinning a newer version than a bound now fails to resolve instead of
  silently drifting.
- Stale `[web]` extra references in the README and CI were replaced with
  `[fastapi]`; there is no `[web]` extra.
- Docs corrected: `configuration.md` claimed `TASKQ_ENVIRONMENT` selects
  `.env.{env}` files — `ENV` does; `TASKQ_ENVIRONMENT` is a deployment label
  that gates the unauthenticated-admin warning.

### `job_events` rows past the retention period are deleted

> **Unreleased.** Silent; event history older than the retention window
> disappears.

`job_events` rows older than `TASKQ_EVENT_RETENTION_PERIOD` (default 7
days) are now deleted by a leader sweep regardless of parent-job status;
`timedelta(0)` disables it; the crash-reclaim outbox slice
(`kind='state_change' AND detail->>'reason'='lock_expired'`) is carved out
of the ordinary window so an unread reclaim event survives it — but the
carve-out is bounded, not unconditional. The same sweep deletes that slice
once it is older than 100x the retention period
(`RECLAIM_OUTBOX_RETENTION_MULTIPLIER`), because a fleet with no
`watch_reclaims` consumer would otherwise retain every `lock_expired` event
forever. Shortening the retention period shortens that window
proportionally: a `TaskQ.watch_reclaims` consumer lagging past 100x
retention loses events silently, with no error on either side. Size the
retention period against your slowest consumer's worst outage, not only
against event volume.

### TaskQ-built client pools carry a 10 s per-query bound

> **Unreleased.** Silent; a black-holed database now raises instead of
> parking the client forever.

Every pool TaskQ builds for a client — the DSN pool at `TaskQ.open()` and
the `pg_provider` sugar's factory pools — now carries an asyncpg
`command_timeout` of 10 s. Previously a black-holed Postgres (packets
dropped, no RST) parked the client's first `enqueue`/`get`/`cancel`
indefinitely: client processes arm no watchdogs, so nothing converted the
hang into a crash. The query now fails with `TimeoutError` after 10 s.

The trade-off cuts the other way for a slow-but-alive database: a
legitimate client query exceeding 10 s is now cancelled, so size
client-side expectations (and any outer retry) accordingly. The bound is
deliberately above the enqueue-path lock budgets (5 s defaults) — the
server-side `lock_timeout` still fires first, so a lock refusal surfaces
as the typed `MaxPendingLockTimeoutError` / `UniqueForLockTimeoutError` /
`IdempotencyKeyLockTimeoutError` rather than a bare `TimeoutError`. Pools
you supply yourself (`pool=` / `pool_factory=`) stay caller-owned: their
timeouts are your choice.

### The queue-depth alert fires on oldest-pending age, not raw depth

> **Unreleased.** Operator-visible: the bundled `TaskQQueueDepthHigh`
> alert's expression changed; any override of it must be re-expressed.

The bundled Prometheus rule `TaskQQueueDepthHigh`
(`src/taskq/contrib/prometheus/rules.yaml`) previously fired on raw depth —
`taskq_queue_depth > 1000`; it now fires on the oldest pending job's age —
`max by (actor, queue) (taskq_jobs_oldest_pending_age_seconds) > 900`,
sustained for 5 minutes. Depth alone is ambiguous — a deep queue that
drains is healthy throughput — and a queue-summed number cannot tell a
busy shared queue apart from an actor nobody consumes: a misrouted actor
produces no refusal and no failed job, its rows simply pile up pending
while every probe stays green, and its age series rises without bound
while its queue-mates stay flat. The per-`(actor, queue)` attribution
names exactly whose `TASKQ_QUEUES` coverage to check. `taskq_queue_depth`
is still emitted, so dashboards and any rules you wrote against it keep
working — but if you overrode the bundled alert's threshold, re-express
the override in seconds of oldest-pending age.

---

## Bounded inputs

New upper bounds reject input that was previously accepted. Each is a clean
error, not a crash, but a caller that exceeded the bound will now fail.

| Surface | New bound | Failure |
| --- | --- | --- |
| `taskq queues set-max-concurrent --max-concurrent` | `>= 1` (was `>= 0`) | typer argument-parsing error, exit 2 |
| `taskq.worker.queue_ops.set_queue_max_concurrent` | `>= 1` or `None` (was `>= 0`) | `ValueError` |
| `SubJobEnqueuer.enqueue_batch(items)` | at least 1 item | `ValueError` |
| `BatchFilter(limit=...)` | `<= 500` (default 100, `0` still means "no rows") | `ValueError` at construction |
| Admin `/jobs`, `/jobs/count`, `/history` — `status` | values outside the closed status set | HTTP 400 |
| Admin `/jobs` — `tags` | 255 chars each (no item-count cap) | HTTP 400 |
| `JobFilter` — `queue`, `actor`, `identity_key`, `tags` | no NUL bytes | `ValueError` in `__post_init__` |
| `ScheduleCreateArgs` — `actor`, `name`, `timezone`, `payload_factory`, `identity_key` | no NUL bytes | `ValueError` in `__post_init__` |
| Admin text filters — `actor`, `queue`, `search`, `identity_key`, `fairness_key`, `tags` | no NUL bytes | HTTP 400 |
| `ScheduleCreateArgs.dst_strategy` | a value in `taskq.cron.DST_STRATEGIES` | `ValueError` in `__post_init__` |
| `BatchHandle.status()` / `wait_for_batch()` — `schema` | schema-identifier regex | `ValueError` |
| `taskq.testing.pg` — `schema` | schema-identifier regex | `ValueError` |

Notes:

- **`max_concurrent=0`.** `0` previously passed both the CLI and the ops-layer
  guard and then hit the table's `CHECK (max_concurrent IS NULL OR
  max_concurrent >= 1)`, producing a raw asyncpg `CheckViolationError`
  traceback — so scripts passing `0` were already failing, just messily. `NULL`
  (via `--clear`) is the uncapped state; an emergency drain to `0` belongs to
  the per-actor `taskq actor-config set --max-concurrent 0`, which still
  accepts it.
- **`enqueue_batch([])`.** Previously returned `[]` silently when no connection
  was in play, while `JobsClient.enqueue_batch` already raised. Guard the call
  site if your item list can legitimately be empty.
- **Admin `status` filter.** Values are deduplicated in first-occurrence
  order, so a request repeating a status still succeeds and returns the same
  rows — only requests containing a value outside the closed status set are
  rejected, and the dedup alone bounds the list to the set's eight members,
  however long the request is.
- **NUL bytes.** Previously these reached Postgres and came back as an opaque
  asyncpg `22021` — a 500 from the admin routes. The values were never
  storable; the rejection surfaces the fault at the boundary instead.
- **`dst_strategy`.** An unrecognized strategy previously constructed fine and
  took the default branch at cron-tick time. The known set is newly exported
  as `taskq.cron.DST_STRATEGIES`. A schedule that already declares
  `firstof`/`allof` also changes firing behaviour — see
  [Breaking API changes](#breaking-api-changes).
- **SQL interpolation guards.** The `schema` parameter of the batch public API
  (`BatchHandle.status()`, `wait_for_batch()`) was previously interpolated
  without validation — a SQL-injection surface; it is now validated against
  the canonical schema-identifier regex before interpolation, and
  `taskq.testing.pg` follows the same rule.

---

## Configuration that no longer loads

`WorkerSettings` gained several load-time validators. A configuration
containing any of the values below **stops the worker from starting** — you
get a settings-load error instead of the opaque mid-startup failure it used
to produce. Check these before rolling out, not during.

| Setting | Now rejected | Previously |
| --- | --- | --- |
| `schema_name` | longer than 63 characters | Postgres truncated it silently; Redis channel templates used the full string, so the two stores diverged |
| `workgroup_instance` | not a valid UUID | raw `ValueError` mid-registration |
| `worker_label` | contains a NUL | opaque asyncpg `22021 CharacterNotInRepertoireError` at startup |
| `queues[*]` | outside the queue-name charset | accepted; jobs stranded on a queue no worker drains |

The queue-name charset is: letters, digits, `_`, `.`, `-`, with the first
character a letter or `_`.

Workgroup TOML configs follow the same load-time rule through
`WorkgroupConfig.from_toml`:

- `[[workers]] name` is capped at 43 characters. The supervisor binds a
  health socket for every child at
  `/tmp/taskq_health_<name>_<uuid>.sock` — 60 fixed chars around the
  name — and the path must fit every supported platform's AF_UNIX
  `sun_path` budget (104 bytes on macOS/BSD, 108 on Linux, NUL
  included). A name of 44-64 chars previously loaded and then died at
  child spawn with `OSError: AF_UNIX path too long`, restart-looping
  against the burst budget until give-up; longer names still do.
- `queues = []` (on a worker or in `[defaults]`) is refused. A worker
  that consumes no queue dispatches nothing; omit the key to inherit
  `[defaults].queues`, or `["default"]` when no default is set.

Both raise `ValueError` at config load — from `taskq workgroup start` /
`taskq workgroup validate` before any process spawns.

Two further invariants apply **only when `watchdog_enabled=True`**:

- `watchdog_loop_lag_budget + heartbeat_interval` must be `< lock_lease`. A
  stalled event loop has to die before its leases expire, otherwise the leader
  sweep reclaims live jobs' locks mid-stall and the worker wakes to find its
  work reassigned.
- `watchdog_loop_lag_budget` must be `> watchdog_check_interval`. The lag
  detector samples once per check interval and schedules the beat it measures
  from the same poll, so a healthy loop's observed lag is roughly the check
  interval by construction. A budget at or below the sampling period trips on
  health rather than on stalls — measured, a budget of `1.0` against the
  default `1.0` s check interval force-exits an idle worker on its first armed
  poll.

If you tune either watchdog knob, move both together and keep the lag budget
comfortably inside `lock_lease`.

These raise dotenvmodel's `ValidationError` / `MultipleValidationErrors`, not
`ValueError`. The cross-field invariants (`lock_lease >= 4 *
heartbeat_interval`, the grace-budget checks) previously raised `ValueError`,
so callers that catch `ValueError` around `WorkerSettings.load*()` will no
longer catch them — catch `DotEnvModelError` (the common base) to cover both
single and aggregate cases, or `ValidationError` when at most one invariant
can fire. `ConstraintViolationError` (field validators) was already not a
`ValueError`; field-level validation (`prune_retention_*`,
`default_start_to_close`, `log_format`, etc.) already raised it and is
unaffected. See the dotenvmodel 1.x note under
[Breaking API changes](#breaking-api-changes) for the dependency-level
changes.

---

## Structured-log event rename: `state_change` → `state-change`

> **Unreleased.** Breaking for log pipelines. `state_change` shipped in
> v0.2.2.

The job state-transition event is now emitted as `state-change`, from every
backend that logs a transition — one name now covers the Postgres and
in-memory paths alike. **Nothing fails; your saved searches and alert rules
simply stop matching.** Update any query, dashboard panel, or alert rule that
selects on `state_change` before upgrading, or you lose visibility silently.

Three other event names were kebab-cased in the same pass —
`batch_streaming_enqueued`, `pg_credential_refresh_failed`, and
`cancel_where_notify_failed` — but none of them ever appeared in a release, so
no consumer can be matching the old spellings.

---

## Trailing newlines in queue names, tags, and keyed keys

The queue-name, tag, and keyed rate-limit key regexes are now anchored
`\A...\Z` instead of `^...$`. Python's `$` also matches immediately before a
trailing newline, so `"default\n"`, `"mytag\n"`, and `"key\n"` all satisfied
the old patterns and are now rejected with a `ValueError`.

The realistic sources of a stray trailing newline are a shell `$(...)`
substitution, a value read from a file, and an unstripped environment
variable:

```python
# This used to pass validation and now raises ValueError:
queue = pathlib.Path("/etc/taskq/queue").read_text()  # "default\n"

# Strip at the boundary:
queue = pathlib.Path("/etc/taskq/queue").read_text().strip()
```

Note that a queue name with a trailing newline was never actually *usable* —
jobs enqueued onto it were stranded, since no worker's `queue = ANY($1)` ever
matched. The new error surfaces a fault that was previously silent.

Separately, queue names are now validated at both the enqueue and the
actor-declaration chokepoints. The `QueueName` annotation is
inert at runtime (its `AfterValidator` only fires inside pydantic model
validation), so a typo'd queue name previously sailed through. It now raises
at decoration time — **import time in the common case**, so a typo that used
to strand jobs quietly will now stop your process from starting.

---

## Unreleased features

The features and notes below land with the next release. The canonical
release notes for every release are generated by release-please from the
repository's conventional commits — `CHANGELOG.md` is generator-owned, and a
hand-written block there is invisible to the release-notes pipeline — which
is why the pending notes live in this guide. At release time the generated
changelog becomes the authoritative record and these notes age out.

### Jobs and batches

- **`JobsClient.cancel_where(filter, reason)`** — bulk cancel all jobs
  matching a `JobFilter` in a single set-based operation. Pending/scheduled
  jobs go straight to terminal `cancelled`; running jobs get cooperative
  cancel (`cancel_phase=1`). Returns `BulkCancelResult` with counts and
  affected IDs. Empty filters are rejected with `EmptyFilterError` unless
  `allow_empty_filter=True` is passed. `BulkCancelResult` and
  `EmptyFilterError` are exported from the `taskq` top level.
- **Batch failure policies (`AbortBatchAfter`)** — an opt-in `failure_policy`
  parameter on `enqueue_batch()` / `enqueue_batch_streaming()` creates a
  `batches` row and drives abort-on-consecutive-failure semantics via the
  `apply_batch_terminal_outcome` hook. When the threshold is reached the
  batch is aborted: pending/scheduled child jobs are cancelled and the batch
  row is set to `aborted`.
- **Batch finalizer (transactional enqueue with batch)** — a `finalizer`
  parameter on `enqueue_batch()` / `enqueue_batch_streaming()` enqueues a
  finalizer job alongside the batch in the same transaction. The finalizer
  is NOT stamped with `batch_id` (deadlock prevention); `wait_for_batch`
  automatically excludes it from counts via the batch row's
  `finalizer_job_id`.
- **Batch discovery (`list_batches`, `BatchSummary`)** —
  `JobsClient.list_batches(BatchFilter)` returns `BatchSummary` objects with
  live job-count aggregates. `BatchFilter` carries only batch-relevant fields
  (`queue`, `active`, `batch_id`, `limit`).
- **`enqueue_batch_streaming` for unbounded iterables** — accepts an
  `Iterable[EnqueueItem]` (including generators) and inserts in chunks of
  `chunk_size` (1–1000). All items share the same `batch_id`.
- **`wait_for_batch` with `expect_at_least`, `on_empty`, `exclude_job_id`** —
  `expect_at_least` raises `EmptyBatchError` when fewer than the expected
  number of jobs are present; `on_empty` controls behaviour when zero jobs
  and no `batches` row exist (`"error"` raises, `"ok"` returns empty
  status); `exclude_job_id` omits a specific job from counts (defaults to
  the batch row's `finalizer_job_id`).
- **Backend protocol batch methods (10 new methods)** —
  `enqueue_batch_atomic`, `create_batch`, `increment_batch_failures`,
  `reset_batch_failures`, `abort_batch`, `complete_batch`, `get_batch`,
  `list_batches`, `count_batch_non_terminal`, `prune_old_batches`.
- **Batches table migration (01.00.05_01)** — adds the `batches` table with
  columns for status tracking, failure counters, finalizer linkage, and
  batch-level metadata.

### Sub-job enqueueing

- **`SubJobEnqueuer.enqueue()` now accepts `tags`, `inherit_tags`,
  `schedule_to_close`, and `start_to_close` parameters.** Sub-jobs inherit
  the parent job's tags by default (`inherit_tags=True`); pass
  `inherit_tags=False` to suppress inheritance for a specific sub-job.

### Managed identities and connections

- **Connection hook points for managed-identity / BYO connections** —
  `WorkerConnections` dataclass with per-role pre-constructed resources
  (caller-owned) or zero-arg async factories (TaskQ-owned) for the worker's
  three PG pools, notify/leader dedicated connections, and Redis client.
  `worker_main(..., connections=...)` and `open_worker_deps(...,
  connections=...)` accept it; fields left `None` fall back to DSN
  construction. `PoolFactory`, `ConnFactory`, `RedisFactory` type aliases
  are exported from the `taskq` top level.
- **Vendor-neutral credential provider abstraction (`taskq.auth`)** —
  `PgCredentialProvider` and `RedisCredentialProvider` async Protocols with
  reusable `make_pg_pool_factory`, `make_dedicated_conn_factory`,
  `make_redis_client_factory` builders. Any provider implementing the
  Protocols gets all factory builders for free. The PG factories pass the
  credential to asyncpg as `user=` / `password=` keyword arguments (which
  take precedence over both DSN userinfo and DSN query parameters), so the
  token never appears in the DSN string; `enrich_pg_dsn` remains as the
  string-helper variant (writes the credential into DSN userinfo; adds
  `sslmode=require` only when the DSN has no explicit sslmode —
  `verify-full` is never downgraded). All four helpers are exported from the
  `taskq` top level as well as `taskq.auth`.
- **Per-worker Postgres credential providers in workgroup configs** — a
  `pg_credential_provider = "module:attr"` key on a `[[workers]]` entry (or
  the `pg_credential_provider=` field on `WorkerSpec`) is forwarded to that
  worker's child command line as `--pg-credential-provider`, so two workers
  in one workgroup can use different providers.
- **`taskq[aad]` extra** — `taskq.aad` module with Microsoft Entra ID
  providers (`EntraIdProvider`, `EntraIdPgProvider`, `EntraIdRedisProvider`)
  backed by `azure.identity.aio` (the extra includes `aiohttp`, required by
  the async credentials). Providers constructed with `credential=None`
  lazily create one `DefaultAzureCredential` and reuse it; sync
  `azure.identity` credentials are supported and offloaded to a thread. See
  [managed-identities.md](managed-identities.md).
- **`taskq[aws]` extra** — `taskq.aws` module with `RdsIamProvider` for AWS
  IAM RDS Postgres authentication, backed by `boto3`.
- **`taskq[vault]` extra** — `taskq.vault` module with
  `VaultDynamicDbProvider` for HashiCorp Vault database secrets engine
  dynamic credentials, backed by `hvac`.
- **`TaskQ` stream hooks** — `pg_conn_factory` and `listen_conn` parameters
  for the LISTEN/NOTIFY transport in `TaskQ.stream()`, so pool-only / AAD
  deployments can stream without a DSN. `stream()` now uses
  `contextlib.aclosing` to ensure the inner generator's `finally` (conn
  close) runs promptly on early return.
- **`migrate.apply_pending_locked` hooks** — `conn` (caller-owned) and
  `conn_factory` (TaskQ-owned) parameters replace the DSN-only path.
- **Credential hot-reload (SIGHUP / interval / programmatic)** — hot-swaps
  every factory-backed PG pool, dedicated connection, and Redis client with
  freshly-built replacements (each factory fetches a fresh credential).
  Triggers: SIGHUP; `TASKQ_RELOAD_INTERVAL` (seconds, unset by default) for
  periodic reloads with no external signal — the only rotation path on
  Windows; and `WorkerDeps.request_reload()` / `reload_credentials(deps)`
  for embedders. Each factory call is bounded by
  `TASKQ_RELOAD_FACTORY_TIMEOUT` (default 30 s). The swap is atomic: the old
  pool stops serving new acquisitions immediately and is closed in the
  background with a bounded drain (default 5 s), then terminated — an
  in-flight actor that outlives the drain sees its next acquire fail and
  the job retries on the new pool. DI-injected `db: asyncpg.Pool` actors
  resolve the new pool (LOOP-scope cache refresh) and progress flushing
  follows the swap. A SIGHUP arriving mid-reload (success or failure)
  triggers exactly one follow-up reload; reloads are skipped while shutdown
  is in progress. Each resource reloads independently — one factory failure
  is logged and does not abort the rest; the `credentials-reloaded` log
  line's `failed` field reports any resource that didn't rotate.
  Caller-owned resources are not swapped.
- **NOTIFY listener resilience** — the reconnect loop rebuilds a dropped
  LISTEN connection through the user-supplied `notify_conn_factory` (or the
  DSN closure it was opened with) instead of a stale/absent DSN. A
  caller-owned `notify_conn` that drops disables the listener (poll-based
  dispatch fallback) instead of crashing the worker.
- **Ownership-contract enforcement** — caller-owned pools/connections/Redis
  clients are never closed by TaskQ (including shutdown paths). A
  caller-owned `leader_conn` with no `leader_conn_factory` and no
  `pg_dsn_direct` is a startup `ValueError` (no rebuild path). TaskQ-owned
  dedicated connections (DSN- or factory-built) get TCP keepalive.
- `taskq.worker` re-exports `WorkerConnections` and `reload_credentials`
  (lazy, alongside the existing `WorkerDeps` / `open_worker_deps`).

### Actor and retry hooks

- **`ErrorReporter` Protocol** for vendor-neutral terminal failure routing
  (Sentry, Datadog, DLQ) with `NullErrorReporter` default and a
  `taskq.error_reporter.failures` OTel counter. `report()` takes
  `(job, exception)` — the same argument order as `on_retry_exhausted` —
  and is guarded by the `error_reporter_timeout` setting (default 3 s).
- **`retry_classifier` hook on `@actor`** for exception-instance-level
  retry classification (inspect attributes like HTTP status codes, return
  `RetryOverride` to refine kind/delay per occurrence). Non-`RetryOverride`
  returns are caught and logged rather than crashing, and the hook is
  skipped for `non_retryable_exceptions` and `PayloadValidationError`,
  matching the documented contract. `RetryOverride` and `RetryClassifierHook`
  are exported from the `taskq` top level.
- **`on_success` hook on `@actor`** for success callbacks (mirrors
  `on_retry_exhausted` with timeout guard).
- **`start_to_close` per-attempt execution timeout** with precedence chain:
  per-enqueue > `@actor(start_to_close=...)` > `TASKQ_DEFAULT_START_TO_CLOSE`
  worker fallback.
- **`KeyedReservationRef`** for dynamic per-key (session/tenant) concurrency
  caps computed from job payload at dispatch time.
- **`max_keyed_reservations` setting** to guard against unbounded keyed
  reservation growth.

### Cron, queries, and admin

- **`name` and `identity_key` fields on `CronScheduleSpec`** for per-property
  cron schedules and cron↔on-demand dedup.
- **`JobSortField` enum and `JobFilter.order_by`** for "latest run by
  business key" queries.
- **Admin UI security settings — `admin_actions_enabled` and
  `admin_ui_require_auth`.** The admin UI fails closed by default in non-dev
  environments: `admin_ui_require_auth=True` (default) raises `RuntimeError`
  at startup when no `auth_dependency` is configured, with explicit opt-out
  `TASKQ_ADMIN_UI_REQUIRE_AUTH=false`; the health endpoints follow the same
  fail-closed pattern — `health_require_token=True` (default) raises
  `RuntimeError` in non-dev when `health_token` is empty
  (`TASKQ_HEALTH_TOKEN` / `TASKQ_HEALTH_REQUIRE_TOKEN=false` to opt out).
  Destructive admin actions (run-schedule, retry-job, cancel-job) are gated
  behind `admin_actions_enabled` (default `False`); `POST
  /schedules/{id}/run` checks the schedule's `enabled` flag and has
  per-process cooldown rate limiting, and its cron `payload_factory` error
  redirect uses a generic error code instead of reflecting exception text.
- **`TASKQ_ADMIN_UI_SECURE_COOKIES` (`admin_ui_secure_cookies`, default
  `True`)** — sets the `Secure` flag on the admin UI's CSRF cookie. The flag
  was previously derived from `request.url.scheme`, so behind a
  TLS-terminating edge (Azure Application Gateway, App Service) the app saw
  plain `http` and silently dropped `Secure` on exactly the deployments that
  need it — while the session cookie, which already used a configured flag,
  kept it. Set it to `False` only for local http dev, where a `Secure` cookie
  is rejected by the browser and the UI stops working. A one-shot
  `admin-ui-cookie-scheme-mismatch` warning fires when the configured value
  contradicts the observed scheme; run uvicorn with `--proxy-headers` so
  `X-Forwarded-Proto` is honoured.
- **`TASKQ_ADMIN_UI_FRAME_ANCESTORS` (`admin_ui_frame_ancestors`, default
  `none`)** — who may frame admin pages. Every admin response now carries
  `Content-Security-Policy: frame-ancestors '<value>'` and the legacy
  `X-Frame-Options` (`DENY` for `none`, `SAMEORIGIN` for `self`). **Admin
  pages can no longer be iframed**: a host application that embeds the admin
  UI in its own dashboard must set `TASKQ_ADMIN_UI_FRAME_ANCESTORS=self` or
  the frame renders blank. Only `none` and `self` are accepted; anything
  else fails at settings construction rather than silently emitting no
  header. CSRF is no defence against UI redress — the framed page is the
  real, authenticated, same-origin page, so a tricked click carries a valid
  token.
- **SSO / SAML auth for admin UI** — OIDC backend (`taskq[oidc]`): PKCE
  flow, JWKS validation, signed-cookie sessions; SAML backend
  (`taskq[saml]`): python3-saml, SP metadata, attribute extraction; shared
  `AuthBundle`/`IdentityClaims` abstraction (both backends use the same
  session handling and group/role allowlist); `token_auth()` helper for
  machine-to-machine bearer-token auth; `TASKQ_SSO_BACKEND=none/oidc/saml`
  CLI integration for standalone `taskq ui serve`. The `taskq[oidc]` extra
  no longer installs `httpx`; its `authlib` floor is now `>=1.8.0` (authlib
  1.8.0's `httpx_client` integration is httpx2-first, the direct OIDC calls
  use `httpx2`, and nothing under `src/taskq` imports `httpx`).
  `OIDCSettings`/`SAMLSettings` are separate DotEnvConfig classes with
  prefix scoping.

### Testing and documentation

- Consolidated testing guide ([testing.md](testing.md)).

### Fixes and internal notes

- `_di/solver.py` debug log now reports the real `cache_hit` value instead
  of a hardcoded `False`.
- `worker/_leader_sweeps.py` logs a warning on invalid schema and includes
  error detail in exception handlers.
- `worker/notify.py` logs debug on NOTIFY payload parse failures.
- Test containers are shared singletons: one Postgres and one Dragonfly
  container per pytest invocation, shared across all xdist workers (filelock
  refcount, stale-leftover sweep) with per-module database and per-test
  schema isolation preserved — full suite ~152 s vs the ~226–240 s baseline.
- Docker/testcontainers calls in tests run off the event loop
  (`asyncio.to_thread`) — docker-py's blocking HTTP round-trips no longer
  stall the event loop mid-test.
- Behavioral timing tests assert in a single clock domain (one statement
  reads the server clock and the row together), so application/database
  clock divergence cannot corrupt an assertion; liveness freshness is
  bounded by the missed-at-most-one-tick contract.

### Bulk enqueues partition `max_pending` admission per actor

> **Unreleased.** Breaking for handlers of bulk cap refusals.

`enqueue_batch()` / `enqueue_batch_fast()` (and the chunked arm of
`enqueue_batch_streaming()`) now admit the within-cap actors' items and
refuse only the over-cap actors' items as whole groups, raising
`BatchMaxPendingExceededError` **after** the admitted items are stored.
Previously one capped actor aborted the whole call with
`MaxPendingExceededError` and nothing enqueued.

- `except MaxPendingExceededError` no longer catches bulk cap refusals —
  the new error is deliberately not its subclass. Catch it explicitly; it
  names each refused actor (`refusals`), the refused item indices
  (`refused_indices`), and the admitted count (`admitted_count`).
- `except BackpressureError` now catches an error under which part of the
  batch is stored: a handler that blindly retries the whole batch
  duplicates the admitted items. Consult `admitted_count` /
  `refused_indices` and retry only the refused items, or rely on
  `idempotency_key`s.
- Durability is path-dependent: committed when the call owned its
  transaction; uncommitted on a caller-supplied open transaction (that
  transaction decides); on the streaming no-connection path a refusal
  surfaces after a durably committed chunk prefix (indices are
  stream-global). The atomic path keeps the legacy all-or-nothing
  contract and still raises plain `MaxPendingExceededError`.

### `taskq.cron.consecutive_failures` is relabeled and bounded

> **Unreleased.** Breaking for dashboards and alert rules keyed on the
  old label.

The metric's dimension is now the schedule row's `actor` (bounded to the
first 100 distinct names a worker process sees; later names collapse
onto the fixed `_other_` value) instead of the per-schedule UUID.
Dashboards grouping by `schedule_id` lose their series on upgrade.
Per-schedule attribution lives on the `cron fired` / `cron fire failed`
log lines and the `cron fire` span's `taskq.cron_schedule_id` attribute.
The per-actor balance can carry permanent residue from disabled,
re-enabled or deleted schedules — `cron_schedules.consecutive_failures`
and the logs are authoritative; alert on
`taskq.cron.disabled_schedules > 0` rather than on this balance.
