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

4. **Apply migrations explicitly**, or let the worker apply them at startup
   via `TASKQ_MIGRATE_ON_START=true`:

   ```shell
   taskq migrate up
   ```

   The command is idempotent — migrations already recorded in
   `{schema}.schema_migrations` are skipped. See [cli.md](cli.md#taskq-migrate-up)
   for the full option reference (`--phase`, `--target`, `--max-steps`).

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

---

## Silent behaviour changes

These change what your code *does* without changing what it *accepts*. Nothing
raises, so nothing points you at the call site — audit for them explicitly.

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
| Admin `/jobs`, `/jobs/count`, `/history` — `status` | at most 8 values | HTTP 400 |
| Admin `/jobs` — `tags` | at most 16 items, 255 chars each | HTTP 400 |

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
- **Admin `status` filter.** Values are now deduplicated in first-occurrence
  order, so a request repeating a status still succeeds and returns the same
  rows — only requests with more than 8 total values are rejected.

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
`ValueError` — see the dotenvmodel 1.x note in the changelog if you catch
around `WorkerSettings.load*()`.

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
`@actor(queue=...)` declaration chokepoints. The `QueueName` annotation is
inert at runtime (its `AfterValidator` only fires inside pydantic model
validation), so a typo'd queue name previously sailed through. It now raises
at decoration time — **import time in the common case**, so a typo that used
to strand jobs quietly will now stop your process from starting.
