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
