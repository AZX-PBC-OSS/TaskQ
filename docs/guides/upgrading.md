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

## If a migration goes wrong

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
