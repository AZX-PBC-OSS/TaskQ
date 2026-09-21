# TimescaleDB Hypertables (Optional)

## What this feature is

TaskQ's retention tables (`jobs_archive`, `job_attempts_archive`,
`job_events`) are plain Postgres tables by default, and their retention is
owned by the row-level sweeps: the prune sweep moves terminal jobs into
`jobs_archive`, the archive expiry sweep hard-deletes rows whose `expire_at`
passed, and the event TTL sweep deletes `job_events` rows past
`event_retention_period`, all in bounded batches.

On a server with the TimescaleDB extension available, TaskQ can convert those
three tables into hypertables (partitioned on their time columns) and register
Timescale retention policies, so aged data leaves the database as whole-chunk
drops instead of long-window `DELETE`s. This is an opt-in feature:
`TASKQ_TIMESCALEDB_HYPERTABLES=true`.

Vanilla Postgres remains fully supported and is the default. Nothing in this
document is required to run TaskQ.

## The opt-in flag

| Setting | Default | Meaning |
|---|---|---|
| `TASKQ_TIMESCALEDB_HYPERTABLES` | `false` | When `true`, the `taskq migrate up` deploy step requires the TimescaleDB extension and converts the retention tables. When `false` (the default), setup issues zero new statements against the server and behavior is identical to plain Postgres. |

The flag is read by the deploy step only. Workers and clients never consult
it, and no runtime code path branches on it: the sweeps keep running unchanged
on both modes (see "The sweeps still run" below).

## Enabling on Azure Database for PostgreSQL Flexible Server

1. Allow the extension on the server: add `timescaledb` to the
   `azure.extensions` server parameter, then create it once with an
   administrative role:

   ```sql
   CREATE EXTENSION IF NOT EXISTS timescaledb;
   ```

2. Set `TASKQ_TIMESCALEDB_HYPERTABLES=true` in the environment the deploy
   step (`taskq migrate up`) runs with, alongside the retention settings the
   conversion derives from (`TASKQ_ARCHIVE_RETENTION_PERIOD`,
   `TASKQ_EVENT_RETENTION_PERIOD`). The deploy step loads the worker settings
   model for this step, so the deploy environment and the workers'
   environment cannot disagree.

3. Run `taskq migrate up`. The conversion runs inside the migration advisory
   lock, after pending migrations apply.

## Local development

Any Postgres with the extension works. The suite pins
`timescale/timescaledb:2.30.1-pg18` (the newest tagged release matching the
suite's Postgres 18 image); the tests read `TASKQ_TEST_TIMESCALEDB_IMAGE` when
set, so CI and local runs can point at their own tag:

```sh
docker run -d --name taskq-timescale -p 5432:5432 \
  -e POSTGRES_USER=taskq -e POSTGRES_PASSWORD=taskq -e POSTGRES_DB=taskq \
  timescale/timescaledb:2.30.1-pg18
```

The image preloads the extension (`shared_preload_libraries=timescaledb`), so
the flag alone is enough: `TASKQ_TIMESCALEDB_HYPERTABLES=true taskq migrate up`.

## What the conversion does

Three tables, each partitioned on its own time column, with chunk intervals
derived from the retention settings (retention / 4, clamped to a range of one
day to thirty days so a retention window spans a handful of chunks and a chunk
drop stays bounded work):

| Table | Partition column | Uniqueness | Retention policy from |
|---|---|---|---|
| `job_events` | `occurred_at` | `UNIQUE (id, occurred_at)` | `event_retention_period` |
| `jobs_archive` | `finished_at` | `UNIQUE (id, finished_at)` | `archive_retention_period` |
| `job_attempts_archive` | `started_at` | `UNIQUE (job_id, attempt, started_at)` | `archive_retention_period` |

The details that matter:

* **The migration ledger is untouched.** The conversion is setup-time,
  idempotent DDL outside the checksummed ledger, run on every deploy. The
  ledger's files stay identical on both engines; there is no migration whose
  body depends on the server's capabilities. A conversion cannot live in the
  ledger because the ledger applies each file exactly once, and opt-in happens
  at an arbitrary point in a database's life; re-running a deploy converges
  the schema instead (changed retention settings are honored on the next
  deploy, already-converted tables are skipped).
* **Uniqueness widens, semantics hold.** A hypertable requires the partition
  column in every unique constraint, so `jobs_archive`'s `PRIMARY KEY (id)`
  becomes `UNIQUE (id, finished_at)`. The re-archive guarantee the bare
  primary key enforced is enforced instead by an explicit `NOT EXISTS` guard
  in the archive write: a job id that already holds an archive row is never
  archived again. The same guard runs on vanilla Postgres, so the prune's
  ghost semantics (fold an already-archived id, never wedge, never duplicate)
  are identical in both modes and pinned by the same tests.
* **One foreign key drops.** No table may reference a hypertable, so
  `job_attempts_archive`'s foreign key to `jobs_archive(id) ON DELETE CASCADE`
  is removed. Chunk retention replaces the cascade: both tables register
  policies from the same `archive_retention_period`. The alignment is by
  chunk, not by row: an attempt whose `started_at` precedes its job's
  `finished_at` can outlive the parent's chunk by up to one chunk interval.
  `job_events`' foreign key to `jobs(id) ON DELETE CASCADE` is kept
  (hypertable-to-regular-table foreign keys are supported) and enforcement is
  unchanged.
* **The first conversion rewrites populated tables.** Conversion uses
  `migrate_data`, which takes an access-exclusive lock and rewrites existing
  rows into chunks inside the deploy step. Size the deploy window for the
  archive's current row count; an empty or young schema converts in seconds.

## Retention is owned by the policies, mostly

Once the tables are hypertables, the aged end of the timeline is owned by
Timescale's background workers: each policy drops chunks whose time range
fully passed its retention. Two consequences to know:

* **The retention clock is the partition column, not `expire_at`.** Archive
  chunks drop on `finished_at` age; a row's `expire_at` stamp (archive time
  plus `archive_retention_period`) is always later than that, so hypertable
  retention is stricter than vanilla expiry by however long the row sat in
  the hot table before archiving. `event_retention_period = 0` (the disable
  sentinel, keep everything) registers no policy, which is exactly "keep
  everything".
* **The sweeps still run.** Chunk granularity, not row granularity, governs
  the policy drops: the chunk containing the newest rows is dropped only when
  its newest row ages out. Rows inside young chunks still age past the
  retention setting, and the row-level sweeps keep honoring them: the event
  TTL sweep deletes them (including the reclaim-outbox carve-out, honored
  inside chunk lifetime), and the archive expiry sweep deletes rows whose
  `expire_at` passed while their chunk is still present. The reclaim-outbox
  carve-out's 100x age cap is bounded by chunk drops, not by the sweep: a
  chunk ages out at plain retention regardless of its events' kind, so a
  `watch_reclaims` consumer must keep its lag inside
  `event_retention_period` (or one chunk interval, whichever is larger) on
  the hypertable mode.

## Monitoring

* Policies and their next runs: `timescaledb_information.jobs`
  (`proc_name = 'policy_retention'`; `config` carries the `drop_after`
  interval the deploy step last registered).
* Converted tables: `timescaledb_catalog.hypertable` (the internal catalog;
  `timescaledb_information.hypertables` is the friendlier view).
* Chunk inventory and sizes: `timescaledb_catalog.chunk` joined on
  `hypertable_id`, or `show_chunks('"taskq".job_events')`.
* A policy that stops firing shows up as chunk counts that grow without
  bound on the aged end; alert on chunk-count growth per hypertable.

## Turning it back off

The conversion is forward-only, like the migrations. Setting
`TASKQ_TIMESCALEDB_HYPERTABLES=false` after enabling stops the deploy step
from issuing any hypertable SQL, but does not convert the tables back and does
not remove the registered policies: a previously converted schema keeps its
chunk retention at the last-registered intervals. To return to a vanilla
schema, restore from a backup or migrate the data out by hand.

## Test coverage

`tests/test_timescaledb_hypertables.py` pins all of this: the flag-off path
issues zero statements (a stub connection that raises on first use); opting in
on a vanilla server raises `TimescaleDBUnavailableError` naming the setting;
the conversion's catalog shape (hypertables, widened uniqueness, surviving and
dropped foreign keys, registered policy intervals); re-enable convergence; a
real policy run dropping aged chunks while fresh rows survive; the sweeps'
young-chunk behavior; the differential prune script agreeing between vanilla
Postgres and the hypertable mode; and chunk pruning in a windowed
`job_events` query's plan. The container legs skip with a reason when Docker
is unreachable.
