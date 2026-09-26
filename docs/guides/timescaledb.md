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
  deploy, already-converted tables are skipped). A changed chunk interval is
  re-asserted the same way, but it shapes future chunks only: existing
  chunks keep the interval they were created with.
* **Uniqueness widens, semantics hold.** A hypertable requires the partition
  column in every unique constraint, so `jobs_archive`'s `PRIMARY KEY (id)`
  becomes `UNIQUE (id, finished_at)`. The re-archive guarantee the bare
  primary key enforced is enforced instead by an explicit `NOT EXISTS` guard
  in the archive write: a job id that already holds an archive row is never
  archived again. The same guard runs on vanilla Postgres, so the prune's
  ghost semantics (fold an already-archived id, never wedge, never duplicate)
  are identical in both modes and pinned by the same tests. One stated edge:
  the guard's promise is keyed to the archive row's existence, and a chunk
  drop can remove that witness. When the ghost's archive chunk is gone, a
  retried job's re-prune re-archives — fresh `archived_at`/`expire_at`
  stamps, the live payload wins — where vanilla would still fold, because
  the row outlives the chunk by the archive-to-expire margin. Bare-id
  uniqueness would pin the invariant structurally, and a hypertable forbids
  exactly that; "archived at most once" therefore holds only while the
  archive chunk lives.
* **One foreign key drops.** No table may reference a hypertable, so
  `job_attempts_archive`'s foreign key to `jobs_archive(id) ON DELETE CASCADE`
  is removed. Chunk retention replaces the cascade: both tables register
  policies from the same `archive_retention_period`. The alignment is by
  chunk, not by row: an attempt whose `started_at` precedes its job's
  `finished_at` can outlive the parent's chunk by up to one chunk interval.
  And unlike the cascade it replaces, the chunk drop does not take the
  attempts with the parent: when the parent's chunk drops first, the
  parentless `job_attempts_archive` rows stay queryable until their own
  chunk drops — at most one chunk interval later, capped at the clamp's
  thirty days. Vanilla never shows a parentless attempt row (the cascade
  removes them together); anything that queries attempts directly on the
  hypertable mode should expect them in that window.
  `job_events`' foreign key to `jobs(id) ON DELETE CASCADE` is kept
  (hypertable-to-regular-table foreign keys are supported) and enforcement is
  unchanged.
* **The first conversion rewrites populated tables.** Conversion uses
  `migrate_data`, which takes an access-exclusive lock and rewrites existing
  rows into chunks inside the deploy step. Size the deploy window for the
  archive's current row count; an empty or young schema converts in seconds.
  The window does not start at the copy, though — the constraint surgery in
  the next two bullets locks and scans the same tables first, and the first
  background policy run after the deploy drops the whole aged tail in one
  sweep.
* **The constraint surgery takes its own access-exclusive locks.** Before
  `create_hypertable` runs, the foreign-key drop, the primary-key drops, and
  every `ADD CONSTRAINT ... UNIQUE` statement (one per table) each take
  `ACCESS EXCLUSIVE` on their table, and each unique add full-scans that
  table to validate the uniqueness it asserts. On a big archive those scans
  are part of the deploy window, not a footnote to it: size the window for
  the constraint scans and the `migrate_data` copy together.
* **The primary key drops before the unique constraint lands.** These
  statements run one per transaction on the deploy connection's autocommit,
  and the migration advisory lock serializes migrators only — workers keep
  running through the whole conversion. Between a table's primary-key drop
  and its unique add, that table holds no unique constraint on its id
  columns; a duplicate insert that lands in the window makes every
  subsequent deploy fail on the unique add until the duplicates are removed
  by hand. Run the enabling deploy in a maintenance window when no worker
  archives, or accept the window: it spans two adjacent statements, so it is
  tiny and bounded, but it is not zero.
* **Bounded runs convert anyway.** `migrate up --phase pre`, `--target`, and
  `--max-steps` all still run the conversion and re-register the policies
  when the flag is on: the deploy step's flag check is independent of which
  migrations a bounded run applies. A run intended to apply one unrelated
  migration does the full hypertable work too — pinned as current behavior
  by `tests/test_timescale_deploy_e2e.py::test_bounded_run_still_converts`.
* **The first policy run drops the whole aged tail at once.** With policies
  armed, every chunk older than the retention interval is drop-eligible
  immediately: the first registration makes the entire historical backlog
  drop-eligible the moment the enabling deploy finishes, because policy
  deletion is chunk-granularity — whatever has fully aged past the interval
  goes as whole-chunk drops. The first time Timescale's background workers
  fire, that aged tail leaves in one sweep, potentially gigabytes of IO,
  where vanilla mode expires the same rows gradually, row by row, in bounded
  batch deletes. Size the post-deploy window for that first policy run too.

## The columnstore (compression) is adopted for the archive tables

The deploy step arms the columnstore on the two archive tables when it
converts them (and on every deploy after, converging like the retention
policies do):

| Table | `segmentby` | `orderby` | Rationale |
|---|---|---|---|
| `jobs_archive` | `actor, queue` | `finished_at DESC` | The admin archive tab's newest-first read; low-cardinality filter columns. Never the per-row-unique `id`: a unique segmentby key makes every compressed batch a single row and collapses the ratio (the documented anti-pattern). |
| `job_attempts_archive` | `job_id` | `started_at DESC` | Per-job attempt history reads in job order. |
| `job_events` | — | — | **Stays rowstore** — measured, nothing to gain (its writes dominate and its reads are windowed). |

The compression policy's `compress_after` is one chunk interval (the same
retention/4-clamped derivation the chunk sizing uses): a chunk compresses
once it has stopped receiving rows. The policy is remove-then-add
registered like its retention sibling, so a changed
`archive_retention_period` moves `compress_after` on the next deploy. A
changed `segmentby`/`orderby` with compressed chunks already on disk is
ACCEPTED by the server — with a NOTICE, not an error ("updated compression
settings will only apply to future compressions; existing compressed
chunks will not be recompressed", measured on 2.30.1): the new shape is
the FUTURE chunks' shape, and the already-compressed ones keep their old
segmentation until you decompress and recompress them
(`decompress_chunk`, then `compress_chunk(chunk, recompress => true)`).
A deploy that changes the columnstore shape therefore converges the
policy silently and leaves the on-disk chunks mixed; the
`timescaledb_information.compression_settings` view shows the setting,
not what each chunk actually carries.

**One server prerequisite, probed and warned about loudly:**
`timescaledb.max_tuples_decompressed_per_dml_transaction` defaults to
100000, and at that default the archive expiry sweep hard-errors on
compressed chunks with `ConfigurationLimitExceededError` (measured: one
10k-row expiry batch decompressed 356633 tuples). At or under the
default, `enable_hypertables` logs a WARNING and returns the warning in
the report's `decompression_guc_warning`. Raise the budget server-wide
before relying on the columnstore:

```sql
ALTER SYSTEM SET timescaledb.max_tuples_decompressed_per_dml_transaction = '0';  -- 0 = unlimited
SELECT pg_reload_conf();
```

(or set it on the workers' sessions; it is user-settable). The measured
trade-offs: `benchmarks/timescale_compression.py` recorded a 6.20x
storage reduction on the 400k-row archive corpus, the cold archive reads
4-9x faster over compressed chunks, the young chunk's hot page unchanged,
and the id point lookup (a non-segmentby key) the documented weakness.

## Retention is owned by the policies, mostly

Once the tables are hypertables, the aged end of the timeline is owned by
Timescale's background workers: each policy drops chunks whose time range
fully passed its retention. Two consequences to know:

* **The retention clock is the partition column, not `expire_at`.** Archive
  chunks drop on `finished_at` age; a row's `expire_at` stamp (archive time
  plus `archive_retention_period`) is always later than that, so hypertable
  retention is stricter than vanilla expiry by however long the row sat in
  the hot table before archiving. The aged-side rule, stated once: with
  policies armed, `expire_at` is honored exactly inside chunk lifetime — the
  expiry sweep still deletes a row whose `expire_at` passed while its chunk
  is present — and the aged end is governed by the policy's
  partition-column clock. That is the trade the feature makes for
  chunk-drop performance. `event_retention_period = 0` (the disable
  sentinel, keep everything) registers no policy, which is exactly "keep
  everything".
* **The sweeps still run — above the policy floor.** Each sweep probes the
  armed policy's own `drop_after` horizon once per run
  (`retention_policy_floor` in `src/taskq/timescale.py`, one catalog query
  against `timescaledb_information`, failing open to "no floor" on vanilla
  Postgres or any probe error) and bounds its DELETE with it: rows older
  than the floor are the POLICY's — silently, chunk-granular, watermark-blind
  — and the sweep no longer pays the chunk-fan-out tax to re-delete them.
  Below the floor the sweep owns deletion row-exactly: the event TTL sweep
  honors the reclaim-outbox carve-out there, and the archive expiry sweep
  honors `expire_at` exactly inside chunk lifetime (the policy's clock is
  `finished_at` — the chunk alignment column — so the floor bounds
  `finished_at` while `expire_at` stays the row-precise predicate inside the
  window). Rows older than the floor but inside a young chunk leave at most
  one chunk interval late, when the chunk itself ages past the boundary —
  chunk granularity is the trade. The floor is read from the registered
  policy's own `config` (`drop_after`), not re-derived from the settings, so
  the sweep and the policy can never disagree about where the boundary sits.
  One more zero needs distinguishing on this mode: an expiry DELETE's first
  execution after a chunk aged into the columnstore can silently skip the
  qualifying rows that chunk holds (the decompression path catches up on
  the next tick — ops.md's watch list has the two zeros and the query that
  tells them apart).
  Chunk granularity, not row granularity, governs
  the policy drops: the chunk containing the newest rows is dropped only when
  its newest row ages out. The reclaim-outbox
  carve-out's 100x age cap is bounded by chunk drops, not by the sweep: a
  chunk ages out at plain retention regardless of its events' kind, so a
  `watch_reclaims` consumer must keep its lag inside
  `event_retention_period` (or one chunk interval, whichever is larger) on
  the hypertable mode.
* **The chunk drop is a third deleter, and the gap signal does not see it.**
  Migration 01.00.20_02's watermark (`job_events_prune_state.pruned_through_id`)
  is what makes a stale consumer cursor fail visible with
  `EventRetentionGapError` instead of silently skipping — and that contract
  ("the watermark can never lag what is already gone") is kept by the two
  sweep deleters only, each of which advances the watermark in the same
  statement that deletes. The retention policy advances nothing: a chunk
  drop removes event ids without moving `pruned_through_id`, so a consumer
  cursor below the dropped ids still reads as safe (cursor at or above the
  watermark) and silently loses the dropped events. No code advances the
  watermark from the policy path today. That is why the lag rule in the
  previous bullet is a hard requirement on this mode, not a comfort: on
  hypertables, keep the `watch_reclaims` cursor strictly inside
  `event_retention_period` — the chunk policy provides no gap signal.

## Measured trade-offs: hypertables vs plain PostgreSQL

Measured on this repo's own benchmark (`benchmarks/timescale_tradeoffs.py`,
rerunnable end-to-end; 1,000,000 jobs / 400k archive / 400k events / 100k
attempts seeded identically on both engines, three stable runs, every
dashboard row identity-asserted cross-engine before any timing counted):

**Retention drains (the sweeps' real batch shapes):**

| Drain | plain | hypertable (since the policy floor) | ratio since |
|---|---:|---:|---:|
| Prune 100k jobs → archive | 4.5 s | 35.9 s | ~7.9× slower (untouched by the floor: its DELETE runs on the plain `jobs` table) |
| Event TTL (96k rows) | 0.30 s | **0.04 s** | ~6.8× **faster** |
| Archive expiry (100k rows) | 1.14 s | **0.32 s** | ~3.6× **faster** |

Before the retention-policy floor the hypertable ran the same row-level
batch DELETEs plain still runs, with the chunk fan-out tax on top — the
pre-floor runs measured ~6.6× plain's per-batch cost on the event TTL
(those runs predate the committed artifacts; every number in this table
is read off `benchmarks/results/timescale-tradeoffs.json`, and the same
tax is visible today on the one drain the floor cannot touch: the
prune's, ~7.9× above). The floor moved
the aged end's deletion to the policy's chunk drops — the sweeps' cost on
the hypertable collapsed to the floor probe plus the empty-window index
scan, and the two TTL/expiry legs are now FASTER than plain, which still
pays the row-level DELETEs. The trade is unchanged, only cheaper: the
aged end leaves at chunk granularity (silently, watermark-blind), one
chunk interval later at worst, instead of row-exactly.

**The admin dashboard at 1M jobs - parity except the archive tab, which wins big:**

| Query | plain p50 | hypertable p50 |
|---|---:|---:|
| Live jobs pages/counts (`jobs` is never a hypertable) | ±10% | parity |
| Archive tab, newest-first page | 45.0 ms | **8.6 ms (5.2×)** |

Chunk pruning serves "recent history" reads from the youngest chunk - the
dashboard's most common archive read is the hypertable's best case.

**Write path - no cliff:** ~3% enqueue tax (that table is plain in both
modes - planning noise), ~6% on the hypertable `job_events` insert.

**Aftermath:** dead tuples after row-level DELETEs are identical per
engine - deleting buys no bloat relief on either. The hypertable's
retention win lives in the chunk-drop path (whole-chunk, policy-
driven, silent), and since the retention-policy floor the sweeps' aged-end
DELETEs are gone from the hypertable entirely - the sweeps' remaining
young-chunk work is small, precise, and row-exact. What the feature still
trades away: the deletion guarantees above (expire_at exactness on the
aged end, the gap signal, the fold guard) against the archive tab's read
speed and the aged end's removal cost.

## Monitoring

* Policies and their next runs: `timescaledb_information.jobs`
  (`proc_name = 'policy_retention'`; `config` carries the `drop_after`
  interval the deploy step last registered; the compression policies
  register as `policy_compression` with `compress_after`).
* Columnstore settings per column:
  `timescaledb_information.compression_settings`
  (`segmentby_column_index` / `orderby_column_index`), and
  `compression_enabled` on `timescaledb_information.hypertables`.
* Converted tables: `timescaledb_catalog.hypertable` (the internal catalog;
  `timescaledb_information.hypertables` is the friendlier view).
* Chunk inventory and sizes: `timescaledb_catalog.chunk` joined on
  `hypertable_id`, or `show_chunks('"taskq".job_events')`.
* A policy that stops firing shows up as chunk counts that grow without
  bound on the aged end; alert on chunk-count growth per hypertable.

## Turning it back off

The deploy step's conversion is forward-only, like the migrations. Setting
`TASKQ_TIMESCALEDB_HYPERTABLES=false` after enabling stops the deploy step
from issuing any hypertable SQL, but does not convert the tables back and does
not remove the registered policies: a previously converted schema keeps its
chunk retention at the last-registered intervals (pinned by
`tests/test_timescale_deploy_e2e.py::test_flag_off_after_enable_changes_nothing`).

The explicit way back is `taskq migrate disable-hypertables` — the CLI
wiring of `disable_hypertables` in `src/taskq/timescale.py`, the mirror of
the deploy step's enable side. Flip the flag off first: with the flag still
true the command exits 1 with the remedy (the library-level gate is a
zero-statement no-op; the CLI refuses to let a mistyped invocation look
like a completed disable). The command holds the same migration advisory
lock the enabling deploy used, and on failure prints the same
self-diagnosing report `migrate up` prints.

The mechanics per hypertable, rename-first so no order of death loses rows:
every registered policy (retention and compression) is removed first
(idempotently, refusing to swap tables under a live policy), then the rows
are copied into a bare restore heap and the copy is count-verified — this
exercises the full read path (compressed chunks decompress) BEFORE any name
moves — then the hypertable RENAMES to `{table}__hypertable_trash`
(metadata-only and instant: from that moment the rows exist in two places
and the vanilla name is free), the migration-built vanilla table moves into
the freed name — its shape is cloned from a fresh application of the bundled
migrations themselves, never re-typed by hand, so the restored pkeys,
indexes, foreign keys, column order, and defaults are byte-equal to a schema
that never converted — and the rows return FROM THE TRASH, twin-verified
against both copies, with only then the trash and the heap dropped. The
traded-away behaviors come back: the bare primary keys reject duplicates
again, the `job_attempts_archive → jobs_archive` foreign key cascades again,
and the event id sequence continues from the restored maximum. It is
idempotent (re-runs converge) and pinned end to end by the disable legs in
`tests/test_timescaledb_hypertables.py` and
`tests/test_timescale_deploy_e2e.py` (the latter runs the real CLI
subprocess). The in-memory twins need nothing: vanilla semantics are the
default — there is nothing to disable but the schema.

Two windows to know about, both bounded, both documented honestly:

* **The copy window (the mirror of enable's pkey-drop window).** Workers
  keep running through the whole disable: the migration advisory lock
  serializes migrators only. A row committed between the copy's
  count-verify and the trash rename lands in the TRASH — and comes back
  (the rows return from the trash, the superset), so the rename-first
  swap loses nothing there. The window that remains is the enable path's
  documented shape: run the disabling deploy in a maintenance window when
  no worker archives anyway. There is also one loud refusal: the
  hypertable's widened uniqueness admits rows (same id, different
  partition-column value) the restored vanilla table's primary key cannot
  hold; the swap refuses to choose which row survives and names the
  conflicts instead.
* **The event id sequence's one honest gap.** The restored sequence is
  re-anchored to GREATEST(its own position, the restored max(id)) —
  forward-only. On an EMPTY restored table (a schema whose events were
  fully reaped before the disable) the anchor re-issues the anchor value
  itself, and the sequence's own position can sit past ids retention
  already deleted: the next issued event id can reuse one
  `job_events_prune_state.pruned_through_id` already claims gone. A
  caught-up `watch_reclaims` consumer (cursor at or above the watermark)
  will not see that one event; a lagging consumer fails visibly, as
  designed. One id, once, at the boundary — the price of anchoring
  forward-only against a table that cannot say which ids below the
  sequence's position were already issued and reaped.

Crash safety is structural, not procedural: a crash at ANY swap stage
(after the verified copy, after the trash rename, after the move-in)
leaves every row in at least two places, and the NEXT disable's first act —
before any `DROP SCHEMA CASCADE` — finishes every crashed table's move
under the same count/twin verification (`_converge_crashed_swaps`): a
stranded trash renames back, stranded copies are twin-absorbed into the
live table, and nothing holding a row whose live twin is missing is ever
dropped. Pinned by the crash matrix in
`tests/test_timescaledb_hypertables.py::test_disable_crash_at_every_swap_stage_converges`
(every stage × re-run → the full vanilla shape with every row).

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
is unreachable. `tests/test_timescale_deploy_e2e.py` runs the real
`taskq migrate up` subprocess against TimescaleDB (conversion with data,
preservation, the capability refusal, roll-forward-only, and the bounded-run
interaction), and `tests/test_timescale_retention_interplay.py` pins the
policy/sweep composition edges named above: the carve-out defeated by chunk
drops, the watermark not advancing on a policy drop, the re-archive
witness a chunk drop removes — and the retention-policy floor itself:
the below-floor range deleted-count going to zero (with the pre-fix
counterfactual), the inside-window rows still deleting row-exactly and
watermark-visibly, the floor-or-not end-state parity, the vanilla probe
failing open to full-range sweeps, and the policy-less hypertable owning
no aged end at all. The columnstore adoption is pinned by
`test_compression_adopted_on_archives_only` (the per-column settings via
`timescaledb_information.compression_settings`, the registered
`policy_compression` jobs, `job_events` staying rowstore, the hot page
read, and re-run convergence) and
`test_decompression_guc_warning_fires_at_default_and_quiets_when_raised`
(the loud GUC warning at the 100000 default, quiet at a raised budget).
The disable mirror is pinned by the disable legs: the vanilla shape
byte-equal to a fresh migration's with every row counted
(`test_disable_restores_vanilla_shape_and_every_row`), the restored
behaviors — duplicate rejected by the bare PK, orphan attempts rejected
by the restored FK, the sequence continuing
(`test_disable_restores_vanilla_behaviors`), idempotence
(`test_disable_is_idempotent`), the full
enable→disable→enable round trip
(`test_enable_disable_enable_round_trip`), the deploy-path disable
end to end (`test_disable_after_deploy_restores_vanilla`) plus the real
`taskq migrate disable-hypertables` subprocess
(`test_disable_hypertables_cli_subprocess_end_to_end`), the crash matrix
(killed at every swap stage, converging on the re-run with every row —
`test_disable_crash_at_every_swap_stage_converges`), and the loud refusal
of rows the restored table's primary key could not hold
(`test_disable_refuses_vanilla_key_collisions_loudly`).
