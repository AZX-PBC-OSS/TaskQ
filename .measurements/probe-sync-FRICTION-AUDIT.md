# Fresh-dev friction audit — the sync/import/cron probe (feat/wf-probe-sync)

Written by a probe that learned the library from `docs/` + `examples/` +
docstrings ONLY (no `src/` reading), then built and ran
`examples/metering_sync/` against a real Postgres. Every finding below was
hit, not hypothesized. Evidence: `probe-sync-*.json` in this directory.

## 1. `enqueue_batch_streaming(finalizer=...)` silently defeats the streaming

`docs/guides/jobs-clients.md:841-849` — "When `failure_policy` or
`finalizer` is set and `connection` is None, the entire operation is
delegated to `Backend.enqueue_batch_atomic` for single-transaction
atomicity." The `enqueue_batch_streaming` section (lines 829-960) never
repeats this: a reader who adds `finalizer=` to the streaming call (the
obvious way to get a completion gate) silently flips the API to
all-or-nothing — nothing is committed until the WHOLE stream drains,
which is the exact opposite of the eager handoff they chose the
streaming API for. The trap is only discoverable by reading the
`enqueue_batch` section three screens earlier. A loud warning inside the
streaming section (and on the `finalizer` row of its parameter table,
line ~900) would have saved the probe's first design.

## 2. The repo's own example contradicts the docs' finalizer deadlock warning

`docs/guides/jobs-clients.md:831-837`: "if the finalizer were stamped
[with `metadata.batch_id`], `wait_for_batch` would count it as a child,
and the finalizer would wait for itself: a deadlock."
`examples/actors/batch.py:60-64` stamps exactly that:
`metadata={"batch_id": str(batch_id), "role": "finalizer"}` on the
`ctx.jobs.enqueue(batch_finalizer, ...)`. A fresh dev who copies the
committed example gets the documented deadlock. One of the two is wrong;
the docs' version is the one `wait_for_batch`'s own contract implies
(`exclude_job_id` defaults to the batch row's `finalizer_job_id`, which
is NULL on the in-actor path).

## 3. `jobs.status` is a Postgres enum — raw SQL needs `status::text`

Nothing in the batch docs (`docs/guides/jobs-clients.md:640-660`,
`docs/guides/jobs-clients.md:960+`) says the `status` column type. The
probe's operator queries (the admin-view receipts) failed with
`operator does not exist: taskq.job_status = text` until each comparison
was rewritten `status::text = 'succeeded'`. `BatchHandle.status()` hides
this (the docs' only taught path), but the moment an operator writes
their own queue-depth query — which the same docs recommend for
scripts — they hit it. One sentence ("status is the `taskq.job_status`
enum; cast to text to compare against literals") belongs in the
`BatchHandle.status()` section.

## 4. `taskq worker --queues a b c` fails with a message that mis-teaches

The `--queues` flag must repeat (`--queues=ingest --queues=review ...`).
Passing three positionals — the natural reading — errors with
`No such command 'review'`, which points at typer subcommands, not at
the flag's repeat-me contract. The docs show the right form once
(`docs/guides/cli.md:316`, `--queues default --queues priority`) but the
error message never teaches it. The error should name the remedy
("repeat the --queues flag once per queue").

## 5. The workflows/run machinery is invisible to a docs-only reader

The library ships `taskq.workflows` (`FlowRunner`, `map`/`step`/
`gather`, run explorer in the admin) and `examples/workflows.py:4`
references `docs/examples/doc-ingest.md` — which does not exist (no
`docs/examples/` directory). `step_key` / `map_index` — the run's node
display identity — appear in ZERO docs files (verified by grep over
`docs/`). The probe's admin-view face was built from what IS documented
(`JobFilter(batch_id=...)` + `TaskQ.list`); the richer per-node run
display is undiscoverable without reading `examples/workflows.py`, which
is itself no substitute for a guide. Either a workflows guide or the
referenced doc-ingest page is missing.

## 6. The worker's bounded mode (`until_idle=True`) is buried

`docs/guides/workers.md:917-924` teaches `worker_main(..., until_idle=True)`
in eight lines inside a 1000-line guide; `docs/getting-started/quick-start.md`
never mentions it. A fresh dev writing a demo or test harness wants
exactly this ("run until the queue is empty, then exit") and finds the
unbounded `worker_main` first (line 75). One paragraph in the quick-start
("run a worker to idle in-process") would surface it.

## 7. The default DSN is a silent footgun on custom ports

`TASKQ_PG_DSN` defaults to `postgresql://taskq:taskq@localhost:5432/taskq`
(`docs/getting-started/quick-start.md:80`). The probe's hand-started
worker forgot the env var, connected to whatever was on 5432, and failed
with `InvalidPasswordError` — which teaches "wrong password", not "wrong
server". On a machine without a 5432 server the worker instead boots and
waits quietly against nothing. The resolved DSN should be printed at
worker boot (the startup budget log already prints pool sizes; not the
host).

## 8. Progress receipts on fast jobs are thinner than the docs imply

`docs/guides/progress.md:106-127`'s example (a transcode sleeping
between steps) produces one event per step. A fast batch actor that
calls `ctx.progress()` once per item coalesces to ~one snapshot per
flush interval: the probe's 600-item run captured 3 events on the
finalizer's stream (`progress.md:19` documents the 500 ms poll fallback
but not the coalescing thinness from the CONSUMER's side). The progress
face works exactly as designed — the receipt is just sparser than the
guide's example suggests. One note ("per-item progress on sub-second
jobs coalesces; push step counters, not per-item events, if you want a
dense stream") would set expectations.

## 9. `TaskQ` facade / `JobsClient` parity is undocumented

`docs/api-reference/taskq.md:79-94` says the facade mirrors the client
"plus `tq.actors`" but does not name the method set. The probe needed
`enqueue_batch_streaming`, `list`, and `get_batch` on the facade and had
to `dir()` the class at runtime to confirm they exist. A parity table —
or the actual diff — belongs in the api-reference.

## 10. The CLI has no schedule run-now; drills must wait out the tick

The admin UI has run-now (`POST /schedules/{id}/run`,
`docs/guides/cron.md:624-628`) but the CLI does not (verified against
`docs/guides/cli.md`'s command list). A drill proving the scheduled face
must keep a worker up for two tick intervals of wall clock (the probe's
cron drill sleeps 75 s). A `taskq schedule run-now SCHEDULE_ID` mirroring
the UI endpoint would make scheduled-face drills instant.

## Non-findings (things the docs taught cleanly)

- The fan-out patterns (cursor chain / batch+finalizer / streaming) and
  the idempotency-key discipline (`docs/guides/ops.md:544-760`) were
  sufficient to design the whole pipeline from scratch — including the
  deterministic `uuid5` batch id and per-item keys.
- `wait_for_batch` + the `Snooze` pattern worked exactly as documented.
- The cron declaration (6-field seconds form, `docs/guides/cron.md:47-51`)
  needed no source reading and fired on the first tick.
- `apply_pending_locked` from the quick-start worked first try against a
  fresh Postgres.
