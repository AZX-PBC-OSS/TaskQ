# Bulk Ledger Import — a bulk ingest workflow on TaskQ Workflows

One scenario, every face a bulk import needs, built on the
`taskq.workflows` engine (import it directly — `import taskq.workflows`;
the engine is the `taskq[flows]` extra's opt-in):

- **Ingest + transform + partition** — one step reads a generated feed
  (a few thousand JSONL records), runs the per-row transform, and
  partitions the rows into chunks. The step is declared `cache=True`
  (the cross-run cache opt-in): a LATER run over the same feed content
  is served the recorded result instead of re-running the transform.
  The spec is WIRED as the node's payload, so the cache address is
  content-keyed.
- **Chunk fan-out (the map face)** — `map_source` forks one fresh job
  per chunk; the child's `ctx.map_index` IS the chunk index.
- **Per-chunk validation** — each chunk body validates every row
  against the ledger's intake rules and emits the chunk's verdict
  batches: one `ValidBatch` and/or one `InvalidBatch` per chunk.
- **The typed route (valid/invalid branch)** — the verdict batches feed
  a `route`: elements decoded as `ValidBatch` run the idempotent
  upsert arm; elements decoded as `InvalidBatch` run the quarantine
  arm. Every element is a fresh job with its own ledger identity, and
  the route is TOTAL: a union member with no arm is a build-time
  refusal. (Batches, not rows, are the elements on purpose: a join
  fans in its children and a join holds 1000 children max — a
  per-row route over a few thousand rows is above the bound.)
- **Idempotent upserts** — the upsert arm writes with
  `ON CONFLICT … DO UPDATE … WHERE` change detection: a re-import of
  the same feed lands zero changed rows (imported = 0, updated = 0,
  unchanged = all) and the table's content fingerprint is stable.
- **Quarantine with reasons** — invalid rows land in a quarantine
  table with machine-readable reasons; the valid rest completes.
- **Progress push** — chunk bodies report through `ctx.progress`
  ("chunk N validated"); a `ProgressListener` receives the pushed
  events live, and the durable per-node snapshot carries the payload.
- **Fan-in + the completion receipt** — the route's join feeds a tally
  body; the run's decoded result is a typed `LedgerReceipt`
  (imported / updated / unchanged / quarantined — counts exact).
- **Operator view + operate** — `taskq flows list` and `taskq flows
  status RUN_ID` show the run and its nodes mid-flight and after (the
  chunk index is the node's map face); `taskq flows cancel` stops a
  run through the engine's cancel cascade.
- **Crash + recovery** — a killed driver's run is claimed
  `existing-running` on restart (never a second silent run); the
  ledger's replay contract keeps terminal-succeeded nodes from
  re-executing; and because the idempotent upserts absorb re-runs, a
  re-issued import lands the same fingerprint with the exact receipt.

## Run it

Start Postgres (any instance 15+), then:

```bash
export TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq

# 1. generate a feed (2,400 rows, ~8% invalid by construction)
uv run python -m examples.ledger_import.run_import \
    generate-feed --rows 2400 --path /tmp/ledger-feed.jsonl

# 2. run the import
uv run python -m examples.ledger_import.run_import run \
    --feed /tmp/ledger-feed.jsonl --run-key my-first-run

# 3. re-import the same feed — the receipt reports zero changed rows
uv run python -m examples.ledger_import.run_import run \
    --feed /tmp/ledger-feed.jsonl --run-key my-second-run

# 4. watch the run from the operator's seat
uv run taskq flows list
uv run taskq flows status <run_id>
```

The whole drill suite runs as one command (it resets the demo tables,
runs the cold import, the re-import, the cache run, a mid-import
crash + recovery, and the operator-view captures, writing its receipts
into `.measurements/probe-import/`):

```bash
uv run python -m examples.ledger_import.drills "postgresql://postgres:taskq@localhost:5432/taskq"
```

## Where the faces live

| Face | Code |
|---|---|
| Feed generation (deterministic, invalid rows by stride) | `feed.py` |
| Typed wire models (rows, batches, receipt) | `models.py` |
| Idempotent batched upsert + quarantine store | `store.py` |
| The workflow (authoring + bodies) | `workflow.py` |
| The run door (create → listen → drive → receipt) | `run_import.py` |
| The drill suite | `drills.py` |

## Notes

- `run(..., execute=True)` drives the bodies in-process — the dev
  loop's door. A fleet worker executes the same rows through the queue
  (`execute=False`), which is the deployment shape.
- The engine's run-key arbiter owns CONCURRENT dedup: two callers
  racing the same run key get one run — the loser receives the
  winner's claim (`existing-running`), never a second silent run. A
  TERMINAL run's key replays as `existing-terminal` with the recorded
  result.
- `cache=True` promises the body is DETERMINISTIC for the same input.
  The example makes the promise honest by feeding the feed's content
  hash through the run input, so the cache address is content-keyed.
  A body whose source file cannot be verified (stdin, exec) is not
  cacheable — the runner logs `step-cache-unaddressable` and caches
  nothing.
- The crash drill injects a hard `os._exit` after the Nth apply-batch
  commits (`LEDGER_CRASH_AT`), which is the crash class where the
  process vanishes with some batches terminal and some in flight.
  The restart's drive re-executes ready work idempotently; reclaiming
  the crashed node's own lease is the worker-hosted sweep's job, so a
  driver-only deployment finishes the recovery with the operator
  doors (`taskq flows cancel`, then a re-issued run under a new key).
