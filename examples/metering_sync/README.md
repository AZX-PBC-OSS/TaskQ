# Meter-readings sync — a batch-ingestion pipeline example

A textbook data-sync pipeline in one directory: a producer that scans a
source feed and hands work off **eagerly**, workers that land each item
with an **idempotent upsert**, a **finalizer** that waits the batch out
and returns a **typed receipt**, and a **cron schedule** that runs the
same sync on an interval.

## The four faces

| Face | Where | What it proves |
|------|-------|----------------|
| Eager handoff | `pipeline.run_pipeline` | The first item is consumed **before** the producer finishes producing — workers start while the scan is still running. |
| Idempotent re-run | `actors.process_reading` | `INSERT ... ON CONFLICT ... WHERE` content changed: re-running the pipeline over the same feed duplicates nothing. |
| Progress + admin view | `ctx.progress()`, `progress_stream()` | The run pushes observable progress while it runs; the batch's node statuses are listable mid-flight and after. |
| Typed success | `actors.SyncReceipt` | The pipeline is done when the receipt's counts add up — not merely when a process exits zero. |

Plus the scheduled face: `actors.py` declares a 30-second cron tick whose
fire scans a small feed and fans the readings out with per-fire
idempotency keys.

## Run it

You need a Postgres (15+) and the `taskq` CLI on your `PATH`.

```bash
export TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq

# one-time: apply the queue schema (idempotent)
taskq migrate up   # or let the drills do it

# a worker for the example's registry
taskq worker --actors examples.metering_sync.actors:registry \
    --queues=ingest --queues=review --queues=finalize

# in another shell: ONE pipeline run, whose last line is the typed receipt
python -m examples.metering_sync.pipeline

# or the full drill suite: overlap, idempotency, resume-after-SIGKILL,
# load, admin view, cron — each writing a JSON receipt into .measurements/
python -m examples.metering_sync.drills --drill all
```

The drills manage their own worker processes; run `--drill all` without a
worker of your own.

## The drills

| Drill | What it does | The receipt's claim |
|-------|--------------|---------------------|
| `overlap` | Runs 600 items; an observer watches the queue while the producer call is still blocked. | `first_consumed_at < producer_finished_at` (the measured overlap). |
| `idempotency` | Runs the whole pipeline twice over the same source data. | Row counts and content hashes identical; zero new worker touches. |
| `resume` | `SIGKILL`s the worker mid-run, restarts it. | All 400 rows present, hash identical to the source, no duplicates. |
| `load` | 3000 items. | The honest wall-clock throughput. |
| `admin` | Peeks at the run's nodes mid-flight and at the terminal state. | Both views captured, plus the final receipt. |
| `cron` | Keeps a worker up for two ticks. | Tick fires observed; the ticks' readings landed. |

## File map

- `actors.py` — the actors (process/review/finalize/tick), the idempotent
  upserts, the cron declaration, the worker registry.
- `pipeline.py` — the client side: the paginated source scan, the eager
  handoff, the progress capture, the typed receipts, and the standalone
  demo (`python -m examples.metering_sync.pipeline`).
- `drills.py` — the proof harness: each drill against a real Postgres,
  receipts into `.measurements/`.

## Notes

- The source feed is deterministic (`pipeline.source_readings`), so the
  drills can hash it and prove the stored readings match it exactly.
- The producer enqueues page by page rather than passing a `finalizer=`
  to the batch APIs — the finalizer parameter forces the atomic
  single-transaction path, which commits nothing until the whole batch is
  enqueued (the opposite of the eager handoff being demonstrated).
- The finalizer is enqueued separately and carries no `metadata.batch_id`
  on purpose: it must not be counted among the children it waits on.
