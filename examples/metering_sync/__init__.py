"""Meter-readings ingestion — a sync/import pipeline example.

A textbook data-pipeline shape in four faces:

1. A **producer** scans a source feed and streams work items (batches) to
   the queue chunk by chunk, so workers begin consuming the first chunk
   while the producer is still producing (eager handoff).
2. A **worker actor** branches on the item type (a flagged reading fans
   out to a review actor) and lands each reading with an **idempotent
   upsert** — re-running the pipeline against the same source data
   duplicates nothing.
3. A **finalizer actor** snoozes until the batch is terminal, pushing
   observable **progress** the whole time, and returns a typed receipt
   with the exact counts.
4. A **cron schedule** fires a small sync tick on an interval — the
   scheduled trigger for the same pipeline.

Run the pipeline and its drills against a real Postgres::

    # 1. a Postgres (any 15+) with the DSN below
    # 2. apply migrations + run the drills:
    TASKQ_PG_DSN=postgresql://taskq:taskq@localhost:5432/taskq \
        python -m examples.metering_sync.drills --drill all

See ``README.md`` in this directory for the full walkthrough.
"""
