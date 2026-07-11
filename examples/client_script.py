"""Standalone client script — enqueue jobs from a CLI or one-off script.

Demonstrates the pattern for enqueueing jobs outside of a long-running
application: a script that opens a TaskQ client, enqueues work, waits
for results, and exits. Use this for backfills, admin tasks, and
one-off job dispatch.

Usage::

    # Enqueue a single job and wait for the result
    uv run python examples/client_script.py

    # Backfill: enqueue N jobs via enqueue_batch
    uv run python examples/client_script.py --backfill 100

    # Cancel a running job by ID
    uv run python examples/client_script.py --cancel <job_id>

Requires: Postgres running (``docker compose up -d postgres redis``) and
migrations applied (``uv run taskq migrate up``).
"""

import argparse
import asyncio
import sys
from uuid import UUID

from examples.actors.basic import CounterPayload, counter
from examples.actors.realworld import (
    DigestEmailPayload,
    ThumbnailPayload,
    generate_thumbnail,
    send_digest_email,
)
from taskq import EnqueueItem, JobFilter, TaskQ
from taskq.settings import TaskQSettings


async def enqueue_single(tq: TaskQ) -> None:
    """Enqueue a single job, wait for it, and print the result."""
    handle = await tq.enqueue(
        counter,
        CounterPayload(n=5),
        tags=["cli-demo", "single"],
    )
    print(f"enqueued job {handle.job_id} (was_existing={handle.was_existing})")

    try:
        await handle.wait(timeout=30.0)
        print(f"job {handle.job_id} succeeded")
    except TimeoutError:
        print(f"job {handle.job_id} did not finish within 30s", file=sys.stderr)
        sys.exit(1)


async def enqueue_backfill(tq: TaskQ, n: int) -> None:
    """Bulk-enqueue N counter jobs via enqueue_batch with idempotency keys."""
    items = [
        EnqueueItem(
            actor_ref=counter,
            payload=CounterPayload(n=3),
            idempotency_key=f"backfill:{i}",
            tags=["cli-demo", "backfill"],
        )
        for i in range(n)
    ]
    batch = await tq.enqueue_batch(items)
    print(f"enqueued {batch.size} jobs, batch_id={batch.batch_id}")

    import asyncpg

    settings = TaskQSettings.load()
    pool = await asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=2)
    assert pool is not None
    try:
        async with pool.acquire() as conn:
            for _ in range(60):
                status = await batch.status(conn, schema=settings.schema_name)
                if status.is_complete:
                    break
                print(f"  {status.pending}/{status.total} pending...")
                await asyncio.sleep(1.0)
            else:
                print("batch did not complete within 60s", file=sys.stderr)
                sys.exit(1)

        print(f"batch done: {status.succeeded} succeeded, {status.failed} failed")
    finally:
        await pool.close()


async def enqueue_realworld(tq: TaskQ) -> None:
    """Enqueue real-world scenario actors to demonstrate realistic usage."""
    digest_handle = await tq.enqueue(
        send_digest_email,
        DigestEmailPayload(
            user_id="user-42",
            email="alice@example.com",
            period="weekly",
        ),
        identity_key="user-42",
        tags=["cli-demo", "email"],
    )
    print(f"enqueued digest email job {digest_handle.job_id}")

    thumb_handle = await tq.enqueue(
        generate_thumbnail,
        ThumbnailPayload(
            image_url="https://cdn.example.com/photos/vacation.jpg",
            width=300,
            height=300,
            format="webp",
        ),
        tags=["cli-demo", "thumbnail"],
    )
    print(f"enqueued thumbnail job {thumb_handle.job_id}")

    try:
        result = await thumb_handle.wait(timeout=30.0)
        print(
            f"thumbnail result: {result.output_path} ({result.width}x{result.height} {result.format})"
        )
    except TimeoutError:
        print("thumbnail job did not finish within 30s", file=sys.stderr)


async def cancel_job(tq: TaskQ, job_id: UUID) -> None:
    """Cancel a running job by ID."""
    result = await tq.cancel(job_id, reason="cli-cancel")
    print(
        f"cancel result: previous={result.previous_status} "
        f"new={result.new_status} initiated={result.cancellation_initiated}"
    )


async def list_jobs(tq: TaskQ) -> None:
    """List recent jobs to demonstrate the query API."""
    page = await tq.list(JobFilter(limit=10))
    print(f"recent jobs ({len(page.jobs)} shown):")
    for job in page.jobs:
        print(f"  {job.id} {job.actor} {job.status} created={job.created_at.isoformat()}")
    if page.next_cursor:
        print(f"  (more available, cursor={page.next_cursor})")


async def main() -> None:
    parser = argparse.ArgumentParser(description="TaskQ standalone client example")
    parser.add_argument(
        "--backfill", type=int, metavar="N", help="Enqueue N counter jobs as a batch"
    )
    parser.add_argument("--cancel", type=UUID, metavar="JOB_ID", help="Cancel a job by ID")
    parser.add_argument("--list", action="store_true", help="List recent jobs")
    parser.add_argument(
        "--realworld", action="store_true", help="Enqueue real-world scenario actors"
    )
    args = parser.parse_args()

    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn), schema=settings.schema_name) as tq:
        if args.cancel:
            await cancel_job(tq, args.cancel)
        elif args.list:
            await list_jobs(tq)
        elif args.backfill is not None:
            await enqueue_backfill(tq, args.backfill)
        elif args.realworld:
            await enqueue_realworld(tq)
        else:
            await enqueue_single(tq)


if __name__ == "__main__":
    asyncio.run(main())
