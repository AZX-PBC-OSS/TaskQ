"""Fleet-shape load driver for py-spy/cProfile: 50 consumers, capped claims.

Worker-shaped concurrency: a producer runs capped dispatch_batch(limit=50)
rounds and feeds the local queue; 50 consumer tasks each take one row, run a
tiny handler, and write the terminal state through the same pool shape the
worker uses (the claim-to-register chain is emulated at the backend level).
Runs until interrupted or until --jobs are drained; py-spy samples it from
outside (py-spy record --pid ... or py-spy top --pid ...).

Usage:
    python benchmarks/fleet_load.py --dsn ... --jobs 20000 --consumers 50 &
    py-spy top --pid <pid>
"""

from __future__ import annotations

import argparse
import asyncio
import time
from datetime import timedelta

import asyncpg

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps

DSN_DEFAULT = "postgresql://taskq:taskq@localhost:55431/taskq"
SCHEMA_DEFAULT = "tq_bench_fleet"
ACTOR = "bench_actor"
QUEUE = "default"


def make_payload(i: int, pad: int = 96) -> dict[str, object]:
    return {
        "order_id": f"ord-{i:08d}",
        "channel": ["web", "ios", "android"][i % 3],
        "customer": {"id": f"cus-{i % 997:05d}", "tier": ["free", "pro", "ent"][i % 3]},
        "items": [{"sku": f"SKU-{(i + k) % 50:03d}", "qty": (i + k) % 5 + 1} for k in range(3)],
        "notes": f"benchmark payload #{i} " + "x" * pad,
        "flags": {"expedited": i % 7 == 0, "gift": i % 11 == 0},
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DSN_DEFAULT)
    parser.add_argument("--schema", default=SCHEMA_DEFAULT)
    parser.add_argument("--jobs", type=int, default=20000)
    parser.add_argument("--consumers", type=int, default=50)
    parser.add_argument("--batch", type=int, default=50)
    args = parser.parse_args()

    admin = await asyncpg.connect(args.dsn)
    await admin.execute(
        f'DROP SCHEMA IF EXISTS "{args.schema}" CASCADE'
    )  # Why: benchmark-controlled identifier
    await admin.execute(f'CREATE SCHEMA "{args.schema}"')  # Why: benchmark-controlled identifier
    await apply_pending(admin, schema=args.schema)
    await admin.execute(
        f'INSERT INTO "{args.schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608
        ACTOR,
        QUEUE,
    )
    await admin.close()

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": args.dsn, "TASKQ_SCHEMA_NAME": args.schema}
    )
    pool = await asyncpg.create_pool(args.dsn, min_size=10, max_size=30)
    assert pool is not None
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )

    worker_id = new_uuid()
    lock_lease = timedelta(seconds=30)
    local_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=args.consumers * 2)

    t0 = time.perf_counter()
    done = 0

    async def producer() -> None:
        i = 0
        while i < args.jobs:
            chunk = [
                EnqueueArgs(
                    id=new_job_id(),
                    actor=ACTOR,
                    queue=QUEUE,
                    payload=make_payload(i + k),
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=None,
                )
                for k in range(min(args.batch, args.jobs - i))
            ]
            await backend.enqueue_batch(chunk)
            i += len(chunk)

    async def dispatcher() -> None:
        total = 0
        while True:
            rows = await backend.dispatch_batch(worker_id, [QUEUE], args.batch, lock_lease)
            if not rows:
                await asyncio.sleep(0.005)
                continue
            print(f"[fleet_load] dispatch {len(rows)} claimed={total}", flush=True)
            for row in rows:
                total += len(rows) - len(rows)  # no-op
                await local_queue.put(row)
            if rows:
                continue
            await asyncio.sleep(0.005)

    async def consumer(n: int) -> None:
        nonlocal done
        while True:
            # A bounded get: every consumer exits once the job count is
            # spent, and a consumer parked on an empty queue cannot wait
            # out the shutdown.
            try:
                row = await asyncio.wait_for(local_queue.get(), 2.0)
            except TimeoutError:
                if done >= args.jobs:
                    return
                continue
            # The production consumer retries the infra family through
            # _terminal_write_with_retry; the harness mirrors that shape
            # (a bounded retry) so a contended pool checkout cannot kill
            # the task and strand the queue.
            for attempt in range(4):
                try:
                    await backend.mark_succeeded(
                        row.id,  # type: ignore[union-attr]
                        worker_id,
                        None,
                        attempt=row.attempt,  # type: ignore[union-attr]
                        claim_epoch=row.claim_epoch,  # type: ignore[union-attr]
                    )
                    break
                except Exception as exc:
                    if attempt == 3:
                        print(f"[fleet_load] consumer {n} gave up: {exc!r}", flush=True)
                        raise
                    await asyncio.sleep(0.05 * (attempt + 1))
            local_queue.task_done()
            done += 1
            if done % 1000 == 0:
                print(f"[fleet_load] done={done}", flush=True)
            if done >= args.jobs:
                return

    prod = asyncio.create_task(producer())
    disp = asyncio.create_task(dispatcher())
    consumers = [asyncio.create_task(consumer(k)) for k in range(args.consumers)]
    print("[fleet_load] producer done", flush=True)
    await prod
    await asyncio.gather(*consumers)
    disp.cancel()
    wall = time.perf_counter() - t0
    print(
        f"[fleet_load] jobs={done} consumers={args.consumers} wall={wall:.2f}s "
        f"jobs/sec={done / wall:.0f}"
    )
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
