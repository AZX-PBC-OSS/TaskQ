"""Lifecycle 3: a redis outage window spanning enqueues, dispatches, progress.

The broker stops serving commands (``CLIENT PAUSE ALL``: every redis
round trip in the window times out and drops) while the fleet keeps
taking work. Jobs are enqueued INTO the window, dispatched inside it,
and publish progress inside it - the outage covers all three.

System invariants: the durable PG surface never loses a consumed seq
(progress-loss accounting: the poll-state ``progress_seq`` carries every
seq the body consumed, and every seq any subscriber received is <= the
durable seq), jobs that ran inside the window still terminate (the
terminal write is PG, not broker), the worker process survives the
outage, and after the pause lifts the fanout resumes for the next job
(the loss was bounded to the window, not a permanent dead broker).
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import TYPE_CHECKING

import pytest
import redis.asyncio as redis_async

from taskq.constants import progress_channel
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e.actors import SysPayload, sys_fast, sys_progress

if TYPE_CHECKING:
    import asyncpg
    from testcontainers.community.redis import RedisContainer

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s3"
_PAUSE_MS = 15000
_BEATS = 6


@pytest.mark.timeout(300)
async def test_paused_broker_spans_enqueue_dispatch_progress_and_conserves(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    killable_redis_container: RedisContainer,
) -> None:
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    from taskq.testing.fixtures import (
        redis_url_for,  # Why: keeps the container fixture import lazy, matching the main suite's pattern.
    )

    redis_url = redis_url_for(killable_redis_container)

    worker: WorkerProc | None = None
    admin = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    subscriber = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    psub = subscriber.pubsub()
    try:
        await admin.ping()
        worker = spawn_worker(pg_dsn, schema, redis_url=redis_url, tag="s3")
        wait_worker_ready(worker)

        # Enqueue before the pause and subscribe on the healthy broker:
        # the subscription proves what the fanout DID deliver, so the
        # loss assertions below cannot be vacuous.
        handle = await sys_client.enqueue(sys_progress, SysPayload(beats=_BEATS), tags=[_TAG])
        job_x = handle.job_id
        await psub.subscribe(progress_channel(schema, job_x))
        await asyncio.sleep(0.5)

        # The outage: every command from every client in the window
        # stalls past its timeout and drops - enqueued publishes,
        # dispatch-side publishes, and progress publishes alike.
        await admin.execute_command("CLIENT", "PAUSE", str(_PAUSE_MS), "ALL")

        # Enqueues INSIDE the window (PG surface: never blocked by the
        # broker) and dispatch/progress of the pre-pause job both run
        # into the pause.
        inside_handles = [
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG]) for _ in range(2)
        ]

        # The window: the pre-pause job's whole fanout runs and drops
        # inside it.
        await asyncio.sleep(9.0)

        received: list[int] = []
        msg = await psub.get_message(timeout=0.1)
        while msg is not None:
            payload = msg.get("data")
            if isinstance(payload, bytes) and b'"seq"' in payload:
                received.append(int(json.loads(payload)["seq"]))
            msg = await psub.get_message(timeout=0.1)

        row = await conn.fetchrow(
            f"SELECT status::text AS status, progress_seq AS seq "
            f'FROM "{schema}".jobs WHERE id = $1 AND tags @> ARRAY[$2::text]',
            job_x,
            _TAG,
        )
        assert row is not None, "the pre-pause job vanished from the jobs table"

        # Progress-loss accounting: the durable surface carries every
        # consumed seq (beats + the terminal event's seq), and the
        # subscriber saw strictly less (the pause dropped publishes).
        assert row["status"] == "succeeded", (
            f"the job did not reach terminal inside the broker outage: {row}"
        )
        assert row["seq"] >= _BEATS + 1, (
            f"durable progress_seq {row['seq']} lost consumed seqs (expected >= {_BEATS + 1})"
        )
        for seq in received:
            assert seq <= row["seq"], f"subscriber saw seq {seq} the durable surface never carried"
        assert len(received) < _BEATS + 1, (
            "the subscriber saw the whole fanout: the pause dropped nothing "
            "and the loss assertion is vacuous"
        )

        # Let the pause expire fully (the admin connection is inside the
        # paused population too), then lift it defensively.
        await asyncio.sleep((_PAUSE_MS - 9000) / 1000.0 + 1.0)
        await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")

        # The fanout resumes: the next job's events flow to a subscriber
        # again, on the SAME worker process (the outage did not kill it).
        assert worker is not None and worker.proc.poll() is None, (
            "the worker did not survive the outage window"
        )
        handle_b = await sys_client.enqueue(sys_progress, SysPayload(beats=_BEATS), tags=[_TAG])
        await psub.subscribe(progress_channel(schema, handle_b.job_id))
        deadline = time.monotonic() + 30.0
        got_after = 0
        row_b: asyncpg.Record | None = None
        while time.monotonic() < deadline:
            msg = await psub.get_message(timeout=0.2)
            data = msg.get("data") if msg is not None else None
            if isinstance(data, bytes) and b'"seq"' in data:
                got_after += 1
            row_b = await conn.fetchrow(
                f'SELECT status::text AS status FROM "{schema}".jobs WHERE id = $1',
                handle_b.job_id,
            )
            if row_b is not None and row_b["status"] == "succeeded":
                break
        assert row_b is not None and row_b["status"] == "succeeded", (
            f"the post-pause job never went terminal: {row_b}"
        )
        assert got_after >= 1, (
            "the fanout never resumed after the pause: the loss was not "
            "bounded to the outage window"
        )

        # The population balances: the two in-window enqueues terminated
        # too, and the conservation counter holds for the whole tag.
        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("succeeded", 0) == 2 + len(inside_handles), (
            f"the outage window lost jobs: {counts}"
        )
        assert set(counts) <= {"succeeded"}, f"the outage manufactured outcomes: {counts}"
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")
        with contextlib.suppress(Exception):
            await admin.aclose()
        with contextlib.suppress(Exception):
            await subscriber.aclose()
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)
