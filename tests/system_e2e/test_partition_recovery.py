"""Partition weather IV: RECOVERY - after the weather clears, everything converges.

The prior families prove the contracts DURING the cut. This family pins
the other half of a partition's lifecycle: a worker that took real
weather - latency, a silent blackhole, a hold-then-close pass, in that
order - must converge WITHOUT a restart when the wire heals: its
connection pools refill (asyncpg replaces the cut connections on the
next acquire, never re-serving a poisoned one), its broker fanout
resubscribes/reconnects, and it keeps claiming work at the normal
cadence. The worker is held out of the heartbeat isolate cascade on
purpose (F = 40): the isolate-exit contract has its own cell in
``test_partition_pg_blackhole``; this cell attacks the pool and fanout
surfaces a surviving worker owns.

Budget arithmetic (derived, never a blind sleep):
* The weather stays strictly inside the cascade floor
  ``max(0.5, 0.25) + 41 * 0.75 = 31.25s`` of CUMULATIVE failed-beat
  exposure is the isolate budget at F = 40; the phases below sum to
  5.5s of weather, so the worker cannot isolate even if EVERY beat in
  the window failed.
* Post-recovery claim budget: poll 0.05s + body 0.1s + pool-repair
  slack (a cut connection costs one failed command before the pool
  replaces it) -> 10s for a five-job batch is the honest fleet bound.
"""

# Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

import pytest
import redis.asyncio as redis_async

from taskq.constants import progress_channel
from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, assert_effects_balance, delete_tagged
from tests.system_e2e._toxiproxy import dsn_host_port, proxied_dsn
from tests.system_e2e.actors import SysPayload, sys_fast, sys_progress

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema
    from tests.system_e2e._toxiproxy import Toxiproxy

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-partition-recovery"

#: Weather phases, all strictly inside the F=40 cascade budget (31.25s
#: of cumulative failed-beat exposure; the phases sum to 5.5s):
_LATENCY_MS = 1500
_LATENCY_HOLD_S = 3.0
_BLACKHOLE_HOLD_S = 1.5
_HOLD_THEN_CLOSE_MS = 1000
_HOLD_THEN_CLOSE_S = 2.0

#: Post-recovery fleet budget: claim (poll 0.05) + body (0.1) + pool
#: repair (one failed command per cut connection) + co-tenant slack.
_POST_RECOVERY_BUDGET_S = 10.0


@pytest.mark.timeout(240)
async def test_weather_then_recovery_converges_without_restart(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    redis_container: Any,
    toxiproxy: Toxiproxy,
) -> None:
    """Every weather shape in one worker's lifetime, then the heal: the
    SAME process refills its pools, resumes its fanout, and drains the
    post-recovery batch at the normal cadence."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    pg_host, pg_port = dsn_host_port(pg_dsn)
    pg_proxy = toxiproxy.create_proxy_sync("pg_recovery", pg_host, pg_port)
    redis_host, redis_port = (
        redis_container.get_container_host_ip(),
        redis_container.get_exposed_port(6379),
    )
    redis_proxy = toxiproxy.create_proxy_sync("redis_recovery", redis_host, redis_port)

    worker: WorkerProc | None = None
    subscriber: redis_async.Redis | None = None
    try:
        worker = spawn_worker(
            pg_dsn,
            schema,
            redis_url=f"redis://{redis_proxy.url_host}:{redis_proxy.port}/0",
            tag="part-recovery",
            extra_env={
                "TASKQ_PG_DSN": proxied_dsn(pg_dsn, pg_proxy),
                "TASKQ_MAX_HEARTBEAT_FAILURES": "40",
                # The lease validator requires the lease to cover the F=40
                # cascade (0.5 + 41 * 1.0 = 41.5s): this worker must ride
                # OUT every weather phase, never isolate.
                "TASKQ_LOCK_LEASE": "45.0",
            },
        )
        wait_worker_ready(worker)

        # Work BEFORE the weather, work DURING each phase.
        handles = [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG]) for _ in range(3)
        ]
        await pg_proxy.latency(_LATENCY_MS)
        await asyncio.sleep(_LATENCY_HOLD_S)
        await pg_proxy.clear()
        handles += [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG]) for _ in range(2)
        ]

        await pg_proxy.blackhole()
        await asyncio.sleep(_BLACKHOLE_HOLD_S)
        await pg_proxy.clear()
        handles += [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG]) for _ in range(2)
        ]

        await pg_proxy.hold_then_close(_HOLD_THEN_CLOSE_MS)
        await asyncio.sleep(_HOLD_THEN_CLOSE_S)
        await pg_proxy.clear()
        await redis_proxy.clear()

        # THE heal, asserted on the same process: no restart, pools
        # refilled, fanout reconnected.
        assert worker.proc.poll() is None, "the worker exited during the weather"

        # (a) Pool refill: the post-recovery batch must DRAIN - each cut
        # pool connection is replaced on its next acquire, never re-served.
        handles += [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG]) for _ in range(5)
        ]
        settle_deadline = time.monotonic() + _POST_RECOVERY_BUDGET_S
        counts: dict[str, int] = {}
        while time.monotonic() < settle_deadline:
            counts = await assert_balanced(conn, schema, _TAG)
            if set(counts) == {"succeeded"}:
                break
            await asyncio.sleep(0.5)
        assert set(counts) == {"succeeded"}, (
            f"the post-recovery batch did not drain within {_POST_RECOVERY_BUDGET_S}s "
            f"- the pools did not refill: {counts}"
        )

        # (b) Fanout resubscribe: a subscriber that dials the HEALED wire
        # sees the worker's progress frames for a new job - the worker's
        # broker client reconnected, the fanout is alive again. The
        # channel name is the library's own ``progress_channel`` shape.
        fanout_handle = await sys_client.enqueue(sys_progress, SysPayload(beats=8), tags=[_TAG])
        subscriber = redis_async.from_url(
            f"redis://{redis_proxy.url_host}:{redis_proxy.port}/0",
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
        psub = subscriber.pubsub()
        await psub.subscribe(progress_channel(schema, fanout_handle.job_id))
        saw_frame = False
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and not saw_frame:
            msg = await psub.get_message(timeout=0.5)
            if msg is not None and msg.get("type") == "message":
                saw_frame = True
        assert saw_frame, (
            "the worker's progress fanout never resumed on the healed wire: the "
            "broker client did not reconnect after the cut"
        )

        # (c) The full balance: every job that crossed the weather - 12
        # fast + the fanout job - terminalised exactly once.
        assert len(handles) == 12
        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) == {"succeeded"}, f"the weather manufactured outcomes: {counts}"
        await assert_effects_balance(conn, schema, _TAG)
        assert worker.proc.poll() is None, (
            "the worker did not survive to the end of the scenario on the same process"
        )
    finally:
        if subscriber is not None:
            with contextlib.suppress(Exception):
                await subscriber.aclose()
        if worker is not None:
            reap(worker)
        # Each proxy's clear is suppressed SEPARATELY: a wedged server
        # failing the PG sweep must not skip the redis sweep - one
        # leaked toxic on a shared server is exactly the residue the
        # per-proxy ledgers exist to prevent.
        with contextlib.suppress(Exception):
            await pg_proxy.clear()
        with contextlib.suppress(Exception):
            await redis_proxy.clear()
        await delete_tagged(conn, schema, _TAG)
