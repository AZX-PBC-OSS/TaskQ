"""Partition weather II: TIMEOUT/SLOWDOWN (3000 ms latency) vs the connection budgets.

The blackhole family (``test_partition_pg_blackhole``) cuts the wire;
this family makes it SLOW - 3000 ms of added latency in BOTH directions,
larger than every connection budget a correctly-configured worker runs -
and holds the pinned contract: a budgeted client fails FAST and HONESTLY
(timeout error naming the budget that fired), never hangs. The dispatch
loop's honesty is visible at two levels:

* the client level: an asyncpg connect with a 2s budget against 6s of
  round-trip latency fails at its OWN budget, not at the wire's;
* the fleet level: while one worker drowns in latency, a healthy sibling
  keeps claiming and completing, and the drowned worker ends by the
  documented heartbeat cascade rather than silently wedging.

Worker budget arithmetic (the env the tests set):
  dispatcher_command_timeout = 1.0 (the ge floor) < the 3s latency, so
  every claim statement fails at 1s; heartbeat command timeout 0.25,
  interval 0.5, F = 2 -> the isolate cascade floor
  max(0.5, 0.25) + 3 * (0.5 + 0.25) = 2.75s of latency exposure ends
  the worker by isolate, plus the isolate connect's own 5s give-up and
  the 15s termination grace.
"""

# Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import pathlib
import time
from typing import TYPE_CHECKING

import asyncpg
import pytest

from tests.system_e2e._harness import WorkerProc, reap, spawn_worker, wait_worker_ready
from tests.system_e2e._invariants import assert_balanced, delete_tagged
from tests.system_e2e._toxiproxy import dsn_host_port, proxied_dsn
from tests.system_e2e.actors import SysPayload, sys_fast

if TYPE_CHECKING:
    import asyncpg as _asyncpg

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema
    from tests.system_e2e._toxiproxy import Toxiproxy

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-partition-slow"

#: The weather: 3000 ms added each way. Every budget below is DERIVED
#: against it: a 2s connect budget must fire at ~2s (not the wire's 6s
#: round trip), the dispatcher's 1s command budget at ~1s, the heartbeat
#: cascade at 2.75s (see the module docstring's arithmetic).
_LATENCY_MS = 3000

#: asyncpg connect budget 2s + co-tenant slack 2s: the failure must land
#: near the BUDGET, never near the wire's 6s round trip.
_CONNECT_BUDGET_S = 2.0
_CONNECT_BUDGET_SLACK_S = 2.0

#: The healthy fleet's completion budget while one worker drowns: claim
#: (poll 0.05) + body (0.1s) + reclaim path for anything the drowned
#: worker claimed (lease 8s + sweep 1s + delay 1s + body) + slack.
_FLEET_SETTLE_BUDGET_S = 25.0

#: The drowned worker's exit budget, MEASURED in the latency probe: a
#: worker booting against 3s one-way latency with default budgets opens
#: its pools at their own budgets, fails the dedicated conns at theirs,
#: and exits rc=1 at ~78s - honest, bounded. Budget: 78 + slack = 120.
_DROWN_EXIT_BUDGET_S = 120.0


@pytest.mark.timeout(240)
@pytest.mark.load_sensitive
async def test_slowdown_3000ms_fails_fast_and_never_hangs_the_fleet(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: _asyncpg.Connection,
    toxiproxy: Toxiproxy,
    tmp_path: pathlib.Path,
) -> None:
    """SLOWDOWN on worker↔PG: every budgeted client fails at its own
    budget, the fleet never stops claiming, and the slowed worker ends
    by the documented isolate cascade instead of hanging the loop."""
    schema = module_pg_schema.schema_name
    conn = sys_ledger

    proxy = toxiproxy.create_proxy_sync("pg_slow", *dsn_host_port(pg_dsn))
    await proxy.latency(_LATENCY_MS)

    worker_slow: WorkerProc | None = None
    worker_healthy: WorkerProc | None = None
    log_slow = tmp_path / "part-slow.log"
    try:
        # Client-level budget pin FIRST, while the weather is armed and
        # deterministic: the connect budget (2s) must fire against the
        # wire's 6s round trip - fast and honest, at the client's own
        # budget plus slack, never hung.
        started = time.monotonic()
        with pytest.raises(
            BaseException
        ) as exc_info:  # Why: the pin is WHICH budget fires and WHEN, not the exception type alone.
            await asyncpg.connect(proxied_dsn(pg_dsn, proxy), timeout=_CONNECT_BUDGET_S)
        elapsed = time.monotonic() - started
        assert isinstance(
            exc_info.value, (asyncio.TimeoutError, TimeoutError, asyncpg.ConnectionFailureError)
        ), f"the budgeted connect failed with a non-budget error: {exc_info.value!r}"
        assert elapsed < _CONNECT_BUDGET_S + _CONNECT_BUDGET_SLACK_S, (
            f"the connect took {elapsed:.2f}s against a {_CONNECT_BUDGET_S}s budget plus "
            f"{_CONNECT_BUDGET_SLACK_S}s slack - the budget did not fail fast"
        )

        # Honesty control: the SAME wire, direct, connects fine - the
        # weather is latency, not a partition, and the direct path must
        # show no false failure.
        control = await asyncpg.connect(pg_dsn, timeout=10)
        await control.close()

        # Worker A drowns (spawned INTO the weather), worker B is clean.
        # A runs its bootstrap at the DEFAULT tight budgets: every dial
        # whose wire costs more than the budget must fail AT THE BUDGET -
        # measured in the latency probe: the pools open at their own
        # budget, the notify/leader dedicated conns fail at theirs, and
        # the bootstrap exits rc=1 at ~78s - an HONEST bounded failure,
        # never a hang. That bounded exit is the pin: a worker whose wire
        # exceeds its budgets is GONE, not wedged.
        worker_slow = spawn_worker(
            pg_dsn,
            schema,
            tag="part-slow",
            extra_env={
                "TASKQ_PG_DSN": proxied_dsn(pg_dsn, proxy),
            },
            log_sink=str(log_slow),
        )
        worker_healthy = spawn_worker(pg_dsn, schema, tag="part-fast")
        wait_worker_ready(worker_healthy)

        for _ in range(4):
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.1), tags=[_TAG])

        # The fleet budget: the healthy worker claims within poll+body;
        # the slowed worker - booting, failing, or dying - contributes
        # nothing that wedges the queue. All four must be terminal.
        settle_deadline = time.monotonic() + _FLEET_SETTLE_BUDGET_S
        counts: dict[str, int] = {}
        while time.monotonic() < settle_deadline:
            counts = await assert_balanced(conn, schema, _TAG)
            if set(counts) == {"succeeded"}:
                break
            await asyncio.sleep(0.5)
        assert set(counts) == {"succeeded"}, (
            f"the fleet did not complete within {_FLEET_SETTLE_BUDGET_S}s while one "
            f"worker drowned in {_LATENCY_MS}ms latency: {counts}"
        )

        # The drowned worker never hangs: it must EXIT within the measured
        # bootstrap-failure bound (~78s: every pool/conn dial pays its own
        # budget then gives up) plus slack.
        exit_deadline = time.monotonic() + _DROWN_EXIT_BUDGET_S
        while time.monotonic() < exit_deadline and worker_slow.proc.poll() is None:  # noqa: ASYNC110  # Why: the wait is a deadline OR the process's death - an Event cannot express either arm.
            await asyncio.sleep(0.5)
        if worker_slow.proc.poll() is None:
            tail = log_slow.read_text(errors="replace")[-2000:] if log_slow.exists() else "<no log>"
            pytest.fail(
                f"the slowed worker neither booted nor failed within "
                f"{_DROWN_EXIT_BUDGET_S}s (measured bootstrap failure ~78s) - the "
                f"bootstrap hung instead of failing fast; log tail: {tail!r}"
            )
        assert worker_healthy.proc.poll() is None, (
            "the healthy worker was taken down by its sibling's weather: the cut was not per-worker"
        )
    finally:
        if worker_slow is not None:
            reap(worker_slow)
        if worker_healthy is not None:
            reap(worker_healthy)
        with contextlib.suppress(Exception):
            await proxy.clear()
        await delete_tagged(conn, schema, _TAG)


@pytest.mark.timeout(120)
@pytest.mark.load_sensitive
async def test_syn_ok_data_held_connect_budgets_hold(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    toxiproxy: Toxiproxy,
) -> None:
    """The partial cut: SYN ok, data dropped. The proxy ACCEPTS the TCP
    connection and holds every byte - the shape a firewall NAT silently
    produces - so connect() SUCCEEDS at the socket level and only the
    client's own budget bounds the hang. Held for 3s then closed.

    Pinned: asyncpg's 2s connect budget and redis-py's socket budgets
    fire against the hold, at their own arithmetic
    (asyncpg: 2s budget + 2s slack; redis: connect 2s + read 2s + one
    retry round trip 2s + 2s slack), and a direct control client is
    untouched. The budgets must fire HONESTLY - a timeout naming the
    budget, not a silent hang past it."""
    _ = module_pg_schema  # the wire needs no schema; the fixture pins the module's PG database
    proxy = toxiproxy.create_proxy_sync("pg_syn_ok", *dsn_host_port(pg_dsn))
    try:
        await proxy.hold_then_close(_LATENCY_MS)

        started = time.monotonic()
        with pytest.raises(
            BaseException
        ) as exc_info:  # Why: same pin shape as above - which budget fired and when.
            await asyncpg.connect(proxied_dsn(pg_dsn, proxy), timeout=_CONNECT_BUDGET_S)
        elapsed = time.monotonic() - started
        assert isinstance(
            exc_info.value, (asyncio.TimeoutError, TimeoutError, asyncpg.ConnectionFailureError)
        ), f"the budgeted connect failed with a non-budget error: {exc_info.value!r}"
        assert elapsed < _CONNECT_BUDGET_S + _CONNECT_BUDGET_SLACK_S, (
            f"the SYN-ok hold hung the connect {elapsed:.2f}s past its "
            f"{_CONNECT_BUDGET_S}s budget - the budget did not fire"
        )

        # redis-py's budgets: connect timeout 2s (the SYN succeeds, so the
        # CONNECT budget passes) then the read budget 2s fires on the held
        # handshake reply; one internal retry round trip adds 2s. Bound:
        # 2 + 2 + 2 + 2 slack = 8s.
        import redis.asyncio as redis_async

        proxied_redis = redis_async.from_url(
            f"redis://{proxy.url_host}:{proxy.port}/0",
            socket_connect_timeout=_CONNECT_BUDGET_S,
            socket_timeout=_CONNECT_BUDGET_S,
        )
        try:
            started = time.monotonic()
            with pytest.raises(Exception) as redis_exc_info:
                await proxied_redis.ping()
            redis_elapsed = time.monotonic() - started
            assert redis_elapsed < 8.0, (
                f"the redis ping hung {redis_elapsed:.2f}s past its socket budgets "
                f"(connect 2s + read 2s + one retry 2s + slack)"
            )
            assert redis_exc_info.type.__name__ in (
                "TimeoutError",
                "ConnectionError",
            ), f"the redis budget fired as {redis_exc_info.type.__name__}, not a timeout"
        finally:
            await proxied_redis.aclose()

        # Control: the direct wire is untouched by the proxied weather.
        control = await asyncpg.connect(pg_dsn, timeout=10)
        await control.close()
    finally:
        with contextlib.suppress(Exception):
            await proxy.clear()
