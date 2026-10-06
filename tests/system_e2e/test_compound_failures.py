"""The compound-failure stories: two dependencies failing in ONE scenario.

Every prior scenario in this tier cuts ONE wire at a time. Real incidents
do not queue: the broker outage lands while the database wire is already
degraded, the admin's pool is exhausted by the same storm that took the
broker, the operator's cancel lands while a job sits out a Retry-After
reprieve. This module runs those compounds on the real containers and the
real worker subprocesses, and pins what each surface owes when BOTH
failures are live:

1. PG degraded (a blackhole on the worker's whole PG wire) WHILE the
   broker is paused - the worker survives on its budgets, the jobs land
   in a consistent state (durable rows, nothing running on a lie), and
   the recovery is clean when both dependencies return;
2. the admin's PG pool exhausted WHILE the broker is paused - the admin
   answers its 503/Retry-After contract within its bound instead of
   hanging, and the worker (whose pools are its own) keeps serving;
3. the operator's cancel DURING a hint-wait - a job sitting out a
   Retry-After reprieve (the limiter's denial arm re-pended it with a
   future ``scheduled_at``) is terminalised promptly, the cancel wins
   BEFORE the reprieve expires (the durable receipt: ``finished_at`` is
   stamped earlier than the hint it never waited out), and no zombie
   waits out the hint to run the work the operator cancelled.

System invariants close every scenario: the conservation counter, the
exactly-once effects ledger, and every timing bound derived from the
scenario's own knobs (the tier constants, the worker env's arithmetic) -
never a bare wall-clock number.
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from a fixture the settings boundary validated, and every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest
import redis.asyncio as redis_async
from fastapi import FastAPI

from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.web.admin import create_router, setup_admin_state
from tests.system_e2e._harness import (
    BOOT_READY_BOUND_S,
    CANCELLATION_GRACE_S,
    HEARTBEAT_INTERVAL_S,
    SWEEP_INTERVAL_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    reap,
    scoped_dsn,
    spawn_worker,
    wait_worker_ready,
)
from tests.system_e2e._invariants import (
    assert_balanced,
    assert_effects_balance,
    conservation_violations,
    delete_tagged,
)
from tests.system_e2e._toxiproxy import dsn_host_port, proxied_dsn
from tests.system_e2e.actors import RatedPayload, SysPayload, sys_fast, sys_rated_slow

if TYPE_CHECKING:
    from httpx import AsyncClient

    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema
    from tests.system_e2e._toxiproxy import Toxiproxy

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-compound"

# ── The derived bounds (the tier's doctrine, in-code) ────────────────────
#: The dispatch cadence every wait here budgets: the harness's sweep tick
#: plus the poll floor.
_POLL_FLOOR_S = 1.0
_CLAIM_CYCLE_S = SWEEP_INTERVAL_S + _POLL_FLOOR_S

#: One token per twenty seconds (the slow bucket the cancel-during-a-
#: hint-wait scenario drains against): a denial's Retry-After hint
#: prices the NEXT token, so the second and third concurrent enqueue are
#: re-pended ~20s and ~40s out - a DEEP reprieve the scenario can sit
#: inside (the fast bucket's ~2s hint re-claims before an operator can
#: act).
_REFILL_PACE_S = 20.0

#: The operator-cancel bound: the worker observes the phase-1 flag on its
#: heartbeat poll (three beats of slack), the body/cooperative path
#: answers, the terminal write lands - stretched by the tier's load
#: factor. A cancel that never wins reds here.
_CANCEL_LAND_BOUND_S = (HEARTBEAT_INTERVAL_S * 3 + CANCELLATION_GRACE_S) * TIER_LOAD_STRETCH

#: The web admin's own checkout budget (TASKQ_ADMIN_ACQUIRE_TIMEOUT, set
#: by the scenario below) plus the slack a starved runner's observation
#: gets - the 503 must land near the BUDGET, never hang to the wire.
_ADMIN_ACQUIRE_TIMEOUT_S = 1.0
_ADMIN_503_SLACK_S = _ADMIN_ACQUIRE_TIMEOUT_S + 2.0


async def _poll(
    predicate: Any,
    timeout: float,  # noqa: ASYNC109  # Why: the wait budget, not a missing asyncio.timeout pattern - the predicate, not the clock, decides.
    desc: str,
    interval: float = 0.25,
) -> None:
    """Wait until *predicate* holds, or fail naming what never came true."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {desc}")


async def _wait_registered(conn: asyncpg.Connection, schema: str, pid: int) -> None:
    """The joined-fleet standard for a single-worker scenario: a
    registered row within the harness's boot-readiness bound."""
    deadline = time.monotonic() + BOOT_READY_BOUND_S

    async def _registered() -> bool:
        row = await conn.fetchval(f'SELECT count(*) FROM "{schema}".workers WHERE pid = $1', pid)
        return int(row) >= 1

    await _poll(_registered, deadline, "the worker registered its fleet row")


async def _job_succeeded(conn: asyncpg.Connection, schema: str, job_id: Any) -> bool:
    """The durable receipt of one job's terminal success."""
    row = await conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE id = $1 AND status = 'succeeded'",
        job_id,
    )
    return int(row) == 1


class _AdminSettings:
    schema_name: str

    def __init__(self, schema_name: str) -> None:
        self.schema_name = schema_name


class _AdminDeps:
    """The deps protocol the backend reads (the operational-loop family's
    idiom): the settings boundary the admin routes' schema identifiers
    come from, and the pools the backend's own checkouts use."""

    settings: _AdminSettings
    worker_pool: Any
    heartbeat_pool: Any
    dispatcher_pool: Any = None

    def __init__(self, schema: str, pool: Any) -> None:
        self.settings = _AdminSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


async def _open_embedded_admin(dsn: str, schema: str, pool: asyncpg.Pool) -> AsyncClient:
    """The embedded admin router over a caller-owned pool (the admin-ui.md
    embedding contract, the operational-loop family's idiom): the pool is
    passed in so a scenario can hold its connections out - the exhaustion
    is real, not simulated."""
    backend = PostgresBackend(
        _AdminDeps(schema, pool),
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )
    app = FastAPI()
    bundle = create_router(pool, schema=schema, backend=backend, base_path="/embed")
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/embed")
    from httpx import ASGITransport, AsyncClient

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver")


def _admin_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dev admin wiring the embedded router's construction reads (the
    fail-closed auth check and the CSRF cookie policy over plain HTTP),
    the same pair the operational-loop family's autouse fixture pins."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_ACQUIRE_TIMEOUT", str(_ADMIN_ACQUIRE_TIMEOUT_S))


# ══ 1. PG degraded WHILE the broker is down ══════════════════════════════


@pytest.mark.timeout(300)
async def test_pg_degraded_while_redis_down_worker_survives_and_recovers_clean(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    killable_redis_container: Any,
    toxiproxy: Toxiproxy,
    tmp_path: Any,
) -> None:
    """A blackhole on the worker's WHOLE PG wire lands while the broker is
    mid-pause: both dependencies fail in the same window.

    Pinned: the worker survives the window ON ITS BUDGETS (the heartbeat
    cascade's fastest legal span outlasts the window by arithmetic, so
    surviving is not luck), the jobs enqueued inside the window land in a
    consistent durable state (pending rows, nothing running on a dead
    wire, the broker's outage real from an independent probe), and when
    BOTH dependencies return the recovery is clean - the backlog
    completes, the conservation counter balances, the effects ledger
    reconciles, and the worker's own row is still heartbeating.
    """
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    from taskq.testing.fixtures import redis_url_for

    redis_url = redis_url_for(killable_redis_container)

    # The worker's compound-rideout arithmetic (the blackhole cell's
    # derivation, sized for THIS window): with command_timeout >= 0.75 *
    # interval the two failed-cycle shapes coincide at one interval per
    # cycle, the settings default F=3 puts the isolate decision at the
    # 4th consecutive failure, so the cascade's FASTEST legal span is
    # (F+1) * interval = 24s. A 20s window sits strictly inside it: a
    # worker that died mid-window would be a defect, not weather. The
    # lease carries the settings' post_load floor
    # (tail + (F+1) * worst_cycle = 6 + 4 * 10.5 = 48) with headroom.
    _interval_s = 6.0
    _cmd_timeout_s = 4.5  # >= 0.75 * interval: the two cycle shapes coincide
    _max_failures = 3
    _worst_cycle_s = _interval_s + _cmd_timeout_s  # 10.5
    _isolate_floor_s = (_max_failures + 1) * _interval_s  # 24.0
    _window_s = 20.0
    assert _window_s < _isolate_floor_s, "the window must sit inside the cascade floor"
    _lease_s = max(_interval_s, _cmd_timeout_s) + (_max_failures + 1) * _worst_cycle_s + 12.0
    # The recovery bound: two worst failed cycles (the wedged statements'
    # budgets firing) + one claim cycle + the poll floor, stretched.
    _recover_bound_s = (_worst_cycle_s * 2 + _CLAIM_CYCLE_S + _POLL_FLOOR_S) * TIER_LOAD_STRETCH

    proxy = toxiproxy.create_proxy_sync("pg_compound_a", *dsn_host_port(pg_dsn))
    worker: WorkerProc | None = None
    admin = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    probe = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=3)
    log_sink = tmp_path / "compound-a.log"
    try:
        await admin.ping()
        worker = spawn_worker(
            pg_dsn,
            schema,
            redis_url=redis_url,
            tag="compound-a",
            extra_env={
                # scoped BEFORE proxied (the blackhole cell's measured
                # ordering): the application_name stamp survives the swap.
                "TASKQ_PG_DSN": proxied_dsn(scoped_dsn(pg_dsn, schema), proxy),
                "TASKQ_HEARTBEAT_INTERVAL": str(_interval_s),
                "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": str(_cmd_timeout_s),
                "TASKQ_MAX_HEARTBEAT_FAILURES": str(_max_failures),
                "TASKQ_LOCK_LEASE": str(_lease_s),
            },
            log_sink=str(log_sink),
        )
        wait_worker_ready(worker)
        assert worker.proc.poll() is None
        await _wait_registered(conn, schema, worker.proc.pid)

        # Premise receipt: the fleet serves BEFORE either failure arms.
        premise = await sys_client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
        await _poll(
            lambda: _job_succeeded(conn, schema, premise.job_id),
            (_CLAIM_CYCLE_S + _POLL_FLOOR_S) * TIER_LOAD_STRETCH,
            "the premise job completed before the compound window",
        )

        # ══ The compound window: BOTH dependencies fail together ═════
        # The broker: every command in the window stalls past its
        # timeout and drops. The wire: the worker's PG packets are
        # accepted and never delivered - existing connections included.
        await admin.execute_command("CLIENT", "PAUSE", "20000", "ALL")
        pause_deadline = time.monotonic() + 20.0
        await proxy.blackhole()

        # The broker's outage is real for EVERY speaker: an independent
        # pre-pause probe client cannot even answer a ping inside the
        # window (bounded by its own socket budget plus slack - honest
        # failure, never a hang).
        started = time.monotonic()
        with pytest.raises(Exception) as ping_exc:
            await probe.ping()
        assert time.monotonic() - started < 3.0 + 2.0, (
            "the broker outage probe neither failed nor failed fast - it hung"
        )
        assert ping_exc.type.__name__ in ("TimeoutError", "ConnectionError"), (
            f"the broker outage surfaced as {ping_exc.type.__name__}, not a budget failure"
        )

        # The worker's enqueue path is PG-side: the in-window work lands
        # in the ledger even with both dependencies dark.
        inside_jobs = [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
            for _ in range(4)
        ]
        inside_ids = [str(h.job_id) for h in inside_jobs]

        # Mid-window consistency: the rows are HONEST - pending (no
        # claim can cross a blackholed wire), nothing of this tag
        # running on a dead holder, and the worker process alive.
        await asyncio.sleep(2.0)

        async def _inside_consistent() -> bool:
            rows = await conn.fetch(
                f"""
                SELECT id, status::text AS status FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text] AND id = ANY($2::uuid[])
                """,
                _TAG,
                inside_ids,
            )
            return all(r["status"] == "pending" for r in rows) and len(rows) == len(inside_ids)

        await _poll(
            _inside_consistent,
            (_CLAIM_CYCLE_S + _POLL_FLOOR_S) * TIER_LOAD_STRETCH,
            "the in-window jobs settled into honest pending rows",
        )
        running_lies = await conn.fetchval(
            f"""
            SELECT count(*) FROM "{schema}".jobs
            WHERE tags @> ARRAY[$1::text] AND status = 'running'
              AND lock_expires_at < clock_timestamp()
            """,
            _TAG,
        )
        assert int(running_lies) == 0, (
            f"{running_lies} running row(s) hold a lapsed lease mid-window - "
            "the compound failure left work marked running on a dead wire"
        )
        assert worker.proc.poll() is None, (
            "the worker did not survive the compound window (both "
            "dependencies dark): the cascade arithmetic says it must"
        )

        # ══ The recovery: BOTH dependencies return ═══════════════════
        await proxy.clear()
        # The pause's admin connection is inside the paused population
        # too: wait the window out, then lift defensively (the
        # redis-outage family's ordering).
        if time.monotonic() < pause_deadline:
            await asyncio.sleep(pause_deadline - time.monotonic())
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")

        for job_id in inside_ids:
            await _poll(
                lambda jid=job_id: _job_succeeded(conn, schema, jid),
                _recover_bound_s,
                "the in-window backlog completed after both dependencies returned",
            )

        # The worker is STILL the same process, and its row is STILL
        # heartbeating (the stale-worker reaping window outlasts the
        # window by arithmetic: interval * (F+3) = 36s > the staleness a
        # 20s window can produce).
        assert worker.proc.poll() is None, "the worker exited during the recovery"
        beat_1 = await conn.fetchval(
            f'SELECT last_seen_at FROM "{schema}".workers WHERE pid = $1',
            worker.proc.pid,
        )
        assert beat_1 is not None, "the worker's fleet row vanished during the compound window"
        await asyncio.sleep(_interval_s * 2)
        beat_2 = await conn.fetchval(
            f'SELECT last_seen_at FROM "{schema}".workers WHERE pid = $1',
            worker.proc.pid,
        )
        assert beat_2 is not None and beat_2 > beat_1, (
            "the worker's heartbeat never resumed after the compound window"
        )

        # The ledger reconciles: the whole tag's population (premise +
        # in-window) balances, and the exactly-once effects ledger agrees.
        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) == {"succeeded"}, f"the compound failure manufactured outcomes: {counts}"
        assert counts["succeeded"] == 1 + len(inside_jobs), counts
        violations = await conservation_violations(conn, schema, _TAG)
        assert not violations, "the compound window broke conservation:\n" + "\n".join(
            violations[:20]
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        with contextlib.suppress(Exception):
            await proxy.clear()
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")
        with contextlib.suppress(Exception):
            await admin.aclose()
        with contextlib.suppress(Exception):
            await probe.aclose()
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ══ 2. Pool exhaustion DURING a broker outage ════════════════════════════


@pytest.mark.timeout(300)
async def test_pool_exhaustion_during_broker_outage_answers_503_and_worker_serves(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    killable_redis_container: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The admin's PG pool exhausted WHILE the broker is paused.

    Pinned: the admin's bounded-checkout contract holds under the
    compound - a page whose checkout cannot arrive answers 503 with
    ``Retry-After`` within the acquire budget (never hangs, never 500s) -
    and the worker, whose pools are its own, keeps claiming and
    terminating work through BOTH failures; when the broker returns and
    the pool's connection is released, the same page answers 200 again.
    """
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    from taskq.testing.fixtures import redis_url_for

    redis_url = redis_url_for(killable_redis_container)
    # The admin router reads the checkout budget from settings at build;
    # the dev pair the operational-loop family pins (the fail-closed auth
    # check and the CSRF cookie policy over plain HTTP).
    _admin_dev_env(monkeypatch)

    worker: WorkerProc | None = None
    client: AsyncClient | None = None
    pool: asyncpg.Pool | None = None
    holder: Any = None
    holder_out = False
    admin = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=10)
    try:
        await admin.ping()
        worker = spawn_worker(pg_dsn, schema, redis_url=redis_url, tag="compound-b")
        wait_worker_ready(worker)
        await _wait_registered(conn, schema, worker.proc.pid)

        premise = await sys_client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
        await _poll(
            lambda: _job_succeeded(conn, schema, premise.job_id),
            (_CLAIM_CYCLE_S + _POLL_FLOOR_S) * TIER_LOAD_STRETCH,
            "the premise job completed before the compound window",
        )

        # The embedded admin on a ONE-connection pool: the holder below
        # IS the exhaustion (every checkout the routes attempt finds an
        # empty pool, exactly the BoundedPool docstring's "all held"
        # shape - the same fact a wedged Postgres or a stream-hogging
        # page produces on the wire).
        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
        assert pool is not None
        client = await _open_embedded_admin(pg_dsn, schema, pool)
        holder = await pool.acquire()
        holder_out = True

        # ══ The compound window ═══════════════════════════════════════
        await admin.execute_command("CLIENT", "PAUSE", "25000", "ALL")
        pause_deadline = time.monotonic() + 25.0

        # The broker outage is real (the independent probe's bounded
        # failure - the same receipt scenario 1 pins).
        probe = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=3)
        try:
            started = time.monotonic()
            with pytest.raises(Exception) as ping_exc:
                await probe.ping()
            assert time.monotonic() - started < 5.0
            assert ping_exc.type.__name__ in ("TimeoutError", "ConnectionError")
        finally:
            with contextlib.suppress(Exception):
                await probe.aclose()

        # THE 503 CONTRACT: with the pool exhausted AND the broker down,
        # the admin page's bounded checkout answers 503 + Retry-After
        # within the budget - a checkout that cannot arrive is a clean,
        # fast refusal naming the knob, not a hang on a dead store.
        target = premise.job_id
        started = time.monotonic()
        page = await client.get(f"/embed/jobs/{target}")
        elapsed = time.monotonic() - started
        assert page.status_code == 503, (
            f"the exhausted admin answered {page.status_code} - the bounded "
            "checkout contract did not hold under the compound failure"
        )
        assert elapsed < _ADMIN_503_SLACK_S, (
            f"the 503 took {elapsed:.2f}s against a {_ADMIN_ACQUIRE_TIMEOUT_S}s "
            "acquire budget - the checkout was not bounded"
        )
        assert page.headers.get("retry-after") == "2", (
            f"the 503 lost its Retry-After header: {dict(page.headers)}"
        )

        # THE WORKER UNDER BOTH FAILURES: its pools are its own - the
        # admin's exhaustion does not couple into dispatch, and the
        # broker's pause does not couple into the PG terminal write.
        # Work enqueued inside the window completes inside the bound the
        # outage's own arithmetic prices: the worker's broker round
        # trips ride redis-py's socket budgets and the resilience retry
        # ladder (3 attempts x 2s + 1s of backoffs, the partition
        # family's derivation) BEFORE falling back to the poll cadence -
        # so the bound is the ladder + one claim cycle + the poll floor,
        # stretched. A worker that wedged (never served) reds here.
        inside_jobs = [
            await sys_client.enqueue(sys_fast, SysPayload(sleep=0.05), tags=[_TAG])
            for _ in range(2)
        ]
        _outage_serve_bound_s = (3 * 2.0 + 1.0 + _CLAIM_CYCLE_S + _POLL_FLOOR_S) * TIER_LOAD_STRETCH
        for handle in inside_jobs:
            await _poll(
                lambda h=handle: _job_succeeded(conn, schema, h.job_id),
                _outage_serve_bound_s,
                "the worker served through the pool exhaustion and the broker outage",
            )
        assert worker.proc.poll() is None, "the worker did not survive the compound window"

        # ══ The recovery: broker back, pool connection released ═══════
        if time.monotonic() < pause_deadline:
            await asyncio.sleep(pause_deadline - time.monotonic())
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")
        await pool.release(holder)
        holder_out = False

        page_after = await client.get(f"/embed/jobs/{target}")
        assert page_after.status_code == 200, (
            f"the admin page never recovered after the pool's connection "
            f"was released: {page_after.status_code}"
        )

        counts = await assert_balanced(conn, schema, _TAG)
        assert set(counts) == {"succeeded"}, f"the compound window manufactured outcomes: {counts}"
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        with contextlib.suppress(Exception):
            await admin.execute_command("CLIENT", "PAUSE", "0", "ALL")
        with contextlib.suppress(Exception):
            await admin.aclose()
        if client is not None:
            with contextlib.suppress(Exception):
                await client.aclose()
        if pool is not None:
            with contextlib.suppress(Exception):
                if holder is not None and holder_out:
                    await pool.release(holder)
                    holder_out = False
            with contextlib.suppress(Exception):
                await asyncio.wait_for(pool.close(), 10.0)
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)


# ══ 3. The operator's cancel DURING a hint-wait ══════════════════════════


@pytest.mark.timeout(300)
async def test_operator_cancel_wins_promptly_during_a_retry_after_reprieve(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job sitting out a Retry-After reprieve (the limiter's denial arm
    re-pended it with a future ``scheduled_at``) gets cancelled by the
    operator - the cancel wins PROMPTLY, and no zombie waits out the hint.

    The no-zombie receipt is durable and clock-free: the row's
    ``finished_at`` is stamped EARLIER than the ``scheduled_at`` it never
    waited out (the DB's own columns, compared in the receipt read), the
    body never ran (no effects row), and the reprieve cohort around the
    cancel still drains through the bucket - the cancel did not wedge the
    limiter or strand the siblings.
    """
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    _admin_dev_env(monkeypatch)

    pool: asyncpg.Pool | None = None
    client: AsyncClient | None = None
    worker: WorkerProc | None = None
    try:
        # One worker (a zombie would have a claimant to ride the hint
        # with); its cancel-poll rides the heartbeat. The limiter is
        # PG-backed (the operational-loop family's bucket), so no broker
        # is in this story - the hint-wait is a DURABLE state.
        worker = spawn_worker(pg_dsn, schema, tag="compound-c")
        wait_worker_ready(worker)
        await _wait_registered(conn, schema, worker.proc.pid)

        # A deep reprieve: the slow bucket refills one token per twenty
        # seconds, so the second and third jobs' denial hints price ~20s
        # and ~40s out - far past the cancel's prompt bound, so
        # "cancelled" can only mean the cancel WON, not that the hint
        # expired first. (The fast bucket's hint prices only the NEXT
        # token - ~2s - and re-claims before an operator can act; the
        # deep reprieve needs the slow refill - the
        # _REFILL_PACE_S bucket at the top of this module.)
        _cohort = 3
        for _ in range(_cohort):
            await sys_client.enqueue(sys_rated_slow, RatedPayload(tenant="t0"), tags=[_TAG])

        # The durable reprieve receipt: rows re-pended by the denial arm
        # - scheduled past the horizon, the denial counter on the row.
        # The first claim takes the bucket's token; the rest are denied.
        reprieve_horizon_s = 5.0
        deadline = time.monotonic() + 60.0
        reprieved: list[asyncpg.Record] = []
        while time.monotonic() < deadline:
            reprieved = await conn.fetch(
                f"""
                SELECT id::text AS id, scheduled_at, rate_limit_blocked_count::int AS blocked
                FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text] AND actor = 'sys_rated_slow'
                  AND status = 'scheduled' AND rate_limit_blocked_count >= 1
                  AND scheduled_at > clock_timestamp() +
                      make_interval(secs => $2::double precision)
                ORDER BY scheduled_at DESC
                """,
                _TAG,
                reprieve_horizon_s,
            )
            if len(reprieved) >= 2:
                break
            await asyncio.sleep(0.25)
        assert len(reprieved) >= 2, (
            f"the denial arm's Retry-After reprieve never landed: "
            f"{len(reprieved)} row(s) past the horizon"
        )
        target = reprieved[0]
        now_ts = await conn.fetchval("SELECT statement_timestamp()")
        hint_s = (target["scheduled_at"] - now_ts).total_seconds()
        assert hint_s > reprieve_horizon_s, (
            f"the deepest reprieve's hint is only {hint_s:.1f}s - the scenario "
            "cannot distinguish a prompt cancel from an expired hint"
        )

        # The operator's cancel through the embedded admin router (the
        # admin-ui.md embedding contract): a real backend, a real route,
        # the CSRF handshake the web_admin tier's idiom carries.
        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        assert pool is not None
        client = await _open_embedded_admin(pg_dsn, schema, pool)
        arm = await client.get(f"/embed/jobs/{target['id']}")
        assert arm.status_code == 200, f"the embedded job page did not render: {arm.status_code}"
        token = client.cookies.get("taskq_csrf_token", "")
        assert token, "no taskq_csrf_token cookie before the admin POST"
        resp = await client.post(
            f"/embed/jobs/{target['id']}/cancel",
            data={"reason": "the reprieve is over for this one", "csrf_token": token},
            follow_redirects=False,
        )
        assert resp.status_code in (200, 303), (
            f"the admin cancel did not land: {resp.status_code} {resp.text[:300]}"
        )

        # PROMPT: the row terminalises 'cancelled' inside the derived
        # cancel bound - one heartbeat's observation + the terminal
        # write, stretched. (The hint it is sitting out is 5+ seconds
        # longer than this bound: landing here means the cancel WON.)
        async def _cancelled() -> bool:
            row = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid',
                target["id"],
            )
            return row == "cancelled"

        await _poll(_cancelled, _CANCEL_LAND_BOUND_S, "the reprieved job reached cancelled")

        # THE NO-ZOMBIE RECEIPT (durable, on the DB's own clocks): the
        # finished stamp precedes the hint the row never waited out; the
        # body never ran; the event carries the operator's reason.
        row = await conn.fetchrow(
            f"""
            SELECT status::text AS status, finished_at, scheduled_at,
                   rate_limit_blocked_count::int AS blocked
            FROM "{schema}".jobs WHERE id = $1::uuid
            """,
            target["id"],
        )
        assert row is not None and row["status"] == "cancelled"
        assert row["finished_at"] is not None, "the cancel left no finished_at stamp"
        assert row["finished_at"] < row["scheduled_at"], (
            f"the cancelled row's finished_at ({row['finished_at']}) is not earlier "
            f"than the hint it sat out ({row['scheduled_at']}) - the cancel waited "
            "out the reprieve instead of winning it"
        )
        effects = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".sys_effects WHERE job_id = $1::uuid',
            target["id"],
        )
        assert int(effects) == 0, (
            "the cancelled reprieved job ran its body - a zombie waited out "
            "the hint and executed cancelled work"
        )
        event = await conn.fetchrow(
            f"""
            SELECT detail FROM "{schema}".job_events
            WHERE job_id = $1::uuid AND kind = 'cancel_request'
            ORDER BY occurred_at DESC LIMIT 1
            """,
            target["id"],
        )
        assert event is not None, "the cancel wrote no cancel_request event"
        detail = event["detail"]
        if isinstance(detail, str):  # Why: asyncpg returns jsonb as text uncoded.
            detail = json.loads(detail)
        assert detail.get("reason") is not None, f"the event lost its reason: {detail}"

        # The cohort around the cancel still drains: eleven succeed
        # through the bucket (the deepest hint the ledger recorded, plus
        # one claim cycle, stretched - the bucket's own arithmetic), the
        # cancelled row STAYS cancelled, and nothing else was
        # manufactured.
        deepest = await conn.fetchval(
            f"""
            SELECT max(scheduled_at) FROM "{schema}".jobs
            WHERE tags @> ARRAY[$1::text] AND actor = 'sys_rated_slow'
            """,
            _TAG,
        )
        assert deepest is not None
        now_ts = await conn.fetchval("SELECT statement_timestamp()")
        # The drain bound prices the bucket's own arithmetic: the deepest
        # remaining hint, PLUS one full refill period (a re-denial costs
        # the cohort one token's wait - the refill is the bucket's own
        # 1-per-_REFILL_PACE_S pace the actor's comment derives), plus
        # one claim cycle, all stretched, plus the poll floor.
        drain_bound_s = (
            max(0.0, (deepest - now_ts).total_seconds()) + _REFILL_PACE_S + _CLAIM_CYCLE_S
        ) * TIER_LOAD_STRETCH + _POLL_FLOOR_S

        cohort_seen: list[asyncpg.Record] = []

        async def _cohort_settled() -> bool:
            # A plain count, not the invariant (the invariant's own settle
            # would raise inside the predicate instead of waiting the
            # poll bound out); the invariant closes the scenario below.
            rows = await conn.fetch(
                f"""
                SELECT id::text AS id, status::text AS status, attempt::int AS attempt,
                       rate_limit_blocked_count::int AS blocked
                FROM "{schema}".jobs
                WHERE tags @> ARRAY[$1::text] AND actor = 'sys_rated_slow'
                ORDER BY id
                """,
                _TAG,
            )
            cohort_seen[:] = rows
            ok = sum(1 for r in rows if r["status"] == "succeeded")
            cx = sum(1 for r in rows if r["status"] == "cancelled")
            return ok >= _cohort - 1 and cx == 1 and len(rows) == ok + cx

        try:
            await _poll(
                _cohort_settled,
                drain_bound_s,
                "the reprieve cohort drained past the cancel",
            )
        except AssertionError:
            raise AssertionError(
                f"the reprieve cohort never drained past the cancel within "
                f"{drain_bound_s:.0f}s; cohort rows: {[dict(r) for r in cohort_seen]}"
            ) from None
        still = await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid',
            target["id"],
        )
        assert still == "cancelled", f"the cancelled row moved after the fact: {still}"
        counts = await assert_balanced(conn, schema, _TAG)
        assert counts.get("cancelled", 0) == 1, counts
        violations = await conservation_violations(conn, schema, _TAG)
        assert not violations, "the cancel broke the ledger's conservation:\n" + "\n".join(
            violations[:20]
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                await client.aclose()
        if pool is not None:
            with contextlib.suppress(Exception):
                await pool.close()
        if worker is not None:
            reap(worker)
        await delete_tagged(conn, schema, _TAG)
