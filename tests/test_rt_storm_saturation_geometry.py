# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team storm: pool-contention GEOMETRY at full saturation.

The question (distinct from the pinned per-site reds): with EVERY
dimension of the worker's connection budget loaded at once — both
consumer slots holding their slot-pool transaction connections while
their terminal writes wait on ``worker_pool``
(``max_size = int(max_concurrency * 1.5)``), the heartbeat loop ticking
on ``heartbeat_pool`` (4), and the leader sweep running on the
dispatcher/notify pool (4) — does the system deadlock-livelock, or does
each bounded piece time out and degrade?

Geometry (from ``worker/dispatch.py`` ``dispatch_one_job``,
``worker/deps.py``, ``settings.py``):

* The slot pool (``max_concurrency + 1``) and ``worker_pool``
  (``int(max_concurrency * 1.5)``) are DISJOINT pools, and no code path
  acquires a slot conn while holding a worker conn — the
  slot-holds-while-worker-waits shape is a one-way edge, so no circular
  wait exists between them. Heartbeat (4) and dispatcher (4) are
  similarly disjoint from both.
* The heartbeat loop's own acquire IS bounded
  (``heartbeat_pool.acquire(timeout=...)``, ``worker/heartbeat.py``),
  and its timeout is classified transient — saturation there degrades
  loudly (counted tick failure), not silently.
* ``mark_succeeded``'s worker-pool acquire has no timeout (the
  pinned-red class — ``test_rt_locks_terminal_write_pool_starvation``
  covers ``mark_cancelled``/heartbeat starvation and
  ``test_rt_locks_sweep_notify_pool_unbounded`` covers the sweep
  notify-pool acquire; NOT duplicated here). This file's angle: at
  full worker_pool saturation the queued terminal write merely WAITS
  (degrade), resolves the moment one conn returns (no deadlock), and
  the affected row's designed recovery is lock-lease expiry + sweep 1.

Pinned below at small-but-real scale (``max_concurrency=2``, every pool
at its true default size, real asyncpg pools against real Postgres):
(a) the full concurrent geometry completes within a bounded window —
no livelock; (b) worker_pool saturation degrades-and-resolves, not
deadlocks; (c) heartbeat_pool saturation fails FAST and loudly inside
its bounded acquire (the "something reports" half).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import JobId
from taskq.backend._sql import UPDATE_JOBS_LOCK_SQL_TEMPLATE
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.settings import make_integration_settings

pytestmark = pytest.mark.integration

_ACTOR = "storm_geom_actor"
_QUEUE = "default"
_MAX_CONCURRENCY = 2
_WORKER_POOL_SIZE = 3  # int(2 * 1.5)
_SLOT_POOL_SIZE = 3  # max_concurrency + 1
_HEARTBEAT_POOL_SIZE = 4  # settings default
_DISPATCHER_POOL_SIZE = 4  # settings default
_LEASE = timedelta(seconds=60)

# Every wait in this module is bounded so a red can never hang: the
# full-geometry window, the saturation observation window, and the
# resolve window are all asyncio.wait_for deadlines.
_GEOMETRY_BOUND_S = 10.0
_OBSERVE_S = 1.0
_RESOLVE_BOUND_S = 5.0


class _PoolsDeps:
    """Duck-typed ``BackendDeps`` carrying the real per-role pools."""

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        worker_pool: asyncpg.Pool,
        heartbeat_pool: asyncpg.Pool,
        dispatcher_pool: asyncpg.Pool,
    ) -> None:
        self.settings = settings
        self.worker_pool = worker_pool
        self.heartbeat_pool = heartbeat_pool
        self.dispatcher_pool = dispatcher_pool


class _Holder:
    """Saturates a pool by holding acquired connections on an event.

    ``hold()`` returns once every connection is actually acquired (the
    holder tasks keep holding until ``release``), so the caller can
    start the saturated operation immediately.
    """

    def __init__(self, pool: asyncpg.Pool, n: int) -> None:
        self._pool = pool
        self._n = n
        self._release = asyncio.Event()
        self._acquired: list[asyncio.Event] = [asyncio.Event() for _ in range(n)]
        self._tasks: list[asyncio.Task[None]] = []

    async def hold(self) -> None:
        self._release.clear()

        async def _one(acquired: asyncio.Event) -> None:
            async with self._pool.acquire():
                acquired.set()
                await self._release.wait()

        self._tasks = [asyncio.create_task(_one(acquired)) for acquired in self._acquired]
        # Bounded: if the pool cannot hand out n conns the holders fail
        # here instead of wedging the test.
        await asyncio.wait_for(
            asyncio.gather(*(evt.wait() for evt in self._acquired)),
            timeout=_GEOMETRY_BOUND_S,
        )

    def release_one(self) -> None:
        self._release.set()

    async def release_all(self) -> None:
        self._release.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)


async def _seed_running(
    admin: asyncpg.Connection, schema: str, job_id: UUID, worker_id: UUID, *, lease_seconds: float
) -> None:
    await admin.execute(
        f'INSERT INTO "{schema}".jobs ('
        "    id, actor, queue, payload, max_attempts, retry_kind,"
        "    status, priority, attempt, scheduled_at,"
        "    locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,"
        "    cancel_phase, cancel_requested_at"
        ") VALUES ("
        "    $1, $2::text, $3::text, '{}'::jsonb,"
        "    3::smallint, 'transient',"
        "    'running', 0, 1::smallint, clock_timestamp(),"
        "    $4::uuid,"
        "    clock_timestamp() + ($5::double precision * interval '1 second'),"
        "    clock_timestamp() - interval '30 seconds',"
        "    clock_timestamp() - interval '30 seconds',"
        "    0::smallint, NULL)",
        job_id,
        _ACTOR,
        _QUEUE,
        worker_id,
        lease_seconds,
    )


@pytest.mark.load_sensitive
async def test_full_fleet_geometry_saturates_without_livelock(pg_dsn: str) -> None:
    """max_concurrency=2, every pool at its true default size, all four
    dimensions loaded concurrently: both slots hold slot conns across
    their worker-pool terminal writes, the heartbeat tick runs, and the
    leader sweep reclaims an expired-lock victim — everything completes
    inside the bounded window. Then each saturation half: worker_pool
    fully held degrades-and-resolves (no deadlock); heartbeat_pool fully
    held fails fast inside its bounded acquire (loud, classified).

    Load-sensitive, deliberately: the test saturates one containerized
    Postgres from every pool dimension with two-second heartbeat command
    budgets, which is the livelock property it exists to pin. Under
    parallel-lane neighbors a busy shared PG trips the budget on a
    healthy run, so the pin belongs on the quiet serial lane where a
    timeout means a real hang and nothing else.
    """
    schema = f"tst_{new_base62()}".lower()
    settings = make_integration_settings(
        pg_dsn, schema_name=schema, max_concurrency=str(_MAX_CONCURRENCY)
    )
    admin = await asyncpg.connect(pg_dsn)
    pools: list[asyncpg.Pool] = []
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
        await admin.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _ACTOR,
            _QUEUE,
        )
        worker_id = new_uuid()
        dead_worker = new_uuid()  # the crash victim's holder: no live heartbeat renews its lease
        for wid in (worker_id, dead_worker):
            await admin.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
                "VALUES ($1, 'storm-host', 12345, ARRAY['default'])",
                wid,
            )
        # Two mid-flight slot jobs (live leases) + one more for the
        # saturation phase, all held by the LIVE worker; one expired-lock
        # sweep victim held by the dead worker (the mass-crash shape —
        # the live worker's heartbeat tick must not re-lease it).
        job_a, job_b, job_c, victim = (new_job_id() for _ in range(4))
        for job_id in (job_a, job_b, job_c):
            await _seed_running(admin, schema, job_id, worker_id, lease_seconds=60.0)
        await _seed_running(admin, schema, victim, dead_worker, lease_seconds=-1.0)

        worker_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=_WORKER_POOL_SIZE)
        heartbeat_pool = await asyncpg.create_pool(
            pg_dsn,
            min_size=1,
            max_size=_HEARTBEAT_POOL_SIZE,
            command_timeout=settings.heartbeat_command_timeout,
        )
        dispatcher_pool = await asyncpg.create_pool(
            pg_dsn,
            min_size=1,
            max_size=_DISPATCHER_POOL_SIZE,
            command_timeout=settings.dispatcher_command_timeout,
        )
        slot_pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=_SLOT_POOL_SIZE)
        pools.extend([worker_pool, heartbeat_pool, dispatcher_pool, slot_pool])
        backend = PostgresBackend(
            _PoolsDeps(
                settings,
                worker_pool=worker_pool,
                heartbeat_pool=heartbeat_pool,
                dispatcher_pool=dispatcher_pool,
            ),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps — settings plus the real per-role pools, the full surface the swept/terminal/heartbeat paths touch.
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )

        # ── Phase 1: the full geometry, concurrently, no over-saturation.
        async def slot_job(job_id: JobId) -> bool:
            # The dispatch_one_job shape: the slot's transaction conn is
            # held for the whole dispatch, across the terminal write's
            # worker-pool acquire.
            async with slot_pool.acquire() as _slot_conn:
                # attempt=1: the rows are seeded at their first epoch and
                # the landed attempt-epoch fence no-ops an epoch-less write
                # — the sibling PG test presents the row's epoch the same
                # way, mirroring the production caller's attempt=job.attempt.
                return await backend.mark_succeeded(job_id, worker_id, {"ok": True}, attempt=1)

        async def heartbeat_tick() -> None:
            # The heartbeat.py loop shape: bounded acquire, then the
            # lease-extension UPDATE on the acquired conn.
            async with heartbeat_pool.acquire(timeout=settings.heartbeat_command_timeout) as conn:
                await conn.execute(
                    UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=schema),
                    worker_id,
                    _LEASE,
                    [],
                )

        async def leader_sweep() -> int:
            return await backend.reclaim_expired_locks(timedelta(0), timedelta(0))

        results = await asyncio.wait_for(
            asyncio.gather(
                slot_job(job_a),
                slot_job(job_b),
                heartbeat_tick(),
                leader_sweep(),
            ),
            timeout=_GEOMETRY_BOUND_S,
        )
        assert results[0] is True and results[1] is True, (
            "both slot-held terminal writes must land — at the true default "
            "geometry (slots 3 / worker_pool 3 / heartbeat 4 / dispatcher 4) "
            "two concurrent slot jobs + heartbeat + sweep must not livelock; "
            f"a timeout after {_GEOMETRY_BOUND_S}s would be the livelock red"
        )
        assert results[3] == 1, (
            f"the concurrent leader sweep must reclaim the one expired-lock "
            f"victim (got {results[3]!r}) — a starved sweep is the "
            "maintenance-stops-under-load red"
        )
        assert await backend.reclaim_expired_locks(timedelta(0), timedelta(0)) == 0, (
            "a second sweep call must find nothing left — the first "
            "concurrent call drained the eligible set"
        )
        victim_status = await admin.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1', victim
        )
        assert victim_status == "pending", (
            f"the expired-lock victim must be re-pended by the concurrent "
            f"sweep (got {victim_status!r})"
        )

        # ── Phase 2: worker_pool fully saturated → degrade, not deadlock.
        # (The no-timeout acquire itself is the pinned-red class named in
        # the module docstring; pinned here is only the geometry claim.)
        saturator = _Holder(worker_pool, _WORKER_POOL_SIZE)
        await saturator.hold()
        queued_write = asyncio.create_task(
            backend.mark_succeeded(job_c, worker_id, {"ok": 1}, attempt=1)
        )
        await asyncio.sleep(_OBSERVE_S)
        assert not queued_write.done(), (
            "with every worker_pool conn held, the queued terminal write "
            "waits (degrade) rather than erroring — the row stays running "
            "and lock-lease expiry is its designed recovery"
        )
        saturator.release_one()
        landed = await asyncio.wait_for(queued_write, timeout=_RESOLVE_BOUND_S)
        assert landed is True, "releasing ONE connection must resolve the queued write"
        job_c_status = await admin.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_c
        )
        assert job_c_status == "succeeded", (
            "the degraded write resolved with the real terminal state — a "
            "wait that never resolves would be the deadlock-livelock red"
        )
        await saturator.release_all()

        # ── Phase 3: heartbeat_pool fully saturated → bounded, loud fail.
        hb_saturator = _Holder(heartbeat_pool, _HEARTBEAT_POOL_SIZE)
        await hb_saturator.hold()
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(TimeoutError):
            async with heartbeat_pool.acquire(timeout=settings.heartbeat_command_timeout):
                pass
        elapsed = loop.time() - t0
        assert elapsed < _RESOLVE_BOUND_S, (
            f"the heartbeat loop's bounded acquire must fail fast under "
            f"saturation (took {elapsed:.2f}s) — the loud, classified-"
            "transient degrade is the geometry's saturation report; an "
            "unbounded wait here would starve the lease and cascade the "
            "whole worker into isolate_self"
        )
        await hb_saturator.release_all()

        # Recovery: with the pools released, the same concurrent shape
        # completes again.
        await asyncio.wait_for(heartbeat_tick(), timeout=_RESOLVE_BOUND_S)
    finally:
        for pool in pools:
            await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()
