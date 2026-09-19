"""Unit pins for keyed rate-limit bucket-row reclamation.

The PG tier of this contract is
``tests/test_rt_keyed_bucket_reclamation.py``: an evicted keyed rate
limit's published ``rate_limit_buckets`` row must be reclaimed by the
pending-reclaim drain. This file pins the same contract one level down,
where no PG is needed - the registry bookkeeping that records an evicted
bucket for reclamation, the drain statement it issues, and the bounds
that keep that statement constant-size against any evicted-key backlog.

What each observable pins:

- **The drain statement** - one batched ``DELETE`` against
  ``"{schema}".rate_limit_buckets`` whose predicate is
  ``bucket_name = ANY($1)`` over the drain's bounded name slice: the
  same statement shape the reservation twin
  (``_RECLAIM_SLICE_DELETE_SQL_TEMPLATE`` in ``ratelimit/reservation.py``)
  uses for ``reservation_slots``, minus the lease guard - a bucket row
  has no holder, so nothing can survive the DELETE and there is no
  survivor probe to run.
- **The gate** - ``has_pending_reservation_reclaims`` is the sweep
  loop's drain gate, so it must cover rate-limit pendings too: a
  pending rate-limit row with no pending reservation must still run the
  drain, or the reclamation never happens in production.
- **Live buckets** - a name that re-registered since eviction is
  dropped from the pending set WITHOUT a statement (a re-activated key
  owns its row again), and a bucket materialized with no PG pool
  published no row, so its eviction records nothing and the drain
  acquires no connection at all.
- **The bound** - one drain call deletes at most ``batch_names``
  buckets' rows per schema, the front of the insertion-ordered pending
  set, so one tick's write set is constant-size against any backlog.
- **The cap** - eviction refuses to record past
  ``max_pending_reclaims``; the overflow entries stay registered and
  re-scanned on the next sweep (fail-closed, the reservation twin's
  veto).
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any, NamedTuple

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _TenantPayload(BaseModel):
    tenant_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_krl_reclaim_unit"


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
            "TASKQ_SCHEMA_NAME": _schema(),
        },
        validate=False,
    )


def _ref(base_name: str) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="memory",
    )


class _RecordedStatement(NamedTuple):
    sql: str
    args: tuple[object, ...]


class _RecordingConn:
    """Connection double that records every statement it is handed.

    ``fetch`` answers a ``DELETE ... RETURNING bucket_name`` with the
    sliced names echoed back as the deleted rows - the rowcount shape
    the drain counts; every other fetch returns no rows.
    """

    def __init__(self) -> None:
        self.statements: list[_RecordedStatement] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.statements.append(_RecordedStatement(sql, args))
        return "OK"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.statements.append(_RecordedStatement(sql, args))
        if "DELETE" in sql.upper() and args and isinstance(args[0], list):
            return [{"bucket_name": name} for name in args[0]]
        return []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.statements.append(_RecordedStatement(sql, args))
        return None

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        self.statements.append(_RecordedStatement(sql, args))
        return None

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False


class _RecordingPool:
    """Pool double handing every acquire the same recording connection."""

    def __init__(self) -> None:
        self.conn = _RecordingConn()

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
    ) -> AsyncGenerator[_RecordingConn, None]:
        yield self.conn


class _AcquireRaisingPool:
    """A pool whose ``acquire`` raises - proves a drain touches no connection."""

    def acquire(self, *, timeout: float | None = None) -> Any:
        raise asyncpg.PostgresConnectionError("no drain should reach the pool")


async def _materialize(
    reg: RateLimitRegistry,
    ref: KeyedRateLimitRef,
    pool: _RecordingPool | None,
    *,
    tenant_id: str,
) -> str:
    """Register a keyed bucket and publish its row WITHOUT acquiring."""
    return await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing a keyed bucket without acquiring, matching test_keyed_reservation_drain_rotation.py's pattern.
        ref,
        _TenantPayload(tenant_id=tenant_id),
        settings=_settings(),
        pg_pool=pool,  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool; only acquire()/execute() are reached.
    )


def _seed_idle(reg: RateLimitRegistry, *buckets: str) -> None:
    """Stamp every bucket's tracking entry far past the idle threshold."""
    for bucket in buckets:
        reg._keyed_rate_limit_last_used[bucket] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding entries idle for eviction, matching test_keyed_reservation_drain_rotation.py's pattern.


def _rate_limit_delete_statements(pool: _RecordingPool) -> list[_RecordedStatement]:
    return [
        s
        for s in pool.conn.statements
        if "DELETE" in s.sql.upper() and "rate_limit_buckets" in s.sql
    ]


async def test_evicted_published_keyed_rate_limit_bucket_row_is_drained() -> None:
    """Evicting an idle keyed rate limit must feed the pending-reclaim
    drain a ``rate_limit_buckets`` DELETE for its published row - the
    unit tier of the PG pin in test_rt_keyed_bucket_reclamation.py."""
    reg = RateLimitRegistry()
    pool = _RecordingPool()
    settings = _settings()

    acquired = await reg.acquire_for_actor(
        rate_limits=[_ref("krl-reclaim")],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_TenantPayload(tenant_id="acme"),
        pg_pool=pool,  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool; the memory-backed acquire never reads it, only the publish does.
        clock=FakeClock(_START),
        settings=settings,
    )
    assert len(acquired) == 1
    bucket = "krl-reclaim:acme"
    assert any(
        f'INSERT INTO "{_schema()}".rate_limit_buckets' in s.sql for s in pool.conn.statements
    ), "fixture broken: the keyed bucket's publish did not land"

    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0))
    assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"
    assert reg.has_pending_reservation_reclaims, (
        "an evicted keyed rate limit's published row must reach the pending-reclaim "
        "set - and has_pending_reservation_reclaims is the sweep loop's drain gate, "
        "so a pending rate-limit row that does not trip it is never drained in "
        "production"
    )

    deleted = await reg.drain_pending_reservation_reclaims(pool)  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool; acquire()/fetch() are the only members reached.

    assert deleted == 1, "the drain must report the reclaimed bucket row"
    deletes = _rate_limit_delete_statements(pool)
    assert len(deletes) == 1, "the drain must issue exactly one bucket-row DELETE"
    assert f'FROM "{_schema()}".rate_limit_buckets' in deletes[0].sql, (
        "the drain must delete the bucket row from the schema the publish wrote it to"
    )
    assert "bucket_name = ANY($1)" in deletes[0].sql, (
        "the drain's DELETE must scope by the bounded name slice (ANY($1)), "
        "the same statement shape as the reservation twin - an unscoped DELETE "
        "is the backlog-sized write the bounded-write guard exists for"
    )
    assert deletes[0].args == ([bucket],), "the DELETE's slice must name the evicted bucket"
    assert not reg.has_pending_reservation_reclaims, "the drained bucket must leave the pending set"


async def test_re_registered_keyed_bucket_is_dropped_without_a_statement() -> None:
    """A key that re-activates after eviction owns its row again: the
    drain drops the pending name without touching PG, and nothing
    pending means the next drain acquires no connection at all."""
    reg = RateLimitRegistry()
    pool = _RecordingPool()
    ref = _ref("krl-reregistered")

    bucket = await _materialize(reg, ref, pool, tenant_id="k1")
    _seed_idle(reg, bucket)
    assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 1
    assert reg.has_pending_reservation_reclaims

    # The key re-activates before any drain ran.
    await _materialize(reg, ref, pool, tenant_id="k1")
    pool.conn.statements.clear()

    assert await reg.drain_pending_reservation_reclaims(pool) == 0  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool.
    assert not _rate_limit_delete_statements(pool), (
        "a re-registered bucket's row must not be deleted - the drain drops "
        "the pending name without a statement"
    )
    assert not reg.has_pending_reservation_reclaims

    # Nothing pending on either side: the next drain must return before
    # acquiring a connection.
    assert await reg.drain_pending_reservation_reclaims(_AcquireRaisingPool()) == 0  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() is reached, and only if the bug is present.


async def test_unpublished_keyed_rate_limit_records_no_pending_reclaim() -> None:
    """A bucket materialized with no PG pool published no row, so its
    eviction records nothing and the drain never reaches a connection."""
    reg = RateLimitRegistry()
    ref = _ref("krl-nopublish")

    await _materialize(reg, ref, None, tenant_id="k1")
    assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 1
    assert not reg.has_pending_reservation_reclaims, (
        "evicting a bucket that never published a row must not record a "
        "pending reclaim - there is nothing to delete, and a phantom pending "
        "entry would burn drain ticks and pending-cap budget forever"
    )
    assert await reg.drain_pending_reservation_reclaims(_AcquireRaisingPool()) == 0  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() is reached, and only if the bug is present.


async def test_rate_limit_pending_cap_vetoes_eviction() -> None:
    """Eviction refuses to record past max_pending_reclaims: the set never
    exceeds the cap, and the overflow entries stay registered (re-scanned
    next sweep) - the fail-closed bound, the reservation twin's veto."""
    reg = RateLimitRegistry()
    pool = _RecordingPool()
    ref = _ref("krl-cap")

    for i in range(7):
        await _materialize(reg, ref, pool, tenant_id=f"k{i}")

    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0), max_pending_reclaims=5)

    assert evicted == 5, "the pending cap must veto the excess evictions"
    assert reg.has_pending_reservation_reclaims
    assert len(reg.rate_limits) == 2, "vetoed entries must stay registered for the next sweep"
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]  # Why: the veto is observable as tracked entries the eviction left behind.


async def test_drain_delete_slice_is_bounded_by_batch_names() -> None:
    """One drain call's bucket-row DELETE carries at most ``batch_names``
    names - the front of the insertion-ordered pending set - so one tick's
    write set is constant-size against any evicted-key backlog, and the
    next drain continues where the slice bound stopped the last."""
    reg = RateLimitRegistry()
    pool = _RecordingPool()
    ref = _ref("krl-slice")

    buckets = [await _materialize(reg, ref, pool, tenant_id=f"k{i}") for i in range(1, 4)]
    _seed_idle(reg, *buckets)
    assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 3

    await reg.drain_pending_reservation_reclaims(pool, batch_names=2)  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool.
    deletes = _rate_limit_delete_statements(pool)
    assert len(deletes) == 1
    assert deletes[0].args == (buckets[:2],), (
        "one drain must delete only the front slice of pending buckets, not "
        "the whole backlog - an unbounded DELETE is the backlog-ricochet "
        "defect the bounded-write guard exists for"
    )
    assert reg.has_pending_reservation_reclaims, "the unsliced tail must stay pending"

    pool.conn.statements.clear()
    await reg.drain_pending_reservation_reclaims(pool, batch_names=2)  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool.
    deletes = _rate_limit_delete_statements(pool)
    assert deletes[0].args == ([buckets[2]],), "the next drain must continue at the tail"
    assert not reg.has_pending_reservation_reclaims


async def test_sweep_loop_drains_pending_rate_limit_reclaims() -> None:
    """The per-worker sweep tick - the production driver - must evict an
    idle keyed rate limit, record its published row as pending, and drain
    it through the dispatcher pool on the same tick (the wiring twin of
    test_leader_sweep_reclaim_drain_wiring.py's reservation pin)."""
    from taskq.worker._leader_shared import SweepContext
    from taskq.worker.deps import WorkerDeps

    class _SimpleBackend:
        """Backend whose reclaim/deadline sweeps return 0 and lacks PG-only sweeps."""

        async def reclaim_expired_locks(self, now: datetime, cg: timedelta, ug: timedelta) -> int:
            return 0

        async def deadline_sweep(self, now: datetime) -> int:
            return 0

    reg = RateLimitRegistry()
    pool = _RecordingPool()
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
            "TASKQ_SCHEMA_NAME": _schema(),
            "TASKQ_SWEEP_INTERVAL": "0.05",
        },
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
        heartbeat_pool=_RecordingPool(),  # type: ignore[arg-type]
        worker_pool=_RecordingPool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    ctx = SweepContext(
        deps=deps,
        backend=_SimpleBackend(),  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=FakeClock(_START),
        worker_id=new_uuid(),
        rate_limit_registry=reg,
    )

    bucket = await _materialize(reg, _ref("krl-wiring"), pool, tenant_id="k1")
    _seed_idle(reg, bucket)

    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the sweep loop directly, matching test_leader_sweep_reclaim_drain_wiring.py's pattern.
    try:
        for _ in range(200):
            if _rate_limit_delete_statements(pool):
                break
            await asyncio.sleep(0.01)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    deletes = _rate_limit_delete_statements(pool)
    assert deletes, (
        "the sweep tick must drain an evicted keyed rate limit's published row "
        "through the dispatcher pool - the gate it consults "
        "(has_pending_reservation_reclaims) has to see rate-limit pendings"
    )
    assert deletes[0].args == ([bucket],)
    assert not reg.has_pending_reservation_reclaims
