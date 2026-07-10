"""Maintenance leader: election, watchdog, and recovery sweeps.
A single elected leader per cluster runs cooperative loops inside one
asyncio.TaskGroup: election, watchdog, scheduled-wake (sweep 3), cron,
sweep (sweeps 1/2/4), prune (sweep 5), archive expiry (sweep 6), stale
worker cleanup, queue depth, and reservation slots.  Non-leader pods retry
election periodically and skip the gated work.
Failover SLA:
  Worker killed      ≤ heartbeat_interval + 1 s
  Partition detect   ≤ watchdog_interval + heartbeat_interval + 2 s
  PG failover        ≤ heartbeat_interval
  Watchdog detect    ≤ watchdog_interval + heartbeat_interval
"""

import asyncio
import contextlib
import time
from collections.abc import Iterable
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq.backend._protocol import Backend
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    wake_channel,
)
from taskq.obs import get_logger, get_meter, record_election_attempt
from taskq.worker._leader_shared import (
    _EK1,
    ARCHIVE_EXPIRY_LOCK_NAME,
    PRUNE_LOCK_NAME,
    ArchiveExpiryResult,
    PruneResult,
    SweepContext,
    _build_retention_per_status,
    _dbg,
    _err,
    _load_actor_retention_overrides,
    _metric,
    _schedule_utc_to_cron,
    archive_expiry_sweep,
    cleanup_stale_workers,
    prune_terminal_jobs,
)
from taskq.worker._leader_sweeps import (
    _archive_expiry_loop,
    _prune_loop,
    _queue_depth_loop,
    _reservation_slots_loop,
    _stranded_jobs_loop,
    _sweep_loop,
)
from taskq.worker.cron_loop import tick_cron
from taskq.worker.deps import (
    WorkerDeps,
    open_dedicated_conn,
)

__all__ = [
    "ARCHIVE_EXPIRY_LOCK_NAME",
    "MAINTENANCE_LEADER_LOCK_NAME",
    "PRUNE_LOCK_NAME",
    "ArchiveExpiryResult",
    "MaintenanceLeader",
    "PruneResult",
    "_build_retention_per_status",
    "_load_actor_retention_overrides",
    "_schedule_utc_to_cron",
    "archive_expiry_sweep",
    "cleanup_stale_workers",
    "prune_terminal_jobs",
]

log: structlog.stdlib.BoundLogger = get_logger(__name__)
MAINTENANCE_LEADER_LOCK_NAME: str = "taskq:maintenance_leader"
_WATCHDOG_INTERVAL_SECS: float = 5.0
_meter = get_meter()


def _observe_is_leader(options: CallbackOptions) -> Iterable[Observation]:
    for leader in _active_leaders:
        yield Observation(
            1 if leader._deps.is_leader.is_set() else 0,  # pyright: ignore[reportPrivateUsage]  # Why: OTel gauge callback reads the authoritative is_leader state from WorkerDeps; the callback is at module scope to close over the gauge registry.
            {"worker_id": str(leader._worker_id)},  # pyright: ignore[reportPrivateUsage]  # Why: gauge callback needs worker_id for the observation label; the field is private by convention but accessible from module scope by design.
        )


_is_leader_gauge = _meter.create_observable_gauge(
    name="taskq.maintenance_leader.is_leader",
    description="1 on the elected leader pod, 0 elsewhere.",
    callbacks=[_observe_is_leader],
)


class MaintenanceLeader:
    """Elected leader that runs watchdog, sweeps, cron, and prune loops."""

    def __init__(
        self, deps: WorkerDeps, worker_id: UUID, backend: Backend, *, clock: Clock
    ) -> None:
        self._deps = deps
        self._worker_id = worker_id
        self._backend = backend
        self._clock = clock
        self._sweep_ctx = SweepContext(deps=deps, backend=backend, clock=clock, worker_id=worker_id)
        self._leader_monitor_conn: asyncpg.Connection | None = None
        self._cron_conn: asyncpg.Connection | None = None

    async def _close_leader_owned_conns(self) -> None:
        for attr in ("_cron_conn", "_leader_monitor_conn"):
            conn = getattr(self, attr)
            if conn is not None and not conn.is_closed():
                with contextlib.suppress(asyncpg.PostgresConnectionError, OSError):
                    await conn.close()
            setattr(self, attr, None)
        self._deps.is_leader.clear()

    async def run(self, shutdown: asyncio.Event) -> None:
        _active_leaders.add(self)
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._election_loop(shutdown), name="leader.election")
                tg.create_task(self._watchdog_loop(shutdown), name="leader.watchdog")
                tg.create_task(self._scheduled_wake_loop(shutdown), name="leader.scheduled_wake")
                tg.create_task(self._cron_loop(shutdown), name="leader.cron")
                tg.create_task(self._sweep_loop(shutdown), name="leader.sweep")
                tg.create_task(self._prune_loop(shutdown), name="leader.prune")
                tg.create_task(self._archive_expiry_loop(shutdown), name="leader.archive_expiry")
                tg.create_task(self._queue_depth_loop(shutdown), name="leader.queue_depth")
                tg.create_task(
                    self._reservation_slots_loop(shutdown), name="leader.reservation_slots"
                )
                tg.create_task(self._stranded_jobs_loop(shutdown), name="leader.stranded_jobs")
                await shutdown.wait()
        finally:
            await self._close_leader_owned_conns()
            _active_leaders.discard(self)

    async def _election_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            if self._deps.is_leader.is_set():
                if self._deps.leader_conn is None or self._deps.leader_conn.is_closed():
                    conn_state = "None" if self._deps.leader_conn is None else "closed"
                    log.warning(
                        "leader-conn-died",
                        kind="leader_conn_died",
                        worker_id=str(self._worker_id),
                        error=f"leader_conn is {conn_state} while is_leader is set",
                    )
                    self._deps.leader_conn = None
                    await self._close_leader_owned_conns()
                else:
                    try:
                        await self._deps.leader_conn.execute("SELECT 1")
                        await asyncio.sleep(self._deps.settings.heartbeat_interval)
                        continue
                    except (
                        asyncpg.PostgresConnectionError,
                        asyncpg.InterfaceError,
                        OSError,
                    ) as exc:
                        if not self._deps.leader_conn.is_closed():
                            await self._deps.leader_conn.close()
                        self._deps.leader_conn = None
                        await self._close_leader_owned_conns()
                        log.warning(
                            "leader-conn-died",
                            kind="leader_conn_died",
                            worker_id=str(self._worker_id),
                            error=repr(exc),
                        )
            if self._deps.leader_conn is None or self._deps.leader_conn.is_closed():
                try:
                    self._deps.leader_conn = await open_dedicated_conn(
                        str(self._deps.settings.pg_dsn_direct),
                        label="leader_conn",
                        apply_keepalive=True,
                    )  # Why: only post-construction mutation of WorkerDeps allowed in M1; leader_conn may be None after watchdog or error handler closes it — a fresh connection is required before the lock attempt.
                except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as exc:
                    self._deps.leader_conn = None
                    log.warning(
                        "leader-conn-open-failed",
                        kind="leader_conn_open_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
            try:
                got_lock = await self._deps.leader_conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                    MAINTENANCE_LEADER_LOCK_NAME,
                )
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as exc:
                if not self._deps.leader_conn.is_closed():
                    await self._deps.leader_conn.close()
                self._deps.leader_conn = None
                await self._close_leader_owned_conns()
                record_election_attempt(str(self._worker_id), won=False)
                log.warning(
                    "election-lock-attempt-failed",
                    kind="election_lock_attempt_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            if got_lock:
                schema_name = self._deps.settings.schema_name
                if not _IDENT_RE.match(schema_name):
                    raise ValueError(f"invalid schema identifier: {schema_name!r}")
                upsert_sql = (
                    f'INSERT INTO "{schema_name}".maintenance_leader (singleton, worker_id, elected_at, last_seen_at) '  # noqa: S608  # Why: schema_name validated against _IDENT_RE before interpolation; asyncpg cannot bind identifiers as parameters.
                    "VALUES (true, $1, now(), now()) "
                    "ON CONFLICT (singleton) DO UPDATE SET "
                    "worker_id = EXCLUDED.worker_id, "
                    "elected_at = EXCLUDED.elected_at, "
                    "last_seen_at = EXCLUDED.last_seen_at"
                )
                try:
                    await self._deps.leader_conn.execute(upsert_sql, self._worker_id)
                except asyncpg.ForeignKeyViolationError as exc:
                    log.error(
                        "leader-upsert-fk-violation",
                        kind="leader_upsert_fk_violation",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    shutdown.set()
                    return
                try:
                    self._leader_monitor_conn = await open_dedicated_conn(
                        str(self._deps.settings.pg_dsn_direct),
                        label="leader_monitor_conn",
                        apply_keepalive=True,
                    )
                    self._cron_conn = await open_dedicated_conn(
                        str(self._deps.settings.pg_dsn_direct),
                        label="cron_conn",
                        apply_keepalive=True,
                    )
                except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as exc:
                    if not self._deps.leader_conn.is_closed():
                        await self._deps.leader_conn.close()
                    self._deps.leader_conn = None
                    await self._close_leader_owned_conns()
                    log.warning(
                        "leader-dedicated-conn-failed",
                        kind="leader_dedicated_conn_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
                self._deps.is_leader.set()
                record_election_attempt(str(self._worker_id), won=True)
                log.info(
                    "leader-elected",
                    kind="leader_elected",
                    worker_id=str(self._worker_id),
                )
            else:
                record_election_attempt(str(self._worker_id), won=False)
                log.info(
                    "leader-retry",
                    kind="leader_retry",
                    worker_id=str(self._worker_id),
                    next_retry_secs=self._deps.settings.heartbeat_interval,
                )
            await asyncio.sleep(self._deps.settings.heartbeat_interval)

    async def _watchdog_loop(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            await self._deps.is_leader.wait()
            while not shutdown.is_set() and self._deps.is_leader.is_set():
                conn = self._leader_monitor_conn
                if conn is None:
                    break
                try:
                    await conn.fetchval("SELECT 1")
                except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as exc:
                    if (
                        self._deps.leader_conn is not None
                        and not self._deps.leader_conn.is_closed()
                    ):
                        await self._deps.leader_conn.close()
                    self._deps.leader_conn = None
                    await self._close_leader_owned_conns()
                    log.warning(
                        "leadership-lost",
                        kind="leadership_lost",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    break
                await asyncio.sleep(_WATCHDOG_INTERVAL_SECS)

    async def _scheduled_wake_loop(self, shutdown: asyncio.Event) -> None:
        warned = False
        while not shutdown.is_set():
            if self._deps.is_leader.is_set():
                now_utc = self._clock.now()
                start = time.monotonic()
                try:
                    count = await self._backend.scheduled_to_pending(now=now_utc)
                except NotImplementedError as exc:
                    if not warned:
                        _err("scheduled_wake_backend_unimplemented", _EK1, self._worker_id, exc)
                        warned = True
                else:
                    _metric("scheduled_to_pending", count, start)
                    _dbg("scheduled_wake_tick", "scheduled_wake_tick", count, start)
                    if count > 0:
                        channel = wake_channel(self._deps.settings.schema_name)
                        async with self._deps.dispatcher_pool.acquire() as conn:
                            await conn.execute("SELECT pg_notify($1, '')", channel)
            await asyncio.sleep(1.0)

    async def _cron_loop(self, shutdown: asyncio.Event) -> None:
        """Tick cron schedules every second when this worker is the leader.

        Separate asyncio.Task from _leader_sweep_loop.
        Each tick opens a transaction on ``_cron_conn`` (a dedicated
        connection owned exclusively by this loop) and delegates to
        :func:`~taskq.worker.cron_loop.tick_cron`.
        ``CancelledError`` (a ``BaseException``) is not caught by
        ``except Exception`` and propagates to the ``TaskGroup`` for
        clean shutdown.
        """
        while not shutdown.is_set():
            if not self._deps.is_leader.is_set():
                await asyncio.sleep(1)
                continue
            conn = self._cron_conn
            if conn is None:
                await asyncio.sleep(1)
                continue
            try:
                async with conn.transaction():
                    await tick_cron(
                        conn,
                        self._deps.settings,
                        self._backend,
                        self._deps.settings.schema_name,
                        self._worker_id,
                    )
            except Exception as exc:
                if isinstance(
                    exc, (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError)
                ):
                    await self._close_leader_owned_conns()
                    log.warning(
                        "cron-conn-lost",
                        kind="cron_conn_lost",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    continue
                log.error("cron-tick-failed", kind="cron_fire", error=str(exc))
            await asyncio.sleep(1)

    async def _sweep_loop(self, shutdown: asyncio.Event) -> None:
        await _sweep_loop(self._sweep_ctx, shutdown)

    async def _prune_loop(self, shutdown: asyncio.Event) -> None:
        await _prune_loop(self._sweep_ctx, shutdown)

    async def _archive_expiry_loop(self, shutdown: asyncio.Event) -> None:
        await _archive_expiry_loop(self._sweep_ctx, shutdown)

    async def _queue_depth_loop(self, shutdown: asyncio.Event) -> None:
        await _queue_depth_loop(self._sweep_ctx, shutdown)

    async def _reservation_slots_loop(self, shutdown: asyncio.Event) -> None:
        await _reservation_slots_loop(self._sweep_ctx, shutdown)

    async def _stranded_jobs_loop(self, shutdown: asyncio.Event) -> None:
        await _stranded_jobs_loop(self._sweep_ctx, shutdown)


_active_leaders: set[MaintenanceLeader] = set()
