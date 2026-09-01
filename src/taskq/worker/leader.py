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
import threading
import time
from collections.abc import Iterable
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq.backend._protocol import Backend
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    wake_channel,
)
from taskq.obs import get_logger, get_meter, record_election_attempt
from taskq.ratelimit.registry import RateLimitRegistry
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
    complete_stale_batches,
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
from taskq.worker._transient import TRANSIENT_PG_ERRORS, UnexpectedLoopErrorGuard
from taskq.worker.cron_loop import tick_cron
from taskq.worker.deps import (
    WorkerDeps,
    apply_keepalive_to_conn,
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
    "complete_stale_batches",
    "prune_terminal_jobs",
]

log: structlog.stdlib.BoundLogger = get_logger(__name__)
MAINTENANCE_LEADER_LOCK_NAME: str = "taskq:maintenance_leader"
_WATCHDOG_INTERVAL_SECS: float = 5.0
_meter = get_meter()

# Guards _active_leaders against concurrent access: the OTel SDK reader
# thread invokes _observe_is_leader while the event-loop thread mutates the
# set via run() add/discard. Unsynchronized iteration raises RuntimeError:
# Set changed size during iteration. Same failure class as the _tick_age_cache
# race fixed in _watchdog.py.
_active_leaders_lock = threading.Lock()


def _observe_is_leader(options: CallbackOptions) -> Iterable[Observation]:
    with _active_leaders_lock:
        snapshot = list(_active_leaders)
    for leader in snapshot:
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
        self,
        deps: WorkerDeps,
        worker_id: UUID,
        backend: Backend,
        *,
        clock: Clock,
        rate_limit_registry: RateLimitRegistry | None = None,
    ) -> None:
        self._deps = deps
        self._worker_id = worker_id
        self._backend = backend
        self._clock = clock
        self._sweep_ctx = SweepContext(
            deps=deps,
            backend=backend,
            clock=clock,
            worker_id=worker_id,
            rate_limit_registry=rate_limit_registry,
        )
        self._leader_monitor_conn: asyncpg.Connection | None = None
        self._cron_conn: asyncpg.Connection | None = None

    async def _close_leader_owned_conns(self, *, mid_run: bool = True) -> None:
        """Close the leader-owned dedicated conns (cron, monitor), bounded.

        Two call contexts: mid-run demotion (watchdog/election/cron
        conn-died paths - the default ``mid_run=True``, the ``conn-close-*``
        alert family) and ``run()``'s finally (final teardown - passes
        ``mid_run=False`` for the ``conn-teardown-close-*`` family), so an
        ordinary shutdown never pages as an unexpected mid-run close
        timeout.
        """
        # Why first: demotion must be observable immediately - the bounded
        # closes below can park for seconds on a dead PG, and this flag
        # backs the leader gauge, /metrics, and the health report.
        self._deps.is_leader.clear()
        for attr in ("_cron_conn", "_leader_monitor_conn"):
            conn = getattr(self, attr)
            if conn is not None and not conn.is_closed():
                # Why bounded: a dead PG can block conn.close() indefinitely,
                # which stalled the election/watchdog/cron paths that call
                # this (#38). The helper never raises - a superset of the
                # previous suppress(PostgresConnectionError, OSError) - and
                # terminates the conn on timeout. Labels match the keepalive
                # labels ("cron_conn" / "leader_monitor_conn").
                await close_conn_bounded(
                    conn,
                    attr.removeprefix("_"),
                    CLOSE_TIMEOUT_SECS,
                    mid_run=mid_run,
                )
            # Identity guard: the await above suspends, and the election loop
            # can run a full cycle during that suspension — creating fresh
            # conns and re-setting is_leader. Unconditionally nulling would
            # orphan the fresh conn, leaving is_leader set with no cron/monitor
            # conn (a CPU busy-spin until the next leader_conn death). Only
            # null if the attribute still points to the SAME conn we closed.
            if getattr(self, attr) is conn:
                setattr(self, attr, None)

    async def _drop_leader_conn(self, *, reason: str) -> None:
        """Null ``deps.leader_conn``, closing it only when TaskQ-owned.

        The ownership contract ("TaskQ never closes caller-owned
        resources") forbids closing a caller-provided leader_conn even when
        it is dead - the caller owns the corpse. A caller-owned conn is
        therefore abandoned: our reference is dropped so the election loop
        rebuilds via ``leader_conn_factory`` / ``pg_dsn_direct``, and the
        caller's own handle is left for them to dispose of.
        """
        conn = self._deps.leader_conn
        if conn is None:
            return
        if self._deps.owns_leader_conn:
            if not conn.is_closed():
                # Why bounded: same dead-PG stall risk on the watchdog/
                # election drop path (#38). The helper never raises, so
                # leader_conn is always nulled below and the loop can
                # rebuild - previously a close error propagated out of the
                # drop path and skipped the nulling.
                await close_conn_bounded(conn, "leader", CLOSE_TIMEOUT_SECS, mid_run=True)
        else:
            log.warning(
                "leader-conn-abandoned-caller-owned",
                kind="leader_conn_abandoned_caller_owned",
                worker_id=str(self._worker_id),
                reason=reason,
            )
        self._deps.leader_conn = None

    async def _open_leader_conn(self) -> asyncpg.Connection:
        """Open or reopen the leader advisory-lock connection.

        Uses ``deps.leader_conn_factory`` when set (credential-provider-
        backed deployments - AAD/AWS/Vault), so reconnection after a drop
        re-fetches a fresh credential rather than falling back to a
        stale/absent DSN. Falls back to ``open_dedicated_conn`` with the
        DSN only when no factory is available.
        """
        factory = self._deps.leader_conn_factory
        if factory is not None:
            conn = await factory()
            # Why: the factory path bypasses open_dedicated_conn, so the
            # worker's keepalive policy must be applied here - the factory
            # owns the credential, TaskQ owns the socket policy.
            apply_keepalive_to_conn(conn, label="leader")
            return conn
        dsn = self._deps.settings.pg_dsn_direct
        if dsn is None:
            # Why: open_worker_deps validates this at startup, so None here
            # means deps were built by hand - fail fast instead of letting
            # asyncpg.connect(str(None)) DNS-retry the host "None" forever.
            raise RuntimeError(
                "no leader_conn_factory and pg_dsn_direct is None - "
                "cannot rebuild leader connection"
            )
        return await open_dedicated_conn(
            str(dsn),
            label="leader",
            apply_keepalive=True,
            command_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def _open_dedicated_conn(self, label: str) -> asyncpg.Connection:
        """Open a leader-owned dedicated connection (monitor / cron).

        Uses ``deps.leader_conn_factory`` when set so the same credential
        source is used for all leader connections. Falls back to
        ``open_dedicated_conn`` with the DSN otherwise.
        """
        factory = self._deps.leader_conn_factory
        if factory is not None:
            conn = await factory()
            apply_keepalive_to_conn(conn, label=label)
            return conn
        dsn = self._deps.settings.pg_dsn_direct
        if dsn is None:
            # Why: same fail-fast as _open_leader_conn - never connect to
            # the literal host "None".
            raise RuntimeError(
                f"no leader_conn_factory and pg_dsn_direct is None - cannot rebuild {label}"
            )
        return await open_dedicated_conn(
            str(dsn),
            label=label,
            apply_keepalive=True,
            command_timeout=self._deps.settings.dispatcher_command_timeout,
        )

    async def run(self, shutdown: asyncio.Event) -> None:
        with _active_leaders_lock:
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
            # Final teardown, not a mid-run demotion: close with the
            # conn-teardown-close-* family so an ordinary shutdown never
            # pages as an unexpected mid-run close timeout.
            await self._close_leader_owned_conns(mid_run=False)
            with _active_leaders_lock:
                _active_leaders.discard(self)

    async def _election_loop(self, shutdown: asyncio.Event) -> None:
        guard = UnexpectedLoopErrorGuard("leader.election")
        while not shutdown.is_set():
            self._deps.liveness.tick(
                "leader.election", period=self._deps.settings.heartbeat_interval
            )
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
                        guard.ok()
                        await asyncio.sleep(self._deps.settings.heartbeat_interval)
                        continue
                    except TRANSIENT_PG_ERRORS as exc:
                        await self._drop_leader_conn(reason="probe_failed")
                        await self._close_leader_owned_conns()
                        log.warning(
                            "leader-conn-died",
                            kind="leader_conn_died",
                            worker_id=str(self._worker_id),
                            error=repr(exc),
                        )
                    except Exception as exc:
                        # Backstop (see _transient.py): tolerated + logged a
                        # few times, then deliberately fatal; cleanup mirrors
                        # the transient path since conn state is unknown.
                        # Cleanup runs BEFORE guard.unexpected so the fatal
                        # iteration still drops the conn and clears
                        # is_leader — otherwise the dead leader gauge and
                        # leader_conn reference stay stale until run()'s
                        # finally.
                        await self._drop_leader_conn(reason="probe_failed")
                        await self._close_leader_owned_conns()
                        log.warning(
                            "leader-conn-died",
                            kind="leader_conn_died",
                            worker_id=str(self._worker_id),
                            error=repr(exc),
                        )
                        guard.unexpected(exc)
                        continue
            if self._deps.leader_conn is None or self._deps.leader_conn.is_closed():
                try:
                    self._deps.leader_conn = await self._open_leader_conn()
                except Exception as exc:
                    # Why: ``except Exception`` is deliberate at this retry
                    # point - credential-provider factories raise
                    # azure/hvac/botocore exceptions, and a rejected fresh
                    # token raises asyncpg.InvalidPasswordError (an
                    # InvalidAuthorizationSpecificationError, NOT a
                    # PostgresConnectionError). All are transient at this
                    # boundary and must retry, not crash the worker
                    # TaskGroup. CancelledError is BaseException (3.8+), so
                    # shutdown still propagates.
                    self._deps.leader_conn = None
                    log.warning(
                        "leader-conn-open-failed",
                        kind="leader_conn_open_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                        error_type=type(exc).__name__,
                    )
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
            try:
                got_lock = await self._deps.leader_conn.fetchval(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                    MAINTENANCE_LEADER_LOCK_NAME,
                )
            except TRANSIENT_PG_ERRORS as exc:
                await self._drop_leader_conn(reason="lock_attempt_failed")
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
            except Exception as exc:
                # Backstop (see _transient.py): tolerated + logged a few
                # times, then deliberately fatal; cleanup mirrors the
                # transient path since conn state is unknown.
                await self._drop_leader_conn(reason="lock_attempt_failed")
                await self._close_leader_owned_conns()
                record_election_attempt(str(self._worker_id), won=False)
                log.warning(
                    "election-lock-attempt-failed",
                    kind="election_lock_attempt_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                guard.unexpected(exc)
                await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            if got_lock:
                schema_name = self._deps.settings.schema_name
                if not _IDENT_RE.match(schema_name):
                    raise ValueError(f"invalid schema identifier: {schema_name!r}")
                upsert_sql = (
                    f'INSERT INTO "{schema_name}".maintenance_leader (singleton, worker_id, elected_at, last_seen_at) '  # noqa: S608  # Why: schema_name validated against _IDENT_RE before interpolation; asyncpg cannot bind identifiers as parameters.
                    "VALUES (true, $1, clock_timestamp(), clock_timestamp()) "
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
                except TRANSIENT_PG_ERRORS as exc:
                    # The advisory lock was won but the conn died before the
                    # UPSERT landed: transient, and mirrors the lock-attempt
                    # guard above. Unguarded it escapes into the worker's
                    # TaskGroup, cancelling every sibling WITHOUT setting
                    # shutdown_event. is_leader stays clear, so the leader-only
                    # loops keep gating until a later attempt succeeds.
                    await self._drop_leader_conn(reason="leader_upsert_failed")
                    await self._close_leader_owned_conns()
                    record_election_attempt(str(self._worker_id), won=False)
                    log.warning(
                        "leader-upsert-failed",
                        kind="leader_upsert_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
                except Exception as exc:
                    # Backstop (see _transient.py): tolerated + logged a few
                    # times, then deliberately fatal; cleanup mirrors the
                    # transient path since conn state is unknown.
                    await self._drop_leader_conn(reason="leader_upsert_failed")
                    await self._close_leader_owned_conns()
                    record_election_attempt(str(self._worker_id), won=False)
                    log.warning(
                        "leader-upsert-failed",
                        kind="leader_upsert_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    guard.unexpected(exc)
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
                try:
                    self._leader_monitor_conn = await self._open_dedicated_conn(
                        "leader_monitor_conn"
                    )
                    self._cron_conn = await self._open_dedicated_conn("cron_conn")
                except Exception as exc:
                    # Why: ``except Exception`` is deliberate here for the
                    # same reason as the leader_conn reopen path above —
                    # factory-built conns surface provider (azure/hvac/
                    # botocore) and asyncpg.InvalidPasswordError failures,
                    # which are transient and must retry, not escape into
                    # the worker TaskGroup. CancelledError (BaseException)
                    # is unaffected.
                    await self._drop_leader_conn(reason="dedicated_conn_open_failed")
                    await self._close_leader_owned_conns()
                    log.warning(
                        "leader-dedicated-conn-failed",
                        kind="leader_dedicated_conn_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                        error_type=type(exc).__name__,
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
            # Reaching here means a full election cycle completed (lock won,
            # lost, or not attempted) without an unexpected error, so the
            # backstop streak resets. (The failure paths above continue
            # earlier, deliberately without resetting.)
            guard.ok()
            await asyncio.sleep(self._deps.settings.heartbeat_interval)

    async def _watchdog_loop(self, shutdown: asyncio.Event) -> None:
        guard = UnexpectedLoopErrorGuard("leader.watchdog")
        while not shutdown.is_set():
            # Parking on is_leader.wait() alone can never wake when PG is
            # unreachable: the election loop cannot re-elect, so nothing
            # sets is_leader again — MaintenanceLeader.run's TaskGroup
            # (and with it the whole worker) would hang on exit after
            # isolate_self. Race the park against shutdown.
            leader_wait = asyncio.create_task(self._deps.is_leader.wait())
            shutdown_wait = asyncio.create_task(shutdown.wait())
            try:
                await asyncio.wait(
                    {leader_wait, shutdown_wait}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (leader_wait, shutdown_wait):
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
            if shutdown.is_set():
                return
            while not shutdown.is_set() and self._deps.is_leader.is_set():
                self._deps.liveness.tick("leader.watchdog", period=_WATCHDOG_INTERVAL_SECS)
                conn = self._leader_monitor_conn
                if conn is None:
                    break
                try:
                    await conn.fetchval("SELECT 1")
                    guard.ok()
                except TRANSIENT_PG_ERRORS as exc:
                    await self._drop_leader_conn(reason="watchdog_probe_failed")
                    await self._close_leader_owned_conns()
                    log.warning(
                        "leadership-lost",
                        kind="leadership_lost",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    break
                except Exception as exc:
                    # Backstop (see _transient.py): tolerated + logged a few
                    # times, then deliberately fatal; cleanup mirrors the
                    # transient path since conn state is unknown.
                    await self._drop_leader_conn(reason="watchdog_probe_failed")
                    await self._close_leader_owned_conns()
                    log.warning(
                        "leadership-lost",
                        kind="leadership_lost",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                    guard.unexpected(exc)
                    break
                await asyncio.sleep(_WATCHDOG_INTERVAL_SECS)
            # Leaving the inner loop means the gate closed: demotion (the
            # probe-failure break above, which clears is_leader), a dropped
            # monitor conn, or shutdown. Drop the registration HERE, before
            # re-parking on is_leader.wait() — this loop stops ticking through
            # no fault of its own, and a lingering registration goes stale
            # while parked, so detector 2 would force-exit a healthy
            # non-leader worker ~grace seconds after every ordinary
            # leadership change. Placing this after the park is too late: the
            # park only returns once is_leader is set again.
            self._deps.liveness.forget("leader.watchdog")

    async def _scheduled_wake_loop(self, shutdown: asyncio.Event) -> None:
        warned = False
        guard = UnexpectedLoopErrorGuard("leader.scheduled_wake")
        while not shutdown.is_set():
            self._deps.liveness.tick("leader.scheduled_wake", period=1.0)
            if self._deps.is_leader.is_set():
                now_utc = self._clock.now()
                start = time.monotonic()
                try:
                    # Why one deadline for the WHOLE iteration: the count > 0
                    # path awaits PG twice (scheduled_to_pending, then the
                    # acquire + pg_notify), and per-statement timeouts alone
                    # admit a tick gap of k * timeout + 1.0s, over the
                    # staleness budget for k > 1: a false detector-2 trip of
                    # a healthy leader. asyncio.timeout raises TimeoutError,
                    # which the transient-PG branch below already handles.
                    async with asyncio.timeout(self._deps.settings.dispatcher_command_timeout):
                        count = await self._backend.scheduled_to_pending(now=now_utc)
                        _metric("scheduled_to_pending", count, start)
                        _dbg("scheduled_wake_tick", "scheduled_wake_tick", count, start)
                        if count > 0:
                            channel = wake_channel(self._deps.settings.schema_name)
                            async with self._deps.dispatcher_pool.acquire(
                                timeout=self._deps.settings.dispatcher_command_timeout
                            ) as conn:
                                await conn.execute("SELECT pg_notify($1, '')", channel)
                    guard.ok()
                except NotImplementedError as exc:
                    if not warned:
                        _err("scheduled_wake_backend_unimplemented", _EK1, self._worker_id, exc)
                        warned = True
                except TRANSIENT_PG_ERRORS as exc:
                    # PG loss is transient: the next tick retries, and a
                    # missed wake NOTIFY is covered by the producer's poll
                    # interval. Unguarded it escapes into the worker's
                    # TaskGroup and wedges shutdown (see _leader_sweeps).
                    log.warning(
                        "scheduled-wake-failed",
                        kind="scheduled_wake_failed",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                except Exception as exc:
                    # Backstop for anything outside the transient set (see
                    # _transient.py): tolerated and logged a few times, then
                    # deliberately fatal rather than an infinite silent retry.
                    guard.unexpected(exc)
            await asyncio.sleep(1.0)

    async def _cron_loop(self, shutdown: asyncio.Event) -> None:
        """Tick cron schedules every second when this worker is the leader.

        Separate asyncio.Task from _sweep_loop.
        Each tick opens a transaction on ``_cron_conn`` (a dedicated
        connection owned exclusively by this loop) and delegates to
        :func:`~taskq.worker.cron_loop.tick_cron`.
        ``CancelledError`` (a ``BaseException``) is not caught by
        ``except Exception`` and propagates to the ``TaskGroup`` for
        clean shutdown.
        """
        guard = UnexpectedLoopErrorGuard("leader.cron")
        while not shutdown.is_set():
            self._deps.liveness.tick("leader.cron", period=1.0)
            if not self._deps.is_leader.is_set():
                await asyncio.sleep(1)
                continue
            conn = self._cron_conn
            if conn is None:
                await asyncio.sleep(1)
                continue
            try:
                # Why one deadline for the WHOLE tick: a tick is BEGIN + N
                # statements (one per due schedule, plus catch-up bursts) +
                # COMMIT, each separately bounded by the conn's
                # command_timeout, so per-statement timeouts alone let a
                # degraded PG stretch one tick past the detector-2 budget
                # and force-exit a healthy leader. asyncio.timeout raises
                # the exact builtin TimeoutError, which the deadline-family
                # branch below treats as retry-next-tick (the transaction
                # rolls back bounded by the same command_timeout).
                async with asyncio.timeout(self._deps.settings.dispatcher_command_timeout):
                    async with conn.transaction():
                        await tick_cron(
                            conn,
                            self._deps.settings,
                            self._backend,
                            self._deps.settings.schema_name,
                            self._worker_id,
                        )
                guard.ok()
            except TRANSIENT_PG_ERRORS as exc:
                # Why TRANSIENT_PG_ERRORS first: the cron loop used to
                # hand-roll its error classification with isinstance checks
                # that missed 7 of 12 transient shapes (DeadlockDetectedError,
                # SerializationError, AdminShutdownError, etc.). Deadlock and
                # serialization inside a transaction are routine, not
                # surprises — 5 consecutive killed the worker via the
                # backstop guard before this fix.
                if type(exc) is TimeoutError or isinstance(exc, asyncpg.QueryCanceledError):
                    # Deadline family (iteration deadline or server-side
                    # cancel): the conn is provably responsive, because it
                    # answered the cancel, or asyncpg has already terminated
                    # it, which the next tick's transaction() surfaces as a
                    # conn-state error below. Keep the conn and retry:
                    # dropping it (and demoting) on every slow tick would
                    # churn leadership during catch-up bursts.
                    #
                    # Why type(exc) is, not isinstance: TimeoutError is an
                    # OSError subclass — isinstance would also match raw
                    # OSError here, but the deadline family (asyncio.timeout /
                    # command_timeout) must keep the conn, while a raw OSError
                    # (socket death) must drop it.
                    log.warning(
                        "cron-tick-timeout",
                        kind="cron_tick_timeout",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                elif isinstance(
                    exc, (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError)
                ):
                    # Conn-state family: the conn is dead or unusable. Drop
                    # it and rebuild on a later tick.
                    await self._close_leader_owned_conns()
                    log.warning(
                        "cron-conn-lost",
                        kind="cron_conn_lost",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
                else:
                    # Other transient (deadlock, serialization, admin
                    # shutdown, cannot-connect-now, too-many-connections,
                    # idle-session timeouts): retry next tick. The conn may
                    # or may not be dead — the next tick's transaction()
                    # will surface a conn-state error if it is, and the
                    # transaction rolls back for deadlock/serialization
                    # leaving the conn usable.
                    log.warning(
                        "cron-tick-transient",
                        kind="cron_tick_transient",
                        worker_id=str(self._worker_id),
                        error=repr(exc),
                    )
            except Exception as exc:
                # Backstop for anything outside the transient set (see
                # _transient.py): tolerated and logged a few times (this
                # loop's historical blanket catch), then deliberately fatal
                # rather than retrying a real bug forever. Cleanup and log
                # BEFORE guard.unexpected so the fatal iteration still drops
                # the conn and the cron-specific log survives.
                await self._close_leader_owned_conns()
                log.warning(
                    "cron-tick-failed",
                    kind="cron_tick_unexpected",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                guard.unexpected(exc)
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
