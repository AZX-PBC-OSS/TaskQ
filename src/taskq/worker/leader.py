"""Maintenance leader: election, watchdog, and recovery sweeps.
A single elected leader per cluster runs cooperative loops inside one
asyncio.TaskGroup: election, watchdog, scheduled-wake (sweep 3), cron,
sweep (sweeps 1/2/4), prune (sweep 5), archive expiry (sweep 6), stale
worker cleanup, queue depth, reservation slots, and backlog detection.
Non-leader pods retry election periodically and skip the gated work —
the backlog detector is the deliberate exception (every worker samples;
see ``_backlog_detection_loop``).
Leadership is a row lease in ``maintenance_leader``: the holder writes an
``expires_at`` it chose from ``leader_lease`` and renews it every heartbeat
interval, and any pod may take the row over once that instant has passed.
The row alone decides the election in every state — held, lapsed, or
absent — so nothing about the role depends on the liveness of a
connection: a holder that dies without a FIN is replaced on a horizon
TaskQ controls rather than the server's connection-reaping schedule, and
the replacement needs no privilege beyond UPDATE on that row. The
schema-qualified advisory lock is still taken after winning, as a courtesy
to pods from releases that only understand the lock; it is never required,
never waited on, and its absence never gates or costs the role.

Failover SLA:
  Graceful stop      ≤ heartbeat_interval + one round trip (the resign
                       deletes the row; the next election wins it)
  Worker killed      ≤ leader_lease + heartbeat_interval + one round trip
  Silent leader      ≤ leader_lease + heartbeat_interval + one round trip
  Partition detect   ≤ watchdog_interval + heartbeat_interval + 2 s
  PG failover        ≤ heartbeat_interval
  Watchdog detect    ≤ watchdog_interval + heartbeat_interval
"""

import asyncio
import contextlib
import threading
import time
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Final
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq.backend._protocol import Backend
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    schema_lock_name,
    wake_channel,
)
from taskq.obs import (  # pyright: ignore[reportPrivateUsage]  # Why: the sweep-health caches are module-level singletons owned by the obs layer; the demotion path clears them directly (see health.py for the same seam).
    _otel,
    get_logger,
    get_meter,
    record_election_attempt,
    record_leader_lease_expires_in_seconds,
    record_lock_contention,
    record_sweep_success,
    record_sweep_timeout,
    update_queue_depth_cache,
    update_reservation_slots_cache,
    update_stranded_jobs_cache,
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.worker._leader_shared import (
    _EK1,
    ArchiveExpiryResult,
    PruneResult,
    SweepContext,
    _build_retention_per_status,
    _dbg,
    _err,
    _load_actor_retention_overrides,
    _metric_duration,
    _metric_rows,
    _schedule_utc_to_cron,
    archive_expiry_sweep,
    cleanup_stale_workers,
    complete_stale_batches,
    prune_terminal_jobs,
)
from taskq.worker._leader_sweeps import (
    _archive_expiry_loop,
    _backlog_detection_loop,
    _is_deadline_family,  # pyright: ignore[reportPrivateUsage]  # Why: the one deadline-family classifier, shared by every leader loop's timeout accounting instead of each site re-deriving it.
    _prune_loop,
    _queue_depth_loop,
    _reservation_slots_loop,
    _stranded_jobs_loop,
    _sweep_loop,
)
from taskq.worker._transient import (
    PERMANENT_PG_REFUSALS,
    TRANSIENT_PG_ERRORS,
    UnexpectedLoopErrorGuard,
)
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron
from taskq.worker.deps import (
    LeaderTerm,
    WorkerDeps,
    apply_keepalive_to_conn,
    open_dedicated_conn,
)

__all__ = [
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
_WATCHDOG_INTERVAL_SECS: float = 5.0

#: How much earlier than the server's ``expires_at`` this process stops
#: trusting its own term. The local window opens at the instant the renewal
#: attempt STARTS while the server's expiry is written after the round trip,
#: so this margin plus that round trip is the gap in which no process acts as
#: leader — the gap that makes the two windows unable to overlap.
_LEADER_TRUST_MARGIN_SECS: Final[float] = 1.0

#: How many heartbeat intervals the recorded holder's ``last_seen_at`` ping may
#: be silent before its row is takeable. This is the horizon for a holder from
#: a release that does not lease, whose ping is the only liveness signal it
#: writes; a holder that does lease is additionally held by its own expiry,
#: which is never shorter (``resolved_leader_lease``), so this slack never
#: decides a leasing holder's fate. Four missed beats is the same slack the
#: jobs' lock leases carry (the ``lock_lease >= 4 * heartbeat_interval``
#: invariant).
_PRE_LEASE_STALE_HEARTBEATS: Final[int] = 4

#: Claim or take over the lease. Zero rows returned means the role is not
#: this pod's to take, which is the ordinary follower state and not an error.
#: Concurrent takers serialise on the singleton row, so the loser re-evaluates
#: the conflict predicate against the winner's fresh expiry and gets zero rows.
#:
#: The insert side is deliberately unguarded: an absent row IS the unclaimed
#: role, and claiming it must never consult anything a session can hold
#: (River's LeaderAttemptElect grain — INSERT, no lock gate). A candidate
#: that dies between the courtesy lock attempt and this write, or a leader
#: whose resign lands while its session outlives the delete, leaves the lock
#: held with no row behind it; gating the insert on the lock would leave the
#: whole fleet unelectable until the server reaps that session. The conflict
#: predicate below is the only gate, and it alone protects a live holder.
#:
#: The recorded holder is displaced only once BOTH of the liveness signals it
#: could have written have lapsed: the lease it chose (``$2``-derived
#: ``expires_at``) and its heartbeat ping (``last_seen_at``, within the
#: ``$3`` pre-lease slack). Either alone is insufficient, because the row can
#: be written by two protocols at once during a roll. A pod from a release
#: that does not lease names only four columns in its upsert, so its takeover
#: leaves the PREVIOUS holder's ``expires_at`` in place: judging that row on
#: the expiry alone would declare a pod that is actively pinging the row
#: lapsed, and two processes would lead. Requiring the ping to have stopped
#: too costs nothing against the failover bound — the lease is never shorter
#: than the slack (``resolved_leader_lease``), so the expiry is always the
#: later of the two horizons for a pod that leases.
#:
#: The first arm lets the recorded holder refresh its OWN row (a new term,
#: not a renewal — the renewal fence is narrower). While the row names this
#: worker with a live lease no peer could legally have taken it, so the arm
#: cannot put a second leader on the row; what it buys is a cheap route back
#: for a holder that stepped down without its row lapsing — a credential
#: reload that dropped leader_conn, a transient renewal failure that spent
#: the trust window — which would otherwise pay a whole lease's lapse per
#: occurrence (Oban's upsert refreshes the node's own row the same way).
_LEADER_ELECT_SQL_TEMPLATE: Final[str] = (
    'INSERT INTO "{schema}".maintenance_leader '
    "(singleton, worker_id, elected_at, last_seen_at, expires_at) "
    "VALUES (true, $1, clock_timestamp(), clock_timestamp(), "
    "clock_timestamp() + make_interval(secs => $2)) "
    "ON CONFLICT (singleton) DO UPDATE SET "
    "worker_id = EXCLUDED.worker_id, elected_at = EXCLUDED.elected_at, "
    "last_seen_at = EXCLUDED.last_seen_at, expires_at = EXCLUDED.expires_at "
    "WHERE maintenance_leader.worker_id = $1 "
    "OR ("
    "(maintenance_leader.expires_at IS NULL "
    "OR maintenance_leader.expires_at < clock_timestamp()) "
    "AND maintenance_leader.last_seen_at < clock_timestamp() - make_interval(secs => $3)"
    ") "
    "RETURNING elected_at"
)

#: Renew, fenced on the term. Zero rows means the term is gone — taken over,
#: or lapsed at the server. A lapsed-but-untaken lease is deliberately not
#: renewable: the holder must re-elect through the same statement every peer
#: runs, so being the previous holder confers no advantage.
_LEADER_RENEW_SQL_TEMPLATE: Final[str] = (
    'UPDATE "{schema}".maintenance_leader '
    "SET last_seen_at = clock_timestamp(), "
    "expires_at = clock_timestamp() + make_interval(secs => $3) "
    "WHERE singleton = true AND worker_id = $1 AND elected_at = $2 "
    "AND expires_at >= clock_timestamp() "
    "RETURNING expires_at"
)

#: Step-down reasons that mean this pod's link to PG is what failed, mapped to
#: the default description of that failure. They carry the ``leader_conn_died``
#: kind alongside ``leadership_lost`` so a network or pool fault stays
#: distinguishable from an ordinary handover to a peer.
_LEADER_CONN_DEATH_REASONS: Final[Mapping[str, str]] = {
    "conn_lost": "leader_conn is unavailable while is_leader is set",
    "renew_failed": "the lease could not be renewed before the term lapsed",
    "probe_failed": "the leader monitor probe could not reach PG",
}

#: Hand the lease back at shutdown, fenced so a resign issued late (after a
#: takeover) cannot delete the successor's row.
_LEADER_RESIGN_SQL_TEMPLATE: Final[str] = (
    'DELETE FROM "{schema}".maintenance_leader '
    "WHERE singleton = true AND worker_id = $1 AND elected_at = $2"
)


def build_leader_lease_sql(schema: str) -> tuple[str, str, str]:
    """Render the elect / renew / resign statements for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting; asyncpg cannot bind identifiers as parameters.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        _LEADER_ELECT_SQL_TEMPLATE.format(schema=schema),
        _LEADER_RENEW_SQL_TEMPLATE.format(schema=schema),
        _LEADER_RESIGN_SQL_TEMPLATE.format(schema=schema),
    )


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
        actor_policies: Mapping[str, ActorFirePolicy] | None = None,
    ) -> None:
        self._deps = deps
        self._worker_id = worker_id
        self._backend = backend
        self._clock = clock
        # Singleton / max_pending flags the cron tick stamps and enforces
        # on its fires (parity with the client enqueue path); None keeps
        # the tick's no-stamping behavior.
        self._actor_policies = actor_policies
        self._sweep_ctx = SweepContext(
            deps=deps,
            backend=backend,
            clock=clock,
            worker_id=worker_id,
            rate_limit_registry=rate_limit_registry,
        )
        self._leader_monitor_conn: asyncpg.Connection | None = None
        self._cron_conn: asyncpg.Connection | None = None
        # The (worker_id, elected_at) term this pod last found holding the
        # role while it lost an election, so contention distinguishes a
        # transition from the steady state of following one live peer.
        self._observed_holder: tuple[UUID, datetime] | None = None
        # Log-once latch for a refused courtesy advisory-lock probe: a
        # managed Postgres refusing ``pg_try_advisory_lock`` refuses it on
        # every election win for the life of the grants, so the WARN is
        # emitted on the first refusal and re-armed only when a probe
        # succeeds again (the grant appearing IS a new operational fact).
        self._advisory_lock_refused = False

    def _demote(self) -> None:
        """Stop being the leader, synchronously and before anything can await.

        Every path that gives up the role calls this FIRST. It must not be
        an ``async def`` and must not follow an await on any of them: the
        steps that make demotion real at the server — closing the conn that
        carries the courtesy lock, closing the leader-owned conns — park for
        up to a bounded close on a dead PG, and a peer that wins the lock
        during that suspension may take the row legitimately. Anything still
        reading ``leading()`` as true meanwhile would be a second leader.
        """
        # The flag backs the leader gauge, /metrics, and the health report;
        # the term is what every leader-gated loop consults per iteration.
        self._deps.stop_leading()
        # Why here, and why empty rather than zero: queue depth, reservation
        # slots and stranded jobs are sampled ONLY by the leader's sweep
        # loops, so a demoted process that keeps its last sample keeps
        # exporting numbers it no longer has any authority over - during a
        # failover, which is exactly when those dashboards are being read.
        # An observable gauge whose callback yields nothing produces no data
        # point, so the series goes stale and the new leader's is the only
        # one answering; exporting a 0 would instead be an active claim that
        # the queue is empty, silencing depth alerts and corrupting any
        # cross-pod sum/min. Cleared with the flag rather than after the
        # bounded closes that follow demotion, which can park for seconds on
        # a dead PG; if the election loop re-elects during that suspension
        # the sweep loops repopulate on their next tick.
        # The backlog gauges (jobs-by-status, oldest due age) are deliberately
        # NOT in this list: _backlog_detection_loop samples them on every
        # worker, so a demoted process keeps full authority over its own
        # series and clearing them would mute the detectors under the exact
        # leadership failure they exist to expose.
        update_queue_depth_cache({})
        update_reservation_slots_cache({})
        update_stranded_jobs_cache({})
        # The sweep-health stamps (last success, batch size) are leader-loop
        # samples and lose authority with the rest: a demoted process
        # exporting frozen stamps reports a degraded maintenance view forever
        # after an ordinary failover, and its frozen sweep_last_success
        # series pages promotion-stalled while the new leader promotes fine.
        _otel.clear_sweep_health_caches()
        # Same authority loss for the lease-TTL gauge: the stamp claims a
        # lease this process no longer holds, and a frozen one masks the
        # failover the gauge exists to make visible.
        _otel.clear_leader_lease_expires_in_seconds()

    async def _close_leader_owned_conns(self, *, mid_run: bool = True) -> None:
        """Demote, then close the leader-owned dedicated conns, bounded.

        Two call contexts: mid-run demotion (watchdog/election/cron
        conn-died paths - the default ``mid_run=True``, the ``conn-close-*``
        alert family) and ``run()``'s finally (final teardown - passes
        ``mid_run=False`` for the ``conn-teardown-close-*`` family), so an
        ordinary shutdown never pages as an unexpected mid-run close
        timeout.
        """
        self._demote()
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

        The factory call is bounded by ``settings.reload_factory_timeout``
        — the SAME bound the notify reconnect loop, the bootstrap opens,
        and the reload path apply to every factory call. Unbounded, a hung
        token endpoint parks the election loop past every staleness
        budget and the in-worker watchdog force-exits the whole worker
        instead of this loop's own retry/backoff handling it; the bound's
        exhaustion IS the loop's ordinary factory-failure path (logged,
        heartbeat-interval backoff, retry).
        """
        factory = self._deps.leader_conn_factory
        if factory is not None:
            conn = await asyncio.wait_for(
                factory(),
                timeout=float(self._deps.settings.reload_factory_timeout),
            )
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

        The factory call is bounded by ``settings.reload_factory_timeout``,
        exactly as ``_open_leader_conn`` bounds it — the election loop's
        callers already treat any factory failure (including this
        TimeoutError) as retry-with-backoff, never a crash.
        """
        factory = self._deps.leader_conn_factory
        if factory is not None:
            conn = await asyncio.wait_for(
                factory(),
                timeout=float(self._deps.settings.reload_factory_timeout),
            )
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
                    self._backlog_detection_loop(shutdown), name="leader.backlog_detection"
                )
                tg.create_task(
                    self._reservation_slots_loop(shutdown), name="leader.reservation_slots"
                )
                tg.create_task(self._stranded_jobs_loop(shutdown), name="leader.stranded_jobs")
                await shutdown.wait()
        finally:
            # Hand the lease back before the conns go: a replacement pod then
            # elects on its next cycle instead of waiting out the lease this
            # one would otherwise leave behind. Best-effort — the lease lapses
            # on its own, so a resign that cannot reach the database costs at
            # most that wait.
            await self.resign()
            # Final teardown, not a mid-run demotion: close with the
            # conn-teardown-close-* family so an ordinary shutdown never
            # pages as an unexpected mid-run close timeout.
            await self._close_leader_owned_conns(mid_run=False)
            with _active_leaders_lock:
                _active_leaders.discard(self)

    async def _try_election_lock(self) -> bool:
        """Try the schema's advisory lock as a courtesy; report whether held.

        The lease row is what confers the role; this lock is taken by the
        winner only so a pod from a release that knows only the lock cannot
        lead beside a lease holder during a roll. It is never required and
        never waited on: a miss — or a transient failure asking, or a
        refused privilege — is logged and leadership proceeds, because a
        lock that outlives the row behind it (a candidate dead between the
        lock attempt and the election write, a departed leader's lingering
        session) must never again gate the election it used to decide.
        """
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            return False
        lock_name = schema_lock_name("maintenance_leader", self._deps.settings.schema_name)
        try:
            got = await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
            )
        except TRANSIENT_PG_ERRORS:
            # The probe rides the conn the win just arrived on; if that conn
            # is dying, the next renewal's own failure path stands the pod
            # down. A courtesy probe must not cost a lease the row already
            # granted.
            return False
        except PERMANENT_PG_REFUSALS as exc:
            # A managed Postgres restricting advisory-lock functions to
            # admin/superuser roles refuses this probe on every win, for as
            # long as the grants stand — permanent, not transient (see
            # PERMANENT_PG_REFUSALS). Degrade to a lock miss exactly as a
            # False return would: the lease row is the authority, and a
            # courtesy probe must never cost the leadership the row already
            # granted. Logged once per refusal streak so a refused
            # deployment does not WARN-spam on every re-election.
            if not self._advisory_lock_refused:
                self._advisory_lock_refused = True
                log.warning(
                    "leader-advisory-lock-refused",
                    kind="leader_advisory_lock_refused",
                    worker_id=str(self._worker_id),
                    function="pg_try_advisory_lock",
                    error=repr(exc),
                    error_type=type(exc).__name__,
                )
            return False
        self._advisory_lock_refused = False
        return got is True

    async def _step_down(self, reason: str, *, error: str | None = None) -> None:
        """Give up leadership and stop acting on it, for *reason*.

        Demotion lands before either await: releasing the courtesy lock is
        what lets a peer elect, so this process must already have stopped
        reading ``leading()`` as true by the time the lock can go.

        Dropping ``leader_conn`` rather than unlocking explicitly releases
        the courtesy lock with the session: an explicit unlock would have to
        succeed on a connection that may be exactly what failed, while a
        close is certain and bounded. The next cycle rebuilds the conn and
        re-enters the election as an ordinary candidate.
        """
        self._demote()
        # A leadership loss the connection caused keeps its own kind: an
        # operator triaging a failover needs to tell "this pod's link to PG
        # went away" from "a peer holds the row now", because only the first
        # is a network or pool fault to chase.
        died = _LEADER_CONN_DEATH_REASONS.get(reason)
        await self._drop_leader_conn(reason=reason)
        await self._close_leader_owned_conns()
        if died is not None:
            log.warning(
                "leader-conn-died",
                kind="leader_conn_died",
                worker_id=str(self._worker_id),
                reason=reason,
                error=error if error is not None else died,
            )
        log.warning(
            "leadership-lost",
            kind="leadership_lost",
            worker_id=str(self._worker_id),
            reason=reason,
        )

    async def _renew_lease(self, term: LeaderTerm, renew_sql: str) -> LeaderTerm | None:
        """Renew *term*, returning the new term or ``None`` if it is gone.

        Raises on a connection failure so the caller can decide whether the
        remaining trust allows another attempt; a returned ``None`` is the
        settled answer that some other pod now holds the row.
        """
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            raise ConnectionError("leader_conn is unavailable for lease renewal")
        attempt_started = asyncio.get_running_loop().time()
        # Never outlive the trust window: a renewal still in flight when this
        # process stops trusting its term is a renewal whose answer can no
        # longer be acted on, and the statement's own deadline must not push
        # the decision past the instant a peer may take over.
        budget = min(
            max(term.trusted_until - attempt_started, 0.0),
            float(self._deps.settings.dispatcher_command_timeout),
        )
        async with asyncio.timeout(budget):
            expires_at = await conn.fetchval(
                renew_sql,
                self._worker_id,
                term.elected_at,
                self._deps.settings.resolved_leader_lease,
            )
        if expires_at is None:
            return None
        return LeaderTerm(
            elected_at=term.elected_at,
            trusted_until=attempt_started
            + self._deps.settings.resolved_leader_lease
            - _LEADER_TRUST_MARGIN_SECS,
        )

    async def resign(self) -> None:
        """Hand the lease back so a peer elects on its next cycle.

        Fenced on the term, so a resign issued after a takeover cannot delete
        the successor's row. Best-effort by design: the lease lapses on its
        own, so a resign that cannot reach the database costs a replacement
        pod at most one lease rather than correctness.

        Runs at ``run()`` teardown, AFTER the shutdown orchestrator has
        already closed and nulled a TaskQ-owned ``leader_conn`` — so the
        write rides the leader-owned monitor conn when the primary is gone.
        Both conns are idle by then (the loops that used them have exited),
        and the monitor conn is never the orchestrator's to close.
        """
        term = self._deps.leader_term
        if term is None:
            return
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            conn = self._leader_monitor_conn
        if conn is None or conn.is_closed():
            return
        _elect, _renew, resign_sql = build_leader_lease_sql(self._deps.settings.schema_name)
        try:
            async with asyncio.timeout(CLOSE_TIMEOUT_SECS):
                await conn.execute(resign_sql, self._worker_id, term.elected_at)
        except Exception as exc:
            log.warning(
                "leader-resign-failed",
                kind="leader_resign_failed",
                worker_id=str(self._worker_id),
                error=repr(exc),
            )
            return
        log.info(
            "leader-resigned",
            kind="leader_resigned",
            worker_id=str(self._worker_id),
        )

    async def _election_loop(self, shutdown: asyncio.Event) -> None:
        guard = UnexpectedLoopErrorGuard("leader.election")
        elect_sql, renew_sql, _ = build_leader_lease_sql(self._deps.settings.schema_name)
        pre_lease_slack = _PRE_LEASE_STALE_HEARTBEATS * self._deps.settings.heartbeat_interval
        while not shutdown.is_set():
            self._deps.liveness.tick(
                "leader.election", period=self._deps.settings.heartbeat_interval
            )
            term = self._deps.leader_term
            if self._deps.is_leader.is_set() and term is not None:
                if await self._renew_term(term, renew_sql, guard):
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            if self._deps.is_leader.is_set():
                # The flag without a term is not a state this loop can renew
                # from; stand down and re-enter as an ordinary candidate.
                await self._step_down("term_missing")
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
            attempt_started = asyncio.get_running_loop().time()
            try:
                elected_at = await self._deps.leader_conn.fetchval(
                    elect_sql,
                    self._worker_id,
                    self._deps.settings.resolved_leader_lease,
                    pre_lease_slack,
                )
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
                await self._election_attempt_failed(exc)
                await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            except Exception as exc:
                # Backstop (see _transient.py): tolerated + logged a few
                # times, then deliberately fatal; cleanup mirrors the
                # transient path since conn state is unknown.
                await self._election_attempt_failed(exc)
                guard.unexpected(exc)
                await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            if isinstance(elected_at, datetime):
                # The assume path (courtesy lock probe, dedicated-conn
                # opens) runs inside the same error boundary as the
                # election statement: nothing it raises may escape into
                # the TaskGroup — one election cycle's failure is a retry
                # next tick, never a cancelled maintenance plane.
                try:
                    assumed = await self._assume_leadership(elected_at, attempt_started)
                except TRANSIENT_PG_ERRORS as exc:
                    await self._election_attempt_failed(exc)
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
                except Exception as exc:
                    await self._election_attempt_failed(exc)
                    guard.unexpected(exc)
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
                if not assumed:
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                    continue
            else:
                record_election_attempt(str(self._worker_id), won=False)
                await self._record_lost_election()
            # Reaching here means a full election cycle completed (won, lost,
            # or not attempted) without an unexpected error, so the backstop
            # streak resets. (The failure paths above continue earlier,
            # deliberately without resetting.)
            guard.ok()
            await asyncio.sleep(self._deps.settings.heartbeat_interval)

    async def _election_attempt_failed(self, exc: BaseException) -> None:
        """Shared cleanup for one failed election cycle.

        Applies to the election statement and the assume-leadership path
        alike: the leader conn's state is unknown after either failure, so
        it (and every leader-owned conn) is dropped and the lost attempt
        recorded. The caller decides what the error class buys the loop —
        a transient failure just retries next tick; an unexpected one is
        budgeted by the :class:`UnexpectedLoopErrorGuard` first.
        """
        await self._drop_leader_conn(reason="election_attempt_failed")
        await self._close_leader_owned_conns()
        record_election_attempt(str(self._worker_id), won=False)
        log.warning(
            "election-attempt-failed",
            kind="election_attempt_failed",
            worker_id=str(self._worker_id),
            error=repr(exc),
        )

    async def _record_lost_election(self) -> None:
        """Account for a lost election, recording contention only when it is real.

        Contention is a signal about transitions, and the runbook reads it as
        one: brief and intermittent during a handover, sustained only when
        the observed holder keeps changing hands or the row cannot be read
        at all. Losing to the same live holder every heartbeat is neither —
        it is what every pod in a fleet larger than one does for its whole
        life, and recording it made the sustained-rate alert fire
        permanently wherever the fleet was doing exactly what it should.

        So this records once per *distinct* holder this pod finds in its
        way: the term it observes changing is the transition, and a term
        that keeps answering is the steady state. A lost election with no
        row to observe at all — the holder resigned between this pod's
        attempt and this probe — is a handover in flight, a transition too,
        and is recorded once the same way.
        """
        conn = self._deps.leader_conn
        schema_name = self._deps.settings.schema_name
        if not _IDENT_RE.match(schema_name):
            raise ValueError(f"invalid schema identifier: {schema_name!r}")
        holder: asyncpg.Record | None = None
        if conn is not None and not conn.is_closed():
            with contextlib.suppress(Exception):
                holder = await conn.fetchrow(
                    f'SELECT worker_id, elected_at FROM "{schema_name}".maintenance_leader '  # noqa: S608  # Why: schema_name validated against _IDENT_RE above; asyncpg cannot bind identifiers as parameters.
                    "WHERE singleton = true"
                )
        observed = None if holder is None else (holder["worker_id"], holder["elected_at"])
        if observed is None or observed != self._observed_holder:
            record_lock_contention(schema_lock_name("maintenance_leader", schema_name))
        self._observed_holder = observed
        log.debug(
            "leader-retry",
            kind="leader_retry",
            worker_id=str(self._worker_id),
            holder_worker_id=None if holder is None else str(holder["worker_id"]),
            next_retry_secs=self._deps.settings.heartbeat_interval,
        )

    async def _assume_leadership(self, elected_at: datetime, attempt_started: float) -> bool:
        """Finish a won election, or stand back down if the conns will not open."""
        # Courtesy only, and only ever attempted by the winner: an
        # old-release pod understands the lock and not the lease, so the
        # lease holder takes it to keep such a pod from electing itself
        # beside this one during a roll. A miss changes nothing about this
        # pod's leadership — the lease is the authority — but the miss must
        # be visible, because during a roll it is the difference between
        # "old pods are excluded" and "they are not".
        if not await self._try_election_lock():
            log.info(
                "leader-advisory-lock-unavailable",
                kind="leader_advisory_lock_unavailable",
                worker_id=str(self._worker_id),
            )
        try:
            self._leader_monitor_conn = await self._open_dedicated_conn("leader_monitor_conn")
            self._cron_conn = await self._open_dedicated_conn("cron_conn")
        except Exception as exc:
            # Why: ``except Exception`` is deliberate here for the same reason
            # as the leader_conn reopen path above — factory-built conns
            # surface provider (azure/hvac/botocore) and
            # asyncpg.InvalidPasswordError failures, which are transient and
            # must retry, not escape into the worker TaskGroup. CancelledError
            # (BaseException) is unaffected.
            await self._drop_leader_conn(reason="dedicated_conn_open_failed")
            await self._close_leader_owned_conns()
            log.warning(
                "leader-dedicated-conn-failed",
                kind="leader_dedicated_conn_failed",
                worker_id=str(self._worker_id),
                error=repr(exc),
                error_type=type(exc).__name__,
            )
            return False
        self._deps.lead(
            LeaderTerm(
                elected_at=elected_at,
                trusted_until=attempt_started
                + self._deps.settings.resolved_leader_lease
                - _LEADER_TRUST_MARGIN_SECS,
            )
        )
        # Whatever this pod was following is gone; the next peer it finds in
        # its way is a fresh transition, not a continuation.
        self._observed_holder = None
        record_election_attempt(str(self._worker_id), won=True)
        # The lease gauge's elect arm: the server just stamped
        # expires_at = now + leader_lease, so the TTL as of this win is the
        # full lease. Mirror-armed on every successful renewal below.
        record_leader_lease_expires_in_seconds(
            str(self._worker_id), self._deps.settings.resolved_leader_lease
        )
        log.info(
            "leader-elected",
            kind="leader_elected",
            worker_id=str(self._worker_id),
            elected_at=str(elected_at),
            leader_lease=self._deps.settings.resolved_leader_lease,
        )
        return True

    async def _renew_term(
        self, term: LeaderTerm, renew_sql: str, guard: UnexpectedLoopErrorGuard
    ) -> bool:
        """Hold leadership for another lease, or stand down.

        Returns whether the caller should sleep a full heartbeat interval; a
        false return means the loop should re-enter immediately, either
        because a retry still fits inside the remaining trust or because the
        pod has just become a candidate again.
        """
        if asyncio.get_running_loop().time() >= term.trusted_until:
            # Stepping down on this process's own clock, without asking the
            # database: past this instant the server may already have let a
            # peer take the row, and acting further would be the split-brain
            # the trust window exists to prevent.
            await self._step_down("trust_expired")
            return False
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            # Nothing to renew on. Standing down now rather than waiting out
            # the trust window lets this cycle rebuild the conn and re-enter
            # the election, which is the fastest route back to leading.
            await self._step_down("conn_lost")
            return False
        try:
            renewed = await self._renew_lease(term, renew_sql)
        except TRANSIENT_PG_ERRORS as exc:
            return await self._renew_failed(term, exc, guard=guard, unexpected=False)
        except Exception as exc:
            # Backstop (see _transient.py): tolerated + logged a few times,
            # then deliberately fatal; the conn state is unknown either way.
            return await self._renew_failed(term, exc, guard=guard, unexpected=True)
        if renewed is None:
            # The fence did not match or the server had already let the lease
            # lapse: a successor holds the row.
            await self._step_down("term_lost")
            return False
        self._deps.leader_term = renewed
        guard.ok()
        # The renewal re-stamped expires_at = now + leader_lease on the
        # server; the gauge's renew arm keeps the series moving so a
        # leader that stops renewing is visible as a stale/absent series.
        record_leader_lease_expires_in_seconds(
            str(self._worker_id), self._deps.settings.resolved_leader_lease
        )
        log.debug(
            "leader-lease-renewed",
            kind="leader_lease_renewed",
            worker_id=str(self._worker_id),
        )
        return True

    async def _renew_failed(
        self,
        term: LeaderTerm,
        exc: BaseException,
        *,
        guard: UnexpectedLoopErrorGuard,
        unexpected: bool,
    ) -> bool:
        """Back off inside the remaining trust, or stand down once it is spent."""
        log.warning(
            "leader-lease-renew-failed",
            kind="leader_lease_renew_failed",
            worker_id=str(self._worker_id),
            error=repr(exc),
        )
        if unexpected:
            guard.unexpected(exc)
        remaining = term.trusted_until - asyncio.get_running_loop().time()
        if remaining <= 0:
            await self._step_down("renew_failed", error=repr(exc))
            return False
        # Retry sooner than the heartbeat cadence while trust remains: the
        # renewal is what keeps this pod leading, and the window to recover a
        # transient failure is only as long as the trust left.
        await asyncio.sleep(min(1.0, remaining))
        return False

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
                    await self._step_down("probe_failed", error=repr(exc))
                    break
                except Exception as exc:
                    # Backstop (see _transient.py): tolerated + logged a few
                    # times, then deliberately fatal; cleanup mirrors the
                    # transient path since conn state is unknown.
                    await self._step_down("probe_failed", error=repr(exc))
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
            if self._deps.leading():
                start = time.monotonic()
                rows: int | None = None
                try:
                    # Why one deadline for the WHOLE iteration: the count > 0
                    # path awaits PG twice (scheduled_to_pending, then the
                    # acquire + pg_notify), and per-statement timeouts alone
                    # admit a tick gap of k * timeout + 1.0s, over the
                    # staleness budget for k > 1: a false detector-2 trip of
                    # a healthy leader. asyncio.timeout raises TimeoutError,
                    # which the transient-PG branch below already handles.
                    async with asyncio.timeout(self._deps.settings.dispatcher_command_timeout):
                        # No `now` argument — the sweep's server-side
                        # predicate (scheduled_at <= clock_timestamp()) is
                        # the single arbiter.
                        rows = await self._backend.scheduled_to_pending()
                        if rows > 0:
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
                    # rows is None means the awaited sweep call itself was
                    # cut short — a sweep-timeouts increment. rows bound
                    # means the sweep COMPLETED and the deadline casualty
                    # was the wake NOTIFY: a different failure (pool
                    # exhaustion / notify timeout), already logged below,
                    # and counting the completed call as aborted would page
                    # the sweep-timeouts alert for a healthy sweep.
                    if rows is None and _is_deadline_family(exc):
                        record_sweep_timeout("scheduled_to_pending")
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
                finally:
                    # rows is bound only by the awaited call above; the
                    # deadline that aborts it also aborts the binding, so
                    # the failure path records duration WITHOUT a row sample
                    # (a 0-row sample would be indistinguishable from a
                    # healthy empty sweep).
                    _metric_duration("scheduled_to_pending", start)
                    if rows is not None:
                        _metric_rows("scheduled_to_pending", rows)
                        record_sweep_success("scheduled_to_pending")
                        _dbg("scheduled_wake_tick", "scheduled_wake_tick", rows, start)
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
            if not self._deps.leading():
                await asyncio.sleep(1)
                continue
            conn = self._cron_conn
            if conn is None:
                await asyncio.sleep(1)
                continue
            start = time.monotonic()
            fired: int | None = None
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
                        fired = await tick_cron(
                            conn,
                            self._deps.settings,
                            self._backend,
                            self._deps.settings.schema_name,
                            self._worker_id,
                            limit=self._deps.settings.cron_tick_limit,
                            actor_policies=self._actor_policies,
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
                    record_sweep_timeout("cron")
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
            finally:
                # fired is bound only when tick_cron returned; a timed-out
                # tick records duration WITHOUT a row sample (a 0-row sample
                # would be indistinguishable from a healthy empty tick).
                _metric_duration("cron", start)
                if fired is not None:
                    _metric_rows("cron", fired)
                    record_sweep_success("cron")
            await asyncio.sleep(1)

    async def _sweep_loop(self, shutdown: asyncio.Event) -> None:
        await _sweep_loop(self._sweep_ctx, shutdown)

    async def _prune_loop(self, shutdown: asyncio.Event) -> None:
        await _prune_loop(self._sweep_ctx, shutdown)

    async def _archive_expiry_loop(self, shutdown: asyncio.Event) -> None:
        await _archive_expiry_loop(self._sweep_ctx, shutdown)

    async def _queue_depth_loop(self, shutdown: asyncio.Event) -> None:
        await _queue_depth_loop(self._sweep_ctx, shutdown)

    async def _backlog_detection_loop(self, shutdown: asyncio.Event) -> None:
        await _backlog_detection_loop(self._sweep_ctx, shutdown)

    async def _reservation_slots_loop(self, shutdown: asyncio.Event) -> None:
        await _reservation_slots_loop(self._sweep_ctx, shutdown)

    async def _stranded_jobs_loop(self, shutdown: asyncio.Event) -> None:
        await _stranded_jobs_loop(self._sweep_ctx, shutdown)


_active_leaders: set[MaintenanceLeader] = set()
