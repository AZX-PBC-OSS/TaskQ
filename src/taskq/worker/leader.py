"""Maintenance leader: election, watchdog, and recovery sweeps.
A single elected leader per cluster runs cooperative loops inside one
asyncio.TaskGroup: election, watchdog, scheduled-wake (sweep 3), cron,
sweep (sweeps 1/2/4), prune (sweep 5), archive expiry (sweep 6), stale
worker cleanup, queue depth, reservation slots, and backlog detection.
Non-leader pods retry election periodically and skip the gated work —
the backlog detector is the deliberate exception (every worker samples;
see ``_backlog_detection_loop``).
The role itself is the ``maintenance_leader`` row, held for as long as the
horizon on it is in the future and renewed by the holder every
``heartbeat_interval``. A holder that stops renewing loses the role on a
horizon it wrote itself, so the wait is bounded by a setting rather than by
how long the server takes to notice a connection has gone. The advisory
lock survives only as a handover courtesy for a fleet still running a
release that predates the horizon; it is never required and never waited on.

Failover SLA:
  Leader gone silent ≤ leader_lease + heartbeat_interval
  Clean handover     ≤ heartbeat_interval (the leaving pod resigns)
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
from taskq.worker._transient import TRANSIENT_PG_ERRORS, UnexpectedLoopErrorGuard
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron
from taskq.worker.deps import (
    LEADER_TRUST_MARGIN_SECS,
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

#: Multiple of ``heartbeat_interval`` a holder may go without pinging
#: ``last_seen_at`` before a peer may consider it silent. Four missed beats
#: is the same slack the jobs' locks carry, and a leader that loses its own
#: connection stands down within one beat of noticing — comfortably inside.
_PING_STALE_HEARTBEATS: Final[int] = 4

# ── The lease statements ────────────────────────────────────────────────
#
# Why module-level templates rather than f-strings built in the loop: an
# inline statement escapes the bounded-write audit that walks this
# package's module-level SQL, so every write here is a constant the audit
# can see and a reviewer can read in one place. All three address the one
# singleton row; none can grow with anything.
#
# Time is the SERVER's throughout: every instant these statements write and
# every comparison they make is against ``clock_timestamp()``, so the
# horizon a holder records and the horizon a peer reads are on one clock.
# No pod's own clock enters the decision.

#: Take the role, or find it already held. ``$1`` worker_id, ``$2`` lease
#: seconds, ``$3`` ping slack seconds, ``$4`` whether the caller holds the
#: transition lock.
#:
#: Zero rows is the ordinary follower answer, not an error: someone holds
#: the role and is still alive by at least one liveness signal.
#: Concurrent takers serialise on the singleton row, and the loser
#: re-evaluates the predicate against the winner's freshly written horizon,
#: so exactly one of them comes away with a row.
#:
#: Two signals, and why neither alone is enough.
#:
#: The recorded horizon is what makes the wait bounded by a setting the
#: deployment chose rather than by the server's connection bookkeeping, and
#: it is the only signal a peer can read without knowing the holder's
#: renewal cadence.
#:
#: The ping is what protects a holder whose recorded horizon says nothing
#: about it. During a rolling upgrade a pod from the release that predates
#: the lease takes the role with a write that names no horizon at all,
#: leaving whatever the PREVIOUS holder wrote there. Reading the horizon
#: alone would hand the role to a peer the instant that stale value passed,
#: while the holder was alive and pinging every beat. So a fresh ping
#: protects a holder whatever its recorded horizon says.
#:
#: The transition lock is the third fact, and the only one that reports on
#: the holder's PROCESS rather than on what it last wrote. A session lock
#: outlives nothing: the server releases it when the session ends. A caller
#: that holds it therefore knows no session is holding it, and a holder with
#: no session cannot renew — so its horizon, however far ahead it was
#: written, will never move again. Waiting out a horizon nobody can extend
#: costs the fleet its maintenance plane for no gain, and the holder is not
#: merely unheard from but provably gone. That is why the horizon protects
#: only a holder whose session may still exist. The ping still applies in
#: full: a leader whose own connection dropped for a moment stands itself
#: down within a beat of noticing, well inside the slack a peer must wait
#: out here, so the two cannot overlap.
_LEADER_ELECT_SQL_TEMPLATE = (
    'INSERT INTO "{schema}".maintenance_leader '
    "(singleton, worker_id, elected_at, last_seen_at, expires_at) "
    "VALUES (true, $1, clock_timestamp(), clock_timestamp(), "
    "clock_timestamp() + make_interval(secs => $2)) "
    "ON CONFLICT (singleton) DO UPDATE SET "
    "worker_id = EXCLUDED.worker_id, "
    "elected_at = EXCLUDED.elected_at, "
    "last_seen_at = EXCLUDED.last_seen_at, "
    "expires_at = EXCLUDED.expires_at "
    "WHERE NOT ("
    "(maintenance_leader.expires_at IS NOT NULL "
    "AND maintenance_leader.expires_at >= clock_timestamp() "
    "AND NOT $4) "
    "OR maintenance_leader.last_seen_at >= clock_timestamp() - make_interval(secs => $3)"
    ") "
    "RETURNING elected_at, expires_at"
)

#: Extend the current term. ``$1`` worker_id, ``$2`` elected_at, ``$3``
#: lease seconds.
#:
#: Zero rows means the term is over — a peer took the role, or the lease
#: lapsed at the server. A lapsed-but-untaken lease is deliberately not
#: renewable: the holder must re-elect through the statement above, the
#: same one every peer runs, so going silent buys it no advantage over the
#: pods that stayed up.
_LEADER_RENEW_SQL_TEMPLATE = (
    'UPDATE "{schema}".maintenance_leader '
    "SET last_seen_at = clock_timestamp(), "
    "expires_at = clock_timestamp() + make_interval(secs => $3) "
    "WHERE singleton = true AND worker_id = $1 AND elected_at = $2 "
    "AND expires_at >= clock_timestamp() "
    "RETURNING expires_at"
)

#: Hand the role back on a clean exit. ``$1`` worker_id, ``$2`` elected_at.
#:
#: Fenced on the term so a resignation issued late — after a peer has
#: already taken over — deletes nothing. The next election recreates the
#: row; until then the role is simply free, which is the point of
#: resigning rather than waiting out the lease.
_LEADER_RESIGN_SQL_TEMPLATE = (
    'DELETE FROM "{schema}".maintenance_leader '
    "WHERE singleton = true AND worker_id = $1 AND elected_at = $2"
)


def build_leader_lease_sql(schema: str) -> tuple[str, str, str]:
    """Render the elect / renew / resign statements for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting: asyncpg cannot bind an identifier as a parameter, and every
    value the statements carry is ``$N``-bound.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        _LEADER_ELECT_SQL_TEMPLATE.format(schema=schema),
        _LEADER_RENEW_SQL_TEMPLATE.format(schema=schema),
        _LEADER_RESIGN_SQL_TEMPLATE.format(schema=schema),
    )


async def resign_leadership(deps: WorkerDeps, worker_id: UUID) -> None:
    """Hand the role back so a successor takes it on its next cycle.

    Called on the way out of an orderly shutdown. Without it the role stays
    nominally held until the lease lapses, which is correct but slow: a
    deploy would leave the fleet with no maintenance for the remainder of a
    lease nobody is renewing. Resigning turns that into one election cycle.

    Fenced on the term, so a resignation that arrives after a peer has
    already taken over deletes nothing — it can only ever give up a role
    this pod still holds.

    Best-effort and bounded: a failure costs the fleet the lease's
    remainder, not correctness, so it is logged and the caller proceeds with
    its shutdown either way. The term is cleared regardless, because this
    pod stops acting as leader whether or not the row came away.
    """
    term = deps.leader_term
    conn = deps.leader_conn
    if term is None:
        return
    deps.leader_term = None
    deps.is_leader.clear()
    if conn is None or conn.is_closed():
        return
    _elect, _renew, resign_sql = build_leader_lease_sql(deps.settings.schema_name)
    try:
        async with asyncio.timeout(CLOSE_TIMEOUT_SECS):
            await conn.execute(resign_sql, worker_id, term.elected_at)
    except (
        Exception
    ) as exc:  # Why: a shutdown courtesy must never fail the shutdown; the lease lapses on its own.
        log.warning(
            "leader-resign-failed",
            kind="leader_resign_failed",
            worker_id=str(worker_id),
            error=repr(exc),
        )
        return
    log.info(
        "leader-resigned",
        kind="leader_resigned",
        worker_id=str(worker_id),
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
        # The term this pod last saw holding the role, as (worker_id,
        # elected_at). Backs the contention verdict: see ``_note_refusal``.
        self._observed_holder: tuple[UUID, datetime] | None = None
        # Loop-clock instant this pod last gave up the role, or None if it
        # never has. Backs the stand-back wait: see ``_may_stand``.
        self._stood_down_at: float | None = None

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
        # backs the leader gauge, /metrics, and the health report. The term
        # goes with it: the two are one state, and a leader-gated loop
        # entering its next iteration must not find a term still standing
        # behind a cleared event.
        self._deps.is_leader.clear()
        self._deps.leader_term = None
        # Why here, and why empty rather than zero: queue depth, reservation
        # slots and stranded jobs are sampled ONLY by the leader's sweep
        # loops, so a demoted process that keeps its last sample keeps
        # exporting numbers it no longer has any authority over - during a
        # failover, which is exactly when those dashboards are being read.
        # An observable gauge whose callback yields nothing produces no data
        # point, so the series goes stale and the new leader's is the only
        # one answering; exporting a 0 would instead be an active claim that
        # the queue is empty, silencing depth alerts and corrupting any
        # cross-pod sum/min. Cleared before the bounded closes below because
        # those can park for seconds on a dead PG (same reason is_leader is
        # cleared first); if the election loop re-elects during that
        # suspension the sweep loops repopulate on their next tick.
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
        # Re-election during the bounded closes below repopulates them on the
        # sweep loops' next tick, same as the three clears above.
        _otel.clear_sweep_health_caches()
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
        # Losing this connection is losing the means to renew, so the role
        # goes with it. Resigning here — while the connection can still
        # carry the statement — turns what would be a fleet-wide wait for
        # the lease to lapse into one election cycle for the successor. On a
        # connection that is already gone the call falls through and the
        # lease does the work instead, which is the point of having one.
        await resign_leadership(self._deps, self._worker_id)
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
            # Final teardown, not a mid-run demotion: close with the
            # conn-teardown-close-* family so an ordinary shutdown never
            # pages as an unexpected mid-run close timeout.
            await self._close_leader_owned_conns(mid_run=False)
            with _active_leaders_lock:
                _active_leaders.discard(self)

    async def _try_courtesy_lock(self, lock_name: str) -> bool:
        """Take the transition advisory lock, reporting whether it was free.

        The lock no longer decides anything: the role is the row, and a pod
        that cannot get the lock still leads if the row says so. It is held
        for one reason only, and for one release only — a pod from the
        release that predates the lease understands nothing but this lock,
        so a leader holding it is a leader such a pod cannot elect over
        while both generations are running. It is never waited on, and
        losing it is never a refusal to lead.
        """
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            return False
        got = await conn.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name)
        return got is True

    async def _step_down(self, *, reason: str) -> None:
        """Give up the term and everything gated on it.

        Ordering is the whole point. The term is cleared FIRST and with it
        ``is_leader``, because until both are false a leader-gated loop
        entering its next iteration will still act, and the courtesy lock's
        release below is a bounded close that can park for seconds on an
        unreachable server. Releasing the lock first would open exactly the
        window this method exists to close: a pod that has published "the
        role is free" while its own loops are still doing the role's work.
        """
        self._deps.leader_term = None
        # Clears is_leader, drops the leader-only gauge samples, and closes
        # the cron and monitor conns (aborting any open cron transaction at
        # the server) — all bounded.
        await self._close_leader_owned_conns()
        # Dropping the connection releases the courtesy lock with the
        # session, which is both cheaper and more certain than an explicit
        # unlock on a session that may already be gone. The election loop
        # rebuilds the connection on its next cycle as an ordinary follower.
        await self._drop_leader_conn(reason=reason)
        self._stood_down_at = asyncio.get_running_loop().time()
        log.warning(
            "leadership-lost",
            kind="leadership_lost",
            worker_id=str(self._worker_id),
            reason=reason,
        )

    def _may_stand(self, now: float) -> bool:
        """Whether this pod may stand for election at *now*.

        Always, unless it has recently given the role up — see the call site
        for why a pod that has just stood down waits out its own ping before
        standing again. The wait is one ping slack, the same horizon every
        peer is already counting against that ping, so it costs nothing a
        peer was not going to spend anyway.
        """
        stood_down_at = self._stood_down_at
        if stood_down_at is None:
            return True
        slack = _PING_STALE_HEARTBEATS * self._deps.settings.heartbeat_interval
        if now - stood_down_at < slack:
            return False
        self._stood_down_at = None
        return True

    def _new_term(self, elected_at: datetime, attempt_started: float) -> LeaderTerm:
        """Build the term this attempt won.

        ``trusted_until`` is measured from when the attempt STARTED, never
        from when its answer came back. The server's horizon was stamped at
        or after that instant, so trusting from the earlier one guarantees
        this pod's window closes before the server opens the role to peers,
        however long the round trip took.
        """
        return LeaderTerm(
            elected_at=elected_at,
            trusted_until=(
                attempt_started + self._deps.settings.leader_lease - LEADER_TRUST_MARGIN_SECS
            ),
        )

    async def _renew_term(self, shutdown: asyncio.Event) -> bool:
        """Extend the term, or stand down when it can no longer be trusted.

        Three ways a term ends here, and the pod's obligation is the same in
        each: stop acting as leader. The local clock says the window closed;
        the server says a peer holds the row; or the renewal could not be
        completed while any trust remained.

        Returns whether the pod may stand for election again straight away
        rather than waiting a cycle. It may when what ended the term was
        something LOCAL that it has already repaired — a connection it lost
        and can reopen, a credential it just rotated — because there the
        role may well still be nominally its own and a wait costs the fleet
        a beat of maintenance for nothing. It may NOT when the role itself
        moved on: a term this pod could not trust, or a row that now names
        someone else. Standing again immediately in those cases would let
        the pod whose term just ended beat every peer to the role on the
        strength of being already awake, which is the opposite of what a
        failover is for.
        """
        term = self._deps.leader_term
        if term is None:
            # is_leader without a term is not a state the loop can reason
            # about; treat it as the end of a term and re-elect cleanly.
            await self._step_down(reason="term_missing")
            return True
        loop = asyncio.get_running_loop()
        settings = self._deps.settings
        _elect, renew_sql, _resign = build_leader_lease_sql(settings.schema_name)
        while not shutdown.is_set():
            attempt_started = loop.time()
            remaining = term.trusted_until - attempt_started
            if remaining <= 0:
                # Nothing is asked of the server: the window this pod
                # promised itself has closed, so it stands down on its own
                # clock before any peer is entitled to the row.
                await self._step_down(reason="trust_expired")
                return False
            conn = self._deps.leader_conn
            if conn is None or conn.is_closed():
                # Told apart by origin, not by shape. A connection this
                # process took away — a credential rotation swapping it out
                # — leaves the reference None, and nothing about the role
                # changed: the pod reopens and carries on. A connection that
                # was CLOSED under it is an infrastructure event, and the
                # fleet's survivors have been standing throughout; the
                # deposed pod waits them out rather than racing them back.
                swapped_out = conn is None
                log.warning(
                    "leader-conn-died",
                    kind="leader_conn_died",
                    worker_id=str(self._worker_id),
                    error=f"leader_conn is {'None' if swapped_out else 'closed'} "
                    "while is_leader is set",
                )
                await self._step_down(reason="conn_lost")
                return swapped_out
            try:
                async with asyncio.timeout(min(remaining, settings.dispatcher_command_timeout)):
                    renewed = await conn.fetchval(
                        renew_sql, self._worker_id, term.elected_at, settings.leader_lease
                    )
            except Exception as exc:
                # Every failure shape is the same problem from the term's
                # point of view, so they are treated alike: the term is not
                # renewed YET, and the window is closing. What must not
                # happen here is standing down on the first error. A term is
                # still this pod's until its own clock says otherwise, and
                # nothing else may take the role before then — so giving it
                # up early costs the fleet a failover it never needed, for a
                # blip that the next attempt inside the same window would
                # have ridden out. Deadlines, a momentarily busy connection,
                # a server that dropped one statement: all of them get
                # retried until the trust this pod promised itself runs out,
                # and only then does it stand down.
                log.warning(
                    "leader-renew-failed",
                    kind="leader_renew_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                backoff = min(1.0, max(0.0, term.trusted_until - loop.time()))
                if backoff <= 0:
                    await self._step_down(reason="renew_failed")
                    return False
                await asyncio.sleep(backoff)
                continue
            if renewed is None:
                # The row no longer matches this term: a peer took the role
                # after the lease lapsed, or the lease lapsed at the server
                # and must be won again through an ordinary election.
                await self._step_down(reason="term_lost")
                return False
            self._deps.leader_term = self._new_term(term.elected_at, attempt_started)
            log.debug(
                "leader-lease-renewed",
                kind="leader_lease_renewed",
                worker_id=str(self._worker_id),
                expires_at=str(renewed),
            )
            return False
        return False

    async def _election_loop(self, shutdown: asyncio.Event) -> None:
        guard = UnexpectedLoopErrorGuard("leader.election")
        settings = self._deps.settings
        lock_name = schema_lock_name("maintenance_leader", settings.schema_name)
        elect_sql, _renew, _resign = build_leader_lease_sql(settings.schema_name)
        while not shutdown.is_set():
            self._deps.liveness.tick("leader.election", period=settings.heartbeat_interval)
            loop = asyncio.get_running_loop()
            if self._deps.is_leader.is_set():
                stand_again = await self._renew_term(shutdown)
                guard.ok()
                if not stand_again:
                    await asyncio.sleep(settings.heartbeat_interval)
                    continue
                # Local repair, not a lost race: nothing took the role from
                # this pod, so it stands again without the stand-back wait.
                self._stood_down_at = None
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
                    await asyncio.sleep(settings.heartbeat_interval)
                    continue
            if not self._may_stand(loop.time()):
                # Standing down and standing straight back up is not a
                # failover. A pod that has just lost the role holds the very
                # thing keeping its peers out — its own last ping on the row
                # — so until that ping has aged past the slack every peer is
                # waiting on, this pod would be racing them for a role it
                # just failed to hold, and winning on nothing but having
                # been awake first. It waits them out instead. The role goes
                # to whoever is still standing; if nobody is, this pod's own
                # next cycle takes it.
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            attempt_started = loop.time()
            try:
                got_lock = await self._try_courtesy_lock(lock_name)
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
                await asyncio.sleep(settings.heartbeat_interval)
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
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            try:
                row = await self._attempt_election(elect_sql, got_lock=got_lock)
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
                # The conn died between taking the lock and landing the
                # write. Unguarded this escapes into the worker's TaskGroup,
                # cancelling every sibling WITHOUT setting shutdown_event.
                await self._drop_leader_conn(reason="leader_elect_failed")
                await self._close_leader_owned_conns()
                record_election_attempt(str(self._worker_id), won=False)
                log.warning(
                    "leader-elect-failed",
                    kind="leader_elect_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            except Exception as exc:
                # Backstop (see _transient.py): tolerated + logged a few
                # times, then deliberately fatal; cleanup mirrors the
                # transient path since conn state is unknown.
                await self._drop_leader_conn(reason="leader_elect_failed")
                await self._close_leader_owned_conns()
                record_election_attempt(str(self._worker_id), won=False)
                log.warning(
                    "leader-elect-failed",
                    kind="leader_elect_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                )
                guard.unexpected(exc)
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            if row is None:
                # Someone else holds the role and is alive. The pod stays a
                # follower and, if it took the courtesy lock on the way in,
                # gives it straight back: holding it would block the pods
                # from the pre-lease release that still need it to elect.
                if got_lock:
                    await self._release_courtesy_lock(lock_name)
                record_election_attempt(str(self._worker_id), won=False)
                log.info(
                    "leader-retry",
                    kind="leader_retry",
                    worker_id=str(self._worker_id),
                    lock=lock_name,
                    next_retry_secs=settings.heartbeat_interval,
                )
                guard.ok()
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            if not got_lock:
                # Led without the courtesy lock, which is the ordinary case
                # once the fleet has rolled and the only case after a holder
                # dies without closing its session. Noted rather than
                # treated as a problem: nothing about the role needs it.
                log.info(
                    "leader-advisory-lock-unavailable",
                    kind="leader_advisory_lock_unavailable",
                    worker_id=str(self._worker_id),
                    lock=lock_name,
                )
            try:
                self._leader_monitor_conn = await self._open_dedicated_conn("leader_monitor_conn")
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
                await asyncio.sleep(settings.heartbeat_interval)
                continue
            # Term before event: a loop woken by is_leader must never find
            # the event set with no term behind it to consult.
            self._deps.leader_term = self._new_term(row["elected_at"], attempt_started)
            self._deps.is_leader.set()
            record_election_attempt(str(self._worker_id), won=True)
            log.info(
                "leader-elected",
                kind="leader_elected",
                worker_id=str(self._worker_id),
                expires_at=str(row["expires_at"]),
                leader_lease=settings.leader_lease,
                advisory_lock_held=got_lock,
            )
            guard.ok()
            await asyncio.sleep(settings.heartbeat_interval)

    async def _attempt_election(self, elect_sql: str, *, got_lock: bool) -> asyncpg.Record | None:
        """Run one election attempt; return the row won, or ``None``.

        Whether the courtesy lock was free decides only how a refusal is
        READ, never whether the attempt is made. The holder of the lock is
        not necessarily the holder of the role — a pod whose session
        outlived its usefulness holds one and not the other, and that is
        exactly the case this design recovers from without asking the
        database for a privilege it may not grant.

        The lock still decides one thing. A pod holding it with no role row
        behind it is either mid-election and about to write one, or a
        session that will never write one at all; electing in the first case
        would be taking a role another pod is in the middle of claiming, and
        the second is precisely what the contention metric names. Deferring
        and counting covers both.
        """
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            return None
        settings = self._deps.settings
        holder = await self._observe_holder(conn)
        if not got_lock and holder is None:
            self._observed_holder = None
            record_lock_contention(schema_lock_name("maintenance_leader", settings.schema_name))
            return None
        ping_slack = _PING_STALE_HEARTBEATS * settings.heartbeat_interval
        row = await conn.fetchrow(
            elect_sql, self._worker_id, settings.leader_lease, ping_slack, got_lock
        )
        if row is None:
            self._note_refusal(holder)
        else:
            self._observed_holder = None
        return row

    def _note_refusal(self, holder: tuple[UUID, datetime] | None) -> None:
        """Count a refusal as contention only while the role is still moving.

        A refusal says a live holder exists, which for every pod that is not
        the leader is the ordinary state of a healthy fleet — on every cycle,
        for the life of the process. Counting it is what made this metric
        rise forever in any fleet larger than one, so that the alert it backs
        could never clear and the runbook's own recovery check ("stops
        rising") was unsatisfiable.

        What the metric is for is a role that is contended rather than
        simply held: a term this pod has not seen settle. The first refusal
        against an unfamiliar holder counts, because this pod cannot yet
        tell a handover from a steady state; once the same term answers
        twice, it has settled and the pod goes quiet about it. A genuine
        handover — the term changing under repeated attempts — keeps
        counting, which is the brief-and-intermittent shape the alert's
        sustained-rate condition was written around.
        """
        if holder is not None and holder == self._observed_holder:
            return
        self._observed_holder = holder
        record_lock_contention(
            schema_lock_name("maintenance_leader", self._deps.settings.schema_name)
        )

    async def _observe_holder(self, conn: asyncpg.Connection) -> tuple[UUID, datetime] | None:
        """The term currently on the role row, or ``None`` when there is no row."""
        schema_name = self._deps.settings.schema_name
        if not _IDENT_RE.match(schema_name):
            raise ValueError(f"invalid schema identifier: {schema_name!r}")
        row = await conn.fetchrow(
            f'SELECT worker_id, elected_at FROM "{schema_name}".maintenance_leader '  # noqa: S608  # Why: schema_name validated against _IDENT_RE above; asyncpg cannot bind identifiers as parameters.
            "WHERE singleton = true"
        )
        if row is None:
            return None
        return (row["worker_id"], row["elected_at"])

    async def _release_courtesy_lock(self, lock_name: str) -> None:
        """Give the transition lock back after an election this pod lost.

        A follower holding it would deny the role to the very pods it
        exists for. Best-effort: on a connection that has gone the lock is
        already gone with the session.
        """
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            return
        with contextlib.suppress(Exception):
            await conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)

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
