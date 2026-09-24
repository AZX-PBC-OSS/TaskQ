"""Maintenance leader: election, watchdog, and recovery sweeps.
A single elected leader per cluster runs cooperative loops inside one
asyncio.TaskGroup: election, watchdog, scheduled-wake (sweep 3), cron,
sweep (sweeps 1/2/4), prune (sweep 5), archive expiry (sweep 6), stale
worker cleanup, queue depth, reservation slots, and backlog detection.
Non-leader pods retry election periodically and skip the gated work ,
the backlog detector is the deliberate exception (every worker samples;
see ``_backlog_detection_loop``).
Leadership is a row lease in ``maintenance_leader``: the holder writes an
``expires_at`` it chose from ``leader_lease`` and renews it every heartbeat
interval, and any pod may take the row over once that instant has passed.
The row alone decides the election in every state, held, lapsed, or
absent, so nothing about the role depends on the liveness of a
connection: a holder that dies without a FIN is replaced on a horizon
TaskQ controls rather than the server's connection-reaping schedule, and
the replacement needs no privilege beyond UPDATE on that row. The
schema-qualified advisory lock is still taken after winning, as a courtesy
to pods from releases that only understand the lock; it is never required,
never waited on, and its absence never gates or costs the role.

Failover SLA:
  Graceful stop      ≤ one round trip from the stop signal (the
                       shutdown-ordering contract below: the resign is
                       the FIRST act of the shutdown, not its last, so
                       the drain adds no latency to the handover)
  Worker killed      ≤ leader_lease + heartbeat_interval + one round trip
  Silent leader      ≤ leader_lease + heartbeat_interval + one round trip
  Won, unassumable   ≤ leader_lease + heartbeat_interval + one failing
                       cycle's connection attempts (each bounded by
                       reload_factory_timeout; the trust-spent hand-back
                       deletes the row: the lapse backstop cannot fire
                       while sub-lease re-wins keep refreshing it; see
                       ``_hand_back_unassumable_lease``)
  Partition detect   ≤ watchdog_interval + heartbeat_interval + 2 s
  PG failover        ≤ heartbeat_interval
  Watchdog detect    ≤ watchdog_interval + heartbeat_interval

Shutdown ordering contract (the no-leadership-while-stopping invariant):
a worker that is shutting down NEVER wins, holds, or renews the leader
lease, and a leader that begins shutting down hands the lease over at
shutdown START, so a rolling deploy never has a leaderless window and
never sees a dying pod win or hold leadership.

1. ``orchestrate_shutdown`` sets ``deps.shutdown_start_event`` BEFORE the
   DRAINING phase touches any row. That event is the stop signal this
   runtime obeys, earlier and narrower than the worker-wide
   ``shutdown_event``, which does not fire until every phase has run.
2. The election loop parks the moment the stop signal is observed: no
   attempts and no renewals for the rest of the shutdown, however long
   the drain runs. It drops its liveness registration when it parks
   (detector 2 must not trip on a loop that stopped by design) and
   ignores the resign-broadcast wake (:meth:`wake_election` no-ops).
3. A pod that is LEADING when the stop lands hands the lease over at
   shutdown START: demote first (the flag drops before any await), then
   the fenced resign over ``leader_conn``, which is still open - the
   orchestrator closes it only after the phases. The successor's
   election tick (or the broadcast wake) elects while THIS pod is still
   draining its jobs. The teardown resign in ``run()``'s finally
   remains as the backstop for exits that never run the orchestrator
   (a sibling crash, a bare cancel of ``_main``).
4. An elect statement that was in flight when the stop landed and WON
   anyway is handed straight back: fenced resign over the conn the
   elect just used, and the assume path never runs - the monitor/cron
   conns do not open, ``lead()`` never sets the flag, so no leader-gated
   loop, sweep or cron tick included, can begin.
5. The drain does not depend on this pod remaining leader: every phase
   of ``orchestrate_shutdown`` writes through the dispatcher pool and
   the backend with no leadership premise, and the successor covers the
   maintenance sweeps from its first tick.
"""

import asyncio
import contextlib
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Final
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq.backend._protocol import Backend
from taskq.backend._sql import WAKE_NOTIFY_SQL
from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    schema_lock_name,
    wake_channel,
)
from taskq.cron import CronScheduleSpec
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
from taskq.worker._cron_recovery import revert_stale_auto_disables
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
#: leader, the gap that makes the two windows unable to overlap.
_LEADER_TRUST_MARGIN_SECS: Final[float] = 1.0

#: How many heartbeat intervals the recorded holder's ``last_seen_at`` ping may
#: be silent before its row is takeable. This is the horizon for a holder from
#: a release that does not lease, whose ping is the only liveness signal it
#: writes; a holder that does lease is additionally held by its own expiry,
#: which is never shorter (``resolved_leader_lease``), so this slack never
#: decides a leasing holder's fate. Four missed beats is the same slack the
#: jobs' lock leases carry (the ``lock_lease`` failed-beat-cascade
#: invariant,.
_PRE_LEASE_STALE_HEARTBEATS: Final[int] = 4

#: Claim or take over the lease. Zero rows returned means the role is not
#: this pod's to take, which is the ordinary follower state and not an error.
#: Concurrent takers serialise on the singleton row, so the loser re-evaluates
#: the conflict predicate against the winner's fresh expiry and gets zero rows.
#:
#: The insert side is deliberately unguarded: an absent row IS the unclaimed
#: role, and claiming it must never consult anything a session can hold,
#: so the claim is a bare INSERT with no lock gate. A candidate
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
#: too costs nothing against the failover bound, the lease is never shorter
#: than the slack (``resolved_leader_lease``), so the expiry is always the
#: later of the two horizons for a pod that leases.
#:
#: The first arm lets the recorded holder refresh its OWN row (a new term,
#: not a renewal, the renewal fence is narrower). While the row names this
#: worker with a live lease no peer could legally have taken it, so the arm
#: cannot put a second leader on the row; what it buys is a cheap route back
#: for a holder that stepped down without its row lapsing, a credential
#: reload that dropped leader_conn, a transient renewal failure that spent
#: the trust window, which would otherwise pay a whole lease's lapse per
#: occurrence.
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

#: Renew, fenced on the term. Zero rows means the term is gone, taken over,
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
        cron_specs: Sequence[CronScheduleSpec] | None = None,
    ) -> None:
        self._deps = deps
        self._worker_id = worker_id
        self._backend = backend
        self._clock = clock
        # Singleton / max_pending flags the cron tick stamps and enforces
        # on its fires (parity with the client enqueue path); None keeps
        # the tick's no-stamping behavior.
        self._actor_policies = actor_policies
        # The code's declared cron specs, for the takeover recovery. At
        # every leadership assumption the leader re-runs the stale
        # auto-disable recovery over these (the same predicates the boot
        # registration pass applies): during a mixed-version rolling
        # deploy the OLD leader's cron tick can auto-disable a schedule
        # AFTER every new pod has booted, an unmarked (disabled_by=NULL)
        # write the boot pass has already finished matching, so the next
        # leadership change is the last moment the new code can revert it
        # without a full restart. None (tests, spec-less deployments) runs
        # no takeover pass.
        self._cron_specs = list(cron_specs) if cron_specs else []
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
        # The last election this pod WON, kept past every demotion for
        # resign()'s fence.  ``stop_leading`` nulls ``deps.leader_term`` on
        # every mid-run step-down, so without this copy a leader that
        # demoted itself (the cron/monitor conn-death paths) and is then
        # torn down cannot fence its resign: the row it left behind lapses
        # only after a whole ``leader_lease``, stalling every successor by
        # exactly the wait resign exists to remove.  The fence stays sound
        # because the resign DELETE matches ``(worker_id, elected_at)``
        # exactly, a successor's takeover rewrites both, so a stale fence
        # deletes nothing.
        self._resign_fence: LeaderTerm | None = None
        # The FIRST term of this pod's current won-but-unassumable episode:
        # a run of election wins whose dedicated-conn opens keep failing
        # Its ``trusted_until`` is the episode's whole trust
        # budget (the same window ``_renew_failed`` gives a renewing
        # leader to recover a transient failure before standing down),
        # because every re-win mints a fresh full window, so nothing
        # else bounds how long the pod can keep a row it cannot use.
        # Cleared by a successful assume (the pod can lead again) and by
        # an OBSERVED ELECTION LOSS (a peer holds the row now, the
        # episode is over, and a later win starts a fresh one with a
        # fresh budget); deliberately NOT cleared by the trust-spent
        # resign itself, so a pod that re-wins while still broken, with
        # no peer having taken the row in between, hands the row
        # straight back instead of buying another window.
        self._unassumable_anchor: LeaderTerm | None = None
        # Log-once latch for a refused courtesy advisory-lock probe: a
        # managed Postgres refusing ``pg_try_advisory_lock`` refuses it on
        # every election win for the life of the grants, so the WARN is
        # emitted on the first refusal and re-armed only when a probe
        # succeeds again (the grant appearing IS a new operational fact).
        self._advisory_lock_refused = False
        # The election-wake seam: the resign broadcast (the leadership
        # channel's NOTIFY when a leader hands the lease over) lands here
        # so a follower attempts NOW instead of on its next heartbeat
        # tick. Consumed by the loop's tick wait; a stopping worker
        # ignores it (see ``wake_election``) - the shutdown-ordering
        # contract in this module's doc header.
        self._wake_event = asyncio.Event()

    def _stopping(self) -> bool:
        """Whether this worker's shutdown has begun.

        The stop signal is ``deps.shutdown_start_event``, set by the
        shutdown orchestrator BEFORE its first phase touches a row -
        earlier and narrower than the ``shutdown`` event the loops exit
        on, which does not fire until the phases complete. Everything
        the no-leadership-while-stopping invariant needs hangs off this
        one predicate: the election park, the mid-elect hand-back, the
        wake refusal.
        """
        return self._deps.shutdown_start_event.is_set()

    def wake_election(self) -> None:
        """Request an immediate election attempt (the resign-broadcast seam).

        A hint, never a command: the loop still runs every gate it runs
        on its ordinary cadence. A worker whose shutdown has begun
        IGNORES the wake - no attempt, no win, no lease - the shutdown
        ordering contract in this module's doc header. Safe to call from
        any task on this loop; the wake is consumed by the election
        loop's tick wait.
        """
        if self._stopping():
            log.debug(
                "election-wake-ignored-stopping",
                kind="election_wake_ignored_stopping",
                worker_id=str(self._worker_id),
            )
            return
        self._wake_event.set()

    async def _wait_next_tick(self, seconds: float) -> None:
        """Wait out one election tick, cut short by the stop signal or a wake.

        Why the waits race at all: the stop handover is due at shutdown
        START (before the job drain), and a follower woken by the resign
        broadcast must attempt now, not a heartbeat later - a plain
        ``asyncio.sleep`` would charge both to the tick cadence. The
        wait tasks are always reaped (the ``_watchdog_loop`` idiom), so
        a wake racing the stop leaks nothing. The wake is consumed even
        when the stop wins the race: a stale wake must not outlive the
        tick it interrupted.
        """
        stop = self._deps.shutdown_start_event
        wake = self._wake_event
        if stop.is_set():
            return
        if wake.is_set():
            wake.clear()
            return
        sleep_task = asyncio.create_task(asyncio.sleep(seconds))
        stop_task = asyncio.create_task(stop.wait())
        wake_task = asyncio.create_task(wake.wait())
        try:
            await asyncio.wait(
                {sleep_task, stop_task, wake_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (sleep_task, stop_task, wake_task):
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            wake.clear()

    def _demote(self) -> None:
        """Stop being the leader, synchronously and before anything can await.

        Every path that gives up the role calls this FIRST. It must not be
        an ``async def`` and must not follow an await on any of them: the
        steps that make demotion real at the server, closing the conn that
        carries the courtesy lock, closing the leader-owned conns, park for
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
                # this. The helper never raises - a superset of the
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
            # can run a full cycle during that suspension, creating fresh
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
                # election drop path. The helper never raises, so
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
        , the SAME bound the notify reconnect loop, the bootstrap opens,
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
        exactly as ``_open_leader_conn`` bounds it, the election loop's
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
            # one would otherwise leave behind. Best-effort, the lease lapses
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
        never waited on: a miss, or a transient failure asking, or a
        refused privilege, is logged and leadership proceeds, because a
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
            # long as the grants stand, permanent, not transient (see
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

    async def resign(self) -> bool:
        """Hand the lease back so a peer elects on its next cycle.

        Fenced on the term, so a resign issued after a takeover cannot delete
        the successor's row. Best-effort by design: the lease lapses on its
        own, so a resign that cannot reach the database costs a replacement
        pod at most one lease rather than correctness.

        Runs at ``run()`` teardown, AFTER the shutdown orchestrator has
        already closed and nulled a TaskQ-owned ``leader_conn``, so the
        write rides the leader-owned monitor conn when the primary is gone.
        Both conns are idle by then (the loops that used them have exited),
        and the monitor conn is never the orchestrator's to close.

        The fence prefers the live term and falls back to the last election
        this pod won: a mid-run demotion clears ``deps.leader_term`` long
        before teardown, and without the fallback this pod's own row, never
        taken over, or the DELETE would fence it out, would sit until the
        lease lapses while a replacement pod waits on it.

        The other caller is ``_hand_back_unassumable_lease``, mid-run and
        on a pod that never led (``leader_term`` is None, so the fallback
        fence is the one that applies): it rides the leader conn the
        winning elect just used, before that conn is dropped.

        Returns whether the resign actually DELETED the row. ``False`` is
        the ordinary fenced no-op (a successor's row, or none left) and
        every best-effort shape (no term to resign, no live conn to ride,
        the write raising): the no-op and no-term/no-conn shapes are
        silent by design (at teardown they are normal), while a raised
        write carries its own ``leader-resign-failed`` WARN. A caller
        whose policy REPORTS a hand-back must not do so on a ``False``
        here, which is why the trust-spent hand-back WARNs only on
        a ``True``.
        """
        term = self._deps.leader_term or self._resign_fence
        if term is None:
            return False
        conn = self._deps.leader_conn
        if conn is None or conn.is_closed():
            conn = self._leader_monitor_conn
        if conn is None or conn.is_closed():
            return False
        _elect, _renew, resign_sql = build_leader_lease_sql(self._deps.settings.schema_name)
        try:
            async with asyncio.timeout(CLOSE_TIMEOUT_SECS):
                tag = await conn.execute(resign_sql, self._worker_id, term.elected_at)
        except Exception as exc:
            log.warning(
                "leader-resign-failed",
                kind="leader_resign_failed",
                worker_id=str(self._worker_id),
                error=repr(exc),
            )
            return False
        # The command tag is the truthful delete count: "DELETE 1" is a
        # row handed back, "DELETE 0" is the fence correctly matching
        # nothing (a successor's row, or the row already resigned). A
        # caller reporting the hand-back must read this, not the absence
        # of an error: a fenced no-op is a success-shaped failure.
        parts = tag.split()
        deleted = len(parts) == 2 and parts[0].upper() == "DELETE" and parts[1] != "0"
        if not deleted:
            log.info(
                "leader-resign-noop",
                kind="leader_resign_noop",
                worker_id=str(self._worker_id),
                tag=tag,
            )
            return False
        log.info(
            "leader-resigned",
            kind="leader_resigned",
            worker_id=str(self._worker_id),
        )
        return True

    async def _hand_over_at_stop(self) -> None:
        """The early handover: resign at shutdown START, before the job drain.

        The shutdown-ordering contract's leading branch (this module's
        doc header): a leader that begins shutting down does not carry
        the lease through the drain - today's shape resigned in
        ``run()``'s teardown, AFTER every phase, so a rolling deploy's
        successor waited out the whole drain window. Demote comes first
        (the ``_demote`` rule: the flag drops before anything can
        await), then the fenced resign over ``leader_conn``, which is
        still open here: the orchestrator closes it only after the
        phases, so the write rides a conn that survives the close.

        Idempotent across the park's retries: once a resign DELETES the
        row the fence is cleared (there is nothing left of ours to hand
        back), and a fenced no-op (a successor already holds the row)
        leaves the fence in place only until the next retry confirms
        the row is not ours to delete. No retry can ever touch a
        successor's term: the fence is ``(worker_id, elected_at)``.
        """
        if not (
            self._deps.leading()
            or self._deps.leader_term is not None
            or self._resign_fence is not None
        ):
            return
        self._demote()
        if await self.resign():
            self._resign_fence = None

    async def _resign_won_lease(self, elected_at: datetime, attempt_started: float) -> None:
        """Hand back a lease this pod won but must not hold: the stop race.

        The elect statement was in flight when the stop signal landed
        and won anyway - the row now carries this pod's name, and every
        peer's lapse predicate is re-falsified by it until it is handed
        back (the same shape ``_hand_back_unassumable_lease`` exists
        for). The resign rides ``deps.leader_conn``, the conn the
        winning elect just used, before the loop parks and the
        orchestrator's later close takes the conn down. The assume path
        never starts: no monitor/cron conns, no ``lead()``, no
        leader-gated sweep, the lease is never held across the drain.
        """
        self._resign_fence = LeaderTerm(
            elected_at=elected_at,
            trusted_until=attempt_started
            + self._deps.settings.resolved_leader_lease
            - _LEADER_TRUST_MARGIN_SECS,
        )
        if await self.resign():
            self._resign_fence = None

    async def _hand_back_unassumable_lease(self, *, reason: str) -> None:
        """Give back a lease this pod won but could not assume.

        A won row whose dedicated conns will not open is the worst state
        an election can end in: this pod's name is on the row, so every
        peer's lapse predicate is re-falsified on each own-row re-win,
        and ``deps.lead()`` never runs, so no maintenance loop,
        including the reclaim sweep, runs anywhere in the fleet. Before
        the row lease, dropping ``leader_conn`` released the advisory
        lock and a peer took over; the row outlives the connection now,
        so this pod must hand the row back itself.

        The budget is the episode's first won term's trust window, set
        in ``_unassumable_anchor``: one failed conn open is a blip, and
        the own-row arm's cheap route back (a credential reload, a
        momentary ``TooManyConnections``) must keep working: resigning
        on the first failure would hand the lease to a peer per blip and
        thrash leadership. Past the window this pod no longer trusts the
        term it keeps refreshing (the split-brain rule ``_renew_term``
        already enforces as the ``trust_expired`` stand-down), so the
        row goes back through the fenced ``resign()`` (its fence is the
        CURRENT win's term, re-captured at the top of every
        ``_assume_leadership``), which rides ``deps.leader_conn``: the
        same connection the winning elect just used, still open here
        because every caller runs this BEFORE dropping it. Best-effort
        like every resign: a failure costs at most the wait the lapse
        would have charged, and the next cycle re-attempts while the
        episode persists.

        An episode ends two ways: a successful assume (this pod proved
        it can lead) or an observed election loss (a peer holds the row;
        the election loop clears the anchor there). The resign itself
        does NOT end it: a pod that re-wins while still broken, with no
        peer having taken the row in between, must hand the row straight
        back, not buy a fresh window per cycle.
        """
        anchor = self._unassumable_anchor
        if anchor is None:
            # First failure of the episode: this win's own trust window
            # is the budget, and _resign_fence holds the same term.
            self._unassumable_anchor = self._resign_fence
            return
        if asyncio.get_running_loop().time() < anchor.trusted_until:
            # Still inside the budget a renewing leader would get; a peer
            # could not legally hold the row yet, so keeping it costs the
            # fleet nothing a live leader's transient failure would not.
            return
        deleted = await self.resign()
        if not deleted:
            # The WARN below is the policy record ("this pod handed the
            # lease back because it cannot assume it") and must not
            # claim a hand-back that did not land: a resign with no live
            # conn returns silently and a fenced no-op succeeds at
            # nothing, and reporting either as a hand-back sends the
            # runbook's "the fleet is leading from elsewhere" advice to
            # an operator staring at a row nobody resigned. The shapes
            # that tried and failed carry ``leader-resign-failed``; the
            # episode persists either way, so the next won cycle retries.
            return
        log.warning(
            "leader-resigned-unassumable",
            kind="leader_resigned_unassumable",
            worker_id=str(self._worker_id),
            reason=reason,
            episode_elected_at=str(anchor.elected_at),
        )

    async def _election_loop(self, shutdown: asyncio.Event) -> None:
        guard = UnexpectedLoopErrorGuard("leader.election")
        elect_sql, renew_sql, _ = build_leader_lease_sql(self._deps.settings.schema_name)
        pre_lease_slack = _PRE_LEASE_STALE_HEARTBEATS * self._deps.settings.heartbeat_interval
        while not shutdown.is_set():
            # The shutdown-ordering contract, checked before anything
            # else this loop can do: once the stop signal is observed
            # this loop NEVER attempts the election again - no elect, no
            # renew, however long the drain runs. The handover (below)
            # is the loop's last act of leadership; the liveness
            # registration is dropped first so detector 2 does not trip
            # on a loop that stopped by design mid-shutdown.
            if self._stopping():
                self._deps.liveness.forget("leader.election")
                await self._hand_over_at_stop()
                if self._resign_fence is None:
                    # The handover landed (or there was never a term of
                    # ours): park attempt-free for the rest of the
                    # shutdown. A wake from the resign broadcast finds
                    # nothing here to wake into - no attempt can run.
                    await shutdown.wait()
                else:
                    # The resign could not reach the database; retry it
                    # on the tick cadence, still fenced, still
                    # attempt-free. A plain sleep: the stop-racing wait
                    # would return instantly now the stop is set, and a
                    # failed resign has no conn to wait on, so this is
                    # the only thing pacing the retry.
                    await asyncio.sleep(self._deps.settings.heartbeat_interval)
                continue
            self._deps.liveness.tick(
                "leader.election", period=self._deps.settings.heartbeat_interval
            )
            term = self._deps.leader_term
            if self._deps.is_leader.is_set() and term is not None:
                if await self._renew_term(term, renew_sql, guard):
                    await self._wait_next_tick(self._deps.settings.heartbeat_interval)
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
                    await self._wait_next_tick(self._deps.settings.heartbeat_interval)
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
                await self._wait_next_tick(self._deps.settings.heartbeat_interval)
                continue
            except Exception as exc:
                # Backstop (see _transient.py): tolerated + logged a few
                # times, then deliberately fatal; cleanup mirrors the
                # transient path since conn state is unknown.
                await self._election_attempt_failed(exc)
                guard.unexpected(exc)
                await self._wait_next_tick(self._deps.settings.heartbeat_interval)
                continue
            if isinstance(elected_at, datetime):
                # The stop race: this elect was in flight when the stop
                # signal landed and WON anyway. The lease is never held
                # across the drain - hand the row straight back, fenced,
                # over the conn the elect just used, and never run the
                # assume path below (no monitor/cron conns, no lead(),
                # no leader-gated sweep can begin).
                if self._stopping():
                    await self._resign_won_lease(elected_at, attempt_started)
                    # Not break: the top-of-loop gate is the one park,
                    # and it drops the liveness registration and retries
                    # the resign if this one could not reach the
                    # database.
                    continue
                # The assume path (courtesy lock probe, dedicated-conn
                # opens) runs inside the same error boundary as the
                # election statement: nothing it raises may escape into
                # the TaskGroup, one election cycle's failure is a retry
                # next tick, never a cancelled maintenance plane.
                try:
                    assumed = await self._assume_leadership(elected_at, attempt_started)
                except TRANSIENT_PG_ERRORS as exc:
                    await self._election_attempt_failed(exc, won_row=True)
                    await self._wait_next_tick(self._deps.settings.heartbeat_interval)
                    continue
                except Exception as exc:
                    await self._election_attempt_failed(exc, won_row=True)
                    guard.unexpected(exc)
                    await self._wait_next_tick(self._deps.settings.heartbeat_interval)
                    continue
                if not assumed:
                    await self._wait_next_tick(self._deps.settings.heartbeat_interval)
                    continue
            else:
                # A lost election is the observable end of any
                # won-but-unassumable episode: the row a live peer now
                # holds is not this pod's to hand back, so a LATER win of
                # this pod starts a fresh episode with a fresh trust
                # budget. Without this, an anchor left spent by an old
                # episode resigned the first conn-open blip of every
                # later term (the per-blip thrash the budget exists to
                # reject, arriving for exactly the pods that once had an
                # episode). The anti-fresh-window rule is untouched: a
                # re-win while still broken never passes through here
                # (the own-row arm or a free-row INSERT wins instead), so
                # the anchor still survives the resign itself.
                self._unassumable_anchor = None
                record_election_attempt(str(self._worker_id), won=False)
                await self._record_lost_election()
            # Reaching here means a full election cycle completed (won, lost,
            # or not attempted) without an unexpected error, so the backstop
            # streak resets. (The failure paths above continue earlier,
            # deliberately without resetting.)
            guard.ok()
            await self._wait_next_tick(self._deps.settings.heartbeat_interval)

    async def _election_attempt_failed(self, exc: BaseException, *, won_row: bool = False) -> None:
        """Shared cleanup for one failed election cycle.

        Applies to the election statement and the assume-leadership path
        alike: the leader conn's state is unknown after either failure, so
        it (and every leader-owned conn) is dropped and the lost attempt
        recorded. The caller decides what the error class buys the loop ,
        a transient failure retries on the next tick; an unexpected one is
        budgeted by the :class:`UnexpectedLoopErrorGuard` first.

        ``won_row`` marks the cycles whose election statement had already
        WON the lease before failing (an exception escaping
        :meth:`_assume_leadership`): those leave this pod's name on a row
        it may never manage to assume, so the trust-spent hand-back runs FIRST,
        before the leader conn it rides is dropped.
        """
        if won_row:
            await self._hand_back_unassumable_lease(reason="assume_failed")
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
        at all. Losing to the same live holder every heartbeat is neither ,
        it is what every pod in a fleet larger than one does for its whole
        life, and recording it made the sustained-rate alert fire
        permanently wherever the fleet was doing exactly what it should.

        So this records once per *distinct* holder this pod finds in its
        way: the term it observes changing is the transition, and a term
        that keeps answering is the steady state. A lost election with no
        row to observe at all, the holder resigned between this pod's
        attempt and this probe, is a handover in flight, a transition too,
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
        """Finish a won election, or give the row back if the conns will not open.

        "Give the row back" is budgeted, not immediate: the first failed
        open keeps the row for its own trust window (the own-row arm's
        cheap route back must survive), and a failure past that window
        hands it back fenced (see :meth:`_hand_back_unassumable_lease`).
        """
        term = LeaderTerm(
            elected_at=elected_at,
            trusted_until=attempt_started
            + self._deps.settings.resolved_leader_lease
            - _LEADER_TRUST_MARGIN_SECS,
        )
        # Captured the moment the row is won, BEFORE the conn opens that
        # complete the assume: even a won-then-unassumable election leaves
        # this pod's name on the row, and the resigns that hand exactly
        # that row back, the trust-spent hand-back and the
        # teardown resign, fence on it.  Renewals never change
        # ``elected_at``, so the fence stays valid for the life of the
        # term.
        self._resign_fence = term
        # Courtesy only, and only ever attempted by the winner: an
        # old-release pod understands the lock and not the lease, so the
        # lease holder takes it to keep such a pod from electing itself
        # beside this one during a roll. A miss changes nothing about this
        # pod's leadership, the lease is the authority, but the miss must
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
            # as the leader_conn reopen path above, factory-built conns
            # surface provider (azure/hvac/botocore) and
            # asyncpg.InvalidPasswordError failures, which are transient and
            # must retry, not escape into the worker TaskGroup. CancelledError
            # (BaseException) is unaffected.
            await self._hand_back_unassumable_lease(reason="dedicated_conn_open_failed")
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
        if self._stopping():
            # The stop signal landed during the assume's conn opens:
            # this pod never acts on the won row. Hand it straight back
            # over the conn the elect just used (the fence was captured
            # at the top), before ``lead()`` can set the flag - the
            # lease is never held across the drain. The conns opened
            # here are idle and are closed by run()'s teardown.
            await self.resign()
            return False
        self._deps.lead(term)
        # The takeover half of the stale-auto-disable recovery (issue #460's
        # deploy window): an old leader's unmarked auto-disable can land
        # after every new pod has booted, so the boot pass has already run
        # when it appears. The new leader inherits the cron table at this
        # instant and re-applies the boot pass's ownership predicates, the
        # last moment the new release can revert the disable without a
        # restart. Best-effort by contract (the helper never raises for a
        # per-spec failure), and this call site additionally isolates the
        # pass from the assume: a recovery that cannot reach the pool must
        # cost the term nothing, the next assumption retries.
        if self._cron_specs:
            try:
                await revert_stale_auto_disables(self._deps, self._deps.settings, self._cron_specs)
            except Exception as exc:  # Why: the assume path's own error boundary above treats an exception as assume-failure and hands the lease back; a recovery pass that cannot run is a degraded maintenance plane, not a lost election.
                log.warning(
                    "cron-takeover-recovery-failed",
                    kind="cron_takeover_recovery_failed",
                    worker_id=str(self._worker_id),
                    error=repr(exc),
                    error_type=type(exc).__name__,
                )
        # A completed assume ends any won-but-unassumable episode: this
        # pod has just proven it can lead, so the next failure, whenever
        # it comes, starts a fresh episode with a fresh trust budget.
        self._unassumable_anchor = None
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
            # sets is_leader again, MaintenanceLeader.run's TaskGroup
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
            # re-parking on is_leader.wait(), this loop stops ticking through
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
                        # No `now` argument, the sweep's server-side
                        # predicate (scheduled_at <= clock_timestamp()) is
                        # the single arbiter.
                        rows = await self._backend.scheduled_to_pending()
                        if rows > 0:
                            channel = wake_channel(self._deps.settings.schema_name)
                            async with self._deps.dispatcher_pool.acquire(
                                timeout=self._deps.settings.dispatcher_command_timeout
                            ) as conn:
                                await conn.execute(WAKE_NOTIFY_SQL, channel)
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
                    # cut short, a sweep-timeouts increment. rows bound
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
                # surprises, 5 consecutive killed the worker via the
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
                    # OSError subclass, isinstance would also match raw
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
                    # or may not be dead, the next tick's transaction()
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
