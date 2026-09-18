"""NOTIFY listener loop, health-check, and reconnect.

Wires the dedicated ``deps.notify_conn`` into the per-instance subscriber
registry on ``PostgresBackend`` via asyncpg's ``add_listener``.  Runs an
in-process ``SELECT 1`` health-check with bounded exponential-backoff
reconnect so the listener survives connection loss without crashing the
worker.

Three channels are subscribed per worker (names from ``taskq.constants``,
each carrying the schema's fixed-width tag rather than the schema name):
  - ``wake_channel(schema)``: enqueue wakeup (payload ignored)
  - ``events_channel(schema)``: fleet-wide worker events with JSON payload
    ``{"type": "<event>", ...}``
  - ``worker_channel(schema, worker_id)``: per-worker targeted events,
    same payload format, no filtering needed

The reconnect backoff carries multiplicative jitter (±25% around the
doubled delay). Without it, a large worker fleet that loses PG
simultaneously (failover, container restart) retries in lockstep: every
worker sleeps identical delays and lands identical simultaneous connect
waves, re-synchronizing on each doubling and at the 30s cap. The jitter
desynchronizes those retries the same way the deadlock backoff in
``taskq.backend._cancel_bulk`` does.
"""

import asyncio
import contextlib
import logging
import random
from collections.abc import Callable, Iterable
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq._close import CLOSE_TIMEOUT_SECS, close_conn_bounded
from taskq._dsn import dsn_host
from taskq._json import loads as json_loads
from taskq.backend.postgres import PostgresBackend
from taskq.constants import events_channel, wake_channel, worker_channel
from taskq.obs import get_logger, get_meter
from taskq.worker.deps import (
    WorkerDeps,
    _drain_tasks,  # pyright: ignore[reportPrivateUsage]  # Why: module-level drain-task set for fire-and-forget background close; accessed at module scope by both notify.py and deps.py.
    apply_keepalive_to_conn,
)

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()

# -- OTel instruments --------------------------------------------

_notify_received_counter = _meter.create_counter(
    name="taskq.notify.received",
    description="Total NOTIFY callbacks delivered from asyncpg.",
)
_notify_reconnects_counter = _meter.create_counter(
    name="taskq.notify.reconnects",
    description="Total successful listener reconnects.",
)
_cancel_notify_received_counter = _meter.create_counter(
    name="taskq.notify.cancel_received",
    description="Total cancel NOTIFY callbacks delivered to this worker.",
)

_active_listeners: set[PostgresBackend] = set()
_connected_lookup: dict[PostgresBackend, bool] = {}


def _observe_connected(options: CallbackOptions) -> Iterable[Observation]:
    for backend in _active_listeners:
        yield Observation(
            1 if _connected_lookup.get(backend, False) else 0,
            {"schema": backend._schema_name},  # pyright: ignore[reportPrivateUsage]  # Why: OTel gauge callback reads schema_name from the backend instance; the field is private by convention but accessible from module scope by design.
        )


_connected_gauge = _meter.create_observable_gauge(
    name="taskq.notify.connected",
    description="1 if the NOTIFY listener connection is healthy, 0 otherwise.",
    callbacks=[_observe_connected],
)

# -- Internal helpers ----------------------------------------------------


def _make_callback(
    backend: PostgresBackend,
) -> Callable[[asyncpg.Connection, int, str, str], None]:
    """Return a sync closure invoked by asyncpg on each NOTIFY.

    The closure captures *backend*, takes a snapshot of
    ``backend._wake_subscribers``, and calls ``event.set()`` on each.
    The closure ignores ``payload`` entirely.
    """

    def _on_notify(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        _notify_received_counter.add(1)
        for event in list(backend._wake_subscribers):  # pyright: ignore[reportPrivateUsage]  # Why: snapshot iteration per ; safe because event.set() is idempotent
            event.set()
        # Guarded: every enqueue in the schema wakes every listener, and a
        # structlog call runs the full processor chain before the stdlib
        # level check drops the record, the level check here is the only
        # per-notification cost at INFO.
        if logger.is_enabled_for(logging.DEBUG):
            logger.debug(
                "notify-received",
                kind="notify_received",
                channel=channel,
                pid=pid,
            )

    return _on_notify


def _make_events_callback(
    backend: PostgresBackend,
    worker_id: UUID,
) -> Callable[[asyncpg.Connection, int, str, str], None]:
    """Return a sync closure for the fleet-wide events channel.

    Payload is a JSON object with a ``"type"`` discriminator.  Currently
    only ``"cancel"`` is handled.  The ``"worker_id"`` field is checked
    against this worker's ID; non-matching events are silently dropped -
    the heartbeat poll remains authoritative.  Unparseable payloads (e.g.
    empty reconnect-triggers) are silently ignored.
    """
    worker_id_str = str(worker_id)

    def _on_event(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        _notify_received_counter.add(1)
        if not payload:
            return
        try:
            msg: dict[str, object] = json_loads(payload)
        except Exception:
            logger.debug("notify-payload-parse-failed", channel=channel, payload=payload[:200])
            return
        if msg.get("type") != "cancel":
            return
        if str(msg.get("worker_id", "")) != worker_id_str:
            return
        _cancel_notify_received_counter.add(1)
        for event in list(backend._cancel_subscribers):  # pyright: ignore[reportPrivateUsage]  # Why: snapshot iteration; event.set() is idempotent
            event.set()
        raw_job_id = msg.get("job_id")
        logger.debug(
            "cancel_event_received",
            kind="cancel_event_received",
            channel=channel,
            pid=pid,
            job_id=str(raw_job_id) if raw_job_id is not None else None,
        )

    return _on_event


def _make_worker_events_callback(
    backend: PostgresBackend,
) -> Callable[[asyncpg.Connection, int, str, str], None]:
    """Return a sync closure for the per-worker events channel.

    This channel is subscribed by only one worker, so no worker_id
    filtering is needed.  The ``"type"`` discriminator is still parsed so
    future event types can be routed here without a channel rename.
    """

    def _on_worker_event(
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        _notify_received_counter.add(1)
        if not payload:
            return
        try:
            msg: dict[str, object] = json_loads(payload)
        except Exception:
            logger.debug("notify-payload-parse-failed", channel=channel, payload=payload[:200])
            return
        if msg.get("type") != "cancel":
            return
        _cancel_notify_received_counter.add(1)
        for event in list(backend._cancel_subscribers):  # pyright: ignore[reportPrivateUsage]  # Why: snapshot iteration; event.set() is idempotent
            event.set()
        raw_job_id = msg.get("job_id")
        logger.debug(
            "worker_cancel_event_received",
            kind="worker_cancel_event_received",
            channel=channel,
            pid=pid,
            job_id=str(raw_job_id) if raw_job_id is not None else None,
        )

    return _on_worker_event


async def reconnect_notify_conn(
    deps: WorkerDeps,
    backend: PostgresBackend,
    channels: list[tuple[str, Callable[[asyncpg.Connection, int, str, str], None]]],
    *,
    close_old: bool = False,
) -> None:
    """Rebuild ``deps.notify_conn``, re-issue LISTEN, and re-register callbacks.

    Uses ``deps.notify_conn_factory`` when set - the credential source (DSN
    closure or a user-supplied AAD/AWS/Vault factory) the connection was
    originally opened with - so a factory-backed deployment (which may have
    no DSN at all) reconnects through the same source rather than falling
    back to a stale ``pg_dsn_direct``. Falls back to the raw DSN only when
    ``notify_conn_factory`` is unset (caller-owned ``notify_conn`` - nothing
    TaskQ can rebuild; raises if called in that case).

    ``close_old`` additionally closes ``deps.notify_conn`` (the connection
    being replaced) in the background after the swap - used by
    :func:`~taskq.worker.deps.reload_credentials` for a SIGHUP-triggered
    hot reload, where the old connection is still live and must be drained
    rather than assumed already dead (the health-check reconnect path never
    passes this - the old connection is already closed by the time it calls
    in).

    The factory call is bounded by ``settings.reload_factory_timeout`` ,
    the same bound the SIGHUP reload path (deps.reload_credentials) and
    the bootstrap slot-pool open use, and each post-factory ``LISTEN``
    execute is bounded by ``settings.notify_listener_setup_timeout``,
    the same bound the ``add_listener`` beside it and the initial
    listener setup use, so neither a hung credential provider/TCP
    connect nor a rebuilt connection that completes the handshake and
    then black-holes on LISTEN can park the reconnect (and with it
    ``notify_reconnect_lock``). A timeout is the retry loop's ordinary
    failure path: logged as a reconnect attempt, backoff, retry.

    A SIGHUP-triggered call can race a concurrent SIGTERM/SIGINT shutdown
    (the shutdown clears ``deps.notify_reconnect_fn`` and removes listeners
    once ``notify_listener_loop`` observes the shutdown event, which may
    happen mid-reconnect). Any exception from that race is caught and
    logged by :func:`~taskq.worker.deps.reload_credentials`'s caller - it
    does not crash the worker; the reload is simply reported as failed for
    ``notify_conn`` on an already-terminating worker.
    """
    # Serialized on deps.notify_reconnect_lock: the health-check loop and
    # reload_credentials (via deps.notify_reconnect_fn) can both trigger a
    # reconnect - without mutual exclusion both build a new conn, last
    # writer wins, and the loser's LISTEN-registered conn leaks.
    async with deps.notify_reconnect_lock:
        old_conn = deps.notify_conn
        factory = deps.notify_conn_factory
        if factory is None:
            raise RuntimeError(
                "notify_conn has no factory to reconnect through (caller-owned "
                "connection) - TaskQ cannot rebuild it automatically."
            )
        # Why bounded: a hung credential provider or TCP connect parked the
        # health-check reconnect loop here while holding
        # notify_reconnect_lock. reload_factory_timeout is the SAME
        # bound the SIGHUP reload path applies to every factory call
        # (deps.reload_credentials), not a second mechanism, and its
        # exhaustion here behaves like any factory failure: the retry
        # loop logs the attempt, backs off, and retries.
        new_conn = await asyncio.wait_for(
            factory(),
            timeout=float(deps.settings.reload_factory_timeout),
        )
        # The DSN path gets TCP keepalive via open_dedicated_conn; a conn
        # rebuilt through the factory must get the same policy - the worker
        # owns this policy, not the user's factory. Safe on fakes (returns
        # False when no socket is available).
        apply_keepalive_to_conn(new_conn, label="notify")
        try:
            for channel, on_notify in channels:
                # Why bounded: a rebuilt conn can complete the factory
                # handshake and still black-hole on the LISTEN execute ,
                # the same black-hole shape the health-check query bound
                # closes, parking
                # the reconnect loop (and notify_reconnect_lock) past
                # every other bound. The SAME
                # notify_listener_setup_timeout that bounds the
                # add_listener beside it applies here (not a second
                # mechanism); exhaustion is the retry loop's ordinary
                # failure path: logged as a reconnect attempt, backoff,
                # retry.
                await asyncio.wait_for(
                    new_conn.execute(f'LISTEN "{channel}"'),
                    timeout=float(deps.settings.notify_listener_setup_timeout),
                )
                await asyncio.wait_for(
                    new_conn.add_listener(channel, on_notify),  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime asyncpg accepts sync callbacks per asyncpg/connection.py:_process_notification
                    timeout=float(deps.settings.notify_listener_setup_timeout),
                )
        except BaseException:
            # BaseException: CancelledError (e.g. reload's factory_timeout
            # firing mid-LISTEN-setup) must also close the freshly-built
            # conn - otherwise it leaks until GC with a ResourceWarning.
            # Why bounded: the conn being closed here failed LISTEN setup,
            # so it may already be half-dead; an unbounded close could
            # stall the reconnect loop. The helper never raises
            # (except CancelledError, which must propagate), so the
            # original exception is always re-raised below.
            await close_conn_bounded(new_conn, "notify", CLOSE_TIMEOUT_SECS, mid_run=True)
            raise
        deps.notify_conn = new_conn
        # Simulate a wake notify so any pending subscribers are unblocked after reconnect.
        if channels:
            wake_ch, wake_cb = channels[0]
            wake_cb(new_conn, 0, wake_ch, "")
        _notify_reconnects_counter.add(1)
        _connected_lookup[backend] = True
        logger.info(
            "notify-listener-connect",
            kind="notify_listener_connect",
            channels=[ch for ch, _ in channels],
            host=dsn_host(str(deps.settings.pg_dsn_direct))
            if deps.settings.pg_dsn_direct
            else None,
        )
        if close_old and old_conn is not None and old_conn is not new_conn:

            async def _close_old() -> None:
                # Why bounded: this is the same dead-PG hang class as
                # deps._drain_old_conn - an unbounded close would
                # leak the background task forever (never completing,
                # never collected). The helper bounds the wait, terminates
                # on timeout, and never raises, subsuming the old
                # suppress(Exception).
                await close_conn_bounded(old_conn, "notify", CLOSE_TIMEOUT_SECS, mid_run=True)

            # Store the reference so the task is not garbage-collected before
            # completing. The set is module-level (single event loop, async-safe).
            _t = asyncio.create_task(_close_old())
            _drain_tasks.add(_t)
            _t.add_done_callback(_drain_tasks.discard)


async def _health_check_loop(
    deps: WorkerDeps,
    backend: PostgresBackend,
    shutdown: asyncio.Event,
    channels: list[tuple[str, Callable[[asyncpg.Connection, int, str, str], None]]],
) -> None:
    # Deliberately does NOT tick LoopLiveness. This loop is exempt from
    # watchdog detector 2 (see _watchdog's module docstring), for three
    # independent reasons, a stale registration force-exits the worker:
    #   1. It returns early on legitimate paths (conn dropped, and the
    #      notify-listener-disabled poll-fallback that keeps the worker
    #      running by design), which would leave the entry to go stale.
    #   2. The reconnect retry loop backs off to 30s between attempts
    #      without ticking, so a small health-check interval would put the
    #      budget at the floor and trip during exactly the PG outage the
    #      reconnect logic exists to survive.
    #   3. Its cadence tracks IO, not progress.
    # Detectors 1, 3 and 4 cover this loop instead.
    while not shutdown.is_set():
        await asyncio.sleep(float(deps.settings.notify_health_check_interval))
        if shutdown.is_set():
            return

        conn = deps.notify_conn
        if conn is None:
            return

        try:
            # Why bounded: a hand-rolled factory or caller-owned conn
            # carries no command_timeout (the DSN path's
            # dispatcher_command_timeout already bounds this probe there),
            # and this loop is deliberately exempt from the watchdog's
            # stale-loop detector, an unbounded probe on a wedged conn
            # parks the health check forever with nothing to recover it.
            # notify_listener_setup_timeout is the SAME bound this loop
            # family already applies to every bounded execute/registration
            # (the LISTEN at setup and reconnect), not a second
            # mechanism; exhaustion is treated like any dead conn: the
            # reconnect path runs.
            await asyncio.wait_for(
                conn.execute("SELECT 1"),
                timeout=float(deps.settings.notify_listener_setup_timeout),
            )
        except (
            TimeoutError,
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            asyncpg.InternalClientError,  # Why: container stop can leave the protocol in an inconsistent state (e.g. "cannot switch to state 15"); must trigger reconnect, not crash the worker.
            asyncpg.AdminShutdownError,  # Why: graceful PG shutdown raises AdminShutdownError, not PostgresConnectionError; without this the listener crashes the worker.
            OSError,
        ) as exc:
            _connected_lookup[backend] = False
            logger.warning(
                "notify-conn-error",
                kind="notify_conn_error",
                error=repr(exc),
                channels=[ch for ch, _ in channels],
            )
            for channel, on_notify in channels:
                # Why bounded + TimeoutError suppressed: the UNLISTEN is a
                # best-effort network round trip on a conn already judged
                # dead, unbounded, a wedged conn parks the health check's
                # reconnect path forever. notify_listener_setup_timeout is
                # the loop family's existing execute bound; a timeout is
                # another suppressed failure, not a crash.
                with contextlib.suppress(asyncpg.InterfaceError, TimeoutError):
                    await asyncio.wait_for(
                        conn.remove_listener(channel, on_notify),  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime accepts sync callbacks
                        timeout=float(deps.settings.notify_listener_setup_timeout),
                    )
            if deps.owns_notify_conn:
                # Ownership contract (connections.py): TaskQ never closes
                # caller-owned resources - the caller owns its lifecycle even
                # on the error path. The remove_listener calls above are kept
                # unconditionally: harmless on a caller's conn, needed before
                # a rebuild.
                # Why bounded: close can raise on a half-dead socket and must
                # be swallowed to enter the reconnect loop - and a dead PG
                # can block close() indefinitely, which would stall the
                # health-check loop before reconnect even starts. The
                # helper bounds the wait, terminates on timeout, and never
                # raises, subsuming the old suppress(Exception).
                await close_conn_bounded(conn, "notify", CLOSE_TIMEOUT_SECS, mid_run=True)

            delay = float(deps.settings.notify_reconnect_backoff_initial)
            attempt = 0
            while not shutdown.is_set():
                try:
                    await reconnect_notify_conn(deps, backend, channels)
                    conn = deps.notify_conn
                    if conn is None:
                        break
                    break
                except Exception as exc:  # Why: the retry loop must survive ANY factory/reconnect failure - a credential provider raises non-asyncpg errors (azure ClientAuthenticationError, hvac VaultError, botocore ClientError) and a rejected fresh token raises asyncpg.InvalidPasswordError, which is a PostgresError, NOT a PostgresConnectionError. Catching only asyncpg connection errors here would crash the worker during exactly the IdP outage this loop exists to survive. asyncio.CancelledError is BaseException (3.8+), so shutdown cancellation still propagates.
                    if isinstance(exc, RuntimeError) and deps.notify_conn_factory is None:
                        # Caller-owned notify_conn dropped and there is no
                        # factory to rebuild through - retrying could never
                        # succeed. Disable the listener; poll-based dispatch
                        # remains as the fallback.
                        logger.warning(
                            "notify-listener-disabled",
                            kind="notify_listener_disabled",
                            reason="caller-owned notify_conn dropped and no "
                            "notify_conn_factory to rebuild through; falling "
                            "back to poll-based dispatch",
                            channels=[ch for ch, _ in channels],
                        )
                        return
                    attempt += 1
                    # Multiplicative jitter (±25%) applied AFTER the
                    # exponential doubling (the pristine base doubles below,
                    # unpolluted by previous jitter) and BEFORE the sleep, so
                    # the logged delay is the delay actually slept. Applied
                    # on every retry including the first: without it a fleet
                    # that lost PG in the same instant (failover) retries in
                    # lockstep waves, identical delays re-synchronize every
                    # attempt, most visibly at the 30s cap where 100 workers
                    # reconnect as one, exactly the storm the deadlock
                    # backoff's jitter (taskq.backend._cancel_bulk) prevents
                    # for batch retries.
                    slept = delay * random.uniform(0.75, 1.25)  # noqa: S311  # Why: uniform is for reconnect-timing jitter, not cryptography; same non-crypto use as _cancel_bulk's deadlock backoff.
                    logger.warning(
                        "notify-reconnect-attempt",
                        kind="notify_reconnect_attempt",
                        attempt=attempt,
                        delay=slept,
                        error=repr(exc),
                        error_type=type(exc).__name__,
                        channels=[ch for ch, _ in channels],
                    )
                    await asyncio.sleep(slept)
                    delay = min(delay * 2, 30.0)


async def notify_listener_loop(
    deps: WorkerDeps,
    backend: PostgresBackend,
    shutdown: asyncio.Event,
    worker_id: UUID,
) -> None:
    schema = deps.settings.schema_name
    worker_id_str = str(worker_id)
    channels: list[tuple[str, Callable[[asyncpg.Connection, int, str, str], None]]] = [
        (wake_channel(schema), _make_callback(backend)),
        (events_channel(schema), _make_events_callback(backend, worker_id)),
        (worker_channel(schema, worker_id_str), _make_worker_events_callback(backend)),
    ]

    _active_listeners.add(backend)
    _connected_lookup[backend] = False

    # Store a reconnect closure on deps so reload_credentials can trigger
    # a callback-aware reconnect (re-registers LISTEN + callbacks on the
    # new connection) without needing access to the channels itself.
    async def _reconnect_for_reload() -> None:
        await reconnect_notify_conn(deps, backend, channels, close_old=True)

    if deps.notify_conn_factory is not None:
        # Only register when there is a factory to rebuild through - with a
        # caller-owned notify_conn the closure could only raise RuntimeError
        # if invoked. reload_credentials already skips factory-less notify.
        deps.notify_reconnect_fn = _reconnect_for_reload

    try:
        try:
            for channel, on_notify_callback in channels:
                await asyncio.wait_for(
                    deps.notify_conn.add_listener(channel, on_notify_callback),  # pyright: ignore[reportArgumentType, reportOptionalMemberAccess]  # Why: stubs over-narrow callback type; notify_conn is non-None after open_worker_deps
                    timeout=float(deps.settings.notify_listener_setup_timeout),
                )
        except TimeoutError:
            logger.warning(
                "notify-listener-setup-timeout",
                kind="notify_listener_setup_timeout",
                timeout=float(deps.settings.notify_listener_setup_timeout),
                channels=[ch for ch, _ in channels],
            )
            if deps.owns_notify_conn:
                conn = deps.notify_conn
                if conn is not None:
                    await close_conn_bounded(conn, "notify", CLOSE_TIMEOUT_SECS, mid_run=True)
            raise
        _connected_lookup[backend] = True

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                _health_check_loop(deps, backend, shutdown, channels),
                name="notify.health_check",
            )
            await shutdown.wait()
    finally:
        deps.notify_reconnect_fn = None
        _connected_lookup[backend] = False
        _connected_lookup.pop(backend, None)
        for channel, on_notify_callback in channels:
            # Why bounded + TimeoutError suppressed: the teardown UNLISTEN
            # is a best-effort network round trip, unbounded, a wedged
            # notify conn stalls the listener's shutdown past every
            # shutdown budget and the ShutdownWatchdog force-exits the
            # process for it. notify_listener_setup_timeout is the loop
            # family's existing execute bound; a timeout is another
            # suppressed failure, not a crash.
            with contextlib.suppress(
                asyncpg.InterfaceError, RuntimeError, AttributeError, TimeoutError
            ):
                await asyncio.wait_for(
                    deps.notify_conn.remove_listener(channel, on_notify_callback),  # pyright: ignore[reportArgumentType, reportOptionalMemberAccess]  # Why: stubs over-narrow callback type; notify_conn is non-None after open_worker_deps
                    timeout=float(deps.settings.notify_listener_setup_timeout),
                )
        logger.info(
            "notify-listener-stop",
            kind="notify_listener_stop",
            channels=[ch for ch, _ in channels],
        )
        _active_listeners.discard(backend)
