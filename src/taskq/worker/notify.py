"""NOTIFY listener loop, health-check, and reconnect.

Wires the dedicated ``deps.notify_conn`` into the per-instance subscriber
registry on ``PostgresBackend`` via asyncpg's ``add_listener``.  Runs an
in-process ``SELECT 1`` health-check with bounded exponential-backoff
reconnect so the listener survives connection loss without crashing the
worker.

Two channels are subscribed per worker:
  - ``taskq_wake_{schema}``: enqueue wakeup (payload ignored)
  - ``taskq_events_{schema}``: fleet-wide worker events with JSON payload
    ``{"type": "<event>", ...}``
  - ``taskq_worker_{schema}_{worker_id}``: per-worker targeted events,
    same payload format, no filtering needed
"""

import asyncio
import contextlib
from collections.abc import Callable, Iterable
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.metrics import CallbackOptions, Observation

from taskq._dsn import dsn_host
from taskq._json import loads as json_loads
from taskq.backend.postgres import PostgresBackend
from taskq.constants import events_channel, wake_channel, worker_channel
from taskq.obs import get_logger, get_meter
from taskq.worker.deps import (
    WorkerDeps,
    _drain_tasks,  # pyright: ignore[reportPrivateUsage]  # Why: module-level drain-task set for fire-and-forget background close; accessed at module scope by both notify.py and deps.py.
)

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

_meter = get_meter()

# ── OTel instruments ────────────────────────────────────────────

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
_active_channels: dict[
    PostgresBackend, list[tuple[str, Callable[[asyncpg.Connection, int, str, str], None]]]
] = {}


def active_channels(
    backend: PostgresBackend,
) -> list[tuple[str, Callable[[asyncpg.Connection, int, str, str], None]]] | None:
    """Return the live channel/callback list for *backend*, or ``None``.

    Populated by :func:`notify_listener_loop` for the duration of its run;
    used by :func:`~taskq.worker.deps.reload_credentials` to re-issue
    LISTEN and re-register callbacks on a SIGHUP-triggered notify_conn swap
    without duplicating the channel-construction logic.
    """
    return _active_channels.get(backend)


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

# ── Internal helpers ────────────────────────────────────────────────────


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
    against this worker's ID; non-matching events are silently dropped —
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
        logger.debug(
            "cancel_event_received",
            kind="cancel_event_received",
            channel=channel,
            pid=pid,
            job_id=msg.get("job_id"),
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
        logger.debug(
            "worker_cancel_event_received",
            kind="worker_cancel_event_received",
            channel=channel,
            pid=pid,
            job_id=msg.get("job_id"),
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

    Uses ``deps.notify_conn_factory`` when set — the credential source (DSN
    closure or a user-supplied AAD/AWS/Vault factory) the connection was
    originally opened with — so a factory-backed deployment (which may have
    no DSN at all) reconnects through the same source rather than falling
    back to a stale ``pg_dsn_direct``. Falls back to the raw DSN only when
    ``notify_conn_factory`` is unset (caller-owned ``notify_conn`` — nothing
    TaskQ can rebuild; raises if called in that case).

    ``close_old`` additionally closes ``deps.notify_conn`` (the connection
    being replaced) in the background after the swap — used by
    :func:`~taskq.worker.deps.reload_credentials` for a SIGHUP-triggered
    hot reload, where the old connection is still live and must be drained
    rather than assumed already dead (the health-check reconnect path never
    passes this — the old connection is already closed by the time it calls
    in).

    A SIGHUP-triggered call can race a concurrent SIGTERM/SIGINT shutdown
    (the shutdown clears ``deps.notify_reconnect_fn`` and removes listeners
    once ``notify_listener_loop`` observes the shutdown event, which may
    happen mid-reconnect). Any exception from that race is caught and
    logged by :func:`~taskq.worker.deps.reload_credentials`'s caller — it
    does not crash the worker; the reload is simply reported as failed for
    ``notify_conn`` on an already-terminating worker.
    """
    old_conn = deps.notify_conn
    factory = deps.notify_conn_factory
    if factory is None:
        raise RuntimeError(
            "notify_conn has no factory to reconnect through (caller-owned "
            "connection) — TaskQ cannot rebuild it automatically."
        )
    new_conn = await factory()
    try:
        for channel, on_notify in channels:
            await new_conn.execute(f'LISTEN "{channel}"')
            await new_conn.add_listener(channel, on_notify)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime asyncpg accepts sync callbacks per asyncpg/connection.py:_process_notification
    except Exception:
        with contextlib.suppress(Exception):
            await new_conn.close()
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
        host=dsn_host(str(deps.settings.pg_dsn_direct)) if deps.settings.pg_dsn_direct else None,
    )
    if close_old and old_conn is not None and old_conn is not new_conn:

        async def _close_old() -> None:
            with contextlib.suppress(Exception):
                await old_conn.close()

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
    while not shutdown.is_set():
        await asyncio.sleep(float(deps.settings.notify_health_check_interval))
        if shutdown.is_set():
            return

        conn = deps.notify_conn
        if conn is None:
            return

        try:
            await conn.execute("SELECT 1")
        except (
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
                with contextlib.suppress(asyncpg.InterfaceError):
                    await conn.remove_listener(channel, on_notify)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime accepts sync callbacks
            with contextlib.suppress(
                Exception
            ):  # Why:  — close can raise on a half-dead socket and must be swallowed to enter the reconnect loop
                await conn.close()

            delay = float(deps.settings.notify_reconnect_backoff_initial)
            attempt = 0
            while not shutdown.is_set():
                try:
                    await reconnect_notify_conn(deps, backend, channels)
                    conn = deps.notify_conn
                    if conn is None:
                        break
                    break
                except (
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    asyncpg.InternalClientError,  # Why: same deviation — InternalClientError from a half-dead connection must enter the reconnect loop, not crash the worker.
                    asyncpg.AdminShutdownError,  # Why: same deviation — AdminShutdownError must enter the reconnect loop, not crash the worker
                    OSError,
                ) as exc:
                    attempt += 1
                    logger.warning(
                        "notify-reconnect-attempt",
                        kind="notify_reconnect_attempt",
                        attempt=attempt,
                        delay=delay,
                        error=repr(exc),
                        channels=[ch for ch, _ in channels],
                    )
                    await asyncio.sleep(delay)
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
    _active_channels[backend] = channels

    # Store a reconnect closure on deps so reload_credentials can trigger
    # a callback-aware reconnect (re-registers LISTEN + callbacks on the
    # new connection) without needing access to the channels itself.
    async def _reconnect_for_reload() -> None:
        await reconnect_notify_conn(deps, backend, channels, close_old=True)

    deps.notify_reconnect_fn = _reconnect_for_reload

    try:
        for channel, on_notify_callback in channels:
            await deps.notify_conn.add_listener(channel, on_notify_callback)  # pyright: ignore[reportArgumentType, reportOptionalMemberAccess]  # Why: stubs over-narrow callback type; notify_conn is non-None after open_worker_deps
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
        _active_channels.pop(backend, None)
        for channel, on_notify_callback in channels:
            with contextlib.suppress(asyncpg.InterfaceError, RuntimeError, AttributeError):
                await deps.notify_conn.remove_listener(channel, on_notify_callback)  # pyright: ignore[reportArgumentType, reportOptionalMemberAccess]  # Why: stubs over-narrow callback type; notify_conn is non-None after open_worker_deps
        logger.info(
            "notify-listener-stop",
            kind="notify_listener_stop",
            channels=[ch for ch, _ in channels],
        )
        _active_listeners.discard(backend)
