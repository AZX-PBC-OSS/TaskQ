"""The shared LISTEN pump: one body for the three surfaces that ride
the same reconnect loop — the HITL listener and the progress listener
(task-driven, fanning out to N subscribers) and the admin SSE feed (a
generator yielding payloads). Each grew its own copy of the reconnect
skeleton, the bounded raw queue, and the teardown; this module keeps
the one shared copy.

The teardown is the part the copies disagreed on. The admin's release
ran under :func:`taskq._shield.shield_with_retrieval` (a cancellation
delivered mid-cleanup detaches the cleanup, and the release happens),
while both listeners' releases were bare ``suppress(Exception)`` — a
cancellation landing on a cleanup step skipped ``pool.release`` and
stranded the dedicated connection against the pool cap, the exact shape
the admin's own docstring documented as unsafe. The shared
:func:`release_listen_connection` is the shielded form everywhere.

Layer rule: this module is a leaf of the workflows api layer. Its
imports are asyncio, asyncpg, the standard-library helpers, and three
taskq leaves — ``taskq._shield``, ``taskq.constants`` and
``taskq.obs``. It imports nothing from ``taskq.workflows``, so the
workflows package and the admin package can both depend on it without a
layering inversion. The admin keeps its own import law at its side: the
import is lazy (function scope), never at module scope.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Self

import asyncpg
import structlog

from taskq._shield import shield_with_retrieval
from taskq.constants import require_schema
from taskq.obs import get_logger

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

__all__ = [
    "DEFAULT_QUEUE_MAXSIZE",
    "_BroadcastListener",
    "make_notify_callback",
    "pump",
    "release_listen_connection",
]

#: The listener's own queue bound (the same tradeoff all three surfaces
#: made: a slow consumer drops the OLDEST event — the row is the truth,
#: the poll converges). Bounds the raw queue, every subscriber queue, and
#: the late-subscriber replay history alike.
DEFAULT_QUEUE_MAXSIZE: int = 1000

# The pool hands out BOTH faces (the proxy and the bare connection) —
# every consumer's own union.
type _Conn = asyncpg.Connection | PoolConnectionProxy

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


def make_notify_callback(
    q: asyncio.Queue[Any],
    *,
    channel: str = "",
    maxsize: int = DEFAULT_QUEUE_MAXSIZE,
    overflow_event: str = "listen-queue-overflow-drop-oldest",
    log: structlog.stdlib.BoundLogger = logger,
    carry_channel: bool = False,
    on_payload: Callable[[str, str], None] | None = None,
) -> Callable[[_Conn, int, str, str], None]:
    """The session callback's shared body (sync, loop-thread): skip the
    empty payloads, drop-oldest on overflow — a slow/disconnected SSE
    consumer must not accumulate NOTIFY payloads without limit, an
    unbounded queue is an unbounded-memory-growth risk, and admin-UI live
    updates are best-effort, so losing a stale event in favor of newer
    ones is the right tradeoff — the overflow logged ONCE per callback,
    then enqueue. *carry_channel* enqueues the ``(channel, payload)``
    pair (the listeners' decoders key on the channel); the bare payload
    otherwise (the admin feed's). *on_payload* runs per non-empty
    payload before the enqueue as ``on_payload(channel, payload)`` — a
    knock owed at the raw notify fires here even when the decode later
    drops the payload."""
    dropped_logged = False

    def _on_notify(c: _Conn, pid: int, ch: str, payload: str) -> None:
        nonlocal dropped_logged
        if not payload:
            return
        if on_payload is not None:
            on_payload(ch, payload)
        if q.full():
            with suppress(asyncio.QueueEmpty):
                q.get_nowait()
            if not dropped_logged:
                log.warning(
                    overflow_event,
                    channel=channel or ch,
                    maxsize=maxsize,
                )
                dropped_logged = True
        q.put_nowait((ch, payload) if carry_channel else payload)

    return _on_notify


async def release_listen_connection(
    conn: _Conn,
    pool: asyncpg.Pool,
    channels: Sequence[str],
    cb: Callable[[_Conn, int, str, str], None],
) -> None:
    """Tear down one LISTEN connection: remove_listener, UNLISTEN, release —
    for every channel in *channels*.

    Every step below is exception-suppressed, but ``suppress(Exception)``
    does NOT hold ``CancelledError`` (a BaseException since 3.8): a
    cancellation delivered while the cleanup runs dies at whatever step it
    lands on and skips ``pool.release`` -- and a generator abandoned inside
    its own finally is terminated and can never be closed again, so the
    connection strands against the pool cap for the life of the process
    (no GC revival: the generator is already closed). This cancellation is
    not hypothetical: the consumer bounds its close with
    ``asyncio.wait_for`` (sse.py), whose timeout cancels the close
    mid-flight -- exactly this shape.

    The whole cleanup therefore runs under a shield: if THIS await is
    cancelled, the cleanup detaches and runs to completion -- the release
    happens -- while the CancelledError still propagates to the closer.
    ``shield_with_retrieval`` retrieves the detached cleanup's outcome, so
    a late failure is a structured warning rather than asyncio's "Task
    exception was never retrieved" noise.

    Defense boundary: the detached cleanup itself is not re-shielded
    against a cancellation delivered INTO it -- no source reaches it in the
    shapes the consumers produce (the consumer's bound cancels the close
    task once; the stream's own cancellation stays behind the consumer's
    shield).
    """

    async def _cleanup() -> None:
        for channel in channels:
            with suppress(Exception):
                await conn.remove_listener(channel, cb)  # pyright: ignore[reportArgumentType]  # Why: carried over from the inline cleanup this replaces; stubs over-narrow callback type; runtime asyncpg accepts sync callbacks
            with suppress(Exception):
                await conn.execute(f'UNLISTEN "{channel}"')
        with suppress(Exception):
            await pool.release(conn)  # pyright: ignore[reportArgumentType]  # Why: carried over from the inline cleanup this replaces; asyncpg's release accepts both connection types it hands out

    await shield_with_retrieval(_cleanup())


async def pump(
    resolve_pool: Callable[[], asyncpg.Pool],
    channels: Sequence[str],
    on_payload: Callable[[str, str], None] | None = None,
    *,
    on_connect: Callable[[_Conn], Awaitable[None]] | None = None,
    keepalive: float = 30.0,
    backoff_initial: float = 1.0,
    backoff_max: float = 30.0,
    acquire_timeout: float = 5.0,
    queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
    overflow_event: str = "listen-queue-overflow-drop-oldest",
    overflow_log: structlog.stdlib.BoundLogger = logger,
    closed_message: str = "the LISTEN connection was closed underneath the pump",
    closing: Callable[[], bool] | None = None,
    on_reconnect: Callable[[Exception, float], None] | None = None,
) -> AsyncGenerator[tuple[str, str] | None, None]:
    """The reconnect loop every LISTEN tail rides: acquire a dedicated
    connection (LISTEN is session-scoped — a pooled round-robin would
    UNLISTEN on every release), LISTEN every channel in order, run
    *on_connect* (the backfill — LISTEN FIRST, THEN the snapshot: the
    order IS the zero-window construction), then tail the bounded raw
    queue, yielding each ``(channel, payload)`` pair as it arrives.

    ``None`` yields are the stream's own ticks, not payloads: a keepalive
    when nothing arrived within *keepalive*, and one before every
    reconnect sleep. A keepalive tick re-checks the connection — a closed
    session (a credential rotation terminates the old pool's connections,
    and a terminated LISTEN connection raises nothing on its own; the
    queue just goes quiet) raises :class:`ConnectionError` into the
    reconnect arm instead of yielding quiet keepalives forever.

    Every failure in the loop — a lost session, a pool that cannot
    connect, a bouncer rejecting the session-scoped LISTEN — is logged
    through *on_reconnect* when given, then the backoff doubles up to
    *backoff_max* and the loop reconnects: the feed degrades to
    keepalives only until a connect succeeds, never to silence.  When
    *closing* is given and returns True, the loop ends instead of
    reconnecting (the task-driven listeners' stop flag).

    *resolve_pool* is called on every (re)connect rather than a pool being
    captured once: the admin pool is replaced by a credential rotation
    (``taskq ui serve``), and a stream that kept reconnecting to the pool
    it started on would be reconnecting to a closed one for the rest of
    its life. The teardown is :func:`release_listen_connection` — the
    SHIELDED form — in the generator's ``finally``, so every exit
    (consumer close, cancellation mid-flight, a failed feed) releases the
    dedicated connection.

    *on_payload* is called per non-empty notify BEFORE the enqueue (the
    progress listener's knock fires here — a knock is owed even when the
    decode then drops the payload as another run's).
    """
    backoff = backoff_initial
    while closing is None or not closing():
        queue: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue(maxsize=queue_maxsize)
        conn: _Conn | None = None
        pool: asyncpg.Pool | None = None
        cb = make_notify_callback(
            queue,
            maxsize=queue_maxsize,
            overflow_event=overflow_event,
            log=overflow_log,
            carry_channel=True,
            on_payload=on_payload,
        )
        try:
            pool = resolve_pool()
            conn = await asyncio.wait_for(pool.acquire(), timeout=acquire_timeout)
            for channel in channels:
                await conn.execute(f'LISTEN "{channel}"')
                await conn.add_listener(channel, cb)  # pyright: ignore[reportArgumentType]  # Why: stubs over-narrow callback type; runtime asyncpg accepts sync callbacks
            if on_connect is not None:
                await on_connect(conn)
            backoff = backoff_initial
            while closing is None or not closing():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=keepalive)
                except TimeoutError:
                    if conn.is_closed():
                        raise ConnectionError(closed_message) from None
                    yield None
                    continue
                if item is None:
                    return
                yield item
        except Exception as exc:
            if closing is not None and closing():
                return
            if on_reconnect is not None:
                on_reconnect(exc, backoff)
            yield None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, backoff_max)
        finally:
            if conn is not None and pool is not None:
                await release_listen_connection(conn, pool, channels, cb)


class _FanOut[EventT]:
    """The fan-out's mechanics: per-SUBSCRIBER queues (each its own
    sentinel on stop), a bounded history each late subscriber replays
    first (the buffer-then-consume behavior — a backfill announced before
    the subscribe is still delivered), and the end-of-stream sentinels.

    The fan-out, not a partition: every subscriber gets its OWN queue —
    a shared queue would PARTITION the stream, each consumer stealing
    the other's events, and a single sentinel on stop would strand all
    but one waiter. Every queue is drop-oldest on overflow: the writer
    NEVER blocks; the NEWEST events survive; a slow consumer loses
    OBSERVABILITY first, correctness never.

    :meth:`subscribe` is ATOMIC by construction: no await between the
    history copy and the registry insert, so no event is both replayed
    AND fanned out, and none is lost in between (the loop thread
    schedules nothing else mid-function).
    """

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._subs: dict[int, asyncio.Queue[EventT | None]] = {}
        self._history: deque[EventT] = deque(maxlen=maxsize)
        #: The stop flag: a subscriber after stop ends immediately (its own
        #: sentinel), and the pump loop stops reconnecting.
        self.closing = False

    def subscribe(self) -> asyncio.Queue[EventT | None]:
        """Register ONE subscriber: its own bounded queue, seeded with the
        bounded event history."""
        queue: asyncio.Queue[EventT | None] = asyncio.Queue(maxsize=self._maxsize)
        if self.closing:
            queue.put_nowait(None)  # a subscriber after stop ends immediately
            return queue
        for event in self._history:
            queue.put_nowait(event)
        self._subs[id(queue)] = queue
        return queue

    def unsubscribe(self, queue: asyncio.Queue[EventT | None]) -> None:
        with suppress(KeyError):
            del self._subs[id(queue)]

    def emit(self, event: EventT) -> None:
        """The event to EVERY subscriber (the fan-out — never a
        partition), plus the bounded history (each late subscriber
        replays it first). Each queue is drop-oldest on overflow — the
        same bound as the raw leg."""
        self._history.append(event)
        for queue in list(self._subs.values()):
            if queue.full():
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    def end(self) -> None:
        """End EVERY subscriber with its OWN sentinel, then clear the
        registry and the history (stop's second half)."""
        for queue in list(self._subs.values()):
            with suppress(asyncio.QueueFull):
                queue.put_nowait(None)  # each subscriber's OWN sentinel
        self._subs.clear()
        self._history.clear()


class _BroadcastListener[EventT]:
    """The task-driven listener's shared body: ONE dedicated connection,
    LISTEN → BACKFILL → TAIL on the :func:`pump` generator (automatic
    reconnect with the admin feed's backoff discipline), the bounded raw
    queue only the pump reads, and the :class:`_FanOut` to N concurrent
    subscribers.

    The subclass carries the per-listener face: the typed public surfaces
    (their names and docstrings are the public contract — the ceremony-free
    ``__aiter__`` form, the named form, the SSE face's ``frames()``), the
    channel set (:meth:`_channels`), the snapshot (:meth:`_backfill`), the
    raw knock's decode (:meth:`_decode`), and the log/event names (the
    class attributes).

    The teardown is SHIELDED (the pump's own ``finally`` through
    :func:`release_listen_connection`): a cancellation delivered while the
    pump's cleanup releases the LISTEN connection must not strand it
    against the pool cap — the admin feed's release discipline, now the
    shared one.
    """

    # ── the per-listener face (the subclass states these) ────────────
    _log: structlog.stdlib.BoundLogger
    _task_name: str
    _reconnect_event: str
    _overflow_event: str
    _closed_message: str

    def __init__(
        self,
        pool: asyncpg.Pool,
        schema: str,
        *,
        keepalive_interval: float = 30.0,
        backoff_initial: float = 1.0,
        backoff_max: float = 30.0,
        acquire_timeout: float = 5.0,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._keepalive = keepalive_interval
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._acquire_timeout = acquire_timeout
        self._fanout: _FanOut[EventT] = _FanOut(self._queue_maxsize())
        self._pump: asyncio.Task[None] | None = None

    # ── the subclass hooks ───────────────────────────────────────────

    def _channels(self) -> Sequence[str]:
        """The LISTEN set (the pump LISTENs them in this order, then
        backfills)."""
        raise NotImplementedError

    async def _backfill(self, conn: _Conn) -> None:
        """The snapshot (the zero-window law's second half): run after
        every LISTEN, on every (re)connect."""
        raise NotImplementedError

    def _decode(self, channel: str, payload: str) -> EventT | None:
        """The raw knock → the typed event (``None``: not this schema's,
        or a shape the decoder refuses)."""
        raise NotImplementedError

    def _reconnect_fields(self) -> dict[str, object]:
        """The reconnect warning's extra fields (the progress listener's
        run scope)."""
        return {}

    def _note_activity(self) -> None:
        """The per-event hook (the progress listener's knock): called on
        every non-empty raw notify and on every emit."""
        return None

    def _queue_maxsize(self) -> int:
        """The queue bound (the module global, so a test's monkeypatch
        binds every queue the listener creates)."""
        return DEFAULT_QUEUE_MAXSIZE

    # ── the lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the pump task (idempotent)."""
        if self._pump is not None:
            return
        require_schema(self._schema)
        self._fanout.closing = False
        self._pump = asyncio.create_task(self._pump_loop(), name=self._task_name)

    async def stop(self) -> None:
        """Stop the pump and release the dedicated connection, then end
        EVERY subscriber with its OWN sentinel (the teardown is SHIELDED: a
        cancellation delivered while the pump's own finally releases the
        LISTEN connection must not strand it against the pool cap)."""
        self._fanout.closing = True
        task, self._pump = self._pump, None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await shield_with_retrieval(task)
        self._fanout.end()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ── the subscriber registry (the fan-out's mechanics) ────────────

    def _subscribe(self) -> asyncio.Queue[EventT | None]:
        return self._fanout.subscribe()

    def _unsubscribe(self, queue: asyncio.Queue[EventT | None]) -> None:
        self._fanout.unsubscribe(queue)

    async def _iterate(self, keepalive: float | None) -> AsyncGenerator[EventT | None, None]:
        if self._pump is None:
            await self.start()
        queue = self._subscribe()
        try:
            while True:
                try:
                    if keepalive is None:
                        item = await queue.get()
                    else:
                        item = await asyncio.wait_for(queue.get(), timeout=keepalive)
                except TimeoutError:
                    yield None
                    continue
                if item is None:
                    return
                yield item
        finally:
            self._unsubscribe(queue)

    # ── the pump (connect → LISTEN → backfill → tail, with reconnect) ──

    def _log_reconnect(self, exc: Exception, backoff: float) -> None:
        self._log.warning(
            self._reconnect_event,
            schema=self._schema,
            **self._reconnect_fields(),
            error_type=type(exc).__name__,
            error=str(exc),
            backoff=backoff,
        )

    async def _pump_loop(self) -> None:
        try:
            async for item in pump(
                lambda: self._pool,
                self._channels(),
                lambda channel, payload: self._note_activity(),
                on_connect=self._backfill,
                keepalive=self._keepalive,
                backoff_initial=self._backoff_initial,
                backoff_max=self._backoff_max,
                acquire_timeout=self._acquire_timeout,
                queue_maxsize=self._queue_maxsize(),
                overflow_event=self._overflow_event,
                overflow_log=self._log,
                closed_message=self._closed_message,
                closing=lambda: self._fanout.closing,
                on_reconnect=self._log_reconnect,
            ):
                if item is None:
                    continue
                channel, payload = item
                event = self._decode(channel, payload)
                if event is not None:
                    self._emit(event)
        except asyncio.CancelledError:
            # The stop request: the pump generator's finally has already
            # released the dedicated connection (shielded). The task ends
            # clean, exactly as the pre-pump bodies did.
            return

    def _emit(self, event: EventT) -> None:
        """The typed event to EVERY subscriber (the fan-out — never a
        partition), plus the bounded history (each late subscriber
        replays it first)."""
        self._note_activity()
        self._fanout.emit(event)
