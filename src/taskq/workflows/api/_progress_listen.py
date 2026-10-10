"""THE PROGRESS LISTENER (the consumer-face lane's CURE 3 — the
HitlListener's sibling): the typed, public face over the progress push —
ONE dedicated connection, LISTEN → BACKFILL → TAIL — fanning out to N
concurrent subscribers.

THE ZERO-WINDOW CONSTRUCTION (the #316 mid-stream re-check pattern, the
T26 broadcast's proven shape): the connection LISTENS FIRST, THEN the
backfill snapshot reads the run's STATE-channel rows. Any emission
visible to the snapshot either knocked BEFORE we listened (no knock will
ever come — the snapshot is the only witness) or AFTER (its knock is
deduped against the snapshot by the node's ``last_seq``). A missed event
is unrepresentable by construction — not by luck, not by polling.

THE ROW IS STILL THE TRUTH: every event is a POINTER (the flow, the
node, the seq — never the payload's content; the redact law holds at
the knock). A consumer that misses everything still converges by the
seq-cursor read (:func:`taskq.workflows._progress_read.progress_sse_face`);
the listener buys LATENCY, never correctness.

THE FAN-OUT, NOT A PARTITION (P7's law, carried): every subscriber —
``async for event in listener``, ``updates()``, ``frames()`` — gets its
OWN queue and its OWN end-of-stream sentinel; two concurrent consumers
each see EVERY event.

THE COALESCE ORDER (the backpressure's law — the asymmetry's law on the
push face): every queue is drop-oldest on overflow. The writer NEVER
blocks; the NEWEST events survive; a slow consumer loses OBSERVABILITY
FIRST, correctness never — the row is the truth, the poll converges.

THE OPERATIONAL LANDMINES (the T26 listener's own four, verbatim): the
PgBouncer transaction-pooling mode silently breaks LISTEN (the
dedicated connection IS the cure); the dedicated connection is ONE POOL
SLOT for the stream's life (size the pool for the streams you run);
asyncpg is fork-unsafe (start the listener IN EACH WORKER after the
fork); and the run-scoped backfill is bounded by THIS run's node count
— one listener per RUN is the shape (the SSE face's own scope), never
one per browser tab beyond that.

THE CHANNEL IS GLOBAL; THE SCHEMA RIDES THE PAYLOAD (P8): the listener
filters by the payload's ``schema`` field — the isolation between the
estate's schemas IS this filter. AND the run filter is the listener's
own scope: a knock whose ``flow_id`` is another run's is dropped here.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import suppress
from typing import TYPE_CHECKING, Annotated, Literal, cast
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel, Field

from taskq._json import loads as _json_loads
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import JobId
from taskq.constants import require_schema
from taskq.obs import get_logger
from taskq.workflows._progress import PROGRESS_NOTIFY_CHANNEL
from taskq.workflows._sql import WorkflowSql

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

__all__ = [
    "ProgressBackfilled",
    "ProgressEvent",
    "ProgressListener",
    "ProgressUpdated",
]  # RUF022: the events, then the listener

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The listener's own queue bound (the same tradeoff the admin's
#: ``_listen.py`` and the T26 listener made: a slow consumer drops the
#: OLDEST event — the row is the truth, the poll converges). Bounds the
#: raw queue, every subscriber queue, and the late-subscriber replay
#: history alike.
_QUEUE_MAXSIZE = 1000


class ProgressUpdated(BaseModel):
    """One node's progress MOVED (or the snapshot announces it): the
    pointer's fields + the BACKFILL's own pct/message (a live knock
    carries none — the row is the truth, the consumer reads the
    stream/state; the backfill has already paid the read)."""

    event: Literal["progress_updated"] = "progress_updated"
    flow_id: str
    node_id: str
    last_seq: int
    pct: float | None = None
    message: str | None = None
    source: Literal["backfill", "notify"] = "notify"


class ProgressBackfilled(BaseModel):
    """THE RECONCILE SNAPSHOT (the T26 listener's fourth member): emitted
    after EVERY (re)backfill, carrying the node → ``last_seq`` map the
    ROWS witness right now (this run's STATE channel). The consumer
    drops any progress card whose node the snapshot has moved past; the
    same map is the dedup's own ledger (a knock at-or-below the
    snapshot's seq yields nothing)."""

    event: Literal["progress_backfilled"] = "progress_backfilled"
    last_seqs: dict[str, int]


#: The TYPED event union (the discriminated kind): the backfill leg + the
#: live knocks.
ProgressEvent = Annotated[
    ProgressUpdated | ProgressBackfilled, Field(discriminator="event")
]

type _Raw = tuple[str, str]

# The pool hands out BOTH faces (the proxy and the bare connection) —
# the T26 listener's own union.
type _Conn = asyncpg.Connection | PoolConnectionProxy


class ProgressListener:
    """THE TYPED PROGRESS LISTENER (public — the SSE face and
    programmatic consumers share it), DIRECTLY ASYNC-ITERABLE (the T26
    amendment — no ceremony between the author and the events):

    >>> listener = ProgressListener(pool, schema, flow_id=flow_id)
    >>> async with listener:
    ...     async for event in listener.updates():
    ...         match event:  # the CLOSED union: Updated/Backfilled
    ...             case ProgressUpdated(): ...
    ...             case ProgressBackfilled(): ...

    ONE dedicated connection from the pool for the stream's life (LISTEN
    is session-scoped — see the module docstring's PgBouncer landmine);
    automatic reconnect with the admin feed's backoff discipline; the
    backfill re-runs on every (re)connect and each backfill ends with
    ONE :class:`ProgressBackfilled` reconcile snapshot. EVERY subscriber
    gets its own queue and its own end sentinel — the fan-out, never a
    partition (P7). :meth:`wait` is the SSE face's knock-wait (the push
    primary's own primitive; the poll tick is the belt)."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        schema: str,
        *,
        flow_id: JobId,
        keepalive_interval: float = 30.0,
        backoff_initial: float = 1.0,
        backoff_max: float = 30.0,
        acquire_timeout: float = 5.0,
        wsql: WorkflowSql | None = None,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._flow_id = str(flow_id)
        self._keepalive = keepalive_interval
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._acquire_timeout = acquire_timeout
        if wsql is not None:
            self._backfill_sql = wsql.progress_state_read_run
        else:
            from taskq.workflows.engine import render_workflow_sql

            self._backfill_sql = render_workflow_sql(schema).progress_state_read_run
        self._raw: asyncio.Queue[_Raw | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        # THE FAN-OUT (P7's cure): per-SUBSCRIBER queues (each its own
        # sentinel on stop), a bounded history each late subscriber
        # replays first (the buffer-then-consume behavior).
        self._subs: dict[int, asyncio.Queue[ProgressEvent | None]] = {}
        self._history: deque[ProgressEvent] = deque(maxlen=_QUEUE_MAXSIZE)
        # THE DEDUP'S LEDGER (per-connection): node_id → the snapshot's
        # last_seq. Reset on every (re)backfill — a previous connection's
        # announced seqs would suppress nothing valid.
        self._last_seqs: dict[str, int] = {}
        # THE KNOCK (the SSE face's primitive): set on EVERY event — the
        # generator's wait returns, the replay read is owed.
        self._knock = asyncio.Event()
        self._pump: asyncio.Task[None] | None = None
        self._closing = False
        self._dropped_logged = False

    # ── the lifecycle ────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the pump task (idempotent)."""
        if self._pump is not None:
            return
        require_schema(self._schema)
        self._closing = False
        self._pump = asyncio.create_task(self._pump_loop(), name="taskq-progress-listener")

    async def stop(self) -> None:
        """Stop the pump and release the dedicated connection, then end
        EVERY subscriber with its OWN sentinel (the pre-cure single
        ``None`` on one shared queue stranded all but one waiter — P7's
        convicted shape; the teardown is SHIELDED: a cancellation
        delivered while the pump's own finally releases the LISTEN
        connection must not strand it against the pool cap)."""
        self._closing = True
        pump, self._pump = self._pump, None
        if pump is not None:
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await shield_with_retrieval(pump)
        for queue in list(self._subs.values()):
            with suppress(asyncio.QueueFull):
                queue.put_nowait(None)  # each subscriber's OWN sentinel
        self._subs.clear()
        self._history.clear()

    async def __aenter__(self) -> ProgressListener:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ── the surfaces ─────────────────────────────────────────────────

    def __aiter__(self) -> AsyncGenerator[ProgressEvent, None]:
        """THE CEREMONY-FREE FORM (the T26 amendment): ``async for event
        in listener:`` — the typed stream directly."""
        return self.updates()

    async def updates(self) -> AsyncGenerator[ProgressEvent, None]:
        """THE NAMED FORM of the same surface: every yield is a
        :data:`ProgressEvent` (the keepalive sentinel is filtered
        out)."""
        async for event in self._iterate(None):
            if event is not None:
                yield event

    async def frames(self) -> AsyncGenerator[ProgressEvent | None, None]:
        """THE SSE FACE'S FORM: typed events + ``None`` keepalives (a
        quiet stream must still say something, or the proxies eat it)."""
        async for event in self._iterate(self._keepalive):
            yield event

    async def wait(self, timeout: float) -> bool:
        """THE KNOCK-WAIT (the SSE face's own primitive): ``True`` = an
        event arrived (a replay read is owed — the push primary);
        ``False`` = the belt's timeout (the poll tick — the fallback).
        One listener, one stream: a second concurrent waiter shares the
        knock (the face's documented shape)."""
        if self._knock.is_set():
            self._knock.clear()
            return True
        try:
            await asyncio.wait_for(self._knock.wait(), timeout)
        except TimeoutError:
            return False
        self._knock.clear()
        return True

    async def _iterate(self, keepalive: float | None) -> AsyncGenerator[ProgressEvent | None, None]:
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

    # ── the subscriber registry (the fan-out's mechanics) ────────────

    def _subscribe(self) -> asyncio.Queue[ProgressEvent | None]:
        """Register ONE subscriber: its own bounded queue, seeded with
        the bounded event history (a backfill announced before the
        subscribe is still delivered — the buffer-then-consume
        behavior). ATOMIC by construction (the T26 listener's own
        argument): no await between the history copy and the registry
        insert."""
        queue: asyncio.Queue[ProgressEvent | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        if self._closing:
            queue.put_nowait(None)  # a subscriber after stop ends immediately
            return queue
        for event in self._history:
            queue.put_nowait(event)
        self._subs[id(queue)] = queue
        return queue

    def _unsubscribe(self, queue: asyncio.Queue[ProgressEvent | None]) -> None:
        with suppress(KeyError):
            del self._subs[id(queue)]

    # ── the pump (connect → LISTEN → backfill → tail, with reconnect) ──

    async def _pump_loop(self) -> None:
        backoff = self._backoff_initial
        while not self._closing:
            conn: _Conn | None = None
            try:
                conn = await asyncio.wait_for(self._pool.acquire(), timeout=self._acquire_timeout)
                await self._listen_and_backfill(conn)
                backoff = self._backoff_initial
                while not self._closing:
                    try:
                        raw = await asyncio.wait_for(self._raw.get(), timeout=self._keepalive)
                    except TimeoutError:
                        if conn.is_closed():
                            raise ConnectionError(
                                "the progress listener's connection was closed "
                                "underneath the pump"
                            ) from None
                        continue
                    if raw is None:
                        return
                    event = self._decode(*raw)
                    if event is not None:
                        self._emit(event)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                logger.warning(
                    "progress-listener-reconnect",
                    schema=self._schema,
                    flow_id=self._flow_id,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    backoff=backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._backoff_max)
            finally:
                if conn is not None:
                    await self._release(conn)

    async def _listen_and_backfill(self, conn: _Conn) -> None:
        """THE ZERO-WINDOW SEQUENCE, IN ORDER: LISTEN the channel FIRST,
        THEN the snapshot — the order IS the construction (a backfill
        that ran before the LISTEN would open the missed-event window
        the design closes)."""
        await conn.execute(f'LISTEN "{PROGRESS_NOTIFY_CHANNEL}"')
        await conn.add_listener(PROGRESS_NOTIFY_CHANNEL, self._on_notify)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow the callback type; the runtime accepts sync callbacks (the admin feed's own carry-over).
        await self._backfill(conn)

    async def _backfill(self, conn: _Conn) -> None:
        """THE RUN'S STATE ROWS, FROM THE SNAPSHOT (the zero-window
        law's half): every STATE-channel row this run carries becomes a
        ``ProgressUpdated(source="backfill")`` — including an emission
        written BEFORE this listener existed. The snapshot's per-node
        ``last_seq`` enters the dedup ledger: the rows' own (already-
        queued) knocks yield nothing. AND THE RECONCILE (the fourth
        member): ONE :class:`ProgressBackfilled` event carries the
        node → ``last_seq`` map — the consumer drops any progress card
        the rows have moved past."""
        rows = await conn.fetch(self._backfill_sql, JobId(UUID(self._flow_id)))
        self._last_seqs.clear()
        for row in rows:
            if row["channel"] != "progress":
                continue
            node_id = str(row["node_id"])
            self._last_seqs[node_id] = int(row["last_seq"])
            self._emit(
                ProgressUpdated(
                    flow_id=self._flow_id,
                    node_id=node_id,
                    last_seq=int(row["last_seq"]),
                    pct=row["pct"],
                    message=row["message"],
                    source="backfill",
                )
            )
        self._emit(ProgressBackfilled(last_seqs=dict(self._last_seqs)))

    def _on_notify(self, _conn: object, _pid: int, channel: str, payload: str) -> None:
        """The session callback (sync, loop-thread): the raw pair into
        the bounded queue — drop-oldest on overflow (THE COALESCE ORDER:
        the writer never blocks, observability degrades first — logged
        once)."""
        if not payload:
            return
        self._knock.set()
        if self._raw.full():
            with suppress(asyncio.QueueEmpty):
                self._raw.get_nowait()
            if not self._dropped_logged:
                logger.warning(
                    "progress-listener-queue-overflow-drop-oldest",
                    channel=channel,
                    maxsize=_QUEUE_MAXSIZE,
                )
                self._dropped_logged = True
        self._raw.put_nowait((channel, payload))

    def _decode(self, channel: str, payload: str) -> ProgressEvent | None:
        """The raw knock → the typed event (or ``None``: another
        schema's, another run's, or a shape this decoder refuses — a
        malformed payload is dropped LOUDLY-once, never fatal: the row
        is the truth). THE DEDUP: a knock whose seq is at-or-below the
        snapshot's own ``last_seq`` for that node yields nothing (one
        emission, ONE event per connection)."""
        try:
            parsed = _json_loads(payload)
        except Exception:
            logger.warning("progress-listener-malformed-payload", channel=channel)
            return None
        if not isinstance(parsed, dict):
            return None
        doc = cast("dict[str, object]", parsed)  # pyright: ignore[reportUnknownVariableType]  # Why: the notify payload's jsonb walk — the isinstance guard above is the runtime shape check.
        if doc.get("schema") != self._schema:
            return None  # ANOTHER SCHEMA'S EVENT (global channels — the payload routes; the isolation IS this filter, P8)
        flow_id = doc.get("flow_id")
        node_id = doc.get("node_id")
        seq = doc.get("seq")
        if not isinstance(flow_id, str) or not isinstance(node_id, str) or not isinstance(seq, int):
            return None
        if flow_id != self._flow_id:
            return None  # ANOTHER RUN'S EVENT (the listener is run-scoped — the backfill's bound)
        last = self._last_seqs.get(node_id)
        if last is not None and seq <= last:
            return None  # THE DEDUP (the zero-window's other half): the snapshot already announced this seq
        self._last_seqs[node_id] = seq
        return ProgressUpdated(
            flow_id=flow_id, node_id=node_id, last_seq=seq, source="notify"
        )

    def _emit(self, event: ProgressEvent) -> None:
        """The typed event to EVERY subscriber (the fan-out — never a
        partition), plus the bounded history (each late subscriber
        replays it first). Each queue is drop-oldest on overflow — the
        same bound as the raw leg."""
        self._knock.set()
        self._history.append(event)
        for queue in list(self._subs.values()):
            if queue.full():
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    async def _release(self, conn: _Conn) -> None:
        """The dedicated connection's teardown (the admin feed's
        shielded discipline)."""
        with suppress(Exception):
            await conn.remove_listener(PROGRESS_NOTIFY_CHANNEL, self._on_notify)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow the callback type.
        with suppress(Exception):
            await conn.execute(f'UNLISTEN "{PROGRESS_NOTIFY_CHANNEL}"')
        with suppress(Exception):
            await self._pool.release(conn)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow release to the proxy; the runtime accepts both faces it hands out.
