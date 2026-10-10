"""THE HITL LISTENER (T26): the typed, public face over the approval
broadcast — ONE dedicated connection, LISTEN → BACKFILL → TAIL — fanning
out to N concurrent subscribers.

THE ZERO-WINDOW CONSTRUCTION (the #316 mid-stream re-check pattern
generalized to the open): the connection LISTENS FIRST, THEN the
backfill snapshot reads the still-``held`` rows. Any hold visible to
the snapshot either notified BEFORE we listened (no notify will ever
come — the snapshot is the only witness) or AFTER (its notify is
deduped against the snapshot by ``hold_id``). A missed event is
unrepresentable by construction — not by luck, not by polling.

THE ROW IS STILL THE TRUTH: every event is a POINTER (ids, names, the
verdict's declared KIND — never the payload's content; the redact law
holds at the knock). A consumer that misses everything still converges
by polling ``HitlClient.list(run=…)``; the listener buys LATENCY, never
correctness.

THE RECONNECT RECONCILE (T26's fourth union member — the hostile
review's C1): a hold RESOLVED or EXPIRED while the listener was down
never announces its own death (the notify went to no one — the
connection was not listening). So every (re)backfill ends with ONE
``Backfilled(open_hold_ids=…)`` snapshot event: the consumer drops any
card whose ``hold_id`` the snapshot disowns. This is also the death of
the STALE events — a dead hold's ``HoldCreated`` still sitting in the
replayed history or the pre-outage raw queue (the ghost) is dropped by
the same reconcile: the ghost may cross the stream; its card cannot
survive the snapshot (P6).

THE FAN-OUT, NOT A PARTITION (the hostile review's C2 — the product
shape is ONE backend listener fanning out to N watching users, the
progress stream's own pub/sub discipline): every subscriber —
``async for event in listener``, ``events()``, ``holds(run=…)``,
``frames()`` — gets its OWN queue and its OWN end-of-stream sentinel;
two concurrent consumers each see EVERY event (a shared queue would
PARTITION the stream, each consumer stealing the other's events, and a
single sentinel on stop would strand all but one waiter — P7's
convicted shape). A subscriber attaching late replays the bounded event
history first (the buffer-then-consume behavior — a backfill announced
before the subscribe is still delivered), then goes live.

THE OPERATIONAL LANDMINES (each one real, named for the operator —
the guide's HITL section carries the same four):

1. **PGBouncer**: transaction-pooling modes (the default
   ``pool_mode = transaction`` of every managed PgBouncer) SILENTLY
   break LISTEN — a LISTEN is SESSION-scoped, and a transaction pooler
   hands the session away after every transaction, so the listener's
   notifications stop arriving with no error anywhere. The listener
   OWNS its dedicated connection for the stream's life precisely for
   this: it must be a DIRECT connection (bypass PgBouncer, or run a
   session-pooling port) — a pooler in the path is the #1 real-world
   NOTIFY failure.
2. **THE CAPACITY TAX**: that dedicated connection is ONE POOL SLOT
   FOR THE STREAM'S LIFE (not per query — per listener). Size the pool
   for the listeners you run; an SSE face holding a listener per
   browser tab spends one slot per tab until the tab closes.
3. **FORK SAFETY**: the dedicated connection is an asyncio transport
   bound to the loop and process that opened it (asyncpg is documented
   fork-unsafe — see :mod:`taskq._forkguard`). An app that forks after
   the listener starts (uvicorn/gunicorn multi-worker shapes, the
   ``--preload`` master) must start the listener IN EACH WORKER after
   the fork — never inherit a parent's live listener.
4. **EXPIRY PRECISION IS AT-LEAST**: the deadline fires on the DB clock
   at the FIRST leader-sweep pass at-or-after it — the real bound is
   ``timeout_s`` + the sweep interval (+ a leader failover's gap).
   Never promise a precise 120 s: the expiry is a floor, and the
   fail-close is the guarantee, not the stopwatch.

THE CHANNELS ARE GLOBAL; THE SCHEMA RIDES THE PAYLOAD (T26's read:
``pg_notify`` is per-database and the schema-per-module estate shares
one database across many schemas — the listener filters by the
payload's ``schema`` field, never by channel arithmetic; that payload
filter IS the isolation between the estate's schemas — P8).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import suppress
from typing import TYPE_CHECKING, Annotated, Literal, cast

import asyncpg
import structlog
from pydantic import BaseModel, Field

from taskq._json import loads as _json_loads
from taskq._shield import shield_with_retrieval
from taskq.backend._protocol import JobId
from taskq.constants import require_schema
from taskq.obs import get_logger
from taskq.workflows.api._hitl import (
    HOLD_CREATED_CHANNEL,
    HOLD_EXPIRED_CHANNEL,
    HOLD_RESOLVED_CHANNEL,
)

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

__all__ = [
    "BROADCAST_CHANNELS",
    "Backfilled",
    "HitlListener",
    "HoldCreated",
    "HoldEvent",
    "HoldExpired",
    "HoldResolved",
]  # RUF022: the register's own order (the channels, then the events, then the listener)

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The broadcast legs (the listener's LISTEN set — the create leg, the
#: resolve leg, the expiry leg; the legacy ``taskq_wf_holds`` pointer
#: knob is NOT here: its shape is pinned separately and it carries no
#: schema, so a listener on it could not filter).
BROADCAST_CHANNELS: frozenset[str] = frozenset(
    {HOLD_CREATED_CHANNEL, HOLD_RESOLVED_CHANNEL, HOLD_EXPIRED_CHANNEL}
)

#: The listener's own queue bound (the same tradeoff the admin's
#: ``_listen.py`` made: a slow consumer drops the OLDEST event — the
#: row is the truth, the poll converges). Bounds the raw queue, every
#: subscriber queue, and the late-subscriber replay history alike.
_QUEUE_MAXSIZE = 1000


class HoldCreated(BaseModel):
    """A hold NOW EXISTS (the approval is owed): from the start
    backfill (``source="backfill"``) or a live create
    (``source="notify"``)."""

    event: Literal["hold_created"] = "hold_created"
    hold_id: str
    run_id: str
    signal: str
    node_key: str
    created_at: str | None = None
    source: Literal["backfill", "notify"] = "notify"


class HoldResolved(BaseModel):
    """A hold was ANSWERED through the typed door: ``verdict_kind`` is
    the payload model the boundary validated against (the verdict's
    DECLARED kind — never the verdict's content)."""

    event: Literal["hold_resolved"] = "hold_resolved"
    hold_id: str
    run_id: str
    verdict_kind: str | None = None


class HoldExpired(BaseModel):
    """A hold's deadline passed the DB clock and the sweep abandoned it
    — the wait's outcome is the CLOSED UNION's ``Expired`` MEMBER, not
    an exception: the body MATCHES ``case Expired():`` (the fail-close
    arm the checker forces; a body that wants the FAILURE raises
    ``SignalTimeoutError`` ITSELF off the member — the machinery never
    raises)."""

    event: Literal["hold_expired"] = "hold_expired"
    hold_id: str
    run_id: str
    signal: str
    node_key: str


class Backfilled(BaseModel):
    """THE RECONNECT-RECONCILE SNAPSHOT (T26's fourth union member —
    the hostile review's C1): emitted after EVERY (re)backfill, carrying
    the OPEN hold ids the ROWS witness right now (this schema's still-
    ``held`` rows). The consumer drops any open card the snapshot
    disowns — a hold resolved or expired during a listener outage never
    announces its own death, so the snapshot is the only witness; the
    same reconcile is the death of the STALE events (a ghost
    ``HoldCreated`` replayed from the pre-outage history announces a
    hold the rows have moved past — its card cannot survive the
    snapshot; P6/C7). The ``holds(run=…)`` filter passes this member
    through: every consumer reconciles its OWN cards against it."""

    event: Literal["backfilled"] = "backfilled"
    open_hold_ids: list[str]


#: The TYPED event union (the discriminated kind — the SSE frame's
#: ``event: hold`` carries the union's JSON; the consumer re-narrows on
#: the literal): the three NOTIFY legs + the reconcile snapshot.
HoldEvent = Annotated[
    HoldCreated | HoldResolved | HoldExpired | Backfilled, Field(discriminator="event")
]

type _Raw = tuple[str, str]

# The pool hands out BOTH faces (the proxy and the bare connection) —
# the admin feed's own union (\_listen.py).
type _Conn = asyncpg.Connection | PoolConnectionProxy


class HitlListener:
    """THE TYPED LISTENER (public — the SSE face and programmatic
    consumers share it), DIRECTLY ASYNC-ITERABLE (T26's amendment — no
    ceremony between the author and the events):

    >>> listener = HitlListener(pool, schema)
    >>> async with listener:
    ...     async for event in listener.holds(run=flow_id):
    ...         match event:  # the CLOSED union: Created/Resolved/Expired/Backfilled
    ...             case HoldCreated(): ...
    ...             case HoldResolved(): ...
    ...             case HoldExpired(): ...
    ...             case Backfilled(): ...  # the reconcile: drop cards not in open_hold_ids

    ONE dedicated connection from the pool for the stream's life
    (LISTEN is session-scoped — a pooled round-robin would UNLISTEN on
    every release; see the module docstring's PgBouncer landmine);
    automatic reconnect with the admin feed's backoff discipline; the
    backfill re-runs on every (re)connect and each backfill ends with
    the :class:`Backfilled` reconcile snapshot. The plain ``async for
    event in listener`` (or ``listener.events()``) is the unfiltered
    typed stream; :meth:`holds` filters to ONE run; :meth:`frames` is
    the SSE face's keepalive form (yields ``None`` on the keepalive
    tick). EVERY subscriber gets its own queue and its own end sentinel
    — the fan-out, never a partition (P7)."""

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
        self._raw: asyncio.Queue[_Raw | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        # THE FAN-OUT (P7's cure): per-SUBSCRIBER queues (each its own
        # sentinel on stop), a bounded history each late subscriber
        # replays first (the buffer-then-consume behavior), and the
        # single raw queue only the pump reads. A shared consumer queue
        # here is the convicted shape: N consumers would PARTITION the
        # stream, each stealing the other's events.
        self._subs: dict[int, asyncio.Queue[HoldEvent | None]] = {}
        self._history: deque[HoldEvent] = deque(maxlen=_QUEUE_MAXSIZE)
        self._backfilled: set[str] = set()
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
        self._pump = asyncio.create_task(self._pump_loop(), name="taskq-hitl-listener")

    async def stop(self) -> None:
        """Stop the pump and release the dedicated connection, then end
        EVERY subscriber with its OWN sentinel (the pre-cure single
        ``None`` on one shared queue stranded all but one waiter — P7's
        convicted shape; the teardown is SHIELDED: a cancellation
        delivered while the pump's own finally releases the LISTEN
        connection must not strand it against the pool cap — the admin
        feed's release discipline, carried over)."""
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

    async def __aenter__(self) -> HitlListener:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    # ── the surfaces ─────────────────────────────────────────────────

    def __aiter__(self) -> AsyncGenerator[HoldEvent, None]:
        """THE CEREMONY-FREE FORM (T26's amendment): ``async for event
        in listener:`` — the typed stream directly; no ``.events()``
        lookup between the author and the events. The keepalive form
        stays on :meth:`frames` (the SSE face's)."""
        return self.events()

    async def events(self) -> AsyncGenerator[HoldEvent, None]:
        """THE NAMED FORM of the same surface: ``async for event in
        listener.events():`` — every yield is a :data:`HoldEvent` (the
        keepalive sentinel is filtered out; a direct consumer just
        awaits)."""
        async for event in self._iterate(None):
            if event is not None:
                yield event

    async def holds(self, run: JobId | str) -> AsyncGenerator[HoldEvent, None]:
        """THE TYPED FILTER (T26's amendment): one run's events —
        ``listener.holds(run=flow_id)`` — the approval stream for ONE
        flow, typed. The unfiltered stream still flows (the raw tail's
        surface: every schema-routed event reaches it). The
        :class:`Backfilled` reconcile passes EVERY run's filter (it
        carries no ``run_id`` — the snapshot is the SCHEMA's open-hold
        set, and every consumer reconciles its own cards against it)."""
        run_id = str(run)
        async for event in self._iterate(None):
            if event is None:
                continue
            if isinstance(event, Backfilled) or event.run_id == run_id:
                yield event

    async def frames(self) -> AsyncGenerator[HoldEvent | None, None]:
        """THE SSE FACE'S FORM: typed events + ``None`` keepalives (the
        admin feed's own discipline — a quiet stream must still say
        something, or the proxies eat it)."""
        async for event in self._iterate(self._keepalive):
            yield event

    async def _iterate(self, keepalive: float | None) -> AsyncGenerator[HoldEvent | None, None]:
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

    def _subscribe(self) -> asyncio.Queue[HoldEvent | None]:
        """Register ONE subscriber: its own bounded queue, seeded with
        the bounded event history (a backfill announced before the
        subscribe is still delivered — the buffer-then-consume
        behavior). ATOMIC by construction: no await between the history
        copy and the registry insert, so no event is both replayed AND
        fanned out, and none is lost in between (the loop thread
        schedules nothing else mid-function)."""
        queue: asyncio.Queue[HoldEvent | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        if self._closing:
            queue.put_nowait(None)  # a subscriber after stop ends immediately
            return queue
        for event in self._history:
            queue.put_nowait(event)
        self._subs[id(queue)] = queue
        return queue

    def _unsubscribe(self, queue: asyncio.Queue[HoldEvent | None]) -> None:
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
                                "the HITL listener's connection was closed underneath the pump"
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
                    "hitl-listener-reconnect",
                    schema=self._schema,
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
        """THE ZERO-WINDOW SEQUENCE, IN ORDER: LISTEN every broadcast
        channel FIRST, THEN the snapshot — the order IS the
        construction (a backfill that ran before the LISTEN would open
        the missed-event window the design closes)."""
        for channel in sorted(BROADCAST_CHANNELS):
            await conn.execute(f'LISTEN "{channel}"')
            await conn.add_listener(channel, self._on_notify)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow the callback type; the runtime accepts sync callbacks (the admin feed's own carry-over).
        await self._backfill(conn)

    async def _backfill(self, conn: _Conn) -> None:
        """The OPEN holds, FROM THE ROWS (the snapshot the zero-window
        law leans on): every still-``held`` row in the schema becomes a
        ``HoldCreated(source="backfill")`` — including a hold created
        BEFORE this listener existed (pin T26-P2). The snapshot's ids
        enter the dedup set: their own (already-queued) notifies yield
        nothing. AND THE RECONCILE (the fourth member — P6's cure):
        after the snapshot's creations, ONE :class:`Backfilled` event
        carries the OPEN ids — the consumer drops any card the rows
        disown (a hold resolved/expired during the outage never
        announces its own death)."""
        rows = await conn.fetch(
            f"SELECT id, workflow_id, node_key, signal_name, created_at "  # noqa: S608  # Why: the f-string's ONLY interpolation is the require_schema-validated schema identifier — values never interpolate.
            f"FROM \"{self._schema}\".wf_signals WHERE status = 'held' ORDER BY id"
        )
        # The dedup set is PER-CONNECTION (the LISTEN→snapshot race's
        # witness): reset on every (re)backfill — a previous
        # connection's announced ids would suppress nothing valid, and
        # a stale set could never shrink.
        self._backfilled.clear()
        open_ids: list[str] = []
        for row in rows:
            hold_id = str(row["id"])
            self._backfilled.add(hold_id)
            open_ids.append(hold_id)
            created = row["created_at"]
            self._emit(
                HoldCreated(
                    hold_id=hold_id,
                    run_id=str(row["workflow_id"]),
                    signal=str(row["signal_name"]),
                    node_key=str(row["node_key"]),
                    created_at=(created.isoformat() if hasattr(created, "isoformat") else None),
                    source="backfill",
                )
            )
        self._emit(Backfilled(open_hold_ids=open_ids))

    def _on_notify(self, _conn: object, _pid: int, channel: str, payload: str) -> None:
        """The session callback (sync, loop-thread): the raw pair into
        the bounded queue — drop-oldest on overflow (the admin feed's
        own tradeoff, logged once)."""
        if not payload:
            return
        if self._raw.full():
            with suppress(asyncio.QueueEmpty):
                self._raw.get_nowait()
            if not self._dropped_logged:
                logger.warning(
                    "hitl-listener-queue-overflow-drop-oldest",
                    channel=channel,
                    maxsize=_QUEUE_MAXSIZE,
                )
                self._dropped_logged = True
        self._raw.put_nowait((channel, payload))

    def _decode(self, channel: str, payload: str) -> HoldEvent | None:
        """The raw knock → the typed event (or ``None``: another
        schema's event, or a shape this decoder refuses — a malformed
        payload is dropped LOUDLY-once, never fatal: the row is the
        truth)."""
        try:
            parsed = _json_loads(payload)
        except Exception:
            logger.warning("hitl-listener-malformed-payload", channel=channel)
            return None
        if not isinstance(parsed, dict):
            return None
        doc = cast("dict[str, object]", parsed)  # pyright: ignore[reportUnknownVariableType]  # Why: the notify payload's jsonb walk — the isinstance guard above is the runtime shape check.
        if doc.get("schema") != self._schema:
            return None  # ANOTHER SCHEMA'S EVENT (global channels — the payload routes; the isolation IS this filter, P8)
        hold_id = doc.get("hold_id")
        run_id = doc.get("run_id") or doc.get("flow_id")
        if not isinstance(hold_id, str) or not isinstance(run_id, str):
            return None
        if channel == HOLD_CREATED_CHANNEL:
            # THE DEDUP (the zero-window's other half): a hold the
            # backfill already announced yields nothing — one id, ONE
            # created event per connection (pin T26-P2's dedup leg).
            if hold_id in self._backfilled:
                self._backfilled.discard(hold_id)
                return None
            return HoldCreated(
                hold_id=hold_id,
                run_id=run_id,
                signal=str(doc.get("signal", "")),
                node_key=str(doc.get("node_key", "")),
                created_at=(
                    str(doc["created_at"]) if isinstance(doc.get("created_at"), str) else None
                ),
                source="notify",
            )
        if channel == HOLD_RESOLVED_CHANNEL:
            verdict_kind = doc.get("verdict_kind")
            return HoldResolved(
                hold_id=hold_id,
                run_id=run_id,
                verdict_kind=verdict_kind if isinstance(verdict_kind, str) else None,
            )
        if channel == HOLD_EXPIRED_CHANNEL:
            return HoldExpired(
                hold_id=hold_id,
                run_id=run_id,
                signal=str(doc.get("signal", "")),
                node_key=str(doc.get("node_key", "")),
            )
        return None

    def _emit(self, event: HoldEvent) -> None:
        """The typed event to EVERY subscriber (the fan-out — never a
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

    async def _release(self, conn: _Conn) -> None:
        """The dedicated connection's teardown (the admin feed's
        shielded discipline: a cancellation delivered mid-cleanup must
        not strand the connection against the pool cap)."""
        for channel in sorted(BROADCAST_CHANNELS):
            with suppress(Exception):
                await conn.remove_listener(channel, self._on_notify)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow the callback type.
            with suppress(Exception):
                await conn.execute(f'UNLISTEN "{channel}"')
        with suppress(Exception):
            await self._pool.release(conn)  # pyright: ignore[reportArgumentType]  # Why: the stubs over-narrow release to the proxy; the runtime accepts both faces it hands out (the admin feed's carry-over).
