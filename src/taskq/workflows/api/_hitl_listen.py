"""THE HITL LISTENER (T26): the typed, public face over the approval
broadcast — ONE dedicated connection, LISTEN → BACKFILL → TAIL.

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
correctness. A reconnect RE-BACKFILLS: a re-delivered
``HoldCreated`` is IDEMPOTENT by ``hold_id`` at the consumer (the row
it points at is unchanged).

THE CHANNELS ARE GLOBAL; THE SCHEMA RIDES THE PAYLOAD (T26's read:
``pg_notify`` is per-database and the schema-per-module estate shares
one database across many schemas — the listener filters by the
payload's ``schema`` field, never by channel arithmetic).
"""

from __future__ import annotations

import asyncio
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
#: row is the truth, the poll converges).
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
    """A hold's deadline passed the DB clock: the sweep abandoned it —
    the wait site will raise the typed timeout (the body's fail-close
    owns it from there)."""

    event: Literal["hold_expired"] = "hold_expired"
    hold_id: str
    run_id: str
    signal: str
    node_key: str


#: The TYPED event union (the discriminated kind — the SSE frame's
#: ``event: hold`` carries the union's JSON; the consumer re-narrows on
#: the literal).
HoldEvent = Annotated[HoldCreated | HoldResolved | HoldExpired, Field(discriminator="event")]

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
    ...         match event:  # the CLOSED union: HoldCreated / HoldResolved / HoldExpired
    ...             case HoldCreated(): ...
    ...             case HoldResolved(): ...
    ...             case HoldExpired(): ...

    ONE dedicated connection from the pool for the stream's life
    (LISTEN is session-scoped — a pooled round-robin would UNLISTEN on
    every release); automatic reconnect with the admin feed's backoff
    discipline; the backfill re-runs on every (re)connect. The plain
    ``async for event in listener`` (or ``listener.events()``) is the
    unfiltered typed stream; :meth:`holds` filters to ONE run;
    :meth:`frames` is the SSE face's keepalive form (yields ``None`` on
    the keepalive tick).
    """

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
        self._out: asyncio.Queue[HoldEvent | None] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
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
        """Stop the pump and release the dedicated connection (the
        teardown is SHIELDED: a cancellation delivered while the pump's
        own finally releases the LISTEN connection must not strand it
        against the pool cap — the admin feed's release discipline,
        carried over)."""
        self._closing = True
        pump, self._pump = self._pump, None
        if pump is not None:
            pump.cancel()
            with suppress(asyncio.CancelledError):
                await shield_with_retrieval(pump)
        with suppress(asyncio.QueueEmpty):
            self._out.get_nowait()
        self._out.put_nowait(None)  # the consumer's sentinel

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
        surface: every schema-routed event reaches it)."""
        run_id = str(run)
        async for event in self._iterate(None):
            if event is not None and event.run_id == run_id:
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
        while True:
            try:
                if keepalive is None:
                    item = await self._out.get()
                else:
                    item = await asyncio.wait_for(self._out.get(), timeout=keepalive)
            except TimeoutError:
                yield None
                continue
            if item is None:
                return
            yield item

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
        nothing."""
        rows = await conn.fetch(
            f"SELECT id, workflow_id, node_key, signal_name, created_at "  # noqa: S608  # Why: the f-string's ONLY interpolation is the require_schema-validated schema identifier — values never interpolate.
            f"FROM \"{self._schema}\".wf_signals WHERE status = 'held' ORDER BY id"
        )
        for row in rows:
            hold_id = str(row["id"])
            self._backfilled.add(hold_id)
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
            return None  # ANOTHER SCHEMA'S EVENT (global channels — the payload routes)
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
        """The typed event into the consumer queue (drop-oldest on
        overflow — the same bound as the raw leg)."""
        if self._out.full():
            with suppress(asyncio.QueueEmpty):
                self._out.get_nowait()
        self._out.put_nowait(event)

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
