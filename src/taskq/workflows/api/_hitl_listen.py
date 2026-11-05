"""The typed, public face over the approval
broadcast — one dedicated connection, LISTEN → BACKFILL → TAIL — fanning
out to N concurrent subscribers.

the ZERO-WINDOW CONSTRUCTION (the #316 mid-stream re-check pattern
generalized to the open): the connection LISTENS first, then the
backfill snapshot reads the still-``held`` rows. Any hold visible to
the snapshot either notified before we listened (no notify will ever
come — the snapshot is the only witness) or after (its notify is
deduped against the snapshot by ``hold_id``). A missed event is
unrepresentable by construction — not by luck, not by polling.

Every event is a POINTER (ids, names, the
verdict's declared KIND — never the payload's content; the redact law
holds at the knock). A consumer that misses everything still converges
by polling ``HitlClient.list(run=…)``; the listener buys LATENCY, never
correctness.

the RECONNECT RECONCILE (the fourth union member — the hostile
contract): a hold RESOLVED or EXPIRED while the listener was down
never announces its own death (the notify went to no one — the
connection was not listening). So every (re)backfill ends with one
``Backfilled(open_hold_ids=…)`` snapshot event: the consumer drops any
card whose ``hold_id`` the snapshot disowns. This is also the death of
the STALE events — a dead hold's ``HoldCreated`` still sitting in the
replayed history or the pre-outage raw queue (the ghost) is dropped by
the same reconcile: the ghost may cross the stream; its card cannot
survive the snapshot (P6).

The fan-out, not a partition (the product
shape is one backend listener fanning out to N watching users, the
progress stream's own pub/sub discipline): every subscriber —
``async for event in listener``, ``events()``, ``holds(run=…)``,
``frames()`` — gets its own queue and its own end-of-stream sentinel;
two concurrent consumers each see every event (a shared queue would
PARTITION the stream, each consumer stealing the other's events, and a
single sentinel on stop would strand all but one waiter). A subscriber attaching late replays the bounded event
history first (the buffer-then-consume behavior — a backfill announced
before the subscribe is still delivered), then goes live.

the OPERATIONAL LANDMINES (each one real, named for the operator —
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
2. **the CAPACITY TAX**: that dedicated connection is one POOL SLOT
   FOR the STREAM'S LIFE (not per query — per listener). Size the pool
   for the listeners you run; an SSE face holding a listener per
   browser tab spends one slot per tab until the tab closes.
3. **FORK SAFETY**: the dedicated connection is an asyncio transport
   bound to the loop and process that opened it (asyncpg is documented
   fork-unsafe — see :mod:`taskq._forkguard`). An app that forks after
   the listener starts (uvicorn/gunicorn multi-worker shapes, the
   ``--preload`` master) must start the listener IN EACH WORKER after
   the fork — never inherit a parent's live listener.
4. **EXPIRY PRECISION IS AT-LEAST**: the deadline fires on the DB clock
   at the first leader-sweep pass at-or-after it — the real bound is
   ``timeout_s`` + the sweep interval (+ a leader failover's gap).
   Never promise a precise 120 s: the expiry is a floor, and the
   fail-close is the guarantee, not the stopwatch.

``pg_notify`` is per-database and the schema-per-module estate shares
one database across many schemas — the listener filters by the
payload's ``schema`` field, never by channel arithmetic; that payload
filter IS the isolation between the estate's schemas — P8).
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from typing import Annotated, Literal, cast

import asyncpg
import structlog
from pydantic import BaseModel, Field

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows.api._hitl import (
    HOLD_CREATED_CHANNEL,
    HOLD_EXPIRED_CHANNEL,
    HOLD_RESOLVED_CHANNEL,
    _as_hold_id,  # pyright: ignore[reportPrivateUsage]  # Why: the id's canonical form is the hitl module's own normalizer — the listener door binds through it (the same surface, the same parse).
)
from taskq.workflows.api._listen_pump import (
    DEFAULT_QUEUE_MAXSIZE,
    _BroadcastListener,  # pyright: ignore[reportPrivateUsage]  # Why: the shared LISTEN→backfill→tail body; the listener keeps only its own face (the channels, the snapshot, the decode, the log names).
    _Conn,  # pyright: ignore[reportPrivateUsage]  # Why: the pool's two faces, one union — the pump's own alias.
)

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
#: knob is not here: its shape is pinned separately and it carries no
#: schema, so a listener on it could not filter).
BROADCAST_CHANNELS: frozenset[str] = frozenset(
    {HOLD_CREATED_CHANNEL, HOLD_RESOLVED_CHANNEL, HOLD_EXPIRED_CHANNEL}
)

#: The listener's own queue bound (the same tradeoff the admin's
#: ``_listen.py`` made: a slow consumer drops the oldest event — the
#: row is the truth, the poll converges). Bounds the raw queue, every
#: subscriber queue, and the late-subscriber replay history alike.
_QUEUE_MAXSIZE = DEFAULT_QUEUE_MAXSIZE


class HoldCreated(BaseModel):
    """A hold now EXISTS (the approval is owed): from the start
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
    """A hold landed in the DEFINED ``abandoned`` state — whichever door
    wrote it: the sweep (the deadline passed the DB clock) or the
    operator's abandon (``HitlClient.abandon``). ``abandoned_by`` names
    the door (``"timeout"`` — the sweep's broadcast; ``"operator"`` —
    the abandon door's). The wait's outcome is the CLOSED UNION's
    ``Expired`` MEMBER, not an exception: the body MATCHES ``case
    Expired():`` (the fail-close arm the checker forces; a body that
    wants the FAILURE raises ``SignalTimeoutError`` — or, for the
    operator door, ``SignalAbandonedError`` — ITSELF off the member,
    which carries the same door; the machinery never raises)."""

    event: Literal["hold_expired"] = "hold_expired"
    hold_id: str
    run_id: str
    signal: str
    node_key: str
    abandoned_by: Literal["timeout", "operator"] = "timeout"


class Backfilled(BaseModel):
    """the RECONNECT-RECONCILE SNAPSHOT (the fourth union member —
    the reconcile event's contract): emitted after every (re)backfill, carrying
    the OPEN hold ids the ROWS witness right now (this schema's still-
    ``held`` rows). The consumer drops any open card the snapshot
    disowns — a hold resolved or expired during a listener outage never
    announces its own death, so the snapshot is the only witness; the
    same reconcile is the death of the STALE events (a ghost
    ``HoldCreated`` replayed from the pre-outage history announces a
    hold the rows have moved past — its card cannot survive the
    snapshot; the reconcile contract). The ``holds(run=…)`` filter passes this member
    through: every consumer reconciles its own cards against it."""

    event: Literal["backfilled"] = "backfilled"
    open_hold_ids: list[str]


#: The typed event union (the discriminated kind — the SSE frame's
#: ``event: hold`` carries the union's JSON; the consumer re-narrows on
#: the literal): the three NOTIFY legs + the reconcile snapshot.
HoldEvent = Annotated[
    HoldCreated | HoldResolved | HoldExpired | Backfilled, Field(discriminator="event")
]


class HitlListener(_BroadcastListener[HoldEvent]):
    """the TYPED LISTENER (public — the SSE face and programmatic
    consumers share it), DIRECTLY ASYNC-ITERABLE (the amendment — no
    ceremony between the author and the events):

    >>> listener = HitlListener(pool, schema)
    >>> async with listener:
    ...     async for event in listener.holds(run=flow_id):
    ...         match event:  # the CLOSED union: Created/Resolved/Expired/Backfilled
    ...             case HoldCreated(): ...
    ...             case HoldResolved(): ...
    ...             case HoldExpired(): ...
    ...             case Backfilled(): ...  # the reconcile: drop cards not in open_hold_ids

    one dedicated connection from the pool for the stream's life
    (LISTEN is session-scoped — a pooled round-robin would UNLISTEN on
    every release; see the module docstring's PgBouncer landmine);
    automatic reconnect with the admin feed's backoff discipline; the
    backfill re-runs on every (re)connect and each backfill ends with
    the :class:`Backfilled` reconcile snapshot. The plain ``async for
    event in listener`` (or ``listener.events()``) is the unfiltered
    typed stream; :meth:`holds` filters to one run; :meth:`frames` is
    the SSE face's keepalive form (yields ``None`` on the keepalive
    tick). every subscriber gets its own queue and its own end sentinel
    — the fan-out, never a partition (P7)."""

    _log = logger
    _task_name = "taskq-hitl-listener"
    _reconnect_event = "hitl-listener-reconnect"
    _overflow_event = "hitl-listener-queue-overflow-drop-oldest"
    _closed_message = "the HITL listener's connection was closed underneath the pump"

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
        super().__init__(
            pool,
            schema,
            keepalive_interval=keepalive_interval,
            backoff_initial=backoff_initial,
            backoff_max=backoff_max,
            acquire_timeout=acquire_timeout,
        )
        self._backfilled: set[str] = set()

    # ── the per-listener face ────────────────────────────────────────

    def _channels(self) -> Sequence[str]:
        return sorted(BROADCAST_CHANNELS)

    def _reconnect_fields(self) -> dict[str, object]:
        return {}

    def _queue_maxsize(self) -> int:
        return _QUEUE_MAXSIZE

    # ── the surfaces ─────────────────────────────────────────────────

    def __aiter__(self) -> AsyncGenerator[HoldEvent, None]:
        """``async for event
        in listener:`` — the typed stream directly; no ``.events()``
        lookup between the author and the events. The keepalive form
        stays on :meth:`frames` (the SSE face's)."""
        return self.events()

    async def events(self) -> AsyncGenerator[HoldEvent, None]:
        """NAMED FORM of the same surface: ``async for event in
        listener.events():`` — every yield is a :data:`HoldEvent` (the
        keepalive sentinel is filtered out; a direct consumer just
        awaits)."""
        async for event in self._iterate(None):
            if event is not None:
                yield event

    async def holds(self, run: JobId | str) -> AsyncGenerator[HoldEvent, None]:
        """One run's events —
        ``listener.holds(run=flow_id)`` — the approval stream for one
        flow, typed. The id binds through its canonical form (the same
        spelling the list/get doors accept — a braced or uppercase
        spelling yields this run's holds); a spelling that does not
        even parse as the id fails loud at the stream's setup — the
        list doors' named-refusal class, never a silent empty stream.
        The unfiltered stream still flows (the raw tail's
        surface: every schema-routed event reaches it). The
        :class:`Backfilled` reconcile passes every run's filter (it
        carries no ``run_id`` — the snapshot is the SCHEMA's open-hold
        set, and every consumer reconciles its own cards against it)."""
        run_id = str(_as_hold_id(run))
        async for event in self._iterate(None):
            if event is None:
                continue
            if isinstance(event, Backfilled) or event.run_id == run_id:
                yield event

    async def frames(self) -> AsyncGenerator[HoldEvent | None, None]:
        """Typed events + ``None`` keepalives (the
        admin feed's own discipline — a quiet stream must still say
        something, or the proxies eat it)."""
        async for event in self._iterate(self._keepalive):
            yield event

    # ── the listener's own halves of the pump ────────────────────────

    async def _backfill(self, conn: _Conn) -> None:
        """The OPEN holds, FROM the ROWS (the snapshot the zero-window
        law leans on): every still-``held`` row in the schema becomes a
        ``HoldCreated(source="backfill")`` — including a hold created
        before this listener existed. The snapshot's ids
        enter the dedup set: their own (already-queued) notifies yield
        nothing. The reconcile (the fourth member):
        after the snapshot's creations, one :class:`Backfilled` event
        carries the OPEN ids — the consumer drops any card the rows
        disown (a hold resolved/expired during the outage never
        announces its own death)."""
        rows = await conn.fetch(
            f"SELECT id, workflow_id, node_key, signal_name, created_at "  # noqa: S608 — the f-string's only interpolation is the require_schema-validated schema identifier; values never interpolate.
            f"FROM \"{self._schema}\".wf_signals WHERE status = 'held' "
            # The timer's exclusion (the reserved prefix's one
            # mechanism, applied at this face too):
            # the snapshot is the pending-approvals read — a timer hold
            # is the clock's, never a human's decision (the same split
            # HitlClient.list's SQL keeps; the create broadcast skips
            # the prefix at the source, this filter covers the rows that
            # predate any listener).
            "AND NOT starts_with(signal_name, '__timer__') "
            "ORDER BY id"
        )
        # The dedup set is per-connection (the LISTEN→snapshot race's
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
            return None  # another SCHEMA'S event (global channels — the payload routes; the isolation is this filter, P8)
        hold_id = doc.get("hold_id")
        run_id = doc.get("run_id") or doc.get("flow_id")
        if not isinstance(hold_id, str) or not isinstance(run_id, str):
            return None
        if channel == HOLD_CREATED_CHANNEL:
            # a hold the backfill already announced yields nothing — one id,
            # one created event per connection (the snapshot's dedup leg).
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
            # the door marker: the sweep's broadcast names 'timeout', the
            # abandon door's 'operator'; a foreign/legacy payload without
            # the marker reads as the sweep's (the default).
            is_operator = doc.get("abandoned_by") == "operator"
            return HoldExpired(
                hold_id=hold_id,
                run_id=run_id,
                signal=str(doc.get("signal", "")),
                node_key=str(doc.get("node_key", "")),
                abandoned_by="operator" if is_operator else "timeout",
            )
        return None
