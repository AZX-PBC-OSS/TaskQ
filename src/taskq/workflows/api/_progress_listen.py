"""the PROGRESS LISTENER (the push-primary surface — the
HitlListener's sibling): the typed, public face over the progress push —
one dedicated connection, LISTEN → BACKFILL → TAIL — fanning out to N
concurrent subscribers.

the ZERO-WINDOW CONSTRUCTION (the mid-stream re-check pattern, the
broadcast's proven shape): the connection LISTENS first, then the
backfill snapshot reads the run's STATE-channel rows. Any emission
visible to the snapshot either knocked before we listened (no knock will
ever come — the snapshot is the only witness) or after (its knock is
deduped against the snapshot by the node's ``last_seq``). A missed event
is unrepresentable by construction — not by luck, not by polling.

Every event is a POINTER (the flow, the
node, the seq — never the payload's content; the redact law holds at
the knock). A consumer that misses everything still converges by the
seq-cursor read (:func:`taskq.workflows._progress_read.progress_sse_face`);
the listener buys LATENCY, never correctness.

FAN-out, not A PARTITION (P7's law, carried): every subscriber —
``async for event in listener``, ``updates()``, ``frames()`` — gets its
own queue and its own end-of-stream sentinel; two concurrent consumers
each see every event.

The coalesce order (the backpressure rule on the
push face): every queue is drop-oldest on overflow. The writer never
blocks; the NEWEST events survive; a slow consumer loses OBSERVABILITY
first, correctness never — the row is the truth, the poll converges.

The operational landmines (the listener's own four): the
PgBouncer transaction-pooling mode silently breaks LISTEN (the
dedicated connection IS the fix); the dedicated connection is one POOL
SLOT for the stream's life (size the pool for the streams you run);
asyncpg is fork-unsafe (start the listener IN EACH WORKER after the
fork); and the run-scoped backfill is bounded by THIS run's node count
— one listener per RUN is the shape (the SSE face's own scope), never
one per browser tab beyond that.

The channel is global; the schema rides the payload (P8): the listener
filters by the payload's ``schema`` field — the isolation between the
estate's schemas IS this filter. and the run filter is the listener's
own scope: a knock whose ``flow_id`` is another run's is dropped here.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Annotated, Literal, cast
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel, Field

from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.obs import get_logger
from taskq.workflows._progress import PROGRESS_NOTIFY_CHANNEL
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.api._listen_pump import (
    DEFAULT_QUEUE_MAXSIZE,
    _BroadcastListener,  # pyright: ignore[reportPrivateUsage]  # Why: the shared LISTEN→backfill→tail body; the listener keeps only its own face (the channel, the snapshot, the decode, the knock, the log names).
    _Conn,  # pyright: ignore[reportPrivateUsage]  # Why: the pool's two faces, one union — the pump's own alias.
)

__all__ = [
    "ProgressBackfilled",
    "ProgressEvent",
    "ProgressListener",
    "ProgressUpdated",
]  # RUF022: the events, then the listener

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The listener's own queue bound (the same tradeoff the admin's
#: ``_listen.py`` and the hitl listener made: a slow consumer drops the
#: oldest event — the row is the truth, the poll converges). Bounds the
#: raw queue, every subscriber queue, and the late-subscriber replay
#: history alike.
_QUEUE_MAXSIZE = DEFAULT_QUEUE_MAXSIZE


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
    """Emitted
    after every (re)backfill, carrying the node → ``last_seq`` map the
    ROWS witness right now (this run's STATE channel). The consumer
    drops any progress card whose node the snapshot has moved past; the
    same map is the dedup's own ledger (a knock at-or-below the
    snapshot's seq yields nothing)."""

    event: Literal["progress_backfilled"] = "progress_backfilled"
    last_seqs: dict[str, int]


#: The typed event union (the discriminated kind): the backfill leg + the
#: live knocks.
ProgressEvent = Annotated[ProgressUpdated | ProgressBackfilled, Field(discriminator="event")]


class ProgressListener(_BroadcastListener[ProgressEvent]):
    """the TYPED PROGRESS LISTENER (public — the SSE face and
    programmatic consumers share it), DIRECTLY ASYNC-ITERABLE (the
    amendment — no ceremony between the author and the events):

    >>> listener = ProgressListener(pool, schema, flow_id=flow_id)
    >>> async with listener:
    ...     async for event in listener.updates():
    ...         match event:  # the CLOSED union: Updated/Backfilled
    ...             case ProgressUpdated(): ...
    ...             case ProgressBackfilled(): ...

    one dedicated connection from the pool for the stream's life (LISTEN
    is session-scoped — see the module docstring's PgBouncer landmine);
    automatic reconnect with the admin feed's backoff discipline; the
    backfill re-runs on every (re)connect and each backfill ends with
    one :class:`ProgressBackfilled` reconcile snapshot. every subscriber
    gets its own queue and its own end sentinel — the fan-out, never a
    partition (P7). :meth:`wait` is the SSE face's knock-wait (the push
    primary's own primitive; the poll tick is the belt)."""

    _log = logger
    _task_name = "taskq-progress-listener"
    _reconnect_event = "progress-listener-reconnect"
    _overflow_event = "progress-listener-queue-overflow-drop-oldest"
    _closed_message = "the progress listener's connection was closed underneath the pump"

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
        super().__init__(
            pool,
            schema,
            keepalive_interval=keepalive_interval,
            backoff_initial=backoff_initial,
            backoff_max=backoff_max,
            acquire_timeout=acquire_timeout,
        )
        self._flow_id = str(flow_id)
        if wsql is not None:
            self._backfill_sql = wsql.progress_state_read_run
        else:
            from taskq.workflows.engine import render_workflow_sql

            self._backfill_sql = render_workflow_sql(schema).progress_state_read_run
        # per-connection. node_id → the snapshot's last_seq. Reset on every
        # (re)backfill — a previous connection's announced seqs would suppress
        # nothing valid.
        self._last_seqs: dict[str, int] = {}
        # set on every event — the generator's wait returns, the replay read is
        # owed.
        self._knock = asyncio.Event()

    # ── the per-listener face ────────────────────────────────────────

    def _channels(self) -> tuple[str, ...]:
        return (PROGRESS_NOTIFY_CHANNEL,)

    def _reconnect_fields(self) -> dict[str, object]:
        return {"flow_id": self._flow_id}

    def _queue_maxsize(self) -> int:
        return _QUEUE_MAXSIZE

    def _note_activity(self) -> None:
        # set on every event — the generator's wait returns, the replay read is
        # owed. Fires at the raw notify (a knock is owed even when the decode
        # drops another run's payload) and on every emit (the backfill's
        # emissions included).
        self._knock.set()

    # ── the surfaces ─────────────────────────────────────────────────

    def __aiter__(self) -> AsyncGenerator[ProgressEvent, None]:
        """``async for event
        in listener:`` — the typed stream directly."""
        return self.updates()

    async def updates(self) -> AsyncGenerator[ProgressEvent, None]:
        """NAMED FORM of the same surface: every yield is a
        :data:`ProgressEvent` (the keepalive sentinel is filtered
        out)."""
        async for event in self._iterate(None):
            if event is not None:
                yield event

    async def frames(self) -> AsyncGenerator[ProgressEvent | None, None]:
        """Typed events + ``None`` keepalives (a
        quiet stream must still say something, or the proxies eat it)."""
        async for event in self._iterate(self._keepalive):
            yield event

    async def wait(self, timeout: float) -> bool:  # noqa: ASYNC109  # Why: a `timeout` parameter is the point — the SSE face passes its poll interval as the belt bound, not an enclosing asyncio.timeout scope; the repo's established wait-helper shape.
        """``True`` = an
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

    # ── the listener's own halves of the pump ────────────────────────

    async def _backfill(self, conn: _Conn) -> None:
        """the RUN'S STATE ROWS, FROM the SNAPSHOT (the zero-window
        law's half): every STATE-channel row this run carries becomes a
        ``ProgressUpdated(source="backfill")`` — including an emission
        written before this listener existed. The snapshot's per-node
        ``last_seq`` enters the dedup ledger: the rows' own (already-
        queued) knocks yield nothing. and the RECONCILE (the fourth
        member): one :class:`ProgressBackfilled` event carries the
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

    def _decode(self, channel: str, payload: str) -> ProgressEvent | None:
        """The raw knock → the typed event (or ``None``: another
        schema's, another run's, or a shape this decoder refuses — a
        malformed payload is dropped LOUDLY-once, never fatal: the row
        is the truth). the DEDUP: a knock whose seq is at-or-below the
        snapshot's own ``last_seq`` for that node yields nothing (one
        emission, one event per connection)."""
        try:
            parsed = _json_loads(payload)
        except Exception:
            logger.warning("progress-listener-malformed-payload", channel=channel)
            return None
        if not isinstance(parsed, dict):
            return None
        doc = cast("dict[str, object]", parsed)  # pyright: ignore[reportUnknownVariableType]  # Why: the notify payload's jsonb walk — the isinstance guard above is the runtime shape check.
        if doc.get("schema") != self._schema:
            return None  # another SCHEMA'S event (global channels — the payload routes; the isolation is this filter, P8)
        flow_id = doc.get("flow_id")
        node_id = doc.get("node_id")
        seq = doc.get("seq")
        if not isinstance(flow_id, str) or not isinstance(node_id, str) or not isinstance(seq, int):
            return None
        if flow_id != self._flow_id:
            return None  # another run's event (the listener is run-scoped — the backfill's bound)
        last = self._last_seqs.get(node_id)
        if last is not None and seq <= last:
            return None  # the dedup (the zero-window's other half): the snapshot already announced this seq
        self._last_seqs[node_id] = seq
        return ProgressUpdated(flow_id=flow_id, node_id=node_id, last_seq=seq, source="notify")
