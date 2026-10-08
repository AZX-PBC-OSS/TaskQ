"""The workflow progress emission surface (T21): ``ctx.progress`` — the
emission op, the client-side coalesce, and the auto-class projection.

DECISION (a) — THE EMISSION OP: typed + bounded. ``pct`` is an int in
0..100 or None (validated — a wrong shape is the TYPED refusal,
:class:`ProgressRefusedError`, never a silent coercion); ``message`` is
chars-capped (the ErrorInfo chars discipline); ``data`` is jsonb, capped
T18's D5 shape (truncated WITH the ``__truncated__`` marker, never
silent), optionally validated against the node's DECLARED payload schema
(the TypedGate-door pattern — the declaration is what makes a separate UI
render the emission, the context-contract law applied to progress).

DECISION (d) — THE PERSISTENCE the flush serves: the STATE channel (the
latest-wins upsert, one row per (node, channel) forever) + the STREAM
channel (the bounded per-node ring, drop-oldest with the dropped counter
ON THE RECORD).

DECISION (e) — THE ASYMMETRY, the law of this module: **observability
degrades FIRST, never correctness.** Emissions land latest-wins in an
in-memory buffer; ONE flush task drains it at the cadence bound (~20
deltas/s at the 50 ms cadence); the flush is BEST-EFFORT — its failures
are counted and logged, never raised into the body; the buffer NEVER
touches the finalize path (``aclose`` runs before the node's finalize
tx1, bounded by a timeout, and swallows its own failures). A lost
emission costs FRESHNESS; a blocked node costs CORRECTNESS. The
occurrence counter makes the coalescing HONEST: the state row's
``occurrences`` counts every emission that coalesced into it, so the
record always shows both the delivered state and the emission rate.

DECISION (b) — THE AUTO PROJECTION: the engine's node-start/terminal
events project into the SAME stream with ``class='auto'`` — ONE seq
space, ONE class discriminator, the closed kind vocabulary (DH3's fence;
the storage domain's CHECK constraints are its teeth). The projection is
ADDITIVE writes at the claim/finalize seams (the runner's), never inside
the finalize's transactions — :func:`finalize_node` is unchanged (the
zero-finalize-changes probe is a shipped pin).
"""

from __future__ import annotations

import asyncio
from typing import Any, Final

import asyncpg
import structlog

from taskq._json import dumps as _json_dumps
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import _jsonb

__all__ = [
    "CLASS_AUTO",
    "CLASS_USER",
    "DATA_MAX_BYTES",
    "FLUSH_CADENCE_S",
    "KIND_NODE_STARTED",
    "KIND_NODE_TERMINAL",
    "KIND_PROGRESS",
    "KIND_VOCABULARY",
    "MESSAGE_MAX",
    "MESSAGE_TERMINAL_MAX",
    "PROGRESS_RING_BOUND",
    "STREAM_CHANNEL",
    "ProgressEmitter",
    "ProgressRefusedError",
    "project_auto_event",
    "validate_emission",
]

# ── THE CLOSED VOCABULARY (decision b — DH3's fence) ─────────────────────

#: The user class: emissions the BODY made (``ctx.progress``).
CLASS_USER: Final[str] = "user"
#: The auto class: the ENGINE's projection (the node's claim/terminal
#: seams — additive writes, never second-guessing the ledger, which owns
#: the state).
CLASS_AUTO: Final[str] = "auto"

#: The user class's only kind.
KIND_PROGRESS: Final[str] = "progress"
#: The auto class: the claim seam's projection.
KIND_NODE_STARTED: Final[str] = "wf.node.started"
#: The auto class: the finalize seam's projection.
KIND_NODE_TERMINAL: Final[str] = "wf.node.terminal"

#: The closure itself: every kind the stream may carry, validated at emit
#: AND enforced by the storage domain's CHECK (01.00.29). A third
#: vocabulary cannot sprout — the fleet's SSE-fragmentation fossil is
#: structurally closed here.
KIND_VOCABULARY: Final[frozenset[str]] = frozenset(
    {KIND_PROGRESS, KIND_NODE_STARTED, KIND_NODE_TERMINAL}
)

#: The STATE channel's row for a node's stream counters (the dropped
#: counter's home — the honest emitted-vs-delivered pair).
STREAM_CHANNEL: Final[str] = "__stream__"

# ── THE BOUNDS (decision a + e) ──────────────────────────────────────────

#: The stream ring's per-node bound (decision d; T18's retention owns the
#: bounds — the sweep arm prunes leaked rings back to it).
PROGRESS_RING_BOUND: Final[int] = 64
#: The coalesce cadence bound — the PoC's ~20 deltas/s measured shape.
FLUSH_CADENCE_S: Final[float] = 0.05
#: The emission op's message cap (the ErrorInfo chars discipline).
MESSAGE_MAX: Final[int] = 1024
#: The finalize's terminal projection's message cap (the ledger terminal's
#: own 500-char rule — the projection carries no traceback).
MESSAGE_TERMINAL_MAX: Final[int] = 500
#: The emission op's data cap (T18's D5 cap shape — the tors call shape:
#: truncate WITH the marker, never silently).
DATA_MAX_BYTES: Final[int] = 8 * 1024

#: The flush-loss log (the asymmetry's LOUDNESS half — the PoC's
#: red-team note: a systematically failing flush is invisible unless the
#: record stays loud). The FIRST loss warns (a body whose flushes all
#: fail must not look healthy in the logs); subsequent losses ride the
#: emitter's counted record (``write_errors``) at debug.
logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


class ProgressRefusedError(TypeError):
    """A wrong-shaped emission — the TYPED REFUSAL (decision a's door).

    Raised for a ``pct`` that is not an int in 0..100, or ``data`` that
    violates the node's declared payload schema. NEVER raised for
    infrastructure failures — those are the flush's counted, logged
    best-effort path (the asymmetry: the refusal is an AUTHORING error
    the body must see; a lost write is an observability degradation it
    must not)."""


def validate_emission(
    pct: int | None,
    message: str | None,
    data: dict[str, Any] | None,
    schema_decl: type[Any] | None,
) -> tuple[int | None, str | None, dict[str, Any] | None]:
    """The emission op's typed gate — validate + bound, then hand back the
    shape the flush writes. A wrong shape is :class:`ProgressRefusedError`.

    ``schema_decl`` is the node's DECLARED payload schema (the
    TypedGate-door pattern): a pydantic model the ``data`` must satisfy —
    the declaration is what makes a separate UI render the emission (the
    context-contract law). A declaration the data violates is a typed
    refusal, never a silent pass."""
    if pct is not None:
        # The isinstance checks look unnecessary to a type checker because
        # the annotations say int | None — body code is under no such
        # discipline at runtime, and the confused types are exactly what
        # this gate exists for (the jobs ctx.progress gate's own pattern).
        if isinstance(pct, bool) or not isinstance(pct, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ProgressRefusedError(f"pct must be int 0..100, got {type(pct).__name__}")
        if not 0 <= pct <= 100:
            raise ProgressRefusedError(f"pct must be int 0..100, got {pct}")
    if message is not None and not isinstance(message, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        message = str(message)
    if message is not None:
        message = message[:MESSAGE_MAX]
    if data is not None:
        if not isinstance(data, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ProgressRefusedError(f"data must be a dict, got {type(data).__name__}")
        if schema_decl is not None:
            from pydantic import ValidationError

            try:
                schema_decl.model_validate(data)
            except ValidationError as exc:
                raise ProgressRefusedError(
                    f"data violates the node's declared progress schema: {exc}"
                ) from exc
        data = _cap_data(data)
    return pct, message, data


def _cap_data(data: dict[str, Any]) -> dict[str, Any]:
    """T18's D5 cap shape: oversize data truncated WITH THE MARKER (never
    silently — the reader must know it read a truncation)."""
    raw = _json_dumps(data)
    if len(raw) <= DATA_MAX_BYTES:
        return data
    return {"__truncated__": len(raw)}


class ProgressEmitter:
    """One node attempt's emission buffer — the client-side coalesce
    (DH2's fence) + the honest counters.

    THE COALESCE: emissions land LATEST-WINS in the buffer; ONE flush
    task drains it at the cadence bound. The occurrence counter makes the
    coalescing honest — the STATE row counts every emission that
    coalesced into it, so the record shows both the delivered state and
    the emission rate.

    THE ASYMMETRY (decision e): :meth:`progress` never awaits the
    network; the flush task is best-effort — failures are counted
    (``write_errors``) and logged, never raised into the body;
    :meth:`aclose` is bounded and swallowing (it runs BEFORE the node's
    finalize — the buffer never touches the finalize path). A body whose
    every flush fails still terminalizes normally: the emission is
    best-effort, proven by pin.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        wsql: WorkflowSql,
        *,
        flow_id: JobId,
        node_id: JobId,
        ring_bound: int = PROGRESS_RING_BOUND,
        cadence_s: float = FLUSH_CADENCE_S,
        schema_decl: type[Any] | None = None,
        enabled: bool = True,
    ) -> None:
        self._pool = pool
        self._wsql = wsql
        self._flow_id = flow_id
        self._node_id = node_id
        self._ring_bound = ring_bound
        self._cadence_s = cadence_s
        self._schema_decl = schema_decl
        self._enabled = enabled
        # THE HONEST COUNTERS: the body's emission calls; the STATE
        # writes; the STREAM appends; the ring's drop-oldest casualties;
        # the flush failures (best-effort, never raised).
        self.emitted = 0
        self.flushes = 0
        self.appended = 0
        self.dropped_total = 0
        self.write_errors = 0
        self._pending: dict[str, Any] | None = None
        self._pending_n = 0
        self._flush_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def schema_decl(self) -> type[Any] | None:
        """The node's DECLARED payload schema (the TypedGate-door pattern)
        — the validation contract ``ctx.progress``'s typed gate enforces."""
        return self._schema_decl

    # ── the op the body calls (via ctx.progress) ────────────────────────
    async def emit(
        self,
        pct: int | None,
        message: str | None,
        data: dict[str, Any] | None,
    ) -> None:
        """The validated path (``ctx.progress``'s delegate): the typed
        gate runs FIRST (a wrong shape is :class:`ProgressRefusedError`,
        raised INTO the body — an authoring error is the body's problem),
        then the buffer takes it latest-wins and the cadence flush is
        armed. Never awaits the network."""
        pct_v, message_v, data_v = validate_emission(pct, message, data, self._schema_decl)
        self.submit(pct_v, message_v, data_v)

    def submit(
        self,
        pct: int | None,
        message: str | None,
        data: dict[str, Any] | None,
    ) -> None:
        """Buffer one ALREADY-VALIDATED emission (latest-wins) and arm the
        cadence flush. Sync — the caller (``ctx.progress``) validated the
        shape first (the typed gate is :func:`validate_emission`'s, never
        repeated here)."""
        self.emitted += 1
        if not self._enabled:
            return  # counted, written NOWHERE (the best-effort asymmetry)
        self._pending = {"pct": pct, "message": message, "data": data}
        self._pending_n += 1
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_at_cadence())

    async def _flush_at_cadence(self) -> None:
        """THE COALESCE: hold the cadence, write the latest, let the
        emissions that arrived during the hold ride along (the occurrence
        counter counts them)."""
        await asyncio.sleep(self._cadence_s)
        await self._flush_once()

    async def _flush_once(self) -> None:
        """One drain: the STREAM append+trim (the drop count RETURNED) +
        the STATE upsert (latest-wins) + the stream counters row — three
        statements, one connection, best-effort throughout."""
        async with self._lock:
            if self._pending is None:
                return
            snap, n = self._pending, self._pending_n
            self._pending = None
            self._pending_n = 0
        self.flushes += 1
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    self._wsql.progress_stream_append,
                    self._node_id,
                    self._flow_id,
                    CLASS_USER,
                    KIND_PROGRESS,
                    _jsonb(snap),
                    self._ring_bound,
                )
                assert row is not None  # the statement always returns its row
                seq = int(row["seq"])
                dropped = int(row["dropped"])
                self.dropped_total += dropped
                await conn.execute(
                    self._wsql.progress_state_upsert,
                    self._node_id,
                    "progress",
                    snap["pct"],
                    snap["message"],
                    _jsonb(snap["data"]) if snap["data"] is not None else None,
                    n,
                    0,
                    seq,
                )
                # The stream's own counters row (channel=STREAM_CHANNEL) —
                # the dropped counter ON THE RECORD (the honest
                # emitted-vs-delivered pair, DH2's fence).
                await conn.execute(
                    self._wsql.progress_state_upsert,
                    self._node_id,
                    STREAM_CHANNEL,
                    None,
                    None,
                    None,
                    1,
                    dropped,
                    seq,
                )
                self.appended += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # THE ASYMMETRY: never the body's problem — counted on the
            # record (the flush_lost surface) and swallowed. The FIRST
            # loss warns (the loudness half); the rest ride the counted
            # record at debug.
            self.write_errors += 1
            if self.write_errors == 1:
                logger.warning(
                    "progress_flush_lost",
                    kind="progress_flush_lost",
                    flow_id=str(self._flow_id),
                    node_id=str(self._node_id),
                    error=str(exc)[:200],
                    note="the emission is best-effort: the node's correctness "
                    "is unaffected; freshness degrades until the next flush",
                )
            else:
                logger.debug(
                    "progress_flush_lost",
                    kind="progress_flush_lost",
                    flow_id=str(self._flow_id),
                    node_id=str(self._node_id),
                    write_errors=self.write_errors,
                )

    async def aclose(self) -> None:
        """The attempt's last flush — BEST-EFFORT and BOUNDED: never
        raises, never outlives its timeout, never blocks the finalize
        (the caller runs this BEFORE the finalize tx1, never inside
        it)."""
        if self._flush_task is not None and not self._flush_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(self._flush_task), timeout=1.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug(
                    "progress_flush_lost",
                    kind="progress_flush_lost",
                    flow_id=str(self._flow_id),
                    node_id=str(self._node_id),
                    phase="aclose_inflight",
                    error=str(exc)[:200],
                )
        try:
            await asyncio.wait_for(self._flush_once(), timeout=1.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.write_errors += 1
            logger.debug(
                "progress_flush_lost",
                kind="progress_flush_lost",
                flow_id=str(self._flow_id),
                node_id=str(self._node_id),
                phase="aclose",
                write_errors=self.write_errors,
                error=str(exc)[:200],
            )


async def project_auto_event(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    node_id: JobId,
    kind: str,
    payload: dict[str, Any],
    ring_bound: int = PROGRESS_RING_BOUND,
) -> int:
    """The engine's node-start/terminal PROJECTION (decision b): the same
    stream, the ``class='auto'`` discriminator, the ONE seq space. The
    kind is validated against the closed vocabulary (DH3's fence — no
    third vocabulary can sprout; the storage CHECK is the backstop).

    Best-effort by the caller's contract (the runner's seams wrap this —
    a failed projection is a logged freshness loss, never a node
    failure). Returns the appended seq."""
    if kind not in KIND_VOCABULARY:
        raise ValueError(
            f"kind {kind!r} is outside the closed vocabulary "
            f"({sorted(KIND_VOCABULARY)}) — the stream's vocabulary is "
            "closed by design (DH3's fence)"
        )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            wsql.progress_stream_append,
            node_id,
            flow_id,
            CLASS_AUTO,
            kind,
            _jsonb(payload),
            ring_bound,
        )
        assert row is not None  # the statement always returns its row
    return int(row["seq"])
