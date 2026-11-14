"""the EMIT TX — the streaming source's per-page statement group,
the design's one new primitive (PROOF.md §6a).

The shipped fork inserts children only inside the parent's terminal-mark
tx1 (fork-at-finalize): a child's existence is INDEXED to its parent's
death. A STREAMING source must do the opposite — emit children while
ITSELF staying ``running`` — so its pages stream (children claim +
terminal while the source is mid-fetch; the spike measured 236 chain
claims while the source was still streaming).

EMIT TX, one transaction
  1. the children INSERT   (the certified FORK_CHILDREN shape re-bound)
  2. the edge rows         (the certified FORK_EDGES shape re-bound —
                           child→source, fail_closed: a mid-stream source
                           death is the honest fail-closed parent)
  3. the CURSOR CHECKPOINT (the source row's own metadata under the FULL
                           claim fence — no new table, no new column)

Atomicity is the resume invariant: a kill at any statement window —
including after the cursor write but before the commit — rolls the whole
tx back; the reclaim re-pends the source (it never finalized), the
re-claim re-emits exactly the lost page, never a partial one, never a
duplicate one. the FENCE IS the CLAIM: a zombie source (its attempt
superseded) updates nothing in step 3 → :class:`EmitFencedError` → the
children and edges roll back with the tx — a zombie cannot emit.

The ledger's
claim-arbiter column is a SMALLINT — the ceiling is 32767. A page whose
record's map_index crosses it raises :class:`MapIndexExhaustedError` at
the emit door (typed, named, the ceiling's number on the record); the
valid page-mates' tx commits first (the poison record kills itself,
never its mates), and the deterministic route terminalizes the source.
The ceiling is per-map-index, not per-run.

``emit_batch`` accepts ``_window_hook``
(the attack suite's seam), fired
after EACH statement with ``emit:{1|2|3}``; the pins terminate the
backend there (a real server-side kill) and prove zero re-emitted, zero
lost children.

The MAX-IN-FLIGHT bound is DECLARED at the workflow level (the runner
supplies the workflow's declaration; ``None`` = explicit unbounded). Every
whole page passes the same admission lock and count immediately before its
write. Ordinary saturation raises :class:`EmitBackpressure`: it writes no
page and the runner parks the claimed source without a timer, lease, worker
slot, failed ledger outcome, or retry-budget charge. Truly parked sources
do not count as their siblings' outstanding work.

Wake is a fresh post-commit read, never a jobs trigger or deferred in-tx
lookup. The engine, direct tick, post-park completion, and the bounded leader
recovery all call :func:`wake_emit_backpressure`. Its row+root locks,
whole-page recheck, admission lock, and atomic live-marker removal preserve
the bound under concurrent wakes. Durable rows, not NOTIFY, are authority.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, cast

import structlog

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.constants import wake_channel
from taskq.obs import get_logger
from taskq.workflows._fork import FORK_CHUNK
from taskq.workflows._policy import canonical_retry_kind, row_retry_curve
from taskq.workflows._sql_finalize import (
    EMIT_BACKPRESSURE_CANDIDATE_LOCK_SQL,
    EMIT_BACKPRESSURE_CANDIDATES_SQL,
    EMIT_BACKPRESSURE_CURSOR_CANDIDATES_SQL,
    EMIT_BACKPRESSURE_FLOW_CANDIDATES_SQL,
    EMIT_BACKPRESSURE_ROOT_LOCK_SQL,
    EMIT_BACKPRESSURE_WAKE_SQL,
)
from taskq.workflows._types import MAP_INDEX_CEILING, EmitChild, _jsonb, _row_metadata

if TYPE_CHECKING:
    import asyncpg

    from taskq.backend._protocol import ConnLike
    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "EMIT_CURSOR_KEY",
    "EMIT_MAX_IN_FLIGHT_DEFAULT",
    "EmitBackpressure",
    "EmitCancelledError",
    "EmitFencedError",
    "EmitStaleClaimError",
    "EmitWakeResult",
    "MapIndexExhaustedError",
    "PageDivergedError",
    "emit_batch",
    "wake_emit_backpressure",
    "wake_emit_backpressure_safely",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The cursor checkpoint's metadata key on the source row itself (the
#: checkpoint rides the row's jsonb — no new table, no new column, no
#: migration). The value is the caller's cursor document, merged at the
#: top level of the row's metadata under this one key.
EMIT_CURSOR_KEY: Final[str] = "emit_cursor"

#: the bound a workflow gets when it does not declare one. The author opts out
#: loudly (``max_in_flight=None`` on the workflow declaration — the explicit
#: unbounded, never a silent default).
EMIT_MAX_IN_FLIGHT_DEFAULT: Final[int] = 1000

#: The window-hook type: fired after each in-tx statement with the
#: window's name (``emit:1`` children, ``emit:2`` edges, ``emit:3``
#: cursor) and the connection — the statement-window kill pins' seam.
#: (``ConnLike`` spelled as ``Any`` at runtime — the alias is a
#: TYPE_CHECKING-only import; the hook's contract is the statement
#: boundary it fires at, not the connection's type.)
WindowHook = Callable[[str, Any], Awaitable[None]]


class EmitFencedError(RuntimeError):
    """The emit lost its exact claim or the run's durable terminal fence."""


class EmitCancelledError(EmitFencedError):
    """Cancellation won before this page's cursor CAS; rollback is whole-page."""


class EmitStaleClaimError(EmitFencedError):
    """The attempt's claim identity no longer owns the source row."""


#: ``jobs.map_index`` is a smallint — the ceiling is 32767. A page whose
#: record's map_index crosses it raises :class:`MapIndexExhaustedError` at the
#: emit door (typed, named, the ceiling's number on the record); the valid
#: page-mates' tx commits first (the poison record kills itself, never its
#: mates), and the deterministic route terminalizes the source. The ceiling is
#: per-map-index, not per-run: a map's children count against the same smallint
#: the ledger's claim-arbiter keys on.


class PageDivergedError(RuntimeError):
    """The emit's children INSERT
    hit the idempotency arbiter — the
    page's rows ALREADY EXIST under the page's ``(flow, emit,
    map_index, step)`` keys — while the source's cursor checkpoint says
    the page never ran. The cursor and the row history have diverged:
    the atomic emit tx can produce this only from a cursor the row
    history outgrew (a hand-rewound checkpoint, a resumed source whose
    body re-pages an already-committed page). the RAW DEATH IT FIXES:
    the raw ``UniqueViolationError`` escaped the tx, the body boundary
    laddered it as transient, and the re-claim re-read the same cursor
    and collided again — the ladder burned to a raw-typed terminal,
    never named. the NAMED DEATH: this error terminalizes the source
    LOUDLY (the deterministic route — re-running cannot fix a diverged
    cursor), and the message carries the remedy: RE-SYNC the CURSOR (the
    checkpoint is the source row's metadata under
    :data:`EMIT_CURSOR_KEY` — point it at the last page the rows prove
    committed). the TX ROLLED BACK: the divergent page wrote NOTHING."""

    def __init__(self, flow_id: JobId, source_id: JobId, detail: str) -> None:
        super().__init__(
            f"the emit's page already exists (the idempotency arbiter "
            f"refused the re-insert): flow {flow_id}, source {source_id} — "
            f"the resume's cursor diverged from the row history. "
            f"THE REMEDY: re-sync the cursor (the source row's metadata "
            f"under '{EMIT_CURSOR_KEY}') to the last page the rows prove "
            f"committed; re-running without the re-sync collides again. "
            f"{detail}"
        )


class MapIndexExhaustedError(RuntimeError):
    """A page carried a record whose ``map_index`` is past the smallint
    ceiling (:data:`taskq.workflows._types.MAP_INDEX_CEILING` — 32767;
    the ledger's claim-arbiter column is a smallint). the BATCH
    SEMANTICS: a poison record kills ITSELF loudly, never ITS MATES —
    the emit emits the VALID page-mates first (their tx commits; the
    valid work survives), then raises THIS naming the offending record.
    The smallint cast's raw DataError escaped
    MID-TX and rolled the whole page back — the valid page-mates died
    with the poison record, an accidental untyped ceiling. The
    deterministic route terminalizes the source with the ceiling on the
    record (re-running cannot grow a record's identity past the
    column's own domain)."""


EMIT_REFUSAL_STATUS_SQL_TEMPLATE = """\
SELECT j.status, j.metadata, j.cancel_phase, root.status AS root_status
FROM {schema}.jobs AS j
LEFT JOIN {schema}.jobs AS root
  ON root.id = (j.metadata->>'flow_id')::uuid
WHERE j.id = $1
"""


class EmitBackpressure(Exception):  # noqa: N818  # Why: mandated typed control-signal vocabulary, not an error ladder.
    """Ordinary capacity control: this whole page did not fit.

    No child, edge, or cursor write has occurred when this escapes. Only the
    owning source runner may translate it into the dedicated park law; it is
    deliberately outside ``Snooze``/``RetryAfter`` and the failure ladder.
    ``backpressure_timeout_s`` at the public emit boundary is diagnostic-only
    compatibility input, never a failure deadline for this control.
    """

    def __init__(self, *, page_width: int, bound: int, outstanding: int) -> None:
        self.page_width = page_width
        self.bound = bound
        self.outstanding = outstanding
        super().__init__(
            f"emit page width {page_width} plus {outstanding} outstanding rows "
            f"exceeds max_in_flight={bound}; the source parks without spending "
            "a retry until a committed terminal makes the whole page fit"
        )


@dataclass(frozen=True, slots=True)
class EmitWakeResult:
    """One bounded marker pass: committed promotions and its resume cursor."""

    woken: int
    next_cursor: tuple[JobId, JobId] | None = None


@dataclass
class EstateWakeCursor:
    """The estate wake pass's FAIR cursor (the wrap/round-robin law).

    A pure keyset cursor advances monotonically and never revisits a
    row it passed over: under continuous arrivals every pass is full,
    the cursor never resets on its own, and a parked row whose fit
    arrives AFTER the cursor passed it starves forever. Merely
    alternating the pass START between the cursor and the beginning is
    not fair either: a blocked prefix at least as wide as the batch
    re-fills every from-start pass's window, and a fitted waiter
    behind it is never examined.

    The fair cursor alternates each pass between a FORWARD STRIDE (a
    keyset batch from the cursor) and a WRAP (a from-start pass with
    ``exhaust=True`` — the wake pages the WHOLE matching set in
    chunked transactions, whatever the population): every parked row
    is examined at least once per two-pass cycle for ANY population,
    so a row whose fit arrives at any time wakes within two estate
    passes. The wrap never advances the stride cursor (it re-examines
    everything next cycle); a short stride pass (the estate's tail)
    resets the stride to the beginning by itself. The leader's
    recovery arm owns one of these."""

    cursor: tuple[JobId, JobId] | None = None
    _next_is_wrap: bool = False
    _issued_wrap: bool = False

    def next_pass(self, batch_size: int) -> tuple[tuple[JobId, JobId] | None, int, bool]:
        """The next pass's ``(after, batch_size, exhaust)``: a forward
        keyset stride at the caller's batch, then a from-start wrap that
        pages the whole parked population."""
        if self._next_is_wrap:
            self._next_is_wrap = False
            self._issued_wrap = True
            return None, batch_size, True
        self._issued_wrap = False
        self._next_is_wrap = True
        return self.cursor, batch_size, False

    def observe(self, result: EmitWakeResult) -> None:
        """Fold one pass's result. Only a STRIDE pass moves the cursor (a
        wrap re-examines everything next cycle and must not skip the tail
        it wrapped over); a short stride pass (next_cursor None — the
        estate's tail) restarts the stride from the beginning by itself."""
        if self._issued_wrap:
            return
        self.cursor = result.next_cursor


#: The wake's per-transaction lock budget's own ceiling: one chunk's
#: advisory locks (per distinct flow) plus row locks stay far below
#: ``max_locks_per_transaction * max_connections`` at any deployment shape.
_WAKE_MAX_CHUNK: Final[int] = 1000


class _AdmissionLostError(Exception):
    """The locked recheck lost capacity; roll back before typed parking."""


async def _outstanding(conn: ConnLike, wsql: WorkflowSql, flow_id: JobId, source_id: JobId) -> int:
    """The run's non-terminal row count (the ledger's truth), excluding
    the source's own claim row."""
    n = await conn.fetchval(wsql.emit_in_flight, flow_id, source_id)
    return int(n or 0)


async def _emit_tx(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    source_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    children: Sequence[EmitChild],
    cursor: dict[str, object],
    child_ids: list[JobId],
    max_attempts: int,
    retry_kind: str,
    _window_hook: WindowHook | None,
    run_priority: int | None = None,
) -> None:
    """The emit tx's body (the caller owns the transaction): the children
    INSERT, the edge rows, the cursor checkpoint — the certified fork
    shapes re-bound, chunked parallel arrays, ids minted app-side.

    The children INSERT is the page's
    only unique-violation surface (the
    idempotency arbiter — the composite ``(scope, key)`` index). The raw
    ``UniqueViolationError`` used to escape whole: the body boundary
    laddered it transient, the re-claim re-read the same cursor and
    collided again — the ladder burned to a raw-typed death. Here the
    arbiter's refusal is :class:`PageDivergedError` — the named, loud
    death carrying the remedy (re-sync the cursor) — and the tx rolls
    back WHOLE (the divergent page wrote nothing)."""
    # Function-local, never module level (the module's own TYPE_CHECKING
    # import law — the runtime name is needed only at the except).
    import asyncpg

    for start in range(0, len(children), FORK_CHUNK):
        chunk = children[start : start + FORK_CHUNK]
        ids = child_ids[start : start + FORK_CHUNK]
        try:
            await conn.execute(
                wsql.emit_children,
                ids,
                [c.actor for c in chunk],
                [c.queue for c in chunk],
                [_jsonb(c.payload) for c in chunk],
                [c.max_attempts or max_attempts for c in chunk],
                [c.retry_kind or canonical_retry_kind(retry_kind) for c in chunk],
                [source_id] * len(chunk),
                [c.map_index for c in chunk],
                [c.step_key for c in chunk],
                [c.trace_id for c in chunk],
                [
                    _jsonb(
                        _row_metadata(
                            flow_id,
                            rate_limits=c.rate_limits,
                            reservations=c.reservations,
                            retry=c.retry,
                        )
                    )
                    for c in chunk
                ],
                # The scope is the fork's own (one scope per flow) — the
                # uniqueness pair is (scope, key), the key carries the
                # emit scope + the per-record index.
                [f"workflow:{flow_id}"] * len(chunk),
                [f"wf:{flow_id}:emit:{c.map_index}:{c.step_key}" for c in chunk],
                # one cohort per run (the flow_id), the fork's own law at the
                # page inserts.
                [str(flow_id)] * len(chunk),
                [run_priority if run_priority is not None else (c.priority or 0) for c in chunk],
                [c.start_to_close for c in chunk],
                *[[row_retry_curve(c.retry_policy)[column] for c in chunk] for column in range(4)],
            )
        except asyncpg.UniqueViolationError as exc:
            raise PageDivergedError(
                flow_id,
                source_id,
                f"the colliding chunk carried map_indexes "
                f"{[c.map_index for c in chunk]} (step keys "
                f"{sorted({c.step_key for c in chunk})}); the tx rolled "
                "back whole — nothing was written",
            ) from exc
        if _window_hook is not None:
            await _window_hook("emit:1", conn)
        await conn.execute(
            wsql.emit_edges,
            ids,
            [source_id] * len(chunk),
            [flow_id] * len(chunk),
            # fail_closed: the edges reconcile emit debt (the fork's
            # own edges' role), and a mid-stream source death is the
            # honest fail-closed parent.
            ["fail_closed"] * len(chunk),
        )
        if _window_hook is not None:
            await _window_hook("emit:2", conn)

    # the cursor checkpoint — under the full claim fence. The metadata merge is
    # a top-level concat under one key: the caller's cursor document is never
    # diffed field-by-field, and the row's other metadata (flow_id, the hold
    # stamps) survives.
    fenced = await conn.fetchval(
        wsql.emit_cursor,
        source_id,
        _jsonb({EMIT_CURSOR_KEY: cursor}),
        worker_id,
        attempt,
        claim_epoch,
    )
    if _window_hook is not None:
        await _window_hook("emit:3", conn)
    if fenced is None:
        # Read the durable discriminator only after the atomic fence refused;
        # the reader performs no writes and cannot turn stale into cancelled.
        from taskq.workflows.api._sql_runner import render_sql

        observed = await conn.fetchrow(
            render_sql(EMIT_REFUSAL_STATUS_SQL_TEMPLATE, wsql.schema),
            source_id,
        )
        terminal = {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
        raw_metadata: object = observed["metadata"] if observed is not None else {}
        metadata = _json_loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        cancelled = observed is not None and (
            observed["root_status"] in terminal
            or observed["cancel_phase"] != 0
            or (isinstance(metadata, dict) and "cancel_phase" in metadata)
        )
        if cancelled:
            raise EmitCancelledError(
                f"workflow cancellation fenced source {source_id}'s page; "
                "the children, edges and cursor rolled back atomically"
            )
        raise EmitStaleClaimError(
            f"the emit's cursor checkpoint lost this exact source claim "
            f"({source_id}, worker, attempt, claim_epoch); rollback wrote nothing"
        )


def _raise_poison(flow_id: JobId, source_id: JobId, poison: list[EmitChild]) -> None:
    """Name the page's poison records and raise (the ceiling's loud
    terminal — after the valid page-mates' tx committed, so the valid
    work survives on the record; the batch semantics, the error's
    docstring)."""
    raise MapIndexExhaustedError(
        f"the page's records past the map_index smallint ceiling "
        f"({MAP_INDEX_CEILING}): flow {flow_id}, source {source_id}, the "
        f"offending records' step keys {sorted({c.step_key for c in poison})}, "
        f"map_indexes {[c.map_index for c in poison]} — the VALID "
        "page-mates COMMITTED above (a poison record kills itself loudly, "
        "never its mates); the ceiling is documented on MAP_INDEX_CEILING"
    )


async def emit_batch(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    source_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    children: Sequence[EmitChild],
    cursor: dict[str, object],
    max_attempts: int = 3,
    retry_kind: str = "transient",
    max_in_flight: int | None = None,
    backpressure_timeout_s: float = 30.0,
    _window_hook: WindowHook | None = None,
    run_priority: int | None = None,
) -> tuple[JobId, ...]:
    """Emit one page of chain starts: the children + the edges + the
    cursor checkpoint, one transaction, while the source stays
    ``running``.

    The REFUTED-CLAIM DISCIPLINE is enforced at the door: every child
    carries its ``map_index`` (the discriminator the fork's idempotency
    key and the ledger's arbiter both ride — a ``None`` is refused before
    any row exists) and its ``trace_id`` (the per-record lineage).

    The idempotency keys are PARENT-SCOPED at the emit scope —
    ``wf:{flow}:emit:{map_index}:{step_key}`` — distinct from the
    fork-at-finalize keys (``wf:{flow}:{parent_step}:{step}:{idx}``): a
    source that also forks at its finalize can never key-collide with its
    own emitted chains.

    *max_in_flight* is the workflow's declared whole-page bound (``None`` is
    explicit unbounded). Saturation raises :class:`EmitBackpressure` before
    any write; the source runner then releases its physical claim and parks
    without a timer. The locked second gate remains the actual bound defense
    against concurrent emitters. The legacy timeout argument is retained only
    as diagnostic-age input and never turns healthy capacity into failure.

    Returns the minted child ids (uuid7 via the seam). Raises
    :class:`EmitFencedError` when the claim was superseded (nothing
    written). Raises :class:`EmitBackpressure` when the whole page cannot fit
    (nothing written and no elapsed-time failure). Raises
    :class:`PageDivergedError` when the page's rows already exist (the
    cursor diverged from the row history — the tx rolled back, nothing
    written). Raises :class:`MapIndexExhaustedError` when a record's
    ``map_index`` is past the smallint ceiling — after emitting the
    valid page-mates (the batch semantics: a poison record kills ITSELF
    loudly, never its mates — the valid work SURVIVES on the record)."""
    for child in children:
        if child.map_index is None:  # pyright: ignore[reportUnnecessaryComparison]  # Why: the runtime guard behind the typed door — EmitChild.map_index's int type is the declaration; a hand-built child carrying None must be refused here (the refuted-claim discipline), never trusted to the checker.
            raise ValueError(
                f"emit child {child.step_key!r} has map_index=None — the "
                "emit stamps map_index per record (the discriminator the "
                "fork's idempotency key and the ledger's arbiter both "
                "ride); without it every record's children collide onto "
                "one row (the measured 198 UniqueViolations)"
            )
        if not child.trace_id:
            raise ValueError(
                f"emit child {child.step_key!r} (map_index="
                f"{child.map_index}) has no trace_id — the emit stamps "
                "the per-record trace (the one-query lineage)"
            )

    # jobs.map_index is a smallint — a record past :data:`MAP_INDEX_CEILING`
    # used to die as the raw DataError mid-tx, taking the valid page-mates down
    # with the rollback (an accidental untyped ceiling). the batch semantics:
    # the poison record kills itself loudly, never its mates — the valid
    # page-mates emit (their tx commits below), then the poison is named and
    # raised (the deterministic route terminalizes the source). An all-poison
    # page is refused before any work.
    poison = [c for c in children if not 0 <= int(c.map_index) <= MAP_INDEX_CEILING]
    if not poison:
        valid = list(children)
    else:
        valid = [c for c in children if c not in poison]
        if not valid:
            raise MapIndexExhaustedError(
                f"the emit's page is entirely past the map_index smallint "
                f"ceiling ({MAP_INDEX_CEILING}): flow {flow_id}, source "
                f"{source_id}, the offending records' step keys "
                f"{sorted({c.step_key for c in poison})}, map_indexes "
                f"{[c.map_index for c in poison]} — nothing was written "
                "(the record's identity cannot ride the ledger's smallint "
                "arbiter; the ceiling is documented on MAP_INDEX_CEILING)"
            )

    child_ids = [JobId(new_uuid()) for _ in valid]
    # Compatibility only: wait age belongs to observability, not a provider or
    # source retry deadline. Deliberately no clock/poll loop survives here.
    _ = backpressure_timeout_s

    if max_in_flight is not None:
        # an admission that can never succeed is refused at the door.
        if len(valid) > max_in_flight:
            raise ValueError(
                f"the emit's page ({len(valid)} children) can never fit "
                f"under the declared max_in_flight={max_in_flight} — the "
                "admission is impossible (shrink the page or raise the "
                "bound; the loud config refusal, never a silent starve)"
            )

        async with pool.acquire() as conn:
            count = await _outstanding(conn, wsql, flow_id, source_id)
        if count + len(valid) > max_in_flight:
            raise EmitBackpressure(page_width=len(valid), bound=max_in_flight, outstanding=count)
        inside = count
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(wsql.emit_admission_lock, str(flow_id))
                inside = await _outstanding(conn, wsql, flow_id, source_id)
                if inside + len(valid) > max_in_flight:
                    raise _AdmissionLostError
                await _emit_tx(
                    conn,
                    wsql,
                    flow_id=flow_id,
                    source_id=source_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                    children=valid,
                    cursor=cursor,
                    child_ids=child_ids,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    _window_hook=_window_hook,
                    run_priority=run_priority,
                )
            if poison:
                _raise_poison(flow_id, source_id, poison)
            return tuple(child_ids)
        except _AdmissionLostError:
            raise EmitBackpressure(
                page_width=len(valid), bound=max_in_flight, outstanding=inside
            ) from None

    async with pool.acquire() as conn, conn.transaction():
        await _emit_tx(
            conn,
            wsql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            children=valid,
            cursor=cursor,
            child_ids=child_ids,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            _window_hook=_window_hook,
            run_priority=run_priority,
        )
    # the poison's loud death is outside the tx (the valid work committed).
    if poison:
        _raise_poison(flow_id, source_id, poison)
    return tuple(child_ids)


@dataclass(frozen=True, slots=True)
class _WaitDescriptor:
    page_width: int
    bound: int


def _metadata_document(raw: object) -> dict[str, object]:
    decoded: object = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise ValueError("a live emit-backpressure row must have object metadata")
    return cast("dict[str, object]", decoded)


def _counter(record: dict[str, object], key: str, *, minimum: int = 0) -> int:
    value = record.get(key)
    if type(value) is not int or value < minimum:
        raise ValueError(
            f"wf_emit_backpressure.{key} must be an integer >= {minimum}; "
            f"got {type(value).__name__} {value!r}"
        )
    return value


def _timestamp(record: dict[str, object], key: str, *, optional: bool = False) -> None:
    value = record.get(key)
    if optional and value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"wf_emit_backpressure.{key} must be a UTC timestamp string")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError(f"wf_emit_backpressure.{key} is not a timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"wf_emit_backpressure.{key} must carry a timezone")


def _wait_descriptor(metadata: object) -> _WaitDescriptor:
    """Validate every live field before wake interprets its admission facts."""
    document = _metadata_document(metadata)
    if (
        document.get("blocking_reason") != "backpressure"
        or not isinstance(document.get("wf_deferral"), dict)
        or cast("dict[str, object]", document["wf_deferral"]).get("signal") != "EmitBackpressure"
    ):
        raise ValueError("the selected row has no coherent EmitBackpressure marker")
    raw_record = document.get("wf_emit_backpressure")
    if not isinstance(raw_record, dict):
        raise ValueError("metadata.wf_emit_backpressure must be an object")
    record = cast("dict[str, object]", raw_record)
    if _counter(record, "version", minimum=1) != 1:
        raise ValueError("unsupported wf_emit_backpressure version")
    page_width = _counter(record, "page_width")
    bound = _counter(record, "bound", minimum=1)
    _counter(record, "deferrals", minimum=1)
    _counter(record, "wakes")
    _counter(record, "outstanding")
    _timestamp(record, "deferred_at")
    _timestamp(record, "last_wake_at", optional=True)
    return _WaitDescriptor(page_width=page_width, bound=bound)


async def wake_emit_backpressure(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId | None = None,
    batch_size: int = 100,
    after: tuple[JobId, JobId] | None = None,
    chunk_size: int = 200,
    exhaust: bool = False,
) -> EmitWakeResult:
    """Promote fitted, exactly parked sources; the estate pass pages the
    matching set in CHUNKED TRANSACTIONS under a hard per-transaction lock
    budget.

    ``flow_id`` is the post-commit accelerator: one flow's own parked
    waiters (bounded by that run's own graph) in one transaction, at most
    ``batch_size`` of them. With no flow this is the estate recovery pass,
    and the caller's cursor must be FAIR — :class:`EstateWakeCursor`
    alternates a forward keyset stride with a from-start WRAP
    (``exhaust=True``). The ACTUAL guarantees, stated for ANY parked
    population:

    * COVERAGE: every parked row is examined at least once per two-pass
      cycle — the wrap pass pages the WHOLE matching set (keyset pages of
      ``chunk_size`` until the set is exhausted), so no blocked prefix,
      however long, and no population size can starve a fitted waiter
      behind it.
    * LOCK BUDGET: each transaction processes at most ``chunk_size``
      candidates — one advisory lock per distinct flow plus the chunk's
      own row locks — a hard per-transaction ceiling (``chunk_size`` is
      validated to at most 1000, far below
      ``max_locks_per_transaction * max_connections`` at any deployment
      shape). The wrap can never exhaust the shared lock table; its own
      cost is ``ceil(matching/chunk_size)`` committed transactions, the
      recovery arm's honest work for the population it recovers.
    * ISOLATION: one malformed parked record skips its own row with a
      bounded, named diagnostic; the valid waiters still wake. An
      infrastructure fault aborts THAT chunk (its committed predecessors
      stand) and propagates — the sweep's failure arm owns the retry, and
      the next cycle's wrap re-examines the aborted chunk's rows.
    """
    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("emit-backpressure wake batch_size must be positive")
    if type(chunk_size) is not int or not 1 <= chunk_size <= _WAKE_MAX_CHUNK:
        raise ValueError(
            f"emit-backpressure wake chunk_size must be an integer in "
            f"[1, {_WAKE_MAX_CHUNK}] — the per-transaction lock budget (an "
            "advisory lock per distinct flow plus the chunk's own row locks "
            "must stay far below max_locks_per_transaction * connections)"
        )
    if after is not None and flow_id is not None:
        raise ValueError("an estate cursor cannot be combined with one flow target")

    from taskq.workflows.api._sql_runner import render_sql

    root_lock_sql = render_sql(EMIT_BACKPRESSURE_ROOT_LOCK_SQL, wsql.schema)
    candidate_lock_sql = render_sql(EMIT_BACKPRESSURE_CANDIDATE_LOCK_SQL, wsql.schema)
    wake_sql = render_sql(EMIT_BACKPRESSURE_WAKE_SQL, wsql.schema)
    first_page_sql: str
    next_page_sql: str | None
    base_args: tuple[object, ...]
    if flow_id is not None:
        first_page_sql = render_sql(EMIT_BACKPRESSURE_FLOW_CANDIDATES_SQL, wsql.schema)
        next_page_sql = None  # the flow's own parked set is its graph's bound
        base_args = (flow_id,)
    else:
        first_page_sql = render_sql(EMIT_BACKPRESSURE_CANDIDATES_SQL, wsql.schema)
        next_page_sql = render_sql(EMIT_BACKPRESSURE_CURSOR_CANDIDATES_SQL, wsql.schema)
        base_args = ()

    woken = 0
    processed = 0
    last: tuple[JobId, JobId] | None = None
    page_after: tuple[JobId, JobId] | None = after
    while True:
        # THE PAGE'S OWN LIMIT: the wrap pages at the chunk budget; a
        # non-exhaust pass pages at min(chunk, its remaining coverage
        # target) — the stride's batch bound is never swallowed by a
        # larger chunk.
        if flow_id is not None:
            page_limit = batch_size
        elif exhaust:
            page_limit = chunk_size
        else:
            page_limit = max(1, min(chunk_size, batch_size - processed))
        # ONE TRANSACTION PER CHUNK: the locks (the roots' FOR UPDATE NOWAIT,
        # the candidates' row locks, one advisory lock per distinct flow)
        # release at this chunk's commit — the pass's lock footprint is
        # chunk_size, never the parked population's size.
        async with pool.acquire() as conn, conn.transaction():
            if page_after is None:
                candidates = await conn.fetch(first_page_sql, *base_args, page_limit)
            else:
                assert next_page_sql is not None
                candidates = await conn.fetch(next_page_sql, *page_after, page_limit)
            count = len(candidates)
            processed += count
            locked_roots: dict[JobId, bool] = {}
            for candidate in candidates:
                source_id = JobId(candidate["id"])
                candidate_flow = JobId(candidate["flow_id"])
                last = (candidate_flow, source_id)
                live_root = locked_roots.get(candidate_flow)
                if live_root is None:
                    root = await conn.fetchrow(root_lock_sql, candidate_flow)
                    live_root = root is not None and root["status"] not in {
                        "succeeded",
                        "failed",
                        "cancelled",
                        "crashed",
                        "abandoned",
                    }
                    locked_roots[candidate_flow] = live_root
                if not live_root:
                    continue

                # Canonical root -> waiter -> admission order. This fresh locked
                # row read refutes the initial index probe before descriptor use.
                source = await conn.fetchrow(candidate_lock_sql, source_id)
                if source is None:
                    continue
                try:
                    descriptor = _wait_descriptor(source["metadata"])
                except ValueError as exc:
                    # PER-ROW ISOLATION: one malformed/corrupt parked record is
                    # that row's own fault, never the estate's — the pass skips
                    # it (the paging cursor already advanced past it) and the
                    # valid waiters still wake. The durable marker stays: the
                    # row is visible, named, and bounded — the operator's
                    # alert, never a poisoned recovery loop. An infrastructure
                    # fault is NOT isolated: it aborts the chunk (its
                    # committed predecessors stand) and the sweep's own
                    # failure arm owns the retry.
                    logger.warning(
                        "wf.emit_backpressure_waiter_invalid",
                        flow_id=str(candidate_flow),
                        job_id=str(source_id),
                        error_class=type(exc).__name__,
                        error=str(exc)[:500],
                    )
                    continue
                await conn.execute(wsql.emit_admission_lock, str(candidate_flow))
                outstanding = await _outstanding(conn, wsql, candidate_flow, source_id)
                if outstanding + descriptor.page_width > descriptor.bound:
                    continue
                promoted = await conn.fetchrow(wake_sql, source_id)
                if promoted is None:
                    continue
                woken += 1
                # NOTIFY accelerates an already durable pending transition. The
                # effective actor assignment, not a stale original label, is named.
                await conn.execute(
                    "SELECT pg_notify($1, $2)",
                    wake_channel(wsql.schema),
                    str(promoted["effective_queue"]),
                )
        # THE CHUNK COMMITTED (its locks are released). The paging decision:
        if count < page_limit:
            # The set's end: the pass is done; a short pass resets the
            # caller's cursor by itself (the next_cursor is None).
            return EmitWakeResult(woken=woken, next_cursor=None)
        if flow_id is not None:
            # The flow accelerator's own shape: one transaction, at most
            # batch_size of the flow's own waiters — the next post-commit
            # seat (or the estate wrap) covers the rest.
            return EmitWakeResult(woken=woken, next_cursor=None)
        if not exhaust and processed >= batch_size:
            # The pass's own coverage target: the caller's cursor advances
            # to the last processed row.
            return EmitWakeResult(woken=woken, next_cursor=last)
        page_after = last


async def wake_emit_backpressure_safely(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    batch_size: int = 100,
) -> int:
    """Fail-soft inline wake seat; committed rows and bounded sweep remain truth."""
    try:
        result = await wake_emit_backpressure(pool, wsql, flow_id=flow_id, batch_size=batch_size)
    except Exception as exc:  # Why: an accelerator must not fail the successful child.
        logger.warning(
            "wf.emit_backpressure_wake_failed",
            flow_id=str(flow_id),
            error_class=type(exc).__name__,
            error=str(exc)[:500],
        )
        return 0
    return result.woken
