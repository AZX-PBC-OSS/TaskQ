"""THE EMIT TX (T20) — the streaming source's per-page statement group,
the design's ONE new primitive (PROOF.md §6a).

The shipped fork inserts children ONLY inside the parent's terminal-mark
tx1 (fork-at-finalize): a child's existence is INDEXED to its parent's
death. A STREAMING source must do the opposite — emit children while
ITSELF staying ``running`` — so its pages stream (children claim +
terminal while the source is mid-fetch; the spike measured 236 chain
claims while the source was still streaming).

THE EMIT TX, ONE transaction:
  1. the children INSERT   (the certified FORK_CHILDREN shape re-bound)
  2. the edge rows         (the certified FORK_EDGES shape re-bound —
                           child→source, fail_closed: a mid-stream source
                           death is the honest fail-closed parent)
  3. the CURSOR CHECKPOINT (the source row's own metadata under the FULL
                           claim fence — no new table, no new column)

Atomicity is the resume invariant: a kill at ANY statement window —
including after the cursor write but BEFORE the commit — rolls the whole
tx back; the reclaim re-pends the source (it never finalized), the
re-claim re-emits exactly the lost page, never a partial one, never a
duplicate one. THE FENCE IS THE CLAIM: a zombie source (its attempt
superseded) updates nothing in step 3 → :class:`EmitFencedError` → the
children and edges roll back with the tx — a zombie cannot emit.

THE STATEMENT-WINDOW KILL PINS: ``emit_batch`` accepts ``_window_hook``
(the attack suite's seam — the spike's storm drove the same hook), fired
after EACH statement with ``emit:{1|2|3}``; the pins terminate the
backend there (a real server-side kill) and prove zero re-emitted, zero
lost children.

THE EMIT BACKPRESSURE (DH9 — the ticket's decision 7): the
MAX-IN-FLIGHT bound is DECLARED at the workflow level (the runner
supplies the workflow's declaration; ``None`` = the explicit
unbounded). The emit admits its WHOLE page: the run's outstanding
non-terminal rows (the ledger's truth — pending/scheduled/running,
excluding the source's own claim) + the page's width must fit under the
bound, or the pager BLOCKS — the generator's laziness IS the pause (a
bounded poll OUTSIDE any tx: no connection, no transaction held while
waiting). The admission's second gate sits INSIDE the tx under the
flow's ADMISSION LOCK (``pg_advisory_xact_lock`` — DH4's pattern: the
count and the insert serialize on the same lock, so two concurrent
emitters of one run cannot both admit past the bound; a lost admission
rolls the tx back WHOLE and the wait retries — a guard, never a timing
argument). A stall past the wait's deadline is the LOUD refusal
(:class:`EmitBackpressureTimeout`) — and the certified ladder owns it:
the source re-pends with backoff, the re-claim resumes FROM THE CURSOR,
the blocked page emits fresh (it never ran — nothing was written). The
degradation is the dispatch band's latency, never unbounded
materialization.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Final

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._fork import FORK_CHUNK
from taskq.workflows._types import EmitChild, _jsonb, _metadata

if TYPE_CHECKING:
    import asyncpg

    from taskq.backend._protocol import ConnLike
    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "EMIT_CURSOR_KEY",
    "EMIT_MAX_IN_FLIGHT_DEFAULT",
    "EmitBackpressureTimeout",
    "EmitFencedError",
    "emit_batch",
]

#: The cursor checkpoint's metadata key on the SOURCE row itself (the
#: checkpoint rides the row's jsonb — no new table, no new column, no
#: migration). The value is the caller's cursor document, merged at the
#: top level of the row's metadata under this one key.
EMIT_CURSOR_KEY: Final[str] = "emit_cursor"

#: THE MAX-IN-FLIGHT DEFAULT (DH9's fence is ON for the new surface): the
#: bound a workflow gets when it does not declare one. The author opts
#: OUT loudly (``max_in_flight=None`` on the workflow declaration — the
#: explicit unbounded, never a silent default).
EMIT_MAX_IN_FLIGHT_DEFAULT: Final[int] = 1000

#: The backpressure wait's default term: the pager blocks at most this
#: long before the LOUD refusal. The body may tighten it (the pager
#: knows its own pace); nothing waits forever.
_EMIT_BACKPRESSURE_DEFAULT_S: Final[float] = 30.0

#: The backpressure poll's tick.
_EMIT_BACKPRESSURE_POLL_S: Final[float] = 0.05

#: The window-hook type: fired after each in-tx statement with the
#: window's name (``emit:1`` children, ``emit:2`` edges, ``emit:3``
#: cursor) and the connection — the statement-window kill pins' seam.
#: (``ConnLike`` spelled as ``Any`` at runtime — the alias is a
#: TYPE_CHECKING-only import; the hook's contract is the statement
#: boundary it fires at, not the connection's type.)
WindowHook = Callable[[str, Any], Awaitable[None]]


class EmitFencedError(RuntimeError):
    """The emit's cursor checkpoint updated nothing — the claim was
    superseded (the source row was reclaimed and re-claimed under us).
    The tx aborts: the children and edges roll back with it. THE LOUD
    REFUSAL — a zombie source cannot emit, silently or otherwise."""


class EmitBackpressureTimeout(RuntimeError):
    """THE MAX-IN-FLIGHT BOUND STALLED (DH9): the run's outstanding
    non-terminal rows plus the page's width did not fit under the
    declared bound for the whole wait's term — the LOUD refusal, never a
    silent unbounded emit, never an infinite block. THE LADDER OWNS IT:
    the source re-pends with backoff, the re-claim resumes FROM THE
    CURSOR, and the blocked page emits fresh (it never ran — the block
    is BEFORE the tx, nothing was written)."""


class _AdmissionLost(Exception):
    """The admission re-check INSIDE the tx (under the flow's admission
    lock) found the bound taken by a concurrent emitter — the tx aborts
    (nothing partial) and the wait loop retries. Never escapes."""


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
) -> None:
    """The emit tx's body (the caller owns the transaction): the children
    INSERT, the edge rows, the cursor checkpoint — the certified fork
    shapes re-bound, chunked parallel arrays, ids minted app-side."""
    for start in range(0, len(children), FORK_CHUNK):
        chunk = children[start : start + FORK_CHUNK]
        ids = child_ids[start : start + FORK_CHUNK]
        await conn.execute(
            wsql.emit_children,
            ids,
            [c.actor for c in chunk],
            [c.queue for c in chunk],
            [_jsonb(c.payload) for c in chunk],
            [max_attempts] * len(chunk),
            [retry_kind] * len(chunk),
            [source_id] * len(chunk),
            [c.map_index for c in chunk],
            [c.step_key for c in chunk],
            [c.trace_id for c in chunk],
            [_jsonb(_metadata(flow_id, blocking_reason=None))] * len(chunk),
            # The scope is the fork's own (one scope per flow) — the
            # uniqueness pair is (scope, key), the key carries the
            # emit scope + the per-record index.
            [f"workflow:{flow_id}"] * len(chunk),
            [f"wf:{flow_id}:emit:{c.map_index}:{c.step_key}" for c in chunk],
        )
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

    # THE CURSOR CHECKPOINT — under the FULL claim fence. The metadata
    # merge is a top-level concat under ONE key: the caller's cursor
    # document is never diffed field-by-field, and the row's other
    # metadata (flow_id, the hold stamps) survives.
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
        raise EmitFencedError(
            f"the emit's cursor checkpoint updated nothing — the source "
            f"{source_id}'s claim was superseded (reclaimed and "
            "re-claimed under this emit); the tx rolled back, nothing "
            "was written"
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
    backpressure_timeout_s: float = _EMIT_BACKPRESSURE_DEFAULT_S,
    _window_hook: WindowHook | None = None,
) -> tuple[JobId, ...]:
    """Emit ONE page of chain starts: the children + the edges + the
    cursor checkpoint, ONE transaction, while the source stays
    ``running``.

    The REFUTED-CLAIM DISCIPLINE is enforced at the door: every child
    carries its ``map_index`` (the discriminator the fork's idempotency
    key and the ledger's arbiter both ride — a ``None`` is refused BEFORE
    any row exists) and its ``trace_id`` (the per-record lineage).

    The idempotency keys are PARENT-SCOPED at the emit scope —
    ``wf:{flow}:emit:{map_index}:{step_key}`` — distinct from the
    fork-at-finalize keys (``wf:{flow}:{parent_step}:{step}:{idx}``): a
    source that ALSO forks at its finalize can never key-collide with its
    own emitted chains.

    THE MAX-IN-FLIGHT BOUND (DH9): *max_in_flight* is the DECLARED bound
    (the workflow-level declare — the runner supplies the workflow's;
    ``None`` = the explicit unbounded). The emit admits its WHOLE page:
    the run's outstanding non-terminal rows + the page's width must fit
    under the bound, or the pager BLOCKS (the generator's laziness is
    the pause — a bounded poll outside any tx, no connection held while
    waiting). The admission's second gate sits INSIDE the tx under the
    flow's admission lock — the concurrent emitters' count+insert
    serialize (a lost admission aborts the tx and retries the wait; a
    guard, never a timing argument). A stall past the wait's term is the
    LOUD refusal — the certified ladder's subject.

    Returns the minted child ids (uuid7 via the seam). Raises
    :class:`EmitFencedError` when the claim was superseded (nothing
    written). Raises :class:`EmitBackpressureTimeout` when the declared
    bound stalled the whole wait's term (nothing written)."""
    for child in children:
        if child.map_index is None:  # pyright: ignore[reportUnnecessaryComparison]  # Why: the runtime guard behind the typed door — EmitChild.map_index's int type is the declaration; a hand-built child carrying None must be REFUSED here (the refuted-claim discipline), never trusted to the checker.
            raise ValueError(
                f"emit child {child.step_key!r} has map_index=None — the "
                "emit stamps map_index per record (the discriminator the "
                "fork's idempotency key and the ledger's arbiter both "
                "ride); without it every record's children collide onto "
                "one row (the spike's 198 UniqueViolations)"
            )
        if not child.trace_id:
            raise ValueError(
                f"emit child {child.step_key!r} (map_index="
                f"{child.map_index}) has no trace_id — the emit stamps "
                "the per-record trace (the one-query lineage)"
            )

    child_ids = [JobId(new_uuid()) for _ in children]
    loop = asyncio.get_running_loop()
    deadline = loop.time() + backpressure_timeout_s

    if max_in_flight is not None:
        # AN ADMISSION THAT CAN NEVER SUCCEED is refused at the door.
        if len(children) > max_in_flight:
            raise ValueError(
                f"the emit's page ({len(children)} children) can never fit "
                f"under the declared max_in_flight={max_in_flight} — the "
                "admission is impossible (shrink the page or raise the "
                "bound; the loud config refusal, never a silent starve)"
            )

        while True:
            # THE WAIT (outside any tx — the pager blocks here; no
            # connection held while waiting).
            async with pool.acquire() as conn:
                count = await _outstanding(conn, wsql, flow_id, source_id)
            if count + len(children) > max_in_flight:
                if loop.time() > deadline:
                    raise EmitBackpressureTimeout(
                        f"the emit backpressure bound (max_in_flight="
                        f"{max_in_flight}) stalled the run for "
                        f"{backpressure_timeout_s}s: {count} non-terminal "
                        f"rows + a page of {len(children)} never fit — the "
                        "workers are not draining (the loud refusal; the "
                        "ladder re-pends the source and the resume "
                        "continues from the cursor)"
                    )
                await asyncio.sleep(_EMIT_BACKPRESSURE_POLL_S)
                continue
            # THE TX — the admission's second gate inside (the flow's
            # admission lock serializes the concurrent emitters'
            # count+insert; a lost admission rolls back WHOLE and
            # retries the wait).
            try:
                async with pool.acquire() as conn, conn.transaction():
                    await conn.execute(wsql.emit_admission_lock, str(flow_id))
                    inside = await _outstanding(conn, wsql, flow_id, source_id)
                    if inside + len(children) > max_in_flight:
                        raise _AdmissionLost(
                            f"the admission was taken between the wait and "
                            f"the tx (outstanding {inside} + "
                            f"{len(children)} > {max_in_flight})"
                        )
                    await _emit_tx(
                        conn,
                        wsql,
                        flow_id=flow_id,
                        source_id=source_id,
                        worker_id=worker_id,
                        attempt=attempt,
                        claim_epoch=claim_epoch,
                        children=children,
                        cursor=cursor,
                        child_ids=child_ids,
                        max_attempts=max_attempts,
                        retry_kind=retry_kind,
                        _window_hook=_window_hook,
                    )
                return tuple(child_ids)
            except _AdmissionLost:
                if loop.time() > deadline:
                    raise EmitBackpressureTimeout(
                        f"the emit backpressure bound (max_in_flight="
                        f"{max_in_flight}) stalled the run for "
                        f"{backpressure_timeout_s}s (the admission kept "
                        "being taken by a concurrent emitter) — the loud "
                        "refusal; the ladder re-pends the source and the "
                        "resume continues from the cursor"
                    ) from None
                await asyncio.sleep(_EMIT_BACKPRESSURE_POLL_S)

    async with pool.acquire() as conn, conn.transaction():
        await _emit_tx(
            conn,
            wsql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            children=children,
            cursor=cursor,
            child_ids=child_ids,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            _window_hook=_window_hook,
        )
    return tuple(child_ids)
