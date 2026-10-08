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
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, Final

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._fork import FORK_CHUNK
from taskq.workflows._types import EmitChild, _jsonb, _metadata

if TYPE_CHECKING:
    import asyncpg

    from taskq.workflows._sql import WorkflowSql

__all__ = ["EMIT_CURSOR_KEY", "EmitFencedError", "emit_batch"]

#: The cursor checkpoint's metadata key on the SOURCE row itself (the
#: checkpoint rides the row's jsonb — no new table, no new column, no
#: migration). The value is the caller's cursor document, merged at the
#: top level of the row's metadata under this one key.
EMIT_CURSOR_KEY: Final[str] = "emit_cursor"

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

    Returns the minted child ids (uuid7 via the seam). Raises
    :class:`EmitFencedError` when the claim was superseded (nothing
    written)."""
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
    async with pool.acquire() as conn, conn.transaction():
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

        # THE CURSOR CHECKPOINT — under the FULL claim fence. The
        # metadata merge is a top-level concat under ONE key: the
        # caller's cursor document is never diffed field-by-field, and
        # the row's other metadata (flow_id, the hold stamps) survives.
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
                f"the emit's cursor checkpoint updated nothing — the "
                f"source {source_id}'s claim was superseded (reclaimed "
                "and re-claimed under this emit); the tx rolled back, "
                "nothing was written"
            )
    return tuple(child_ids)
