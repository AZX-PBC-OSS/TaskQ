"""_ProgressBuffer: per-job in-memory accumulator for coalesced progress flushes."""

from dataclasses import dataclass, field
from uuid import UUID

__all__ = [
    "_EncodedProgressData",
    "_PendingPublish",
    "_ProgressBuffer",
    "_progress_after_flush",
    "_seq_and_state_after_flush_attempt",
    "_snapshot_progress",
    "_terminal_seq_and_state",
]


@dataclass(frozen=True, slots=True)
class _EncodedProgressData:
    """A progress ``data`` dict paired with its :func:`taskq._json.dumps` bytes.

    ``ctx.progress`` encodes ``data`` once to enforce the size cap and
    keeps the bytes with the very dict that entered ``pending_state``, so
    the flush binds them instead of encoding the dict again. The pairing
    is by identity: the bytes stand in for ``pending_state["data"]`` only
    while that entry is ``source`` itself, so a dict that reached the
    buffer by any other route is encoded from the dict as before.
    """

    source: dict[str, object]
    json: bytes


@dataclass(frozen=True, slots=True)
class _PendingPublish:
    """A progress event latched while another publish is in flight.

    The fields are the publish's own arguments, captured at the
    superseding ``ctx.progress`` call: publishing the latch reproduces
    exactly the event that call would have published uncoalesced, so the
    last event a subscriber sees is byte-for-byte the one the final
    progress call produced.
    """

    step: int | None
    percent: float | None
    detail: str | None
    data: dict[str, object] | None
    seq: int


@dataclass
class _ProgressBuffer:
    """Mutable per-job accumulator; not part of the public API.

    Intentionally not frozen, ``pending_seq_delta``, ``dirty``, and
    ``last_flush_at`` are mutated on every progress call and flush.
    """

    job_id: UUID
    base_seq: int
    # The attempt epoch the accumulated deltas belong to. The flush gate
    # carries it as a per-row conjunct (``jobs.attempt = <epoch>``): a
    # stale flush landing after a same-worker redispatch to a later
    # attempt must no-op instead of clobbering the new epoch's progress.
    # Fail-closed by construction: dispatch stamps ``attempt + 1`` on
    # claiming, so a running row always carries attempt >= 1 and an
    # unseeded epoch of 0 matches no running row, the flush no-ops
    # rather than writing. Production buffers are seeded from the
    # dispatched ``JobRow.attempt``; only direct test construction omits
    # it (against statement doubles that ignore the bound values).
    attempt: int = 0
    pending_seq_delta: int = 0
    pending_state: dict[str, object] = field(default_factory=lambda: {})
    encoded_data: _EncodedProgressData | None = None
    dirty: bool = False
    last_flush_at: float = 0.0
    # The flush gate (``_flush_buffer_immediate`` vs the tick's
    # ``_flush_dirty_set``): at most one flush of this buffer's unretired
    # delta may be in flight at a time. Both surfaces apply the same
    # ``progress_seq = row + delta`` merge, so a second statement issued
    # while one holds an unretired snapshot double-applies the delta (and
    # the second retire then drives ``pending_seq_delta`` negative, so the
    # terminal write's absolute SET regresses the row). The immediate
    # path skips instead of waiting: the buffer keeps its unflushed delta
    # and the terminal write's absolute SET carries it. Mutable like the
    # rest of the accumulator; both surfaces run on the worker's event
    # loop, so the flag never needs a lock.
    flush_in_flight: bool = False
    # The Redis publish gate (``JobContext.progress``): at most one publish
    # task per job is in flight at a time; a progress call that lands while
    # one is running only latches its event on ``pending_publish`` for the
    # in-flight task to re-publish when its round trip lands. The latch is
    # the no-lost-final guarantee: a trailing progress call always reaches
    # the channel either directly or through it. Mutable like the rest of
    # the accumulator; the publish task runs on the same event loop as the
    # progress calls, so the flag never needs a lock.
    publish_in_flight: bool = False
    pending_publish: _PendingPublish | None = None


def _snapshot_progress(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) from a progress buffer for a terminal write.

    If the buffer is None or clean, returns (0, {}), the caller's default.
    If dirty, returns the full accumulated seq (base_seq + pending_seq_delta)
    and a copy of pending_state so the terminal write carries all progress.
    """
    if buffer is None or not buffer.dirty:
        return 0, {}
    return buffer.base_seq + buffer.pending_seq_delta, dict(buffer.pending_state)


def _progress_after_flush(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) after a pre-terminal flush has completed.

    After ``_flush_buffer_immediate`` succeeds, ``buffer.base_seq`` holds the
    authoritative sequence and ``pending_seq_delta == 0``.  This helper reads
    those values directly.  If the buffer is None, returns (0, {}).
    """
    if buffer is None:
        return 0, {}
    return buffer.base_seq, dict(buffer.pending_state)


def _terminal_seq_and_state(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) for a terminal write that directly SETs progress_seq.

    Unlike :func:`_snapshot_progress`, which returns ``(0, {})`` when the buffer
    is clean, this helper always computes ``base_seq + pending_seq_delta`` ,
    the authoritative current sequence regardless of flush state.  All
    ``mark_*`` SQL uses direct assignment (``SET progress_seq = $N``), so
    returning 0 for a clean buffer with ``base_seq > 0`` would clobber the
    previously-flushed value.
    """
    if buffer is None:
        return 0, {}
    return buffer.base_seq + buffer.pending_seq_delta, dict(buffer.pending_state)


def _seq_and_state_after_flush_attempt(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object] | None]:
    """Return (seq, state) after a pre-terminal flush attempt.

    If the flush succeeded (buffer is clean), reads ``base_seq`` and
    ``pending_state`` directly via :func:`_progress_after_flush`.  If the
    flush failed silently (buffer still dirty, connection error, pool
    timeout, etc.), falls back to :func:`_snapshot_progress` which returns
    ``base_seq + pending_seq_delta`` and a copy of ``pending_state`` so
    the pending delta is not lost in the terminal write.

    Returns ``(int, dict | None)`` where ``None`` means no progress to write.
    """
    if buffer is not None and buffer.dirty:
        seq, state = _snapshot_progress(buffer)
    else:
        seq, state = _progress_after_flush(buffer)
    if not state:
        return seq, None
    return seq, state
