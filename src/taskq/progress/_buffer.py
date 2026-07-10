"""_ProgressBuffer: per-job in-memory accumulator for coalesced progress flushes."""

from dataclasses import dataclass, field
from uuid import UUID

__all__ = [
    "_ProgressBuffer",
    "_progress_after_flush",
    "_seq_and_state_after_flush_attempt",
    "_snapshot_progress",
    "_terminal_seq_and_state",
]


@dataclass
class _ProgressBuffer:
    """Mutable per-job accumulator; not part of the public API.

    Intentionally not frozen — ``pending_seq_delta``, ``dirty``, and
    ``last_flush_at`` are mutated on every progress call and flush.
    """

    job_id: UUID
    base_seq: int
    pending_seq_delta: int = 0
    pending_state: dict[str, object] = field(default_factory=lambda: {})
    dirty: bool = False
    last_flush_at: float = 0.0


def _snapshot_progress(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) from a progress buffer for a terminal write.

    If the buffer is None or clean, returns (0, {}) — the caller's default.
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
    is clean, this helper always computes ``base_seq + pending_seq_delta`` —
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
    flush failed silently (buffer still dirty — connection error, pool
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
