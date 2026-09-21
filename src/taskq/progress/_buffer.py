"""_ProgressBuffer: per-job in-memory accumulator for coalesced progress flushes."""

from dataclasses import dataclass, field
from uuid import UUID

__all__ = [
    "_EncodedProgressData",
    "_PendingPublish",
    "_ProgressBuffer",
    "_consume_state_change_seq",
    "_progress_after_flush",
    "_seq_and_state_after_flush_attempt",
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

    The seq it accumulates (``base_seq + pending_seq_delta``) is the job's
    total event order: progress calls consume one value each, state-change
    events consume one via :func:`_consume_state_change_seq`, and the
    terminal/requeue helpers (:func:`_terminal_seq_and_state`,
    :func:`_seq_and_state_after_flush_attempt`) consume one past the head
    for the write they serve. No event on the wire can repeat another's
    seq, and every consumed value reaches the durable row riding a flush
    delta or a ``mark_*`` absolute SET.

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


def _progress_after_flush(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) from the flushed buffer WITHOUT consuming a seq.

    After ``_flush_buffer_immediate`` succeeds, ``buffer.base_seq`` holds the
    authoritative flushed seq and ``pending_seq_delta == 0``.  This helper reads
    those values directly.  If the buffer is None, returns (0, {}).
    """
    if buffer is None:
        return 0, {}
    return buffer.base_seq, dict(buffer.pending_state)


def _consume_state_change_seq(buffer: _ProgressBuffer | None) -> int:
    """Consume the next seq for a state-change event; return the consumed seq.

    The seq is a strict total order over the job's whole event stream,
    progress and state-change events alike, so a state-change event
    carries ``head + 1``, never the head itself: a consumer deduping or
    resuming by seq alone (EventSource ``Last-Event-ID`` discipline)
    must be able to tell the state-change event apart from the progress
    event before it, and one carrying the head is indistinguishable
    from a duplicate of that event.

    The consumption is recorded on ``pending_seq_delta``, so every later
    reader of the head (the next ``ctx.progress`` call, the flush
    delta, the terminal helpers) stacks on it, and it reaches the
    durable row riding the next flush delta or the next ``mark_*``
    absolute SET. Deliberately not ``dirty``: a consumption alone is
    not unflushed progress work, and one extra flush UPDATE per
    zero-progress dispatch buys nothing the terminal write's absolute
    SET does not already carry. A ``None`` buffer (no progress surfaces
    wired) consumes nothing and returns 0, the caller's default.
    """
    if buffer is None:
        return 0
    buffer.pending_seq_delta += 1
    return buffer.base_seq + buffer.pending_seq_delta


def _terminal_seq_and_state(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object]]:
    """Return (seq, state) for a state-change write that directly SETs progress_seq.

    The seq is CONSUMED: the returned value is ``base_seq +
    pending_seq_delta + 1``, one past the buffer's head, so the
    state-change event the write publishes is strictly greater than
    every event before it on the stream, and the write's absolute
    ``SET progress_seq = $N`` lands that consumed value durably, so the
    next attempt's buffer seeds from it and the total order survives
    redispatch. The helper always computes one past the head regardless
    of flush state.  All ``mark_*`` SQL uses
    direct assignment (``SET progress_seq = $N``), so returning 0 for a
    clean buffer with ``base_seq > 0`` would clobber the
    previously-flushed value.
    """
    if buffer is None:
        return 0, {}
    return buffer.base_seq + buffer.pending_seq_delta + 1, dict(buffer.pending_state)


def _seq_and_state_after_flush_attempt(
    buffer: _ProgressBuffer | None,
) -> tuple[int, dict[str, object] | None]:
    """Return (seq, state) after a pre-terminal flush attempt.

    The projection is the same whatever the flush attempt did: a
    succeeded flush retired the delta into ``base_seq`` (clean,
    ``pending_seq_delta == 0``), a failed one left it standing (dirty,
    connection error, pool timeout, etc.), and both shapes put the
    authoritative head at ``base_seq + pending_seq_delta``. The
    terminal write CONSUMES one past that head
    (:func:`_terminal_seq_and_state`), so the terminal event's seq is
    exactly one greater than the last event the buffer carried,
    progress or state-change alike, and the absolute SET carries the
    consumed value so neither an unflushed delta nor the consumption
    is lost in the write.

    Returns ``(int, dict | None)`` where ``None`` means no progress to write.
    """
    seq, state = _terminal_seq_and_state(buffer)
    if not state:
        return seq, None
    return seq, state
