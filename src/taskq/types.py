"""Client-facing result and event-detail types.

``CancelResult`` is the structured return value of ``JobsClient.cancel()``.
``BulkCancelResult`` is the structured return value of
``JobsClient.cancel_where()`` (defined in ``backend._protocol`` to avoid
a circular import, see its docstring).
``StateChangeEvent`` is the JSON payload stored in
``job_events.detail`` for rows with ``kind='state_change'``.

``BulkCancelResult`` is re-exported here (not defined) because
``types.py`` imports from ``backend._protocol``, defining it here would
create a circular import (``_protocol → types → _protocol``).
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from taskq.backend._protocol import BulkCancelResult, JobId, JobStatus

__all__ = [
    "BackpressureSnapshot",
    "BackpressureState",
    "BulkCancelResult",
    "CancelResult",
    "QueueBackpressure",
    "StateChangeEvent",
]

BackpressureState = Literal["ok", "over", "unknown"]
"""The backpressure verdict's domain.

``ok``: every routing actor whose stored cap resolves is BELOW its
admission boundary. ``over``: at least one routing actor's own
pending+scheduled count (ALL queues — that is the count
``enqueue_max_pending_count`` governs) is at or above its cap, so the
next enqueue for this queue's actor is likely refused with
:class:`~taskq.exceptions.MaxPendingExceededError`. ``unknown``: the
verdict could not be computed (sick database, a backend without the
staged reads, no capacity snapshot, a routing actor cap that lives only
in code) — ``reason`` names what could not be seen. NEVER a fabricated
verdict: the read fails open to ``unknown``, the
``maybe_warn_unserved_queue`` posture.
"""


class QueueBackpressure(BaseModel):
    """One queue's backpressure verdict, the entry type of
    :class:`BackpressureSnapshot`.

    All depth/cap halves are ``None``-able on purpose: ``None`` always
    means "this half could not be seen", and any ``None`` half forces
    the ``unknown`` state. A verdict is only ``ok``/``over`` when every
    half it compares was actually read.
    """

    model_config = ConfigDict(frozen=True)

    queue: str
    depth: int | None = None
    """Jobs holding a pending slot in THIS queue (pending + scheduled) —
    the queue-local, ops view. NOT the verdict's basis: the admission
    cap governs the ACTOR's count across ALL queues (see
    :attr:`admission_load`); a queue-local count against an actor cap
    would false-OK exactly when an actor's traffic splits across queues
    (the F1 review finding). ``None``: the depth read was unavailable."""
    children_depth: int | None = None
    """The fan-out parent's pending children OUTSIDE this queue — the
    exact ``parent_id`` ledger, REPORTED not verdict-bearing: every
    pending child already counts toward its OWN actor's cap, which is
    the number the admission check enforces, so the fan-out pressure
    reaches the verdict through the routing actors' admission loads
    without a second copy of it here. ``None``: no fan-out parent in
    play, or the children read was unavailable."""
    binding_actor: str | None = None
    """The routing actor whose admission state binds the verdict: the
    smallest headroom (stored cap minus its own pending+scheduled count)
    among the actors whose stored assignment routes this queue.
    ``None``: no cap resolved (then ``state`` is ``unknown``)."""
    effective_max_pending: int | None = None
    """:attr:`binding_actor`'s stored cap — the tightest effective
    admission boundary for this queue. ``None``: no cap resolvable from
    this process (no snapshot, no routing row, or the routing actor
    enforces only the ``@actor`` literal — code-side, invisible here)."""
    admission_load: int | None = None
    """:attr:`binding_actor`'s pending+scheduled count across ALL queues
    — the number ``enqueue_max_pending_count`` actually compares against
    its cap. The verdict is ``over`` exactly when this is at or above
    :attr:`effective_max_pending`. ``None``: the count read was
    unavailable."""
    state: BackpressureState
    reason: str | None = None
    """Set exactly when ``state == 'unknown'``: what could not be seen."""


class BackpressureSnapshot(BaseModel):
    """The typed read of the submit path's backpressure state.

    Returned by ``JobsClient.backpressure()`` / ``TaskQ.backpressure()``.
    One :class:`QueueBackpressure` entry per requested queue;
    ``parent_id`` is the fan-out parent whose pending children the
    verdicts include (explicit or ambient — the worker's parent
    context), ``None`` when no parent is in play.

    AS-OF semantics, carried explicitly (a snapshot that does not say
    how old it is would be a lie by omission): :attr:`as_of` is the
    wall-clock instant the read was taken; the depth half was EXACT at
    that instant but is advisory from it on (the race between check and
    later enqueue is acceptable, documented); the cap half is the
    capacity snapshot named by :attr:`cap_age_seconds`, which is stale
    by up to the cache's TTL (default 5s) by design.

    ADVISORY semantics, documented and bounded: the read is a check and
    the enqueue is a later act — the race between them is acceptable
    (the signal predicts admission, it does not reserve it). The depth
    half is one indexed aggregate per call; the cap half is the
    ``ActorCapacityCache`` TTL snapshot.
    """

    model_config = ConfigDict(frozen=True)

    queues: dict[str, QueueBackpressure]
    parent_id: JobId | None = None
    as_of: datetime
    """UTC wall clock at the read. The whole snapshot is "as of" this
    instant; the depth half was exact here, the cap half was already up
    to :attr:`cap_age_seconds` old."""
    cap_age_seconds: float | None = None
    """How old the capacity snapshot the caps came from was, in seconds
    (monotonic-clock delta at read time). Bounded by the cache's TTL
    (default 5) when a snapshot exists; ``None`` when no snapshot
    exists — then every cap half reads ``unknown`` with its reason."""


class CancelResult(BaseModel):
    """Structured outcome of a cancellation request.

    Returned by ``JobsClient.cancel()`` so callers can inspect whether
    the cancellation was initiated and what the status transition was.
    """

    model_config = ConfigDict(frozen=True)

    job_id: JobId
    previous_status: JobStatus
    new_status: JobStatus
    cancellation_initiated: bool


@dataclass(frozen=True, slots=True)
class StateChangeEvent:
    """JSON payload for ``job_events.detail`` when ``kind='state_change'``.

    Serialized via ``taskq._json.dumps`` (orjson with UUID support), not
    pydantic, to keep the event-detail path free of validation overhead.
    """

    from_state: JobStatus
    to_state: JobStatus
    error_class: str | None = None
    worker_id: UUID | None = None
