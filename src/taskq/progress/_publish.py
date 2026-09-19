"""Redis fire-and-forget publish helpers for progress events.

Failure emission contract: every failed publish round trip bumps the
``progress.publish_failures`` counter (the per-attempt aggregate, the
observable that outages are alerted on), but the ``progress-publish-failure``
WARNING is window-gated to one per channel per window, a sustained Redis
death fails every publish attempt of every job, and a warning line per
attempt is a log flood, not a signal.
"""

import asyncio
from datetime import UTC, datetime
from time import monotonic
from typing import TYPE_CHECKING, Final, Literal
from uuid import UUID

import structlog

from taskq.constants import progress_channel, progress_global_channel
from taskq.obs import get_logger, record_progress_publish_failure
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._events import ProgressEvent

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.settings import WorkerSettings

__all__ = [
    "_publish_event",
    "_publish_event_dual",
    "_publish_progress_event",
    "_publish_progress_event_coalesced",
    "_publish_state_change_event",
]

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

_PUBLISH_TIMEOUT_S: Final[float] = 1.0
"""Bounded worst-case wait for any progress publish round trip."""

_PUBLISH_FAILURE_LOG_WINDOW_S: Final[float] = 60.0
"""Window gating the progress-publish-failure WARNING, the same bound
the registry's keyed heal-failure emission applies to a failure that
repeats on every attempt. The OTel counter stays the per-attempt
aggregate."""

_publish_failure_warned: dict[str, float] = {}
"""Monotonic stamp of the last emitted publish-failure WARNING, keyed by
channel label (bounded: the two-value channel vocabulary)."""


def _publish_failure_warning_due(channel_labels: tuple[str, ...]) -> bool:
    """One WARNING per channel per window; every failure still counts.

    A dual publish's single failed round trip covers every channel it
    buffered, so its caller passes all covered labels: the warning fires
    when any covered channel is outside the window and the stamp then
    covers all of them.
    """
    now = monotonic()
    stamps = [_publish_failure_warned.get(label) for label in channel_labels]
    if all(s is not None and now - s < _PUBLISH_FAILURE_LOG_WINDOW_S for s in stamps):
        return False
    for label in channel_labels:
        _publish_failure_warned[label] = now
    return True


def _failure_identity(job_id: UUID, actor: str, seq: int, status: str | None) -> dict[str, object]:
    """The event-identifying fields of a ``progress-publish-failure`` WARNING.

    Built only on the failure path, a publish that succeeds never
    formats them, and ``status`` appears only for state-change events,
    which are the only ones that carry one.
    """
    fields: dict[str, object] = {"job_id": str(job_id), "actor": actor, "seq": seq}
    if status is not None:
        fields["status"] = status
    return fields


async def _publish_event(
    redis_client: "redis_async.Redis",  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    channel: str,
    event_json: str,
    *,
    job_id: UUID,
    actor: str,
    seq: int,
    status: str | None = None,
    channel_label: Literal["per_job", "global"],
) -> None:
    """Fire-and-forget publish to a single Redis channel. Never raises.

    ``job_id`` / ``actor`` / ``status`` identify the event on the failure
    WARNING only (see :func:`_failure_identity`).
    """
    try:
        await asyncio.wait_for(
            redis_client.publish(channel, event_json), timeout=_PUBLISH_TIMEOUT_S
        )
    except Exception as exc:
        if _publish_failure_warning_due((channel_label,)):
            _log.warning(
                "progress-publish-failure",
                kind="progress_publish_failure",
                channel=channel,
                error_type=type(exc).__name__,
                **_failure_identity(job_id, actor, seq, status),
            )
        record_progress_publish_failure(
            channel=channel_label,
            error_type=type(exc).__name__,
        )


async def _publish_event_dual(
    redis_client: "redis_async.Redis",  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    per_job_channel: str,
    global_channel: str,
    event_json: str,
    *,
    job_id: UUID,
    actor: str,
    seq: int,
    status: str | None = None,
) -> None:
    """Fire-and-forget publish of one event to the per-job and global
    channels in a single pipelined round trip. Never raises.

    ``job_id`` / ``actor`` / ``status`` identify the event on the failure
    WARNING only (see :func:`_failure_identity`).

    Both PUBLISH commands are buffered locally and leave with one
    ``execute``, as two awaited ``publish`` calls, every progress event
    paid two sequential round trips and two worst-case timeout budgets.
    The pipeline is non-transactional on purpose: the two publishes have
    no ordering dependency and no atomicity requirement, so MULTI/EXEC
    would add two extra commands and an EXEC reply for nothing.
    """
    try:
        async with redis_client.pipeline(transaction=False) as pipe:
            pipe.publish(per_job_channel, event_json)
            pipe.publish(global_channel, event_json)
            await asyncio.wait_for(pipe.execute(), timeout=_PUBLISH_TIMEOUT_S)
    except Exception as exc:
        # One execute serves both channels, so the emission gate consults
        # both channel labels: the warning fires once for the round trip
        # and stamps both channels' windows.
        if _publish_failure_warning_due(("per_job", "global")):
            _log.warning(
                "progress-publish-failure",
                kind="progress_publish_failure",
                channels=[per_job_channel, global_channel],
                error_type=type(exc).__name__,
                **_failure_identity(job_id, actor, seq, status),
            )
        # One execute serves both channels, so a failed round trip means
        # both channel-level delivery failures are true, recorded once
        # each, which also preserves the counter total of the sequential
        # shape this replaces, where a hard Redis outage incremented both.
        record_progress_publish_failure(
            channel="per_job",
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel="global",
            error_type=type(exc).__name__,
        )


async def _publish_progress_event(
    redis_client: "redis_async.Redis",  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    settings: "WorkerSettings",
    actor: str,
    job_id: UUID,
    *,
    step: int | None,
    percent: float | None,
    detail: str | None,
    data: dict[str, object] | None,
    seq: int,
) -> None:
    """Build a kind='progress' event and publish to the per-job channel."""
    try:
        event = ProgressEvent(
            kind="progress",
            job_id=job_id,
            actor=actor,
            ts=datetime.now(UTC),
            seq=seq,
            status="running",
            step=step,
            percent=percent,
            detail=detail,
            data=data,
            terminal=False,
        )
        event_json = event.model_dump_json(exclude_none=True)
    except Exception as exc:
        _log.warning(
            "progress-publish-failure",
            kind="progress_publish_failure",
            job_id=str(job_id),
            seq=seq,
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel="per_job",
            error_type=type(exc).__name__,
        )
        return

    per_job_channel = progress_channel(settings.schema_name, job_id)
    if settings.progress_publish_global:
        await _publish_event_dual(
            redis_client,
            per_job_channel,
            progress_global_channel(settings.schema_name),
            event_json,
            job_id=job_id,
            actor=actor,
            seq=seq,
        )
    else:
        await _publish_event(
            redis_client,
            per_job_channel,
            event_json,
            job_id=job_id,
            actor=actor,
            seq=seq,
            channel_label="per_job",
        )


async def _publish_progress_event_coalesced(
    redis_client: "redis_async.Redis",  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    settings: "WorkerSettings",
    actor: str,
    job_id: UUID,
    buffer: _ProgressBuffer,
    *,
    step: int | None,
    percent: float | None,
    detail: str | None,
    data: dict[str, object] | None,
    seq: int,
) -> None:
    """Publish one progress event under the per-job in-flight gate.

    ``ctx.progress()`` fires at actor call rate, and an uncoalesced
    publish per call costs one Redis round trip per call (the Postgres
    side has coalesced into the flush buffer all along). This wrapper
    keeps at most ONE publish in flight per job: a progress call that
    lands while one is running only latches its event on the buffer's
    ``pending_publish`` (set by the caller, see ``JobContext.progress``),
    and the in-flight task re-publishes the latched event when its round
    trip lands, draining the latch until it stays empty.

    The latch is the no-lost-final guarantee: a trailing progress call
    always reaches the channel, either directly (gate free) or through
    the latch (the in-flight task drains it before releasing the gate).
    Residual: intermediate events published while the gate is busy are
    skipped, a live subscriber sees the coalesced tail instead of every
    step (the consumer seq-discard discipline already treats any event
    as replaceable by a later seq), and if this task is CANCELLED
    (worker shutdown does not drain the latch, it abandons the tasks) a
    latched event is lost with it: the terminal state-change publish and
    the durable Postgres flush still carry the final state.
    """
    try:
        await _publish_progress_event(
            redis_client,
            settings,
            actor,
            job_id,
            step=step,
            percent=percent,
            detail=detail,
            data=data,
            seq=seq,
        )
        while buffer.pending_publish is not None:
            # Read, clear, then publish: a progress call landing during the
            # round trip below latches a NEWER event on the buffer, and the
            # next iteration publishes that one.
            pending = buffer.pending_publish
            buffer.pending_publish = None
            await _publish_progress_event(
                redis_client,
                settings,
                actor,
                job_id,
                step=pending.step,
                percent=pending.percent,
                detail=pending.detail,
                data=pending.data,
                seq=pending.seq,
            )
    finally:
        # Cleared with no await since the last round trip completed, so no
        # progress call can observe a free gate with this task's latch
        # still unread: the task either drained it or it is empty.
        buffer.publish_in_flight = False


async def _publish_state_change_event(
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings",
    job_id: UUID,
    actor: str,
    progress_buffers: "dict[UUID, _ProgressBuffer] | None",
    *,
    status: str,
    terminal: bool,
    _override_seq: int | None = None,
    _override_pending_state: dict[str, object] | None = None,
) -> None:
    """Publish a kind='state_change' event to per-job (and optionally global) channels.

    Parameters are explicit (no WorkerDeps struct) for consistency with the
    rest of the progress module. ``actor`` is passed from ``job.actor`` at
    each call site. The ``_override_seq`` and ``_override_pending_state``
    parameters are used only on the cancel path where the buffer has already
    been popped before the publish.

    State-change events do NOT increment progress_seq.
    """
    if redis_client is None:
        return

    if _override_pending_state is not None:
        pending_state = _override_pending_state
        seq = _override_seq if _override_seq is not None else 0
    else:
        buffer = progress_buffers.get(job_id) if progress_buffers is not None else None
        # Read in place: the event is built synchronously from four
        # .get() reads before anything else can touch the buffer, so a
        # defensive copy would only cost.
        pending_state = buffer.pending_state if buffer is not None else {}
        seq = (buffer.base_seq + buffer.pending_seq_delta) if buffer is not None else 0

    try:
        event = ProgressEvent(
            kind="state_change",
            job_id=job_id,
            actor=actor,
            ts=datetime.now(UTC),
            seq=seq,
            status=status,
            step=pending_state.get("step"),  # type: ignore[arg-type]  # Why: pending_state is dict[str, object]; field types (int, float, str, dict) are runtime-correct but pyright cannot verify the narrowing through a generic dict.
            percent=pending_state.get("percent"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
            detail=pending_state.get("detail"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
            data=pending_state.get("data"),  # type: ignore[arg-type]  # Why: same erasure boundary as above.
            terminal=terminal,
        )
        event_json = event.model_dump_json(exclude_none=True)
    except Exception as exc:
        _log.warning(
            "progress-publish-failure",
            kind="progress_publish_failure",
            job_id=str(job_id),
            seq=seq,
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel="per_job",
            error_type=type(exc).__name__,
        )
        return
    schema = settings.schema_name
    per_job_channel = progress_channel(schema, job_id)
    if settings.progress_publish_global:
        await _publish_event_dual(
            redis_client,
            per_job_channel,
            progress_global_channel(schema),
            event_json,
            job_id=job_id,
            actor=actor,
            seq=seq,
            status=status,
        )
    else:
        await _publish_event(
            redis_client,
            per_job_channel,
            event_json,
            job_id=job_id,
            actor=actor,
            seq=seq,
            status=status,
            channel_label="per_job",
        )
