"""Redis fire-and-forget publish helpers for progress events."""

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import structlog

from taskq.constants import progress_channel, progress_global_channel
from taskq.obs import get_logger, record_progress_publish_failure
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._events import ProgressEvent

if TYPE_CHECKING:
    import redis.asyncio as redis_async

    from taskq.settings import WorkerSettings

__all__ = ["_publish_event", "_publish_progress_event", "_publish_state_change_event"]

_log: structlog.stdlib.BoundLogger = get_logger(__name__)


async def _publish_event(
    redis_client: "redis_async.Redis",  # type: ignore[type-arg]  # Why: redis-py stubs expose Redis as an unparameterised generic; type arg cannot be supplied without a stubs update.
    channel: str,
    event_json: str,
    *,
    seq: int,
    log: structlog.stdlib.BoundLogger,
    channel_label: Literal["per_job", "global"],
) -> None:
    """Fire-and-forget publish to a single Redis channel. Never raises."""
    try:
        await asyncio.wait_for(redis_client.publish(channel, event_json), timeout=1.0)
    except Exception as exc:
        log.warning(
            "progress-publish-failure",
            kind="progress_publish_failure",
            channel=channel,
            seq=seq,
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel=channel_label,
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
            job_id=job_id,
            seq=seq,
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel="per_job",
            error_type=type(exc).__name__,
        )
        return

    log = _log.bind(job_id=job_id, actor=actor, seq=seq)

    per_job_channel = progress_channel(settings.schema_name, job_id)
    await _publish_event(
        redis_client,
        per_job_channel,
        event_json,
        seq=seq,
        log=log,
        channel_label="per_job",
    )

    if settings.progress_publish_global:
        global_channel = progress_global_channel(settings.schema_name)
        await _publish_event(
            redis_client,
            global_channel,
            event_json,
            seq=seq,
            log=log,
            channel_label="global",
        )


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
        pending_state = dict(buffer.pending_state) if buffer is not None else {}
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
            job_id=job_id,
            seq=seq,
            error_type=type(exc).__name__,
        )
        record_progress_publish_failure(
            channel="per_job",
            error_type=type(exc).__name__,
        )
        return
    log = _log.bind(job_id=job_id, actor=actor, seq=seq, status=status)

    schema = settings.schema_name
    per_job_channel = progress_channel(schema, job_id)
    await _publish_event(
        redis_client,
        per_job_channel,
        event_json,
        seq=seq,
        log=log,
        channel_label="per_job",
    )

    if settings.progress_publish_global:
        global_channel = progress_global_channel(schema)
        await _publish_event(
            redis_client,
            global_channel,
            event_json,
            seq=seq,
            log=log,
            channel_label="global",
        )
