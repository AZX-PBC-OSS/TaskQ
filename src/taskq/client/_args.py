"""Pure helper for constructing EnqueueArgs from caller-facing parameters.

Extracted from :meth:`JobsClient.enqueue` so that both ``JobsClient`` and
the future ``SubJobEnqueuer`` share the same validation and argument-assembly
logic. The helper is pure: no I/O, no global state.
The clock is a parameter so callers can inject a ``FakeClock`` for test
determinism.
"""

import contextlib
import re
from collections.abc import Generator, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import structlog
from opentelemetry.trace import Span, SpanKind, StatusCode
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey, QueueName
from taskq.backend.clock import Clock
from taskq.obs import record_published_message, safe_start_span
from taskq.retry import time_budget_as_interval

if TYPE_CHECKING:
    from taskq.batch import EnqueueItem

__all__ = ["build_enqueue_args", "enqueue_span"]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

_TAG_RE: re.Pattern[str] = re.compile(r"^[\w][\w\-]+[\w]$")
"""Tag validation regex matching River's pattern: starts/ends with word char, middle allows hyphens, min 3 chars."""
_MAX_TAG_LENGTH: int = 255


def _validate_and_dedup_tags(tags: list[str] | None) -> tuple[str, ...]:
    """Validate and deduplicate a list of tag strings.

    Returns a deduplicated tuple preserving first-occurrence order.
    Raises ValueError for invalid tags.
    """
    if tags is None:
        return ()
    seen: set[str] = set()
    result: list[str] = []
    for tag in tags:
        if not tag:
            raise ValueError("tag must not be empty")
        if len(tag) > _MAX_TAG_LENGTH:
            raise ValueError(f"tag exceeds {_MAX_TAG_LENGTH} characters: {tag!r}")
        if not _TAG_RE.match(tag):
            raise ValueError(
                f"invalid tag {tag!r}: must match pattern ^[\\w][\\w\\-]+[\\w]$ "
                f"(at least 3 chars, word chars and hyphens only, no leading/trailing hyphens)"
            )
        if tag not in seen:
            seen.add(tag)
            result.append(tag)
    return tuple(result)


def build_enqueue_args[P: BaseModel, R: BaseModel | None](
    ref: ActorRef[P, R],
    payload: P,
    *,
    queue: QueueName | None = None,
    scheduled_at: datetime | None = None,
    priority: int | None = None,
    fairness_key: str | None = None,
    metadata: dict[str, object] | None = None,
    identity_key: IdentityKey | None = None,
    idempotency_key: IdempotencyKey | str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    schedule_to_close: datetime | None = None,
    start_to_close: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    max_pending: int | None = None,
    unique_for: timedelta | None = None,
    unique_states: tuple[str, ...] | None = None,
    tags: list[str] | None = None,
    clock: Clock,
) -> EnqueueArgs:
    """Validate inputs and construct :class:`EnqueueArgs`.

    Pure function — no I/O, no global state. The clock is a
    parameter so the caller (JobsClient or SubJobEnqueuer) can pass
    its own injected clock for test determinism.

    ``unique_for`` and ``unique_states`` default to ``None`` so the
    caller can pass actor-declared values (``ref.unique_for``,
    ``ref.unique_states``) or per-call overrides. When ``None``,
    the actor-declared values from ``ref`` are used.
    """
    if idempotency_key is not None:
        if idempotency_key == "":
            raise ValueError("idempotency_key must not be empty")
        if idempotency_key.strip() == "":
            raise ValueError("idempotency_key must not be whitespace-only")
        if len(idempotency_key) > 256:
            raise ValueError(
                f"idempotency_key must be at most 256 characters, got {len(idempotency_key)}"
            )

    if start_to_close is not None and start_to_close <= timedelta(0):
        raise ValueError(f"start_to_close must be > 0, got {start_to_close!r}")

    payload_dict = ref.payload_type.model_validate(payload).model_dump(mode="json")
    metadata_dict: dict[str, object] = dict(metadata) if metadata is not None else {}
    if ref.singleton:
        metadata_dict["singleton"] = True

    budget_interval = time_budget_as_interval(ref.retry)
    resolved_interval: timedelta | None = None
    resolved_datetime: datetime | None = None

    if schedule_to_close is not None:
        resolved_datetime = schedule_to_close
        if budget_interval is not None:
            logger.info(
                "enqueue_schedule_to_close_override",
                actor=ref.name,
                time_budget=str(budget_interval),
                schedule_to_close_override=schedule_to_close.isoformat(),
            )
    elif budget_interval is not None:
        resolved_interval = budget_interval

    resolved_priority = priority if priority is not None else ref.priority
    if resolved_priority < -32768 or resolved_priority > 32767:
        raise ValueError(
            f"priority must fit smallint range (-32768..32767), got {resolved_priority}"
        )

    resolved_unique_for = unique_for if unique_for is not None else ref.unique_for
    resolved_unique_states = unique_states if unique_states is not None else ref.unique_states
    resolved_max_pending = max_pending if max_pending is not None else ref.max_pending
    resolved_start_to_close = start_to_close if start_to_close is not None else ref.start_to_close

    return EnqueueArgs(
        id=new_job_id(),
        actor=ref.name,
        queue=queue if queue is not None else ref.queue,
        payload=payload_dict,
        max_attempts=ref.retry.max_attempts,
        retry_kind=ref.retry.kind,
        scheduled_at=scheduled_at if scheduled_at is not None else clock.now(),
        priority=resolved_priority,
        max_pending=resolved_max_pending,
        schedule_to_close=resolved_datetime,
        schedule_to_close_interval=resolved_interval,
        start_to_close=resolved_start_to_close,
        heartbeat_timeout=heartbeat_timeout,
        identity_key=identity_key,
        fairness_key=fairness_key,
        idempotency_key=idempotency_key,  # type: ignore[arg-type]  # Why: IdempotencyKey is NewType(str); str values accepted at runtime but pyright cannot narrow str to the NewType
        trace_id=trace_id,
        span_id=span_id,
        result_ttl=ref.result_ttl,
        unique_for=resolved_unique_for,
        unique_states=resolved_unique_states,  # type: ignore[arg-type]  # Why: tuple[str, ...] from caller and tuple[JobStatus, ...] from ActorRef both satisfy the runtime contract; JobStatus is Literal[str, ...]
        metadata=metadata_dict,
        tags=_validate_and_dedup_tags(tags),
    )


def build_batch_args(
    items: Sequence["EnqueueItem[Any, Any]"],
    batch_id: UUID,
    clock: Clock,
) -> list[EnqueueArgs]:
    """Build EnqueueArgs for every item in a batch, merging ``batch_id`` into metadata.

    Shared by :class:`~taskq.client.JobsClient` and
    :class:`~taskq.client.SubJobEnqueuer` to avoid duplicating the
    metadata-merge + ``build_enqueue_args`` loop.
    """
    args_list: list[EnqueueArgs] = []
    for item in items:
        merged_metadata: dict[str, object] = dict(item.metadata) | {"batch_id": str(batch_id)}
        args = build_enqueue_args(
            item.actor_ref,
            item.payload,
            scheduled_at=item.scheduled_at,
            priority=item.priority,
            fairness_key=item.fairness_key,
            identity_key=item.identity_key,
            idempotency_key=item.idempotency_key,
            metadata=merged_metadata,
            start_to_close=item.start_to_close,
            tags=item.tags,
            clock=clock,
        )
        args_list.append(args)
    return args_list


@contextlib.contextmanager
def enqueue_span(
    actor_name: str,
    queue_name: str,
    *,
    identity_key: str = "",
) -> Generator[tuple[Span, str | None, str | None], None, None]:
    with safe_start_span(
        f"enqueue {actor_name}",
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.system": "taskq",
            "messaging.destination.name": queue_name,
            "messaging.operation.type": "publish",
            "taskq.actor": actor_name,
            "taskq.identity_key": identity_key,
        },
    ) as span:
        ctx = span.get_span_context()
        if ctx.is_valid:
            extracted_trace_id: str | None = format(ctx.trace_id, "032x")
            extracted_span_id: str | None = format(ctx.span_id, "016x")
        else:
            extracted_trace_id = None
            extracted_span_id = None
        try:
            yield span, extracted_trace_id, extracted_span_id
            span.set_status(StatusCode.OK)
        except Exception:
            span.set_status(StatusCode.ERROR)
            raise
    record_published_message(actor_name, queue_name)
