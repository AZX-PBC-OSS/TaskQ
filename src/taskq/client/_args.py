"""Pure helper for constructing EnqueueArgs from caller-facing parameters.

Extracted from :meth:`JobsClient.enqueue` so that both ``JobsClient`` and
the future ``SubJobEnqueuer`` share the same validation and argument-assembly
logic. The helper is pure: no I/O, no global state.
Neither helper takes a clock: "immediate" is expressed as
``scheduled_at=None`` and the backend's server stamps it and decides the
status, so there is no app-clock value to inject (single clock arbiter).
"""

import contextlib
import inspect
import re
import warnings
from collections.abc import Generator, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from types import FrameType
from typing import TYPE_CHECKING, Any
from uuid import UUID

import structlog
from opentelemetry.trace import Span, SpanKind, StatusCode
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey, QueueName
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


def _user_stacklevel() -> int:
    """stacklevel that blames the first frame outside the taskq package.

    Why not a static level: the user's call line sits at a different depth
    per public entry — 3 frames above ``build_enqueue_args`` via
    ``JobsClient.enqueue``, 4 via the ``TaskQ.enqueue`` facade, and a
    different depth again via ``SubJobEnqueuer`` — and the shared helper
    cannot know which one fired.  Walking to the first frame whose module
    is not ``taskq``/``taskq.*`` attributes the user's call line on every
    path (blaming a third-party wrapper is also correct: it is the
    caller's code).  Degenerate fully-internal stacks fall back to 3, the
    nearest public-wrapper depth.
    """
    current = inspect.currentframe()
    # currentframe() lands in this helper; one f_back step reaches
    # build_enqueue_args's frame (stacklevel 1 blames it).
    frame: FrameType | None = current.f_back if current is not None else None
    level = 1  # stacklevel 1 blames build_enqueue_args itself
    while frame is not None:
        module: object = frame.f_globals.get("__name__", "")
        if isinstance(module, str) and module != "taskq" and not module.startswith("taskq."):
            return level
        level += 1
        frame = frame.f_back
    return 3


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
    idempotency_scope: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    schedule_to_close: datetime | None = None,
    start_to_close: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    max_pending: int | None = None,
    unique_for: timedelta | None = None,
    unique_states: tuple[str, ...] | None = None,
    tags: list[str] | None = None,
) -> EnqueueArgs:
    """Validate inputs and construct :class:`EnqueueArgs`.

    Pure function — no I/O, no global state and no clock (see the module
    docstring): ``scheduled_at`` passes through as ``None`` when the caller
    wants "immediate", and the backend's server stamps and decides it.

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

    if idempotency_scope is not None and len(idempotency_scope) > 256:
        raise ValueError(
            f"idempotency_scope must be at most 256 characters, got {len(idempotency_scope)}"
        )

    if start_to_close is not None and start_to_close <= timedelta(0):
        raise ValueError(f"start_to_close must be > 0, got {start_to_close!r}")

    if scheduled_at is not None and scheduled_at.tzinfo is None:
        raise ValueError(
            f"scheduled_at must be timezone-aware (e.g. datetime.now(UTC)); "
            f"got a naive datetime {scheduled_at.isoformat()!r}"
        )

    # result_ttl and schedule_to_close_interval feed server-side
    # clock-anchored computations; a negative value would silently anchor
    # the deadline/expiry in the past.  Mirrors start_to_close's boundary
    # check (and actor-config ops' non-negative result_ttl rule).
    if ref.result_ttl is not None and ref.result_ttl < timedelta(0):
        raise ValueError(f"result_ttl must be non-negative, got {ref.result_ttl!r}")
    budget_interval = time_budget_as_interval(ref.retry)
    if budget_interval is not None and budget_interval < timedelta(0):
        raise ValueError(
            f"schedule_to_close_interval (from retry.time_budget) must be "
            f"non-negative, got {budget_interval!r}"
        )

    payload_dict = ref.payload_type.model_validate(payload).model_dump(mode="json")
    metadata_dict: dict[str, object] = dict(metadata) if metadata is not None else {}
    # Security boundary: "batch_id" is a reserved key injected by the library
    # during batch enqueue. Strip any caller-supplied value to prevent a
    # single-job enqueue from self-asserting membership in a victim batch,
    # which would drive increment/abort hooks on that batch.
    metadata_dict.pop("batch_id", None)
    if ref.singleton:
        metadata_dict["singleton"] = True

    resolved_interval: timedelta | None = None
    resolved_datetime: datetime | None = None

    if schedule_to_close is not None:
        if schedule_to_close.tzinfo is None:
            raise ValueError(
                f"schedule_to_close must be timezone-aware "
                f"(e.g. datetime.now(UTC) + timedelta(...)); "
                f"got a naive datetime {schedule_to_close.isoformat()!r}"
            )
        warnings.warn(
            "schedule_to_close (absolute datetime) is deprecated; declare "
            "retry.time_budget on the actor (interval form) instead — absolute "
            "datetimes cross clock domains (the app clock that produced them "
            "vs the database clock that evaluates them) and can misbehave "
            "under skew; see docs/architecture.md",
            DeprecationWarning,
            stacklevel=_user_stacklevel(),
        )
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
        scheduled_at=scheduled_at,
        priority=resolved_priority,
        max_pending=resolved_max_pending,
        schedule_to_close=resolved_datetime,
        schedule_to_close_interval=resolved_interval,
        start_to_close=resolved_start_to_close,
        heartbeat_timeout=heartbeat_timeout,
        identity_key=identity_key,
        fairness_key=fairness_key,
        idempotency_key=idempotency_key,  # type: ignore[arg-type]  # Why: IdempotencyKey is NewType(str); str values accepted at runtime but pyright cannot narrow str to the NewType
        idempotency_scope=idempotency_scope if idempotency_scope is not None else "",
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
    *,
    max_pending_by_actor: Mapping[str, int | None] | None = None,
) -> list[EnqueueArgs]:
    """Build EnqueueArgs for every item in a batch, merging ``batch_id`` into metadata.

    Shared by :class:`~taskq.client.JobsClient` and
    :class:`~taskq.client.SubJobEnqueuer` to avoid duplicating the
    metadata-merge + ``build_enqueue_args`` loop.

    ``max_pending_by_actor`` optionally carries the caller-resolved
    *effective* ``max_pending`` per actor name (operator-owned stored
    value when set, else the ``@actor(...)`` literal — see
    :class:`taskq.client._capacity.ActorCapacityCache`). Pass it so the
    per-item args enforce the same limit the caller's aggregated check
    just admitted; when omitted, each actor's literal is used, exactly
    as before.
    """
    args_list: list[EnqueueArgs] = []
    batch_id_str = str(batch_id)
    for item in items:
        item_max_pending = (
            max_pending_by_actor.get(item.actor_ref.name)
            if max_pending_by_actor is not None
            else None
        )
        args = build_enqueue_args(
            item.actor_ref,
            item.payload,
            scheduled_at=item.scheduled_at,
            priority=item.priority,
            fairness_key=item.fairness_key,
            identity_key=item.identity_key,
            idempotency_key=item.idempotency_key,
            idempotency_scope=item.idempotency_scope,
            metadata=item.metadata,
            start_to_close=item.start_to_close,
            max_pending=item_max_pending,
            tags=item.tags,
        )
        # Stamp batch_id AFTER build_enqueue_args, which strips any
        # caller-supplied batch_id as a security boundary (H5).
        args = replace(args, metadata={**args.metadata, "batch_id": batch_id_str})
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
