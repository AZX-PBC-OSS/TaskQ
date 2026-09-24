"""Pure decode/encode helpers for asyncpg records and jsonb parameters.

These functions are stateless and free of backend instance state, so they
live apart from :class:`~taskq.backend.postgres.PostgresBackend` for
reuse (e.g. the rate-limit modules) and unit testing.
"""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal

from taskq._json import NUL_JSONB_ERROR, dumps_jsonb_str, loads
from taskq.backend._protocol import (
    BatchRow,
    EnqueueArgs,
    IdempotencyKey,
    IdentityKey,
    JobId,
    JobRow,
    parse_batch_status,
    parse_cancel_phase,
    parse_retry_kind,
)
from taskq.backend._sql import parse_rowcount
from taskq.exceptions import CorruptJobDataError, PayloadValidationError

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_batch_row_from_record",
    "_job_row_from_record",
    "compute_duration_ms",
    "item_jsonb_param",
    "item_tags_jsonb_param",
    "jsonb_param",
    "jsonb_to_dict",
    "metadata_jsonb_param",
    "parse_rowcount",
    "payload_jsonb_param",
]


def jsonb_to_dict(
    value: str | dict[str, object] | None,
    *,
    column: str = "jsonb",
) -> dict[str, object] | None:
    """Convert a jsonb column value from an asyncpg Record to a dict.

    asyncpg may return jsonb as a Python dict (if a custom codec is
    registered on the connection) or as a text string (default).  This
    helper normalises both paths.

    The decode is classified here, at the boundary: text that parses
    fails with :class:`~taskq.exceptions.CorruptJobDataError` (never the
    raw ``orjson.JSONDecodeError``), and a JSON body that is not an
    object (a list, scalar, or array a different version's codec or a
    hand-corrupted row left in a dict-typed column) raises the same
    class instead of landing a wrong-typed value in a
    ``dict``-typed row field to ``AttributeError`` downstream. PG's own
    jsonb invariant makes malformed text unreachable through an intact
    schema; the classification exists for the shapes that reach Python
    anyway (schema drift, interop writers, corruption).
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        decoded = loads(value)
    except ValueError as exc:
        raise CorruptJobDataError(
            f"{column} column holds text that is not valid JSON: {exc}",
            column=column,
        ) from exc
    if not isinstance(decoded, dict):
        raise CorruptJobDataError(
            f"{column} column holds valid JSON whose body is "
            f"{type(decoded).__name__}, not the object the row contract requires",
            column=column,
        )
    return decoded  # pyright: ignore[reportUnknownVariableType]  # Why: orjson.loads returns Any; the isinstance narrowing above proves the dict shape but pyright sees the Any's dict as partially unknown, the same erasure every loads() consumer tolerates.


def jsonb_param(value: dict[str, object] | None) -> str | None:
    """Serialize a dict for jsonb parameter binding, or return ``None``.

    Uses ``taskq._json.dumps_jsonb_str`` (orjson) so that UUID and datetime
    values inside the dict are serialised correctly.  The caller adds
    ``::jsonb`` in the SQL string.

    Raises ``ValueError`` when the value carries a NUL (U+0000), which
    ``jsonb`` cannot store, see :func:`~taskq._json.dumps_jsonb_str` for why
    that has to fail here rather than inside the INSERT.
    """
    if value is None:
        return None
    return dumps_jsonb_str(value)


def payload_jsonb_param(args: EnqueueArgs) -> str | None:
    """Bind ``args.payload`` for jsonb, memoizing the encoding on the args.

    Why memoized: :func:`~taskq.client._args.build_enqueue_args` already
    owns the payload's only other serialization (the pydantic
    ``model_dump`` into the dict), and the INSERT statement can re-execute
    on the bounded retry arms (savepoint rollback, parked-connection
    retry), each execution of the un-memoized shape re-encoding the same
    dict. The first binding encodes once through the very same
    :func:`jsonb_param` (NUL guard included); later bindings of the same
    args reuse the encoded form. ``dataclasses.replace`` cannot smuggle a
    stale memo through: ``__post_init__`` drops both memos on any
    re-construction.
    """
    if args.payload_jsonb_memo is None:
        object.__setattr__(args, "payload_jsonb_memo", jsonb_param(args.payload))
    return args.payload_jsonb_memo


def metadata_jsonb_param(args: EnqueueArgs) -> str | None:
    """Bind ``args.metadata`` for jsonb, memoizing the encoding on the args.

    Same contract as :func:`payload_jsonb_param`, for the metadata column.
    """
    if args.metadata_jsonb_memo is None:
        object.__setattr__(args, "metadata_jsonb_memo", jsonb_param(args.metadata))
    return args.metadata_jsonb_memo


def _nul_item_payload_error(*, idx: int, field: str, actor: str) -> PayloadValidationError:
    """The per-item NUL rejection, in the client layer's annotation shape.

    Message and ``validation_errors`` mirror ``_item_payload_error`` in
    ``taskq.client._jobs`` (item index + actor in the message; sanitized
    error entries with no ``input``/``url`` keys) so a batch caller sees
    one consistent annotation contract whether the defect was caught by
    pydantic or by the jsonb serialization guard. ``item_index`` carries
    the same position as a field: ``idx`` is already in the CALLER's
    coordinate space at every call site (the bulk build loops add their
    ``index_base`` before calling the guards below), so the field needs
    no second shift anywhere.
    """
    return PayloadValidationError(
        f"Payload validation failed for item {idx} (actor={actor!r}): {field} {NUL_JSONB_ERROR}",
        actor=actor,
        validation_errors=[{"type": "value_error", "loc": (field,), "msg": NUL_JSONB_ERROR}],
        item_index=idx,
    )


def item_jsonb_param(
    value: dict[str, object] | None,
    *,
    idx: int,
    field: Literal["payload", "metadata"],
    actor: str,
) -> str:
    """``jsonb_param`` for one *batch* item, with per-item NUL attribution.

    Why: the batch build loops serialize every item before any SQL runs,
    so a NUL in any item previously raised a bare ``ValueError`` that
    named neither the item nor the field, one bad item aborted the
    whole batch with no attribution. Pydantic validation failures get
    per-item annotation in the client layer; the NUL ``ValueError``
    bypassed that contract, so the same annotation is attached here, at
    the serialization layer both backends share. The batch still refuses
    atomically: callers use this in the build loop, before any statement
    is issued, so nothing is written (attribution, not partial
    admission).

    ``None`` normalizes to ``'{}'``, the batch loops' ``or '{}'``
    folded in so call sites stay one call.
    """
    try:
        return jsonb_param(value) or "{}"
    except ValueError as exc:
        raise _nul_item_payload_error(idx=idx, field=field, actor=actor) from exc


def item_tags_jsonb_param(tags: tuple[str, ...], *, idx: int, actor: str) -> str:
    """``dumps_jsonb_str`` for one *batch* item's tags, same attribution.

    The batch path binds tags as ``$N::jsonb[]`` (jagged-array transit),
    so a NUL tag hits the same ``jsonb_in`` rejection as a NUL payload ,
    see :func:`item_jsonb_param` for the annotation rationale. Only
    reachable by bypassing the ``EnqueueArgs`` text-field chokepoint
    (``__post_init__`` rejects NUL tags at construction); the guard here
    keeps the serialization layer honest about what it actually binds.
    """
    try:
        return dumps_jsonb_str(list(tags))
    except ValueError as exc:
        raise _nul_item_payload_error(idx=idx, field="tags", actor=actor) from exc


def _job_row_from_record(rec: "asyncpg.Record") -> JobRow:
    """Convert an ``asyncpg.Record`` (from ``RETURNING *``) into a frozen
    ``JobRow``.

    Handles jsonb columns (``metadata``, ``payload``, ``progress_state``,
    ``result``) and interval columns (``start_to_close``,
    ``heartbeat_timeout``) which asyncpg returns natively as
    ``datetime.timedelta``.
    """
    raw_identity = rec["identity_key"]
    raw_idempotency = rec["idempotency_key"]
    raw_scope = rec["idempotency_scope"]
    return JobRow(
        id=JobId(rec["id"]),
        actor=rec["actor"],
        queue=rec["queue"],
        identity_key=IdentityKey(raw_identity) if raw_identity is not None else None,
        fairness_key=rec["fairness_key"],
        payload=jsonb_to_dict(rec["payload"], column="payload") or {},
        payload_schema_ver=rec["payload_schema_ver"],
        status=rec["status"],  # type: ignore[arg-type]  # Why: asyncpg returns PG enum as str; JobStatus is Literal[str, ...]
        priority=rec["priority"],
        attempt=rec["attempt"],
        max_attempts=rec["max_attempts"],
        retry_kind=parse_retry_kind(rec["retry_kind"]),
        schedule_to_close=rec["schedule_to_close"],
        start_to_close=rec["start_to_close"],
        heartbeat_timeout=rec["heartbeat_timeout"],
        created_at=rec["created_at"],
        scheduled_at=rec["scheduled_at"],
        started_at=rec["started_at"],
        finished_at=rec["finished_at"],
        last_heartbeat_at=rec["last_heartbeat_at"],
        locked_by_worker=rec["locked_by_worker"],
        lock_expires_at=rec["lock_expires_at"],
        cancel_requested_at=rec["cancel_requested_at"],
        cancel_phase=parse_cancel_phase(rec["cancel_phase"]),
        error_class=rec["error_class"],
        error_message=rec["error_message"],
        error_traceback=rec["error_traceback"],
        progress_state=jsonb_to_dict(rec["progress_state"], column="progress_state") or {},
        progress_seq=rec["progress_seq"],
        result=jsonb_to_dict(rec["result"], column="result"),
        result_size_bytes=rec["result_size_bytes"],
        result_expires_at=rec["result_expires_at"],
        idempotency_key=IdempotencyKey(raw_idempotency) if raw_idempotency is not None else None,
        idempotency_scope=raw_scope,
        trace_id=rec["trace_id"],
        span_id=rec["span_id"],
        metadata=jsonb_to_dict(rec["metadata"], column="metadata") or {},
        tags=tuple(rec["tags"]) if rec["tags"] else (),
        snooze_count=rec["snooze_count"],
        rate_limit_blocked_count=rec["rate_limit_blocked_count"],
        interrupt_count=rec["interrupt_count"],
        retry_base=timedelta(seconds=rec["retry_base_seconds"]),
        retry_cap=timedelta(seconds=rec["retry_cap_seconds"]),
        retry_backoff=rec["retry_backoff"],  # type: ignore[arg-type]  # Why: DB text column; domain is CHECK-constrained to the Literal's values
        retry_jitter=rec["retry_jitter"],
        assignment_routed=rec["assignment_routed"],
        claim_epoch=rec["claim_epoch"],
    )


def _batch_row_from_record(rec: "asyncpg.Record") -> BatchRow:
    """Convert an ``asyncpg.Record`` from the ``batches`` table into a frozen
    :class:`BatchRow`.

    Shared by :mod:`taskq.backend._batch_sql` (PostgresBackend helpers) and
    :mod:`taskq.batch` (``wait_for_batch``) to avoid duplicated field mapping.
    """
    return BatchRow(
        id=rec["id"],
        queue=rec["queue"],
        status=parse_batch_status(rec["status"]),
        expected_size=rec["expected_size"],
        consecutive_failures=rec["consecutive_failures"],
        failure_threshold=rec["failure_threshold"],
        finalizer_job_id=rec["finalizer_job_id"],
        originating_actor=rec["originating_actor"],
        created_at=rec["created_at"],
        completed_at=rec["completed_at"],
        metadata=jsonb_to_dict(rec["metadata"], column="batches.metadata") or {},
    )


def compute_duration_ms(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    """Compute duration in milliseconds between started_at and finished_at."""
    if started_at is not None and finished_at is not None:
        return int((finished_at - started_at).total_seconds() * 1000)
    return None
