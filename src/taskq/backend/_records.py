"""Pure decode/encode helpers for asyncpg records and jsonb parameters.

These functions are stateless and free of backend instance state, so they
live apart from :class:`~taskq.backend.postgres.PostgresBackend` for
reuse (e.g. the rate-limit modules) and unit testing.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from taskq._json import dumps_str, loads
from taskq.backend._protocol import (
    IdempotencyKey,
    IdentityKey,
    JobId,
    JobRow,
    parse_cancel_phase,
    parse_retry_kind,
)
from taskq.backend._sql import parse_rowcount

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "_job_row_from_record",
    "compute_duration_ms",
    "jsonb_param",
    "jsonb_to_dict",
    "parse_rowcount",
]


def jsonb_to_dict(value: str | dict[str, object] | None) -> dict[str, object] | None:
    """Convert a jsonb column value from an asyncpg Record to a dict.

    asyncpg may return jsonb as a Python dict (if a custom codec is
    registered on the connection) or as a text string (default).  This
    helper normalises both paths.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return loads(value)


def jsonb_param(value: dict[str, object] | None) -> str | None:
    """Serialize a dict for jsonb parameter binding, or return ``None``.

    Uses ``taskq._json.dumps_str`` (orjson) so that UUID and datetime
    values inside the dict are serialised correctly.  The caller adds
    ``::jsonb`` in the SQL string.
    """
    if value is None:
        return None
    return dumps_str(value)


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
    return JobRow(
        id=JobId(rec["id"]),
        actor=rec["actor"],
        queue=rec["queue"],
        identity_key=IdentityKey(raw_identity) if raw_identity is not None else None,
        fairness_key=rec["fairness_key"],
        payload=jsonb_to_dict(rec["payload"]) or {},
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
        progress_state=jsonb_to_dict(rec["progress_state"]) or {},
        progress_seq=rec["progress_seq"],
        result=jsonb_to_dict(rec["result"]),
        result_size_bytes=rec["result_size_bytes"],
        result_expires_at=rec["result_expires_at"],
        idempotency_key=IdempotencyKey(raw_idempotency) if raw_idempotency is not None else None,
        trace_id=rec["trace_id"],
        span_id=rec["span_id"],
        metadata=jsonb_to_dict(rec["metadata"]) or {},
        tags=tuple(rec["tags"]) if rec["tags"] else (),
    )


def compute_duration_ms(started_at: datetime | None, finished_at: datetime | None) -> int | None:
    """Compute duration in milliseconds between started_at and finished_at."""
    if started_at is not None and finished_at is not None:
        return int((finished_at - started_at).total_seconds() * 1000)
    return None
