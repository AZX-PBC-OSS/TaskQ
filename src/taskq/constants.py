"""Cross-cutting constants shared across the TaskQ library.

Centralises values that are referenced from multiple modules so that
producer and consumer always agree — e.g. the ``pg_notify`` channel name
used by the worker LISTEN consumer and the future
PostgresBackend enqueue path.
"""

import re
from datetime import timedelta
from typing import Final
from uuid import UUID

__all__ = [
    "CRON_LOCK_NAME",
    "DEFAULT_RESERVATION_BACKOFF",
    "EVENTS_CHANNEL_FMT",
    "MAX_RESULT_BYTES",
    "PROGRESS_CHANNEL_FMT",
    "PROGRESS_GLOBAL_CHANNEL_FMT",
    "WAKE_CHANNEL_FMT",
    "WORKER_CHANNEL_FMT",
    "events_channel",
    "progress_channel",
    "progress_global_channel",
    "quote_ident",
    "wake_channel",
    "worker_channel",
]

DEFAULT_RESERVATION_BACKOFF: Final[timedelta] = timedelta(seconds=5)
"""Default backoff when ``RateLimitDecision.retry_after`` is ``None``.

Callers MUST coalesce via an identity check (``is None``), NOT truthiness,
because ``timedelta(0)`` is falsy and represents an allowed decision that
must be passed through unchanged.
"""

MAX_RESULT_BYTES: Final[int] = 65536
"""Maximum serialised byte length of a job's terminal result dict.

Enforced on both the consumer success path (before ``mark_succeeded``) and
the backend terminal-write path (inside ``_mark_succeeded_on_conn``) so a
result that slips past the consumer check is still rejected at the storage
boundary.
"""

CRON_LOCK_NAME: Final[str] = "taskq:cron"
"""Advisory lock name for the cron scheduler leader."""

WAKE_CHANNEL_FMT: Final[str] = "taskq_wake_{schema}"
"""Format template for the wake-channel name."""

EVENTS_CHANNEL_FMT: Final[str] = "taskq_events_{schema}"
"""Format template for the fleet-wide worker-events channel.

All workers in a schema subscribe to this channel.  Each NOTIFY payload
is a JSON object with a ``"type"`` discriminator field so receivers can
route to the appropriate handler without dedicated per-event channels.
"""

WORKER_CHANNEL_FMT: Final[str] = "taskq_worker_{schema}_{worker_id}"
"""Format template for the per-worker events channel.

Only the target worker subscribes, so no payload filtering is needed.
Uses the same JSON payload format as EVENTS_CHANNEL_FMT.
"""

PROGRESS_CHANNEL_FMT: Final[str] = "taskq:{schema}:progress:{job_id}"
"""Format template for the per-job progress channel."""

PROGRESS_GLOBAL_CHANNEL_FMT: Final[str] = "taskq:{schema}:progress"
"""Format template for the schema-wide progress fanout channel."""

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_ident(identifier: str) -> str:
    """Return *identifier* safely wrapped in double-quotes for SQL interpolation.

    Validates against canonical identifier regex before quoting.
    """
    if not _IDENT_RE.match(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def wake_channel(schema: str) -> str:
    """Return the formatted wake-channel name for *schema*.

    Validates *schema* against the same identifier regex used by the
    migration runner so that only safe names reach SQL interpolation.
    Raises :class:`ValueError` on invalid input.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return WAKE_CHANNEL_FMT.format(schema=schema)


def events_channel(schema: str) -> str:
    """Return the fleet-wide events channel name for *schema*.

    All workers in the schema subscribe to this channel.  The NOTIFY
    payload is a JSON object with a ``"type"`` discriminator so workers
    can route without dedicated per-event channels.  Workers filter on
    the ``"worker_id"`` field where relevant.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return EVENTS_CHANNEL_FMT.format(schema=schema)


def worker_channel(schema: str, worker_id: str) -> str:
    """Return the per-worker events channel name for *schema* and *worker_id*.

    Only the worker with *worker_id* subscribes to this channel, so no
    payload filtering is needed.  Uses the same JSON payload format as the
    fleet-wide events channel.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return WORKER_CHANNEL_FMT.format(schema=schema, worker_id=worker_id)


def progress_channel(schema: str, job_id: UUID | str) -> str:
    """Return the per-job progress channel name for *schema* and *job_id*.

    Workers publish :class:`~taskq.progress.ProgressEvent` JSON to this
    channel; SSE consumers subscribe per job_id.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return PROGRESS_CHANNEL_FMT.format(schema=schema, job_id=job_id)


def progress_global_channel(schema: str) -> str:
    """Return the schema-wide progress fanout channel name for *schema*.

    All progress events for the schema are also published here so that a
    single subscriber can receive updates for every job.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return PROGRESS_GLOBAL_CHANNEL_FMT.format(schema=schema)
