"""Structlog configuration and logger accessor.

Provides the canonical processor chain, OTel span context injection, and
the ``get_logger`` helper that returns a typed ``structlog.stdlib.BoundLogger``
instead of the ``Any`` that ``structlog.get_logger`` returns.
"""

import hashlib
import logging
from uuid import UUID

import structlog
from opentelemetry import trace

from taskq._json import dumps_str
from taskq.obs._redact_exc import (
    EXCEPTION_MESSAGE_FIELDS,
    EXCEPTION_TRACEBACK_FIELDS,
    safe_exception_parts,
    scrub_exception_field,
)

__all__ = [
    "bind_job_context",
    "get_logger",
    "log_cancel_phase_change",
    "log_state_change",
    "redact_payload",
    "setup_logging",
]

_EXCEPTION_FIELD_NAMES = EXCEPTION_MESSAGE_FIELDS | EXCEPTION_TRACEBACK_FIELDS

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.obs._structlog")

_logging_configured: bool = False


def _otel_span_processor(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Inject ``trace_id`` and ``span_id`` from the active OTel span context.

    Reads ``opentelemetry.trace.get_current_span().get_span_context()`` on every
    log call so nested sub-spans within a job are reflected in ``span_id``.
    ``opentelemetry-api`` is a hard dep — no conditional import guard needed.
    """
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _safe_processor_wrapper(
    processor: structlog.types.Processor,
) -> structlog.types.Processor:
    """Wrap a single processor so exceptions are caught and logged.

    Structured logging must not raise exceptions that propagate to user or actor
    code. Each processor is wrapped so that if it raises, the exception is logged
    at ``warning`` level (including the processor name) and the event dict passes
    through unchanged.
    """

    def _wrapper(
        logger: object, method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        try:
            result = processor(logger, method, event_dict)
            if isinstance(result, dict):
                return result
            return event_dict
        except Exception:
            proc_name = getattr(processor, "__name__", repr(processor))
            logging.getLogger("taskq.obs._structlog").warning(
                "structlog processor %s raised; event=%r",
                proc_name,
                event_dict.get("event"),
                exc_info=True,
            )
            return event_dict

    return _wrapper


def _render_exc_info_safe(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Replace ``exc_info`` with scrubbed ``exception.*`` keys on the JSON channel.

    Mirrors structlog's ``format_exc_info`` key semantics — pop ``exc_info``,
    render only when it resolves to a real exception — but renders through the
    ``_redact_exc`` helpers so Postgres DETAIL row values and URI credentials
    never reach the JSON log line. Without this, ``log.exception()`` shipped a
    leftover ``"exc_info": true`` bool and foreign stdlib records with real
    ``exc_info`` tuples died in the orjson fallback (whole line dropped).
    """
    exc_info = event_dict.pop("exc_info", None)
    parts = safe_exception_parts(exc_info)
    if parts is not None:
        event_dict.update(parts)
    return event_dict


class _ExcInfoSafeBoundLogger(structlog.stdlib.BoundLogger):
    """``BoundLogger`` whose ``.exception()`` never sets ``exc_info`` on the record.

    Why: structlog's own ``exception()`` proxies to ``logging.Logger.exception``,
    and *that* hard-codes ``exc_info=True`` in the stdlib call — so
    ``record.exc_info`` is populated with the live ``sys.exc_info()`` triple no
    matter what the processor chain did to the event dict. Every root handler
    then reads it: ``setup_logging`` installs on the ROOT logger and
    ``worker_main`` calls it unconditionally, and Azure Monitor's
    ``configure_azure_monitor()`` attaches its ``LoggingHandler`` alongside,
    reads ``record.exc_info`` directly, and ships the raw ``str(exc)`` plus the
    full traceback to the App Insights ``exceptions`` table — Postgres DETAIL
    row values and all.

    No processor can close that, because the leak is added *after* the chain
    runs. Routing to ``error`` instead keeps ``exc_info`` inside the event dict,
    where :func:`_render_exc_info_safe` replaces it with scrubbed
    ``exception.*`` fields before the record exists.
    """

    def exception(self, event: str | None = None, *args: object, **kw: object) -> object:
        kw.setdefault("exc_info", True)
        return self._proxy_to_logger("error", event, *args, **kw)  # type: ignore[arg-type]  # Why: structlog types *event_args as str; callers pass logging-style args of any type, matching the base class's own Any-typed signature.


def _scrub_exception_fields(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Scrub the known exception-bearing field names (the
    ``EXCEPTION_MESSAGE_FIELDS`` / ``EXCEPTION_TRACEBACK_FIELDS`` sets —
    ``error``, ``error_message``, ``error_traceback``, ``exc``, and the
    terminal-write log's ``job_error_*`` / ``infra_error_*`` names).

    JSON logs ship to the same telemetry backends as spans, so raw
    ``str(exc)``-shaped field values reopen the surface
    ``record_exception_safe`` closes; exception OBJECTS additionally die in
    the orjson fallback and drop the whole line. Values that are not exception
    text (classification strings, ints) pass through unchanged.
    """
    for key in _EXCEPTION_FIELD_NAMES.intersection(event_dict):
        event_dict[key] = scrub_exception_field(key, event_dict[key])
    return event_dict


def setup_logging(
    *,
    level: str = "INFO",
    log_format: str = "json",
) -> None:
    """Configure structlog with the canonical processor chain.

    Production (``log_format="json"``): ``JSONRenderer`` via
    ``ProcessorFormatter`` stdlib bridge. Development (``log_format="console"``):
    ``ConsoleRenderer`` via ``ProcessorFormatter``. Idempotent — guarded
    by ``_logging_configured`` flag. Not called at import time .
    """
    global _logging_configured
    if _logging_configured:
        return

    shared_processors: list[structlog.types.Processor] = [
        _safe_processor_wrapper(structlog.contextvars.merge_contextvars),
        _safe_processor_wrapper(structlog.stdlib.add_log_level),
        _safe_processor_wrapper(structlog.stdlib.add_logger_name),
        _safe_processor_wrapper(structlog.processors.StackInfoRenderer()),
        _safe_processor_wrapper(structlog.processors.TimeStamper(fmt="iso", utc=True)),
        _safe_processor_wrapper(_otel_span_processor),
        _safe_processor_wrapper(structlog.processors.EventRenamer("event")),
        # Last before the formatter handoff: final scrub of exception-bearing
        # fields so both renderers (and any future one) see scrubbed values.
        _safe_processor_wrapper(_scrub_exception_fields),
        # SHARED, not formatter-local: whatever survives this chain becomes
        # ``record.msg`` and is read by every root handler, not just TaskQ's.
        # A raw exception object or ``sys.exc_info()`` triple left on the event
        # dict is therefore an export surface for any vendor handler that
        # stringifies values. Console pays for this with a plain scrubbed
        # ``exception.stacktrace`` field instead of ConsoleRenderer's pretty
        # traceback — the same record reaches the same vendor handlers whichever
        # renderer the operator picked, so the dev view does not get an
        # unredacted exemption.
        _safe_processor_wrapper(_render_exc_info_safe),
    ]

    formatter_processors: list[structlog.types.Processor]
    if log_format == "console":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
        formatter_processors = [
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ]
    else:
        from taskq._json import structlog_serializer

        renderer = structlog.processors.JSONRenderer(serializer=structlog_serializer)
        formatter_processors = [
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # Still needed for FOREIGN records: ``ProcessorFormatter`` lifts
            # their ``record.exc_info`` onto the event dict here, after the
            # shared chain has run, and orjson drops the whole line on a raw
            # tuple. Idempotent for TaskQ's own records — ``exc_info`` is
            # already gone by then.
            _safe_processor_wrapper(_render_exc_info_safe),
            renderer,
        ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=_ExcInfoSafeBoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=formatter_processors,
        foreign_pre_chain=[
            _safe_processor_wrapper(structlog.processors.TimeStamper(fmt="iso", utc=True)),
            _safe_processor_wrapper(structlog.stdlib.add_log_level),
            _safe_processor_wrapper(structlog.stdlib.ExtraAdder()),
            # After ExtraAdder so foreign records' extras are scrubbed too.
            _safe_processor_wrapper(_scrub_exception_fields),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    if not any(
        isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
        for h in logging.root.handlers
    ):
        logging.root.addHandler(handler)

    logging.root.setLevel(level)

    _logging_configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a ``structlog.stdlib.BoundLogger`` for the given dotted name.

    Replaces direct ``structlog.get_logger()`` calls in library code so that
    pyright strict mode gets an explicit return type (``structlog.get_logger``
    returns ``Any``).
    """
    return structlog.get_logger(name)


def bind_job_context(
    log: structlog.stdlib.BoundLogger,
    *,
    job_id: UUID,
    actor: str,
    queue: str,
    attempt: int,
    identity_key: str | None,
    trace_id: str,
    span_id: str | None = None,
    batch_id: str | None = None,
) -> structlog.stdlib.BoundLogger:
    """Bind job-scope fields to a logger, returning a new immutable BoundLogger.

    ``identity_key``, ``span_id``, and ``batch_id`` are omitted from the bound
    dict when ``None`` — not set to null or empty string .  ``trace_id``
    is always bound (defaults to ``""`` when no active OTel span per spec).
    Returns a new ``BoundLogger``; does not mutate the input.
    """
    fields: dict[str, str | int] = {
        "job_id": str(job_id),
        "actor": actor,
        "queue": queue,
        "attempt": attempt,
        "trace_id": trace_id,
    }
    if identity_key is not None:
        fields["identity_key"] = identity_key
    if span_id is not None:
        fields["span_id"] = span_id
    if batch_id is not None:
        fields["batch_id"] = batch_id
    return log.bind(**fields)


def log_state_change(
    log: structlog.stdlib.BoundLogger,
    *,
    from_state: str,
    to_state: str,
    **extra: object,
) -> None:
    """Emit an INFO log line with ``kind="state_change"``.

    ``from_state`` and ``to_state`` are the job-status values before and
    after the transition.  All bound fields from the pre-bound ``log``
    (which carries job context from :func:`bind_job_context`) are included
    automatically.  The event name is ``"state-change"`` so the log is
    queryable by both event and kind.
    """
    log.info("state-change", kind="state_change", from_state=from_state, to_state=to_state, **extra)


def log_cancel_phase_change(
    log: structlog.stdlib.BoundLogger,
    *,
    from_phase: int,
    to_phase: int,
    **extra: object,
) -> None:
    """Emit an INFO log line with ``kind="cancel_phase_change"``.

    ``from_phase`` and ``to_phase`` are the cancel-phase integers before
    and after the escalation.  ``cancel_observed_at`` is NOT included — it
    is per-handler context, not part of the canonical schema.
    """
    log.info(
        "cancel_phase_change",
        kind="cancel_phase_change",
        from_phase=from_phase,
        to_phase=to_phase,
        **extra,
    )


def redact_payload(payload: object) -> str:
    """Return the first 16 characters of the SHA-256 hex digest of the JSON-serialized payload.

    Raw payload content does not appear in the return value.  Deterministic
    for the same input.
    """
    serialized = dumps_str(payload).encode()
    return hashlib.sha256(serialized).hexdigest()[:16]
