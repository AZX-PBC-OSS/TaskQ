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

__all__ = [
    "bind_job_context",
    "get_logger",
    "log_cancel_phase_change",
    "log_state_change",
    "redact_payload",
    "setup_logging",
]

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
    ]

    if log_format == "console":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        from taskq._json import structlog_serializer

        renderer = structlog.processors.JSONRenderer(serializer=structlog_serializer)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=[
            _safe_processor_wrapper(structlog.processors.TimeStamper(fmt="iso", utc=True)),
            _safe_processor_wrapper(structlog.stdlib.add_log_level),
            _safe_processor_wrapper(structlog.stdlib.ExtraAdder()),
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
    fields: dict[str, UUID | str | int] = {
        "job_id": job_id,
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
