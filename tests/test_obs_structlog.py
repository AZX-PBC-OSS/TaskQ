"""Unit tests for structured logging infrastructure.

Validates the obs._structlog functions in isolation using
``structlog.testing.capture_logs()`` and the in-memory backend.

The ``_logging_configured_guard`` autouse fixture (imported into
conftest.py from ``taskq.testing.otel``) snapshots and restores structlog
global state between tests, handling the ``cache_logger_on_first_use`` +
``capture_logs()`` interaction (research.md G-4).
"""

import io
import json
import logging
from uuid import uuid4

import jsonschema
import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider

import taskq.obs as obs_mod

_LOG_LINE_SCHEMA: dict[str, object] = {
    "type": "object",
    "required": ["kind"],
    "properties": {
        "kind": {"type": "string"},
    },
}


# ── Mandatory fields on every log line ────────────────────────────


def test_mandatory_fields_on_captured_log_line() -> None:
    """Mandatory fields on every log line."""
    obs_mod.setup_logging(log_format="json")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("test_event", kind="state_change")

    assert len(captured) == 1
    entry = captured[0]
    assert "event" in entry
    assert "log_level" in entry
    assert entry.get("kind") == "state_change"


def test_captured_log_serializes_to_valid_json() -> None:
    """Output serializes to valid JSON."""
    obs_mod.setup_logging(log_format="json")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("json_check")

    assert len(captured) == 1
    rendered = json.dumps(captured[0])
    parsed = json.loads(rendered)
    assert isinstance(parsed, dict)


def test_rendered_json_log_has_level_and_timestamp() -> None:
    """Rendered JSON log line has level, timestamp, and kind."""
    obs_mod.setup_logging(log_format="json")

    from taskq._json import structlog_serializer

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(serializer=structlog_serializer),
            ],
            foreign_pre_chain=[
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.stdlib.add_log_level,
                structlog.stdlib.ExtraAdder(),
            ],
        )
    )
    test_logger = logging.getLogger("_test_tu1_render")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    log = obs_mod.get_logger("_test_tu1_render")
    log.info("render_test", kind="state_change")

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert "level" in parsed
    assert "timestamp" in parsed
    assert parsed["kind"] == "state_change"


# ── Bound logger carries job context fields ──────────────────────


def test_bound_logger_carries_job_context_fields() -> None:
    """Bound logger carries job context fields."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="ingest_telemetry",
            queue="ingest",
            attempt=1,
            identity_key=None,
            trace_id="abc123",
        )
        bound_log.info("test_event")

    assert len(captured) == 1
    entry = captured[0]
    assert entry["job_id"] == str(job_id)
    assert entry["actor"] == "ingest_telemetry"
    assert entry["queue"] == "ingest"
    assert entry["attempt"] == 1
    assert entry["trace_id"] == "abc123"


# ── identity_key omitted when None ───────────────────────────────


def test_identity_key_omitted_when_none() -> None:
    """identity_key omitted when None."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        )
        bound_log.info("omit_test")

    assert len(captured) == 1
    assert "identity_key" not in captured[0]


# ── batch_id omitted when None; present when supplied ─────────────


def test_batch_id_omitted_when_none() -> None:
    """batch_id omitted when None."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
            batch_id=None,
        )
        bound_log.info("omit_batch")

    assert len(captured) == 1
    assert "batch_id" not in captured[0]


def test_batch_id_present_when_supplied() -> None:
    """batch_id present when supplied."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
            batch_id="batch-123",
        )
        bound_log.info("present_batch")

    assert len(captured) == 1
    assert captured[0]["batch_id"] == "batch-123"


# ── log_state_change emits correct kind and states ───────────────


def test_log_state_change_emits_correct_kind_and_states() -> None:
    """log_state_change emits correct kind and states."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="abc123",
        )
        obs_mod.log_state_change(bound_log, from_state="pending", to_state="running")

    assert len(captured) == 1
    entry = captured[0]
    assert entry["kind"] == "state_change"
    assert entry["from_state"] == "pending"
    assert entry["to_state"] == "running"
    assert entry["job_id"] == str(job_id)
    assert entry["log_level"] == "info"


# ── log_cancel_phase_change emits correct kind and phases ────────


def test_log_cancel_phase_change_emits_correct_kind_and_phases() -> None:
    """log_cancel_phase_change emits correct kind and phases."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=3,
            identity_key=None,
            trace_id="def456",
        )
        obs_mod.log_cancel_phase_change(bound_log, from_phase=1, to_phase=2)

    assert len(captured) == 1
    entry = captured[0]
    assert entry["kind"] == "cancel_phase_change"
    assert entry["from_phase"] == 1
    assert entry["to_phase"] == 2
    assert entry["job_id"] == str(job_id)
    assert entry["log_level"] == "info"


# ── OTel span context processor injects trace_id and span_id ─────


def test_otel_span_processor_injects_trace_id_and_span_id() -> None:
    """OTel span context processor injects trace_id and span_id."""
    obs_mod.setup_logging(log_format="json")
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    from taskq._json import structlog_serializer

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(serializer=structlog_serializer),
            ],
            foreign_pre_chain=[
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.stdlib.add_log_level,
                structlog.stdlib.ExtraAdder(),
            ],
        )
    )
    test_logger = logging.getLogger("_test_tu7_otel")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    with tracer.start_as_current_span("test.span") as span:
        log = obs_mod.get_logger("_test_tu7_otel")
        log.info("otel_test")
        ctx = span.get_span_context()

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert parsed["trace_id"] == format(ctx.trace_id, "032x")
    assert parsed["span_id"] == format(ctx.span_id, "016x")


# ── trace_id="" when no active span; span_id absent ──────────────


def test_trace_id_empty_when_no_active_span() -> None:
    """trace_id="" when no active span."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        )
        bound_log.info("empty_trace")

    assert len(captured) == 1
    assert captured[0]["trace_id"] == ""
    assert "span_id" not in captured[0]


# ── setup_logging idempotency ─────────────────────────────────────


def test_setup_logging_idempotent() -> None:
    """setup_logging idempotency."""
    obs_mod.setup_logging()
    obs_mod.setup_logging()

    pf_handlers: list[logging.StreamHandler[str]] = [  # type: ignore[reportInvalidTypeArguments] # Why: pyright cannot narrow StreamHandler generic from isinstance check on handlers list; the handlers are always StreamHandlers writing to stderr/stdout.
        h
        for h in logging.root.handlers
        if isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
    ]
    assert len(pf_handlers) == 1

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test_idempotency")
        log.info("idempotent_check")

    assert len(captured) == 1


# ── ConsoleRenderer when log_format="console" ────────────────────


def test_console_renderer_output_not_valid_json() -> None:
    """ConsoleRenderer when log_format="console"."""
    obs_mod.setup_logging(log_format="console")

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)

    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(),
            ],
            foreign_pre_chain=[
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.stdlib.add_log_level,
                structlog.stdlib.ExtraAdder(),
            ],
        )
    )
    test_logger = logging.getLogger("_test_tu10_console")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    log = obs_mod.get_logger("_test_tu10_console")
    log.info("console_test", kind="state_change")

    output = buf.getvalue().strip()
    assert output
    with pytest.raises(json.JSONDecodeError):
        json.loads(output)


# ── redact_payload does not include raw payload ──────────────────


def test_redact_payload_returns_16_char_hex() -> None:
    """redact_payload does not include raw payload."""
    result = obs_mod.redact_payload({"ssn": "123-45-6789"})
    assert len(result) == 16
    assert all(c in "0123456789abcdef" for c in result)
    assert "123" not in result


# ── redact_payload is deterministic ──────────────────────────────


def test_redact_payload_deterministic() -> None:
    """redact_payload is deterministic."""
    payload = {"key": "value", "num": 42}
    assert obs_mod.redact_payload(payload) == obs_mod.redact_payload(payload)


# ── worker_id field appears on worker-scope logs ─────────────────


def test_worker_id_appears_on_worker_scope_logs() -> None:
    """worker_id field appears on worker-scope logs."""
    obs_mod.setup_logging()

    try:
        structlog.contextvars.bind_contextvars(worker_id="wkr-test-uuid")

        from taskq._json import structlog_serializer

        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processors=[
                    structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                    structlog.processors.JSONRenderer(serializer=structlog_serializer),
                ],
                foreign_pre_chain=[
                    structlog.processors.TimeStamper(fmt="iso", utc=True),
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.ExtraAdder(),
                ],
            )
        )
        test_logger = logging.getLogger("_test_tu13_worker")
        test_logger.handlers.clear()
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)

        log = obs_mod.get_logger("_test_tu13_worker")
        log.info("heartbeat_tick")

        output = buf.getvalue().strip()
        parsed = json.loads(output)
        assert parsed["worker_id"] == "wkr-test-uuid"
    finally:
        structlog.contextvars.clear_contextvars()


# ── Mandatory field schema validation ───────────────────────────


def test_schema_validation_passes_for_complete_log_line() -> None:
    """Mandatory field schema validation passes for complete line."""
    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="abc123",
        )
        obs_mod.log_state_change(bound_log, from_state="pending", to_state="running")

    assert len(captured) == 1
    jsonschema.validate(instance=captured[0], schema=_LOG_LINE_SCHEMA)


def test_schema_validation_fails_when_kind_absent() -> None:
    """Schema validation fails when kind is absent."""
    log_dict_without_kind: dict[str, object] = {
        "event": "something",
        "log_level": "info",
    }
    with pytest.raises(jsonschema.ValidationError, match="kind"):
        jsonschema.validate(instance=log_dict_without_kind, schema=_LOG_LINE_SCHEMA)


# ── log_state_change does not raise on unknown state values ───────


def test_log_state_change_unknown_states_do_not_raise() -> None:
    """log_state_change does not raise on unknown state values."""
    obs_mod.setup_logging()

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        obs_mod.log_state_change(log, from_state="INVALID", to_state="ALSO_INVALID")

    assert len(captured) == 1
    assert captured[0]["kind"] == "state_change"
    assert captured[0]["from_state"] == "INVALID"
    assert captured[0]["to_state"] == "ALSO_INVALID"


# ── Missing mandatory field detected by schema validator ──────────


def test_missing_mandatory_field_detected_by_schema_validator() -> None:
    """Missing mandatory field detected by schema validator."""
    log_dict: dict[str, object] = {"event": "test", "log_level": "info"}
    with pytest.raises(jsonschema.ValidationError, match="kind") as exc_info:
        jsonschema.validate(instance=log_dict, schema=_LOG_LINE_SCHEMA)
    assert "kind" in str(exc_info.value.message)
