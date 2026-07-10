"""Unit tests for structlog configuration: setup_logging, get_logger, _otel_span_processor."""

import io
import json
import logging
from collections.abc import Generator

import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider

import taskq.obs as obs_mod
import taskq.obs._structlog as structlog_mod


@pytest.fixture(autouse=True)
def _reset_structlog_and_logging() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction] # Why: autouse fixture — consumed by pytest, not called directly.
    """Reset structlog and logging state so each test starts clean.

    ``cache_logger_on_first_use=True`` means loggers cached at import time
    are not intercepted by ``capture_logs()``. Resetting before each test
    ensures fresh configuration. Also removes any ProcessorFormatter handlers
    added by ``setup_logging()`` and resets the ``_logging_configured`` flag.
    """
    structlog.reset_defaults()
    structlog_mod._logging_configured = False
    for handler in list(logging.root.handlers):
        if isinstance(handler, logging.StreamHandler) and isinstance(
            handler.formatter, structlog.stdlib.ProcessorFormatter
        ):
            logging.root.removeHandler(handler)
    logging.root.setLevel(logging.WARNING)
    yield
    structlog.reset_defaults()
    structlog_mod._logging_configured = False
    for handler in list(logging.root.handlers):
        if isinstance(handler, logging.StreamHandler) and isinstance(
            handler.formatter, structlog.stdlib.ProcessorFormatter
        ):
            logging.root.removeHandler(handler)
    logging.root.setLevel(logging.WARNING)


# ── setup_logging idempotency ────────────────────────────────────


def test_setup_logging_idempotent() -> None:
    obs_mod.setup_logging()
    obs_mod.setup_logging()

    pf_handlers = [
        h
        for h in logging.root.handlers
        if isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
    ]
    assert len(pf_handlers) == 1


# ── mandatory fields on every log line ────────────────────────────


def test_setup_logging_json_captures_event_and_level() -> None:
    obs_mod.setup_logging(log_format="json")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("test_event", kind="state_change")

    assert len(captured) == 1
    entry = captured[0]
    assert "event" in entry
    assert "log_level" in entry
    assert entry.get("kind") == "state_change"


def test_setup_logging_json_output_is_valid_json() -> None:
    obs_mod.setup_logging(log_format="json")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("json_check")

    assert len(captured) == 1
    rendered = json.dumps(captured[0])
    parsed = json.loads(rendered)
    assert isinstance(parsed, dict)


def test_rendered_json_log_line_has_level_and_timestamp() -> None:
    obs_mod.setup_logging(log_format="json")

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)

    from taskq._json import structlog_serializer

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

    test_logger = logging.getLogger("_test_render")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    log = obs_mod.get_logger("_test_render")
    log.info("render_test", kind="state_change")

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert "level" in parsed
    assert "timestamp" in parsed
    assert parsed["kind"] == "state_change"


# ── ConsoleRenderer when log_format="console" ────────────────────


def test_setup_logging_console_format() -> None:
    obs_mod.setup_logging(log_format="console")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("console_test")

    assert len(captured) == 1


# ── setup_logging is not called at import time ─────────────────────


def test_setup_logging_not_called_at_import() -> None:
    assert structlog_mod._logging_configured is False


# ── get_logger returns structlog.stdlib.BoundLogger ───────────────────────


def test_get_logger_after_setup_returns_bound_logger() -> None:
    obs_mod.setup_logging()
    log = obs_mod.get_logger("test.module")
    assert hasattr(log, "info")
    assert hasattr(log, "bind")
    assert hasattr(log, "debug")
    log.info("type_check")


# ── _otel_span_processor: injects trace_id and span_id ───────────────────


def test_otel_span_processor_injects_trace_and_span_id() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("test.span") as span:
        ctx = span.get_span_context()
        event_dict: structlog.types.EventDict = {"event": "test"}
        result = structlog_mod._otel_span_processor(None, "info", event_dict)
        assert result["trace_id"] == format(ctx.trace_id, "032x")
        assert result["span_id"] == format(ctx.span_id, "016x")


# ── _otel_span_processor: no injection when no active span ───────────────


def test_otel_span_processor_no_injection_without_span() -> None:
    event_dict: structlog.types.EventDict = {"event": "test"}
    result = structlog_mod._otel_span_processor(None, "info", event_dict)
    assert "trace_id" not in result
    assert "span_id" not in result


# ── OTel span context processor integrates with setup_logging ────


def test_otel_span_processor_in_rendered_json_output() -> None:
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

    test_logger = logging.getLogger("_test_otel_render")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    with tracer.start_as_current_span("test.span") as span:
        log = obs_mod.get_logger("_test_otel_render")
        log.info("otel_test")
        ctx = span.get_span_context()

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert parsed["trace_id"] == format(ctx.trace_id, "032x")
    assert parsed["span_id"] == format(ctx.span_id, "016x")


# ── processor chain exception handling ────────────────────


def test_otel_span_processor_exception_caught_by_wrapper() -> None:
    event_dict: structlog.types.EventDict = {"event": "test"}
    wrapped = structlog_mod._safe_processor_wrapper(structlog_mod._otel_span_processor)
    result = wrapped(None, "info", event_dict)
    assert isinstance(result, dict)


def test_custom_processor_exception_does_not_propagate() -> None:
    def _raising_processor(
        logger: object, method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        raise RuntimeError("processor failure")

    event_dict: structlog.types.EventDict = {"event": "test"}
    wrapped = structlog_mod._safe_processor_wrapper(_raising_processor)
    result = wrapped(None, "info", event_dict)
    assert isinstance(result, dict)
    assert result["event"] == "test"


def test_failing_builtin_processor_does_not_propagate_through_chain() -> None:
    def _raising_timestamper(
        logger: object, method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        raise OSError("clock failure")

    wrapped = structlog_mod._safe_processor_wrapper(_raising_timestamper)
    event_dict: structlog.types.EventDict = {"event": "clock_test", "key": "val"}
    result = wrapped(None, "info", event_dict)
    assert isinstance(result, dict)
    assert result["event"] == "clock_test"
    assert result["key"] == "val"


# ── stdlib bridge — ProcessorFormatter on root handler ─────────────


def test_setup_logging_adds_processor_formatter_handler() -> None:
    obs_mod.setup_logging(log_format="json")

    pf_handlers = [
        h
        for h in logging.root.handlers
        if isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
    ]
    assert len(pf_handlers) == 1


def test_setup_logging_sets_root_level() -> None:
    obs_mod.setup_logging(level="DEBUG")
    assert logging.root.level == logging.DEBUG


# ── JSON output is newline-delimited, self-contained ──────────────


def test_json_renderer_uses_orjson_for_uuid() -> None:
    obs_mod.setup_logging(log_format="json")

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        log.info("orjson_test", job_id="01234567-89ab-cdef-0123-456789abcdef")

    assert len(captured) == 1
    assert "job_id" in captured[0]


# ── timestamp field is ISO 8601 UTC ──────────────────────────────


def test_timestamp_in_rendered_json() -> None:
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

    test_logger = logging.getLogger("_test_ts")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    log = obs_mod.get_logger("_test_ts")
    log.info("ts_test")

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert "timestamp" in parsed


# ── _logging_configured flag prevents double setup ───────────────


def test_logging_configured_flag_set_after_setup() -> None:
    assert structlog_mod._logging_configured is False
    obs_mod.setup_logging()
    assert structlog_mod._logging_configured is True


# ── wrapper_class is structlog.stdlib.BoundLogger ──────────────────


def test_configured_wrapper_class_is_stdlib_bound_logger() -> None:
    obs_mod.setup_logging()

    cfg = structlog.get_config()
    assert cfg["wrapper_class"] is structlog.stdlib.BoundLogger


# ── cache_logger_on_first_use is True ────────────────────────────────────


def test_cache_logger_on_first_use_enabled() -> None:
    obs_mod.setup_logging()

    cfg = structlog.get_config()
    assert cfg["cache_logger_on_first_use"] is True


# ── logger_factory is stdlib LoggerFactory ────────────────────────────────


def test_logger_factory_is_stdlib() -> None:
    obs_mod.setup_logging()

    cfg = structlog.get_config()
    assert isinstance(cfg["logger_factory"], structlog.stdlib.LoggerFactory)


# ── WorkerSettings: log_format and log_level fields ──────────────────────


def test_worker_settings_log_format_default() -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict({"PG_DSN": "postgresql://localhost/test"})
    assert settings.log_format == "json"


def test_worker_settings_log_level_default() -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict({"PG_DSN": "postgresql://localhost/test"})
    assert settings.log_level == "INFO"


def test_worker_settings_log_format_console() -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {"PG_DSN": "postgresql://localhost/test", "TASKQ_LOG_FORMAT": "console"}
    )
    assert settings.log_format == "console"


def test_worker_settings_log_level_debug() -> None:
    from taskq.settings import WorkerSettings

    settings = WorkerSettings.load_from_dict(
        {"PG_DSN": "postgresql://localhost/test", "TASKQ_LOG_LEVEL": "DEBUG"}
    )
    assert settings.log_level == "DEBUG"


def test_worker_settings_log_format_rejects_invalid_value() -> None:
    from taskq.settings import WorkerSettings

    with pytest.raises(ValueError, match="log_format"):
        WorkerSettings.load_from_dict(
            {"PG_DSN": "postgresql://localhost/test", "TASKQ_LOG_FORMAT": "yaml"}
        )


# ── bind_job_context — mandatory fields on bound logger ────────


def test_bind_job_context_binds_mandatory_fields() -> None:
    from uuid import uuid4

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
    assert entry["job_id"] == job_id
    assert entry["actor"] == "ingest_telemetry"
    assert entry["queue"] == "ingest"
    assert entry["attempt"] == 1
    assert entry["trace_id"] == "abc123"


# ── identity_key omitted when None ─────────────────────


def test_bind_job_context_identity_key_omitted_when_none() -> None:
    from uuid import uuid4

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


def test_bind_job_context_identity_key_present_when_set() -> None:
    from uuid import uuid4

    obs_mod.setup_logging()
    job_id = uuid4()

    with structlog.testing.capture_logs() as captured:
        bound_log = obs_mod.bind_job_context(
            obs_mod.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key="ingest:site-42:2025-04-01",
            trace_id="",
        )
        bound_log.info("present_test")

    assert len(captured) == 1
    assert captured[0]["identity_key"] == "ingest:site-42:2025-04-01"


# ── batch_id omitted when None; present when supplied ───


def test_bind_job_context_batch_id_omitted_when_none() -> None:
    from uuid import uuid4

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


def test_bind_job_context_batch_id_present_when_set() -> None:
    from uuid import uuid4

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


# ── span_id omitted when None ────────────────────────────────


def test_bind_job_context_span_id_omitted_when_none() -> None:
    from uuid import uuid4

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
            span_id=None,
        )
        bound_log.info("omit_span")

    assert len(captured) == 1
    assert "span_id" not in captured[0]


# ── trace_id defaults to "" when no active span ────────


def test_bind_job_context_trace_id_empty_string_when_no_otel() -> None:
    from uuid import uuid4

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


# ── bind_job_context returns immutable new BoundLogger ──────────────


def test_bind_job_context_returns_new_logger_does_not_mutate_original() -> None:
    from uuid import uuid4

    obs_mod.setup_logging()
    job_id = uuid4()
    original_log = obs_mod.get_logger("test")

    bound_log = obs_mod.bind_job_context(
        original_log,
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        identity_key=None,
        trace_id="",
    )

    with structlog.testing.capture_logs() as captured:
        original_log.info("original_event")
        bound_log.info("bound_event")

    assert len(captured) == 2
    assert "job_id" not in captured[0]
    assert captured[1]["job_id"] == job_id


# ── worker_id via contextvars ────────────────────────


def test_worker_id_contextvar_appears_on_log_lines() -> None:
    from taskq._json import structlog_serializer

    obs_mod.setup_logging()

    try:
        structlog.contextvars.bind_contextvars(worker_id="wkr-test-uuid")

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
        test_logger = logging.getLogger("_test_wkr_sync")
        test_logger.handlers.clear()
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)

        log = obs_mod.get_logger("_test_wkr_sync")
        log.info("worker_scope_event")

        output = buf.getvalue().strip()
        parsed = json.loads(output)
        assert parsed["worker_id"] == "wkr-test-uuid"
    finally:
        structlog.contextvars.clear_contextvars()


async def test_worker_id_contextvar_propagates_to_coroutine() -> None:
    from taskq._json import structlog_serializer

    obs_mod.setup_logging()

    try:
        structlog.contextvars.bind_contextvars(worker_id="wkr-async-uuid")

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
        test_logger = logging.getLogger("_test_wkr_async")
        test_logger.handlers.clear()
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.DEBUG)

        async def _emit_log() -> None:
            log = obs_mod.get_logger("_test_wkr_async")
            log.info("coroutine_event")

        await _emit_log()

        output = buf.getvalue().strip()
        parsed = json.loads(output)
        assert parsed["worker_id"] == "wkr-async-uuid"
    finally:
        structlog.contextvars.clear_contextvars()


# ── bind_job_context overhead stays bounded ──────────────────────────


@pytest.mark.slow
def test_bind_job_context_performance_bounded() -> None:
    """bind_job_context is a hot-path helper — guard against gross regressions.

    The bound is deliberately loose (100µs vs the ~5µs typical) so the test
    catches an accidental O(n)/IO regression without flaking on loaded CI
    runners; the median is used so a single scheduler hiccup can't fail it.
    """
    import statistics
    import time
    from uuid import uuid4

    obs_mod.setup_logging()
    log = obs_mod.get_logger("test.perf")
    job_id = uuid4()

    durations: list[float] = []
    for _ in range(200):
        t0 = time.perf_counter_ns()
        obs_mod.bind_job_context(
            log,
            job_id=job_id,
            actor="perf_actor",
            queue="default",
            attempt=1,
            identity_key="idk-1",
            trace_id="a" * 32,
            batch_id="batch-1",
        )
        t1 = time.perf_counter_ns()
        durations.append((t1 - t0) / 1000)

    median_us = statistics.median(durations)
    assert median_us < 100, f"bind_job_context median {median_us:.2f}µs exceeds 100µs budget"


# ── log_state_change emits correct kind and states ──────


def test_log_state_change_emits_correct_fields() -> None:
    from uuid import uuid4

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
    assert entry["job_id"] == job_id
    assert entry["log_level"] == "info"


def test_log_state_change_with_extra_fields() -> None:
    from uuid import uuid4

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
        obs_mod.log_state_change(
            bound_log, from_state="running", to_state="succeeded", payload_hash="abcd1234"
        )

    assert len(captured) == 1
    assert captured[0]["payload_hash"] == "abcd1234"


# ── log_state_change does not raise on unknown state values ────


def test_log_state_change_unknown_states_do_not_raise() -> None:
    obs_mod.setup_logging()

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        obs_mod.log_state_change(log, from_state="INVALID", to_state="ALSO_INVALID")

    assert len(captured) == 1
    assert captured[0]["kind"] == "state_change"
    assert captured[0]["from_state"] == "INVALID"
    assert captured[0]["to_state"] == "ALSO_INVALID"


# ── log_cancel_phase_change emits correct kind and phases ──


def test_log_cancel_phase_change_emits_correct_fields() -> None:
    from uuid import uuid4

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
    assert entry["job_id"] == job_id
    assert entry["log_level"] == "info"


def test_log_cancel_phase_change_does_not_include_cancel_observed_at() -> None:
    from uuid import uuid4

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
        obs_mod.log_cancel_phase_change(bound_log, from_phase=0, to_phase=1)

    assert len(captured) == 1
    assert "cancel_observed_at" not in captured[0]


def test_log_cancel_phase_change_with_extra_fields() -> None:
    obs_mod.setup_logging()

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test")
        obs_mod.log_cancel_phase_change(log, from_phase=0, to_phase=1, worker_id="wkr-1")

    assert len(captured) == 1
    assert captured[0]["worker_id"] == "wkr-1"


# ── redact_payload does not include raw payload ────────


def test_redact_payload_returns_16_char_hex() -> None:
    result = obs_mod.redact_payload({"ssn": "123-45-6789"})
    assert len(result) == 16
    assert all(c in "0123456789abcdef" for c in result)


def test_redact_payload_does_not_contain_raw_data() -> None:
    result = obs_mod.redact_payload({"secret": "super_secret_value_42"})
    assert "super_secret_value_42" not in result


def test_redact_payload_deterministic() -> None:
    payload = {"key": "value", "num": 42}
    assert obs_mod.redact_payload(payload) == obs_mod.redact_payload(payload)


def test_redact_payload_different_inputs_different_outputs() -> None:
    a = obs_mod.redact_payload({"id": 1})
    b = obs_mod.redact_payload({"id": 2})
    assert a != b


# ── redact_payload overhead < 10µs ─────────────────────────────


def test_redact_payload_performance_under_10us() -> None:
    import time

    payload = {
        "field1": "value1",
        "field2": 42,
        "field3": True,
        "field4": None,
        "field5": [1, 2, 3],
    }

    durations: list[float] = []
    for _ in range(100):
        t0 = time.perf_counter_ns()
        obs_mod.redact_payload(payload)
        t1 = time.perf_counter_ns()
        durations.append((t1 - t0) / 1000)

    avg_us = sum(durations) / len(durations)
    assert avg_us < 10, f"redact_payload avg {avg_us:.2f}µs exceeds 10µs budget"
