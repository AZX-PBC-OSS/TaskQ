"""Exception rendering in production JSON logs must be diagnostic AND redacted.

``BoundLogger.exception`` puts ``exc_info=True`` into the event dict; TaskQ's
processor chain had no exception renderer, so the JSON channel shipped a
useless ``"exc_info": true`` bool — no class, message, or traceback — while the
dev console rendered tracebacks via ``ConsoleRenderer``, hiding the gap in
development. Worse, foreign stdlib records carrying a real ``exc_info`` tuple
hit the orjson fallback ``TypeError`` and the whole log line was DROPPED.

These tests drive the real configured pipeline (``setup_logging`` root
handler, stream swapped for capture), not a re-declared formatter chain, so a
wrong chain in ``setup_logging`` itself cannot pass them.
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
from collections.abc import Generator

import asyncpg
import pytest
import structlog

import taskq.obs as obs_mod
import taskq.obs._structlog as structlog_mod

from .test_obs_exception_redaction import _unique_violation


@pytest.fixture(autouse=True)
def _reset_structlog_and_logging() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction] # Why: autouse fixture — consumed by pytest, not called directly.
    """Reset structlog and logging state so each test starts clean.

    Same contract as the fixture in ``test_obs_logging.py``: structlog
    defaults, the ``_logging_configured`` flag, and any ProcessorFormatter
    handlers installed by ``setup_logging()``.
    """
    structlog.reset_defaults()
    structlog_mod._logging_configured = False
    _remove_processor_formatter_handlers()
    logging.root.setLevel(logging.WARNING)
    yield
    structlog.reset_defaults()
    structlog_mod._logging_configured = False
    _remove_processor_formatter_handlers()
    logging.root.setLevel(logging.WARNING)


def _remove_processor_formatter_handlers() -> None:
    for handler in list(logging.root.handlers):
        if isinstance(handler, logging.StreamHandler) and isinstance(
            handler.formatter, structlog.stdlib.ProcessorFormatter
        ):
            logging.root.removeHandler(handler)  # pyright: ignore[reportUnknownArgumentType] # Why: pyright cannot narrow StreamHandler generic from isinstance check; the handler is always a valid Handler.


@contextlib.contextmanager
def _capture_root_json_stream() -> Generator[io.StringIO, None, None]:
    """Capture output of the ProcessorFormatter root handler ``setup_logging`` installed.

    Fails the test if the handler is missing. Swapping the stream (rather
    than attaching a second handler) means the capture exercises the exact
    configured formatter chain, not a parallel one.
    """
    handler = next(
        h
        for h in logging.root.handlers
        if isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
    )
    buf = io.StringIO()
    original_stream = handler.stream
    handler.stream = buf  # pyright: ignore[reportUnknownMemberType] # Why: StreamHandler.stream is typed Any; StringIO is a valid runtime assignment.
    try:
        yield buf
    finally:
        handler.stream = original_stream  # pyright: ignore[reportUnknownMemberType] # Why: restore the real stderr stream even on assertion failure.


def _leaky_unique_violation() -> asyncpg.exceptions.UniqueViolationError:
    """DETAIL-carrying violation; built so no secret lands in a quoted source line.

    A traceback quotes the CURRENT source line of each frame, so the canary
    must be assembled in an earlier statement than the raise (same discipline
    as ``test_stacktrace_is_redacted_including_chained_causes``).
    """
    secret = "tenant-88-" + "ssn-123456789"
    return _unique_violation(f"Key (identity_key)=({secret}) already exists.")


def test_json_log_exception_renders_class_and_traceback_scrubbed() -> None:
    """``log.exception`` must render exception class + traceback in the JSON
    line, scrubbed of DETAIL row values, without a leftover ``exc_info`` bool,
    and without reshaping the level/timestamp/event contract."""
    obs_mod.setup_logging(log_format="json")
    exc = _leaky_unique_violation()
    # Precondition: the raw text leaks, or this test proves nothing.
    assert "ssn-123456789" in str(exc)

    with _capture_root_json_stream() as buf:
        log = obs_mod.get_logger("_test_exc_render")
        try:
            raise exc
        except asyncpg.exceptions.UniqueViolationError:
            log.exception("dispatch-batch-error")

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    # Existing JSON shape stays intact — extend, don't reshape.
    assert parsed["event"] == "dispatch-batch-error"
    assert parsed["level"] == "error"
    assert "timestamp" in parsed
    # Diagnostics restored: the class and traceback render.
    assert "UniqueViolationError" in output
    assert "Traceback (most recent call last)" in output
    # Redaction contract holds on the log channel too.
    assert "ssn-123456789" not in output
    assert "tenant-88" not in output
    # No leftover boolean where the exception should have been.
    assert "exc_info" not in parsed


def test_foreign_stdlib_exc_info_log_line_renders() -> None:
    """A foreign stdlib record with a real ``exc_info`` tuple must render as a
    JSON line (it was previously DROPPED by the orjson fallback TypeError),
    with the exception scrubbed."""
    obs_mod.setup_logging(log_format="json")
    exc = _leaky_unique_violation()
    assert "ssn-123456789" in str(exc)

    with _capture_root_json_stream() as buf:
        foreign_logger = logging.getLogger("some_foreign_lib")
        try:
            raise exc
        except asyncpg.exceptions.UniqueViolationError:
            foreign_logger.error("db write failed", exc_info=True)

    output = buf.getvalue().strip()
    assert output, "foreign stdlib record with exc_info was dropped entirely"
    parsed = json.loads(output)
    assert parsed["event"] == "db write failed"
    assert "UniqueViolationError" in output
    assert "ssn-123456789" not in output


def test_known_exception_string_fields_are_scrubbed_in_json_logs() -> None:
    """``error``/``error_message``/``error_traceback`` string fields must be
    scrubbed on the rendered JSON line — the log channel ships to the same
    telemetry backends as spans, so the redaction doctrine applies to it too
    (these are the exception-bearing field names actually used in src/).

    Classification-style values pass through the scrubbers unchanged, so the
    change only affects lines that genuinely carry exception text.
    """
    obs_mod.setup_logging(log_format="json")
    canary = "tenant-77-ssn-987654321"
    message = (
        'duplicate key value violates unique constraint "jobs_identity_key_key"\n'
        f"DETAIL:  Key (identity_key)=({canary}) already exists."
    )
    raw_traceback = (
        "Traceback (most recent call last):\n"
        '  File "job.py", line 1, in run\n'
        f"asyncpg.exceptions.UniqueViolationError: {message}"
    )

    with _capture_root_json_stream() as buf:
        log = obs_mod.get_logger("_test_field_scrub")
        log.error(
            "job-failed",
            error=message,
            error_message=message,
            error_traceback=raw_traceback,
        )

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert canary not in output
    # Message-style fields keep the diagnostic template, drop the DETAIL line.
    assert (
        parsed["error"] == 'duplicate key value violates unique constraint "jobs_identity_key_key"'
    )
    assert parsed["error_message"] == parsed["error"]
    # Traceback-style fields keep the full traceback structure, scrubbed.
    assert parsed["error_traceback"].startswith("Traceback (most recent call last):")
    assert "UniqueViolationError" in parsed["error_traceback"]
    assert "DETAIL" not in parsed["error_traceback"]


def test_terminal_write_failure_fields_are_scrubbed_in_json_logs() -> None:
    """``job_error_message``/``job_error_traceback``/``infra_error_message``/
    ``infra_error_traceback`` must be scrubbed on the rendered JSON line.

    ``_log_terminal_write_failed`` (worker/_handlers.py) logs exactly these
    four names: ``str(job_exc)`` is the ACTOR's exception — the payload-leak
    vector the sanitizer exists for — and ``_format_exc`` applies no
    scrubbing, so a name missing from the obs scrub sets ships the raw text
    to every telemetry backend the JSON channel feeds.
    """
    obs_mod.setup_logging(log_format="json")
    canary = "tenant-66-ssn-555000111"
    message = (
        'duplicate key value violates unique constraint "jobs_identity_key_key"\n'
        f"DETAIL:  Key (identity_key)=({canary}) already exists."
    )
    raw_traceback = (
        "Traceback (most recent call last):\n"
        '  File "job.py", line 1, in run\n'
        f"asyncpg.exceptions.UniqueViolationError: {message}"
    )

    with _capture_root_json_stream() as buf:
        log = obs_mod.get_logger("_test_terminal_write_fields")
        log.error(
            "terminal-write-failed",
            kind="terminal-write-failed",
            job_error_message=message,
            job_error_traceback=raw_traceback,
            infra_error_message=message,
            infra_error_traceback=raw_traceback,
        )

    output = buf.getvalue().strip()
    parsed = json.loads(output)
    assert canary not in output
    # Message-style fields keep the diagnostic template, drop the DETAIL line.
    assert parsed["job_error_message"] == (
        'duplicate key value violates unique constraint "jobs_identity_key_key"'
    )
    assert parsed["infra_error_message"] == parsed["job_error_message"]
    # Traceback-style fields keep the full traceback structure, scrubbed.
    assert parsed["job_error_traceback"].startswith("Traceback (most recent call last):")
    assert "DETAIL" not in parsed["job_error_traceback"]
    assert "DETAIL" not in parsed["infra_error_traceback"]


def test_exception_object_field_renders_scrubbed_not_dropped() -> None:
    """An exception object passed as a field value must render as the scrubbed
    safe message — previously the raw object hit the orjson fallback TypeError
    and the whole log line was dropped."""
    obs_mod.setup_logging(log_format="json")
    canary = "tenant-77-ssn-987654321"
    exc = _unique_violation(f"Key (identity_key)=({canary}) already exists.")
    assert canary in str(exc)

    with _capture_root_json_stream() as buf:
        log = obs_mod.get_logger("_test_field_scrub")
        log.error("job-failed", exc=exc)

    output = buf.getvalue().strip()
    assert output, "log line carrying an exception object field was dropped entirely"
    parsed = json.loads(output)
    assert canary not in output
    assert (
        parsed["exc"] == 'duplicate key value violates unique constraint "jobs_idempotency_key_key"'
    )
