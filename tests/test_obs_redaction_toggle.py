"""Redaction must cost only the row VALUES, and must be switchable off to debug.

Two separate defects lived in one regex. ``_PG_DETAIL_RE`` deleted whole
``DETAIL``, ``HINT`` and ``CONTEXT`` lines, but only ``DETAIL`` carries
caller-supplied row values -- ``HINT`` is Postgres's suggested fix and
``CONTEXT`` is the PL/pgSQL call stack, both structural. Deleting them bought
no privacy and cost the operator the two most useful lines after the primary
message. And there was no way to turn any of it off, so an operator who
legitimately needed the row value at 3am could not get it at all.

These tests assert on what a real exporter and a real foreign log handler
actually received, following ``test_obs_span_exception_export.py`` and
``test_obs_log_exc_info_leak.py`` -- not on the scrubber's return value, which
would pass even if the wiring on either channel were half-applied.
"""

from __future__ import annotations

import logging
from collections.abc import Generator

import asyncpg
import pytest
import structlog
import structlog.testing

import taskq.obs._redact_exc as redact_mod
from taskq.obs import get_logger, safe_start_span, setup_logging
from taskq.settings import WorkerSettings
from taskq.testing.otel import setup_tracer
from taskq.worker._bootstrap import (
    _emit_startup_warnings,  # pyright: ignore[reportPrivateUsage]  # Why: the startup warnings are exercised directly, as tests/test_migrate_on_start_worker.py already does.
)

#: Shaped like the tenant identifiers TaskQ's idempotency/identity/fairness
#: keys actually carry. Lives in DETAIL, the only value-bearing line.
CANARY = "tenant-4417-SSN-078051120"

DIAGNOSTIC = "duplicate key value violates unique constraint"

#: The two lines the old regex destroyed for no privacy benefit. Postgres
#: writes them; neither quotes a row value.
HINT = 'Perhaps you meant to reference the column "jobs.queue".'
CONTEXT = "PL/pgSQL function taskq.enqueue(text) line 12 at SQL statement"

#: Asserted instead of HINT/CONTEXT themselves: both channels JSON-encode, so
#: the double quotes inside a real HINT arrive as ``\"`` and a literal-substring
#: check would fail on the escaping rather than on the redaction. These markers
#: are quote-free and unique to the line they prove survived.
HINT_MARKER = "Perhaps you meant to reference the column"
CONTEXT_MARKER = "PL/pgSQL function taskq.enqueue"


def _unique_violation() -> asyncpg.exceptions.UniqueViolationError:
    """A real asyncpg error whose ``str()`` renders DETAIL and HINT."""
    exc = asyncpg.exceptions.UniqueViolationError(f'{DIAGNOSTIC} "jobs_idempotency_key_key"')
    exc.detail = f"Key (idempotency_key)=({CANARY}) already exists."
    exc.hint = HINT
    return exc


def _context_bearing_error() -> RuntimeError:
    """A CONTEXT line in exception text.

    Its own case because asyncpg's ``__str__`` renders only DETAIL and HINT --
    CONTEXT reaches exception text via other drivers and via wrapped/chained
    messages, and the old regex deleted it wherever it appeared.
    """
    return RuntimeError(f'{DIAGNOSTIC} "jobs_pkey"\nCONTEXT:  {CONTEXT}')


@pytest.fixture(autouse=True)
def _redaction_enabled_guard() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture; referenced by the pytest runner via reflection.
    """Restore the module-level toggle, mirroring ``_otel_enabled_guard``."""
    original = redact_mod._redaction_enabled  # pyright: ignore[reportPrivateUsage]  # Why: test guard snapshots the private module flag, as taskq.testing.otel does for _otel_enabled.
    try:
        yield
    finally:
        redact_mod.set_exception_redaction_enabled(original)


class _ForeignHandler(logging.Handler):
    """Stands in for a vendor ``LoggingHandler`` attached to the root logger."""

    def __init__(self) -> None:
        super().__init__()
        self.rendered: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.rendered.append(logging.Formatter().format(record))


def _log_exception(exc: BaseException, capsys: pytest.CaptureFixture[str]) -> tuple[str, str]:
    """Emit *exc* through the real logging stack; return (foreign, own) text."""
    setup_logging(level="INFO", log_format="json")
    foreign = _ForeignHandler()
    logging.root.handlers.insert(0, foreign)
    try:
        log: structlog.stdlib.BoundLogger = get_logger("taskq.test")
        try:
            raise exc
        except (
            BaseException
        ):  # Why: the test raises and catches its own fixture exception to produce a live exc_info.
            log.exception("job-failed")
    finally:
        logging.root.removeHandler(foreign)
    return "\n".join(foreign.rendered), capsys.readouterr().err


def _export_exception(exc: BaseException, monkeypatch: pytest.MonkeyPatch) -> str:
    """Drive the bare-span shape and return the exported span as JSON."""
    _provider, exporter = setup_tracer(monkeypatch)
    with pytest.raises(BaseException, match=DIAGNOSTIC), safe_start_span("attempt.1"):
        raise exc
    spans = exporter.spans_named("attempt.1")
    assert len(spans) == 1
    return spans[0].to_json()


# ── default configuration: values out, structure in ──────────────────────


@pytest.mark.parametrize(
    ("factory", "survivor"),
    [(_unique_violation, HINT_MARKER), (_context_bearing_error, CONTEXT_MARKER)],
    ids=["hint", "context"],
)
def test_span_drops_detail_values_but_keeps_hint_and_context_by_default(
    factory: object,
    survivor: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regression this change exists to fix, on the span channel."""
    assert callable(factory)
    exc = factory()
    assert survivor in str(exc), "precondition: the line is really in the raw text"

    exported = _export_exception(exc, monkeypatch)

    assert CANARY not in exported, f"row value reached the exporter:\n{exported}"
    assert DIAGNOSTIC in exported
    assert survivor in exported, (
        f"redaction destroyed a structural line that carries no row values:\n{exported}"
    )


@pytest.mark.parametrize(
    ("factory", "survivor"),
    [(_unique_violation, HINT_MARKER), (_context_bearing_error, CONTEXT_MARKER)],
    ids=["hint", "context"],
)
def test_log_drops_detail_values_but_keeps_hint_and_context_by_default(
    factory: object,
    survivor: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Same proof on the log channel -- a half-applied change is worse than none."""
    assert callable(factory)
    foreign, own = _log_exception(factory(), capsys)

    assert CANARY not in foreign, f"row value reached a foreign handler:\n{foreign}"
    assert CANARY not in own, f"row value reached TaskQ's own log line:\n{own}"
    assert DIAGNOSTIC in own
    assert survivor in own, f"redaction destroyed a structural line:\n{own}"


# ── toggle on: raw text, both channels ───────────────────────────────────


def test_span_carries_raw_detail_when_redaction_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redact_mod.set_exception_redaction_enabled(False)
    exported = _export_exception(_unique_violation(), monkeypatch)
    assert CANARY in exported, f"the escape hatch did not reach the span path:\n{exported}"


def test_log_carries_raw_detail_when_redaction_is_disabled(
    capsys: pytest.CaptureFixture[str],
) -> None:
    redact_mod.set_exception_redaction_enabled(False)
    _foreign, own = _log_exception(_unique_violation(), capsys)
    assert CANARY in own, f"the escape hatch did not reach the log path:\n{own}"


def test_uri_credentials_are_masked_even_when_redaction_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The toggle buys row values, never a password. No debugging case wants one."""
    redact_mod.set_exception_redaction_enabled(False)
    exported = _export_exception(
        RuntimeError(f"{DIAGNOSTIC}: postgresql://taskq:hunter2@db.internal:5432/taskq"),
        monkeypatch,
    )
    assert "hunter2" not in exported, f"a DSN password reached the exporter:\n{exported}"
    assert "postgresql://taskq:***@" in exported


# ── the setting and its startup warning ──────────────────────────────────


def test_setting_defaults_to_redacted() -> None:
    """Safe behaviour must be what you get by doing nothing."""
    assert WorkerSettings.load_from_dict({}, validate=False).exception_redaction_enabled is True


def test_setting_is_readable_from_the_env_namespace() -> None:
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_EXCEPTION_REDACTION_ENABLED": "false"}, validate=False
    )
    assert settings.exception_redaction_enabled is False


def test_startup_warns_loudly_when_redaction_is_disabled() -> None:
    """An operator must not be able to leave this on and forget."""
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_EXCEPTION_REDACTION_ENABLED": "false"}, validate=False
    )
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(settings)

    entry = next(e for e in logs if e["event"] == "exception-redaction-disabled")
    assert entry["log_level"] == "warning"
    assert entry["setting"] == "TASKQ_EXCEPTION_REDACTION_ENABLED"
    detail = str(entry["detail"])
    assert "spans" in detail and "logs" in detail
    assert "row values" in detail
    assert "credential" in detail or "password" in detail, (
        "the warning must say the credential mask is NOT lifted"
    )


def test_startup_is_silent_in_the_default_configuration() -> None:
    """The control: no warning for the safe default, or it becomes noise."""
    for env in ({}, {"TASKQ_EXCEPTION_REDACTION_ENABLED": "true"}):
        settings = WorkerSettings.load_from_dict(env, validate=False)
        with structlog.testing.capture_logs() as logs:
            _emit_startup_warnings(settings)
        assert [e for e in logs if e["event"] == "exception-redaction-disabled"] == [], (
            f"no redaction warning may fire for {env or 'the defaults'}"
        )


def test_setting_description_says_credential_masking_is_never_lifted() -> None:
    """``taskq config`` and the docs render this description; it is the place an
    operator checks before flipping the switch."""
    _type, info = WorkerSettings.get_fields()["exception_redaction_enabled"]
    description = info.description or ""
    assert "TASKQ_EXCEPTION_REDACTION_ENABLED" in description
    assert "credential" in description.lower()
