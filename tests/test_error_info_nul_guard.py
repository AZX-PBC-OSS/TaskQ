"""ErrorInfo text must never reach Postgres carrying a NUL codepoint.

The strand cycle this guards against: ``worker._handlers`` builds an
:class:`~taskq.backend._protocol.ErrorInfo` from an actor exception's
``str()`` and formatted traceback, and the terminal-write UPDATE binds
those as ``text``. A NUL in the value surfaces as asyncpg
``CharacterNotInRepertoireError`` (SQLSTATE 22021) — a ``PostgresError``
subclass — which the terminal-write infra error classification reads as
*transient infrastructure failure*. The job is therefore never marked
failed: it stays ``running`` until the lease sweep reclaims it, re-runs,
produces the same NUL-bearing text, and loops forever, re-executing the
actor's already-committed side effects each time.

Two halves break the cycle, and both are pinned here:

* the construction guard — ``ErrorInfo`` itself rejects a NUL, so a
  caller-supplied (unsanitized) value fails fast with a clean
  ``ValueError`` instead of an opaque infra loop;
* the handler sanitization — text DERIVED from an uncontrolled exception
  is sanitized to the visible ``\\x00`` escape before construction,
  because rejecting it would strand the very job the text describes: the
  terminal write must land with the defect visible.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from opentelemetry import trace

from taskq._ids import new_uuid
from taskq._json import sanitize_nul_str
from taskq.backend._protocol import ErrorInfo, JobRow
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.worker._handlers import _handle_generic_exception, _handle_timeout

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


def _seed_running_job(backend: InMemoryBackend, job: JobRow) -> None:
    """Seed *job* as running and locked by this test's worker.

    The in-memory terminal write requires the row to be ``running`` and
    owned by the writer, mirroring the PG ownership guard.
    """
    backend._jobs[job.id] = replace(job, status="running", locked_by_worker=_WORKER_ID)


# ── Construction guard ───────────────────────────────────────────────────


def test_error_info_rejects_nul_in_error_message() -> None:
    """A NUL in error_message raises ValueError at construction, not 22021 at write time."""
    with pytest.raises(ValueError, match="NUL"):
        ErrorInfo(error_class="ValueError", error_message="m\x00sg", error_traceback=None)


def test_error_info_rejects_nul_in_error_traceback() -> None:
    """A NUL in error_traceback raises the same construction-time ValueError."""
    with pytest.raises(ValueError, match="NUL"):
        ErrorInfo(error_class="ValueError", error_message="msg", error_traceback="tb\x00end")


def test_error_info_rejects_nul_in_error_class() -> None:
    """Every text field is guarded — error_class is not exempt."""
    with pytest.raises(ValueError, match="NUL"):
        ErrorInfo(error_class="Bad\x00Class", error_message="msg", error_traceback=None)


def test_error_info_accepts_clean_values() -> None:
    """Clean text constructs unchanged — the guard adds no false positives."""
    info = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback="tb")
    assert info.error_class == "ValueError"
    assert info.error_message == "boom"
    assert info.error_traceback == "tb"

    none_tb = ErrorInfo(error_class="ValueError", error_message="boom", error_traceback=None)
    assert none_tb.error_traceback is None


# ── Sanitizer ────────────────────────────────────────────────────────────


def test_sanitize_nul_str_replaces_nul_with_visible_escape() -> None:
    """sanitize_nul_str round-trips: the stored text keeps the defect visible."""
    sanitized = sanitize_nul_str("a\x00b")
    assert sanitized == "a\\x00b"
    assert "\x00" not in sanitized


# ── Handler-level sanitization ───────────────────────────────────────────
#
# The strand cycle is only broken if the terminal write actually LANDS with
# the escape visible: the handlers must sanitize derived text BEFORE the
# guarded construction, so the write succeeds instead of raising. Driving
# the real handlers against InMemoryBackend pins the stored row — the exact
# input the PG terminal write would bind.


async def test_handle_generic_exception_sanitizes_derived_nul_text() -> None:
    """_handle_generic_exception returns normally for a NUL-bearing exception,
    and the terminal write stores the visible escape — no raw NUL."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    job = make_job_row(attempt=3, max_attempts=3)
    _seed_running_job(backend, job)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

    outcome = await _handle_generic_exception(
        backend,
        job,
        _WORKER_ID,
        RuntimeError("boom\x00bang"),
        cfg,
        timedelta(hours=24),
        trace.get_current_span(),  # non-recording outside a span context
        structlog.get_logger("test"),
    )

    assert outcome == "failed", "the terminal write must land, not strand the job"
    stored = backend._jobs[job.id]
    assert stored.error_class == "RuntimeError"
    assert stored.error_message == "boom\\x00bang"
    assert "\x00" not in stored.error_message
    assert stored.error_traceback is not None
    assert "\x00" not in stored.error_traceback
    assert "\\x00" in stored.error_traceback, "the traceback keeps the defect visible"


async def test_handle_timeout_sanitizes_derived_nul_text() -> None:
    """_handle_timeout sanitizes its derived message and traceback the same
    way, and keeps its empty-message fallback."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    job = make_job_row(attempt=3, max_attempts=3)
    _seed_running_job(backend, job)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

    outcome = await _handle_timeout(
        backend,
        job,
        _WORKER_ID,
        TimeoutError("wait\x00failed"),
        cfg,
        timedelta(hours=24),
        trace.get_current_span(),  # non-recording outside a span context
        structlog.get_logger("test"),
    )

    assert outcome == "failed", "the terminal write must land, not strand the job"
    stored = backend._jobs[job.id]
    assert stored.error_message == "wait\\x00failed"
    assert "\x00" not in stored.error_message
    assert stored.error_traceback is not None
    assert "\x00" not in stored.error_traceback


async def test_handle_timeout_keeps_start_to_close_fallback_sanitized() -> None:
    """An empty TimeoutError message still falls back to 'start_to_close'."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    job = make_job_row(attempt=3, max_attempts=3)
    _seed_running_job(backend, job)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

    outcome = await _handle_timeout(
        backend,
        job,
        _WORKER_ID,
        TimeoutError(""),
        cfg,
        timedelta(hours=24),
        trace.get_current_span(),  # non-recording outside a span context
        structlog.get_logger("test"),
    )

    assert outcome == "failed"
    stored = backend._jobs[job.id]
    assert stored.error_message == "start_to_close"
    assert "\x00" not in stored.error_message
