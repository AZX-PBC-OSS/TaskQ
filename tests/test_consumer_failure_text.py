"""The failed-job text pipeline: one rendering feeds every sink.

A failed attempt reports its exception on three channels -- the
``attempt.N`` span (status description + ``exception`` event), the
``job_exception`` / ``job-failed`` log lines, and the durable ``ErrorInfo``.
Rendering a traceback and scrubbing it are the dominant CPU cost of a failed
job (a 27-frame traceback is ~0.8 ms to render and ~0.4-0.8 ms to scrub, all
GIL-held), so the contract pinned here is that each happens ONCE per failed
job and every channel shares the result -- while every channel still emits
exactly the bytes it always did.
"""

from __future__ import annotations

import json
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
from opentelemetry.trace import StatusCode
from pydantic import BaseModel

import taskq.obs as obs_mod
import taskq.obs._redact_exc as redact_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.testing.otel import ListSpanExporter, setup_tracer
from taskq.worker._consumer import consume_one_job

from .test_consumer_coverage import _FakeConnection, _TxBackend
from .test_obs_exception_redaction import _unique_violation
from .test_obs_exception_rendering import _capture_root_json_stream

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()

#: Shaped like the tenant identifiers TaskQ's idempotency keys carry; it lives
#: only in the DETAIL line, so its presence is the leak and its absence is the
#: scrub.
CANARY = "tenant-4417-SSN-078051120"


class _RenderCounter:
    """Count the expensive text operations while leaving their output intact.

    Wraps ``traceback.format_exception`` and ``_scrub_text`` in place, so the
    consumer path runs exactly as in production; the counters are the only
    observation. Traceback scrubs are told apart from message scrubs by the
    traceback header, which no exception message carries.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.renders = 0
        self.message_scrubs = 0
        self.traceback_scrubs = 0
        real_format: Callable[..., list[str]] = traceback.format_exception
        real_scrub = redact_mod._scrub_text  # pyright: ignore[reportPrivateUsage]  # Why: the cost contract is about this exact function; wrapping it is the only way to count it without changing what it emits.

        def counting_format(*args: Any, **kwargs: Any) -> list[str]:
            self.renders += 1
            return real_format(*args, **kwargs)

        def counting_scrub(text: str) -> str:
            if text.startswith("Traceback (most recent call last)"):
                self.traceback_scrubs += 1
            else:
                self.message_scrubs += 1
            return real_scrub(text)

        monkeypatch.setattr(traceback, "format_exception", counting_format)
        monkeypatch.setattr(redact_mod, "_scrub_text", counting_scrub)


def _leaky_actor_failure() -> asyncpg.exceptions.UniqueViolationError:
    """A DETAIL-carrying constraint violation built away from the raise line.

    A traceback quotes the SOURCE LINE of each frame, so the canary must be
    assembled in an earlier statement than the raise or the traceback would
    carry it legitimately and the scrub assertions would prove nothing.
    """
    return _unique_violation(f"Key (idempotency_key)=({CANARY}) already exists.")


def _terminal_job_and_config() -> tuple[Any, StubActorConfig]:
    """A job on its last attempt, so the failure is terminal and ``job-failed`` fires."""
    job = make_job_row(attempt=3, max_attempts=3, retry_kind="transient")
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    return job, cfg


def _json_lines(text: str) -> dict[str, dict[str, Any]]:
    """Parse the captured JSON log stream, keyed by event name."""
    lines = [json.loads(line) for line in text.splitlines() if line.strip()]
    return {line["event"]: line for line in lines}


def _stored_error(backend: FakeBackend) -> ErrorInfo:
    """The ``ErrorInfo`` the single terminal write carried."""
    (call,) = backend.mark_failed_or_retry_calls
    stored = call["error_info"]
    assert isinstance(stored, ErrorInfo)
    return stored


async def _run_failing_job(
    monkeypatch: pytest.MonkeyPatch,
    exc: BaseException,
    *,
    transactional: bool,
) -> tuple[_RenderCounter, ListSpanExporter, dict[str, dict[str, Any]], FakeBackend]:
    """Drive one terminal failure through the real consumer, OTel and structlog stacks."""

    async def actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise exc

    _, exporter = setup_tracer(monkeypatch)
    obs_mod.setup_logging(level="INFO", log_format="json")
    tracer = obs_mod.get_tracer()
    job, cfg = _terminal_job_and_config()
    clk: Clock = FakeClock(_NOW)

    backend: FakeBackend
    extra: dict[str, Any]
    if transactional:
        backend = _TxBackend()
        extra = {
            "enqueuer": SubJobEnqueuer(
                loop_scope_resolved={asyncpg.Connection: _FakeConnection()},
                worker_pool=None,
                backend=backend,
            ),
            "transaction_conn": _FakeConnection(),
        }
    else:
        backend = FakeBackend()
        extra = {}

    counter = _RenderCounter(monkeypatch)
    with _capture_root_json_stream() as buf, tracer.start_as_current_span("consumer"):
        outcome = await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=clk,
            **extra,
        )
    assert outcome == "failed"
    return counter, exporter, _json_lines(buf.getvalue()), backend


def _assert_channels_share_one_scrubbed_rendering(
    exporter: ListSpanExporter,
    lines: dict[str, dict[str, Any]],
    backend: FakeBackend,
    *,
    span_carries_error: bool,
) -> None:
    """Every channel emits the bytes it always did, and the scrubbed ones agree."""
    warning = lines["job_exception"]
    failed = lines["job-failed"]
    for line in (warning, failed):
        assert CANARY not in line["error_message"]
        assert CANARY not in line["error_traceback"]
        assert "Traceback (most recent call last)" in line["error_traceback"]
        assert "UniqueViolationError" in line["error_traceback"]
    assert warning["error_traceback"] == failed["error_traceback"]
    assert warning["error_message"] == failed["error_message"]

    # The durable row stays inside the trust boundary and keeps the raw text.
    stored = _stored_error(backend)
    assert CANARY in stored.error_message
    assert stored.error_traceback is not None
    assert CANARY in stored.error_traceback

    attempt = exporter.span_named("attempt.3")
    assert attempt is not None
    if not span_carries_error:
        # The transactional path handles the failure inside the span, so the
        # span itself never sees an exception.
        assert attempt.status.status_code is StatusCode.UNSET
        assert len(attempt.events) == 0
        return
    assert attempt.status.status_code is StatusCode.ERROR
    assert attempt.status.description == warning["error_message"]
    events = [event for event in attempt.events if event.name == "exception"]
    assert len(events) == 1
    attributes = events[0].attributes
    assert attributes is not None
    assert attributes["exception.type"] == "UniqueViolationError"
    assert attributes["exception.message"] == warning["error_message"]
    assert attributes["exception.stacktrace"] == warning["error_traceback"]


async def test_autonomous_failure_renders_and_scrubs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Autonomous path: the exception escapes the attempt span and then reaches
    the terminal handler; span, both log lines and the stored row share one
    traceback rendering, one message scrub and one traceback scrub."""
    counter, exporter, lines, backend = await _run_failing_job(
        monkeypatch, _leaky_actor_failure(), transactional=False
    )

    assert (counter.renders, counter.message_scrubs, counter.traceback_scrubs) == (1, 1, 1)
    _assert_channels_share_one_scrubbed_rendering(exporter, lines, backend, span_carries_error=True)


async def test_transactional_failure_renders_and_scrubs_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transactional path: the failure is caught inside the attempt span, so
    only the two log lines and the stored row are fed -- still from one
    rendering, one message scrub and one traceback scrub."""
    counter, exporter, lines, backend = await _run_failing_job(
        monkeypatch, _leaky_actor_failure(), transactional=True
    )

    assert (counter.renders, counter.message_scrubs, counter.traceback_scrubs) == (1, 1, 1)
    _assert_channels_share_one_scrubbed_rendering(
        exporter, lines, backend, span_carries_error=False
    )


async def test_log_channel_masks_a_credential_that_follows_a_nul(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """NUL escaping runs AFTER the scrub on the log channel.

    The credential mask's keyword boundary is a lookbehind for a non-word
    character. Escaping a NUL first rewrites it to the four characters
    ``\\x00``, whose trailing ``0`` satisfies the boundary in the wrong
    direction and lets the password through; scrubbing the raw text first
    masks it, and escaping afterwards cannot reassemble anything the scrub
    removed. The stored row keeps the raw, NUL-escaped text either way.
    """
    _, exporter, lines, backend = await _run_failing_job(
        monkeypatch,
        RuntimeError("connect failed: host=db\x00password=hunter2 user=app"),
        transactional=False,
    )

    for event in ("job_exception", "job-failed"):
        message = lines[event]["error_message"]
        assert "hunter2" not in message
        assert "\\x00password=***" in message
        assert "hunter2" not in lines[event]["error_traceback"]
    stored = _stored_error(backend)
    assert stored.error_message == "connect failed: host=db\\x00password=hunter2 user=app"
    attempt = exporter.span_named("attempt.3")
    assert attempt is not None
    assert attempt.status.description == "connect failed: host=db\x00password=*** user=app"
