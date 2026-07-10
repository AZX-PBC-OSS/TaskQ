"""Behavioral assertions for TaskQ tests — query observable state, not implementation details."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from taskq._json import loads
from taskq.backend._protocol import JobId, JobRow

if TYPE_CHECKING:
    import asyncpg
    from opentelemetry.sdk.trace import ReadableSpan

__all__ = [
    "assert_attempt",
    "assert_has_event",
    "assert_has_otel_event",
    "assert_has_span",
    "assert_job_status",
    "assert_job_terminal",
    "assert_transition_sequence",
    "parse_detail",
    "pg_now",
    "plain_cli_output",
    "wait_for",
    "wait_for_job_status",
    "wait_for_leader",
]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def plain_cli_output(output: str) -> str:
    """Strip ANSI escapes and collapse whitespace for CLI-output assertions.

    Rich/Typer help rendering varies with the detected environment — color
    codes get injected inside words, box-drawing characters wrap lines at
    terminal width — so raw substring assertions on ``result.output`` are
    environment-dependent. Asserting against the plain, whitespace-collapsed
    text is stable in any terminal, CI runner, or width.
    """
    return " ".join(_ANSI_RE.sub("", output).split())


@runtime_checkable
class _EventLike(Protocol):
    """Protocol for OTel span Event objects in test assertions."""

    attributes: dict[str, object] | None


@runtime_checkable
class _SpanExporter(Protocol):
    """Protocol for OTel InMemorySpanExporter test instances."""

    def span_named(self, name: str) -> ReadableSpan | None: ...

    spans: list[ReadableSpan]

    def events_on(self, span_name: str, event_name: str) -> list[_EventLike]: ...


@runtime_checkable
class _AssertBackend(Protocol):
    """Protocol for Backend instances in test assertions — minimal get-only surface."""

    async def get(self, job_id: JobId) -> JobRow | None: ...


@runtime_checkable
class _LeaderDeps(Protocol):
    """Protocol for deps with an is_leader event in test assertions."""

    is_leader: asyncio.Event


def _get(row: object, key: str) -> object:
    try:
        return getattr(row, key)
    except AttributeError:
        return row[key]  # type: ignore[index]  # Why: row is object (asyncpg.Record or dataclass); subscript fallback is intentional duck-typing for test helpers — pyright cannot prove __getitem__ exists on object.


def parse_detail(detail: object) -> dict[str, object]:
    """Normalize a detail value (dict, JSON string, or other) to a dict."""
    if isinstance(detail, dict):
        return detail  # type: ignore[return-value]  # Why: isinstance(detail, dict) guarantees a dict at runtime; the value type is object so pyright cannot narrow dict[unknown, unknown] to dict[str, object].
    if isinstance(detail, str):
        return dict(loads(detail))
    return {}


def assert_job_status(
    row: asyncpg.Record | None,
    status: str,
    *,
    error_class: str | None = None,
    attempt: int | None = None,
    finished: bool | None = None,
) -> asyncpg.Record:
    """Assert a job row has the expected status and optional fields.

    Returns the row (guaranteed non-None on success) so callers can
    chain attribute/subscript access without pyright narrowing issues.
    """
    assert row is not None, "Expected a row but got None"
    actual_status = _get(row, "status")
    if actual_status != status:
        raise AssertionError(f"Expected status {status!r}, got {actual_status!r}")
    if error_class is not None:
        actual_ec = _get(row, "error_class")
        if actual_ec != error_class:
            raise AssertionError(f"Expected error_class {error_class!r}, got {actual_ec!r}")
    if attempt is not None:
        actual_attempt = _get(row, "attempt")
        if actual_attempt != attempt:
            raise AssertionError(f"Expected attempt {attempt}, got {actual_attempt}")
    if finished is not None:
        finished_at = _get(row, "finished_at")
        if finished and finished_at is None:
            raise AssertionError("Expected finished_at to be set, but it is None")
        if not finished and finished_at is not None:
            raise AssertionError(f"Expected finished_at to be None, got {finished_at!r}")
    return row


def assert_attempt(
    attempts: Sequence[object],
    index: int,
    *,
    outcome: str | None = None,
    error_class: str | None = None,
    attempt_num: int | None = None,
) -> object:
    """Assert on the attempt row at *index*."""
    if index < 0 or index >= len(attempts):
        raise AssertionError(f"Attempt index {index} out of range (len={len(attempts)})")
    row = attempts[index]
    if outcome is not None:
        actual = _get(row, "outcome")
        if actual != outcome:
            raise AssertionError(f"attempts[{index}]: expected outcome {outcome!r}, got {actual!r}")
    if error_class is not None:
        actual = _get(row, "error_class")
        if actual != error_class:
            raise AssertionError(
                f"attempts[{index}]: expected error_class {error_class!r}, got {actual!r}"
            )
    if attempt_num is not None:
        actual = _get(row, "attempt")
        if actual != attempt_num:
            raise AssertionError(f"attempts[{index}]: expected attempt {attempt_num}, got {actual}")
    return row


def assert_job_terminal(
    row: asyncpg.Record | None,
    status: str,
    *,
    error_class: str | None = None,
) -> asyncpg.Record:
    """Assert a job is in a terminal status with finished_at set."""
    return assert_job_status(row, status, error_class=error_class, finished=True)


def assert_has_event(
    events: Sequence[asyncpg.Record],
    kind: str,
    *,
    from_state: str | None = None,
    to_state: str | None = None,
) -> asyncpg.Record:
    """Find at least one event matching kind and optional state filters."""
    for ev in events:
        if _get(ev, "kind") != kind:
            continue
        if from_state is not None or to_state is not None:
            detail = parse_detail(_get(ev, "detail"))
            if from_state is not None and detail.get("from_state") != from_state:
                continue
            if to_state is not None and detail.get("to_state") != to_state:
                continue
        return ev
    available = [(i, _get(e, "kind")) for i, e in enumerate(events)]
    msg = f"No event with kind={kind!r}"
    if from_state is not None or to_state is not None:
        msg += f" (from_state={from_state!r}, to_state={to_state!r})"
    msg += f"; available events: {available}"
    raise AssertionError(msg)


def assert_transition_sequence(
    events: Sequence[object],
    expected: Sequence[tuple[str | None, str | None]],
) -> None:
    """Assert the (from_state, to_state) sequence from state_change events matches expected."""
    transitions: list[tuple[object, object]] = []
    for ev in events:
        if _get(ev, "kind") != "state_change":
            continue
        detail = parse_detail(_get(ev, "detail"))
        transitions.append((detail.get("from_state"), detail.get("to_state")))
    if transitions != list(expected):
        raise AssertionError(f"Expected transition sequence {list(expected)}, got {transitions}")


def assert_has_span(
    exporter: _SpanExporter,
    name: str,
    *,
    kind: object = None,
    status: object = None,
) -> ReadableSpan:
    """Find a span by name on the exporter; assert kind/status if provided."""
    span = exporter.span_named(name)
    if span is None:
        names = [s.name for s in exporter.spans]
        raise AssertionError(f"No span named {name!r}; available: {names}")
    if kind is not None and span.kind != kind:
        raise AssertionError(f"Span {name!r}: expected kind={kind!r}, got {span.kind!r}")
    if status is not None and span.status.status_code != status:
        raise AssertionError(
            f"Span {name!r}: expected status_code={status!r}, got {span.status.status_code!r}"
        )
    return span


def assert_has_otel_event(
    exporter: _SpanExporter,
    span_name: str,
    event_name: str,
    *,
    from_state: str | None = None,
    to_state: str | None = None,
) -> object:
    """Find an OTel event by span and event name; assert state attributes if provided."""
    events = exporter.events_on(span_name, event_name)
    if not events:
        raise AssertionError(f"No OTel event {event_name!r} on span {span_name!r}")
    if from_state is not None or to_state is not None:
        for ev in events:
            attrs = ev.attributes
            if attrs is None:
                continue
            if from_state is not None and attrs.get("from_state") != from_state:
                continue
            if to_state is not None and attrs.get("to_state") != to_state:
                continue
            return ev
        raise AssertionError(
            f"No OTel event {event_name!r} on span {span_name!r} "
            f"with from_state={from_state!r}, to_state={to_state!r}"
        )
    return events[0]


async def wait_for(event: asyncio.Event, timeout: float = 2.0) -> None:  # noqa: ASYNC109
    """Wait for an asyncio.Event with test-failure semantics on timeout."""
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
    except TimeoutError:
        raise AssertionError(f"Event not set within {timeout}s") from None


async def wait_for_job_status(
    backend: _AssertBackend,
    job_id: JobId,
    status: str,
    *,
    timeout: float = 2.0,  # noqa: ASYNC109
    poll_interval: float = 0.05,
) -> JobRow:
    """Poll backend.get until the job reaches the expected status."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        row = await backend.get(job_id)
        if row is not None and _get(row, "status") == status:
            return row
        remaining = deadline - loop.time()
        if remaining <= 0:
            actual = _get(row, "status") if row is not None else None
            raise AssertionError(
                f"Job {job_id} did not reach status {status!r} within {timeout}s "
                f"(actual: {actual!r})"
            )
        await asyncio.sleep(min(poll_interval, remaining))


async def wait_for_leader(deps: _LeaderDeps, timeout: float = 5.0) -> None:  # noqa: ASYNC109
    """Wait for the leader event on WorkerDeps with test-failure semantics."""
    try:
        await asyncio.wait_for(deps.is_leader.wait(), timeout=timeout)
    except TimeoutError:
        raise AssertionError(f"Leader event not set within {timeout}s") from None


async def pg_now(conn: asyncpg.Connection) -> datetime:
    """Return PG's ``clock_timestamp()`` — the realtime clock the server uses.

    Use this instead of ``datetime.now(UTC)`` when a test needs to compute
    cutoffs/margins that are compared against rows written via SQL: the
    Python wall clock and PG's realtime clock can diverge enough under
    parallel load to make Python-computed margins flaky.
    """
    value: datetime = await conn.fetchval("SELECT clock_timestamp()")
    return value
