"""Unit tests for taskq.testing.assertions — pure Python, no PG/OTel needed."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from taskq._json import dumps_str
from taskq.testing.assertions import (
    assert_attempt,
    assert_has_event,
    assert_has_otel_event,
    assert_has_span,
    assert_job_status,
    assert_job_terminal,
    assert_transition_sequence,
    parse_detail,
    pg_now,
    plain_cli_output,
    wait_for,
    wait_for_job_status,
    wait_for_leader,
)

# ── plain_cli_output ───────────────────────────────────────────────────────


def test_plain_cli_output_strips_ansi_and_collapses_whitespace() -> None:
    raw = "\x1b[31mHello\x1b[0m   world\n\n  again"
    assert plain_cli_output(raw) == "Hello world again"


def test_plain_cli_output_empty_string() -> None:
    assert plain_cli_output("") == ""


# ── parse_detail ────────────────────────────────────────────────────────────


def test_parse_detail_dict_passthrough() -> None:
    d = {"from_state": "pending", "to_state": "running"}
    assert parse_detail(d) is d


def test_parse_detail_json_string() -> None:
    s = dumps_str({"from_state": "pending", "to_state": "running"})
    assert parse_detail(s) == {"from_state": "pending", "to_state": "running"}


def test_parse_detail_other_fallback() -> None:
    assert parse_detail(None) == {}
    assert parse_detail(42) == {}


# ── assert_job_status ───────────────────────────────────────────────────────


def _row(**kwargs: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "status": "succeeded",
        "error_class": None,
        "attempt": 1,
        "finished_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_assert_job_status_none_row_raises() -> None:
    with pytest.raises(AssertionError, match="Expected a row but got None"):
        assert_job_status(None, "succeeded")  # type: ignore[arg-type]


def test_assert_job_status_mismatch_raises() -> None:
    row = _row(status="failed")
    with pytest.raises(AssertionError, match=r"succeeded.*failed"):
        assert_job_status(row, "succeeded")  # type: ignore[arg-type]


def test_assert_job_status_match_returns_row() -> None:
    row = _row(status="succeeded")
    assert assert_job_status(row, "succeeded") is row  # type: ignore[arg-type]


def test_assert_job_status_dict_fallback_via_get() -> None:
    """A plain dict exercises the except AttributeError -> row[key] branch in _get."""
    row = {
        "status": "succeeded",
        "error_class": None,
        "attempt": 1,
        "finished_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    assert assert_job_status(row, "succeeded") is row  # type: ignore[arg-type]


def test_assert_job_status_error_class_match() -> None:
    row = _row(status="failed", error_class="ValueError")
    assert_job_status(row, "failed", error_class="ValueError")  # type: ignore[arg-type]


def test_assert_job_status_error_class_mismatch() -> None:
    row = _row(status="failed", error_class="ValueError")
    with pytest.raises(AssertionError, match=r"TypeError.*ValueError"):
        assert_job_status(row, "failed", error_class="TypeError")  # type: ignore[arg-type]


def test_assert_job_status_attempt_match() -> None:
    row = _row(attempt=3)
    assert_job_status(row, "succeeded", attempt=3)  # type: ignore[arg-type]


def test_assert_job_status_attempt_mismatch() -> None:
    row = _row(attempt=3)
    with pytest.raises(AssertionError, match="Expected attempt 2, got 3"):
        assert_job_status(row, "succeeded", attempt=2)  # type: ignore[arg-type]


def test_assert_job_status_finished_true_pass() -> None:
    row = _row(finished_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert_job_status(row, "succeeded", finished=True)  # type: ignore[arg-type]


def test_assert_job_status_finished_true_fail() -> None:
    row = _row(finished_at=None)
    with pytest.raises(AssertionError, match="Expected finished_at to be set"):
        assert_job_status(row, "succeeded", finished=True)  # type: ignore[arg-type]


def test_assert_job_status_finished_false_pass() -> None:
    row = _row(finished_at=None)
    assert_job_status(row, "succeeded", finished=False)  # type: ignore[arg-type]


def test_assert_job_status_finished_false_fail() -> None:
    row = _row(finished_at=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(AssertionError, match="Expected finished_at to be None"):
        assert_job_status(row, "succeeded", finished=False)  # type: ignore[arg-type]


# ── assert_attempt ───────────────────────────────────────────────────────


def _attempt_row(**kwargs: object) -> SimpleNamespace:
    base: dict[str, object] = {"outcome": "succeeded", "error_class": None, "attempt": 1}
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_assert_attempt_index_negative_raises() -> None:
    with pytest.raises(AssertionError, match="out of range"):
        assert_attempt([_attempt_row()], -1)


def test_assert_attempt_index_too_large_raises() -> None:
    with pytest.raises(AssertionError, match="out of range"):
        assert_attempt([_attempt_row()], 1)


def test_assert_attempt_outcome_match() -> None:
    row = _attempt_row(outcome="failed")
    assert assert_attempt([row], 0, outcome="failed") is row


def test_assert_attempt_outcome_mismatch() -> None:
    row = _attempt_row(outcome="failed")
    with pytest.raises(AssertionError, match="expected outcome 'succeeded', got 'failed'"):
        assert_attempt([row], 0, outcome="succeeded")


def test_assert_attempt_error_class_match() -> None:
    row = _attempt_row(error_class="ValueError")
    assert_attempt([row], 0, error_class="ValueError")


def test_assert_attempt_error_class_mismatch() -> None:
    row = _attempt_row(error_class="ValueError")
    with pytest.raises(AssertionError, match="expected error_class 'TypeError'"):
        assert_attempt([row], 0, error_class="TypeError")


def test_assert_attempt_attempt_num_match() -> None:
    row = _attempt_row(attempt=2)
    assert_attempt([row], 0, attempt_num=2)


def test_assert_attempt_attempt_num_mismatch() -> None:
    row = _attempt_row(attempt=2)
    with pytest.raises(AssertionError, match="expected attempt 3, got 2"):
        assert_attempt([row], 0, attempt_num=3)


# ── assert_job_terminal ───────────────────────────────────────────────────


def test_assert_job_terminal_pass() -> None:
    row = _row(status="succeeded", finished_at=datetime(2026, 1, 1, tzinfo=UTC))
    assert assert_job_terminal(row, "succeeded") is row  # type: ignore[arg-type]


def test_assert_job_terminal_fail_not_finished() -> None:
    row = _row(status="succeeded", finished_at=None)
    with pytest.raises(AssertionError, match="Expected finished_at to be set"):
        assert_job_terminal(row, "succeeded")  # type: ignore[arg-type]


# ── assert_has_event ─────────────────────────────────────────────────────


def _event(kind: str, detail: object) -> SimpleNamespace:
    return SimpleNamespace(kind=kind, detail=detail)


def test_assert_has_event_found_by_kind_only() -> None:
    events = [_event("cancel_request", {})]
    assert assert_has_event(events, "cancel_request") is events[0]  # type: ignore[arg-type]


def test_assert_has_event_found_with_state_filters_dict_detail() -> None:
    events = [_event("state_change", {"from_state": "pending", "to_state": "running"})]
    ev = assert_has_event(events, "state_change", from_state="pending", to_state="running")  # type: ignore[arg-type]
    assert ev is events[0]


def test_assert_has_event_found_with_state_filters_json_string_detail() -> None:
    detail_json = dumps_str({"from_state": "pending", "to_state": "running"})
    events = [_event("state_change", detail_json)]
    ev = assert_has_event(events, "state_change", from_state="pending", to_state="running")  # type: ignore[arg-type]
    assert ev is events[0]


def test_assert_has_event_skips_from_state_mismatch_then_finds_match() -> None:
    events = [
        _event("state_change", {"from_state": "other", "to_state": "running"}),
        _event("state_change", {"from_state": "pending", "to_state": "running"}),
    ]
    ev = assert_has_event(events, "state_change", from_state="pending")  # type: ignore[arg-type]
    assert ev is events[1]


def test_assert_has_event_skips_to_state_mismatch_then_finds_match() -> None:
    events = [
        _event("state_change", {"from_state": "pending", "to_state": "other"}),
        _event("state_change", {"from_state": "pending", "to_state": "running"}),
    ]
    ev = assert_has_event(events, "state_change", to_state="running")  # type: ignore[arg-type]
    assert ev is events[1]


def test_assert_has_event_not_found_raises_with_available_list() -> None:
    events = [_event("cancel_request", {}), _event("state_change", {"from_state": "a"})]
    with pytest.raises(AssertionError, match="available events"):
        assert_has_event(events, "missing_kind")  # type: ignore[arg-type]


def test_assert_has_event_not_found_with_state_filters_includes_filters_in_message() -> None:
    events = [_event("state_change", {"from_state": "a", "to_state": "b"})]
    with pytest.raises(AssertionError, match=r"from_state='pending', to_state='running'"):
        assert_has_event(events, "state_change", from_state="pending", to_state="running")  # type: ignore[arg-type]


# ── assert_transition_sequence ─────────────────────────────────────────────


def test_assert_transition_sequence_matching_passes() -> None:
    events = [
        _event("cancel_request", {}),
        _event("state_change", {"from_state": None, "to_state": "pending"}),
        _event("state_change", {"from_state": "pending", "to_state": "running"}),
    ]
    assert_transition_sequence(
        events,  # type: ignore[arg-type]
        [(None, "pending"), ("pending", "running")],
    )


def test_assert_transition_sequence_mismatch_raises() -> None:
    events = [_event("state_change", {"from_state": None, "to_state": "pending"})]
    with pytest.raises(AssertionError, match="Expected transition sequence"):
        assert_transition_sequence(events, [(None, "running")])  # type: ignore[arg-type]


# ── assert_has_span ─────────────────────────────────────────────────────


@dataclass
class _FakeStatus:
    status_code: str


@dataclass
class _FakeSpan:
    name: str
    kind: str = "INTERNAL"
    status: _FakeStatus = field(default_factory=lambda: _FakeStatus("OK"))


@dataclass
class _FakeExporter:
    spans: list[_FakeSpan]

    def span_named(self, name: str) -> _FakeSpan | None:
        for s in self.spans:
            if s.name == name:
                return s
        return None

    def events_on(self, span_name: str, event_name: str) -> list[Any]:
        return []


def test_assert_has_span_found_by_name() -> None:
    exporter = _FakeExporter(spans=[_FakeSpan(name="dispatch")])
    span = assert_has_span(exporter, "dispatch")  # type: ignore[arg-type]
    assert span.name == "dispatch"


def test_assert_has_span_kind_mismatch_raises() -> None:
    exporter = _FakeExporter(spans=[_FakeSpan(name="dispatch", kind="CLIENT")])
    with pytest.raises(AssertionError, match="expected kind='SERVER'"):
        assert_has_span(exporter, "dispatch", kind="SERVER")  # type: ignore[arg-type]


def test_assert_has_span_status_mismatch_raises() -> None:
    exporter = _FakeExporter(spans=[_FakeSpan(name="dispatch", status=_FakeStatus("ERROR"))])
    with pytest.raises(AssertionError, match="expected status_code='OK'"):
        assert_has_span(exporter, "dispatch", status="OK")  # type: ignore[arg-type]


def test_assert_has_span_not_found_raises_with_available_names() -> None:
    exporter = _FakeExporter(spans=[_FakeSpan(name="a"), _FakeSpan(name="b")])
    with pytest.raises(AssertionError, match=r"available: \['a', 'b'\]"):
        assert_has_span(exporter, "missing")  # type: ignore[arg-type]


# ── assert_has_otel_event ───────────────────────────────────────────────


@dataclass
class _FakeOtelEvent:
    attributes: dict[str, object] | None


@dataclass
class _EventExporter:
    events: list[_FakeOtelEvent]
    spans: list[_FakeSpan] = field(default_factory=list)

    def span_named(self, name: str) -> _FakeSpan | None:
        return None

    def events_on(self, span_name: str, event_name: str) -> list[_FakeOtelEvent]:
        return self.events


def test_assert_has_otel_event_found_no_filters() -> None:
    exporter = _EventExporter(events=[_FakeOtelEvent(attributes={"x": 1})])
    ev = assert_has_otel_event(exporter, "span", "event")  # type: ignore[arg-type]
    assert ev is exporter.events[0]


def test_assert_has_otel_event_found_with_state_filters() -> None:
    exporter = _EventExporter(
        events=[_FakeOtelEvent(attributes={"from_state": "pending", "to_state": "running"})]
    )
    ev = assert_has_otel_event(exporter, "span", "event", from_state="pending", to_state="running")  # type: ignore[arg-type]
    assert ev is exporter.events[0]


def test_assert_has_otel_event_no_events_raises() -> None:
    exporter = _EventExporter(events=[])
    with pytest.raises(AssertionError, match="No OTel event 'event' on span 'span'"):
        assert_has_otel_event(exporter, "span", "event")  # type: ignore[arg-type]


def test_assert_has_otel_event_no_matching_filters_raises() -> None:
    exporter = _EventExporter(
        events=[_FakeOtelEvent(attributes={"from_state": "a", "to_state": "b"})]
    )
    with pytest.raises(AssertionError, match="from_state='pending'"):
        assert_has_otel_event(exporter, "span", "event", from_state="pending")  # type: ignore[arg-type]


def test_assert_has_otel_event_skips_to_state_mismatch_then_finds_match() -> None:
    exporter = _EventExporter(
        events=[
            _FakeOtelEvent(attributes={"from_state": "pending", "to_state": "other"}),
            _FakeOtelEvent(attributes={"from_state": "pending", "to_state": "running"}),
        ]
    )
    ev = assert_has_otel_event(exporter, "span", "event", from_state="pending", to_state="running")  # type: ignore[arg-type]
    assert ev is exporter.events[1]


def test_assert_has_otel_event_none_attributes_skipped_without_crashing() -> None:
    exporter = _EventExporter(
        events=[
            _FakeOtelEvent(attributes=None),
            _FakeOtelEvent(attributes={"from_state": "pending", "to_state": "running"}),
        ]
    )
    ev = assert_has_otel_event(exporter, "span", "event", from_state="pending", to_state="running")  # type: ignore[arg-type]
    assert ev is exporter.events[1]


# ── wait_for ───────────────────────────────────────────────────────────


async def test_wait_for_resolves_when_event_set_before_timeout() -> None:
    event = asyncio.Event()
    event.set()
    await wait_for(event, timeout=0.05)


async def test_wait_for_raises_when_event_never_set() -> None:
    event = asyncio.Event()
    with pytest.raises(AssertionError, match="Event not set within"):
        await wait_for(event, timeout=0.05)


# ── wait_for_job_status ─────────────────────────────────────────────────


class _QueueBackend:
    def __init__(self, rows: list[SimpleNamespace | None]) -> None:
        self._rows = list(rows)

    async def get(self, job_id: object) -> SimpleNamespace | None:
        if len(self._rows) > 1:
            return self._rows.pop(0)
        return self._rows[0]


async def test_wait_for_job_status_advances_and_succeeds() -> None:
    backend = _QueueBackend(
        [
            SimpleNamespace(status="pending"),
            SimpleNamespace(status="running"),
            SimpleNamespace(status="succeeded"),
        ]
    )
    row = await wait_for_job_status(
        backend,  # type: ignore[arg-type]
        "job-1",  # type: ignore[arg-type]
        "succeeded",
        timeout=0.2,
        poll_interval=0.01,
    )
    assert row.status == "succeeded"


async def test_wait_for_job_status_times_out_with_actual_and_expected_in_message() -> None:
    backend = _QueueBackend([SimpleNamespace(status="pending")])
    with pytest.raises(AssertionError, match=r"did not reach status 'succeeded'.*'pending'"):
        await wait_for_job_status(
            backend,  # type: ignore[arg-type]
            "job-1",  # type: ignore[arg-type]
            "succeeded",
            timeout=0.05,
            poll_interval=0.01,
        )


async def test_wait_for_job_status_times_out_when_row_none() -> None:
    backend = _QueueBackend([None])
    with pytest.raises(AssertionError, match="actual: None"):
        await wait_for_job_status(
            backend,  # type: ignore[arg-type]
            "job-1",  # type: ignore[arg-type]
            "succeeded",
            timeout=0.05,
            poll_interval=0.01,
        )


# ── wait_for_leader ──────────────────────────────────────────────────────


async def test_wait_for_leader_resolves_when_set() -> None:
    deps = SimpleNamespace(is_leader=asyncio.Event())
    deps.is_leader.set()
    await wait_for_leader(deps, timeout=0.05)  # type: ignore[arg-type]


async def test_wait_for_leader_raises_when_never_set() -> None:
    deps = SimpleNamespace(is_leader=asyncio.Event())
    with pytest.raises(AssertionError, match="Leader event not set within"):
        await wait_for_leader(deps, timeout=0.05)  # type: ignore[arg-type]


# ── pg_now ───────────────────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, value: datetime) -> None:
        self._value = value
        self.queries: list[str] = []

    async def fetchval(self, query: str) -> datetime:
        self.queries.append(query)
        return self._value


async def test_pg_now_returns_fetchval_result_and_queries_clock_timestamp() -> None:
    expected = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
    conn = _FakeConn(expected)
    result = await pg_now(conn)  # type: ignore[arg-type]
    assert result == expected
    assert len(conn.queries) == 1
    assert "clock_timestamp()" in conn.queries[0]
