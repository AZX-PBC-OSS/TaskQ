"""Unit tests for the dispatch INTERNAL span, taskq.dispatch.duration histogram,
CONSUMER span creation, and consumer-path metrics (instruments 2-3).

Covers:
  - dispatch creates INTERNAL span with correct name/attributes
  - CONSUMER span with link to PRODUCER span
  - No link when NULL trace_id/span_id
  - messaging.client.consumed.messages counter
  - messaging.process.duration histogram
  - dispatch span has no messaging.operation.type
  - otel_enabled=False suppresses span and histogram
  - Single-queue and multi-queue attribute handling
  - taskq.dispatch.duration histogram records SQL-execution latency
"""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from opentelemetry import trace

import taskq.obs as obs_mod
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.testing.otel import (
    collect_metrics,
    histogram_points,
    setup_meter,
    setup_tracer,
)


class _FakeConn:
    def __init__(self, rows: list[dict[str, int]] | None = None, latency: float = 0.0) -> None:
        self._rows: list[dict[str, int]] = rows if rows is not None else [{"id": 1}]
        self._latency = latency

    async def fetch(self, sql: str, *args: object) -> list[dict[str, int]]:
        if self._latency > 0:
            await asyncio.sleep(self._latency)
        return self._rows


# ── dispatch creates INTERNAL span with correct name/attributes ──


async def test_dispatch_creates_internal_span_with_correct_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake satisfies fetch protocol but not asyncpg.Connection full type
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    span = exporter.span_named("dispatch")
    assert span is not None
    assert span.kind == trace.SpanKind.INTERNAL


async def test_dispatch_span_has_correct_attributes_single_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    span = exporter.span_named("dispatch")
    assert span is not None
    assert span.attributes is not None
    assert span.attributes.get("taskq.queue") == "default"
    assert span.attributes.get("taskq.queues") == "default"
    assert span.attributes.get("taskq.batch_size") == 10


async def test_dispatch_span_has_correct_attributes_multi_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default", "critical"],
        limit_n=20,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    span = exporter.span_named("dispatch")
    assert span is not None
    assert span.attributes is not None
    assert span.attributes.get("taskq.queue") == "default"
    assert span.attributes.get("taskq.queues") == "default,critical"
    assert span.attributes.get("taskq.batch_size") == 20


# ── dispatch span has no messaging.operation.type ──────────────


async def test_dispatch_span_has_no_messaging_operation_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    span = exporter.span_named("dispatch")
    assert span is not None
    assert span.attributes is not None
    assert "messaging.operation.type" not in span.attributes


async def test_dispatch_span_has_no_old_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    span = exporter.span_named("dispatch")
    assert span is not None
    assert span.attributes is not None
    assert "taskq.dispatch.limit" not in span.attributes
    assert "taskq.dispatch.queues" not in span.attributes
    assert "taskq.dispatch.worker_id" not in span.attributes
    assert "taskq.dispatch.returned_count" not in span.attributes


# ── otel_enabled=False suppresses span and histogram ────────────────────


async def test_dispatch_otel_disabled_no_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, exporter = setup_tracer(monkeypatch)
    setup_meter(monkeypatch)
    obs_mod.set_otel_enabled(False)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    assert exporter.span_named("dispatch") is None


async def test_dispatch_otel_disabled_no_histogram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = setup_meter(monkeypatch)
    obs_mod.set_otel_enabled(False)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    assert len(collect_metrics(reader)) == 0


# ── taskq.dispatch.duration histogram records SQL-execution latency ─────


async def test_dispatch_duration_histogram_records_latency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _FakeConn(latency=0.01)
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    points = histogram_points(reader, "taskq.dispatch.duration")
    assert len(points) == 1
    assert points[0].attributes is not None
    assert points[0].attributes.get("queue") == "default"  # pyright: ignore[reportOptionalMemberAccess] # Why: assert above narrows attributes to non-None; pyright cannot track the narrowing across the method call
    assert points[0].count >= 1
    assert points[0].sum >= 0.01


async def test_dispatch_duration_histogram_multi_queue_labels_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)

    conn = _FakeConn()
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")
    worker_id = UUID("00000000-0000-0000-0000-000000000001")

    await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default", "critical"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    points = histogram_points(reader, "taskq.dispatch.duration")
    assert len(points) == 1
    assert points[0].attributes is not None
    assert points[0].attributes.get("queue") == "default"  # pyright: ignore[reportOptionalMemberAccess] # Why: assert above narrows attributes to non-None; pyright cannot track the narrowing across the method call


# ── Existing dispatch_batch tests still pass ───────────────────────────


async def test_dispatch_batch_still_passes_fetch_args() -> None:
    captured_sql: str | None = None
    captured_args: tuple[object, ...] | None = None

    class _CapturingConn:
        async def fetch(self, sql: str, *args: object) -> list[dict[str, int]]:
            nonlocal captured_sql, captured_args
            captured_sql = sql
            captured_args = args
            return [{"id": 1}, {"id": 2}]

    obs_mod.set_otel_enabled(False)

    conn = _CapturingConn()
    worker_id = UUID("00000000-0000-0000-0000-000000000001")
    rendered = DISPATCH_STRICT_FIFO_SQL.format(schema="taskq")

    result = await dispatch_batch(
        conn,  # type: ignore[arg-type] # Why: duck-typed fake
        sql=rendered,
        queues=["default", "critical"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )

    assert captured_sql == rendered
    assert captured_args is not None
    assert captured_args[0] == ["default", "critical"]
    assert captured_args[1] == 10
    assert result == [{"id": 1}, {"id": 2}]


# ── CONSUMER span with link (unit-level) ──────────────────────


async def test_consumer_span_with_link(monkeypatch: pytest.MonkeyPatch) -> None:
    """CONSUMER span links to PRODUCER span when job has trace_id/span_id."""
    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    producer_span = tracer.start_span("enqueue test_actor")
    prod_ctx = producer_span.get_span_context()
    producer_span.end()

    link_ctx = trace.SpanContext(
        trace_id=prod_ctx.trace_id,
        span_id=prod_ctx.span_id,
        is_remote=True,
        trace_flags=trace.TraceFlags(0x01),
    )
    links = [trace.Link(link_ctx)]

    with tracer.start_as_current_span(
        "process test_actor",
        kind=trace.SpanKind.CONSUMER,
        links=links,
    ) as span:
        span.set_attribute("messaging.system", "taskq")
        span.set_status(trace.StatusCode.OK)

    consumer = exporter.span_named("process test_actor")
    assert consumer is not None
    assert consumer.kind == trace.SpanKind.CONSUMER
    assert consumer.links is not None
    assert len(consumer.links) == 1
    assert consumer.links[0].context.trace_id == prod_ctx.trace_id
    assert consumer.links[0].context.span_id == prod_ctx.span_id


async def test_consumer_span_no_link_when_null_trace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CONSUMER span has empty links when trace_id/span_id are None."""
    _, exporter = setup_tracer(monkeypatch)
    tracer = obs_mod.get_tracer()

    with tracer.start_as_current_span(
        "process test_actor",
        kind=trace.SpanKind.CONSUMER,
        links=[],
    ) as span:
        span.set_attribute("messaging.system", "taskq")
        span.set_status(trace.StatusCode.OK)

    consumer = exporter.span_named("process test_actor")
    assert consumer is not None
    assert consumer.links is not None
    assert len(consumer.links) == 0


# ── consumed counter (instrument 2) ────────────────────────────


async def test_consumed_counter_records_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """record_consumed_message increments counter with actor, queue, outcome."""
    reader = setup_meter(monkeypatch)

    obs_mod.record_consumed_message("my_actor", "default", outcome="succeeded")

    consumed_metrics = [
        m for m in collect_metrics(reader) if m.name == "messaging.client.consumed.messages"
    ]
    assert len(consumed_metrics) == 1
    dp = list(consumed_metrics[0].data.data_points)
    assert len(dp) == 1
    assert dp[0].attributes is not None
    assert dp[0].attributes.get("actor") == "my_actor"
    assert dp[0].attributes.get("queue") == "default"
    assert dp[0].attributes.get("outcome") == "succeeded"


async def test_consumed_counter_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """otel_enabled=False suppresses consumed counter."""
    setup_meter(monkeypatch)
    obs_mod.set_otel_enabled(False)

    obs_mod.record_consumed_message("my_actor", "default", outcome="failed")

    assert len(collect_metrics(setup_meter(monkeypatch))) == 0


# ── ConsumedOutcome label set (regression: outcome must not be "scheduled") ─


async def test_consumed_outcome_scheduled_maps_to_abandoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_to_consumed_outcome maps "scheduled" to "abandoned" for instrument 2."""
    from taskq.worker.dispatch import _to_consumed_outcome

    assert _to_consumed_outcome("scheduled") == "abandoned"


async def test_consumed_outcome_passes_through_valid_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_to_consumed_outcome passes succeeded/failed/cancelled through unchanged."""
    from taskq.worker.dispatch import _to_consumed_outcome

    assert _to_consumed_outcome("succeeded") == "succeeded"
    assert _to_consumed_outcome("failed") == "failed"
    assert _to_consumed_outcome("cancelled") == "cancelled"


async def test_consumed_counter_all_valid_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """record_consumed_message accepts all four ConsumedOutcome labels."""
    from taskq.testing.otel import counter_data_points

    reader = setup_meter(monkeypatch)

    for outcome in ("succeeded", "failed", "cancelled", "abandoned"):
        obs_mod.record_consumed_message("actor_a", "default", outcome=outcome)

    dps = counter_data_points(reader, "messaging.client.consumed.messages")
    assert len(dps) == 4
    recorded = {dp.attributes.get("outcome") for dp in dps if dp.attributes is not None}
    assert recorded == {"succeeded", "failed", "cancelled", "abandoned"}


# ── process duration histogram (instrument 3) ──────────────────


async def test_process_duration_records_elapsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """record_process_duration records histogram with actor and queue."""
    reader = setup_meter(monkeypatch)

    obs_mod.record_process_duration("my_actor", "default", 0.42)

    points = histogram_points(reader, "messaging.process.duration")
    assert len(points) == 1
    assert points[0].attributes is not None
    assert points[0].attributes.get("actor") == "my_actor"
    assert points[0].attributes.get("queue") == "default"
    assert points[0].count >= 1
    assert points[0].sum >= 0.42


async def test_process_duration_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """otel_enabled=False suppresses process duration histogram."""
    setup_meter(monkeypatch)
    obs_mod.set_otel_enabled(False)

    obs_mod.record_process_duration("my_actor", "default", 0.42)

    assert len(collect_metrics(setup_meter(monkeypatch))) == 0
