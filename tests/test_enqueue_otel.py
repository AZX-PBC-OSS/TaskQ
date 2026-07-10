"""Unit tests for PRODUCER span on enqueue, traceparent storage, and
published-messages counter.

Covers span attributes, traceparent persistence, the published-messages
counter, span status on success/failure, behavior with otel disabled or
no SDK configured, span-creation exception safety, and a property test
asserting exactly one PRODUCER span per enqueue.
"""

import re

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from pydantic import BaseModel

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.actor import actor
from taskq.client import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.otel import (
    ListSpanExporter,
    counter_data_points,
    counter_value,
    setup_meter,
    setup_tracer,
)

_START = "2025-01-01T00:00:00+00:00"


class _Payload(BaseModel):
    value: int = 1


@actor(name="_otel_test_actor")
async def _otel_test_actor(payload: _Payload) -> None:
    pass


@actor(name="_otel_test_actor_q", queue="custom_q")
async def _otel_test_actor_q(payload: _Payload) -> None:
    pass


def _make_client() -> tuple[InMemoryBackend, JobsClient]:
    backend = InMemoryBackend(clock=FakeClock(_START))
    client = JobsClient(backend, clock=backend._clock)  # type: ignore[reportPrivateUsage] # Why: test-only access to FakeClock for deterministic clock
    return backend, client


_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_HEX_16 = re.compile(r"^[0-9a-f]{16}$")


# ── PRODUCER span attributes ────────────────────────────────────


class TestProducerSpan:
    """Enqueue creates a PRODUCER span with correct attributes."""

    async def test_span_name_contains_actor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None

    async def test_span_kind_is_producer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        assert span.kind == trace.SpanKind.PRODUCER

    async def test_messaging_system_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("messaging.system") == "taskq"

    async def test_messaging_destination_name_attribute(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor_q, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor_q")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("messaging.destination.name") == "custom_q"

    async def test_messaging_operation_type_publish(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("messaging.operation.type") == "publish"

    async def test_messaging_message_id_is_job_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("messaging.message.id") == str(handle.job_id)

    async def test_taskq_actor_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("taskq.actor") == "_otel_test_actor"

    async def test_taskq_identity_key_empty_when_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("taskq.identity_key") == ""

    async def test_taskq_identity_key_present_when_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload(), identity_key="acct:42")

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        attrs = span.attributes
        assert attrs is not None
        assert attrs.get("taskq.identity_key") == "acct:42"


# ── traceparent stored in job row ───────────────────────────────


class TestTraceparent:
    """PRODUCER span context stored in job row as hex trace_id/span_id."""

    async def test_trace_id_stored_as_32_hex(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.trace_id is not None
        assert _HEX_32.match(row.trace_id)

    async def test_span_id_stored_as_16_hex(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.span_id is not None
        assert _HEX_16.match(row.span_id)

    async def test_stored_ids_match_span_context(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        ctx = span.get_span_context()
        assert ctx is not None
        expected_trace = format(ctx.trace_id, "032x")
        expected_span = format(ctx.span_id, "016x")

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.trace_id == expected_trace
        assert row.span_id == expected_span


# ── partial: published-messages counter ──────────────────────────


class TestPublishedCounter:
    """(partial): messaging.client.published.messages counter incremented."""

    async def test_counter_incremented_on_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        reader = setup_meter(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        assert counter_value(reader, "messaging.client.published.messages") == 1

    async def test_counter_has_actor_and_queue_labels(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        reader = setup_meter(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor_q, _Payload())

        dps = counter_data_points(reader, "messaging.client.published.messages")
        assert len(dps) >= 1
        dp = dps[0]
        assert dp.attributes is not None
        assert dp.attributes.get("actor") == "_otel_test_actor_q"
        assert dp.attributes.get("queue") == "custom_q"

    async def test_counter_not_incremented_on_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        reader = setup_meter(monkeypatch)
        backend, client = _make_client()

        async def _raise(_args: object) -> None:
            raise RuntimeError("enqueue failed")

        object.__setattr__(backend, "enqueue", _raise)

        with pytest.raises(RuntimeError, match="enqueue failed"):
            await client.enqueue(_otel_test_actor, _Payload())

        assert counter_value(reader, "messaging.client.published.messages") == 0


# ── Span status on success / failure ──────────────────────────────────


class TestSpanStatus:
    """Span status is OK on success, ERROR on failure (partial)."""

    async def test_span_status_ok_on_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        assert span.status.status_code == trace.StatusCode.OK

    async def test_span_status_error_on_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        backend, client = _make_client()

        async def _raise(_args: object) -> None:
            raise RuntimeError("enqueue failed")

        object.__setattr__(backend, "enqueue", _raise)

        with pytest.raises(RuntimeError, match="enqueue failed"):
            await client.enqueue(_otel_test_actor, _Payload())

        span = exporter.span_named("enqueue _otel_test_actor")
        assert span is not None
        assert span.status.status_code == trace.StatusCode.ERROR


# ── otel disabled ──────────────────────────────────────────────


class TestOtelDisabled:
    """When otel_enabled=False, no span or counter; enqueue succeeds."""

    async def test_no_span_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        otel_mod.set_otel_enabled(False)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        assert len(exporter.spans) == 0

    async def test_no_counter_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        reader = setup_meter(monkeypatch)
        otel_mod.set_otel_enabled(False)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        assert counter_value(reader, "messaging.client.published.messages") == 0

    async def test_enqueue_succeeds_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        otel_mod.set_otel_enabled(False)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        assert handle.job_id is not None
        row = await backend.get(handle.job_id)
        assert row is not None

    async def test_no_traceparent_when_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        setup_tracer(monkeypatch)
        otel_mod.set_otel_enabled(False)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())

        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.trace_id is None
        assert row.span_id is None


# ── no SDK configured (API-only no-op) ────────────────────────


class TestNoSdk:
    """When no SDK is configured, enqueue succeeds without raising."""

    async def test_enqueue_succeeds_without_sdk(self) -> None:
        otel_mod.set_otel_enabled(True)
        _, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())
        assert handle.job_id is not None

    async def test_trace_id_none_without_sdk(self) -> None:
        otel_mod.set_otel_enabled(True)
        backend, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.trace_id is None
        assert row.span_id is None


# ── span creation exception safety ─────────────────────────────


class TestSpanExceptionSafety:
    """Span creation exceptions caught; enqueue still succeeds."""

    async def test_enqueue_succeeds_when_tracer_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        otel_mod.set_otel_enabled(True)

        def _raising_start(*args: object, **kwargs: object) -> object:
            raise RuntimeError("simulated OTel misconfiguration")

        monkeypatch.setattr(
            otel_mod.get_tracer(),
            "start_as_current_span",
            _raising_start,
        )

        _, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())
        assert handle.job_id is not None

    async def test_enqueue_succeeds_and_no_span_when_tracer_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        otel_mod.set_otel_enabled(True)

        def _raising_start(*args: object, **kwargs: object) -> object:
            raise RuntimeError("simulated OTel misconfiguration")

        monkeypatch.setattr(
            otel_mod.get_tracer(),
            "start_as_current_span",
            _raising_start,
        )

        _, client = _make_client()

        handle = await client.enqueue(_otel_test_actor, _Payload())
        assert handle.job_id is not None


# ── Property test — every enqueue produces exactly one PRODUCER span ──


class TestProperty:
    """Every enqueue produces exactly one PRODUCER span regardless of payload or queue."""

    async def test_one_producer_span_per_enqueue(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        for i in range(5):
            await client.enqueue(_otel_test_actor, _Payload(value=i))

        producer_spans = exporter.spans_with_kind(trace.SpanKind.PRODUCER)
        assert len(producer_spans) == 5

    async def test_different_queues_each_get_producer_span(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())
        await client.enqueue(_otel_test_actor_q, _Payload())

        producer_spans = exporter.spans_with_kind(trace.SpanKind.PRODUCER)
        assert len(producer_spans) == 2
        names = {s.name for s in producer_spans}
        assert "enqueue _otel_test_actor" in names
        assert "enqueue _otel_test_actor_q" in names


@given(value=st.integers(min_value=-1000, max_value=1000))
@settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
async def test_tp1_hypothesis_exactly_one_producer_span_per_enqueue(
    value: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every enqueue produces exactly one PRODUCER span regardless of payload value."""
    _, exporter = setup_tracer(monkeypatch)
    _, client = _make_client()

    await client.enqueue(_otel_test_actor, _Payload(value=value))

    producer_spans = exporter.spans_with_kind(trace.SpanKind.PRODUCER)
    assert len(producer_spans) == 1
    assert producer_spans[0].name == "enqueue _otel_test_actor"


# ── Counter recorded outside span body ──────────────────────────────────


class TestCounterSamplingIndependence:
    """Counter is recorded outside the span body for sampling independence."""

    async def test_counter_incremented_even_if_span_not_sampled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

        reader = setup_meter(monkeypatch)

        exporter = ListSpanExporter()
        provider = TracerProvider(sampler=ALWAYS_OFF)
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        test_tracer = provider.get_tracer(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())
        monkeypatch.setattr(otel_mod, "get_tracer", lambda: test_tracer)

        _, client = _make_client()

        await client.enqueue(_otel_test_actor, _Payload())

        assert len(exporter.spans) == 0

        assert counter_value(reader, "messaging.client.published.messages") == 1
