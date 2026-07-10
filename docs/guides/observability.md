# Observability

TaskQ instruments itself with OpenTelemetry (vendor-neutral) and structured
logging (structlog). No vendor SDK is bundled. You export data to any
OTLP-compatible backend — Jaeger, Grafana Tempo, Honeycomb, Datadog, Sentry,
Azure Monitor, PostHog — by pointing the standard OTel environment variables
at your collector or agent.

## Contents

1. [OpenTelemetry — setup](#1-opentelemetry-setup)
2. [Traces — span hierarchy](#2-traces-span-hierarchy)
3. [Metrics reference](#3-metrics-reference)
4. [Structured logging](#4-structured-logging)
5. [Log format examples](#5-log-format-examples)
6. [Testing observability](#6-testing-observability)
7. [Disabling OTel](#7-disabling-otel)
8. [External OTLP collector](#8-external-otlp-collector)
9. [Error reporting (ErrorReporter Protocol)](#9-error-reporting-errorreporter-protocol)

---

## 1. OpenTelemetry — setup

### Prerequisites

`opentelemetry-api` is a core dependency. The `opentelemetry-sdk` and
OTLP exporter require the `[otel]` extra:

```bash
pip install "taskq-py[otel]"
```

### Enabling and disabling

| Variable | Default | Effect |
|---|---|---|
| `TASKQ_OTEL_ENABLED` | `true` | When `false`, all span and metric creation is suppressed; operations still succeed. |

### Worker startup

`worker_main` calls `set_otel_enabled(settings.otel_enabled)` before the
TaskGroup opens. No application code needs to call this directly.

### Exporter configuration

Configure the exporter with standard OTel environment variables. TaskQ does
not override them.

| Variable | Example |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` |
| `OTEL_SERVICE_NAME` | `my-app-worker` |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=production,k8s.pod.name=worker-0` |

Common receiver addresses:

- **Datadog Agent**: `http://localhost:4317` (gRPC OTLP port)
- **Sentry Spotlight / OTel ingest**: same
- **Azure Monitor**: OTLP URL derived from the App Insights connection string
- **PostHog**: PostHog Cloud OTLP endpoint

### Instrumentation name

All spans and metrics are created under instrumentation name `"taskq"` (the
value of `INSTRUMENTATION_NAME`).

### Semconv compliance

TaskQ uses OTel messaging semconv attribute names
(`messaging.operation.type=publish`, `messaging.operation.type=process`,
`messaging.consumer.group.name`, etc.) so that dashboards built against the
spec work without renaming.

---

## 2. Traces — span hierarchy

A complete job lifecycle produces four spans:

```
enqueue <actor>          (PRODUCER)
  └── process <actor>    (CONSUMER, linked to PRODUCER via span link)
        └── attempt.<N>  (INTERNAL)
```

A fifth INTERNAL span wraps the batch dispatch SQL query:

```
dispatch                 (INTERNAL)
```

### Enqueue span

**Name**: `enqueue <actor_name>` (e.g. `enqueue send_email`)

Emitted by `JobsClient.enqueue` and `SubJobEnqueuer.enqueue`.

| Attribute | Value |
|---|---|
| `messaging.system` | `"taskq"` |
| `messaging.destination.name` | queue name |
| `messaging.operation.type` | `"publish"` |
| `messaging.message.id` | job UUID (set after the DB write) |
| `taskq.actor` | registered actor name |
| `taskq.identity_key` | identity key string, or `""` |

The enqueue span's `trace_id` and `span_id` are stored in the `jobs` table row
(`trace_id`, `span_id` columns). The worker reads these values at dispatch time
to reconstruct the span context.

### Dispatch span

**Name**: `dispatch`

Emitted by `dispatch_batch` each time the worker fetches a batch from
PostgreSQL.

| Attribute | Value |
|---|---|
| `taskq.queue` | first queue in the batch |
| `taskq.queues` | comma-separated list of queues |
| `taskq.batch_size` | requested batch limit |

Errors on the SQL call set `StatusCode.ERROR` and call `span.record_exception`.

### Consumer span

**Name**: `process <actor_name>` (e.g. `process send_email`)

Emitted by `dispatch_one_job`. Carries a **span link** back to the enqueue
span so a trace viewer can correlate across the async boundary. If the stored
`trace_id` or `span_id` is malformed (non-hex), the link is silently skipped
and the consumer span is still created.

| Attribute | Value |
|---|---|
| `messaging.system` | `"taskq"` |
| `messaging.destination.name` | queue name |
| `messaging.operation.type` | `"process"` |
| `messaging.message.id` | job UUID |
| `messaging.consumer.group.name` | `TASKQ_WORKER_GROUP` setting |
| `taskq.actor` | registered actor name |
| `taskq.attempt` | attempt number (1-based) |
| `taskq.identity_key` | identity key string, or `""` |
| `taskq.batch_id` | batch ID from job metadata, or `""` |

The consumer span also carries **lifecycle events** added as span events:

| Event name | When added |
|---|---|
| `lifecycle.running` | job transitions to `running` |
| `lifecycle.succeeded` | actor returns without error |
| `lifecycle.cancelled` | job is cancelled |
| `lifecycle.scheduled` | job is snoozed or reservation unavailable |
| `lifecycle.failed` | actor raises an unrecoverable error |

Status is set to `StatusCode.OK` on success, `StatusCode.ERROR` on failure or
cancellation.

### Attempt span

**Name**: `attempt.<N>` (e.g. `attempt.1`)

An INTERNAL child of the consumer span. Wraps the actual actor function call
plus transaction commit for the transactional execution path.

### Trace context propagation

When a job is enqueued inside an active OTel span, the span context
(`trace_id` as a 32-character hex string, `span_id` as a 16-character hex
string) is written to the `jobs` row. At dispatch time the worker reads those
columns, constructs a `SpanContext` with `is_remote=True`, and passes it as a
`trace.Link` to the consumer span. This allows end-to-end tracing across the
async enqueue-to-execute boundary without requiring the worker and the enqueueing
process to share a trace context in-band.

### Accessing the span inside an actor

`ctx.span` is the live consumer span, or `None` when OTel is disabled:

```python
from taskq import actor
from taskq.context import JobContext

@actor
async def my_actor(payload: Payload, ctx: JobContext[Payload]) -> Result:
    span = ctx.span  # opentelemetry.trace.Span | None
    if span:
        span.set_attribute("custom.key", "value")
    ...
```

---

## 3. Metrics reference

All instruments are created from the `"taskq"` meter. Instruments marked
**unconditional** are recorded even when `TASKQ_OTEL_ENABLED=false` because
they represent safety-critical signals.

### Counters

| Metric name | Unit | Attributes | Description | Conditional? |
|---|---|---|---|---|
| `messaging.client.published.messages` | `1` | `actor`, `queue` | Jobs successfully enqueued. | yes |
| `messaging.client.consumed.messages` | `1` | `actor`, `queue`, `outcome` | Jobs consumed. `outcome` is one of `succeeded`, `failed`, `cancelled`, `abandoned`. A snoozed or rescheduled job maps to `abandoned`. | yes |
| `taskq.cancellation.requested` | — | — | Incremented once per `JobsClient.cancel()` call regardless of outcome. | unconditional |
| `taskq.cancellation.phase_transitions` | `1` | — | Cancel phase transitions (0→1, 1→2, etc.). | yes |
| `taskq.backpressure.errors` | — | `actor`, `kind` | Enqueue rejections due to backpressure. `kind` is currently `"max_pending"`. | unconditional |
| `taskq.deadline_exceeded_sweep.jobs_failed` | `1` | `actor` | Jobs failed by the deadline-exceeded sweep. | unconditional |
| `taskq.heartbeat.misses` | `1` | `worker_id` | Heartbeat renewal failures per worker. | yes |
| `taskq.leader.election_attempts` | `1` | `worker_id` | Leader election attempts. | yes |
| `taskq.leader.election_failures` | `1` | `worker_id` | Election attempts that did not win the lock. | yes |
| `taskq.error_reporter.failures` | `1` | `reporter_type` | `ErrorReporter` invocation failures. | yes |
| `taskq.progress.publish_failures` | `1` | — | Redis publish failures for progress fanout. | yes |
| `taskq.ratelimit.refund_failures` | `1` | `bucket`, `backend` | Rate-limit refund/rollback failures. | yes |
| `taskq.pruned.jobs` | `1` | `actor`, `status` | Jobs moved from `jobs` to `jobs_archive` by the prune sweep (Sweep 5). | yes |
| `taskq.archived.jobs` | `1` | `status` | Same prune-sweep event, status-only view (no actor dimension). | yes |
| `taskq.expired_archive.jobs` | `1` | `status` | Jobs hard-deleted from `jobs_archive` by the archive expiry sweep (Sweep 6). | yes |
| `taskq.maintenance_leader.sweep_rows` | — | `sweep_name` | Rows affected per sweep tick. | yes |

### Histograms

| Metric name | Unit | Attributes | Description |
|---|---|---|---|
| `messaging.process.duration` | `s` | `actor`, `queue` | End-to-end job execution duration from dispatch to terminal state. |
| `taskq.dispatch.duration` | `s` | `queue` | Batch dispatch SQL query latency (SQL execution only). |
| `taskq.lock.expires_in_seconds` | `s` | `worker_id` | Remaining lock TTL at each heartbeat renewal. Buckets: 0, 5, 10, 15, 20, 30, 45, 60 s. |
| `taskq.heartbeat.tick_duration_seconds` | `s` | — | Wall-clock seconds per heartbeat tick. |
| `taskq.maintenance_leader.sweep_duration_ms` | `ms` | — | Per-sweep-tick wall-clock duration. |

### Observable gauges (polled)

| Metric name | Unit | Attributes | Description |
|---|---|---|---|
| `taskq.queue.depth` | `1` | `queue` | Pending and scheduled jobs per queue. Sampled by the leader every 15 s. |
| `taskq.reservation.slots_used` | `1` | `bucket` | In-use reservation slots per rate-limit bucket. Sampled by the leader every 15 s. |
| `taskq.maintenance_leader.is_leader` | `1` | `worker_id` | `1` on the elected leader pod, `0` on all others. |
| `taskq.cron.disabled_schedules` | `1` | — | Count of currently disabled cron schedules. |
| `taskq.heartbeat.consecutive_failures` | — | — | Consecutive heartbeat tick failures for this worker (sample-on-scrape). |

### Up-down counters

| Metric name | Unit | Attributes | Description |
|---|---|---|---|
| `taskq.cron.consecutive_failures` | `1` | `schedule_id` | Consecutive cron execution failures per schedule. On success the caller adds a negative delta equal to the current count to reset to zero. |

### Metric recording and sampling independence

Counters and histograms in the dispatch and consume paths are recorded
**outside** their corresponding span bodies. This ensures that a 100% sampled
span does not inflate metric counts relative to a partially-sampled trace.

---

## 4. Structured logging

### Setup

`worker_main` calls `setup_logging(level=settings.log_level, log_format=settings.log_format)`.
`setup_logging` is idempotent — calling it a second time is a no-op.

### Configuration

| Variable | Default | Values |
|---|---|---|
| `TASKQ_LOG_FORMAT` | `json` | `json` (production), `console` (development) |
| `TASKQ_LOG_LEVEL` | `INFO` | Any stdlib level name: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

`log_format` rejects any value outside `{"json", "console"}` at
`WorkerSettings` load time.

### Processor chain

The structlog processor chain applied to every log call (in order):

1. `merge_contextvars` — pulls in `worker_id` (and any other context vars bound by the worker)
2. `add_log_level`
3. `add_logger_name`
4. `StackInfoRenderer`
5. `TimeStamper(fmt="iso", utc=True)` — ISO 8601 UTC timestamp in `timestamp` field
6. `_otel_span_processor` — injects `trace_id` and `span_id` from the active OTel span, if any
7. `EventRenamer("event")` — ensures the event key is always `event`
8. `JSONRenderer` (production) or `ConsoleRenderer` (development)

Every processor is wrapped in a no-raise safety wrapper. A failing
processor logs a warning and passes the event dict through unchanged; it never
propagates to actor or user code.

### Job-context fields

`bind_job_context` returns a new immutable `BoundLogger` pre-bound with:

| Field | Always present? |
|---|---|
| `job_id` | yes |
| `actor` | yes |
| `queue` | yes |
| `attempt` | yes |
| `trace_id` | yes (empty string when no active OTel span) |
| `worker_id` | via contextvars (bound once at worker startup) |
| `identity_key` | only when non-`None` |
| `span_id` | only when non-`None` |
| `batch_id` | only when non-`None` |

`span_id`, `identity_key`, and `batch_id` are omitted entirely when `None`
(they are not serialized as `null`).

### Key log events

| Event | Level | Kind | Key fields | When |
|---|---|---|---|---|
| `state_change` | info | `state_change` | `from_state`, `to_state` | Any job status transition |
| `cancel_phase_change` | info | `cancel_phase_change` | `from_phase`, `to_phase` | Cancel phase escalation |
| `heartbeat-tick-success` | debug | — | `worker_id`, `tick_duration_ms`, `jobs_extended`, `is_leader` | Each successful heartbeat tick |
| `heartbeat-tick-failure` | warning | — | `worker_id`, `consecutive_failures`, `error_class`, `error` | Each failed heartbeat tick |
| `heartbeat-hook-failure` | warning | `state_change` | `worker_id`, `cause`, `error` | Cancel controller failure inside heartbeat transaction |
| `heartbeat-tick-unexpected-error` | error | — | `worker_id` | Unexpected exception in heartbeat loop |
| `dispatch` | info | `dispatch` | `from_state`, `to_state`, `count`, `worker_id`, `queues`, `limit_n` | Each dispatch batch |
| `consume-rate-limit-denied-noop` | debug | — | `from_state`, `to_state`, `cause` | Reservation denied but no state transition occurred |
| `prune` | info | `prune` | `status`, `count`, `cutoff_time`, `duration_ms` | Per-status batch result from the prune sweep (Sweep 5) |
| `archive_expiry` | info | `archive_expiry` | `status`, `count`, `expire_before`, `duration_ms` | Per-status batch result from the archive expiry sweep (Sweep 6) |

`state_change` events also carry `cause`, `bucket_name`, `delay_seconds`, and
similar context fields depending on the transition.

### Payload redaction

`obs.redact_payload(payload)` returns the first 16 hex characters of the
SHA-256 digest of the JSON-serialized payload. Raw payload content is never
written to logs. Use this when you want to correlate log lines with a specific
payload without exposing its contents:

```python
ctx.log.info(
    "state_change",
    kind="state_change",
    from_state="pending",
    to_state="running",
    payload_hash=obs.redact_payload(payload),
)
```

### Logging inside an actor

`ctx.log` is a `structlog.stdlib.BoundLogger` already bound with `job_id`,
`actor`, `queue`, `attempt`, and `trace_id`. `worker_id` arrives via
contextvars. You do not need to add these fields manually.

```python
from taskq import actor
from taskq.context import JobContext

@actor
async def my_actor(payload: Payload, ctx: JobContext[Payload]) -> Result:
    ctx.log.info("processing-started", item_count=len(payload.items))
    for item in payload.items:
        ctx.log.debug("processing-item", item_id=item.id)
    return Result(...)
```

---

## 5. Log format examples

### JSON (`TASKQ_LOG_FORMAT=json`)

```json
{
  "event": "state_change",
  "kind": "state_change",
  "job_id": "018e1234-abcd-7000-8000-000000000001",
  "actor": "send_email",
  "queue": "default",
  "attempt": 1,
  "from_state": "running",
  "to_state": "succeeded",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "worker_id": "018e5678-ef01-7000-8000-000000000002",
  "timestamp": "2025-01-15T10:23:45.123456Z",
  "level": "info",
  "logger": "taskq.worker._consumer"
}
```

### Console (`TASKQ_LOG_FORMAT=console`)

```
2025-01-15T10:23:45.123456Z [info     ] state_change    actor=send_email attempt=1 from_state=running job_id=018e1234-... queue=default to_state=succeeded
```

The console renderer uses `structlog.dev.ConsoleRenderer`. Field ordering and
coloring depend on the structlog version.

---

## 6. Testing observability

### Setting up a test tracer

`taskq.testing.otel.setup_tracer` creates an in-process `TracerProvider`
backed by `ListSpanExporter` and patches `obs.get_tracer` for the duration of
the test:

```python
import pytest
from taskq.testing.otel import setup_tracer, ListSpanExporter

async def test_span_emitted(monkeypatch: pytest.MonkeyPatch) -> None:
    provider, exporter = setup_tracer(monkeypatch)

    # ... run enqueue or dispatch ...

    consumer = exporter.span_named("process my_actor")
    assert consumer is not None
    assert consumer.kind == trace.SpanKind.CONSUMER
```

`ListSpanExporter` helpers:

| Method | Returns |
|---|---|
| `span_named(name)` | First `ReadableSpan` with that name, or `None` |
| `spans_named(name)` | All `ReadableSpan` objects with that name |
| `events_on(span_name, event_name)` | All events named `event_name` on the first span named `span_name` |
| `spans_with_kind(kind)` | All spans with the given `SpanKind` |

### Setting up a test meter

`taskq.testing.otel.setup_meter` creates a per-test `MeterProvider` backed by
`InMemoryMetricReader` and patches the four core dispatch-path instruments:

```python
from taskq.testing.otel import setup_meter, counter_value, histogram_points

async def test_metrics_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = setup_meter(monkeypatch)

    # ... run enqueue and dispatch ...

    assert counter_value(reader, "messaging.client.published.messages") >= 1
    assert counter_value(reader, "messaging.client.consumed.messages") >= 1
    assert len(histogram_points(reader, "messaging.process.duration")) >= 1
```

Metric query helpers:

| Helper | Returns |
|---|---|
| `collect_metrics(reader)` | All `Metric` objects from the reader |
| `counter_value(reader, name)` | Summed integer value for a counter |
| `counter_data_points(reader, name)` | `list[NumberDataPoint]` for a counter |
| `histogram_points(reader, name)` | `list[HistogramDataPoint]` for a histogram |

### Autouse fixtures

`taskq.testing.otel` exports two `autouse` pytest fixtures that are imported
into `conftest.py`:

- `_otel_enabled_guard` — snapshots and restores `_otel_enabled` around each test
- `_logging_configured_guard` — resets structlog configuration and removes
  `ProcessorFormatter` handlers around each test

These fixtures run automatically for any test that imports from
`taskq.testing.otel`. See [../api-reference/testing.md](../api-reference/testing.md) for the full
fixture inventory.

### Verifying trace context propagation

The integration tests in `tests/test_otel_integration.py` show the full
end-to-end pattern: enqueue, manually advance the job to `running`, call
`dispatch_one_job`, then assert that the consumer span carries a link whose
`trace_id` and `span_id` match the producer span:

```python
producer = exporter.span_named("enqueue _integration_test_actor")
consumer = exporter.span_named("process _integration_test_actor")

assert len(consumer.links) == 1
assert consumer.links[0].context.trace_id == producer.get_span_context().trace_id
assert consumer.links[0].context.span_id == producer.get_span_context().span_id
```

---

## 7. Disabling OTel

Set `TASKQ_OTEL_ENABLED=false` to suppress all span and metric creation.
Structured logging is independent of this flag and remains active.

When OTel is disabled:
- `safe_start_span` yields a `NonRecordingSpan` (no-op).
- All `record_*` helpers return immediately without touching the meter.
- `ctx.span` is `None`.
- The unconditional counters (`taskq.cancellation.requested`,
  `taskq.backpressure.errors`, `taskq.deadline_exceeded_sweep.jobs_failed`)
  continue to record because they use the module-level instrument directly
  without checking the flag.

---

## 8. External OTLP collector

Add an OTel collector alongside the TaskQ worker services. The collector
receives spans and metrics on the standard OTLP gRPC port and forwards them
to your backend.

```yaml
# docker-compose.yml — collector service only
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-config.yaml"]
    volumes:
      - ./otel-config.yaml:/etc/otel-config.yaml:ro
    ports:
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
```

Point the worker at the collector:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
OTEL_SERVICE_NAME=taskq-worker \
taskq worker --actors myapp.actors:registry
```

---

## 9. Error reporting (ErrorReporter Protocol)

OTel exception events and structured logs cover most error-observability
needs. For error handling that needs vendor-specific routing — DLQ to Sentry,
a custom alerting webhook, or an external audit log — TaskQ defines an
`ErrorReporter` Protocol that you implement and register as a DI provider.

### The Protocol

`ErrorReporter` is a `typing.Protocol` with a single async method:

```python
from typing import Protocol
from taskq.backend._protocol import JobRow

class ErrorReporter(Protocol):
    async def report(self, job: JobRow, exception: BaseException) -> None: ...
```

The worker invokes `report()` after a job reaches a terminal `failed` state,
passing the final `JobRow` and the exception that caused the failure. The
argument order is `(job, exception)` — matching `OnRetryExhausted` (see
[Retries — `on_retry_exhausted` hook](retries.md#8-on_retry_exhausted-hook)).
The call is fire-and-forget with respect to the job lifecycle — a failing
reporter does not alter the job's terminal state.

The invocation is wrapped by `invoke_error_reporter`, which guards the call
with `asyncio.wait_for` using a timeout of 3 seconds (the `error_reporter_timeout`
default). `TimeoutError` and all other exceptions raised by `report()` are
caught, logged at `WARNING`, and counted on the `taskq.error_reporter.failures`
counter — they never propagate to the consumer loop.

### NullErrorReporter (default)

When no `ErrorReporter` is registered, the worker uses `NullErrorReporter`,
whose `report()` is a no-op. This is the default out of the box — no error
reporting happens beyond OTel exception events and structured logs.

### Registering a custom reporter

Implement the Protocol and register it as a DI provider. The worker resolves
it from the `ProviderRegistry` at dispatch time:

```python
from taskq.di import ProviderRegistry, Scope
from taskq.obs.error_reporter import ErrorReporter
from taskq.backend._protocol import JobRow


class SentryErrorReporter:
    """Routes terminal failures to Sentry as breadcrumbs."""

    async def report(self, job: JobRow, exception: BaseException) -> None:
        # Send to Sentry, a webhook, an audit table, etc.
        # job carries the full final row: id, actor, status, error_class,
        # error_message, error_traceback, attempt, identity_key, ...
        # exception is the BaseException that caused the terminal failure.
        ...


registry = ProviderRegistry()
# Register the Protocol type with a pre-built instance:
registry.register_value(ErrorReporter, Scope.PROCESS, SentryErrorReporter())
```

The reporter is resolved through the standard DI scope chain (see
[Dependency Injection](dependency-injection.md)). Use `Scope.PROCESS` for
stateless reporters (most cases) or `Scope.LOOP` if the reporter holds a
connection that should be reused for the event-loop lifetime.

!!! note "Keep `report()` fast and resilient"
    `report()` runs on the worker's consume path. The library's
    `invoke_error_reporter` wrapper enforces a 3-second `asyncio.wait_for`
    timeout (the `error_reporter_timeout` default) and catches all exceptions,
    so a hanging or crashing reporter cannot block the terminal-state write
    indefinitely or crash the worker. Even so, keep external calls short and
    catch internal exceptions — a reporter that consistently times out delays
    terminal writes by up to 3 seconds per failure and increments the
    `taskq.error_reporter.failures` counter on each miss.

### The `taskq.error_reporter.failures` metric

When a registered `ErrorReporter.report()` itself raises an exception, the
worker catches it, logs a warning, and increments the
`taskq.error_reporter.failures` counter:

| Metric name | Unit | Attributes | Description | Conditional? |
|---|---|---|---|---|
| `taskq.error_reporter.failures` | `1` | `reporter_type` | `ErrorReporter` invocation failures. | yes |

The `reporter_type` attribute is the class name of the reporter that raised
(e.g. `"SentryErrorReporter"`, via `type(reporter).__name__`).
`NullErrorReporter` never raises, so this counter stays at zero unless a
custom reporter is installed and failing.

---

## Related documentation

- [actors.md](actors.md) — `@actor` decorator, `JobContext`, `ctx.log`, `ctx.span`
- [workers.md](workers.md) — worker lifecycle, `WorkerSettings`, pool configuration
- [../api-reference/testing.md](../api-reference/testing.md) — test fixtures, `setup_tracer`, `setup_meter`
- [cancellation.md](cancellation.md) — cancel phases, `cancel_phase_change` log events
