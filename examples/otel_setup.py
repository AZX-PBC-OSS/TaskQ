# Observability example — OTel setup with Jaeger or any OTLP collector.
#
# This script demonstrates how to wire OpenTelemetry exporters for TaskQ
# workers and clients. TaskQ emits spans and metrics via the OTel API
# (opentelemetry-api, a core dependency). The SDK and exporter require
# the [otel] extra:
#
#   uv add "taskq-py[otel]"
#
# Then point the standard OTel environment variables at your collector:
#
#   export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
#   export OTEL_SERVICE_NAME=my-app-worker
#   export OTEL_RESOURCE_ATTRIBUTES="deployment.environment=production"
#
# TaskQ does NOT override any OTel environment variables. The setup below
# is the standard OTel SDK initialization — it works identically for
# Jaeger, Grafana Tempo, Datadog, Sentry, Azure Monitor, etc.
#
# Quick local Jaeger setup:
#
#   docker run -d -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one:latest
#
# Then open http://localhost:16686 to view traces.

import asyncio
import os

from examples.actors.basic import CounterPayload, counter
from taskq import TaskQ
from taskq.settings import TaskQSettings


def setup_otel() -> None:
    """Initialize the OTel SDK with OTLP exporter.

    This is standard OTel boilerplate — nothing TaskQ-specific here.
    TaskQ's spans and metrics flow through automatically once the SDK
    is configured. The worker calls this internally; for client-side
    tracing (e.g. enqueue spans), call this before enqueuing.
    """
    from opentelemetry import metrics, trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import (
        PeriodicExportingMetricReader,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    service_name = os.environ.get("OTEL_SERVICE_NAME", "taskq-example")
    resource = Resource.create(
        {
            "service.name": service_name,
            "deployment.environment": os.environ.get(
                "OTEL_RESOURCE_ATTRIBUTES", "development"
            ).split("=")[-1]
            if "=" in os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
            else "development",
        }
    )

    # Traces
    provider = TracerProvider(resource=resource)
    otlp_exporter = OTLPSpanExporter(
        endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    )
    provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
    trace.set_tracer_provider(provider)

    # Metrics (optional — for local dev you can use InMemoryMetricReader)
    metric_reader = PeriodicExportingMetricReader(
        OTLPSpanExporter(
            endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
        )
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(meter_provider)


async def main() -> None:
    """Enqueue a job with OTel tracing enabled and wait for it.

    The producer span (from enqueue) and the consumer span (from the
    worker) are linked via trace_id/span_id stored on the job row.
    View the linked traces in Jaeger by searching for the job's trace_id.
    """
    setup_otel()

    settings = TaskQSettings.load()
    async with TaskQ(dsn=str(settings.pg_dsn), schema=settings.schema_name) as tq:
        handle = await tq.enqueue(
            counter,
            CounterPayload(n=5),
            tags=["otel-demo"],
        )
        print(f"enqueued job {handle.job_id} — trace it in Jaeger")
        print(
            f"  http://localhost:16686/search?service={os.environ.get('OTEL_SERVICE_NAME', 'taskq-example')}"
        )

        try:
            await handle.wait(timeout=30.0)
            print(f"job {handle.job_id} succeeded")
        except TimeoutError:
            print("job did not finish within 30s (is a worker running?)")


if __name__ == "__main__":
    asyncio.run(main())
