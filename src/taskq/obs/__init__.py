"""Vendor-neutral observability bootstrap.

The library never imports vendor SDKs (Sentry, Datadog, PostHog, App Insights).
Instead, it emits OpenTelemetry spans/metrics/logs and lets operators wire any
OTLP-compatible backend by configuring environment variables (or by passing an
already-configured ``TracerProvider`` / ``MeterProvider``).

Common deployment shapes:

- **Datadog Agent** — accepts OTLP on ``localhost:4317``. Set
  ``OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317``.
- **Sentry** — Sentry Spotlight / Sentry OTel ingest, ditto.
- **App Insights** — set ``OTEL_EXPORTER_OTLP_ENDPOINT`` to the Azure Monitor
  connection string-derived OTLP URL (typically via the App Insights agent).
- **PostHog** — currently via PostHog Cloud OTLP endpoint, same env var.

For error reporting that doesn't fit OTel exception events (e.g., DLQ routing
to Sentry), users implement the ``ErrorReporter`` Protocol
as a DI provider — vendor-neutral and added with the observability surface.

The library depends only on ``opentelemetry-api`` at runtime. Operators
who want to configure providers programmatically (or use the in-process
testing utilities in ``taskq.testing``) install the ``[otel]`` extra::

    pip install "taskq-py[otel]"

which pulls in ``opentelemetry-sdk`` and ``opentelemetry-exporter-otlp``.
For Prometheus scrapes, use the ``[prometheus]`` extra instead.

Semconv compliance: the library uses spec-compliant messaging semconv
attribute names (``messaging.operation.type=publish``,
``messaging.operation.type=process``, ``messaging.consumer.group.name``, etc.)
so that operators who set ``OTEL_SEMCONV_STABILITY_OPT_IN=messaging`` get
consistent behavior. No runtime conditional branching on this env var is
needed — the attribute values are correct by construction.
"""

from taskq.obs._otel import (
    INSTRUMENTATION_NAME,
    ConsumedOutcome,
    get_meter,
    get_tracer,
    record_archived_jobs,
    record_backpressure_error,
    record_cancel_requested,
    record_consumed_message,
    record_cron_failure,
    record_deadline_exceeded_swept,
    record_dispatch_duration,
    record_election_attempt,
    record_error_reporter_failure,
    record_expired_archive_jobs,
    record_heartbeat_miss,
    record_lock_expires_in_seconds,
    record_process_duration,
    record_progress_publish_failure,
    record_pruned_jobs,
    record_published_message,
    record_ratelimit_refund_failure,
    safe_start_span,
    set_otel_enabled,
    update_disabled_schedules_count,
    update_heartbeat_consecutive_failures,
    update_queue_depth_cache,
    update_reservation_slots_cache,
)
from taskq.obs._structlog import (
    bind_job_context,
    get_logger,
    log_cancel_phase_change,
    log_state_change,
    redact_payload,
    setup_logging,
)
from taskq.obs.error_reporter import (
    ErrorReporter,
    ErrorReporterType,
    NullErrorReporter,
    invoke_error_reporter,
)

__all__ = [
    "INSTRUMENTATION_NAME",
    "ConsumedOutcome",
    "ErrorReporter",
    "ErrorReporterType",
    "NullErrorReporter",
    "bind_job_context",
    "get_logger",
    "get_meter",
    "get_tracer",
    "invoke_error_reporter",
    "log_cancel_phase_change",
    "log_state_change",
    "record_archived_jobs",
    "record_backpressure_error",
    "record_cancel_requested",
    "record_consumed_message",
    "record_cron_failure",
    "record_deadline_exceeded_swept",
    "record_dispatch_duration",
    "record_election_attempt",
    "record_error_reporter_failure",
    "record_expired_archive_jobs",
    "record_heartbeat_miss",
    "record_lock_expires_in_seconds",
    "record_process_duration",
    "record_progress_publish_failure",
    "record_pruned_jobs",
    "record_published_message",
    "record_ratelimit_refund_failure",
    "redact_payload",
    "safe_start_span",
    "set_otel_enabled",
    "setup_logging",
    "update_disabled_schedules_count",
    "update_heartbeat_consecutive_failures",
    "update_queue_depth_cache",
    "update_reservation_slots_cache",
]
