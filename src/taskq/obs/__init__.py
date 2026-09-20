"""Vendor-neutral observability bootstrap.

The library never imports vendor SDKs (Sentry, Datadog, PostHog, App Insights).
Instead, it emits OpenTelemetry spans and metrics and lets operators wire any
OTLP-compatible backend by configuring environment variables (or by passing an
already-configured ``TracerProvider`` / ``MeterProvider``).

Logs are NOT an OTel signal here: the library attaches no ``LoggingHandler``
and emits no log records through a ``LoggerProvider``. :func:`setup_logging`
configures structlog over the stdlib ``logging`` root logger, so log lines
reach a telemetry backend only if the operator has attached a handler to that
root logger themselves -- which is what ``configure_azure_monitor()`` does.
That is incidental wiring, not an emission path this library owns, and it is
why exception text has to be scrubbed inside the processor chain rather than
at an exporter (see ``_redact_exc``). (:func:`configure_exporters` lets the
SDK's configurator install a ``LoggerProvider`` alongside the tracer and
meter providers, as ``opentelemetry-instrument`` would; nothing in the
library writes to it.)

Under the ``taskq worker`` CLI the standard ``OTEL_*`` exporter variables are
enough: :func:`configure_exporters` installs SDK providers from them at
startup when the ``[otel]`` extra is installed and no provider is set yet
(see ``_exporter``). Embedding applications call it themselves or configure
the SDK directly.

Common deployment shapes:

- **Datadog Agent**, accepts OTLP on ``localhost:4317``. Set
  ``OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317``.
- **Sentry**, Sentry Spotlight / Sentry OTel ingest, ditto.
- **App Insights**, set ``OTEL_EXPORTER_OTLP_ENDPOINT`` to the Azure Monitor
  connection string-derived OTLP URL (typically via the App Insights agent).
- **PostHog**, currently via PostHog Cloud OTLP endpoint, same env var.

For error reporting that doesn't fit OTel exception events (e.g., DLQ routing
to Sentry), users implement the ``ErrorReporter`` Protocol
as a DI provider, vendor-neutral and added with the observability surface.

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
needed, the attribute values are correct by construction.
"""

from taskq.obs._exporter import (
    ExporterWiring,
    OtelExporterConfigurationError,
    configure_exporters,
)
from taskq.obs._otel import (
    INSTRUMENTATION_NAME,
    ConsumedOutcome,
    StrandedReason,
    TimeoutKind,
    get_meter,
    get_tracer,
    otel_enabled,
    reconcile_cron_failures,
    record_archived_jobs,
    record_attempt_failure,
    record_backpressure_error,
    record_cancel_requested,
    record_capacity_refresh_failure,
    record_consumed_message,
    record_cron_budget_deferral,
    record_cron_failure,
    record_cron_lock_contention,
    record_deadline_exceeded_swept,
    record_dispatch_duration,
    record_dispatch_failure,
    record_election_attempt,
    record_enqueue_dedup,
    record_error_reporter_failure,
    record_expired_archive_jobs,
    record_heartbeat_miss,
    record_job_abandoned,
    record_job_interrupted,
    record_job_interrupted_noop,
    record_job_timeout,
    record_leader_lease_expires_in_seconds,
    record_lock_contention,
    record_lock_expires_in_seconds,
    record_loop_stall_attribution,
    record_pool_acquire_duration,
    record_process_duration,
    record_progress_flush_failure,
    record_progress_publish_failure,
    record_pruned_jobs,
    record_published_message,
    record_queue_wait,
    record_ratelimit_acquire_dependency_failure,
    record_ratelimit_denial,
    record_ratelimit_refund_failure,
    record_reclaimed_jobs,
    record_reservation_denial,
    record_reservation_reclaim_drain_duration,
    record_reservation_reclaim_drain_failure,
    record_reservation_reclaim_drain_rows,
    record_reservation_reclaim_heal_failure,
    record_slot_pool_acquire_failure,
    record_sub_enqueue_failure,
    record_sweep_batch_size,
    record_sweep_batch_size_configured,
    record_sweep_success,
    record_sweep_timeout,
    safe_start_span,
    set_otel_enabled,
    set_slot_pool_occupancy_source,
    set_worker_capacity_source,
    update_actor_backlog_cache,
    update_actor_oldest_pending_age_cache,
    update_actor_oldest_running_age_cache,
    update_disabled_schedules_count,
    update_heartbeat_consecutive_failures,
    update_jobs_by_status_cache,
    update_jobs_running_cache,
    update_keyed_reclaim_pending,
    update_oldest_due_age_cache,
    update_queue_depth_cache,
    update_queue_live_workers_cache,
    update_reservation_slots_cache,
    update_running_lease_expired_cache,
    update_scheduled_count_cache,
    update_stranded_jobs_cache,
    update_sweep_batch_size_cache,
)
from taskq.obs._redact_exc import (
    ExceptionText,
    ScrubbedText,
    record_exception_safe,
    record_exception_text,
    render_exception,
    safe_exception_message,
    set_exception_message_max_chars,
    set_exception_redaction_enabled,
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
    "ExceptionText",
    "ExporterWiring",
    "NullErrorReporter",
    "OtelExporterConfigurationError",
    "ScrubbedText",
    "StrandedReason",
    "TimeoutKind",
    "bind_job_context",
    "configure_exporters",
    "get_logger",
    "get_meter",
    "get_tracer",
    "invoke_error_reporter",
    "log_cancel_phase_change",
    "log_state_change",
    "otel_enabled",
    "reconcile_cron_failures",
    "record_archived_jobs",
    "record_attempt_failure",
    "record_backpressure_error",
    "record_cancel_requested",
    "record_capacity_refresh_failure",
    "record_consumed_message",
    "record_cron_budget_deferral",
    "record_cron_failure",
    "record_cron_lock_contention",
    "record_deadline_exceeded_swept",
    "record_dispatch_duration",
    "record_dispatch_failure",
    "record_election_attempt",
    "record_enqueue_dedup",
    "record_error_reporter_failure",
    "record_exception_safe",
    "record_exception_text",
    "record_expired_archive_jobs",
    "record_heartbeat_miss",
    "record_job_abandoned",
    "record_job_interrupted",
    "record_job_interrupted_noop",
    "record_job_timeout",
    "record_leader_lease_expires_in_seconds",
    "record_lock_contention",
    "record_lock_expires_in_seconds",
    "record_loop_stall_attribution",
    "record_pool_acquire_duration",
    "record_process_duration",
    "record_progress_flush_failure",
    "record_progress_publish_failure",
    "record_pruned_jobs",
    "record_published_message",
    "record_queue_wait",
    "record_ratelimit_acquire_dependency_failure",
    "record_ratelimit_denial",
    "record_ratelimit_refund_failure",
    "record_reclaimed_jobs",
    "record_reservation_denial",
    "record_reservation_reclaim_drain_duration",
    "record_reservation_reclaim_drain_failure",
    "record_reservation_reclaim_drain_rows",
    "record_reservation_reclaim_heal_failure",
    "record_slot_pool_acquire_failure",
    "record_sub_enqueue_failure",
    "record_sweep_batch_size",
    "record_sweep_batch_size_configured",
    "record_sweep_success",
    "record_sweep_timeout",
    "redact_payload",
    "render_exception",
    "safe_exception_message",
    "safe_start_span",
    "set_exception_message_max_chars",
    "set_exception_redaction_enabled",
    "set_otel_enabled",
    "set_slot_pool_occupancy_source",
    "set_worker_capacity_source",
    "setup_logging",
    "update_actor_backlog_cache",
    "update_actor_oldest_pending_age_cache",
    "update_actor_oldest_running_age_cache",
    "update_disabled_schedules_count",
    "update_heartbeat_consecutive_failures",
    "update_jobs_by_status_cache",
    "update_jobs_running_cache",
    "update_keyed_reclaim_pending",
    "update_oldest_due_age_cache",
    "update_queue_depth_cache",
    "update_queue_live_workers_cache",
    "update_reservation_slots_cache",
    "update_running_lease_expired_cache",
    "update_scheduled_count_cache",
    "update_stranded_jobs_cache",
    "update_sweep_batch_size_cache",
]
