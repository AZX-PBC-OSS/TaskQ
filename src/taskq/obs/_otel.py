"""OpenTelemetry tracer, meter, and metric helpers.

Provides safe, no-raise wrappers around OTel API calls so that observability
failures never propagate to user or actor code.  All metric instruments
are module-level singletons created at import time from the global meter provider.
"""

import contextlib
import importlib.metadata
from collections.abc import Generator, Iterable, Sequence
from typing import Literal

import structlog
from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.trace import Span, StatusCode, Tracer
from opentelemetry.util.types import Attributes

from taskq.obs._redact_exc import record_exception_safe, safe_exception_message

INSTRUMENTATION_NAME: str = "taskq"

type ConsumedOutcome = Literal["succeeded", "failed", "cancelled", "abandoned"]

__all__ = [
    "INSTRUMENTATION_NAME",
    "ConsumedOutcome",
    "get_meter",
    "get_tracer",
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
    "safe_start_span",
    "set_otel_enabled",
    "update_disabled_schedules_count",
    "update_heartbeat_consecutive_failures",
    "update_queue_depth_cache",
    "update_reservation_slots_cache",
]

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.obs._otel")

_otel_enabled: bool = True

_NOOP_SPAN_CONTEXT = trace.INVALID_SPAN_CONTEXT


def set_otel_enabled(enabled: bool) -> None:
    """Set the module-level OTel enabled flag.

    Called by worker startup code after loading ``WorkerSettings`` so that
    all safe helpers check the flag without requiring a ``WorkerSettings``
    import at every call site. Avoids circular imports (modules like
    ``dispatch.py`` import from ``obs`` and should not import from
    ``settings.py`` in a circular path).
    """
    global _otel_enabled
    _otel_enabled = enabled


def _version() -> str:
    try:
        return importlib.metadata.version("taskq-py")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


def get_tracer() -> Tracer:
    """Return the library's tracer. Honors any globally-configured provider."""
    return trace.get_tracer(INSTRUMENTATION_NAME, _version())


def get_meter() -> Meter:
    """Return the library's meter. Honors any globally-configured provider."""
    return metrics.get_meter(INSTRUMENTATION_NAME, _version())


def _record_scrubbed_error(span: Span, exc: BaseException) -> None:
    """Give *span* a scrubbed error signal for whichever half the call site left unset.

    Why: :func:`safe_start_span` turns the SDK's automatic exception handling
    OFF (see there), so a call site that does not handle the exception itself
    -- the ``attempt.N`` span in ``worker/_consumer.py``, which wraps user job
    code -- would otherwise export with no error signal at all. Suppressing a
    leak must not cost the signal.

    Each half is supplied only when missing, so the call sites that already
    scrub (``dispatch_batch``, ``cron fire``) do not get a duplicate event or
    have their description rewritten, while ``enqueue_span`` -- which marks the
    span ERROR but records no event -- still gets the scrubbed exception text
    it needs to stay diagnostic.
    """
    try:
        status_code = getattr(getattr(span, "status", None), "status_code", None)
        if status_code is not StatusCode.ERROR:
            span.set_status(StatusCode.ERROR, safe_exception_message(exc))
        events: Iterable[object] = getattr(span, "events", ())
        if not any(getattr(event, "name", None) == "exception" for event in events):
            record_exception_safe(span, exc)
    except Exception:
        _log.warning("otel-span-error-record-failed", span_name=getattr(span, "name", ""))


@contextlib.contextmanager
def safe_start_span(
    name: str,
    *,
    kind: trace.SpanKind | None = None,
    attributes: Attributes = None,
    links: Sequence[trace.Link] | None = None,
    new_root: bool = False,
) -> Generator[Span, None, None]:
    """Start a span safely — never propagates exceptions from OTel API calls.

    Checks ``_otel_enabled`` first; when ``False``, yields a no-op
    ``NonRecordingSpan``. When ``True``, delegates to
    ``get_tracer().start_as_current_span`` with a ``try/except`` around
    span *creation* only. Exceptions from code inside the ``with`` block
    propagate normally — only OTel API failures (misconfiguration,
    exporter unavailability) are suppressed.

    When ``new_root=True``, passes an empty ``Context()`` so the span
    has no parent — it is a root span linked (not parented) to the
    ambient trace. This satisfies the "linked, not parented"
    requirement for PRODUCER spans in the cron loop.

    The SDK's own exception handling is switched OFF and replaced by
    :func:`_record_scrubbed_error` — see the comment at the call below.
    """
    if not _otel_enabled:
        yield trace.NonRecordingSpan(_NOOP_SPAN_CONTEXT)
        return

    ctx: Context | None = Context() if new_root else None

    try:
        span_cm = get_tracer().start_as_current_span(
            name,
            context=ctx,
            kind=kind if kind is not None else trace.SpanKind.INTERNAL,
            attributes=attributes,
            links=links,
            # Why: both default to True, and the SDK derives its event and its
            # status description from the RAW ``str(exc)`` with no hook to
            # override it. At every call site that scrubs and re-raises, that
            # emitted a SECOND, unscrubbed ``exception`` event and overwrote
            # the scrubbed status description -- shipping the Postgres DETAIL
            # row values (idempotency_key / identity_key / fairness_key, all
            # caller-supplied) straight to the telemetry backend, undoing
            # ``_redact_exc`` entirely. No source-level guard can see this:
            # the leaking call is made by the SDK, not by TaskQ.
            record_exception=False,
            set_status_on_exception=False,
        )
    except Exception:
        _log.warning("otel-span-creation-failed", span_name=name)
        yield trace.NonRecordingSpan(_NOOP_SPAN_CONTEXT)
        return

    with span_cm as span:
        try:
            yield span
        except Exception as exc:
            # ``Exception``, not ``BaseException``: mirrors what the SDK's own
            # ``use_span`` catches, so cancellation semantics are unchanged.
            _record_scrubbed_error(span, exc)
            raise


def record_cancel_requested() -> None:
    """Bump the cancel-requested counter.
    This counter is unconditional:
    incremented once per ``JobsClient.cancel()`` call regardless of
    ``cancellation_initiated`` outcome.
    """
    try:
        _cancellation_requested.add(1)
    except Exception:
        _log.warning("otel-metric-record-failed", instrument_name="taskq.cancellation.requested")


_cancellation_requested = get_meter().create_counter("taskq.cancellation.requested")

_backpressure_errors = get_meter().create_counter(
    "taskq.backpressure.errors",
    description=(
        "Synchronous backpressure signals raised at enqueue. "
        "Attributes: actor (registered actor name, bounded cardinality), "
        "kind ('max_pending' | future variants)."
    ),
)


def record_backpressure_error(actor: str, *, kind: str = "max_pending") -> None:
    """Bump the backpressure.errors counter.

    Unconditional (not gated by ``_otel_enabled``): backpressure errors are
    safety-critical signals that must be counted even when OTel is disabled,
    so operators always have visibility into enqueue rejections.
    """
    try:
        _backpressure_errors.add(1, {"actor": actor, "kind": kind})
    except Exception:
        _log.warning("otel-metric-record-failed", instrument_name="taskq.backpressure.errors")


_capacity_refresh_failures = get_meter().create_counter(
    "taskq.backpressure.capacity_refresh_failures",
    description=(
        "Failed refreshes of the enqueue-side actor_config capacity cache. "
        "Attributes: degraded ('stale_snapshot' when a previous snapshot is "
        "still being served, 'no_snapshot' when the cache never loaded and "
        "every enqueue is falling back to the @actor literal)."
    ),
    unit="1",
)


def record_capacity_refresh_failure(*, has_snapshot: bool) -> None:
    """Count a failed capacity-cache refresh.

    The cache fails OPEN by design -- it keeps the last snapshot, or falls back
    to the ``@actor`` literal, and stamps ``refreshed_at`` so a sick backend is
    not re-queried on every enqueue. That reasoning is sound; the problem was
    that a load-shedding gate which relaxes precisely when the backend is
    degraded had no signal an operator could alert on, only a warning log.

    ``has_snapshot=False`` is the materially worse case: the first refresh at
    process start failed, so there is no stored data at all and every enqueue
    enforces the code literal rather than the operator's tightened
    ``max_pending`` -- for a full TTL at a time, indefinitely while the backend
    stays sick.

    Unconditional (not gated by ``_otel_enabled``) for the same reason as
    ``record_backpressure_error``: this is a safety-critical signal.
    """
    try:
        _capacity_refresh_failures.add(
            1, {"degraded": "stale_snapshot" if has_snapshot else "no_snapshot"}
        )
    except Exception:
        _log.warning(
            "otel-metric-record-failed",
            instrument_name="taskq.backpressure.capacity_refresh_failures",
        )


_deadline_exceeded_sweep_jobs_failed = get_meter().create_counter(
    "taskq.deadline_exceeded_sweep.jobs_failed",
    description="Jobs transitioned to failed by the deadline-exceeded sweep, labeled by actor.",
    unit="1",
)


def record_deadline_exceeded_swept(actor: str, count: int = 1) -> None:
    """Bump the deadline-exceeded sweep counter.

    Unconditional (not gated by ``_otel_enabled``): deadline-exceeded sweeps
    indicate jobs that violated their execution budget — a correctness signal
    that must be counted even when OTel is disabled, so operators always have
    visibility into sweep activity.
    """
    try:
        _deadline_exceeded_sweep_jobs_failed.add(count, {"actor": actor})
    except Exception:
        _log.warning(
            "otel-metric-record-failed", instrument_name="taskq.deadline_exceeded_sweep.jobs_failed"
        )


_published_messages = get_meter().create_counter(
    "messaging.client.published.messages",
    description="Count of jobs enqueued, labeled by actor and queue.",
    unit="1",
)


def record_published_message(actor: str, queue: str) -> None:
    """Bump the published-messages counter.

    Called after successful enqueue, outside the PRODUCER span body,
    to ensure sampling independence.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _published_messages.add(1, {"actor": actor, "queue": queue})


_dispatch_duration = get_meter().create_histogram(
    "taskq.dispatch.duration",
    description="Dispatch query latency (SQL execution only), labeled by queue.",
    unit="s",
)


def record_dispatch_duration(queue: str, elapsed: float) -> None:
    """Record dispatch query latency on the histogram.

    Called outside the ``dispatch`` span body for sampling independence.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _dispatch_duration.record(elapsed, {"queue": queue})


_consumed_messages = get_meter().create_counter(
    "messaging.client.consumed.messages",
    description="Count of jobs consumed, labeled by actor, queue, and outcome.",
    unit="1",
)


def record_consumed_message(actor: str, queue: str, *, outcome: ConsumedOutcome) -> None:
    """Bump the consumed-messages counter.

    Called after job completion, outside the CONSUMER span body,
    to ensure sampling independence.
    Respects ``_otel_enabled`` — no-op when False.

    ``outcome`` is constrained to the semconv-specified valid set
    ``{succeeded, failed, cancelled, abandoned}``.
    The consumer-path ``AttemptOutcome`` includes ``"scheduled"`` for
    snooze/retry/reservation-denial; callers must map that to
    ``"abandoned"`` before calling (the consumer released the job back
    to the queue without completing it).
    """
    if not _otel_enabled:
        return
    _consumed_messages.add(1, {"actor": actor, "queue": queue, "outcome": outcome})


_process_duration = get_meter().create_histogram(
    "messaging.process.duration",
    description="Job execution duration, labeled by actor and queue.",
    unit="s",
)


def record_process_duration(actor: str, queue: str, elapsed: float) -> None:
    """Record job execution duration on the histogram.

    Called outside the CONSUMER span body for sampling independence.
    Respects ``_otel_enabled`` — no-op when False.
    Custom buckets are the operator's responsibility via SDK Views.
    """
    if not _otel_enabled:
        return
    _process_duration.record(elapsed, {"actor": actor, "queue": queue})


#: Why identity values are not metric dimensions
#: ------------------------------------------------
#: ``worker_id`` is a fresh UUID per worker PROCESS. Azure Monitor counts every
#: unique (metric, dimension key, dimension value) combination seen in 12 hours
#: as an active time series, caps a subscription at 50,000 of them per region,
#: and advises staying under ~100 values per dimension. On Kubernetes every
#: deploy, restart and autoscale event mints a new ``worker_id``, so the
#: dimension grows without bound -- and the failure mode is throttled ingestion
#: across EVERY custom metric in the subscription, with no backfill of what was
#: dropped. Not repairable after the fact, so it is not carried at all.
#:
#: The signal is not lost: per-worker attribution lives on spans and log lines,
#: where cardinality is free (``worker_id`` is bound via contextvars onto every
#: log line; ``taskq.worker_id`` is a cron-fire span attribute).
#:
#: ``schedule_id`` is NOT in the same class and stays a dimension on
#: ``taskq.cron.consecutive_failures``: schedules are a bounded set an operator
#: creates by hand, not a per-process UUID, and ``cron_auto_disable_threshold``
#: is evaluated per schedule -- summed across schedules the metric no longer
#: matches the mechanism it exists to monitor.
#:
#: The ``worker_id`` parameters below are kept: they are part of the published
#: ``taskq.obs`` surface, and dropping them would be a breaking change for a
#: value the callers already have to hand.

_lock_expires_in_seconds = get_meter().create_histogram(
    "taskq.lock.expires_in_seconds",
    description="Remaining TTL at each heartbeat renewal. No dimensions.",
    unit="s",
    explicit_bucket_boundaries_advisory=(0, 5, 10, 15, 20, 30, 45, 60),
)


def record_lock_expires_in_seconds(worker_id: str, remaining_ttl: float) -> None:
    """Record remaining lock TTL on the histogram.

    Called in heartbeat.py at each successful heartbeat renewal.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    _lock_expires_in_seconds.record(remaining_ttl)


_heartbeat_misses = get_meter().create_counter(
    "taskq.heartbeat.misses",
    description="Heartbeat renewal failures.",
    unit="1",
)


def record_heartbeat_miss(worker_id: str) -> None:
    """Bump the heartbeat.misses counter.

    Called in heartbeat.py on each heartbeat renewal failure.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    _heartbeat_misses.add(1)


_queue_depth_cache: dict[str, int] = {}


def update_queue_depth_cache(data: dict[str, int]) -> None:
    """Replace the queue-depth cache with fresh data from the leader's PG query.

    Called by the background async task in the leader loop every 15s.
    The synchronous gauge callback reads from this cache.
    """
    global _queue_depth_cache
    _queue_depth_cache = dict(data)


def _observe_queue_depth(options: CallbackOptions) -> Iterable[Observation]:
    for queue, depth in _queue_depth_cache.items():
        yield Observation(depth, {"queue": queue})


_queue_depth_gauge = get_meter().create_observable_gauge(
    name="taskq.queue.depth",
    description="Number of pending/scheduled jobs per queue, sampled by the leader.",
    unit="1",
    callbacks=[_observe_queue_depth],
)


_stranded_jobs_cache: dict[str, int] = {}


def update_stranded_jobs_cache(data: dict[str, int]) -> None:
    """Replace the stranded-jobs cache with fresh data from the leader's query.

    Stranded jobs are pending/scheduled jobs whose actor has no `actor_config`
    row, which makes them permanently undispatchable: the dispatch CTE derives
    its candidates from `per_actor_capacity`, which is `FROM actor_config`.

    This gauge exists because the detector previously emitted a log line and
    nothing else, exactly once per actor per process lifetime -- so the
    condition was invisible in metrics and its only trace was a single WARN at
    onset, which is the moment nobody is looking. An empty dict clears the
    gauge, so recovery is visible too.
    """
    global _stranded_jobs_cache
    _stranded_jobs_cache = dict(data)


def _observe_stranded_jobs(options: CallbackOptions) -> Iterable[Observation]:
    for actor, count in _stranded_jobs_cache.items():
        yield Observation(count, {"actor": actor})


_stranded_jobs_gauge = get_meter().create_observable_gauge(
    name="taskq.jobs.stranded",
    description=(
        "Pending/scheduled jobs whose actor has no actor_config row and which "
        "therefore can never be dispatched, sampled by the leader."
    ),
    unit="1",
    callbacks=[_observe_stranded_jobs],
)


_reservation_slots_cache: dict[str, int] = {}


def update_reservation_slots_cache(data: dict[str, int]) -> None:
    """Replace the reservation-slots cache with fresh data from the leader's PG query.

    Called by the background async task in the leader loop every 15s.
    The synchronous gauge callback reads from this cache.
    """
    global _reservation_slots_cache
    _reservation_slots_cache = dict(data)


def _observe_reservation_slots(options: CallbackOptions) -> Iterable[Observation]:
    for bucket, count in _reservation_slots_cache.items():
        yield Observation(count, {"bucket": bucket})


_reservation_slots_gauge = get_meter().create_observable_gauge(
    name="taskq.reservation.slots_used",
    description="In-use reservation slots per bucket, sampled by the leader.",
    unit="1",
    callbacks=[_observe_reservation_slots],
)


_progress_publish_failures = get_meter().create_counter(
    "taskq.progress.publish_failures",
    description=(
        "Redis publish failures for progress fanout. "
        "Attributes: channel ('per_job' | 'global'), error_type (exception class name)."
    ),
    unit="1",
)


def record_progress_publish_failure(channel: str, error_type: str) -> None:
    """Bump the progress.publish_failures counter.

    ``channel`` must be ``'per_job'`` or ``'global'`` — bounded cardinality.
    ``error_type`` is the exception class name (e.g. ``'ResponseError'``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _progress_publish_failures.add(1, {"channel": channel, "error_type": error_type})


_ratelimit_refund_failures = get_meter().create_counter(
    "taskq.ratelimit.refund_failures",
    description="Rate-limit refund/rollback failures, labeled by bucket and backend.",
    unit="1",
)


def record_ratelimit_refund_failure(bucket: str, backend: str) -> None:
    """Bump the ratelimit.refund_failures counter.

    Called at the rate-limit refund failure catch site.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _ratelimit_refund_failures.add(1, {"bucket": bucket, "backend": backend})


_leader_election_attempts = get_meter().create_counter(
    "taskq.leader.election_attempts",
    description="Leader election attempts. No dimensions.",
    unit="1",
)

_leader_election_failures = get_meter().create_counter(
    "taskq.leader.election_failures",
    description="Election attempts that did not win the lock. No dimensions.",
    unit="1",
)


def record_election_attempt(worker_id: str, *, won: bool) -> None:
    """Record a leader election attempt.

    Always increments ``election_attempts``; increments ``election_failures``
    only when the attempt did not win the lock.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    _leader_election_attempts.add(1)
    if not won:
        _leader_election_failures.add(1)


_cron_consecutive_failures = get_meter().create_up_down_counter(
    "taskq.cron.consecutive_failures",
    description="Consecutive cron execution failures, labeled by schedule_id.",
    unit="1",
)


_cron_lock_contention = get_meter().create_counter(
    "taskq.cron.lock_contention",
    unit="1",
    description=(
        "Cron ticks that returned without firing because another session held "
        "the cron advisory lock."
    ),
)


def record_cron_lock_contention(worker_id: str) -> None:
    """Count a cron tick skipped because the advisory lock was held.

    A steady low rate is the benign leader-handover overlap. A rate equal to
    the tick rate, sustained, means cron is not running anywhere: the lock is
    transaction-scoped and releases on COMMIT/ROLLBACK, which never happens if
    the holding session was partitioned without a FIN. Before this counter the
    two were indistinguishable, because the contended branch returned in
    silence.
    Respects ``_otel_enabled`` -- no-op when False.
    """
    if not _otel_enabled:
        return
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    _cron_lock_contention.add(1)


def record_cron_failure(schedule_id: str, delta: int) -> None:
    """Record a cron failure delta on the UpDownCounter.

    On failure, callers add ``+1`` per failure. On success, callers add
    ``-current_count`` for that schedule to reset the counter to zero —
    a simple ``add(-1)`` would leave a non-zero cumulative value if
    there were multiple consecutive failures.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _cron_consecutive_failures.add(delta, {"schedule_id": schedule_id})


_disabled_schedules_count: int = 0


def update_disabled_schedules_count(count: int) -> None:
    """Update the module-level disabled-schedules count.

    Called by the leader's schedule management code when schedules are
    disabled or re-enabled.
    """
    global _disabled_schedules_count
    _disabled_schedules_count = count


def _observe_disabled_schedules(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(_disabled_schedules_count)


_disabled_schedules_gauge = get_meter().create_observable_gauge(
    name="taskq.cron.disabled_schedules",
    description="Currently disabled schedules.",
    unit="1",
    callbacks=[_observe_disabled_schedules],
)


_pruned_jobs = get_meter().create_counter(
    "taskq.pruned.jobs",
    description="Jobs removed by the prune sweep, labeled by actor and status.",
    unit="1",
)


def record_pruned_jobs(actor: str, status: str, count: int = 1) -> None:
    """Bump the pruned.jobs counter.

    Called at the prune sweep call site in leader.py.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _pruned_jobs.add(count, {"actor": actor, "status": status})


_archived_jobs = get_meter().create_counter(
    "taskq.archived.jobs",
    description="Jobs archived (moved to jobs_archive) by the prune sweep, labeled by status.",
    unit="1",
)


def record_archived_jobs(status: str, count: int = 1) -> None:
    """Bump the archived.jobs counter.

    Called alongside record_pruned_jobs at the prune sweep call site in leader.py.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _archived_jobs.add(count, {"status": status})


_expired_archive_jobs = get_meter().create_counter(
    "taskq.expired_archive.jobs",
    description="Archive rows hard-deleted by the archive expiry sweep, labeled by status.",
    unit="1",
)


def record_expired_archive_jobs(status: str, count: int = 1) -> None:
    """Bump the expired_archive.jobs counter.

    Called at the archive expiry sweep call site in leader.py.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _expired_archive_jobs.add(count, {"status": status})


#: One process runs one worker, so this is a scalar, not a per-``worker_id``
#: map: keying it by worker id produced one Observation -- one time series --
#: per id on every scrape. See the cardinality note above.
_heartbeat_consecutive_failures_count: int = 0


def update_heartbeat_consecutive_failures(worker_id: str, count: int) -> None:
    """Update the module-level heartbeat consecutive-failures value.

    Called by the heartbeat loop after each tick so the synchronous
    gauge callback can read the latest value on scrape.
    """
    global _heartbeat_consecutive_failures_count
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    _heartbeat_consecutive_failures_count = count


def _observe_heartbeat_consecutive_failures(
    options: CallbackOptions,
) -> Iterable[Observation]:
    yield Observation(_heartbeat_consecutive_failures_count)


_heartbeat_consecutive_failures_gauge = get_meter().create_observable_gauge(
    name="taskq.heartbeat.consecutive_failures",
    description="Consecutive heartbeat tick failures for this worker (sample-on-scrape).",
    unit="1",
    callbacks=[_observe_heartbeat_consecutive_failures],
)


_error_reporter_failures = get_meter().create_counter(
    "taskq.error_reporter.failures",
    description=(
        "ErrorReporter.report() failures, labeled by reporter_type. "
        "A failing reporter never crashes the worker."
    ),
    unit="1",
)


def record_error_reporter_failure(reporter_type: str) -> None:
    """Bump the error_reporter.failures counter.

    Called at the error-reporter catch site when ``report()`` raises.
    ``reporter_type`` is the exception-safe class name of the reporter
    instance (bounded cardinality — one per registered implementation).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _error_reporter_failures.add(1, {"reporter_type": reporter_type})
