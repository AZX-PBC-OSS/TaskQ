"""OpenTelemetry tracer, meter, and metric helpers.

Provides safe, no-raise wrappers around OTel API calls so that observability
failures never propagate to user or actor code.  All metric instruments
are module-level singletons created at import time from the global meter provider.

Label cardinality contract
--------------------------

Metric dimensions are limited to values that are bounded by construction:

- ``actor``: the set of registered actor names (bounded by the code the
  user ships), carried as-is.
- ``sweep_name`` / ``lock`` / ``status`` / ``outcome``: closed enums, carried
  as-is.
- ``queue``: caller-supplied per enqueue and only charset-validated -- the
  one open-ended label on the job-side instruments.  The four job-side
  emitters (:func:`record_published_message`, :func:`record_dispatch_duration`,
  :func:`record_consumed_message`, :func:`record_process_duration`) bound it
  to the first ``_MAX_QUEUE_LABEL_VALUES`` distinct names a process sees;
  beyond the cap the label collapses to the fixed ``_other_`` value (see the
  cardinality note above :func:`_bounded_queue`).  Identity-like values
  (``worker_id``, ``job_id``) are never dimensions at all -- see the
  cardinality note above ``_lock_expires_in_seconds``.
"""

import contextlib
import functools
import importlib.metadata
import time
from collections.abc import Generator, Iterable, Sequence
from typing import Literal, Protocol

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


@functools.lru_cache(maxsize=1)
def _version() -> str:
    """Return the installed ``taskq-py`` version, resolved once per process.

    Why cached: ``importlib.metadata.version`` walks the site-packages
    metadata on every call (~300µs, benchmarks/ab_otel_hotspots.py), and
    :func:`get_tracer` runs per span and per enqueue -- the lookup alone
    was ~700µs of every job's telemetry tax.  The installed version cannot
    change while the process runs, so a single lookup is exact.
    """
    try:
        return importlib.metadata.version("taskq-py")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


_library_tracer: Tracer | None = None
"""The tracer :func:`get_tracer` resolves once and hands back (see there)."""


def get_tracer() -> Tracer:
    """Return the library's tracer. Honors any globally-configured provider.

    The tracer object is resolved on first use and memoized.  That is safe
    across the no-provider → real-provider transition: with no SDK set up,
    ``trace.get_tracer`` returns a ``ProxyTracer``, which re-checks the
    global provider on every span start and rebinds to the real one when an
    SDK registers later -- so memoization never pins the proxy/no-op
    behavior.
    """
    global _library_tracer
    if _library_tracer is None:
        _library_tracer = trace.get_tracer(INSTRUMENTATION_NAME, _version())
    return _library_tracer


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


#: Why the ``queue`` label is capped on the job-side instruments
#: ------------------------------------------------------------
#: Unlike ``actor`` (bounded by the registered actor set the user ships),
#: ``queue`` is caller-supplied per enqueue and only charset-validated
#: (``backend/_protocol.py``) -- nothing bounds it.  5,000 distinct queue
#: names minted 100k+ time series and 763ms scrapes in the cardinality
#: bench, and the failure mode is the Azure Monitor one described in the
#: ``worker_id`` note below: throttled ingestion across EVERY custom metric
#: in the subscription, not repairable after the fact.  So the four
#: job-side emitters admit the first ``_MAX_QUEUE_LABEL_VALUES`` distinct
#: names a process sees (the ~100-values-per-dimension ceiling Azure's
#: guidance sets) and collapse everything past the cap onto the fixed
#: ``_other_`` value.  The cap never evicts: admitted names keep their own
#: series for the life of the process, so steady-state traffic on real
#: queues is unaffected and the series count is hard-bounded at cap + 1.
#: Per-queue attribution is not lost -- the queue name rides on the
#: enqueue/dispatch/consume span attributes and log lines, where
#: cardinality is free.
#:
#: The admission check runs on the emitting (event-loop) thread only and
#: is never iterated by the SDK reader thread, so the rebind discipline
#: below does not apply; the worst a racing thread could do is admit one
#: name past the cap, which stays bounded.

_MAX_QUEUE_LABEL_VALUES: int = 100
_QUEUE_LABEL_OVERFLOW: str = "_other_"

_queue_label_values: set[str] = set()


def _bounded_queue(queue: str) -> str:
    """Return *queue*, or the fixed overflow label once the cap is reached.

    See the cardinality note above.
    """
    if queue in _queue_label_values:
        return queue
    if len(_queue_label_values) >= _MAX_QUEUE_LABEL_VALUES:
        return _QUEUE_LABEL_OVERFLOW
    _queue_label_values.add(queue)
    return queue


_published_messages = get_meter().create_counter(
    "messaging.client.published.messages",
    description=(
        "Count of jobs enqueued, labeled by actor and queue "
        "(queue capped at the first _MAX_QUEUE_LABEL_VALUES distinct "
        "names per process; overflow collapses to '_other_')."
    ),
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
    _published_messages.add(1, {"actor": actor, "queue": _bounded_queue(queue)})


_dispatch_duration = get_meter().create_histogram(
    "taskq.dispatch.duration",
    description=(
        "Dispatch query latency (SQL execution only), labeled by queue "
        "(capped -- see _bounded_queue)."
    ),
    unit="s",
)


def record_dispatch_duration(queue: str, elapsed: float) -> None:
    """Record dispatch query latency on the histogram.

    Called outside the ``dispatch`` span body for sampling independence.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _dispatch_duration.record(elapsed, {"queue": _bounded_queue(queue)})


_consumed_messages = get_meter().create_counter(
    "messaging.client.consumed.messages",
    description=(
        "Count of jobs consumed, labeled by actor, queue (capped -- see "
        "_bounded_queue), and outcome."
    ),
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
    _consumed_messages.add(1, {"actor": actor, "queue": _bounded_queue(queue), "outcome": outcome})


_process_duration = get_meter().create_histogram(
    "messaging.process.duration",
    description=(
        "Job execution duration, labeled by actor and queue (capped -- see _bounded_queue)."
    ),
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
    _process_duration.record(elapsed, {"actor": actor, "queue": _bounded_queue(queue)})


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


_slot_pool_acquire_failures = get_meter().create_counter(
    "taskq.worker.slot_pool.acquire_failures",
    description=(
        "Bounded acquires from the per-slot transaction pool that failed "
        "(timeout or connection error). An acquire failure is "
        "infrastructure, not a job outcome: the claimed job recovers by "
        "lock-lease expiry. No dimensions -- the pool name is in the "
        "instrument name and the per-occurrence job id stays in the log "
        "event."
    ),
    unit="1",
)


def record_slot_pool_acquire_failure() -> None:
    """Bump the worker.slot_pool.acquire_failures counter.

    Called from the exception branch of the bounded per-job acquire in
    ``taskq.worker.dispatch`` — never the success path, matching
    ``record_sweep_timeout``'s contract. A rate here is what separates
    one transient timeout from every transactional job on a worker
    failing to acquire.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _slot_pool_acquire_failures.add(1)


class _PoolOccupancySource(Protocol):
    """Structural slice of asyncpg.Pool the occupancy gauge reads.

    Keeps this observability leaf free of an asyncpg import: the only
    producers (slot-pool bootstrap and credential reload) pass real
    pools, which satisfy this shape structurally.
    """

    def get_size(self) -> int: ...

    def get_idle_size(self) -> int: ...


_slot_pool_occupancy_source: _PoolOccupancySource | None = None
"""The asyncpg pool the occupancy gauge reads — set at slot-pool open
and refreshed on credential-reload swaps."""


def set_slot_pool_occupancy_source(pool: _PoolOccupancySource | None) -> None:
    """Point the slot-pool occupancy gauge at *pool*.

    Called at slot-pool bootstrap open and whenever a credential reload
    swaps the pool, so the gauge always reads the live one. ``None``
    clears the source — the gauge reports nothing, matching the
    pool-not-open state.
    """

    global _slot_pool_occupancy_source
    _slot_pool_occupancy_source = pool


def _observe_slot_pool_occupancy(options: CallbackOptions) -> Iterable[Observation]:
    pool = _slot_pool_occupancy_source
    if pool is None:
        return
    try:
        # Why defensive: the source can be a pool that teardown has since
        # closed (the gauge outlives the swap/close notifications); a
        # collection read must never raise into the SDK's export path.
        in_use = pool.get_size() - pool.get_idle_size()
    except Exception:
        return
    yield Observation(in_use)


_slot_pool_occupancy_gauge = get_meter().create_observable_gauge(
    name="taskq.worker.slot_pool.connections_in_use",
    description=(
        "Connections of the per-slot transaction pool currently held by "
        "dispatching jobs. A pool pinned at its maximum for hours with "
        "zero acquire timeouts is healthy saturation, not health - this "
        "gauge is what makes that degradation visible below the "
        "acquire-failure cliff."
    ),
    unit="1",
    callbacks=[_observe_slot_pool_occupancy],
)


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
    description="Cron execution failure balance per schedule, via +1 per failure and -count on a successful reset.",
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


# ── Maintenance-sweep health ───────────────────────────────────────────
#
# A sweep whose instrumentation lives only on the success path is invisible
# exactly when it fails: the timeout that aborts the sweep also aborts the
# code that would have recorded it, so a livelocking sweep emits no samples
# at all — not zero, nothing. The emitters below are called from `finally`
# blocks and failure branches so a timing-out sweep is recorded as such.
#
# Publication discipline: the three cache dicts below are read on TWO
# threads. The event-loop thread publishes stamps via the record_* / update_*
# functions, and the OTel SDK reader thread iterates them from the
# observable-gauge callbacks (``_observe_sweep_success`` et al.) — the
# identical cross-thread shape the ``_active_leaders_lock`` in leader.py
# guards against ("Unsynchronized iteration raises RuntimeError"). So every
# writer REBINDS a fresh dict (copy-on-write, like ``_queue_depth_cache``
# and ``_jobs_by_status_cache`` below) rather than mutating in place: an
# in-place insert that lands while a reader's iterator is open raises
# ``RuntimeError: dictionary changed size during iteration`` — and the first
# success after startup and every post-demotion repopulation are exactly
# such inserts. A rebind is atomic and leaves the reader's already-open
# iterator over a frozen object.


def record_sweep_timeout(sweep_name: str) -> None:
    """Count a sweep call that was cut short by a deadline or server cancel.

    Called on the failure path (``TimeoutError`` / ``QueryCanceledError``),
    never the success path. Any sustained rate means sweeps are being
    aborted, not merely slow. Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _sweep_timeouts.add(1, {"sweep_name": sweep_name})


_sweep_timeouts = get_meter().create_counter(
    "taskq.maintenance_leader.sweep_timeouts",
    description=(
        "Sweep calls aborted by a deadline or server-side statement cancel, "
        "labeled by sweep_name. A non-zero rate means sweeps are being "
        "cancelled, not completing slowly."
    ),
    unit="1",
)


_sweep_success_cache: dict[str, float] = {}


def record_sweep_success(sweep_name: str) -> None:
    """Stamp the wall-clock time of a sweep call's success.

    Feeds the staleness gauge below and ``maintenance_health``'s stalled
    view: ``time() - last_success`` answers "is this sweep still making
    progress?" independently of row counts, so a sweep that finds zero
    eligible rows every tick (healthy) is distinguishable from one that
    never completes (stalled). The row-count counter and the duration
    histogram share this call site but NOT this sample population: a
    timed-out sweep records duration and a timeout but no row sample, so
    rows and duration must be read as different populations, which the
    sweep_timeouts counter reconciles.

    Rebind, never write in place: the cache is iterated on the OTel
    reader thread while this runs on the event-loop thread (see the
    section comment above).
    """
    global _sweep_success_cache
    _sweep_success_cache = {**_sweep_success_cache, sweep_name: time.time()}


def _observe_sweep_success(options: CallbackOptions) -> Iterable[Observation]:
    for sweep_name, stamp in _sweep_success_cache.items():
        yield Observation(stamp, {"sweep_name": sweep_name})


get_meter().create_observable_gauge(
    name="taskq.maintenance_leader.sweep_last_success_seconds",
    description=(
        "Unix timestamp of each sweep's last successful call. "
        "time() - this value is sweep staleness; a value that never moves "
        "while the process runs is a stalled sweep."
    ),
    unit="s",
    callbacks=[_observe_sweep_success],
)


def record_sweep_batch_size(sweep_name: str, batch_size: int) -> None:
    """Record the batch size a sweep call actually used.

    The maintenance sweeps degrade to a reduced batch after repeated
    cancellations; a worker reporting the reduced tier is reporting an
    unhealthy database and must not be silent about it. Feeds BOTH the
    batch-size gauge below AND ``maintenance_health``'s reduced-tier view
    (health.py reads the cache directly) — so it is deliberately NOT gated
    on ``_otel_enabled``: the health body must keep reporting a latched
    reduced tier when an operator has exported telemetry off, because that
    body is the Prometheus-free surface the degraded signal exists for.
    """
    update_sweep_batch_size_cache(sweep_name, batch_size)


_sweep_batch_size_cache: dict[str, int] = {}


def update_sweep_batch_size_cache(sweep_name: str, batch_size: int) -> None:
    """Replace the recorded batch size for *sweep_name* (cache-push gauge).

    Rebind, never write in place — same cross-thread reader discipline as
    :func:`record_sweep_success`.
    """
    global _sweep_batch_size_cache
    _sweep_batch_size_cache = {**_sweep_batch_size_cache, sweep_name: batch_size}


def _observe_sweep_batch_size(options: CallbackOptions) -> Iterable[Observation]:
    for sweep_name, size in _sweep_batch_size_cache.items():
        yield Observation(size, {"sweep_name": sweep_name})


get_meter().create_observable_gauge(
    name="taskq.maintenance_leader.sweep_batch_size",
    description="Rows per committed batch each sweep is currently using.",
    unit="1",
    callbacks=[_observe_sweep_batch_size],
)


_sweep_batch_size_configured_cache: dict[str, int] = {}


def record_sweep_batch_size_configured(sweep_name: str, configured_size: int) -> None:
    """Record the batch size this worker is CONFIGURED to use for *sweep_name*.

    Emitted at the same call site as :func:`record_sweep_batch_size` so
    the two gauges carry the same ``sweep_name`` label set and a
    gauge-to-gauge comparison is always label-matched. The alert pair
    (used vs configured) is what makes the sweep-degraded signal track
    per-worker ``event_writer_batch_size``: a literal threshold is blind
    on every deployment whose configured size is not the default.
    Respects ``_otel_enabled`` — no-op when False (its only consumer is
    the OTel gauge, unlike :func:`record_sweep_batch_size`).

    Rebind, never write in place — same cross-thread reader discipline as
    :func:`record_sweep_success`.
    """
    if not _otel_enabled:
        return
    global _sweep_batch_size_configured_cache
    _sweep_batch_size_configured_cache = {
        **_sweep_batch_size_configured_cache,
        sweep_name: configured_size,
    }


def _observe_sweep_batch_size_configured(options: CallbackOptions) -> Iterable[Observation]:
    for sweep_name, size in _sweep_batch_size_configured_cache.items():
        yield Observation(size, {"sweep_name": sweep_name})


get_meter().create_observable_gauge(
    name="taskq.maintenance_leader.sweep_batch_size_configured",
    description=(
        "Rows per committed batch this worker's event_writer_batch_size "
        "configures for each sweep; compare against "
        "taskq_maintenance_leader_sweep_batch_size to detect the reduced tier."
    ),
    unit="1",
    callbacks=[_observe_sweep_batch_size_configured],
)


def clear_sweep_health_caches() -> None:
    """Drop this process's sweep-success and batch-size stamps.

    Called on leadership demotion (the same authority-release point that
    clears the queue-depth sampler caches): the stamps are this process's
    report of the sweeps its LEADER loops ran, and a demoted process
    exporting frozen stamps reports a degraded maintenance view forever
    after an ordinary failover, while its frozen
    ``sweep_last_success_seconds`` series fires the promotion-stalled
    alert forever even as the new leader promotes fine. Rebound to the
    empty informational state ("no sweep has completed yet") rather than
    zeroed: an empty gauge yields no data point, so the series goes stale
    and the new leader's is the one answering. If the election re-wins
    during the same demotion suspension, the sweep loops repopulate both
    caches on their next tick.
    """
    global _sweep_success_cache, _sweep_batch_size_cache
    _sweep_success_cache = {}
    _sweep_batch_size_cache = {}


def record_lock_contention(lock_name: str) -> None:
    """Count an advisory-lock acquisition lost to another session.

    Recorded by the LOSING side at every maintenance acquisition point
    (leader election, prune, archive-expiry). Contention is the signal that
    would have exposed two schemas in one database silently sharing one
    lock, and a sustained rate equal to the attempt rate means the loser
    never wins at all. Cron's contention stays on its own dedicated
    counter (``taskq.cron.lock_contention``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lock_contention.add(1, {"lock": lock_name})


_lock_contention = get_meter().create_counter(
    "taskq.leader.lock_contention",
    description=(
        "Advisory-lock acquisitions lost to another session, labeled by "
        "lock name. Recorded by the losing side."
    ),
    unit="1",
)


# ── Backlog detection ─────────────────────────────────────────────────
#
# These gauges are the #102 detectors: a scheduled backlog that stops
# moving is invisible in a merged pending+scheduled count and in absolute
# depth thresholds. They are sampled UNCONDITIONALLY (every worker, not
# leader-gated) on purpose: a detector hosted behind the leadership gate
# emits nothing under exactly the failure (a second schema's lock) that
# mutes every leader-gated sampler.


def update_jobs_by_status_cache(data: dict[str, int]) -> None:
    """Replace the per-status job-count cache with fresh data.

    Called by the backlog sampler. Replaces the merged pending+scheduled
    queue-depth view: a growing `scheduled` count next to a flat `pending`
    count is the promotion-stall signature, which a single summed number
    cannot express.
    """
    global _jobs_by_status_cache
    _jobs_by_status_cache = dict(data)


def _observe_jobs_by_status(options: CallbackOptions) -> Iterable[Observation]:
    for status, count in _jobs_by_status_cache.items():
        yield Observation(count, {"status": status})


_jobs_by_status_cache: dict[str, int] = {}

get_meter().create_observable_gauge(
    name="taskq.jobs.by_status",
    description="Jobs per status, sampled by every worker.",
    unit="1",
    callbacks=[_observe_jobs_by_status],
)


def update_oldest_due_age_cache(age_seconds: float) -> None:
    """Record the age of the oldest due-but-still-scheduled job.

    0.0 when nothing is due. Monotonic growth of this gauge is the single
    best promotion-stall detector: it moves under failure regardless of
    queue depth or throughput, where absolute depth thresholds are
    environment-specific guesses.
    """
    global _oldest_due_age_seconds
    _oldest_due_age_seconds = age_seconds


def _observe_oldest_due_age(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(_oldest_due_age_seconds)


_oldest_due_age_seconds: float = 0.0

get_meter().create_observable_gauge(
    name="taskq.jobs.oldest_due_age_seconds",
    description=(
        "Seconds since the oldest scheduled job became due for promotion. "
        "Grows monotonically while promotion is stalled."
    ),
    unit="s",
    callbacks=[_observe_oldest_due_age],
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
