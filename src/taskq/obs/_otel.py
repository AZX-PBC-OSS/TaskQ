"""OpenTelemetry tracer, meter, and metric helpers.

Provides safe, no-raise wrappers around OTel API calls so that observability
failures never propagate to user or actor code.  All metric instruments
are module-level singletons created at import time from the global meter provider,
except the call-time-resolved ones behind :func:`_lazy_counter` /
:func:`_lazy_histogram` (see there for why the singleton pattern does not
fit them).

Label cardinality contract
--------------------------

Metric dimensions are limited to values that are bounded by construction:

- ``actor``: the set of registered actor names (bounded by the code the
  user ships) on every instrument whose actor flows through registration
  — job-side emitters receive ``ActorRef`` names, and the cron loop's
  success/suppression paths only emit after the tick resolved the actor
  against ``actor_config``.  The one exception is
  ``taskq.cron.consecutive_failures``: its failure path emits the raw
  ``cron_schedules.actor`` string, and schedule rows accept any string
  at creation time, so that label is capped like ``queue`` (see the
  cardinality note above ``_bounded_cron_actor``); its per-schedule
  attribution lives on log lines and the cron-fire span instead (see
  the cardinality note above ``_lock_expires_in_seconds``).
- ``sweep_name`` / ``lock`` / ``status`` / ``outcome``: closed enums, carried
  as-is.
- ``queue``: caller-supplied per enqueue and only charset-validated -- the
  one open-ended label on the job-side instruments.  The four job-side
  emitters (:func:`record_published_message`, :func:`record_dispatch_duration`,
  :func:`record_consumed_message`, :func:`record_process_duration`) bound it
  to the first ``_MAX_QUEUE_LABEL_VALUES`` distinct names a process sees;
  beyond the cap the label collapses to the fixed ``_other_`` value (see the
  cardinality note above :func:`_bounded_queue`).  Identity-like values
  (``worker_id``, ``job_id``, ``schedule_id``) are never dimensions at all --
  see the cardinality note above ``_lock_expires_in_seconds``.
"""

import contextlib
import functools
import importlib.metadata
import sys
import time
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from typing import Literal, Protocol
from weakref import WeakKeyDictionary

import structlog
from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.metrics import CallbackOptions, Counter, Histogram, Meter, Observation
from opentelemetry.trace import Span, StatusCode, Tracer
from opentelemetry.util.types import Attributes

from taskq.obs._redact_exc import add_exception_event, render_exception

INSTRUMENTATION_NAME: str = "taskq"

type ConsumedOutcome = Literal["succeeded", "failed", "cancelled", "scheduled"]
"""The ``outcome`` label set of ``messaging.client.consumed.messages``.

Every value has a producer: the three terminal outcomes, and ``scheduled``
for an attempt that ended with the row released back to the queue — a
retryable failure, a ``Snooze`` / ``RetryAfter``, or an admission denial.
``taskq.jobs.attempt_failures`` separates the failure share of
``scheduled``; ``taskq.jobs.abandoned`` counts real abandonment, which is
an operator cancel outlasting its graces and never a consumer outcome.
"""

__all__ = [
    "INSTRUMENTATION_NAME",
    "ConsumedOutcome",
    "StrandedReason",
    "TimeoutKind",
    "get_meter",
    "get_tracer",
    "otel_enabled",
    "reconcile_cron_failures",
    "record_archived_jobs",
    "record_attempt_failure",
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
    "record_job_abandoned",
    "record_leader_lease_expires_in_seconds",
    "record_lock_expires_in_seconds",
    "record_process_duration",
    "record_progress_flush_failure",
    "record_progress_publish_failure",
    "record_pruned_jobs",
    "record_published_message",
    "record_queue_wait",
    "record_ratelimit_denial",
    "record_ratelimit_refund_failure",
    "record_reclaimed_jobs",
    "record_reservation_denial",
    "record_reservation_reclaim_drain_duration",
    "record_reservation_reclaim_drain_failure",
    "record_reservation_reclaim_drain_rows",
    "record_reservation_reclaim_heal_failure",
    "record_sub_enqueue_failure",
    "safe_start_span",
    "set_otel_enabled",
    "update_disabled_schedules_count",
    "update_heartbeat_consecutive_failures",
    "update_jobs_running_cache",
    "update_keyed_reclaim_pending",
    "update_queue_depth_cache",
    "update_queue_live_workers_cache",
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


def otel_enabled() -> bool:
    """Whether telemetry emission is currently enabled.

    Read at call time, never imported by value: the flag is module state
    flipped once by worker startup (see :func:`set_otel_enabled`), and a
    reader checks it to skip WORK whose only consumer is a metric — a
    query whose result only a reconcile would export is a wasted round
    trip when the flag is off.  The emitters themselves stay gated
    individually regardless.
    """
    return _otel_enabled


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


_library_meter: Meter | None = None
"""The meter :func:`get_meter` resolves once and hands back (see there)."""


def get_meter() -> Meter:
    """Return the library's meter. Honors any globally-configured provider.

    The meter object is resolved on first use and memoized, exactly like
    :func:`get_tracer`: with no SDK set up, ``metrics.get_meter`` returns
    a ``_ProxyMeter``, which rebinds to the real provider's meter when an
    SDK registers later (``on_set_meter_provider`` notifies every proxy
    meter, and every proxy instrument on it), so memoization never pins
    the proxy/no-op behavior. And unlike the uncached call, it never asks
    the proxy provider for a second meter: ``_ProxyMeterProvider`` appends
    every ``get_meter`` result to a list with no cleanup path, so an
    unmemoized accessor grows that list on every lazy-instrument call —
    one entry per rate-limit denial, reservation denial, flush failure,
    and drain row-count, in exactly the default deployment (taskq never
    installs a provider itself).
    """
    global _library_meter
    if _library_meter is None:
        _library_meter = metrics.get_meter(INSTRUMENTATION_NAME, _version())
    return _library_meter


def _record_scrubbed_error(span: Span, exc: BaseException) -> None:
    """Give *span* a scrubbed error signal for whichever half the call site left unset.

    Why: :func:`safe_start_span` turns the SDK's automatic exception handling
    OFF (see there), so a call site that does not handle the exception itself
    -- the ``attempt.N`` span in ``worker/_consumer.py``, which wraps user job
    code -- would otherwise export with no error signal at all. Suppressing a
    leak must not cost the signal.

    Each half is supplied only when missing, so the call sites that already
    scrub (``dispatch_batch``, ``cron fire``, the consumer's ``attempt.N``
    span) do not get a duplicate event or have their description rewritten,
    while ``enqueue_span`` -- which marks the span ERROR but records no event
    -- still gets the scrubbed exception text it needs to stay diagnostic.

    A span that is not recording takes neither half: the text would be
    rendered and scrubbed only to be dropped, and rendering is the dominant
    cost of a failed job.
    """
    try:
        if not span.is_recording():
            return
        status_code = getattr(getattr(span, "status", None), "status_code", None)
        needs_status = status_code is not StatusCode.ERROR
        events: Iterable[object] = getattr(span, "events", ())
        needs_event = not any(getattr(event, "name", None) == "exception" for event in events)
        if not (needs_status or needs_event):
            return
        text = render_exception(exc)
        if needs_status:
            span.set_status(StatusCode.ERROR, text.message)
        if needs_event:
            add_exception_event(span, text)
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
        "kind ('max_pending' | 'max_pending_lock_timeout' | "
        "'unique_for_lock_timeout' | 'idempotency_lock_timeout'). The "
        "lock-timeout kinds count identity-serialization refusals beside "
        "their typed errors — never a capacity signal, so an alert keyed "
        "on the capacity kinds is not tripped by them."
    ),
)


def record_backpressure_error(actor: str, *, kind: str = "max_pending") -> None:
    """Bump the backpressure.errors counter.

    Unconditional (not gated by ``_otel_enabled``): backpressure errors are
    safety-critical signals that must be counted even when OTel is disabled,
    so operators always have visibility into enqueue rejections. ``kind``
    is the bounded enum named on the counter: the two capacity kinds, and
    the two identity-serialization lock-timeout kinds that count their
    refusals beside the typed errors' warning logs.
    """
    try:
        _backpressure_errors.add(1, {"actor": actor, "kind": kind})
    except Exception:
        _log.warning("otel-metric-record-failed", instrument_name="taskq.backpressure.errors")


def _resolve_error_type(error_type: str | None) -> str:
    """Resolve the ``error_type`` label value for a failure counter.

    The established failure-counter idiom (``record_progress_publish_failure``,
    ``record_reservation_reclaim_drain_failure``) takes the exception class
    name explicitly. The recorders swept onto that idiom are called from
    ``except`` blocks whose call sites predate the label, so an omitted
    *error_type* derives from the exception currently being handled
    (``sys.exception()``) — every production call site records from an
    ``except`` block, so the derived value is the caught exception's class.
    With no explicit value and no active exception the label falls back to
    the fixed ``"unknown"`` value: the value set stays a closed class set —
    the exception types these paths can raise, plus that one constant —
    never caller-supplied text, so the label cannot mint unbounded series
    the way an identity value would.
    """
    if error_type is not None:
        return error_type
    active = sys.exception()
    return type(active).__name__ if active is not None else "unknown"


_capacity_refresh_failures = get_meter().create_counter(
    "taskq.backpressure.capacity_refresh_failures",
    description=(
        "Failed refreshes of the enqueue-side actor_config capacity cache. "
        "Attributes: degraded ('stale_snapshot' when a previous snapshot is "
        "still being served, 'no_snapshot' when the cache never loaded and "
        "every enqueue is falling back to the @actor literal), error_type "
        "(exception class name — a closed set; see _resolve_error_type)."
    ),
    unit="1",
)


def record_capacity_refresh_failure(*, has_snapshot: bool, error_type: str | None = None) -> None:
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

    ``error_type`` is the exception class name; omitted, it derives from the
    exception being handled (``_resolve_error_type``) — the call site records
    from the refresh read's ``except`` block.

    Unconditional (not gated by ``_otel_enabled``) for the same reason as
    ``record_backpressure_error``: this is a safety-critical signal.
    """
    try:
        _capacity_refresh_failures.add(
            1,
            {
                "degraded": "stale_snapshot" if has_snapshot else "no_snapshot",
                "error_type": _resolve_error_type(error_type),
            },
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
    # The sweep's arm of the whole-job deadline, on the timeouts family
    # beside the handler arms (gated, unlike the sweep counter above).
    record_job_timeout(actor, kind="schedule_to_close", count=count)


_reclaimed_jobs = get_meter().create_counter(
    "taskq.jobs.reclaimed",
    description=(
        "Running jobs reclaimed by the expired-locks sweep (the holder broke "
        "its liveness promise - lease expiry or heartbeat timeout), labeled "
        "by actor and disposition: repended (attempts remained, the row went "
        "back to pending on its retry curve), crashed (budget exhausted, "
        "terminal), cancelled (a cancel request was in-flight when the "
        "holder died - the honest terminal label)."
    ),
    unit="1",
)


def record_reclaimed_jobs(actor: str, disposition: str, count: int = 1) -> None:
    """Bump the per-actor, per-disposition reclaimed-jobs counter.

    Unconditional (not gated by ``_otel_enabled``): a reclaim is a worker
    death the fleet recovered from -- the same class of fleet-health
    signal as :func:`record_deadline_exceeded_swept`, which must be
    counted even when OTel is disabled. Recorded aggregated per (actor,
    disposition) after the sweep's transaction, by the Postgres sweep
    (from its RETURNING) and by the in-memory twin alike, so the two
    backends' label sets cannot drift. The disposition values are the
    code-fixed enum ``repended`` / ``crashed`` / ``cancelled``
    (``taskq.backend._sweeps._RECLAIM_DISPOSITIONS``); ``actor`` flows
    through registration like every other actor-labeled instrument.
    """
    try:
        _reclaimed_jobs.add(count, {"actor": actor, "disposition": disposition})
    except Exception:
        _log.warning("otel-metric-record-failed", instrument_name="taskq.jobs.reclaimed")


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


def _admitted_label_value(admitted: set[str], value: str, cap: int, overflow: str) -> str:
    """Return *value*, or the fixed *overflow* label once *cap* distinct
    values are admitted.

    The shared first-N-then-overflow core of :func:`_bounded_queue` and
    :func:`_bounded_cron_actor`: admission never evicts, so an admitted
    value keeps its own series for the life of the process and the series
    count is hard-bounded at cap + 1 no matter how many distinct values
    the callers mint.
    """
    if value in admitted:
        return value
    if len(admitted) >= cap:
        return overflow
    admitted.add(value)
    return value


def _bounded_queue(queue: str) -> str:
    """Return *queue*, or the fixed overflow label once the cap is reached.

    See the cardinality note above.
    """
    return _admitted_label_value(
        _queue_label_values, queue, _MAX_QUEUE_LABEL_VALUES, _QUEUE_LABEL_OVERFLOW
    )


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


_dispatch_failures = get_meter().create_counter(
    "taskq.dispatch.failures",
    description=(
        "Count of dispatch rounds that raised before returning a claim set, "
        "labeled by queue (capped -- see _bounded_queue) and error_type "
        "(exception class name — a closed set; see _resolve_error_type). A "
        "producer that fails every round emits successful-looking silence on "
        "every other dispatch signal; this counter is what separates a "
        "failing producer from an idle queue without reading logs, and "
        "error_type separates a self-healing class (a lock timeout, a "
        "connection reset) from a permanent one (an auth failure, schema "
        "drift)."
    ),
    unit="1",
)


def record_dispatch_failure(queue: str, error_type: str | None = None) -> None:
    """Bump the dispatch-failure counter.

    Called from the dispatch round's exception path, outside the span body
    for sampling independence.  ``error_type`` is the exception class name;
    omitted, it derives from the exception being handled
    (``_resolve_error_type``) — every call site records from an ``except``
    block, so the derived value is the caught exception's class.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _dispatch_failures.add(
        1, {"queue": _bounded_queue(queue), "error_type": _resolve_error_type(error_type)}
    )


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

    ``outcome`` is the closed :data:`ConsumedOutcome` set. A consumer-path
    ``AttemptOutcome`` of ``"scheduled"`` (retry, snooze, admission denial)
    is recorded as exactly that — the row went back to the queue — never
    as ``abandoned``, which is the operator-cancel outcome and has its own
    counter (:func:`record_job_abandoned`). A ``"noop"`` attempt consumed
    nothing and must not reach this recorder.
    """
    if not _otel_enabled:
        return
    _consumed_messages.add(1, {"actor": actor, "queue": _bounded_queue(queue), "outcome": outcome})


def record_attempt_failure(actor: str, error_type: str | None = None, *, retryable: bool) -> None:
    """Count one attempt that ended in an actor failure, retried or terminal.

    Called from the failure handlers (``worker/_handlers.py``) once per
    handled exception, after the retry decision and before the terminal
    write: an attempt that raised failed whether or not the row write that
    follows lands, and the consumed-messages ``outcome`` says what happened
    to the row. ``retryable`` is the classifier's decision — ``true`` when
    the attempt is rescheduled for another try, ``false`` when the failure
    is terminal (a non-retryable class, or the attempt budget exhausted) —
    which is what a retry-rate alert reads. ``error_type`` is the exception
    class name; omitted, it derives from the exception being handled
    (``_resolve_error_type``): a closed set, never caller text.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.jobs.attempt_failures",
        description=(
            "Attempts that ended in an actor failure. Attributes: actor, "
            "error_type (exception class name — a closed set; see "
            "_resolve_error_type), retryable ('true' when the attempt is "
            "rescheduled for another try, 'false' when the failure is "
            "terminal). The failure share of consumed outcome='scheduled'."
        ),
    ).add(
        1,
        {
            "actor": actor,
            "error_type": _resolve_error_type(error_type),
            "retryable": "true" if retryable else "false",
        },
    )


def record_job_abandoned(actor: str) -> None:
    """Count one job abandoned by an operator cancel that outlasted its graces.

    Called from ``mark_abandoned`` on both backends once the abandon write
    applied: the actor was asked to stop, then forced, and never exited, so
    the row is taken from it. Shutdowns never produce this — a deploy
    interrupts running attempts back to the fleet instead — which is why
    any non-zero rate is worth a page. Attributes: actor.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.jobs.abandoned",
        description=(
            "Jobs abandoned: an operator cancel outlasted the cooperative and "
            "forced grace periods and the running attempt was taken away. "
            "Never produced by a shutdown (those interrupt). Attributes: actor."
        ),
    ).add(1, {"actor": actor})


def record_loop_stall_attribution(actor: str | None, *, kind: str) -> None:
    """Count one event-loop stall attributed to the frame holding the GIL.

    Called from the loop-lag watchdog's daemon thread at the stall's warn
    and trip tiers: sampling the main thread's current frame from there is
    the only attribution that works mid-block, and it needs no loop
    cooperation. ``actor`` is ``None`` when no registered actor function
    appeared in the sampled stack (the block sits under taskq's own code
    or a non-actor coroutine). Respects ``_otel_enabled`` — no-op when
    False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.worker.loop_stall_attributions",
        description=(
            "Event-loop scheduling stalls attributed to the synchronous work "
            "holding the interpreter while the loop could not schedule. "
            "kind='blocking_call' marks a synchronous call that released the "
            "GIL (I/O wait, a subprocess); kind='gil_held' marks synchronous "
            "work that held it (a C extension without GIL release, or a hot "
            "pure-Python loop). The actor label names the registered actor "
            "function whose frame sat under the blocking call; run it down by "
            "moving the cited work off the event loop. Attributes: actor, kind."
        ),
    ).add(
        1,
        {
            "actor": (_bounded_cron_actor(actor) if actor is not None else _ACTOR_LABEL_OVERFLOW),
            "kind": kind,
        },
    )


_process_duration = get_meter().create_histogram(
    "messaging.process.duration",
    description=(
        "Job execution duration, labeled by actor, queue (capped -- see "
        "_bounded_queue) and outcome (the consumed-messages outcome set), so "
        "a timed-out or failed attempt's duration is not folded into the "
        "success distribution."
    ),
    unit="s",
)


def record_process_duration(
    actor: str, queue: str, elapsed: float, *, outcome: ConsumedOutcome
) -> None:
    """Record job execution duration on the histogram.

    Called outside the CONSUMER span body for sampling independence, with
    the same ``outcome`` the consumed-messages counter records for the
    attempt: a ``start_to_close`` timeout lands at exactly the budget and
    a failure at whatever it took, and either would drag a success
    percentile if the distributions were shared.
    Respects ``_otel_enabled`` — no-op when False.
    Custom buckets are the operator's responsibility via SDK Views.
    """
    if not _otel_enabled:
        return
    _process_duration.record(
        elapsed, {"actor": actor, "queue": _bounded_queue(queue), "outcome": outcome}
    )


def record_queue_wait(actor: str, queue: str, waited_seconds: float) -> None:
    """Record how long a job waited from eligibility to claim.

    Called once per dispatch from the claimed row's own server-clock
    stamps: ``started_at - scheduled_at``, eligibility to claim. The
    per-job companion of the sampled ``oldest_pending_age_seconds``: the
    gauge shows the head of the line, this histogram shows what every
    dispatched job actually waited, retries and re-pends included. Labels
    are the job-side pair (actor, queue capped as everywhere).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_histogram(
        "taskq.jobs.queue_wait_seconds",
        description=(
            "Seconds a job waited between becoming eligible (scheduled_at) "
            "and being claimed (started_at), both server-clock stamps on the "
            "dispatched row. Attributes: actor, queue (capped — see "
            "_bounded_queue)."
        ),
        unit="s",
    ).record(waited_seconds, {"actor": actor, "queue": _bounded_queue(queue)})


type TimeoutKind = Literal["start_to_close", "schedule_to_close"]
"""Which budget a job exceeded — the closed ``kind`` label set of
``taskq.jobs.timeouts``."""


def record_job_timeout(actor: str, *, kind: TimeoutKind, count: int = 1) -> None:
    """Count *count* jobs that exceeded a time budget.

    ``start_to_close`` is recorded at the timeout handler once per
    attempt that hit its per-attempt budget, retried or not.
    ``schedule_to_close`` is recorded wherever the whole-job deadline is
    what ended it: the deadline sweep (which counts a batch at a time),
    and the handler arms where the backend's deadline arbitration refused
    a retry, a snooze or a denial's requeue with ``DeadlineExceeded``.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.jobs.timeouts",
        description=(
            "Jobs that exceeded a time budget. Attributes: actor, kind "
            "('start_to_close' — the per-attempt budget, at the timeout "
            "handler; 'schedule_to_close' — the whole-job deadline, at the "
            "deadline sweep and the handler arms the backend refused with "
            "DeadlineExceeded)."
        ),
    ).add(count, {"actor": actor, "kind": kind})


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
#: The signal is not lost: per-worker and per-schedule attribution lives on
#: spans and log lines, where cardinality is free (``worker_id`` is bound via
#: contextvars onto every log line; ``taskq.worker_id`` and
#: ``taskq.cron_schedule_id`` are cron-fire span attributes; ``schedule_id``
#: is on the ``cron fired`` / ``cron fire failed`` / ``cron schedule
#: auto-disabled`` log lines).
#:
#: ``schedule_id`` was the one identity-like dimension that survived the
#: ``worker_id`` campaign, on the argument that schedules are a bounded set
#: an operator creates by hand.  That argument does not hold:
#: cron_schedules rows are runtime-creatable (``create_schedule`` is public
#: client API, and the admin UI exposes it), each minting a fresh per-row
#: UUID, so the value set is unbounded by construction -- nothing the
#: library ships caps it.  ``taskq.cron.consecutive_failures`` is therefore
#: labeled by ``actor`` -- a premise that needs its own guard, because the
#: failure path emits the raw schedule-row actor and schedule rows accept
#: any string at creation time, so the label is capped at the emitter
#: (``_bounded_cron_actor`` below) rather than carried as-is.  The
#: alerting purpose survives the relabel: the series carries the actor's
#: outstanding failure count and both directions hold, because each tick
#: reconciles it against the database's own sum read over the whole
#: ``cron_schedules`` table (``reconcile_cron_failures``) rather than
#: accumulating this process's deltas -- an enable, disable or delete
#: performed anywhere in the fleet self-corrects on the next tick with
#: due work, and the value returns to zero when no schedule is failing.
#: What the summed value loses -- WHICH schedule -- no shipped consumer
#: ever read: ``cron_schedules.last_fire_error`` names it, and
#: per-schedule debugging lives on the log lines and span attributes
#: named above.
#:
#: The ``worker_id`` parameters below are kept: they are part of the published
#: ``taskq.obs`` surface, and dropping them would be a breaking change for a
#: value the callers already have to hand.

_lock_expires_in_seconds = get_meter().create_histogram(
    "taskq.lock.expires_in_seconds",
    description=(
        "Lease remaining on this worker's job locks at the moment the "
        "heartbeat's jobs-lock UPDATE landed: lock_lease minus the gap since "
        "the previous beat's UPDATE, measured, so a late or failed tick "
        "lowers the sample. 0 when the beat landed after expiry. Under "
        "threshold-gated renewal the sample is stamped on every successful "
        "beat (renewed or not), so it measures the beat cadence — the floor "
        "the renewal threshold keeps is pinned by tests, not by this "
        "histogram. No dimensions."
    ),
    unit="s",
    explicit_bucket_boundaries_advisory=(0, 5, 10, 15, 20, 30, 45, 60),
)


def record_lock_expires_in_seconds(worker_id: str, remaining_ttl: float) -> None:
    """Record the measured remaining lock TTL on the histogram.

    Called in heartbeat.py at each successful renewal after the first,
    with the lease the previous renewal stamped minus the time elapsed
    since — a measurement, never the configured constant, so the
    lock-expiry alert can fire when renewals run late.
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
        "lock-lease expiry. One dimension: error_type (exception class "
        "name — a closed set; see _resolve_error_type). The pool name is "
        "in the instrument name and the per-occurrence job id stays in "
        "the log event."
    ),
    unit="1",
)


def record_slot_pool_acquire_failure(error_type: str | None = None) -> None:
    """Bump the worker.slot_pool.acquire_failures counter.

    Called from the exception branch of the bounded per-job acquire in
    ``taskq.worker.dispatch`` — never the success path, matching
    ``record_sweep_timeout``'s contract. A rate here is what separates
    one transient timeout from every transactional job on a worker
    failing to acquire, and ``error_type`` is the exception class name —
    omitted, it derives from the exception being handled
    (``_resolve_error_type``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _slot_pool_acquire_failures.add(1, {"error_type": _resolve_error_type(error_type)})


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


# ── Worker capacity ──────────────────────────────────────────────────
#
# The health socket renders taskq_active_jobs by hand, which no scrape
# reaches; these are the OTel twins a real exporter carries. No labels:
# one series per process is the shape (the pod is the identity, on the
# scrape target's own labels), and utilisation is the per-process ratio
# active_jobs / max_concurrency.


class _ActiveJobsSource(Protocol):
    """Structural slice of ``ActiveJobRegistry`` the active-jobs gauge reads.

    Keeps this observability leaf free of a worker import; bootstrap
    passes the real registry, which satisfies this shape structurally.
    ``count`` is a dict length — safe to read from the SDK reader thread.
    """

    def count(self) -> int: ...


_worker_capacity_source: tuple[_ActiveJobsSource, int] | None = None
"""(active-jobs registry, max_concurrency) — set once by worker bootstrap."""


def set_worker_capacity_source(active_jobs: _ActiveJobsSource | None, max_concurrency: int) -> None:
    """Point the worker capacity gauges at *active_jobs* and *max_concurrency*.

    Called once at worker bootstrap, after the deps that own the
    registry exist. ``None`` clears the source — both gauges report
    nothing, matching a process that hosts no worker.
    """
    global _worker_capacity_source
    _worker_capacity_source = (active_jobs, max_concurrency) if active_jobs is not None else None


def _observe_worker_active_jobs(options: CallbackOptions) -> Iterable[Observation]:
    source = _worker_capacity_source
    if source is None:
        return
    try:
        # Why defensive: a collection read must never raise into the
        # SDK's export path, whatever the registry is mid-way through.
        count = source[0].count()
    except Exception:
        return
    yield Observation(count)


def _observe_worker_max_concurrency(options: CallbackOptions) -> Iterable[Observation]:
    source = _worker_capacity_source
    if source is None:
        return
    yield Observation(source[1])


get_meter().create_observable_gauge(
    name="taskq.worker.active_jobs",
    description=(
        "Jobs in flight on this worker process. No dimensions: one series "
        "per process. Divide by taskq.worker.max_concurrency for utilisation."
    ),
    unit="1",
    callbacks=[_observe_worker_active_jobs],
)

get_meter().create_observable_gauge(
    name="taskq.worker.max_concurrency",
    description=(
        "This worker process's configured max_concurrency — the ceiling "
        "taskq.worker.active_jobs saturates against. No dimensions."
    ),
    unit="1",
    callbacks=[_observe_worker_max_concurrency],
)


_queue_depth_cache: dict[str, int] = {}


def update_queue_depth_cache(data: dict[str, int]) -> None:
    """Replace the queue-depth cache with fresh data from the leader's PG query.

    Called by the background async task in the leader loop every 15s.
    The synchronous gauge callback reads from this cache.
    """
    global _queue_depth_cache
    _queue_depth_cache = dict(data)


def _observe_capped_per_queue(cache: Mapping[str, int]) -> Iterable[Observation]:
    """Yield one observation per queue from *cache*, capped like the depth gauge.

    A gauge is observable, not additive, so the counter sites' per-item
    label mapping cannot be reused here: every overflow queue yielding its
    own `_other_` observation would report one queue's value instead of
    the total. The partition is therefore computed before yielding: the
    `_MAX_QUEUE_LABEL_VALUES` largest queues keep their own series (ties
    broken by queue name, for determinism), and everything smaller
    collapses onto ONE `_other_` observation carrying the summed overflow,
    so the reported total always equals the true total. Value ranking --
    not name order and not first-seen admission -- keeps the largest
    queues, the ones an operator pages on, individually visible past the
    cap. Nothing shared is mutated: `_queue_label_values` stays owned by
    the job-side instruments. Shared by the queue-depth and
    live-workers gauges, which the same sampler tick feeds.
    """
    ranked = sorted(cache.items(), key=lambda item: (-item[1], item[0]))
    admitted = ranked[:_MAX_QUEUE_LABEL_VALUES]
    overflow = ranked[_MAX_QUEUE_LABEL_VALUES:]
    for queue, value in admitted:
        yield Observation(value, {"queue": queue})
    if overflow:
        yield Observation(
            sum(value for _queue, value in overflow), {"queue": _QUEUE_LABEL_OVERFLOW}
        )


def _observe_queue_depth(options: CallbackOptions) -> Iterable[Observation]:
    return _observe_capped_per_queue(_queue_depth_cache)


_queue_depth_gauge = get_meter().create_observable_gauge(
    name="taskq.queue.depth",
    description=(
        "Number of pending/scheduled jobs per queue, sampled by the leader "
        "(capped: the _MAX_QUEUE_LABEL_VALUES deepest queues keep their own "
        f"series; shallower queues collapse onto one '{_QUEUE_LABEL_OVERFLOW}' "
        "series carrying their summed depth)."
    ),
    unit="1",
    callbacks=[_observe_queue_depth],
)


_queue_live_workers_cache: dict[str, int] = {}


def update_queue_live_workers_cache(data: dict[str, int]) -> None:
    """Replace the per-queue live-worker cache with fresh data from the
    leader's query — sampled in the same tick as the queue depth, so the
    two can be joined on ``queue`` without describing different moments.

    A worker is live when its ``last_seen_at`` is within the liveness
    window (``admin_worker_liveness_seconds``); a dead-but-unswept worker
    row does not count. A queue with pending work and no live worker is
    the condition ``TaskQQueueUnserved`` fires on, and it is invisible to
    every other gauge: depth alone cannot say whether anyone is consuming.
    """
    global _queue_live_workers_cache
    _queue_live_workers_cache = dict(data)


def _observe_queue_live_workers(options: CallbackOptions) -> Iterable[Observation]:
    return _observe_capped_per_queue(_queue_live_workers_cache)


_queue_live_workers_gauge = get_meter().create_observable_gauge(
    name="taskq.queue.live_workers",
    description=(
        "Workers whose last_seen_at is within the liveness window, per queue "
        "they subscribe to, sampled by the leader with taskq.queue.depth "
        "(same cap: the largest _MAX_QUEUE_LABEL_VALUES queues keep their own "
        f"series; the rest collapse onto one '{_QUEUE_LABEL_OVERFLOW}' series). "
        "A queue with depth > 0 and no live worker is unserved."
    ),
    unit="1",
    callbacks=[_observe_queue_live_workers],
)


type StrandedReason = Literal["no_actor_config", "unserved_queue"]
"""Why a pending/scheduled row can never dispatch — the closed ``reason``
label set of ``taskq.jobs.stranded``."""

_stranded_jobs_cache: dict[tuple[str, StrandedReason], int] = {}


def update_stranded_jobs_cache(data: Mapping[tuple[str, StrandedReason], int]) -> None:
    """Replace the stranded-jobs cache with fresh data from the leader's query.

    Stranded jobs are pending/scheduled rows that can never dispatch: the
    actor has no `actor_config` row (the dispatch CTE derives its
    candidates from `per_actor_capacity`, which is `FROM actor_config`),
    or the row sits on a queue no LIVE registered worker serves (dispatch
    probes only its own subscription's queues, and a worker whose
    last_seen_at has gone stale is not dispatching). Both shapes
    accumulate invisibly to dispatch and the deadline sweep. Keyed by
    ``(actor, reason)`` so the gauge says which condition held — the two
    have different remediations (register the actor vs. subscribe a
    worker to the queue), and a per-actor total made an operator who
    found the actor_config row present conclude the detector lied.

    This gauge exists because the detector previously emitted a log line and
    nothing else, exactly once per actor per process lifetime -- so the
    condition was invisible in metrics and its only trace was a single WARN at
    onset, which is the moment nobody is looking. An empty mapping clears the
    gauge, so recovery is visible too.
    """
    global _stranded_jobs_cache
    _stranded_jobs_cache = dict(data)


def _observe_stranded_jobs(options: CallbackOptions) -> Iterable[Observation]:
    for (actor, reason), count in _stranded_jobs_cache.items():
        yield Observation(count, {"actor": actor, "reason": reason})


_stranded_jobs_gauge = get_meter().create_observable_gauge(
    name="taskq.jobs.stranded",
    description=(
        "Pending/scheduled jobs that can never be dispatched, sampled by the "
        "leader. Attributes: actor, reason ('no_actor_config' — the actor "
        "has no actor_config row; 'unserved_queue' — the queue dispatch "
        "routes the row on has no live worker subscribed)."
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


def record_progress_flush_failure(stage: str, error_type: str) -> None:
    """Bump the progress.flush_failures counter.

    ``stage`` must be ``'per_job'`` (one job's flush UPDATE failed — that
    job's progress since the last flush is lost) or ``'pool'`` (a pool
    could not be obtained at all — the loop-level getter failed, or the
    per-job acquire failed/exhausted — so every job's progress is lost).
    The two are materially different incidents and must stay
    distinguishable in an alert rule, which is also why both pool-stage
    sites log a different kind than the per-job one.
    ``error_type`` is the exception class name.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.progress.flush_failures",
        description=(
            "Progress flush failures, by stage. Attributes: stage "
            "('per_job' | 'pool'), error_type (exception class name)."
        ),
    ).add(1, {"stage": stage, "error_type": error_type})


def record_sub_enqueue_failure(actor: str, count: int, error_type: str | None = None) -> None:
    """Bump the sub_enqueue.failures counter by *count* failed child enqueues.

    Called at the post-commit flush catch site: the parent job has already
    been reported as succeeded, so every failed child enqueue is a job the
    caller believes exists but does not. ``actor`` is the parent's actor
    (bounded by the registered actor set). ``count`` is the number of
    child enqueues that failed, so one incident with N lost children
    records N, not 1. ``error_type`` is the exception class name; omitted,
    it derives from the exception being handled (``_resolve_error_type``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.sub_enqueue.failures",
        description=(
            "Sub-enqueue flush failures after the parent job committed; "
            "each counted unit is one child job that was reported as "
            "enqueued but was not. Attributes: actor (parent job's actor), "
            "error_type (exception class name — a closed set; see "
            "_resolve_error_type)."
        ),
    ).add(count, {"actor": actor, "error_type": _resolve_error_type(error_type)})


_ratelimit_refund_failures = get_meter().create_counter(
    "taskq.ratelimit.refund_failures",
    description=(
        "Rate-limit refund/rollback failures, labeled by bucket, backend, "
        "and error_type (exception class name — a closed set; see "
        "_resolve_error_type)."
    ),
    unit="1",
)


def record_ratelimit_refund_failure(
    bucket: str, backend: str, error_type: str | None = None
) -> None:
    """Bump the ratelimit.refund_failures counter.

    Called at the rate-limit refund failure catch site. ``error_type`` is
    the exception class name; omitted, it derives from the exception being
    handled (``_resolve_error_type``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _ratelimit_refund_failures.add(
        1,
        {
            "bucket": bucket,
            "backend": backend,
            "error_type": _resolve_error_type(error_type),
        },
    )


_lazy_counters: dict[str, tuple[Meter, Counter]] = {}
"""Memoized lazy-counter instruments: name → (owning meter, instrument)."""

_lazy_histograms: dict[str, tuple[Meter, Histogram]] = {}
"""The histogram sibling of ``_lazy_counters`` (see the note below)."""


def _cached_lazy_instrument[T: (Counter, Histogram)](
    name: str,
    cache: dict[str, tuple[Meter, T]],
    create: Callable[[Meter], T],
) -> T:
    """Return instrument *name* on the current meter, memoized per meter.

    An SDK ``Meter`` caches instruments by name, kind, description, and
    unit, so on an SDK-backed meter the pre-memo shape was already a dict
    lookup. The no-SDK ``_ProxyMeter`` caches nothing: every
    ``create_counter`` mints a fresh ``_ProxyCounter`` and appends it to a
    list with no cleanup path — so without this memo, the default
    deployment (no provider installed, which is taskq's own default)
    leaked one instrument per lazy-instrument call, growing through
    exactly the denial and flush-failure storms the counters exist to
    measure. The cache key carries the owning METER by identity, not just
    the name: a meter swap (the meter-isolating test fixtures patch
    ``get_meter`` per test) must mint a fresh instrument on the new meter
    so the isolated reader sees the counts; a stale entry is replaced on
    the first call after the swap. Emitter-thread only — nothing iterates
    these dicts on the SDK reader thread, so the rebind discipline the
    gauge caches follow does not apply.
    """
    meter = get_meter()
    cached = cache.get(name)
    if cached is not None and cached[0] is meter:
        return cached[1]
    instrument = create(meter)
    cache[name] = (meter, instrument)
    return instrument


def _lazy_counter(name: str, *, description: str) -> Counter:
    """Create-or-lookup counter *name* on the CURRENT global meter provider.

    Unlike the module-level singletons in this file, instruments created
    through this helper are resolved at call time, because the singleton
    pattern freezes whatever meter provider was global at import: an
    application that configures its SDK after importing taskq (and the
    meter-isolating test harnesses, which swap ``get_meter`` per test)
    would never see these counts. Call-time resolution alone is not
    enough — the no-SDK proxy meter caches nothing, so the instrument is
    memoized per (meter identity, name); see
    :func:`_cached_lazy_instrument` for why that exact key.
    """
    return _cached_lazy_instrument(
        name,
        _lazy_counters,
        lambda meter: meter.create_counter(name, description=description, unit="1"),
    )


def _lazy_histogram(name: str, *, description: str, unit: str) -> Histogram:
    """The histogram sibling of :func:`_lazy_counter` (see there for why)."""
    return _cached_lazy_instrument(
        name,
        _lazy_histograms,
        lambda meter: meter.create_histogram(name, description=description, unit=unit),
    )


def record_ratelimit_denial(backend: str) -> None:
    """Bump the ratelimit.denials counter.

    Called from the rate-limit decision logger whenever a decision denies
    admission. ``backend`` is the rate-limit backend enum (bounded). The
    bucket name is deliberately NOT a dimension: keyed bucket names are
    caller-derived and unbounded, so a bucket label would reintroduce the
    cardinality class the queue-label cap exists to prevent.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.ratelimit.denials",
        description=(
            "Rate-limit decisions that denied admission. Attributes: "
            "backend (rate-limit backend). Bucket names are not a "
            "dimension (caller-controlled cardinality)."
        ),
    ).add(1, {"backend": backend})


def record_job_interrupted(actor: str, *, held: bool) -> None:
    """Bump the jobs.interrupted counter.

    Called when a worker shutdown releases a running attempt back to the
    fleet (``mark_interrupted`` landed). ``held`` buckets the release by
    whether the row was parked behind a hold (the actor was still running
    when the graces expired) or re-pended immediately — the split an
    operator reads to see whether deploys are interrupting responsive or
    unresponsive actors. Attributes: actor, hold ("0" | ">0").
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.jobs.interrupted",
        description=(
            "Running attempts released back to the fleet by a worker "
            "shutdown (the claim's attempt increment is refunded). "
            "Attributes: actor, hold ('0' | '>0')."
        ),
    ).add(1, {"actor": actor, "hold": ">0" if held else "0"})


def record_job_interrupted_noop(actor: str | None) -> None:
    """Bump the jobs.interrupted_noop counter.

    Called when ``mark_interrupted``'s fence declines the release (the row
    moved: a reclaim, a terminal write, or an operator cancel in flight
    owns it). A silent no-op here is the failure mode the project rule
    names — an interruption that looks released but never landed — so the
    fenced-out path is instrumented alongside the success path.
    ``actor`` is None when the fenced-out read cannot attribute one.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.jobs.interrupted_noop",
        description=(
            "mark_interrupted calls declined by the fence (row not "
            "running-owned at the attempt epoch, or an operator cancel in "
            "flight). Attributes: actor (when attributable)."
        ),
    ).add(1, {"actor": actor if actor is not None else ""})


def record_enqueue_dedup(dedup_reason: str) -> None:
    """Bump the enqueue.dedups counter.

    Called from the shared dedup-report helper
    (``backend/_enqueue.py::_log_enqueue_dedup``) at every dedup hit, on
    both backends — the log lines are per-hit observability, and the
    per-hit terminal-target WARNING arm is budget-bounded at batch
    scale, so the RATE a stampede produces has no log channel left to
    ride on; this counter is that rate. ``dedup_reason`` is the bounded
    enum of reasons a hit can occur (``unique_for`` |
    ``idempotency_key``) — the same value the helper logs, never a
    caller-controlled string.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.enqueue.dedups",
        description=(
            "Enqueue dedup hits (an enqueue returned an existing row instead "
            "of writing one). Attributes: dedup_reason ('unique_for' | "
            "'idempotency_key'). The per-hit log lines are budget-bounded at "
            "batch scale; this counter is the rate signal that survives the "
            "bound."
        ),
    ).add(1, {"dedup_reason": dedup_reason})


def record_ratelimit_acquire_dependency_failure(error_type: str) -> None:
    """Bump the ratelimit.acquire_dependency_failures counter.

    Called when a rate-limit acquire fails because the limiter's store —
    Redis, or the PG fallback behind it — could not answer, and the
    worker failed the acquire closed as a denial. An AVAILABILITY
    signal, distinct from ``taskq.reservation.denials``, which counts
    admission decisions: a denial with this counter rising is an outage
    masquerading as contention, and an operator must read the two
    together before scaling a bucket. ``error_type`` is the exception
    class name.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.ratelimit.acquire_dependency_failures",
        description=(
            "Rate-limit acquires that failed on a store dependency (Redis "
            "or the PG fallback) and were failed closed as denials — an "
            "availability signal, distinct from reservation.denials "
            "(admission decisions). Attributes: error_type (exception "
            "class name)."
        ),
    ).add(1, {"error_type": error_type})


def record_reservation_denial(bucket_name: str, source: str) -> None:
    """Bump the reservation.denials counter.

    Called at the reservation-class denial handler for every
    ``ReservationUnavailable`` a worker fields. ``source`` is
    ``'reservation'`` or ``'rate_limit'`` (bounded). ``bucket_name`` is
    accepted so the call site reads naturally but is deliberately NOT a
    dimension: keyed bucket names are caller-derived and unbounded, so
    the label would reintroduce the cardinality class the queue-label cap
    exists to prevent.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    del bucket_name  # Why: not a dimension -- see the cardinality note above.
    _lazy_counter(
        "taskq.reservation.denials",
        description=(
            "Reservation/rate-limit admission denials surfaced to a worker "
            "handler. Attributes: source ('reservation' | 'rate_limit'). "
            "Bucket names are not a dimension (caller-controlled "
            "cardinality)."
        ),
    ).add(1, {"source": source})


def record_reservation_reclaim_drain_failure(error_type: str) -> None:
    """Bump the ratelimit.reclaim_drain_failures counter.

    Called when the keyed-reservation slot-row reclaim drain fails. A
    persistently failing drain strands ``reservation_slots`` rows — a
    STORAGE signal, and the pending-depth gauge shows the backlog
    forming. Distinct from heal failures, which are an AVAILABILITY
    signal; the two must not share a counter.
    ``error_type`` is the exception class name.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.ratelimit.reclaim_drain_failures",
        description=(
            "Failures of the keyed-reservation slot-row reclaim drain (a "
            "failed drain strands reservation_slots rows -- storage). "
            "Attributes: error_type. Distinct from reclaim_heal_failures "
            "(availability)."
        ),
    ).add(1, {"error_type": error_type})


def record_reservation_reclaim_drain_duration(elapsed_seconds: float) -> None:
    """Record the wall-clock duration of one reclaim drain statement.

    Recorded on success and failure alike (callers pass it from a
    ``finally``), so a timeout that aborted the drain still leaves a
    duration sample.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_histogram(
        "taskq.ratelimit.reclaim_drain_duration",
        description="Wall-clock duration of one keyed-reservation reclaim drain statement.",
        unit="s",
    ).record(elapsed_seconds)


def record_reservation_reclaim_drain_rows(rows: int) -> None:
    """Count rows deleted by one keyed-reclaim drain — ``reservation_slots``
    slot rows and ``rate_limit_buckets`` bucket rows alike.

    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.ratelimit.reclaim_drain_rows",
        description=(
            "Rows deleted by the keyed-reclaim drain — reservation_slots and "
            "rate_limit_buckets alike (RETURNING-confirmed)."
        ),
    ).add(rows)


def record_reservation_reclaim_heal_failure(error_type: str) -> None:
    """Bump the ratelimit.reclaim_heal_failures counter.

    Called when the acquire-path re-materialisation heal for a keyed
    bucket whose slot rows were deleted by a sibling worker's drain
    fails. A failing heal denies new admissions for that bucket — an
    AVAILABILITY signal, the opposite of a drain failure (storage); the
    two must not share a counter.
    ``error_type`` is the exception class name.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _lazy_counter(
        "taskq.ratelimit.reclaim_heal_failures",
        description=(
            "Failures of the acquire-path re-materialisation heal for keyed "
            "buckets whose slot rows a sibling worker's drain deleted (a "
            "failed heal denies new admissions -- availability). "
            "Attributes: error_type. Distinct from reclaim_drain_failures "
            "(storage)."
        ),
    ).add(1, {"error_type": error_type})


_keyed_reclaim_pending: int = 0


def update_keyed_reclaim_pending(depth: int) -> None:
    """Replace the pending-reclaim depth (evicted bucket names waiting for
    the next drain tick).

    A persistently non-zero value alongside a rising drain-failure
    counter is the visible signature of broken reclamation; the depth is
    a scalar because bucket names are caller-controlled and must not
    become label cardinality.
    """
    global _keyed_reclaim_pending
    _keyed_reclaim_pending = depth


def _observe_keyed_reclaim_pending(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(_keyed_reclaim_pending)


_keyed_reclaim_pending_gauge = get_meter().create_observable_gauge(
    name="taskq.ratelimit.reclaim_pending",
    description=(
        "Evicted keyed bucket names — reservations and rate limits alike — "
        "waiting for their rows (slot or bucket) to be reclaimed by the next "
        "drain tick (scalar: bucket names are not a dimension)."
    ),
    unit="1",
    callbacks=[_observe_keyed_reclaim_pending],
)


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


#: Why the ``actor`` label on the cron failure balance is capped
#: ------------------------------------------------------------
#: Every other actor-labeled instrument receives actors that flowed
#: through registration: the job-side emitters get ``ActorRef`` names
#: (``JobsClient.enqueue`` and its batch variants only accept registered
#: refs), and the cron loop's success/suppression paths
#: (:func:`record_published_message`, :func:`record_backpressure_error`)
#: only emit after the tick resolved the actor against ``actor_config``.
#: :func:`record_cron_failure`'s FAILURE path cannot lean on that: it
#: emits the raw ``cron_schedules.actor`` string, and ``create_schedule``
#: accepts any string at creation time (validation is deferred to fire
#: time by design -- a schedule may legitimately reference an actor that
#: registers later).  A dangling, misspelled or tenant-generated actor
#: name then fails every tick's planning loop, each failure adding +1
#: under its own arbitrary string -- one metric series per distinct
#: string, unbounded, the exact OTLP cardinality failure the module
#: header warns about.  The label is therefore admitted through the same
#: first-N-then-overflow mechanism as ``queue`` (see the note above
#: ``_bounded_queue``): the first ``_MAX_ACTOR_LABEL_VALUES`` distinct
#: names a process sees keep their own series, later names collapse onto
#: the fixed ``_other_`` value, and the real name still rides the
#: ``cron fired`` / ``cron fire failed`` log lines and the cron-fire
#: span, where cardinality is free.

_MAX_ACTOR_LABEL_VALUES: int = 100
_ACTOR_LABEL_OVERFLOW: str = "_other_"

_cron_actor_label_values: set[str] = set()


def _bounded_cron_actor(actor: str) -> str:
    """Return *actor*, or the fixed overflow label once the cap is reached.

    See the cardinality note above.
    """
    return _admitted_label_value(
        _cron_actor_label_values, actor, _MAX_ACTOR_LABEL_VALUES, _ACTOR_LABEL_OVERFLOW
    )


_cron_consecutive_failures = get_meter().create_up_down_counter(
    "taskq.cron.consecutive_failures",
    description=(
        "Cron execution failures currently outstanding per actor: the SUM "
        "of cron_schedules.consecutive_failures over the actor's schedules. "
        "Each tick reconciles the series against that sum over the whole "
        "table, so enables, disables and deletes performed by any process "
        "self-correct on the next tick with due work and the value returns "
        "to zero once no schedule is failing. The actor label is capped "
        "at the first 100 distinct names per process (overflow collapses "
        "to '_other_'). Per-schedule attribution is on the cron fired / "
        "cron fire failed log lines and the cron-fire span's per-schedule "
        "identity attribute, not on this label -- see the cardinality note "
        "above _lock_expires_in_seconds."
    ),
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


_cron_budget_deferrals = get_meter().create_counter(
    "taskq.cron.budget_deferrals",
    unit="1",
    description=(
        "Cron fires deferred because the tick's funded factory budget had no "
        "fundable grant left for them: a schedule planned ahead consumed the "
        "budget, or the leftover fell below the minimum fundable grant. A "
        "brief burst is catch-up draining in tick-sized batches; a SUSTAINED "
        "rate means one schedule's payload factory is monopolizing the tick "
        "budget every tick — a slow-but-successful factory never strikes and "
        "never auto-disables, so its peers retry every tick without ever "
        "being funded (delayed, not lost: the deferral advances "
        "next_fire_at one leader tick and the owed slot stays inside the "
        "catch-up window). Resolve with the operator knobs, not a restart: "
        "tighten TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT below the monopolizing "
        "factory's duration (it then takes the strike-and-auto-disable path "
        "— the intended consequence), or raise "
        "TASKQ_DISPATCHER_COMMAND_TIMEOUT so the funded budget fits the "
        "monopolizer plus a fundable grant for its peers. Per-schedule "
        "attribution is on the cron-fire-budget-deferred log line, not on "
        "this label; the actor label is capped like "
        "taskq.cron.consecutive_failures (first 100 distinct names, "
        "overflow collapses to '_other_')."
    ),
)


def record_cron_budget_deferral(actor: str) -> None:
    """Count one cron fire deferred by the tick's factory budget.

    The alertable face of the budget-deferral path: the log event
    (``cron-fire-budget-deferred``) is unconditional and carries the
    per-schedule attribution; this counter is the series an operator
    alerts on when deferrals stop being transient.  See the counter's
    description for the sustained-rate diagnosis and the operator knobs.
    Labeled by ``actor``, admitted through the same cap as
    :func:`record_cron_failure` (schedule rows accept any string at
    creation time, so the label is bounded).
    Respects ``_otel_enabled``: no-op when False.
    """
    if not _otel_enabled:
        return
    label = _bounded_cron_actor(actor)
    _cron_budget_deferrals.add(1, {"actor": label})


def record_cron_failure(actor: str, delta: int) -> None:
    """Record a cron failure delta on the UpDownCounter.

    On failure, callers add ``+1`` per failure. On success, callers add
    ``-current_count`` for that schedule to reset the counter to zero —
    a simple ``add(-1)`` would leave a non-zero cumulative value if
    there were multiple consecutive failures.

    Labeled by ``actor``, admitted through the same cap as ``queue``:
    the failure path emits the raw ``cron_schedules.actor`` string and
    schedule rows accept any string at creation time, so the first
    ``_MAX_ACTOR_LABEL_VALUES`` distinct names keep their series and
    later names collapse onto ``_other_`` (see the cardinality note
    above ``_bounded_cron_actor``).  Schedules on one actor share a
    series, and the per-schedule attribution the caller already holds
    rides on the ``cron fired`` / ``cron fire failed`` log lines and the
    cron-fire span instead — ``schedule_id`` is a per-row,
    runtime-minted UUID and identity-like (see the cardinality note
    above ``_lock_expires_in_seconds``).
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    label = _bounded_cron_actor(actor)
    _cron_consecutive_failures.add(delta, {"actor": label})
    level = _cron_failure_level()
    level[label] = level.get(label, 0) + delta


_cron_failure_levels: "WeakKeyDictionary[object, dict[str, int]]" = WeakKeyDictionary()
"""Per-instrument record of the value put on each actor's failure series.

An UpDownCounter only accepts deltas, so reconciling it against the
database's own count needs the level the deltas have reached — see
:func:`reconcile_cron_failures`.  Keyed by the instrument because the
level describes THAT instrument's series: a fresh instrument starts from
zero and must not inherit a level accumulated on another one.
"""


def _cron_failure_level() -> dict[str, int]:
    """The level ledger for the failure counter currently installed."""
    level = _cron_failure_levels.get(_cron_consecutive_failures)
    if level is None:
        level = {}
        _cron_failure_levels[_cron_consecutive_failures] = level
    return level


def reconcile_cron_failures(totals: Mapping[str, int]) -> None:
    """Move each actor's failure series onto *totals*, the database's own
    summed ``cron_schedules.consecutive_failures`` per actor.

    The series answers "is any schedule failing right now", and only the
    database knows.  Schedules are enabled, disabled and deleted by
    clients, by the CLI and by the admin UI, every one of them outside
    the process that emits this metric, and each of those actions clears
    or removes a count some worker counted up.  A worker cannot emit
    another process's delta, so a balance built from this process's
    deltas alone can only drift upward: an actor whose last failing
    schedule was deleted a month ago would report a failure level
    forever, and an alert on the series becomes unreadable exactly when
    an operator needs it.  Reconciling against the database each tick
    makes every out-of-process change self-correct on the next tick with
    due work.

    *totals* must be the complete per-actor truth — every actor with a
    failing schedule anywhere in the table (see ``_actor_failure_totals``),
    not the slice one tick's batch happened to touch.  A batch covers only
    DUE schedules, so an actor whose failing schedule was deleted or
    disabled with nothing left due never appears in a batch again; under a
    batch-scoped *totals* its level would strand at its last value.  An
    actor ABSENT from *totals* therefore reads as a true zero — the
    database holds no failing schedule for it — and its series is returned
    to zero here.

    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    wanted: dict[str, int] = {}
    for actor, total in totals.items():
        label = _bounded_cron_actor(actor)
        # Distinct actors can share the overflow label, so their totals
        # sum onto the one series they share.
        wanted[label] = wanted.get(label, 0) + total
    level = _cron_failure_level()
    for label, target in wanted.items():
        delta = target - level.get(label, 0)
        if delta:
            _cron_consecutive_failures.add(delta, {"actor": label})
        level[label] = target
    # *totals* is the complete truth, so a label the database no longer
    # reports is a schedule set that stopped failing somewhere else —
    # return it to zero rather than stranding its last level.
    for label in list(level):
        if label not in wanted:
            stranded = level.pop(label)
            if stranded:
                _cron_consecutive_failures.add(-stranded, {"actor": label})


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
    """Count a sweep or sampler read that did not complete.

    Called on the failure path, never the success path. A sweep reports the
    deadline family (``TimeoutError`` / ``QueryCanceledError``), where the
    distinction between aborted and merely slow is the actionable one; a
    gauge sampler reports every failure, because its gauge keeps serving its
    last value either way and the read not happening is the whole fault.
    Respects ``_otel_enabled`` — no-op when False.
    """
    if not _otel_enabled:
        return
    _sweep_timeouts.add(1, {"sweep_name": sweep_name})


_sweep_timeouts = get_meter().create_counter(
    "taskq.maintenance_leader.sweep_timeouts",
    description=(
        "Sweep calls aborted by a deadline or server-side statement cancel, "
        "and gauge-sampler reads that did not complete for ANY reason (the "
        "sampler sweep_names are queue_depth / backlog_detection / "
        "actor_backlog / reservation_slots), labeled by sweep_name. A "
        "non-zero rate means work is being aborted or going unobserved, "
        "not completing slowly."
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


_leader_lease_expires_in_seconds_cache: float | None = None


def record_leader_lease_expires_in_seconds(worker_id: str, remaining_ttl: float) -> None:
    """Stamp the leader lease's TTL as of this process's last elect/renew.

    The leader-side mirror of :func:`record_lock_expires_in_seconds`
    (heartbeat.py) — but a gauge, not a histogram, because the contract
    that matters for the lease is FRESHNESS, not distribution: the series
    is present only on the pod that holds the lease, its value is the
    TTL the server just stamped (``leader_lease`` seconds out), and a
    series that stops moving or vanishes is a leader that stopped
    renewing — the split-brain/no-leader detector's per-pod evidence.
    Called at election win and each successful renewal in
    ``worker/leader.py``; cleared on demotion by
    :func:`clear_leader_lease_expires_in_seconds` so a demoted pod never
    keeps claiming a lease it no longer holds.
    Respects ``_otel_enabled`` — no-op when False.

    Rebind, never write in place: the cache is read on the OTel reader
    thread while this runs on the event-loop thread (same discipline as
    :func:`record_sweep_success`).
    """
    if not _otel_enabled:
        return
    del worker_id  # Why: not a dimension -- see the cardinality note above.
    global _leader_lease_expires_in_seconds_cache
    _leader_lease_expires_in_seconds_cache = remaining_ttl


def _observe_leader_lease_expires_in_seconds(options: CallbackOptions) -> Iterable[Observation]:
    # Label-free, like the other single-value process gauges
    # (oldest_due_age, scheduled_count): one series per pod, and only
    # while this pod holds the lease.
    if _leader_lease_expires_in_seconds_cache is not None:
        yield Observation(_leader_lease_expires_in_seconds_cache)


_leader_lease_expires_in_seconds_gauge = get_meter().create_observable_gauge(
    name="taskq.maintenance_leader.lease_expires_in_seconds",
    description=(
        "Seconds until the leader lease expires, as stamped by this "
        "process's last successful election or renewal. Present only on "
        "the pod holding the lease; a series that goes stale or vanishes "
        "is a leader that stopped renewing."
    ),
    unit="s",
    callbacks=[_observe_leader_lease_expires_in_seconds],
)


def clear_leader_lease_expires_in_seconds() -> None:
    """Drop this process's lease-TTL stamp.

    Called on leadership demotion with the other leader-only clears: the
    stamp is this process's report of a lease IT holds, and a demoted
    process exporting a frozen one claims authority it no longer has —
    during a failover, which is exactly when the failover bound is being
    read. Rebound to the empty state rather than zeroed: an empty gauge
    yields no data point, so the series goes stale and the new leader's
    is the only one answering.
    """
    global _leader_lease_expires_in_seconds_cache
    _leader_lease_expires_in_seconds_cache = None


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
# These gauges are the backlog-stall detectors: a scheduled backlog that
# stops moving is invisible in a merged pending+scheduled count and in
# absolute depth thresholds. They are sampled UNCONDITIONALLY (every
# worker, not leader-gated) on purpose: a detector hosted behind the
# leadership gate emits nothing under exactly the failure (a second
# schema's lock) that mutes every leader-gated sampler.


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
    description=(
        "Jobs per live status, counted exactly and sampled by every "
        "worker; terminal statuses are not sampled."
    ),
    unit="1",
    callbacks=[_observe_jobs_by_status],
)


# ── Per-actor backlog attribution ─────────────────────────────────────
#
# No worker refuses to start because an actor's queue has no consumer: in
# a multi-worker fleet no single supervisor can know what consumes a
# queue. A misrouted actor therefore produces no refusal, no error and no
# failed job — its rows pile up pending while every probe stays green, so
# monitoring is the only place that condition can surface, and only at
# actor granularity. A queue-summed depth cannot separate "one actor on
# this queue is never consumed" from "this queue is busy", and the
# fleet-wide oldest-DUE-age gauge measures promotion (scheduled →
# pending), reading 0.0 for pending work nobody takes.
#
# Dimensions are exactly (actor, queue): both are bounded by the
# deployment's own registration, and nothing identity-like — job, worker
# or schedule id — may ride along, or the series that exists to be
# alerted on becomes the series that cannot be stored.


def update_actor_backlog_cache(data: dict[tuple[str, str], int]) -> None:
    """Replace the per-(actor, queue) pending-depth cache with fresh data."""
    global _actor_backlog_cache
    _actor_backlog_cache = dict(data)


def _observe_actor_backlog(options: CallbackOptions) -> Iterable[Observation]:
    for (actor, queue), depth in _actor_backlog_cache.items():
        yield Observation(depth, {"actor": actor, "queue": queue})


_actor_backlog_cache: dict[tuple[str, str], int] = {}

get_meter().create_observable_gauge(
    name="taskq.jobs.actor_backlog",
    description=(
        "Pending jobs per (actor, queue). An actor whose jobs are never "
        "consumed is the series that rises without bound while its "
        "queue-mates stay flat."
    ),
    unit="1",
    callbacks=[_observe_actor_backlog],
)


def update_jobs_running_cache(data: Mapping[str, int]) -> None:
    """Replace the per-actor running-count cache with fresh data.

    Fed by the backlog sampler from one grouped read over the running
    population. Per actor, not per (actor, queue): the running row's
    queue label is not what dispatched it (re-pended rows route by the
    actor's assignment), and the capacity question is per actor —
    which actors hold the fleet's slots while pending work waits. An
    actor with no running jobs vanishes from the series rather than
    freezing at its last count.
    """
    global _jobs_running_cache
    _jobs_running_cache = dict(data)


def _observe_jobs_running(options: CallbackOptions) -> Iterable[Observation]:
    for actor, count in _jobs_running_cache.items():
        yield Observation(count, {"actor": actor})


_jobs_running_cache: dict[str, int] = {}

get_meter().create_observable_gauge(
    name="taskq.jobs.running",
    description=(
        "Running jobs per actor, sampled by every worker with taskq.jobs.by_status. "
        "Beside taskq.worker.active_jobs (per process) and "
        "taskq.jobs.oldest_pending_age_seconds, says which actors hold the "
        "fleet's slots while pending work waits."
    ),
    unit="1",
    callbacks=[_observe_jobs_running],
)


def update_actor_oldest_running_age_cache(data: Mapping[str, float]) -> None:
    """Replace the per-actor oldest-running-age cache with fresh data.

    Fed by the backlog sampler from the same grouped read as
    :func:`update_jobs_running_cache`, so count and age describe one
    moment. An attempt older than the actor's normal runtime while
    ``taskq.jobs.timeouts`` stays flat is an actor with no
    ``start_to_close``: nothing will end the attempt, and no other gauge
    can show it (``running_lease_expired`` reads 0 while the heartbeat
    keeps renewing). An actor with nothing running vanishes from the
    series.
    """
    global _actor_oldest_running_age_cache
    _actor_oldest_running_age_cache = dict(data)


def _observe_actor_oldest_running_age(options: CallbackOptions) -> Iterable[Observation]:
    for actor, age in _actor_oldest_running_age_cache.items():
        yield Observation(age, {"actor": actor})


_actor_oldest_running_age_cache: dict[str, float] = {}

get_meter().create_observable_gauge(
    name="taskq.jobs.oldest_running_age_seconds",
    description=(
        "Seconds since the oldest running attempt of each actor started, "
        "sampled by every worker with taskq.jobs.running. Past the actor's "
        "usual p99 with taskq.jobs.timeouts flat, it is an actor with no "
        "start_to_close."
    ),
    unit="s",
    callbacks=[_observe_actor_oldest_running_age],
)


def update_actor_oldest_pending_age_cache(data: dict[tuple[str, str], float]) -> None:
    """Replace the per-(actor, queue) oldest-pending-age cache with fresh data."""
    global _actor_oldest_pending_age_cache
    _actor_oldest_pending_age_cache = dict(data)


def _observe_actor_oldest_pending_age(options: CallbackOptions) -> Iterable[Observation]:
    for (actor, queue), age in _actor_oldest_pending_age_cache.items():
        yield Observation(age, {"actor": actor, "queue": queue})


_actor_oldest_pending_age_cache: dict[tuple[str, str], float] = {}

get_meter().create_observable_gauge(
    name="taskq.jobs.oldest_pending_age_seconds",
    description=(
        "Seconds since the oldest PENDING job became eligible, per (actor, "
        "queue). Depth alone is ambiguous — a deep queue that drains is "
        "healthy throughput — but an actor nobody consumes has a pending "
        "job whose age grows with wall clock."
    ),
    unit="s",
    callbacks=[_observe_actor_oldest_pending_age],
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


def update_scheduled_count_cache(count: int) -> None:
    """Record the current count of `scheduled`-status jobs.

    Emitted label-less (unlike `taskq.jobs.by_status`, which carries a
    `status` label for every status) specifically so it joins on identical
    label sets with `taskq.jobs.oldest_due_age_seconds` — also label-less —
    in `TaskQScheduledBacklogGrowing`. That alert needs both "the oldest
    due job has waited a long time" AND "the scheduled count is actually
    rising", not the age of a single straggling job (which climbs
    monotonically toward its own promotion regardless of how healthily
    everything behind it drains). A vector `and`/comparison between two
    `taskq_*` series with mismatched label sets is a silent no-op join —
    valid PromQL that can never produce a result — so this gauge exists
    to keep the two operands directly comparable without a join modifier.
    """
    global _scheduled_count
    _scheduled_count = count


def _observe_scheduled_count(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(_scheduled_count)


_scheduled_count: int = 0

get_meter().create_observable_gauge(
    name="taskq.jobs.scheduled_count",
    description=(
        "Count of jobs currently in `scheduled` status, sampled by the "
        "backlog detection leader. Label-less twin of "
        '`taskq.jobs.by_status{status="scheduled"}`, kept in step with it '
        "so TaskQScheduledBacklogGrowing can compare it against "
        "`taskq.jobs.oldest_due_age_seconds` (also label-less) without a "
        "PromQL join modifier."
    ),
    unit="1",
    callbacks=[_observe_scheduled_count],
)


def update_running_lease_expired_cache(count: int) -> None:
    """Record the count of running jobs whose lock lease is past.

    Fed by the backlog sampler (one statement beside jobs-by-status and
    oldest-due-age). The zombie-running shape — work claimed, lease
    expired, row still 'running' — is invisible in jobs.by_status (it
    counts as a healthy running job) and in the miss counters (a dead
    worker emits nothing): this gauge is the direct count. A healthy
    fleet reads 0 (the reclaim sweep drains expired leases within a tick
    or two of expiry), so a SUSTAINED non-zero reading means reclaim is
    not draining. Rows with a cancel in flight (cancel_phase != 0) are
    carved out: the reclaim sweep deliberately waits out the cancel grace
    ladder for them, so their lease expiring mid-cancel is the protocol
    working, not a zombie: a cancel that never completes pages
    elsewhere (TaskQAbandonedJobs when its worker is alive to escalate
    through the phases, TaskQHeartbeatMisses when it died mid-cancel;
    reclaim honors the row to 'cancelled' either way). No dimensions:
    the fleet total is the alertable shape, and the per-job truth
    (locked_by_worker, lock_expires_at) lives on the row and the admin
    jobs page, not on a label.
    """
    global _running_lease_expired_count
    _running_lease_expired_count = count


def _observe_running_lease_expired(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(_running_lease_expired_count)


_running_lease_expired_count: int = 0

_running_lease_expired_gauge = get_meter().create_observable_gauge(
    name="taskq.jobs.running_lease_expired",
    description=(
        "Running jobs whose lock lease is past expiry (the zombie-running "
        "shape), with rows in a cancel phase (cancel_phase != 0) carved out "
        "— the reclaim sweep deliberately waits out the cancel grace ladder "
        "for those, so an expired lease mid-cancel is the protocol working, "
        "not a zombie; a cancel that never completes pages elsewhere "
        "(TaskQAbandonedJobs when its worker is alive to escalate, "
        "TaskQHeartbeatMisses when it died mid-cancel — reclaim honors the "
        "row to 'cancelled' either way). Healthy reads 0 — the reclaim "
        "sweep drains expired leases within a tick or two — so a sustained "
        "non-zero reading means reclaim is not draining. Sampled by every "
        "worker with taskq.jobs.by_status."
    ),
    unit="1",
    callbacks=[_observe_running_lease_expired],
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
