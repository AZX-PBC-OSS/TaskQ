"""Structlog configuration and logger accessor.

Provides the canonical processor chain, OTel span context injection, and
the ``get_logger`` helper that returns a typed ``structlog.stdlib.BoundLogger``
instead of the ``Any`` that ``structlog.get_logger`` returns.

## The operational-event-stream verbosity knob (``TASKQ_LOG_EVENTS_LEVEL``)

Every state change a worker makes is durably recorded in the ``job_events``
ledger FIRST (the terminal write and the heartbeat ledger writes commit the
event row in the same transaction as the state itself). The JSON log line is
a STREAMING DUPLICATE of that ledger row: it costs the full structlog chain
plus a stream write on the event loop, and the hot-path audit measured that
duplicated per-job logging at ~23% of the worker's on-CPU time (two
``state-change`` INFO lines per job through structlog -> stdlib -> JSON).
This module implements the knob that trades that duplicate stream away
WITHOUT ever touching the durable ledger: ``job_events`` keeps every event
at every level, and the call sites stay unconditional — the filter below
decides, so a call site can never drift from the setting.

The setting is ``TASKQ_LOG_EVENTS_LEVEL`` (``WorkerSettings.log_events_level``,
wired through :func:`setup_logging`'s ``events_level`` parameter by the
worker bootstrap and the CLI). Four values, case-insensitive:

``info`` (the DEFAULT)
    Today's behavior, byte-identical: nothing is dropped except the
    DEBUG_ONLY internals (which do not exist in the pre-knob stream at
    all — they are new call sites that only ``debug`` reveals). A
    deployment that never sets the knob cannot observe any difference.

 ``warning``
    The failure/anomaly stream only: the failed ticks, the isolates, the
    watchdog trips, the backpressure refusals. The happy-path per-job
    lines (``state-change``, ``cancel_phase_change``) and the
    once-per-process lifecycle INFO lines are dropped. Anomaly events
    survive EVEN WHEN logged at INFO (``cron-fire-budget-deferred``,
    ``cron-schedule-auto-disable-reverted`` — both emitted at INFO by
    the cron loop), because the classification is by event name, not by
    stdlib level — the operator who drops to ``warning`` asked the happy
    path to be quiet, not the anomalies. (The RECLAIMS are the one
    documented exception, argued below: a successful reclaim's
    completion line is emitted at stdlib DEBUG, so no level reveals it.)
    Unclassified events pass here too (fail open).

 ``off``
    For deployments consuming OTel spans or tailing ``job_events``
    directly: the state-change lines are gone entirely, and the
    INFO-method lines of CLASSIFIED anomalies go too. WARNING-and-above
    anomalies STILL emit (``job-failed``, ``heartbeat-tick-failure``,
    ``terminal-write-failed``, ``worker-watchdog-trip``): ``off`` turns
    off the event STREAM, never the operator's sight of failures.
    Unclassified events FAIL OPEN here as at every other level: they
    pass at every method. (The first cut of this filter dropped every
    INFO/DEBUG-method event at ``off``, classified or not — which
    silently failed CLOSED on unclassified INFO events and broke this
    contract. Found by red-team; fixed: the INFO-method drop now applies
    only to names the classification table has actually met.)
``debug``
    Everything ``info`` emits PLUS the DEBUG_ONLY per-tick internals:
    the loop-lag traces and the poll-cadence details (see the
    classification table).

### The classification table

The filter classifies by exact event name (prefix-free: a future event
that is not classified FAILS OPEN — it passes at every level, so a newly
added event can never be silently suppressed by a stale table; the
failure-visibility bias is the contract).

=====================================  ==========  ===========================
Event (family)                         Class       Rationale / boundary case
=====================================  ==========  ===========================
``state-change``                       HAPPY       The per-job duplicate of
                                                   the ``job_events`` row the
                                                   SAME transaction just
                                                   committed; the ledger is
                                                   the audit trail, the log
                                                   line is the copy.
``cancel_phase_change``                HAPPY       Same ledger-duplicate
                                                   shape as ``state-change``
                                                   (cancel phases are
                                                   recorded on the job row /
                                                   events ledger).
``heartbeat-tick-success``             HAPPY       Per-tick duplicate;
                                                   already stdlib DEBUG, so
                                                   the default root level
                                                   drops it before this
                                                   filter runs.
``heartbeat-post-tx-deferred``         HAPPY       Per-tick internal
                                                   (stdlib DEBUG).
``dispatch``                           HAPPY       The per-batch claim
                                                   summary on the hot loop;
                                                   the claim's per-job
                                                   state-changes are already
                                                   ledgered.
``prune-completed``, ``batches         HAPPY       Daily maintenance
pruned``, ``archive-expiry-                         completions (the happy
completed``, ``keepalive-                           path; failures of the
applied``                                           same sweeps classify
                                                    ANOMALY).
lifecycle INFO lines                   HAPPY       Once per process, not per
(``health-server-*``,                              job, but they are the
``pool-draining``,                                 happy path; ``warning``
``pool-using-provided``,                           promises ONLY the
``producer-*``,                                    anomaly stream.
``drain-monitor-exit``,
``workgroup-shutdown-*``,
``workgroup-reload-signal``,
``notify-listen-issued``,
``keepalive-skipped``,
``credential-reload-skipped``,
``di-worker-pool-refreshed``,
``otel-exporter-*`` INFO,
``stale-batches-completed``,
``sweep-evicted-*``,
``cron-schedule-registered``,
``cron-schedule-already-registered``,
``health-http-server-started``)
``isolate-self-*``                     ANOMALY     A worker removed itself
                                                   from the fleet — an
                                                   operator event even at
                                                   INFO (``-complete``).
``sweep-drained-pending-               ANOMALY     The reclaims: rows came
``reservation-reclaims``                           back from a dead worker.
``job-failed``                         ANOMALY     stdlib ERROR; survives at
                                                   every level.
``heartbeat-tick-failure``             ANOMALY     The failed ticks.
``heartbeat-tick-unexpected-error``    ANOMALY     stdlib ERROR.
``heartbeat-post-tx-failure``          ANOMALY
``heartbeat-hook-failure``             ANOMALY
``terminal-write-retry``               ANOMALY     Infra backpressure on the
``terminal-write-retry-                            write path.
``budget-exhausted``,
``terminal-write-failed``,
``terminal-hook-row-reread-*``,
``job_timeout``, ``job_exception``
``rate-limit-dependency-failure``      ANOMALY     The backpressure
``consume-rate-limit-denied-noop``                 refusals (fail-closed
                                                   limiter outages; the
                                                   noop is stdlib DEBUG, the
                                                   root level gates it
                                                   before this filter).
``pool-release-failed``,               ANOMALY     Pool/conn anomalies
``pool-conn-dead-on-acquire``,                     (WARNING/ERROR: survive
``slot-pool-acquire-failed``,                      everywhere by the stdlib
``slot-conn-terminated-``                          rule; classified so the
``transaction-in-flight``,                         table is total).
``slot-pool-release-skipped-*``
``worker-watchdog-trip``               ANOMALY     stdlib CRITICAL; survives
                                                   at every level.
``watchdog-lag-thread-exited``         ANOMALY
``cron-tick-*`` (failed/timeout/       ANOMALY     The cron loop's failures
``transient/lock-contended``),                     and conn losses.
``cron-conn-lost``,
``cron-commit-gate-unavailable``,
``cron-fire-budget-deferred``,
``cron-schedule-drift``,
``cron-schedule-drift-check-failed``,
``cron-schedule-auto-disable-*``,
``cron-*-recovery-failed``
``sweep-*-failed``,                    ANOMALY     Sweep/infra failures.
``cleanup-stale-workers-failed``,
``stale-batches-sweep-failed``,
``batch-prune-failed``,
``*-drain-error``, ``*-drain-timeout``,
``pool-drain-error``, ``dev-worker-exit``,
``dev-import-failed``,
``*-handler-unavailable``,
``drain-monitor-count-error``,
``health-readiness-check-error``,
``health-slot-pool-ping-unexpected``,
``workgroup-health-kill``,
``workgroup-background-task-failed``,
``register-worker-failed``,
``otel-exporter-unavailable``,
``otel-metric-record-failed``,
``otel-span-*-failed``,
``scoped-idempotency-migration-``
``pending-batch``,
``notify-payload-parse-failed``,
``heartbeat-post-tx-failure``
``loop-lag``                           DEBUG_ONLY  The loop scheduler-lag
``poll-cadence``                                   trace and the producer's
                                                   poll-cadence detail: new
                                                   per-tick call sites that
                                                   exist ONLY for ``debug``,
                                                   emitted at stdlib INFO so
                                                   this filter (not the root
                                                   level) owns their
                                                   visibility. Dropped at
                                                   ``info``/``warning``/``off``.
=====================================  ==========  ===========================

Boundary cases, argued:

- *job_events vs the log line*: the ledger row is committed inside the
  state-change transaction; the INFO line re-reports it to a different
  medium. Suppressing the line at ``warning``/``off`` deletes no
  information the ledger does not already hold durably — that is why
  ``state-change`` is the FIRST event classified HAPPY, and why the
  ledger is asserted untouched by the level tests
  (``tests/system_e2e/test_log_events_levels.py``).
- *isolate completions stay at ``warning``*: the mission for
  ``warning`` is "the failure/anomaly stream", and a worker isolating
  itself IS the anomaly, whatever stdlib level it happens to carry
  (``isolate-self-complete`` is emitted at WARNING by the heartbeat's
  shutdown path; ``isolate-self-failure`` too).
- *a successful reclaim has NO log line at any level*:
  ``sweep-drained-pending-reservation-reclaims`` is emitted at stdlib
  DEBUG (worker/_leader_sweeps.py), so the root INFO level gates it
  before this filter ever sees it — at ``warning``, at ``off``, and
  even at ``debug`` (the knob never raises the root level). A
  red-team kill -9/reclaim run proved it: the surviving worker's
  stream shows nothing about the reclaim at any knob setting. The
  ``job_events`` ledger rows are the reclaim's audit trail; the
  failed-reclaim WARNING (``sweep-drain-pending-reservation-reclaims-
  failed``) is the log-visible half. Do not promote the DEBUG line to
  fix this casually — it is per-sweep noise on the leader loop.
- *``off`` still emits WARNING+*: an operator tailing ``job_events`` or
  OTel spans has the event data, but a failing tick with no log line at
  all would blind the incident responder reading stderr. ``off`` is
  short for "off for the event stream", never "off for failures".

### Where the knob applies

The filter is a structlog processor (``_events_level_filter``) installed
in :func:`_shared_processors` immediately AFTER
``func:`structlog.stdlib.filter_by_level`` and, like it, deliberately
UNWRAPPED by ``_safe_processor_wrapper``: ``DropEvent`` is a
``BaseException`` the wrapper would let through anyway, and the filter's
whole job is to be the cheapest point BEFORE the serialization cost
(before ``merge_contextvars``, the renderers, the scrubbers — the entire
per-line cost the audit measured). It is a filter, not per-call-site
branching: the call sites stay unconditional, the classification table
decides, and a call site cannot drift from the setting.
"""

import hashlib
import logging
from uuid import UUID

import structlog
from opentelemetry import trace

from taskq._json import dumps
from taskq.obs._redact_exc import (
    EXCEPTION_MESSAGE_FIELDS,
    EXCEPTION_TRACEBACK_FIELDS,
    safe_exception_parts,
    scrub_exception_field,
)

__all__ = [
    "bind_job_context",
    "get_logger",
    "log_cancel_phase_change",
    "log_state_change",
    "redact_payload",
    "set_events_level",
    "setup_logging",
]

_EXCEPTION_FIELD_NAMES = EXCEPTION_MESSAGE_FIELDS | EXCEPTION_TRACEBACK_FIELDS

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.obs._structlog")

_logging_configured: bool = False

# ── The event-stream verbosity knob (TASKQ_LOG_EVENTS_LEVEL) ─────────
#
# The classification table. Full semantics, the boundary cases, and the
# level grid live in this module's docstring and configuration.md; these
# are the sets the filter consults. Membership is by EXACT event name
# (no prefixes): an event not in any set FAILS OPEN - it passes at every
# level, so a future event can never be silently suppressed by a stale
# table. The failure-visibility bias is the contract.
#
# HAPPY_PATH: the streaming duplicates of job_events rows and the
# lifecycle INFO lines. Dropped at `warning` and `off`.
_HAPPY_PATH_EVENTS: frozenset[str] = frozenset(
    {
        "state-change",
        "cancel_phase_change",
        "heartbeat-tick-success",
        "heartbeat-post-tx-deferred",
        "dispatch",
        # Sweep/prune/archive completions (their failures are ANOMALY,
        # below). The names are the EMITTED ones — the prune family's
        # completion lines are "prune-completed" and "batches pruned"
        # (worker/_leader_sweeps.py), the archive-expiry family's is
        # "archive-expiry-completed"; a table entry under a name no call
        # site emits is a dead classification (found by red-team).
        "prune-completed",
        "batches pruned",
        "archive-expiry-completed",
        "keepalive-applied",
        # Lifecycle INFO lines (once per process, but the happy path).
        "health-server-started",
        "health-server-stopped",
        "health-http-server-started",
        "pool-draining",
        "pool-using-provided",
        "producer-subscribed-wake",
        "producer-loop-exit",
        "drain-monitor-exit",
        "workgroup-shutdown-begin",
        "workgroup-shutdown-complete",
        "workgroup-shutdown-signal",
        "workgroup-reload-signal",
        "notify-listen-issued",
        "keepalive-skipped",
        "credential-reload-skipped",
        "di-worker-pool-refreshed",
        "otel-exporter-configured",
        "otel-exporter-autoconfigure-disabled",
        "otel-exporter-sdk-disabled",
        # Sweep completions (their failures are ANOMALY, below).
        "stale-batches-completed",
        "sweep-evicted-idle-keyed-reservations",
        "sweep-evicted-idle-keyed-rate-limits",
        # Cron ops completions.
        "cron-schedule-registered",
        "cron-schedule-already-registered",
    }
)

# ANOMALY: the failure/anomaly stream - failed ticks, isolates, reclaims,
# watchdog trips, backpressure refusals, sweep/cron failures. Kept at
# `warning` EVEN WHEN logged at INFO (the classification is by name, not
# stdlib level); at `off` only the WARNING-and-above of these survive.
_ANOMALY_EVENTS: frozenset[str] = frozenset(
    {
        "job-failed",
        "heartbeat-tick-failure",
        "heartbeat-tick-unexpected-error",
        "heartbeat-post-tx-failure",
        "heartbeat-hook-failure",
        "terminal-write-retry",
        "terminal-write-retry-budget-exhausted",
        "terminal-write-failed",
        "terminal-hook-row-reread-failed",
        "terminal-hook-row-reread-missing",
        "job_timeout",
        "job_exception",
        "rate-limit-dependency-failure",
        "consume-rate-limit-denied-noop",
        "isolate-self-actor-join-timeout",
        "isolate-self-failure",
        "isolate-self-complete",
        "sweep-drained-pending-reservation-reclaims",
        "worker-watchdog-trip",
        "watchdog-lag-thread-exited",
        "cron-tick-failed",
        "cron-tick-timeout",
        "cron-tick-transient",
        "cron-conn-lost",
        "cron-tick-lock-contended",
        "cron-commit-gate-unavailable",
        "cron-fire-budget-deferred",
        "cron-schedule-drift",
        "cron-schedule-drift-check-failed",
        "cron-schedule-auto-disable-reverted",
        "cron-schedule-auto-disable-reverted-at-takeover",
        "cron-schedule-takeover-recovery-failed",
        "cron-takeover-recovery-failed",
        "sweep-expired-locks-failed",
        "sweep-deadline-exceeded-failed",
        "sweep-leaked-slots-failed",
        "sweep-expired-results-failed",
        "sweep-job-events-retention-failed",
        "sweep-keyed-row-reclaim-failed",
        "sweep-evict-idle-keyed-reservations-failed",
        "sweep-evict-idle-keyed-rate-limits-failed",
        "sweep-drain-pending-reservation-reclaims-failed",
        "cleanup-stale-workers-failed",
        "stale-batches-sweep-failed",
        "batch-prune-failed",
        "conn-drain-error",
        "conn-drain-timeout",
        "redis-drain-timeout",
        "pool-drain-error",
        "pool-release-failed",
        "pool-conn-dead-on-acquire",
        "slot-pool-acquire-failed",
        "slot-conn-terminated-transaction-in-flight",
        "slot-pool-release-skipped-pool-closed",
        "slot-pool-release-skipped-conn-dead",
        "dev-worker-exit",
        "dev-import-failed",
        "sighup-handler-unavailable",
        "sigusr2-handler-unavailable",
        "drain-monitor-count-error",
        "health-readiness-check-error",
        "health-slot-pool-ping-unexpected",
        "workgroup-health-kill",
        "workgroup-background-task-failed",
        "register-worker-failed",
        "otel-exporter-unavailable",
        "otel-metric-record-failed",
        "otel-span-creation-failed",
        "otel-span-error-record-failed",
        "scoped-idempotency-migration-pending-batch",
        "notify-payload-parse-failed",
    }
)

# DEBUG_ONLY: the per-tick internals the `debug` level ADDS - new call
# sites (the loop scheduler-lag trace, the producer's poll-cadence
# detail) that exist for no other level. Emitted at stdlib INFO so THIS
# filter, not the root logger level, owns their visibility; dropped at
# info/warning/off, which is what keeps the default stream
# byte-identical to the pre-knob worker.
_DEBUG_ONLY_EVENTS: frozenset[str] = frozenset({"loop-lag", "poll-cadence"})

# The knob's level, read by the filter. Default `info`: today's behavior,
# byte-identical. Set through set_events_level (setup_logging wires the
# WorkerSettings.log_events_level value in).
_events_level: str = "info"

# The valid levels. Declared HERE too (settings.py's validator has its own
# copy): obs/_structlog must not import taskq.settings at module load, and
# the drift is pinned by tests/test_obs_log_events_level.py asserting the
# two frozensets equal.
_VALID_LOG_EVENTS_LEVELS: frozenset[str] = frozenset({"info", "warning", "off", "debug"})


def set_events_level(level: str) -> None:
    """Set the event-stream verbosity level (validated).

    Valid: ``info`` | ``warning`` | ``off`` | ``debug`` (case-insensitive).
    Raises ``ValueError`` on anything else so a typo cannot silently
    disable the filter's semantics.
    """
    global _events_level
    normalized = level.lower()
    if normalized not in _VALID_LOG_EVENTS_LEVELS:
        raise ValueError(
            f"log_events_level must be one of {sorted(_VALID_LOG_EVENTS_LEVELS)}, got {level!r}"
        )
    _events_level = normalized


def _events_level_filter(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Drop the classified events the configured level suppresses.

    The knob's application point: a structlog processor in the shared
    chain, immediately after ``filter_by_level`` and, like it,
    deliberately UNWRAPPED by ``_safe_processor_wrapper`` (``DropEvent``
    is a ``BaseException`` the wrapper would let through anyway, and the
    filter's whole job is to be the cheapest gate BEFORE the
    serialization cost). The call sites stay unconditional - this filter
    decides, so a call site cannot drift from the setting.

    Level grid (the full table is the module docstring's):

    - ``info``: passthrough except DEBUG_ONLY (byte-identical default);
    - ``warning``: HAPPY_PATH dropped, ANOMALY kept (any stdlib level),
      WARNING+ kept, unclassified kept (fail open);
    - ``off``: HAPPY_PATH dropped at any method, classified-ANOMALY
      INFO/DEBUG-method lines dropped, WARNING-and-above kept,
      unclassified kept at every method (fail open);
    - ``debug``: everything, DEBUG_ONLY included.
    """
    name = event_dict.get("event")
    if _events_level == "debug":
        return event_dict
    if name in _DEBUG_ONLY_EVENTS:
        raise structlog.DropEvent
    if _events_level == "info":
        return event_dict
    # warning / off: the happy path goes (at any method — every HAPPY_PATH
    # member is emitted at INFO or DEBUG, verified across src/).
    if name in _HAPPY_PATH_EVENTS:
        raise structlog.DropEvent
    if _events_level == "warning":
        # Anomalies (any stdlib level) and WARNING+ pass.
        return event_dict
    # off: classified anomalies at INFO/DEBUG method go (the table's
    # documented "INFO-level anomaly lines go too"); everything classified
    # ANOMALY at WARNING-and-above still emits. UNCLASSIFIED events FAIL
    # OPEN here too — they pass at every method — so the contract "a
    # future event can never be silently suppressed by a stale table"
    # holds at every level, not just at warning. (This arm previously
    # dropped EVERY info/debug-method event, classified or not, which
    # silently failed CLOSED on unclassified INFO events: a real event
    # the table had not met yet vanished at off. Found by red-team.)
    if name in _ANOMALY_EVENTS and method in ("info", "debug"):
        raise structlog.DropEvent
    return event_dict


def _otel_span_processor(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Inject ``trace_id`` and ``span_id`` from the active OTel span context.

    Reads ``opentelemetry.trace.get_current_span().get_span_context()`` on every
    log call so nested sub-spans within a job are reflected in ``span_id``.
    ``opentelemetry-api`` is a hard dep, no conditional import guard needed.
    """
    ctx = trace.get_current_span().get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _safe_processor_wrapper(
    processor: structlog.types.Processor,
) -> structlog.types.Processor:
    """Wrap a single processor so exceptions are caught and logged.

    Structured logging must not raise exceptions that propagate to user or actor
    code. Each processor is wrapped so that if it raises, the exception is logged
    at ``warning`` level (including the processor name) and the event dict passes
    through unchanged.
    """

    def _wrapper(
        logger: object, method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        try:
            result = processor(logger, method, event_dict)
            if isinstance(result, dict):
                return result
            return event_dict
        except Exception:
            proc_name = getattr(processor, "__name__", repr(processor))
            logging.getLogger("taskq.obs._structlog").warning(
                "structlog processor %s raised; event=%r",
                proc_name,
                event_dict.get("event"),
                exc_info=True,
            )
            return event_dict

    return _wrapper


def _render_exc_info_safe(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Replace ``exc_info`` with scrubbed ``exception.*`` keys on the JSON channel.

    Mirrors structlog's ``format_exc_info`` key semantics, pop ``exc_info``,
    render only when it resolves to a real exception, but renders through the
    ``_redact_exc`` helpers so Postgres DETAIL row values and URI credentials
    never reach the JSON log line. Without this, ``log.exception()`` shipped a
    leftover ``"exc_info": true`` bool and foreign stdlib records with real
    ``exc_info`` tuples died in the orjson fallback (whole line dropped).
    """
    exc_info = event_dict.pop("exc_info", None)
    parts = safe_exception_parts(exc_info)
    if parts is not None:
        event_dict.update(parts)
    return event_dict


class _ExcInfoSafeBoundLogger(structlog.stdlib.BoundLogger):
    """``BoundLogger`` whose ``.exception()`` never sets ``exc_info`` on the record.

    Why: structlog's own ``exception()`` proxies to ``logging.Logger.exception``,
    and *that* hard-codes ``exc_info=True`` in the stdlib call, so
    ``record.exc_info`` is populated with the live ``sys.exc_info()`` triple no
    matter what the processor chain did to the event dict. Every root handler
    then reads it: ``setup_logging`` installs on the ROOT logger and
    ``worker_main`` calls it unconditionally, and Azure Monitor's
    ``configure_azure_monitor()`` attaches its ``LoggingHandler`` alongside,
    reads ``record.exc_info`` directly, and ships the raw ``str(exc)`` plus the
    full traceback to the App Insights ``exceptions`` table, Postgres DETAIL
    row values and all.

    No processor can close that, because the leak is added *after* the chain
    runs. Routing to ``error`` instead keeps ``exc_info`` inside the event dict,
    where :func:`_render_exc_info_safe` replaces it with scrubbed
    ``exception.*`` fields before the record exists.
    """

    def exception(self, event: str | None = None, *args: object, **kw: object) -> object:
        kw.setdefault("exc_info", True)
        return self._proxy_to_logger("error", event, *args, **kw)  # type: ignore[arg-type]  # Why: structlog types *event_args as str; callers pass logging-style args of any type, matching the base class's own Any-typed signature.


def _scrub_exception_fields(
    logger: object, method: str, event_dict: structlog.types.EventDict
) -> structlog.types.EventDict:
    """Scrub the known exception-bearing field names (the
    ``EXCEPTION_MESSAGE_FIELDS`` / ``EXCEPTION_TRACEBACK_FIELDS`` sets ,
    ``error``, ``error_message``, ``error_traceback``, ``exc``, and the
    terminal-write log's ``job_error_*`` / ``infra_error_*`` names).

    JSON logs ship to the same telemetry backends as spans, so raw
    ``str(exc)``-shaped field values reopen the surface
    ``record_exception_safe`` closes; exception OBJECTS additionally die in
    the orjson fallback and drop the whole line. Values that are not exception
    text (classification strings, ints) pass through unchanged.
    """
    for key in _EXCEPTION_FIELD_NAMES.intersection(event_dict):
        event_dict[key] = scrub_exception_field(key, event_dict[key])
    return event_dict


def _shared_processors() -> list[structlog.types.Processor]:
    """The processor chain every TaskQ record runs, shared by
    :func:`setup_logging`'s stdlib-formatter handoff and the pre-setup
    default chain :func:`_install_default_chain` installs, so the two
    can never drift on what a record carries.

    Built per call: the chain closes over the module-global processors
    (``_otel_span_processor``), and the level-filter pin
    (``tests/test_obs_structlog.py``) monkeypatches that global before
    calling :func:`setup_logging`, so a module-level constant would
    freeze the spy out.
    """
    return [
        # First, and deliberately unwrapped: a call below the configured
        # level must cost nothing beyond this level comparison, so the
        # per-job DEBUG sites do not run the whole chain for a line stdlib
        # then drops. Unwrapped because ``DropEvent`` is a ``BaseException``
        # that ``_safe_processor_wrapper`` would let through anyway, and the
        # comparison itself cannot raise.
        structlog.stdlib.filter_by_level,
        # Second, and equally unwrapped: the TASKQ_LOG_EVENTS_LEVEL knob
        # (see the module docstring's classification table). The cheapest
        # point BEFORE the serialization cost - one frozenset lookup on
        # the event name, then the suppressed line is gone before
        # merge_contextvars, the renderers, or the scrubbers run.
        _events_level_filter,
        _safe_processor_wrapper(structlog.contextvars.merge_contextvars),
        _safe_processor_wrapper(structlog.stdlib.add_log_level),
        _safe_processor_wrapper(structlog.stdlib.add_logger_name),
        _safe_processor_wrapper(structlog.processors.StackInfoRenderer()),
        _safe_processor_wrapper(structlog.processors.TimeStamper(fmt="iso", utc=True)),
        _safe_processor_wrapper(_otel_span_processor),
        _safe_processor_wrapper(structlog.processors.EventRenamer("event")),
        # Last before the formatter handoff: final scrub of exception-bearing
        # fields so both renderers (and any future one) see scrubbed values.
        _safe_processor_wrapper(_scrub_exception_fields),
        # SHARED, not formatter-local: whatever survives this chain becomes
        # ``record.msg`` and is read by every root handler, not just TaskQ's.
        # A raw exception object or ``sys.exc_info()`` triple left on the event
        # dict is therefore an export surface for any vendor handler that
        # stringifies values. Console pays for this with a plain scrubbed
        # ``exception.stacktrace`` field instead of ConsoleRenderer's pretty
        # traceback, the same record reaches the same vendor handlers whichever
        # renderer the operator picked, so the dev view does not get an
        # unredacted exemption.
        _safe_processor_wrapper(_render_exc_info_safe),
    ]


def _install_default_chain() -> None:
    """Install the pre-``setup_logging`` processor chain, at import.

    Why the default configuration needs its own level filter: structlog's
    built-in default chain has none, so an embedding application that
    never calls :func:`setup_logging` paid the full processor chain for
    every record and let stdlib drop the line afterwards, roughly six
    times the filtered cost per enqueue log. The installed chain mirrors
    the configured one's shape: :func:`structlog.stdlib.filter_by_level`
    FIRST and unwrapped, so a call below the effective level costs one
    comparison. The level it filters at is the stdlib effective level of
    the emitting logger, i.e. the embedding application's own root level:
    this configuration never raises or lowers it, and the application
    keeps full control (it also keeps full control of rendering, a later
    :func:`setup_logging` call reconfigures the chain wholesale, and an
    application configuring structlog itself after importing taskq
    overrides this one the same way).

    The default is installed ONLY when structlog is still unconfigured:
    an application that ran its own ``structlog.configure`` before
    importing taskq keeps its chain untouched, because ``configure``
    replaces the process-global config and silently stealing it would be
    an import side effect with no fix except reconfiguring. Note the
    contract for the never-configured app: pre-setup records render as
    JSON through stdlib logging (root-level filtered, the application's
    own handlers), not plain-text stdout, which is the cost fix AND the
    output change on purpose.

    ``cache_logger_on_first_use`` stays False: a logger materialized
    before :func:`setup_logging` runs must re-resolve per call and pick
    the configured chain up, never stay pinned to this pre-config one.
    """
    from taskq._json import structlog_serializer

    if structlog.is_configured():
        # The application owns the global chain already (it configured
        # structlog before importing taskq): installing ours would
        # silently replace it at import time. setup_logging reconfigures
        # wholesale when the application asks for ours explicitly.
        return

    structlog.configure(
        processors=[
            *_shared_processors(),
            # Direct renderer instead of setup_logging's
            # ``wrap_for_formatter`` handoff: there is no
            # ``ProcessorFormatter`` installed yet to receive it, and
            # wrapping without one would hand stdlib's default formatter a
            # non-string record.
            structlog.processors.JSONRenderer(serializer=structlog_serializer),
        ],
        # Why the stdlib factory (not structlog's default PrintLogger):
        # ``filter_by_level`` consults the stdlib logger's effective level,
        # which only this factory produces, and routing through stdlib
        # logging means TaskQ's pre-setup records reach the embedding
        # application's own handlers (or stdlib's last-resort handler for
        # WARNING and above) instead of printing to stdout unconditionally.
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )


_install_default_chain()


def setup_logging(
    *,
    level: str = "INFO",
    log_format: str = "json",
    events_level: str = "info",
) -> None:
    """Configure structlog with the canonical processor chain.

    Production (``log_format="json"``): ``JSONRenderer`` via
    ``ProcessorFormatter`` stdlib bridge. Development (``log_format="console"``):
    ``ConsoleRenderer`` via ``ProcessorFormatter``. Idempotent, guarded
    by ``_logging_configured`` flag. Not called at import time (the
    pre-setup default chain :func:`_install_default_chain` is installed
    at import instead, so an embedding application that never calls this
    still gets level-filtered logging).

    ``events_level``: the ``TASKQ_LOG_EVENTS_LEVEL`` value (validated by
    :func:`set_events_level`, which raises ``ValueError`` on a typo).
    Applied by the ``_events_level_filter`` processor in the shared
    chain; the default ``info`` is today's stream byte-identical.
    """
    global _logging_configured
    if _logging_configured:
        return

    set_events_level(events_level)

    shared_processors = _shared_processors()

    formatter_processors: list[structlog.types.Processor]
    if log_format == "console":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
        formatter_processors = [
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ]
    else:
        from taskq._json import structlog_serializer

        renderer = structlog.processors.JSONRenderer(serializer=structlog_serializer)
        formatter_processors = [
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            # Still needed for FOREIGN records: ``ProcessorFormatter`` lifts
            # their ``record.exc_info`` onto the event dict here, after the
            # shared chain has run, and orjson drops the whole line on a raw
            # tuple. Idempotent for TaskQ's own records, ``exc_info`` is
            # already gone by then.
            _safe_processor_wrapper(_render_exc_info_safe),
            renderer,
        ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=_ExcInfoSafeBoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=formatter_processors,
        foreign_pre_chain=[
            _safe_processor_wrapper(structlog.processors.TimeStamper(fmt="iso", utc=True)),
            _safe_processor_wrapper(structlog.stdlib.add_log_level),
            _safe_processor_wrapper(structlog.stdlib.ExtraAdder()),
            # After ExtraAdder so foreign records' extras are scrubbed too.
            _safe_processor_wrapper(_scrub_exception_fields),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    if not any(
        isinstance(h, logging.StreamHandler)
        and isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
        for h in logging.root.handlers
    ):
        logging.root.addHandler(handler)

    logging.root.setLevel(level)

    _logging_configured = True


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a ``structlog.stdlib.BoundLogger`` for the given dotted name.

    Replaces direct ``structlog.get_logger()`` calls in library code so that
    pyright strict mode gets an explicit return type (``structlog.get_logger``
    returns ``Any``).
    """
    return structlog.get_logger(name)


def bind_job_context(
    log: structlog.stdlib.BoundLogger,
    *,
    job_id: UUID,
    actor: str,
    queue: str,
    attempt: int,
    identity_key: str | None,
    trace_id: str,
    span_id: str | None = None,
    batch_id: str | None = None,
) -> structlog.stdlib.BoundLogger:
    """Bind job-scope fields to a logger, returning a new immutable BoundLogger.

    ``identity_key``, ``span_id``, and ``batch_id`` are omitted from the bound
    dict when ``None``, not set to null or empty string .  ``trace_id``
    is always bound (defaults to ``""`` when no active OTel span per spec).
    Returns a new ``BoundLogger``; does not mutate the input.
    """
    fields: dict[str, str | int] = {
        "job_id": str(job_id),
        "actor": actor,
        "queue": queue,
        "attempt": attempt,
        "trace_id": trace_id,
    }
    if identity_key is not None:
        fields["identity_key"] = identity_key
    if span_id is not None:
        fields["span_id"] = span_id
    if batch_id is not None:
        fields["batch_id"] = batch_id
    return log.bind(**fields)


def log_state_change(
    log: structlog.stdlib.BoundLogger,
    *,
    from_state: str,
    to_state: str,
    **extra: object,
) -> None:
    """Emit an INFO log line with ``kind="state_change"``.

    ``from_state`` and ``to_state`` are the job-status values before and
    after the transition.  All bound fields from the pre-bound ``log``
    (which carries job context from :func:`bind_job_context`) are included
    automatically.  The event name is ``"state-change"`` so the log is
    queryable by both event and kind.
    """
    log.info("state-change", kind="state_change", from_state=from_state, to_state=to_state, **extra)


def log_cancel_phase_change(
    log: structlog.stdlib.BoundLogger,
    *,
    from_phase: int,
    to_phase: int,
    **extra: object,
) -> None:
    """Emit an INFO log line with ``kind="cancel_phase_change"``.

    ``from_phase`` and ``to_phase`` are the cancel-phase integers before
    and after the escalation.  ``cancel_observed_at`` is NOT included, it
    is per-handler context, not part of the canonical schema.
    """
    log.info(
        "cancel_phase_change",
        kind="cancel_phase_change",
        from_phase=from_phase,
        to_phase=to_phase,
        **extra,
    )


def redact_payload(payload: object) -> str:
    """Return the first 16 characters of the SHA-256 hex digest of the JSON-serialized payload.

    Raw payload content does not appear in the return value.  Deterministic
    for the same input.
    """
    # Why bytes directly, not dumps_str(...).encode(): the hash consumes
    # bytes, and dumps() already produces them, the str round-trip was a
    # decode+encode pair per redacted log line.
    serialized = dumps(payload)
    return hashlib.sha256(serialized).hexdigest()[:16]
