"""The ``TASKQ_LOG_EVENTS_LEVEL`` verbosity knob: unit pins.

The knob governs the STREAMING DUPLICATE of the job_events ledger only:
every state change is durably ledgered in the same transaction as the
state itself, so the filter suppresses log lines without ever touching
the audit trail. These tests pin:

- the setting exists, defaults to ``info``, and is case-insensitive;
- ``info`` is a full passthrough (byte-identical stream to pre-change);
- ``warning`` drops the happy path and KEEPS the anomaly stream (even
  the anomaly events logged at INFO);
- ``off`` keeps WARNING-and-above only (never blinds the operator to
  failures);
- ``debug`` ADDS the per-tick internals (``loop-lag``, ``poll-cadence``);
- unclassified events fail open at every level.

The filter is tested through ``capture_logs(processors=[...])``: the
context manager applies exactly the processors passed, so the filter
runs in isolation. The end-to-end proof (a real worker subprocess with
the env var set: the failed-job line survives, the state-change lines
gone, the ledger untouched) lives in
``tests/system_e2e/test_log_events_levels.py``.
"""

from __future__ import annotations

import io
import json
import logging

import pytest
import structlog

from taskq.obs import _structlog as obs_structlog
from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

# One representative per classified family (the full table is the module
# docstring's content; the pins here hold the table's load-bearing members).
_HAPPY_REPRESENTATIVES = (
    "state-change",
    "cancel_phase_change",
    "dispatch",
    "prune",
    "archive_expiry",
    "health-server-started",
    "producer-loop-exit",
    "cron-schedule-registered",
    "stale-batches-completed",
)
_ANOMALY_REPRESENTATIVES = (
    "job-failed",
    "heartbeat-tick-failure",
    "heartbeat-tick-unexpected-error",
    "terminal-write-failed",
    "isolate-self-complete",  # INFO-level: survives warning BY NAME
    "sweep-drained-pending-reservation-reclaims",  # INFO-level reclaim
    "rate-limit-dependency-failure",
    "worker-watchdog-trip",
    "cron-tick-failed",
    "sweep-expired-locks-failed",
)
_DEBUG_ONLY_REPRESENTATIVES = ("loop-lag", "poll-cadence")


@pytest.fixture
def events_level() -> object:
    """Snapshot/restore the module-global events level around a test."""
    saved = obs_structlog._events_level
    yield obs_structlog
    obs_structlog.set_events_level(saved)


def _capture_with_filter(level: str, events: tuple[str, ...]) -> list[dict[str, object]]:
    """Emit each event name at INFO method through the filter at ``level``."""
    obs_structlog.set_events_level(level)
    entries: list[dict[str, object]] = []
    # One capture per event: DropEvent inside a shared capture is fine, but
    # separate captures keep an accidental chain exception attributable.
    for name in events:
        with structlog.testing.capture_logs(processors=[obs_structlog._events_level_filter]) as cap:
            log = obs_structlog.get_logger("test")
            log.info(name, kind="x", job_id="j")
            log.warning(name)  # the same name at WARNING method
            log.error(name)
        if cap:
            entries.append({"event": name, "methods": [e.get("log_level") for e in cap]})
    return entries


def _captured_names(entries: list[dict[str, object]]) -> set[str]:
    return {str(e["event"]) for e in entries}


# ── The setting ──────────────────────────────────────────────────────


def test_setting_exists_and_defaults_to_info() -> None:
    """log_events_level exists and defaults to 'info' (byte-identical)."""
    s = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    assert s.log_events_level == "info"


@pytest.mark.parametrize(
    ("raw", "normalized"),
    [
        ("warning", "warning"),
        ("OFF", "off"),
        ("Debug", "debug"),
        ("INFO", "info"),
    ],
)
def test_setting_case_insensitive(raw: str, normalized: str) -> None:
    """The level loads case-insensitively, normalized lowercase."""
    s = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN, "TASKQ_LOG_EVENTS_LEVEL": raw})
    assert s.log_events_level == normalized


def test_setting_invalid_rejected() -> None:
    """An unknown level raises ConstraintViolationError at load."""
    with pytest.raises(Exception, match=r"log_events_level must be one of"):
        WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN, "TASKQ_LOG_EVENTS_LEVEL": "loud"})


# ── info: the byte-identical default ─────────────────────────────────


def test_filter_at_info_is_a_full_passthrough(events_level: object) -> None:
    """At info, every classified event passes - nothing is dropped."""
    captured = _capture_with_filter("info", _HAPPY_REPRESENTATIVES + _ANOMALY_REPRESENTATIVES)
    assert _captured_names(captured) == set(_HAPPY_REPRESENTATIVES) | set(_ANOMALY_REPRESENTATIVES)
    # The passthrough does not mutate the event dict beyond the chain's
    # own additions: the event name and bound fields survive.
    for entry in captured:
        assert entry["methods"] == ["info", "warning", "error"]


def test_filter_at_info_drops_only_debug_internals(events_level: object) -> None:
    """At info the debug-only internals are dropped (they are new call
    sites: dropping them is what keeps the default stream byte-identical
    to the pre-knob worker, which never emitted them)."""
    captured = _capture_with_filter("info", _DEBUG_ONLY_REPRESENTATIVES)
    assert _captured_names(captured) == set()


def test_default_rendered_stream_is_byte_identical(events_level: object) -> None:
    """A state-change line rendered through the configured chain at the
    default level is byte-identical to the same line rendered by the
    chain WITHOUT the events filter."""
    rendered_with = _render_state_change(include_filter=True)
    rendered_without = _render_state_change(include_filter=False)
    # The TimeStamper runs per render; every other byte must match.
    strip_ts = lambda line: line.replace(  # noqa: E731  # Why: a one-field strip for the comparison; a def would bury the assertion.
        json.loads(line)["timestamp"], "<ts>"
    )
    assert strip_ts(rendered_with) == strip_ts(rendered_without)
    # And the line is the documented state-change shape (the
    # ProcessorFormatter names the level field "level", the logger field
    # "logger").
    parsed = json.loads(rendered_with)
    assert parsed["event"] == "state-change"
    assert parsed["kind"] == "state_change"
    assert parsed["from_state"] == "pending"
    assert parsed["to_state"] == "running"
    assert parsed["level"] == "info"
    assert parsed["logger"] == "render-under-test"
    assert "timestamp" in parsed


def _render_state_change(*, include_filter: bool) -> str:
    """Render one state-change through the shared chain into a string."""
    chain = [
        p
        for p in obs_structlog._shared_processors()
        if include_filter or p is not obs_structlog._events_level_filter
    ]
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    root = logging.getLogger("render-under-test")
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    structlog.configure(
        processors=[*chain, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    log = obs_structlog.get_logger("render-under-test")
    log.info(
        "state-change",
        kind="state_change",
        from_state="pending",
        to_state="running",
    )
    root.handlers = []
    return buf.getvalue()


# ── warning: the anomaly stream only ─────────────────────────────────


def test_filter_at_warning_drops_happy_path(events_level: object) -> None:
    """At warning, the happy-path lines are gone - the per-job duplicates
    and the lifecycle INFO lines."""
    captured = _capture_with_filter("warning", _HAPPY_REPRESENTATIVES)
    assert _captured_names(captured) == set()


def test_filter_at_warning_keeps_anomalies(events_level: object) -> None:
    """At warning, the anomaly stream survives - INCLUDING the anomaly
    events logged at INFO (isolate completions, reclaims): the
    classification is by event NAME, not stdlib level."""
    captured = _capture_with_filter("warning", _ANOMALY_REPRESENTATIVES)
    assert _captured_names(captured) == set(_ANOMALY_REPRESENTATIVES)


# ── off: WARNING-and-above only ──────────────────────────────────────


def test_filter_at_off_drops_the_event_stream(events_level: object) -> None:
    """At off, the happy path is gone entirely, and an INFO-level anomaly
    line goes too - while the SAME anomaly emitted at WARNING survives
    (off never blinds the operator to failures)."""
    captured = _capture_with_filter("off", _HAPPY_REPRESENTATIVES)
    assert _captured_names(captured) == set()
    # isolate-self-complete is an INFO-level anomaly: the info-method
    # line dies at off...
    obs_structlog.set_events_level("off")
    with structlog.testing.capture_logs(processors=[obs_structlog._events_level_filter]) as cap:
        log = obs_structlog.get_logger("test")
        log.info("isolate-self-complete")
    assert cap == []
    # ...but its WARNING-method emission (the production shape) survives.
    with structlog.testing.capture_logs(processors=[obs_structlog._events_level_filter]) as cap:
        log = obs_structlog.get_logger("test")
        log.warning("isolate-self-complete")
    assert len(cap) == 1


def test_filter_at_off_keeps_warning_and_above(events_level: object) -> None:
    """At off, the WARNING-and-above anomalies STILL emit: off never
    blinds the operator to failures."""
    obs_structlog.set_events_level("off")
    survivors: set[str] = set()
    for name in _ANOMALY_REPRESENTATIVES:
        with structlog.testing.capture_logs(processors=[obs_structlog._events_level_filter]) as cap:
            log = obs_structlog.get_logger("test")
            log.warning(name)
            log.error(name)
        if cap:
            survivors.add(name)
    # job-failed, heartbeat-tick-failure, watchdog trip: the failure
    # stream - every one of these is WARNING+ in production.
    assert {
        "job-failed",
        "heartbeat-tick-failure",
        "heartbeat-tick-unexpected-error",
        "terminal-write-failed",
        "rate-limit-dependency-failure",
        "worker-watchdog-trip",
        "cron-tick-failed",
        "sweep-expired-locks-failed",
    } <= survivors


# ── debug: adds the internals ────────────────────────────────────────


def test_filter_at_debug_adds_internals(events_level: object) -> None:
    """At debug, the per-tick internals (loop-lag, poll-cadence) pass."""
    captured = _capture_with_filter("debug", _DEBUG_ONLY_REPRESENTATIVES)
    assert _captured_names(captured) == set(_DEBUG_ONLY_REPRESENTATIVES)


@pytest.mark.parametrize("level", ["info", "warning", "off"])
def test_internals_suppressed_at_non_debug_levels(level: str) -> None:
    """The internals are the debug level's exclusive addition."""
    captured = _capture_with_filter(level, _DEBUG_ONLY_REPRESENTATIVES)
    assert _captured_names(captured) == set()


# ── fail-open default ────────────────────────────────────────────────


@pytest.mark.parametrize("level", ["info", "warning", "off", "debug"])
def test_unclassified_event_fails_open(level: str) -> None:
    """A future event not in the classification table passes at every
    level - a new event can never be silently suppressed by a stale
    table."""
    captured = _capture_with_filter(level, ("some-future-event",))
    assert _captured_names(captured) == {"some-future-event"}


# ── the classification table itself ──────────────────────────────────


def test_classification_table_pins() -> None:
    """The module's classification sets contain the load-bearing members
    (the full table is the module docstring's content)."""
    assert "state-change" in obs_structlog._HAPPY_PATH_EVENTS
    assert "cancel_phase_change" in obs_structlog._HAPPY_PATH_EVENTS
    assert "dispatch" in obs_structlog._HAPPY_PATH_EVENTS
    for name in ("job-failed", "isolate-self-complete", "worker-watchdog-trip"):
        assert name in obs_structlog._ANOMALY_EVENTS
    assert set(_DEBUG_ONLY_REPRESENTATIVES) <= obs_structlog._DEBUG_ONLY_EVENTS


def test_valid_levels_match_the_settings_validator() -> None:
    """The filter's level set and the settings validator's set are the
    same (they live at different layers; this pin is the drift guard)."""
    from taskq import settings as settings_mod

    assert settings_mod._VALID_LOG_EVENTS_LEVELS == obs_structlog._VALID_LOG_EVENTS_LEVELS


# ── wiring ───────────────────────────────────────────────────────────


def test_setup_logging_wires_events_level(events_level: object) -> None:
    """setup_logging(events_level=...) sets the filter's level; the
    default call resets it to info."""
    obs_structlog._logging_configured = False
    try:
        obs_structlog.setup_logging(events_level="warning")
        assert obs_structlog._events_level == "warning"
        obs_structlog._logging_configured = False
        obs_structlog.setup_logging()
        assert obs_structlog._events_level == "info"
    finally:
        obs_structlog._logging_configured = False


def test_setup_logging_rejects_unknown_level(events_level: object) -> None:
    """setup_logging validates the level (a typo cannot silently disable
    the filter's semantics)."""
    obs_structlog._logging_configured = False
    try:
        with pytest.raises(ValueError, match="log_events_level"):
            obs_structlog.setup_logging(events_level="loud")
    finally:
        obs_structlog._logging_configured = False


def test_shared_chain_includes_the_filter() -> None:
    """The filter sits in the shared chain immediately after the level
    gate - the cheapest point BEFORE the serialization cost."""
    chain = obs_structlog._shared_processors()
    filter_positions = [i for i, p in enumerate(chain) if p is obs_structlog._events_level_filter]
    assert len(filter_positions) == 1
    filter_pos = filter_positions[0]
    # Everything before it is only the level gate and the safe-wrapper
    # machinery, none of the expensive processors (renderers, scrubbers).
    for p in chain[:filter_pos]:
        assert p is not obs_structlog._scrub_exception_fields
        assert p is not obs_structlog._render_exc_info_safe
