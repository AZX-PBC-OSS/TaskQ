"""Unit tests for the event-writer / sweep-budget WorkerSettings knobs.

The bounded-sweep design rests on these fields being constrained at load
time: the batch cap and per-batch statement_timeout are what keep one
committed batch inside the reclaim-event visibility margin, so the
degenerate values must be rejected at the settings boundary — a
``batch_size`` of 0 would put ``LIMIT 0`` into every sweep (a silent
drain stall that looks like "nothing to do" to every caller and metric),
and a ``statement_timeout_ms`` of 0 would disable the server-side abort
that is the enforcement layer of the design.  Same convention as
tests/test_worker_settings_drain.py: defaults, env override, rejection.
"""

import pytest
from dotenvmodel.exceptions import ConstraintViolationError

from taskq.constants import (
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
)
from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with sensible defaults.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── Defaults ────────────────────────────────────────────────────────────


def test_event_writer_knob_defaults_track_the_documented_constants() -> None:
    """The field defaults must stay the constants the batch-size derivation
    is written against — a drift between the two would silently change the
    envelope every event-writer batch is sized to fit."""
    s = _load()
    assert s.event_writer_batch_size == DEFAULT_EVENT_WRITER_BATCH_SIZE
    assert s.event_writer_statement_timeout_ms == DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS
    assert s.cron_tick_limit == DEFAULT_EVENT_WRITER_BATCH_SIZE
    assert s.event_writer_reduced_batch_divisor == 4
    assert s.sweep_breaker_failure_threshold == 3
    assert s.sweep_breaker_window_secs == 600.0
    assert s.sweep_drain_batches == 8


# ── Env override via load_from_dict ─────────────────────────────────────


def test_event_writer_knobs_env_plumbable() -> None:
    """The decision record's contract: every knob is an env-plumbable
    WorkerSettings field.  Pin one representative override per knob."""
    s = _load(
        TASKQ_EVENT_WRITER_BATCH_SIZE="250",
        TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS="900",
        TASKQ_EVENT_WRITER_REDUCED_BATCH_DIVISOR="8",
        TASKQ_SWEEP_BREAKER_FAILURE_THRESHOLD="5",
        TASKQ_SWEEP_BREAKER_WINDOW_SECS="120.0",
        TASKQ_SWEEP_DRAIN_BATCHES="3",
        TASKQ_CRON_TICK_LIMIT="50",
    )
    assert s.event_writer_batch_size == 250
    assert s.event_writer_statement_timeout_ms == 900.0
    assert s.event_writer_reduced_batch_divisor == 8
    assert s.sweep_breaker_failure_threshold == 5
    assert s.sweep_breaker_window_secs == 120.0
    assert s.sweep_drain_batches == 3
    assert s.cron_tick_limit == 50


# ── Validation: the degenerate values must be rejected at load ──────────


@pytest.mark.parametrize(
    ("env_key", "bad_value"),
    [
        # A 0 batch size is the silent drain stall: LIMIT 0 is a legal,
        # rowless query, so every sweep would report success on zero rows
        # forever while the backlog sits eligible.
        ("TASKQ_EVENT_WRITER_BATCH_SIZE", "0"),
        ("TASKQ_EVENT_WRITER_BATCH_SIZE", "-5"),
        # 0 disables the server-side statement_timeout — the enforcement
        # layer of the batch design, not a tuning nicety.
        ("TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS", "0"),
        ("TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS", "49"),
        # A divisor of 1 makes the breaker's reduced tier equal to the
        # normal tier: no degradation when the database is falling over.
        ("TASKQ_EVENT_WRITER_REDUCED_BATCH_DIVISOR", "1"),
        # A 0 threshold would latch the breaker on the first failure.
        ("TASKQ_SWEEP_BREAKER_FAILURE_THRESHOLD", "0"),
        # A 0 window expires every failure instantly: the latch becomes
        # unreachable.
        ("TASKQ_SWEEP_BREAKER_WINDOW_SECS", "0.0"),
        # 0 drain batches contradicts the loop's "parent call already ran"
        # arithmetic.
        ("TASKQ_SWEEP_DRAIN_BATCHES", "0"),
        # 0 schedules per tick is a silent cron stall: the tick succeeds
        # on an empty due set forever while schedules sit due.
        ("TASKQ_CRON_TICK_LIMIT", "0"),
    ],
)
def test_degenerate_event_writer_knob_values_rejected_at_load(env_key: str, bad_value: str) -> None:
    with pytest.raises(ConstraintViolationError):
        _load(**{env_key: bad_value})


def test_event_writer_knob_minimums_accepted() -> None:
    """The constraint floors themselves are loadable — the rejection tests
    above pin the floor's existence, this one pins the floor's edge."""
    s = _load(
        TASKQ_EVENT_WRITER_BATCH_SIZE="1",
        TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS="50",
        TASKQ_EVENT_WRITER_REDUCED_BATCH_DIVISOR="2",
        TASKQ_SWEEP_BREAKER_FAILURE_THRESHOLD="1",
        TASKQ_SWEEP_BREAKER_WINDOW_SECS="1.0",
        TASKQ_SWEEP_DRAIN_BATCHES="1",
        TASKQ_CRON_TICK_LIMIT="1",
    )
    assert s.event_writer_batch_size == 1
    assert s.event_writer_statement_timeout_ms == 50.0
    assert s.cron_tick_limit == 1
