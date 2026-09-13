"""NUL-guard red-team attacks on the schedule-update and cron-failure paths.

Two boundaries bind caller- or exception-derived text straight into
``text`` columns without the guard their twins carry:

* ``ScheduleUpdateArgs`` (``src/taskq/backend/_protocol.py``) — the
  update twin of ``ScheduleCreateArgs``, whose ``_check_no_nul_text``
  rejects a NUL at construction.  The update struct had no such guard,
  so ``JobsClient.update_schedule(payload_factory=...)`` and the
  backend's ``update_schedule`` bind the raw text; a NUL surfaces as a
  raw asyncpg ``CharacterNotInRepertoireError`` (SQLSTATE 22021) from
  deep inside the UPDATE instead of the clean ``ValueError`` the create
  path raises at the boundary.
* ``_FireFailure.error_text`` (``src/taskq/worker/cron_loop.py``) —
  ``str(exc)`` of an uncontrolled payload-factory exception, bound into
  the batched failures UPDATE's ``unnest($2::text[])``.  A NUL in the
  message aborts the WHOLE failures UPDATE with 22021, so the tick
  raises, the caller's transaction rolls back, and
  ``consecutive_failures`` never increments — a permanently failing
  schedule can never reach auto-disable, and the defect that would have
  named the reason is the very thing that destroys the bookkeeping.
  Same rationale as ``worker/_handlers.py``: derived from an
  uncontrolled exception, so sanitize (keep the write valid, keep the
  defect visible) rather than reject (strand the bookkeeping).

Also pins the classification contract for the third guard,
``ErrorInfo``: a future raw constructor path must fail as a bug
(``ValueError``, outside every transient/infra set), never as a
transient-retry — which is exactly what the guard's ``ValueError``
buys, because the raw 22021 it pre-empts IS a ``PostgresError`` and
would be transient-misread.
"""

from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import ErrorInfo, ScheduleUpdateArgs
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._handlers import (
    _TERMINAL_WRITE_INFRA_EXCEPTIONS,  # pyright: ignore[reportPrivateUsage]  # Why: the classification under pin IS this tuple; importing it is the assertion.
)
from taskq.worker._transient import TRANSIENT_PG_ERRORS

from .test_rt_cron_harness import (
    _HOURLY,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

_NUL_ACTOR = "rt_nul_factory_actor"


# ── ScheduleUpdateArgs: the update twin must guard like the create twin ──
#
# ``JobsClient.update_schedule`` constructs this struct unguarded
# (``src/taskq/client/_jobs.py``) and ``backend/_schedules.py`` binds
# ``payload_factory``/``last_fire_error`` straight into the UPDATE's
# text parameters, so the struct's constructor is the last boundary
# where a clean ValueError can fire before the pool.


def test_schedule_update_args_rejects_nul_in_payload_factory() -> None:
    """A NUL in payload_factory raises ValueError at construction, not 22021 at bind time."""
    with pytest.raises(ValueError, match="payload_factory contains a NUL"):
        ScheduleUpdateArgs(payload_factory="a\x00b")


def test_schedule_update_args_rejects_nul_in_last_fire_error() -> None:
    """last_fire_error is bound as raw text too — same guard, same clean error."""
    with pytest.raises(ValueError, match="last_fire_error contains a NUL"):
        ScheduleUpdateArgs(last_fire_error="a\x00b")


def test_schedule_update_args_rejects_nul_in_cron_expr() -> None:
    """The create twin skips cron_expr only because ``croniter.is_valid``
    runs first there; this struct performs no expression validation, so
    the NUL guard is what stands between the column and the bind."""
    with pytest.raises(ValueError, match="cron_expr contains a NUL"):
        ScheduleUpdateArgs(cron_expr="0\x00 * * * *", next_fire_at=datetime(2026, 1, 1, tzinfo=UTC))


def test_schedule_update_args_clean_text_still_constructs() -> None:
    """The guard adds no false positives: every text field still accepts
    clean values, and None keeps meaning 'leave unchanged'."""
    args = ScheduleUpdateArgs(
        cron_expr="0 6 * * *",
        next_fire_at=datetime(2026, 1, 1, tzinfo=UTC),
        payload_factory="make_report",
        last_fire_error="old error",
    )
    assert args.payload_factory == "make_report"
    assert args.last_fire_error == "old error"

    cleared = ScheduleUpdateArgs(clear_payload_factory=True)
    assert cleared.payload_factory is None


# ── ErrorInfo: the guard's ValueError must classify as a bug ─────────────
#
# The guard exists so an unsanitized value fails LOUDLY.  This pins where
# that failure lands: a ValueError is outside both error-classification
# sets, so a future raw constructor path surfaces as an unexpected error
# (the loop guards' loud-then-fatal doctrine) instead of a transient
# retry.  The contrast half is the evidence: the raw 22021 the guard
# pre-empts is a PostgresError and would be transient-misread by BOTH
# sets — retrying forever, never failing the job.


def test_error_info_guard_raises_value_error_for_nul_message() -> None:
    with pytest.raises(ValueError, match="NUL"):
        ErrorInfo(error_class="X", error_message="m\x00", error_traceback=None)


def test_guard_value_error_is_not_terminal_write_infra() -> None:
    """The terminal-write infra catch must not swallow the guard's error:
    treating it as infra would leave the job running for lease reclaim
    while the same construction bug re-fires every attempt."""
    try:
        raise ValueError("error_message contains a NUL character (U+0000)")
    except _TERMINAL_WRITE_INFRA_EXCEPTIONS:  # type: ignore[misc]  # Why: tuple-typed except is the classification under test.
        pytest.fail("ValueError must not be classified as terminal-write infra")
    except ValueError:
        pass


def test_guard_value_error_is_not_transient_pg() -> None:
    """Nor may the leader loops retry it: a construction bug retried every
    tick is a functional zombie — ticking, doing no work, alerting on
    nothing."""
    try:
        raise ValueError("error_message contains a NUL character (U+0000)")
    except TRANSIENT_PG_ERRORS:  # type: ignore[misc]  # Why: tuple-typed except is the classification under test.
        pytest.fail("ValueError must not be classified as transient PG")
    except ValueError:
        pass


def test_the_raw_22021_the_guard_pre_empts_would_be_infra_misread() -> None:
    """Evidence for why the guard must raise ValueError rather than let the
    bind fail: CharacterNotInRepertoireError is a PostgresError, and the
    terminal-write infra catch — the exact context ErrorInfo's text is
    bound in — matches ANY PostgresError, so a raw 22021 leaves the job
    running for lease reclaim instead of failing it (the strand cycle
    ErrorInfo's docstring documents).  The leader loops' transient set
    deliberately excludes data errors, so those loops already classify a
    raw 22021 as a bug; the terminal-write path is the one the guard
    keeps honest."""
    assert issubclass(asyncpg.CharacterNotInRepertoireError, asyncpg.PostgresError)
    raw = asyncpg.CharacterNotInRepertoireError("invalid byte sequence")
    assert isinstance(raw, _TERMINAL_WRITE_INFRA_EXCEPTIONS), (
        "a raw 22021 would be terminal-write-infra-misread without the guard"
    )
    assert not isinstance(raw, TRANSIENT_PG_ERRORS), (
        "the leader-loop transient set excludes data errors by design — "
        "a raw 22021 there is already an unexpected (bug) classification"
    )


# ── cron_loop: a NUL-bearing factory exception must not destroy the
# ── failure bookkeeping that would auto-disable the schedule ────────────


async def nul_message_factory() -> dict[str, object]:
    """Payload factory raising the attack's NUL-bearing exception.

    Dotted path: ``tests.test_rt_cancel_nul_guards.nul_message_factory``.
    """
    raise ValueError("bad\x00data")


@pytest.mark.integration
async def test_nul_bearing_factory_failure_still_reaches_auto_disable(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A payload factory whose exception message carries a NUL must not
    abort the batched failures UPDATE: three ticks still drive
    consecutive_failures to the auto-disable threshold, and the stored
    last_fire_error shows the visible ``\\x00`` escape (defect diagnosable)
    with no raw NUL (write valid)."""
    from taskq.worker.cron_loop import tick_cron

    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, _NUL_ACTOR)
    schedule_id = await seed_schedule(
        clean_pg_conn,
        schema,
        actor=_NUL_ACTOR,
        name="nul-factory",
        cron_expr=_HOURLY,
        next_fire_at=hour_floor(datetime.now(UTC)),
        payload_factory="tests.test_rt_cancel_nul_guards.nul_message_factory",
    )

    for _ in range(3):
        # Before the sanitize fix each tick raises here: the failures
        # UPDATE aborts with 22021 and the transaction rolls back, so the
        # failure count never moves and auto-disable is unreachable.
        async with clean_pg_conn.transaction():
            await tick_cron(clean_pg_conn, settings, make_backend(settings), schema, new_uuid())

    row = await schedule_row(clean_pg_conn, schema, schedule_id)
    assert row["enabled"] is False, (
        "a permanently failing schedule must still reach auto-disable when the "
        "failure text carries a NUL — the bookkeeping is the safety net, and "
        "aborting the failures UPDATE removes it for the whole tick"
    )
    assert row["consecutive_failures"] == 3
    error_text: str | None = row["last_fire_error"]
    assert error_text is not None
    assert "\\x00" in error_text, (
        f"the stored reason must keep the defect visible as an escape; got {error_text!r}"
    )
    assert "\x00" not in error_text, "a raw NUL can never be stored in a text column"


@pytest.mark.integration
async def test_fire_failure_error_text_is_sanitized_at_construction(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The sanitize happens where the text is derived (``_record_fire_failure``),
    not at the bind — pinning the construction-site contract the same way
    ``worker/_handlers.py`` pins its ErrorInfo construction sites."""
    from taskq.worker.cron_loop import (
        _record_fire_failure,  # pyright: ignore[reportPrivateUsage]  # Why: the construction site is the property under test.
    )

    schema = module_pg_schema.schema_name
    settings = cron_settings(schema)
    await seed_actor_config(clean_pg_conn, schema, _NUL_ACTOR)
    schedule_id = await seed_schedule(
        clean_pg_conn,
        schema,
        actor=_NUL_ACTOR,
        name="nul-direct",
        cron_expr=_HOURLY,
        next_fire_at=hour_floor(datetime.now(UTC)),
    )
    record = await clean_pg_conn.fetchrow(
        f'SELECT * FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
        schedule_id,
    )
    assert record is not None

    from opentelemetry import trace

    failure = _record_fire_failure(
        trace.get_current_span(),  # non-recording outside a span context
        record,
        ValueError("bad\x00data"),
        settings,
    )
    assert failure.error_text == "bad\\x00data"
    assert "\x00" not in failure.error_text

    # The empty-message fallback survives sanitization: a bare
    # TimeoutError() renders to '' and the class name must still land.
    bare = _record_fire_failure(trace.get_current_span(), record, TimeoutError(), settings)
    assert bare.error_text == "TimeoutError"
