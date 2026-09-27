"""Red-team calendar-edge attacks on the cron machinery.

* **Never-landing day-of-month forms (the finding)** - ``croniter.is_valid``
  certifies expressions no calendar date can ever satisfy (``0 0 30 2 *``:
  February 30; ``0 0 31 4 *``: April 31).  Every entry point that validates
  with ``is_valid`` alone therefore ACCEPTS the poison: the ``@cron``
  decorator registers it, and the worker's startup registration pass then
  calls ``compute_next_fire_after`` OUTSIDE its per-spec try/except
  (``_register_cron_schedules``), so one poisoned decorator kills the whole
  registration pass - specs declared after the poisoned one never register
  and the boot dies with croniter's raw ``failed to find next date``.  The
  operator-visible truth is broken on every path: the strike text for a
  direct-to-DB poisoned row names no expression and no diagnosis.  Pinned
  here: rejection at decoration depth (``ValueError`` naming the
  expression), and a ``compute_next_fire_after`` answer whose error names
  the expression and the diagnosis instead of croniter's bare message.
* **The 6-field (seconds) form across DST** - croniter 6.x's 6-field form
  orders fields ``min hour dom mon dow sec`` (seconds LAST).  Spring-forward:
  a match inside the gap answers the gap-end instant the same day, seconds
  preserved, for every strategy; fall-back: ``skip``/``firstof`` take the
  fold-0 occurrence, ``allof`` returns the pair, and a fold-1 seed advances
  beyond the range.  From the gap-end answer the next compute advances to
  the next day's match - no fixed point, no re-fire loop.
* **Timezone-anchored month-end arithmetic** - the 31st / Feb-29 forms in
  non-UTC zones (Kolkata +05:30 lands Jan 30 18:30Z; Sydney AEDT lands
  Feb 28 13:00Z), seconds form included.
* **Tick-level month-end catch-up** - a ``0 0 31 * *`` row missed beyond the
  catch-up window re-anchors on the server clock to the next real 31st,
  exactly one immediate job, no hallucinated Feb-30/31 fire.
* **Tick-level gap-day wall seed** - a row whose stored instant is the EST
  reading of a spring-forward wall that does not exist fires its slot
  within the window and advances to the NEXT day's match: the due judgment
  stays on the instant (server domain), the advance on the local wall, no
  double fire of the shifted gap instant.
* **Tick-level poison containment** - a direct-to-DB poisoned row (poison
  can still enter through paths that bypass spec validation) strikes once
  per tick without touching its peers, and auto-disables on the third
  strike with the full audit truth (``enabled=false``,
  ``disabled_by='auto'``, non-empty ``last_fire_error``).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.cron import CronScheduleSpec, _factory_cache, compute_next_fire_after, cron
from taskq.scheduler import _CRON_REGISTRY
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    FrozenClockConn,
    cron_settings,
    jobs_for_identity,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_now,
    server_ten_min_floor,
)

pytestmark = pytest.mark.integration

_NY = "America/New_York"
_NYZ = "America/New_York"
_UTC = "UTC"

# The poison family: croniter.is_valid certifies each, no calendar date
# can ever match any of them.
_POISON_EXPRESSIONS = [
    "0 0 30 2 *",  # February 30
    "0 0 31 2 *",  # February 31
    "0 0 31 4 *",  # April 31
    "0 0 31 2,4 *",  # only 29/30/31-day-less months
    "0 0 30-31 2 *",  # range form of the same poison
]

# Controls: landable expressions a naive "day-of-month looks impossible"
# heuristic must not reject.
_LANDABLE_CONTROLS = [
    "0 0 29 2 *",  # Feb 29 - fires on leap years (2028 from a 2026 seed)
    "0 0 31 * *",  # fires in every 31-day month
    "0 0 30 2,3 *",  # February 30 never, but March 30 does
    "0 0 L * *",  # last-day-of-month
    "0 0 29 2 1",  # DOM-vs-DOW OR rule: any February Monday lands
    "59 23 31 12 * *",  # 6-field Dec 31 23:59
]


@pytest.fixture(autouse=True)
def _restore_module_globals() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _CRON_REGISTRY and the factory cache around each test.

    The decoration tests below call ``cron()``/``register_cron()``, which
    append to the module-level registry; without this fixture those entries
    would leak into other test modules.  Mirrors the autouse fixture in
    ``tests/test_rt_cron_calendar_vectors.py``.
    """
    original_registry = list(_CRON_REGISTRY)
    original_cache = dict(_factory_cache)
    try:
        yield
    finally:
        _CRON_REGISTRY.clear()
        _CRON_REGISTRY.extend(original_registry)
        _factory_cache.clear()
        _factory_cache.update(original_cache)


# ── The finding: never-landing expressions are certified poison ─────────


@pytest.mark.parametrize("expr", _POISON_EXPRESSIONS)
def test_poison_expression_rejected_at_decoration(expr: str) -> None:
    """``cron()`` must reject an expression no calendar date can satisfy.

    Regression: ``croniter.is_valid`` certifies ``0 0 30 2 *`` (February 30)
    as valid, the decorator registers it, and the worker's startup
    registration pass then dies computing its first fire - one poisoned
    decorator bricked the boot, and every spec declared after it in the
    registry never registered.  The ValueError-or-accept contract must
    reject at decoration depth, with the expression named.
    """
    with pytest.raises(ValueError, match="never matches any calendar date"):
        cron(expr, actor="rt_poison_actor", name="poison")


@pytest.mark.parametrize("expr", _POISON_EXPRESSIONS)
def test_register_cron_rejects_poison_spec_constructed_directly(expr: str) -> None:
    """The ``register_cron`` path (a directly-built spec, no decorator) must
    run the same landability gate: the registry may never carry a spec whose
    first ``compute_next_fire_after`` can fail the worker's boot."""
    with pytest.raises(ValueError, match="never matches any calendar date"):
        from taskq.scheduler import register_cron

        register_cron(CronScheduleSpec(actor="rt_poison_actor", cron_expr=expr, name="poison"))


@pytest.mark.parametrize("expr", _LANDABLE_CONTROLS)
def test_landable_controls_still_accepted(expr: str) -> None:
    """Guard against an overfix: every control lands on a real date (some
    only on leap years, some only via the DOM-vs-DOW OR rule) and must keep
    passing the gate the poison tests tighten."""
    assert cron(expr, actor="rt_landable_probe").cron_expr == expr


def test_compute_next_fire_after_never_lands_names_expression_and_diagnosis() -> None:
    """The error an unlandable expression surfaces must name the expression
    and say WHY, not croniter's bare ``failed to find next date``.

    Regression: the strike text stamped into ``last_fire_error`` for a
    direct-to-DB poisoned row read ``failed to find next date`` - no
    expression, no diagnosis - and the boot traceback for a bypassed
    validation path carried the same cryptic line.
    """
    with pytest.raises(ValueError) as exc_info:
        compute_next_fire_after("0 0 30 2 *", _UTC, datetime(2026, 1, 1, tzinfo=UTC))
    message = str(exc_info.value)
    assert "0 0 30 2 *" in message, f"the expression must be named, got {message!r}"
    assert "never matches any calendar date" in message, (
        f"the diagnosis must be stated, got {message!r}"
    )


# ── The 6-field (seconds) form across DST ────────────────────────────────


class TestSixFieldSecondsAcrossDst:
    """croniter 6.x 6-field order is ``min hour dom mon dow sec``."""

    def test_spring_forward_gap_match_fires_at_gap_end_same_day(self) -> None:
        """Daily 02:30:30 (6-field) on NY's 2026-03-08 (02:00→03:00 gap):
        the match inside the gap answers 03:00:30 the SAME day (gap end +
        the expression's seconds), not the next day's match.  Seed is the
        day before, 12:00Z."""
        out = compute_next_fire_after("30 2 * * * 30", _NY, datetime(2026, 3, 7, 12, 0, tzinfo=UTC))
        assert [d.isoformat() for d in out] == ["2026-03-08T03:00:30-04:00"], (
            "the transition-day fire must happen at the gap's end with the "
            f"expression's seconds preserved, got {[d.isoformat() for d in out]}"
        )

    def test_spring_forward_on_the_hour_form_answers_gap_start_end(self) -> None:
        """Daily 02:00:00 (6-field): the gap's own start match answers the
        gap end (03:00:00 local) the same day - the 5-field doctrine at
        seconds precision."""
        out = compute_next_fire_after("0 2 * * * 0", _NY, datetime(2026, 3, 7, 12, 0, tzinfo=UTC))
        assert [d.isoformat() for d in out] == ["2026-03-08T03:00:00-04:00"]

    def test_fall_back_skip_and_firstof_take_the_fold0_occurrence(self) -> None:
        """Daily 01:30:30 on NY's 2026-11-01 (01:00→02:00 repeats): the
        match occurs twice; ``skip`` and ``firstof`` owe the fold-0 (EDT,
        05:30:30Z) occurrence only."""
        seed = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
        for strategy in ("skip", "firstof"):
            out = compute_next_fire_after(
                "30 1 * * * 30",
                _NY,
                seed,
                dst_strategy=strategy,  # type: ignore[arg-type]
            )
            assert [d.astimezone(UTC) for d in out] == [
                datetime(2026, 11, 1, 5, 30, 30, tzinfo=UTC)
            ], f"{strategy} must take the earlier occurrence, got {[d.isoformat() for d in out]}"

    def test_fall_back_allof_returns_both_occurrences(self) -> None:
        """``allof`` owes BOTH occurrences of the repeated :30:30 - one
        fold-0 instant, one fold-1, an hour apart in UTC."""
        out = compute_next_fire_after(
            "30 1 * * * 30", _NY, datetime(2026, 11, 1, 0, 0, tzinfo=UTC), dst_strategy="allof"
        )
        assert [d.astimezone(UTC) for d in out] == [
            datetime(2026, 11, 1, 5, 30, 30, tzinfo=UTC),
            datetime(2026, 11, 1, 6, 30, 30, tzinfo=UTC),
        ], f"allof must return the fold pair, got {[d.isoformat() for d in out]}"

    def test_fall_back_fold1_seed_advances_beyond_the_range(self) -> None:
        """From a fold-1 seed inside the repeated range the next owed fire
        is the NEXT day's match (the range's earlier occurrences are all
        spent) - never an instant at or before the seed."""
        seed = datetime(2026, 11, 1, 1, 30, 30, tzinfo=ZoneInfo(_NY), fold=1)
        out = compute_next_fire_after("30 1 * * * 30", _NY, seed)
        assert [d.astimezone(UTC) for d in out] == [datetime(2026, 11, 2, 6, 30, 30, tzinfo=UTC)], (
            f"the fold-1 seed must advance beyond the range, got {[d.isoformat() for d in out]}"
        )

    def test_gap_end_answer_is_not_a_fixed_point(self) -> None:
        """Seeding the NEXT compute with the gap-end answer (03:00:30 EDT,
        not itself a cron match) must advance to the next day's 02:30:30
        EDT: the shifted instant must never leave the schedule re-firing or
        stuck."""
        gap_end = datetime(2026, 3, 8, 3, 0, 30, tzinfo=ZoneInfo(_NY))
        out = compute_next_fire_after("30 2 * * * 30", _NY, gap_end)
        assert [d.astimezone(UTC) for d in out] == [datetime(2026, 3, 9, 6, 30, 30, tzinfo=UTC)], (
            f"the chain must advance off the gap end in one step, got {[d.isoformat() for d in out]}"
        )


# ── Timezone-anchored month-end / leap arithmetic ────────────────────────


class TestTimezoneAnchoredMonthEnd:
    def test_31st_in_kolkata_anchors_to_local_midnight(self) -> None:
        """``0 0 31 * *`` in Asia/Kolkata (+05:30): Jan 31 00:00 IST =
        Jan 30 18:30Z.  The instant must carry the schedule's own anchoring,
        not a UTC-midnight drift."""
        out = compute_next_fire_after(
            "0 0 31 * *", "Asia/Kolkata", datetime(2026, 1, 15, tzinfo=UTC)
        )
        assert [d.isoformat() for d in out] == ["2026-01-31T00:00:00+05:30"]

    def test_31st_seconds_form_in_kolkata_keeps_the_second(self) -> None:
        """The 6-field ``0 0 31 * * 30`` anchors Jan 31 00:00:30 IST, with
        the seconds field surviving the zone conversion."""
        out = compute_next_fire_after(
            "0 0 31 * * 30", "Asia/Kolkata", datetime(2026, 1, 15, tzinfo=UTC)
        )
        assert [d.isoformat() for d in out] == ["2026-01-31T00:00:30+05:30"]

    def test_feb29_in_sydney_anchors_to_aedt_midnight(self) -> None:
        """``0 0 29 2 *`` in Australia/Sydney: 2028-02-29 00:00 AEDT
        (+11:00) = Feb 28 13:00Z - the leap day computed in the schedule's
        zone, not the UTC one."""
        out = compute_next_fire_after(
            "0 0 29 2 *", "Australia/Sydney", datetime(2026, 1, 15, tzinfo=UTC)
        )
        assert [d.isoformat() for d in out] == ["2028-02-29T00:00:00+11:00"]


# ── Tick-level attacks (real PG) ─────────────────────────────────────────


def _most_recent_31st_midnight_at_least_2d_old(now: datetime) -> datetime:
    """The most recent 31st 00:00Z strictly older than 48h: always beyond
    the 1h catch-up window, whatever day the suite runs on."""
    day = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    while day.day != 31:
        day -= timedelta(days=1)
    return day


def _next_31st_midnight_after(now: datetime) -> datetime:
    """The first 31st 00:00Z strictly after *now* (the beyond-window
    re-anchor answer for ``0 0 31 * *``)."""
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    while day <= now or day.day != 31:
        day += timedelta(days=1)
    return day


class TestTickCalendarEdges:
    """Calendar edges through the real tick (advisory lock, due read, UPDATEs)."""

    async def test_month_end_catchup_beyond_window_reanchors_to_next_31st(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A ``0 0 31 * *`` row whose slot is the most recent 31st (>48h
        old, far beyond the 1h window): ONE immediate job, the skip warning,
        and next_fire_at re-anchored on the server clock to the next REAL
        31st - no hallucinated Feb-30/31 fire between."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "rt_month_end_actor")
        now_before = await server_now(clean_pg_conn)
        slot = _most_recent_31st_midnight_at_least_2d_old(now_before)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor="rt_month_end_actor",
            name="month-end-catchup",
            cron_expr="0 0 31 * *",
            next_fire_at=slot,
            identity_key="month-end-catchup",
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )
        now_after = await server_now(clean_pg_conn)

        assert fired == 1
        skipped = [e for e in captured if e["event"] == "cron missed slots skipped"]
        assert len(skipped) == 1, "a beyond-window miss must warn that slots were skipped"

        expected_reanchor = {
            _next_31st_midnight_after(now_before),
            _next_31st_midnight_after(now_after),
        }
        # The collapse doctrine (pinned at C4 in test_rt_cron_time_semantics.py):
        # the immediate job consumes the re-anchored first-future slot, so
        # next_fire_at is the 31st AFTER the re-anchor.  Pinned here at
        # monthly scale: the answer must still be a REAL 31st strictly after
        # the tick (no Feb-30/31 hallucination, no off-by-one), even though
        # the collapse shifts a monthly schedule's phase - the consequence
        # is reported, the arithmetic is what this file pins.
        expected = {_next_31st_midnight_after(reanchor) for reanchor in expected_reanchor}
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] in expected, (
            f"the re-anchor must be the next real 31st {expected}, got "
            f"{row['next_fire_at']} - a Feb-30/31 or off-by-one answer is "
            "month-end drift through the catch-up path"
        )
        assert row["next_fire_at"] > now_after, "the re-anchored slot must be strictly future"

        jobs = await jobs_for_identity(clean_pg_conn, schema, "month-end-catchup")
        assert len(jobs) == 1, f"exactly one immediate fire, got {len(jobs)}"
        assert jobs[0]["status"] == "pending"

    async def test_gap_day_wall_seed_fires_within_window_and_advances_off_the_gap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A row whose stored instant is the EST reading of NY wall 02:30 on
        the 2026-03-08 spring-forward day (07:30Z, which roundtrips to local
        03:30 EDT): the tick fires the slot (within window, instant-domain
        due judgment) and advances EXACTLY to the next day's 02:30 EST -
        the shifted gap instant is consumed once, never re-fired."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "rt_gap_day_actor")
        frozen = datetime(2026, 3, 8, 7, 0, tzinfo=UTC)
        slot = datetime(2026, 3, 8, 7, 30, tzinfo=UTC)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor="rt_gap_day_actor",
            name="gap-day-wall",
            cron_expr="30 2 * * *",
            timezone=_NY,
            next_fire_at=slot,
            identity_key="gap-day-wall",
        )
        frozen_conn = FrozenClockConn(clean_pg_conn, frozen)

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    frozen_conn,  # type: ignore[arg-type]  # Why: real connection; only the tick's planning clock is intercepted.
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                )

        assert fired == 1
        assert [e for e in captured if e["event"] == "cron missed slots skipped"] == [], (
            "the slot is within the catch-up window: it fires its own instant, no re-anchor"
        )
        assert await _next_fire_at(clean_pg_conn, schema, schedule_id) == datetime(
            2026, 3, 9, 6, 30, tzinfo=UTC
        ), (
            "the advance must be the next day's 02:30 EDT (06:30Z, the zone "
            "crossed its own spring-forward the day before) computed from the "
            "LOCAL wall - re-firing the shifted gap instant or re-anchoring on "
            "now are both fire-loop shapes"
        )
        jobs = await jobs_for_identity(clean_pg_conn, schema, "gap-day-wall")
        assert len(jobs) == 1
        assert jobs[0]["status"] == "pending"

    async def test_poisoned_row_strikes_without_harming_peers_and_auto_disables(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A direct-to-DB poisoned row (poison bypasses spec validation on
        that path) must take one strike per tick - healthy peer untouched -
        and auto-disable on the third strike with the full audit truth:
        ``enabled=false``, ``disabled_by='auto'``, non-empty
        ``last_fire_error``, no job ever enqueued."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)  # threshold 3
        actor = "rt_poison_tick_actor"
        await seed_actor_config(clean_pg_conn, schema, actor)
        slot = await server_ten_min_floor(clean_pg_conn)
        healthy_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=actor,
            name="healthy-peer",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=slot,
            identity_key="poison-peer",
        )
        poison_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=actor,
            name="poison-row",
            cron_expr="0 0 30 2 *",
            next_fire_at=slot,
            identity_key="poison-row",
        )

        for expected_consecutive in (1, 2, 3):
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )
            expected_fired = 1 if expected_consecutive == 1 else 0
            assert fired == expected_fired, (
                f"tick {expected_consecutive}: only the healthy peer fires (once), got {fired}"
            )

            poison = await schedule_row(clean_pg_conn, schema, poison_id)
            assert poison["consecutive_failures"] == expected_consecutive, (
                f"the poisoned row must take exactly one strike per tick, got "
                f"{poison['consecutive_failures']} after tick {expected_consecutive}"
            )
            if expected_consecutive < 3:
                assert poison["enabled"] is True, "disabled before the third strike"
            else:
                assert poison["enabled"] is False, "three strikes must auto-disable"
                assert poison["disabled_by"] == "auto", (
                    f"the audit marker must be 'auto' (a boot may revert it), got "
                    f"{poison['disabled_by']!r}"
                )
            assert poison["last_fire_error"], (
                "every strike must carry a non-empty operator-visible reason, got "
                f"{poison['last_fire_error']!r}"
            )

        healthy = await schedule_row(clean_pg_conn, schema, healthy_id)
        assert healthy["consecutive_failures"] == 0, "the peer must keep a clean ledger"
        assert healthy["enabled"] is True
        assert healthy["last_fire_error"] is None
        assert await _next_fire_at(clean_pg_conn, schema, healthy_id) == slot + timedelta(
            minutes=10
        ), "the peer advanced its own cadence, untouched by the neighbour's strikes"
        assert len(await jobs_for_identity(clean_pg_conn, schema, "poison-row")) == 0, (
            "a poisoned schedule must never enqueue a job"
        )


# ── Helpers ────────────────────────────────────────────────────────────


async def _next_fire_at(conn: asyncpg.Connection, schema: str, schedule_id: object) -> datetime:
    value: datetime | None = await conn.fetchval(
        f'SELECT next_fire_at FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
        schedule_id,
    )
    assert value is not None
    return value
