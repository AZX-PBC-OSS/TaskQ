"""Red-team attacks on the cron tick's time semantics (real PG, real clock).

* **Catch-up determinism (C4)** — beyond-window misses re-anchor on the
  tick's server clock (next_fire_at strictly future, "cron missed slots
  skipped" warned); within-window misses fire their scheduled slot and
  advance one cadence (sequential catch-up, no warning).  The exact
  boundary — a miss exactly ``cron_catch_up_window`` old — is the
  strict-inequality property: it is a within-window catch-up, and it can
  only be pinned deterministically by pinning the tick's planning clock,
  so those two tests run the tick on :class:`FrozenClockConn` (a real
  connection whose ONLY intercepted call is the tick's own
  ``SELECT clock_timestamp()``; every statement still hits real PG).
* **DST ``allof`` at a future overlap (C5)** — America/New_York falls
  back on 2026-11-01 (02:00 EDT → 01:00 EST), so local 01:30 occurs at
  both 05:30Z and 06:30Z.  A yearly schedule pinned to that date and
  seeded due now (within window) owes TWO jobs: the overdue fire
  immediately (server-stamped, pending) and the second occurrence
  pre-scheduled at exactly 06:30Z (status ``scheduled`` — the enqueue
  SQL decides status from the server clock, which is why the overlap
  must be in the future for this pin).  A ``skip`` twin owes one.
* **Cap semantics (C6)** — limit fires the EARLIEST due schedules and
  leaves the remainder bit-for-bit untouched; a due set exactly equal to
  the limit fires all (no off-by-one) and the next tick fires nothing; a
  limit above the due set is not a truncation.
* **Success-UPDATE alignment (C7)** — three schedules with distinct
  cadences (5-min, 10-min, hourly) and distinct due slots: each fired
  schedule's next_fire_at must equal ITS OWN slot plus ITS OWN cadence,
  exactly.  A rewrite that zipped the ids array against a misordered
  next_fires array would advance the wrong schedule and swap these
  observably distinct values.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    _TEN_MINUTELY,
    FrozenClockConn,
    cron_settings,
    hour_floor,
    jobs_for_identity,
    make_backend,
    next_ten_min_boundary,
    seed_actor_config,
    seed_schedule,
    server_now,
    ten_min_floor,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_time_actor"
_FIVE_MINUTELY = "*/5 * * * *"

# America/New_York fall-back 2026-11-01: local 01:30 happens twice.
_OVERLAP_FIRST_UTC = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
_OVERLAP_SECOND_UTC = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
_OVERLAP_YEARLY = "30 1 1 11 *"


class TestCatchUpRecompute:
    """C4: the recompute seed and the window boundary."""

    async def test_miss_beyond_window_recomputes_from_server_clock(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """90 minutes missed against a 1h window: the slot is skipped —
        warning logged, one immediate job, next_fire_at re-anchored on the
        server clock (grid-aligned, strictly future), not on the stale slot."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        stale_slot = grid - timedelta(minutes=90)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="beyond",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=stale_slot,
            identity_key="beyond",
        )

        now_before = await server_now(clean_pg_conn)
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )
        now_after = await server_now(clean_pg_conn)

        assert fired == 1
        skipped = [e for e in captured if e["event"] == "cron missed slots skipped"]
        assert len(skipped) == 1, "a beyond-window miss must warn that slots were skipped"
        assert skipped[0]["schedule_id"] == str(schedule_id)

        row = await _next_fire_at(clean_pg_conn, schema, schedule_id)
        expected = {
            next_ten_min_boundary(now_before) + timedelta(minutes=10),
            next_ten_min_boundary(now_after) + timedelta(minutes=10),
        }
        assert row in expected, (
            f"next_fire_at {row} is not the server-clock re-anchor {expected} — the "
            "recompute seed must be the tick's clock_timestamp(), not the stale slot"
        )
        assert row > now_after, "the re-anchored next_fire_at must be strictly future"

        jobs = await jobs_for_identity(clean_pg_conn, schema, "beyond")
        assert len(jobs) == 1
        assert jobs[0]["status"] == "pending", "the skipped slot's fire lands immediately"
        assert now_before <= jobs[0]["scheduled_at"] <= now_after

    async def test_miss_within_window_fires_scheduled_slot_sequentially(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """30 minutes missed against a 1h window: NO skip warning, the slot
        fires, and next_fire_at advances EXACTLY one cadence from the fired
        slot — still in the past, so the next tick continues the catch-up
        one slot at a time."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        slot = grid - timedelta(minutes=30)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="within",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=slot,
            identity_key="within",
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                first = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        assert first == 1
        assert [e for e in captured if e["event"] == "cron missed slots skipped"] == [], (
            "a within-window miss is a catch-up, not a skip — no warning may be logged"
        )
        assert await _next_fire_at(clean_pg_conn, schema, schedule_id) == slot + timedelta(
            minutes=10
        ), "the advance must come from the FIRED slot, not from a re-anchor on now"

        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )
        assert second == 1
        assert await _next_fire_at(clean_pg_conn, schema, schedule_id) == slot + timedelta(
            minutes=20
        ), "sequential catch-up: the second tick fires the next missed slot"
        assert len(await jobs_for_identity(clean_pg_conn, schema, "within")) == 2

    async def test_miss_exactly_at_the_window_boundary_is_within(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A miss exactly ``cron_catch_up_window`` old fires its scheduled
        slot (strict ``<`` at the boundary): no warning, next_fire_at =
        slot + one cadence, exactly.  A rewrite to ``<=`` would re-anchor
        instead and log the skip warning — both assertions catch it.

        The tick's planning clock is pinned (see the module docstring) so
        the equality is exact; the due SELECT's ``statement_timestamp()``
        bound and every write still run on the real server.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        frozen = hour_floor(datetime.now(UTC))
        boundary_slot = frozen - timedelta(hours=1)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="boundary",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=boundary_slot,
            identity_key="boundary",
        )
        frozen_conn = FrozenClockConn(clean_pg_conn, frozen)

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    frozen_conn,  # type: ignore[arg-type]  # Why: real connection; only the tick's clock fetchval is intercepted, on a type typed above.
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                )

        assert fired == 1
        assert [e for e in captured if e["event"] == "cron missed slots skipped"] == []
        assert await _next_fire_at(clean_pg_conn, schema, schedule_id) == (
            boundary_slot + timedelta(minutes=10)
        ), (
            "at exactly the window boundary the miss is still within: the schedule "
            "fires its own slot and advances one cadence, it must not re-anchor"
        )

    async def test_miss_one_microsecond_beyond_the_boundary_reanchors(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The other side of the same boundary, pinned with the same frozen
        clock: one microsecond older than the window re-anchors on the
        (frozen) server clock and warns."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        frozen = hour_floor(datetime.now(UTC))
        just_beyond = frozen - timedelta(hours=1) - timedelta(microseconds=1)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="just-beyond",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=just_beyond,
            identity_key="just-beyond",
        )
        frozen_conn = FrozenClockConn(clean_pg_conn, frozen)

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    frozen_conn,  # type: ignore[arg-type]  # Why: real connection; only the tick's clock fetchval is intercepted.
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                )

        assert fired == 1
        assert len([e for e in captured if e["event"] == "cron missed slots skipped"]) == 1
        expected = next_ten_min_boundary(frozen) + timedelta(minutes=10)
        assert await _next_fire_at(clean_pg_conn, schema, schedule_id) == expected, (
            "beyond the boundary the recompute seed is the (pinned) server clock: "
            f"next_fire_at must be {expected}"
        )


class TestDstAllofOverlap:
    """C5: both occurrences of a repeated hour, through one real tick."""

    async def test_allof_enqueues_both_occurrences_skip_enqueues_one(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A schedule whose next match is the future fall-back overlap
        (2026-11-01 01:30 America/New_York = 05:30Z and 06:30Z):

        * ``allof``: TWO jobs from ONE tick — the overdue fire immediately
          (pending, server-stamped) and the second occurrence at exactly
          06:30Z with status ``scheduled``; the schedule's next_fire_at is
          exactly 05:30Z and a second tick enqueues nothing more.
        * ``skip`` twin (control): ONE job, next_fire_at exactly 05:30Z.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due_slot = await server_now(clean_pg_conn) - timedelta(minutes=5)
        allof_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="allof",
            cron_expr=_OVERLAP_YEARLY,
            timezone="America/New_York",
            dst_strategy="allof",
            next_fire_at=due_slot,
            identity_key="dst-allof",
        )
        skip_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="skip",
            cron_expr=_OVERLAP_YEARLY,
            timezone="America/New_York",
            dst_strategy="skip",
            next_fire_at=due_slot,
            identity_key="dst-skip",
        )

        now_before = await server_now(clean_pg_conn)
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )
        now_after = await server_now(clean_pg_conn)

        assert fired == 2, "both schedules fire in one tick: allof (1 slot) + skip (1 slot)"

        allof_jobs = await jobs_for_identity(clean_pg_conn, schema, "dst-allof")
        assert len(allof_jobs) == 2, (
            f"'allof' must enqueue BOTH occurrences of the repeated hour, got "
            f"{[j['scheduled_at'] for j in allof_jobs]}"
        )
        by_status = {job["status"]: job for job in allof_jobs}
        assert set(by_status) == {"pending", "scheduled"}, (
            f"expected one immediate (pending) and one future (scheduled) job, got "
            f"{[j['status'] for j in allof_jobs]}"
        )
        assert by_status["scheduled"]["scheduled_at"] == _OVERLAP_SECOND_UTC, (
            "the second occurrence must carry its own instant, 06:30Z, verbatim"
        )
        assert now_before <= by_status["pending"]["scheduled_at"] <= now_after, (
            "the overdue occurrence is the immediate fire: server-stamped inside the tick"
        )

        assert await _next_fire_at(clean_pg_conn, schema, allof_id) == _OVERLAP_FIRST_UTC
        assert await _next_fire_at(clean_pg_conn, schema, skip_id) == _OVERLAP_FIRST_UTC

        skip_jobs = await jobs_for_identity(clean_pg_conn, schema, "dst-skip")
        assert len(skip_jobs) == 1
        assert skip_jobs[0]["status"] == "pending"

        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )
        assert second == 0, "both schedules advanced to the future overlap — nothing due"
        assert len(await jobs_for_identity(clean_pg_conn, schema, "dst-allof")) == 2
        assert len(await jobs_for_identity(clean_pg_conn, schema, "dst-skip")) == 1


class TestCapSemantics:
    """C6: the tick's LIMIT contract."""

    async def test_limit_one_fires_earliest_and_leaves_remainder_bit_for_bit(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """limit=1 over three due schedules (distinct slots): the EARLIEST
        slot fires; the other two rows are untouched bit-for-bit."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        # Offsets kept ≤30min against the 1h window so no wall-clock
        # boundary crossing between seeding and the tick can flip a
        # within-window miss into a beyond-window recompute.
        slots = [
            grid - timedelta(minutes=30),
            grid - timedelta(minutes=20),
            grid - timedelta(minutes=10),
        ]
        ids = []
        for i, slot in enumerate(slots):
            ids.append(
                await seed_schedule(
                    clean_pg_conn,
                    schema,
                    actor=_ACTOR,
                    name=f"cap-{i}",
                    cron_expr=_TEN_MINUTELY,
                    next_fire_at=slot,
                    identity_key=f"cap-{i}",
                )
            )
        remainder_before = [await _full_row(clean_pg_conn, schema, sid) for sid in ids[1:]]

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid(), limit=1
            )

        assert fired == 1
        assert len(await jobs_for_identity(clean_pg_conn, schema, "cap-0")) == 1
        assert await jobs_for_identity(clean_pg_conn, schema, "cap-1") == []
        assert await jobs_for_identity(clean_pg_conn, schema, "cap-2") == []
        assert await _next_fire_at(clean_pg_conn, schema, ids[0]) == slots[0] + timedelta(
            minutes=10
        ), "the fired schedule advances from its own slot (sequential catch-up)"
        remainder_after = [await _full_row(clean_pg_conn, schema, sid) for sid in ids[1:]]
        assert remainder_after == remainder_before, (
            "the unfired remainder must be bit-for-bit untouched — a capped tick "
            "leaves the rest due, it does not advance or stamp it"
        )

    async def test_due_set_exactly_at_limit_fires_all_with_no_off_by_one(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Three due, limit=3: all three fire, the next tick fires nothing —
        LIMIT is inclusive at the boundary, no schedule is dropped or held."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        for i in range(3):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"exact-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
                identity_key=f"exact-{i}",
            )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid(), limit=3
            )
        assert fired == 3, f"due set == limit must fire all; fired {fired}"

        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid(), limit=3
            )
        assert second == 0, "every schedule advanced — the second tick must be empty"
        total: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
            _ACTOR,
        )
        assert total == 3

    async def test_limit_above_due_set_fires_all(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Two due, limit=10: both fire — a limit above the due set is not a
        truncation and does not hold work back."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        for i in range(2):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"under-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
                identity_key=f"under-{i}",
            )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid(), limit=10
            )
        assert fired == 2


class TestSuccessUpdateAlignment:
    """C7: the success UPDATE's ids and next_fires arrays stay aligned."""

    async def test_each_schedule_advances_by_its_own_cadence_from_its_own_slot(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Three schedules, three cadences (10-min, hourly, 5-min), three
        distinct slots: after one tick each row's next_fire_at is exactly
        its own slot + its own cadence.  Any ids/next_fires misalignment
        swaps observably distinct values and fails here."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        # B is seeded ON the 10-minute grid (miss < 10min, so no wall-clock
        # crossing between seeding and the tick can push it beyond the 1h
        # window); its hourly next fire is the hour boundary after the grid.
        cases = [
            (
                "align-ten",
                _TEN_MINUTELY,
                grid - timedelta(minutes=30),
                grid - timedelta(minutes=20),
            ),
            ("align-hourly", _HOURLY, grid, hour_floor(grid) + timedelta(hours=1)),
            (
                "align-five",
                _FIVE_MINUTELY,
                grid - timedelta(minutes=10),
                grid - timedelta(minutes=5),
            ),
        ]
        ids = {}
        for name, expr, slot, _ in cases:
            ids[name] = await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=name,
                cron_expr=expr,
                next_fire_at=slot,
                identity_key=name,
            )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )

        assert fired == 3
        for name, _expr, slot, expected_next in cases:
            actual = await _next_fire_at(clean_pg_conn, schema, ids[name])
            assert actual == expected_next, (
                f"schedule {name} (slot {slot}) advanced to {actual}, expected "
                f"{expected_next} — its own cadence from its own slot; the batched "
                "success UPDATE's id/next_fire arrays are misaligned"
            )


# ── Helpers ────────────────────────────────────────────────────────────


async def _next_fire_at(conn: asyncpg.Connection, schema: str, schedule_id: object) -> datetime:
    value: datetime | None = await conn.fetchval(
        f'SELECT next_fire_at FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
        schedule_id,
    )
    assert value is not None
    return value


async def _full_row(
    conn: asyncpg.Connection, schema: str, schedule_id: object
) -> dict[str, object]:
    row = await conn.fetchrow(
        f'SELECT * FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
        schedule_id,
    )
    assert row is not None
    return dict(row)
