"""Red-team attack: the catch-up skip-count walk versus the tick's deadline.

``_plan_fire``'s beyond-window branch counts the dropped slots by hopping
the schedule forward one ``compute_next_fire_after`` call per occurrence,
from the owed slot to the recomputed fire.  Nothing bounds that walk: a
fine-grained schedule (``* * * * * */5``, a form the cron guide documents)
whose ``next_fire_at`` is days in the past -- the fleet-was-down shape --
owes hundreds of thousands of hops, and each hop costs tens of
microseconds of croniter.  The leader wraps the WHOLE tick in
``asyncio.timeout(dispatcher_command_timeout)`` (5s at defaults); a walk
that outlives it cancels the tick every time, the transaction (and its
advisory lock) rolls back, ``next_fire_at`` never advances, and the
identical batch is re-selected one second later: a permanent, fleet-wide
cron livelock -- every other due schedule in the batch is planned behind
the walk and is lost with it.

These tests drive the real tick on real PG under the leader's real
whole-tick deadline.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    cron_settings,
    make_backend,
    seed_actor_config,
    seed_schedule,
    server_now,
    ten_min_floor,
)

_ACTOR = "rt_skip_walk_budget"
_FINE = "* * * * * */5"  # every 5 seconds: 17280 slots per day
_TEN_MINUTELY = "*/10 * * * *"
_DAYS_OVERDUE = 8  # ~138k owed slots; the walk alone outlives the deadline


async def _next_fire_at(conn: asyncpg.Connection, schema: str, schedule_id: object) -> datetime:
    row = await conn.fetchrow(
        f'SELECT next_fire_at FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608
        schedule_id,
    )
    assert row is not None
    val: datetime = row["next_fire_at"]
    return val


@pytest.mark.asyncio
class TestSkipWalkInsideTickBudget:
    """The beyond-window skip-count walk must not outlive the tick."""

    async def test_a_fine_grained_schedule_days_overdue_fires_inside_the_tick_deadline(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A */5 schedule overdue by 8 days must FIRE within the leader's
        whole-tick deadline -- the walk counts at bounded cost, the fire
        lands at the recomputed slot, next_fire_at advances, and the
        advisory lock is released for the fleet.

        RED today: the unbounded walk exceeds the deadline, the tick times
        out, nothing commits, and every subsequent tick re-walks the same
        backlog: cron stops fleet-wide.
        """
        schema = module_pg_schema.schema_name
        settings: WorkerSettings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        now = await server_now(clean_pg_conn)
        stale_slot = now - timedelta(days=_DAYS_OVERDUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fine",
            cron_expr=_FINE,
            next_fire_at=stale_slot,
        )

        # The leader's own wrapper: one deadline for the whole tick.
        with structlog.testing.capture_logs() as captured:
            async with asyncio.timeout(settings.dispatcher_command_timeout):
                async with clean_pg_conn.transaction():
                    fired = await tick_cron(
                        clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                    )

        assert fired == 1, "the overdue schedule must fire exactly once (the re-anchored slot)"
        row = await _next_fire_at(clean_pg_conn, schema, schedule_id)
        assert row > now, (
            f"next_fire_at {row} must advance past the recompute seed; a tick that leaves it "
            "in the past re-walks the same backlog forever (the livelock)"
        )
        warn = [e for e in captured if e["event"] == "cron missed slots skipped"]
        assert len(warn) == 1, "a beyond-window miss must warn that slots were skipped"
        # The backlog (~138k slots) is far deeper than the walk's budget
        # slice can count: the count is a floor, and the log must say so.
        assert warn[0]["skipped_slots"] >= 1
        assert warn[0]["skipped_slots_partial"] is True, (
            "a backlog deeper than the walk's budget slice must flag its "
            "count as partial, not present a floor as the depth"
        )

    async def test_a_shallow_backlog_count_is_exact_and_unflagged(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A backlog well inside the walk's budget counts EXACTLY, with
        the partial flag False: the bound must not cost the common case
        its precision."""
        schema = module_pg_schema.schema_name
        settings: WorkerSettings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        stale_slot = grid - timedelta(minutes=90)  # 9 ten-minute slots
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="shallow",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=stale_slot,
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
                )

        assert fired == 1
        warn = [e for e in captured if e["event"] == "cron missed slots skipped"]
        assert len(warn) == 1
        # The exact count depends on which side of the 10-minute boundary
        # the test's clock read and the tick's own statement_timestamp()
        # land (the same race the re-anchor pins accept two candidates
        # for): the stale slot is floored(now)-90m from the test's read,
        # the recompute seeds from the tick's read.
        assert warn[0]["skipped_slots"] in (9, 10, 11), (
            f"a shallow backlog's count must stay exact, got {warn[0]['skipped_slots']}"
        )
        assert warn[0]["skipped_slots_partial"] is False
        row = await _next_fire_at(clean_pg_conn, schema, schedule_id)
        assert row > await server_now(clean_pg_conn)

    async def test_the_walk_never_exceeds_its_slice_of_the_tick_budget(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The walk's own cost is bounded well inside the tick deadline
        (a slice of it, the same discipline the payload-factory grant
        follows), however deep the backlog it counts."""
        schema = module_pg_schema.schema_name
        settings: WorkerSettings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        now = await server_now(clean_pg_conn)
        stale_slot = now - timedelta(days=_DAYS_OVERDUE)
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fine-timed",
            cron_expr=_FINE,
            next_fire_at=stale_slot,
        )

        import time

        backend = make_backend(settings)
        async with clean_pg_conn.transaction():
            t0 = time.monotonic()
            fired = await tick_cron(clean_pg_conn, settings, backend, schema, new_uuid())
            elapsed = time.monotonic() - t0

        assert fired == 1
        # The tick's deadline is 5s at defaults; the walk may budget a SLICE
        # of it, never the whole thing. One second is generous for a bounded
        # walk and still far under the deadline.
        assert elapsed < 1.0, (
            f"the tick took {elapsed:.2f}s to plan one overdue fine-grained "
            "schedule: the skip-count walk is running unbounded inside the "
            "tick's transaction and its advisory lock"
        )
