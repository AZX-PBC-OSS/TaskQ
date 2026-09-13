"""Red-team attacks on the cron tick's handover double-fire window (real PG).

The advisory lock exists for exactly one property: during leader handover
two ticks, on two real connections, both inside their own transactions,
must never both fire the same schedule.  The pinning file
(``tests/test_cron_tick_bounded.py``) exercises a contended tick in
isolation — a second connection holds the lock first and the tick under
test loses the probe.  What it does NOT pin is the handover race itself:
a second tick STARTING while the first is mid-transaction, and two ticks
free-running concurrently.  Those are the attacks here.

* ``TestHandoverDoubleFire`` — a gated backend pauses tick T1 after it
  took the lock, read the due set and planned its fires; tick T2 starts
  on a second real connection at that exact point and must lose the
  probe and fire nothing.  Then free-running pairs of concurrent ticks
  under ``asyncio.gather`` — whichever interleaving the event loop
  picks, every schedule fires exactly once.
* ``TestTickStatementShape`` — the lock probe must be the tick's FIRST
  statement (a rewrite that reads the due set before probing re-opens
  the window the lock exists to close), and an empty due set must cost
  exactly lock + clock + due SELECT — no actor fetch, no UPDATEs, no
  enqueue.
* ``TestManualScheduleManagementMidTick`` — a schedule created and
  committed by another connection while a tick is inflight is not in
  the tick's due-set snapshot: not fired, not advanced; it fires on the
  NEXT tick.  No lost write, no double-fire.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    CountingConn,
    GatedEnqueueBackend,
    count_jobs,
    cron_settings,
    hour_floor,
    jobs_for_identity,
    make_backend,
    seed_actor_config,
    seed_schedule,
    wedge_events,
)

pytestmark = pytest.mark.integration

_PROBE = "pg_try_advisory_xact_lock"
_ACTOR = "rt_handover_actor"


class TestHandoverDoubleFire:
    """C1: the property the transaction-scoped lock exists to provide."""

    async def test_second_tick_mid_transaction_fires_nothing(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """T2 probes while T1 is mid-tick inside its open transaction → T2
        loses the probe, issues exactly the probe statement, fires nothing;
        T1 then fires every schedule exactly once."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        identities = [f"handover-{i}" for i in range(3)]
        for i, identity in enumerate(identities):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"handover-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
                identity_key=identity,
            )

        gate = asyncio.Event()
        entered = asyncio.Event()
        gate_backend = GatedEnqueueBackend(settings, gate=gate, entered=entered)

        counting_t1 = CountingConn(clean_pg_conn)

        async def _tick_t1() -> int:
            async with clean_pg_conn.transaction():
                return await tick_cron(
                    counting_t1,  # type: ignore[arg-type]  # Why: duck-typed connection; only fetch/fetchrow/fetchval/execute are called, all typed on the wrapper.
                    settings,
                    gate_backend,
                    schema,
                    new_uuid(),
                )

        task_t1 = asyncio.create_task(_tick_t1())
        await entered.wait()

        contender = await asyncpg.connect(module_pg_schema.pg_dsn)
        counting_t2 = CountingConn(contender)
        fired_t2: int = -1
        try:
            async with contender.transaction():
                fired_t2 = await tick_cron(
                    counting_t2,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper, see T1 above.
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                )
        finally:
            gate.set()
            fired_t1: int = await task_t1
        await contender.close()

        assert fired_t2 == 0, "a tick that lost the probe mid-handover must fire nothing"
        assert counting_t2.count == 1, (
            "the losing tick must issue exactly the advisory-lock probe; it issued "
            f"{counting_t2.statements}"
        )
        assert _PROBE in counting_t2.statements[0]

        assert fired_t1 == 3
        for identity in identities:
            rows = await jobs_for_identity(clean_pg_conn, schema, identity)
            assert len(rows) == 1, (
                f"identity {identity} has {len(rows)} jobs after a handover overlap — "
                "the double-fire window the lock exists to close is open"
            )

    async def test_free_running_concurrent_ticks_never_double_fire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Two ticks gathered concurrently on separate real connections:
        whatever interleaving the event loop picks, the fired counts sum to
        the due set and every schedule's identity lands on exactly one job."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        backend = make_backend(settings)
        due = hour_floor(datetime.now(UTC))

        for round_no in range(3):
            tag = f"racy-r{round_no}"
            identities = [f"{tag}-{i}" for i in range(3)]
            for i, identity in enumerate(identities):
                await seed_schedule(
                    clean_pg_conn,
                    schema,
                    actor=_ACTOR,
                    name=f"{tag}-{i}",
                    cron_expr=_HOURLY,
                    next_fire_at=due,
                    identity_key=identity,
                )

            contender = await asyncpg.connect(module_pg_schema.pg_dsn)

            async def _tick(conn: asyncpg.Connection) -> int:
                async with conn.transaction():
                    return await tick_cron(conn, settings, backend, schema, new_uuid())

            fired_a, fired_b = await _gather_two(_tick(clean_pg_conn), _tick(contender))
            await contender.close()

            assert fired_a + fired_b == 3, (
                f"round {round_no}: concurrent ticks fired {fired_a} + {fired_b} for a "
                "3-schedule due set — exactly one leader may consume it"
            )
            for identity in identities:
                rows = await jobs_for_identity(clean_pg_conn, schema, identity)
                assert len(rows) == 1, (
                    f"round {round_no}: identity {identity} fired {len(rows)} times "
                    "under concurrent ticks — double-fire"
                )


class TestTickStatementShape:
    """C1 ordering + C10: what a tick is allowed to cost when nothing is due."""

    async def test_lock_probe_is_the_first_statement_of_a_fired_tick(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """No statement may precede the probe — not the clock, not the due
        SELECT.  Reading the due set before probing is the rewrite that
        re-opens the handover window: both leaders would plan the same
        schedules and the probe would only stop the second UPDATE."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="ordering",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
        )

        counting = CountingConn(clean_pg_conn)
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                counting,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper; all awaited methods typed on it.
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
            )

        assert fired == 1
        assert counting.statements, "the tick issued no statements at all"
        assert _PROBE in counting.statements[0], (
            "the tick's first statement is not the advisory-lock probe: "
            f"{counting.statements[0]!r} — a due-set read before the probe lets two "
            "leaders plan the same schedules concurrently"
        )
        assert counting.matching(_PROBE) == 1, "the probe must be issued exactly once"

    async def test_empty_due_set_costs_exactly_lock_clock_and_due(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Nothing due → probe + clock + due SELECT and NOTHING else: no
        actor_config fetch, no UPDATE, no enqueue call."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)

        enqueue_calls: list[int] = []

        async def _must_not_enqueue(
            args_list: list[object],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[object]:
            enqueue_calls.append(1)
            raise AssertionError("enqueue_batch must not run when the due set is empty")

        monkeypatch.setattr(backend, "enqueue_batch", _must_not_enqueue)

        counting = CountingConn(clean_pg_conn)
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                counting,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper; all awaited methods typed on it.
                settings,
                backend,
                schema,
                new_uuid(),
            )

        assert fired == 0
        assert enqueue_calls == []
        assert counting.count == 3, (
            f"an empty tick issued {counting.count} statements: "
            f"{counting.statements} — the allowed shape is probe, clock, due SELECT"
        )
        assert _PROBE in counting.statements[0]
        assert counting.matching("actor_config") == 0
        assert counting.matching("UPDATE") == 0


class TestManualScheduleManagementMidTick:
    """C11: tick vs schedule CRUD on a second connection."""

    async def test_schedule_created_mid_tick_is_not_fired_by_the_inflight_tick(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A due schedule committed after the tick's due SELECT is not in
        the tick's snapshot: no job, no advance — it fires on the next tick."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, "rt_wedge_actor")
        await seed_actor_config(clean_pg_conn, schema, "rt_late_actor")
        due = hour_floor(datetime.now(UTC))

        await seed_schedule(
            clean_pg_conn,
            schema,
            actor="rt_wedge_actor",
            name="wedged",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_harness.wedge_then_succeed",
        )

        with wedge_events() as (entered, gate):
            counting = CountingConn(clean_pg_conn)

            async def _tick() -> int:
                async with clean_pg_conn.transaction():
                    return await tick_cron(
                        counting,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper; all awaited methods typed on it.
                        settings,
                        make_backend(settings),
                        schema,
                        new_uuid(),
                    )

            task = asyncio.create_task(_tick())
            await entered.wait()

            creator = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                late_id = await seed_schedule(
                    creator,
                    schema,
                    actor="rt_late_actor",
                    name="late",
                    cron_expr=_HOURLY,
                    next_fire_at=due,
                )
            finally:
                await creator.close()
            gate.set()
            fired_first: int = await task

        assert fired_first == 1, "only the wedged schedule was in the tick's snapshot"
        assert await count_jobs(clean_pg_conn, schema, "rt_late_actor") == 0, (
            "a schedule created mid-tick was fired by the inflight tick — the due "
            "SELECT's snapshot was not respected"
        )
        late_after = await _schedule_snapshot(clean_pg_conn, schema, late_id)
        assert late_after["last_fired_at"] is None
        assert late_after["next_fire_at"] == due
        assert late_after["enabled"] is True
        wedge_jobs_before = await count_jobs(clean_pg_conn, schema, "rt_wedge_actor")

        # The second tick must now pick the late schedule up.
        async with clean_pg_conn.transaction():
            fired_second = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
            )
        assert fired_second == 1
        assert await count_jobs(clean_pg_conn, schema, "rt_late_actor") == 1
        assert await count_jobs(clean_pg_conn, schema, "rt_wedge_actor") == wedge_jobs_before


# ── Helpers ────────────────────────────────────────────────────────────


async def _gather_two(a: Awaitable[int], b: Awaitable[int]) -> tuple[int, int]:
    """``asyncio.gather`` over two tick coroutines, typed."""
    return await asyncio.gather(a, b)


async def _schedule_snapshot(
    conn: asyncpg.Connection, schema: str, schedule_id: UUID
) -> dict[str, Any]:
    row = await conn.fetchrow(
        f"SELECT id, actor, name, cron_expr, next_fire_at, last_fired_at, enabled, "  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
        f'consecutive_failures FROM "{schema}".cron_schedules WHERE id = $1',
        schedule_id,
    )
    assert row is not None
    return dict(row)
