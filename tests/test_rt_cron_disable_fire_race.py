"""Red-team pins for the tick's SUCCESS-write races (the failure-write
half is `test_rt_cron_failure_domains.py`'s C3 pin; this file attacks the
other two arms of the same race window).

The batched enqueue and the successes UPDATE are separate statements in
one transaction, and an operator's ``UPDATE cron_schedules SET enabled =
false`` (or ``DELETE``) can commit between them on a second connection.
The failure UPDATE guards itself with ``AND s.enabled = true`` (pinned
there); neither the enqueue nor the successes UPDATE can or does:

* the enqueue is a plain batched INSERT with no schedule conjunct to
  filter on, and
* guarding the successes UPDATE WITHOUT guarding the enqueue would
  strand the already-enqueued job behind an un-advanced
  ``next_fire_at``: on re-enable, the catch-up recompute would fire the
  already-delivered slot a SECOND time.  The two writes must therefore
  agree, and the semantics they implement are: **a disable (or delete)
  takes effect for every tick that starts after it; a fire an in-flight
  tick has already planned lands exactly once**, bounded by one fire per
  disable, never two.

These pins hold the tick inside its gated enqueue (the plan is built,
the advisory lock and transaction are open) while a second connection
commits the operator write - the same deterministic window the failure
domains file uses - then assert the observable end state.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    GatedEnqueueBackend,
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_hour_floor,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_disable_race"


async def _run_gated_tick(
    conn: asyncpg.Connection,
    schema: str,
    operator_conn: asyncpg.Connection,
    *,
    operator_sql: str,
    schedule_id: object,
) -> int:
    """Run one gated tick; commit *operator_sql* on the second connection
    strictly between planning and the batched INSERT."""
    gate = asyncio.Event()
    entered = asyncio.Event()
    settings = cron_settings(schema)
    backend = GatedEnqueueBackend(settings, gate=gate, entered=entered)

    async def _run() -> int:
        async with conn.transaction():
            return await tick_cron(conn, settings, backend, schema, new_uuid())

    task = asyncio.create_task(_run())
    await entered.wait()
    await operator_conn.execute(operator_sql, schedule_id)
    gate.set()
    return await task


class TestMidTickDisable:
    """The operator's disable commits while a tick holds the planned fire."""

    async def test_fire_lands_once_disable_survives_reenable_never_refires(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Gated tick + mid-tick disable, then re-enable and re-tick.

        Pinned contract: the in-flight tick's planned fire lands (exactly
        one job), the disable survives the successes UPDATE (which touches
        no enabled column), the successes advance still applies (the row
        records the fire that happened and moves past the delivered slot),
        and re-enabling afterward does NOT re-deliver the fired slot: the
        guarded-enqueue-without-guarded-advance shape would double-deliver
        here, which is why the advance must stay unguarded.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = await server_hour_floor(clean_pg_conn)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="disable-race",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )
        before = await schedule_row(clean_pg_conn, schema, schedule_id)

        operator = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            fired = await _run_gated_tick(
                clean_pg_conn,
                schema,
                operator,
                operator_sql=(
                    f'UPDATE "{schema}".cron_schedules SET enabled = false '  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
                    "WHERE id = $1"
                ),
                schedule_id=schedule_id,
            )
        finally:
            await operator.close()

        assert fired == 1, "the planned fire of the in-flight tick lands, once"
        jobs = await count_jobs(clean_pg_conn, schema, _ACTOR)
        assert jobs == 1, f"exactly one job for the fired slot, got {jobs}"

        after = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert after["enabled"] is False, "the successes UPDATE must not resurrect the disable"
        assert after["last_fired_at"] is not None, "the row records the fire that happened"
        assert after["next_fire_at"] > before["next_fire_at"], (
            "the advance moves past the delivered slot"
        )
        assert after["consecutive_failures"] == 0, "a fire resets the failure count"
        assert after["last_fire_error"] is None

        # Re-enable: the delivered slot must not re-deliver. next_fire_at
        # is a future hour boundary, so the next tick is idle for this
        # schedule - the double-delivery a naive guarded-advance fix
        # would introduce shows up here as a SECOND job.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET enabled = true WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            schedule_id,
        )
        async with clean_pg_conn.transaction():
            refired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
            )
        assert refired == 0, "the already-delivered slot must not re-fire on re-enable"
        jobs_after = await count_jobs(clean_pg_conn, schema, _ACTOR)
        assert jobs_after == 1, f"exactly one job total, got {jobs_after}"


class TestMidTickDelete:
    """The operator deletes the schedule while a tick holds the planned fire."""

    async def test_fire_lands_row_stays_gone_provenance_stamped(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Gated tick + mid-tick DELETE.

        Pinned contract: the in-flight fire lands (the job row is the
        fire), the deletion survives (no row is resurrected by the
        successes UPDATE - it matches nothing and no error escapes the
        tick), and the tick reports the fire it planned.  The deleted
        schedule's id lives on only in the delivered job's provenance
        metadata.
        """
        schema = module_pg_schema.schema_name
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = await server_hour_floor(clean_pg_conn)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="delete-race",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        operator = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            fired = await _run_gated_tick(
                clean_pg_conn,
                schema,
                operator,
                operator_sql=(
                    f'DELETE FROM "{schema}".cron_schedules WHERE id = $1'  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
                ),
                schedule_id=schedule_id,
            )
        finally:
            await operator.close()

        assert fired == 1, "the planned fire of the in-flight tick lands, once"
        jobs = await count_jobs(clean_pg_conn, schema, _ACTOR)
        assert jobs == 1, f"exactly one job for the fired slot, got {jobs}"
        rows = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            schedule_id,
        )
        assert rows == 0, "the deletion must stand: no success UPDATE may resurrect the row"
        provenance = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            "WHERE metadata->>'cron_schedule_id' = $1",
            str(schedule_id),
        )
        assert provenance == 1, "the delivered job carries the schedule id as provenance"
