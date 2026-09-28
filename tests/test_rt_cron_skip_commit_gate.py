"""Real-PG proof: the skip telemetry rides the REAL commit gate.

The census pins in ``tests/test_cron_skip_census.py`` prove the
count-once / rollback-silent semantics against fakes — but every fake
tick takes :func:`cron_loop._emit_on_commit`'s INLINE fallback (the fake
cannot carry a session ``LISTEN``), so the fallback exercises the
emission, not the gate. This file drives :func:`tick_cron` against a
real Postgres transaction whose COMMIT is forced to fail by a deferred
constraint trigger, with a real ``NOTIFY`` riding a real ``COMMIT`` —
and pins the census's core claim on the shipped path:

* a ROLLED-BACK beyond-window tick — strikes, skip plans, suppression
  UPDATEs and all — reports NOTHING on ``taskq.cron.skipped_slots`` or
  ``taskq.cron.slots_behind`` (the armed emission is never answered);
* the schedule's owed slots survive the rollback unadvanced, so the
  re-attempted tick counts them EXACTLY once (never doubled by the
  rolled-back attempt that planned the same count);
* the committed attempt's count arrives through the gate's NOTIFY
  dispatch (no fallback warning preceded it).
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker import cron_loop
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_now,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_skip_gate_actor"


async def test_a_rolled_back_beyond_window_tick_reports_nothing_and_the_reattempt_counts_once(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The census's no-double-count claim, on the real commit gate.

    Tick 1: a schedule ten fire-units past a 300 s catch-up window
    plans its skip (3 dropped */5 slots), executes the success UPDATE,
    arms the emission behind the gate — and the COMMIT fails. The
    counter and gauge must say NOTHING, and the row's ``next_fire_at``
    must still hold the owed slot: the re-attempt is real.

    Tick 2: the same owed slot, committed this time. The counter reads
    exactly one increment of 3 — the rolled-back attempt's planned
    count must not have stacked — and the row is finally advanced past
    the dropped history.
    """
    from datetime import timedelta

    schema = module_pg_schema.schema_name
    settings = cron_settings(schema, TASKQ_CRON_CATCH_UP_WINDOW="300")
    await seed_actor_config(clean_pg_conn, schema, _ACTOR)

    # The owed slot: the */5 grid point ten minutes before now, so the
    # count is grid-exact (3) regardless of where inside the period the
    # server clock currently sits.
    now = await server_now(clean_pg_conn)
    grid = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
    owed = grid - timedelta(minutes=10)
    schedule_id = await seed_schedule(
        clean_pg_conn,
        schema,
        actor=_ACTOR,
        name="skip-census-rollback",
        cron_expr="*/5 * * * *",
        next_fire_at=owed,
    )

    skip_calls: list[tuple[str, int]] = []
    gauge_calls: list[dict[str, int]] = []
    monkeypatch.setattr(
        cron_loop,
        "record_cron_skipped_slots",
        lambda actor, count: skip_calls.append((actor, count)),
    )
    monkeypatch.setattr(
        cron_loop,
        "update_cron_slots_behind",
        lambda data: gauge_calls.append(dict(data)),
    )

    # DEFERRABLE INITIALLY DEFERRED: evaluated only at COMMIT time,
    # strictly after every statement the tick runs and after the
    # emission's pg_notify was armed. It always raises, so the COMMIT
    # itself fails deterministically.
    await clean_pg_conn.execute(
        f'CREATE OR REPLACE FUNCTION "{schema}".skip_gate_fail() '
        "RETURNS trigger AS $$ "
        "BEGIN RAISE EXCEPTION 'commit-gate: forced commit failure'; END; "
        "$$ LANGUAGE plpgsql"
    )
    await clean_pg_conn.execute(
        "CREATE CONSTRAINT TRIGGER skip_gate_fail_trg "
        f'AFTER UPDATE ON "{schema}".cron_schedules '
        "DEFERRABLE INITIALLY DEFERRED "
        f'FOR EACH ROW EXECUTE FUNCTION "{schema}".skip_gate_fail()'
    )

    with pytest.raises(asyncpg.RaiseError, match="commit-gate"):
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn, settings, make_backend(settings), schema, new_uuid()
            )
            assert fired == 1

    assert skip_calls == [], (
        "the rolled-back attempt must report nothing - its owed slots "
        "were re-attempted, not dropped (the armed emission was never "
        "answered)"
    )
    assert gauge_calls == [], "the gauge must not move for an uncommitted tick"
    row = await schedule_row(clean_pg_conn, schema, schedule_id)
    assert row["next_fire_at"] == owed, (
        "the rollback left the owed slot unadvanced - the re-attempt is real"
    )

    # The re-attempt, committed: drop the trigger and tick again. The
    # same owed slot, counted ONCE.
    await clean_pg_conn.execute(f'DROP TRIGGER skip_gate_fail_trg ON "{schema}".cron_schedules')
    async with clean_pg_conn.transaction():
        fired = await tick_cron(clean_pg_conn, settings, make_backend(settings), schema, new_uuid())
    assert fired == 1

    assert skip_calls == [(_ACTOR, 3)], (
        f"the committed re-attempt counts the drop exactly once; saw {skip_calls} - "
        "the rolled-back attempt's planned count must not have stacked"
    )
    assert gauge_calls == [{_ACTOR: 3}]
    now_after = await server_now(clean_pg_conn)
    row = await schedule_row(clean_pg_conn, schema, schedule_id)
    assert row["next_fire_at"] > now_after, (
        "the committed advance landed strictly in the future - the "
        "dropped history is never re-queued for replay"
    )
