"""Red-team attacks on the cron tick's post-write observability (real PG).

C9, at real-PG level (the fake-conn pins in ``tests/test_cron_loop.py``
cover the SQL shapes; these pin that the calls actually happen around
committed writes, with the values a real tick produces):

* ``record_cron_failure`` — ``+1`` per failed schedule; ``-prev`` on a
  success that follows failures (the counter reset, not a bare ``-1``).
* ``record_published_message`` — once per fired schedule, with the
  actor and the queue from ``actor_config``.
* the ``cron fired`` / ``cron fire failed`` / ``cron schedule
  auto-disabled`` events carry the firing ``worker_id``.
* the ``-> int`` return equals the number of successes — including a
  mixed batch where planning failures and successes share one tick.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker import cron_loop
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_obs_actor"
_QUEUE = "rt_obs_queue"
_BAD_FACTORY = "nonexistent.module.fn"


class TestObservabilityOnCommit:
    async def test_success_after_failures_resets_counter_and_publishes(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A schedule with 2 prior failures fires: the tick records
        ``record_cron_failure(id, -2)`` (the full reset), one
        ``record_published_message(actor, queue)``, a ``cron fired`` event
        with the worker id, and the DB row shows the reset."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="recovering",
            cron_expr=_HOURLY,
            next_fire_at=hour_floor(datetime.now(UTC)),
            consecutive_failures=2,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda sid, delta: cron_failure_calls.append((sid, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 1
        assert cron_failure_calls == [(str(schedule_id), -2)], (
            "a success after 2 failures must reset the UpDownCounter by the FULL "
            f"previous count; saw {cron_failure_calls}"
        )
        assert published == [(_ACTOR, _QUEUE)]
        fired_events = [e for e in captured if e["event"] == "cron fired"]
        assert len(fired_events) == 1
        assert fired_events[0]["worker_id"] == str(worker_id)
        assert fired_events[0]["actor"] == _ACTOR

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None

    async def test_failures_count_up_and_log_worker_attribution(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two failing schedules in one tick: ``+1`` per schedule in due
        order, one ``cron fire failed`` event each with worker id and the
        raw error text, return 0."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        first_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fails-first",
            cron_expr=_HOURLY,
            next_fire_at=due - _ONE_MINUTE,
            payload_factory=_BAD_FACTORY,
        )
        second_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="fails-second",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory=_BAD_FACTORY,
        )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda sid, delta: cron_failure_calls.append((sid, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 0
        assert cron_failure_calls == [
            (str(first_id), 1),
            (str(second_id), 1),
        ], f"one +1 per failed schedule, in due order; saw {cron_failure_calls}"
        assert published == [], "a failed fire publishes nothing"

        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 2
        assert {e["worker_id"] for e in failed} == {str(worker_id)}
        assert all("nonexistent" in e["error"] for e in failed), (
            "the raw resolution error must reach the log event"
        )

    async def test_mixed_batch_return_counts_successes_only(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two successes and one planning failure in one tick: return 2,
        two published messages, one +1 failure record, and the log stream
        shows two ``cron fired`` and one ``cron fire failed``."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR, queue=_QUEUE)
        due = hour_floor(datetime.now(UTC))
        bad_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="mixed-bad",
            cron_expr=_HOURLY,
            next_fire_at=due - _ONE_MINUTE,
            payload_factory=_BAD_FACTORY,
        )
        for i in range(2):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"mixed-good-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
            )

        cron_failure_calls: list[tuple[str, int]] = []
        published: list[tuple[str, str]] = []
        monkeypatch.setattr(
            cron_loop,
            "record_cron_failure",
            lambda sid, delta: cron_failure_calls.append((sid, delta)),
        )
        monkeypatch.setattr(
            cron_loop,
            "record_published_message",
            lambda actor, queue: published.append((actor, queue)),
        )

        worker_id = new_uuid()
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn, settings, make_backend(settings), schema, worker_id
                )

        assert fired == 2, "the return value must equal the successes, not the batch"
        assert cron_failure_calls == [(str(bad_id), 1)]
        assert published == [(_ACTOR, _QUEUE), (_ACTOR, _QUEUE)]
        events = [e["event"] for e in captured]
        assert events.count("cron fired") == 2
        assert events.count("cron fire failed") == 1
        assert all(
            e["worker_id"] == str(worker_id)
            for e in captured
            if e["event"] in ("cron fired", "cron fire failed")
        )


# ── Helpers ────────────────────────────────────────────────────────────

_ONE_MINUTE = timedelta(minutes=1)
