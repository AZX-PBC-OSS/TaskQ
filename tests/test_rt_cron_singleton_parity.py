"""Red-team attacks on cron-fire parity with the client enqueue stamps (real PG).

Two halves of one defect, pinned together because either half alone is
wrong:

* **Parity (the live gap)** — ``@actor(singleton=True)`` and
  ``@actor(max_pending=N)`` reach client enqueues only:
  ``client/_args.py`` stamps ``metadata["singleton"]`` and
  ``max_pending`` onto :class:`EnqueueArgs` at build time, while the cron
  tick builds its own args with neither.  A cron fire for a singleton
  actor carries no flag — the ``jobs_singleton_uniq`` partial index keys
  on exactly that flag, so it never protects cron fires — and no
  max_pending cap applies.  The actor's own schedule happily enqueues a
  second active singleton job.
* **The trap (arms the moment parity lands, unless classified)** — a
  collision that reaches the tick's generic failure path strikes its
  schedule (``consecutive_failures`` +1 per tick), and
  ``cron_auto_disable_threshold`` (default 3) collisions permanently set
  ``enabled = false``: a healthy, busy actor bricks its own schedule.
  Suppression is therefore classified as neither failure nor fire — the
  slot advances ``next_fire_at`` alone, with no ``last_fired_at`` stamp,
  no error write, and no strike.

Every test drives :func:`taskq.worker.cron_loop.tick_cron` against a real
schema with an ``actor_policies`` mapping, the way the worker bootstrap
derives it from its ``actor_registry``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import counter_data_points
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_now,
    ten_min_floor,
)

pytestmark = pytest.mark.integration

_SINGLETON_ACTOR = "rt_parity_singleton"
_CAPPED_ACTOR = "rt_parity_capped"
_HEALTHY_ACTOR = "rt_parity_healthy"
_BACKPRESSURE_COUNTER = "taskq.backpressure.errors"

_SINGLETON_POLICIES = {_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)}


async def boom_factory() -> dict[str, object]:
    """Payload factory that always raises.

    Dotted path: ``tests.test_rt_cron_singleton_parity.boom_factory``.
    """
    raise RuntimeError("parity-check factory exploded")


async def seed_active_job(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    *,
    status: str,
    singleton: bool = False,
) -> UUID:
    """Insert one ``jobs`` row for *actor* in *status*, optionally flagged
    as a singleton job — exactly the rows the tick's suppression preflight
    and the ``jobs_singleton_uniq`` partial index key on."""
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, metadata) "
        f"VALUES ($1, $2, 'rt_queue', '{{}}'::jsonb, 5, 'transient', "
        f'$3::"{schema}".job_status, $4::jsonb)',
        job_id,
        actor,
        status,
        '{"singleton": true}' if singleton else "{}",
    )
    return job_id


class TestSingletonParity:
    """The parity gap: cron fires for singleton actors must be suppressed
    while an active singleton job exists, and must carry the flag when they
    do fire."""

    async def test_singleton_actor_fire_is_suppressed_while_previous_active(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """THE parity pin. A running singleton job blocks the actor's due
        schedule: the tick enqueues nothing, advances the schedule strictly
        into the future, and records neither a fire (``last_fired_at``
        stays NULL) nor a strike. Today the tick enqueues a second active
        singleton job — the exact gap this file exists to close."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        blocker_id = await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        due_slot = ten_min_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="singleton-parity",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="singleton-parity",
        )
        worker_id = new_uuid()

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    worker_id,
                    actor_policies=_SINGLETON_POLICIES,
                )
        now_after = await server_now(clean_pg_conn)

        assert fired == 0, "a suppressed slot is not a fire — the return counts fires only"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "the tick enqueued a second active singleton job — the parity gap: "
            "cron fires bypass the singleton flags the client path stamps"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] > now_after, (
            "a suppressed slot must advance next_fire_at strictly into the "
            "future, or the schedule stays due and re-fires against the blocker"
        )
        assert row["next_fire_at"] > due_slot, "the advance must come from the plan, not a no-op"
        assert row["last_fired_at"] is None, "nothing fired — no last_fired_at stamp"
        assert row["consecutive_failures"] == 0, "suppression is not a failure — no strike"
        assert row["enabled"] is True
        assert row["last_fire_error"] is None

        collisions = [e for e in captured if e["event"] == "singleton-collision"]
        assert len(collisions) == 1
        assert collisions[0]["blocking_job_id"] == str(blocker_id)
        assert collisions[0]["detection_path"] == "cron_tick_preflight"
        assert collisions[0]["schedule_id"] == str(schedule_id)
        assert collisions[0]["worker_id"] == str(worker_id)

        # A second tick while the blocker persists: the cadence elapsing is
        # simulated deterministically by moving next_fire_at back onto the
        # due slot (a wall-clock sleep would be the only alternative). The
        # blocker is untouched and still active.
        advanced_to = row["next_fire_at"]
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $1 WHERE id = $2',  # noqa: S608  # Why: schema is a test-fixture identifier; ids are $-bound.
            due_slot,
            schedule_id,
        )
        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=_SINGLETON_POLICIES,
            )
        assert second == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "a re-due slot against the still-active blocker must not enqueue"
        )
        row_again = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row_again["next_fire_at"] == advanced_to, (
            "the re-due slot must advance again by its own cadence — an "
            "unmoved next_fire_at is the hot re-fire loop against the blocker"
        )
        assert row_again["consecutive_failures"] == 0
        assert row_again["enabled"] is True
        assert row_again["last_fired_at"] is None

    async def test_cron_fired_singleton_job_carries_the_flag(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """No blocker: the fire lands, and the new job's metadata contains
        ``"singleton": true`` — the exact predicate the
        ``jobs_singleton_uniq`` partial index keys on. Without the stamp
        the index never protects cron fires at all."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        due_slot = ten_min_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="singleton-stamp",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="singleton-stamp",
        )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies=_SINGLETON_POLICIES,
            )
        assert fired == 1

        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        has_flag: bool = await clean_pg_conn.fetchval(
            f"SELECT metadata @> '{{\"singleton\": true}}'::jsonb "  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the escaped jsonb literal.
            f'FROM "{schema}".jobs WHERE actor = $1',
            _SINGLETON_ACTOR,
        )
        assert has_flag is True, (
            "the cron-fired job carries no singleton flag — the partial unique "
            "index keys on this exact predicate, so cron fires remain "
            "unprotected by it (the client enqueue path stamps it)"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["last_fired_at"] is not None
        assert row["consecutive_failures"] == 0
        assert row["next_fire_at"] > due_slot

    async def test_blocker_released_next_tick_fires(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Suppression is not sticky: once the blocker goes terminal, the
        next due tick fires normally (and the new fire itself carries the
        flag, becoming the next blocker — singleton semantics, held by the
        schedule's own fires)."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        blocker_id = await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        # A catch-up seeding: the slot is 30 minutes old, so the first
        # (suppressed) advance stays in the past and the schedule is due
        # again immediately — no clock manipulation needed for tick 2.
        slot = ten_min_floor(datetime.now(UTC)) - timedelta(minutes=30)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="singleton-release",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=slot,
            identity_key="singleton-release",
        )
        worker_id = new_uuid()

        async with clean_pg_conn.transaction():
            first = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=_SINGLETON_POLICIES,
            )
        assert first == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        assert (await schedule_row(clean_pg_conn, schema, schedule_id))["next_fire_at"] == (
            slot + timedelta(minutes=10)
        ), "sequential catch-up: the suppressed slot advances one cadence"

        await clean_pg_conn.execute(
            f'UPDATE "{schema}".jobs SET status = \'succeeded\'::"{schema}".job_status '  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
            "WHERE id = $1",
            blocker_id,
        )

        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=_SINGLETON_POLICIES,
            )
        assert second == 1, "with the blocker terminal the due slot must fire"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 2
        new_has_flag: bool = await clean_pg_conn.fetchval(
            f"SELECT metadata @> '{{\"singleton\": true}}'::jsonb "  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the escaped jsonb literal.
            f'FROM "{schema}".jobs WHERE actor = $1 AND id <> $2',
            _SINGLETON_ACTOR,
            blocker_id,
        )
        assert new_has_flag is True, "the released fire must carry the singleton flag"
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["last_fired_at"] is not None
        assert row["consecutive_failures"] == 0
        assert row["enabled"] is True
        assert row["next_fire_at"] == slot + timedelta(minutes=20)

    async def test_two_due_singleton_schedules_one_actor_fire_one_suppress_one(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Two schedules for one singleton actor due in the same tick, no
        pre-existing blocker: the earlier slot fires (it becomes the
        blocker), the later one is suppressed with the fired job named as
        the blocker. Both schedules carry the singleton stamp, so without
        the intra-batch gate the batched INSERT would hit
        ``jobs_singleton_uniq`` itself, abort the whole tick and strike
        BOTH schedules — the trap, re-entered through the batch."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        early_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="intra-early",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=grid - timedelta(minutes=30),
            identity_key="intra-early",
        )
        late_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="intra-late",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=grid - timedelta(minutes=20),
            identity_key="intra-late",
        )
        worker_id = new_uuid()

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    worker_id,
                    actor_policies=_SINGLETON_POLICIES,
                )

        assert fired == 1
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        fired_job_id: UUID = await clean_pg_conn.fetchval(
            f'SELECT id FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
            _SINGLETON_ACTOR,
        )

        early = await schedule_row(clean_pg_conn, schema, early_id)
        assert early["last_fired_at"] is not None, "the earlier due slot is the one that fires"
        assert early["consecutive_failures"] == 0
        late = await schedule_row(clean_pg_conn, schema, late_id)
        assert late["last_fired_at"] is None, "the later slot is suppressed, not fired"
        assert late["consecutive_failures"] == 0, "intra-batch suppression is not a strike"
        assert late["enabled"] is True
        assert late["next_fire_at"] == grid - timedelta(minutes=10), (
            "the suppressed slot advances by its own cadence from its own slot"
        )

        collisions = [e for e in captured if e["event"] == "singleton-collision"]
        assert len(collisions) == 1
        assert collisions[0]["blocking_job_id"] == str(fired_job_id), (
            "the intra-batch collision must attribute the fired job as the blocker"
        )
        assert collisions[0]["schedule_id"] == str(late_id)
        assert [e for e in captured if e["event"] == "cron fire failed"] == []


class TestAutoDisableTrap:
    """The trap: three collisions must never auto-disable a healthy,
    busy actor's schedule."""

    async def test_three_collisions_do_not_auto_disable(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Three ticks with the blocker active across all of them (a
        long-running singleton job): every tick suppresses, none strikes,
        and the schedule is still enabled with ``consecutive_failures`` 0
        after the third — the collision count that would permanently
        disable it if suppression landed in the failure path."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        # Catch-up seeding: each tick's advance stays one cadence behind the
        # wall clock, so all three ticks find the schedule due and suppress
        # for real — a future-advancing seed would make ticks 2 and 3
        # trivially empty and prove nothing about the strike path.
        slot = ten_min_floor(datetime.now(UTC)) - timedelta(minutes=30)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="three-collisions",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=slot,
            identity_key="three-collisions",
        )
        worker_id = new_uuid()

        for tick_no in range(3):
            with structlog.testing.capture_logs() as captured:
                async with clean_pg_conn.transaction():
                    fired = await tick_cron(
                        clean_pg_conn,
                        settings,
                        make_backend(settings),
                        schema,
                        worker_id,
                        actor_policies=_SINGLETON_POLICIES,
                    )
            assert fired == 0, f"tick {tick_no + 1} must suppress, not fire"
            events = [e["event"] for e in captured]
            assert "cron fire failed" not in events, (
                f"tick {tick_no + 1} routed a suppressed slot into the failure path"
            )
            assert "cron schedule auto-disabled" not in events

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["enabled"] is True, (
            "three collisions auto-disabled a healthy busy actor's schedule — "
            "the exact trap the suppression classification exists to foreclose"
        )
        assert row["consecutive_failures"] == 0
        assert row["last_fire_error"] is None
        assert row["last_fired_at"] is None
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        assert row["next_fire_at"] == slot + timedelta(minutes=30), (
            "three suppressed ticks advance one cadence each (sequential catch-up without firing)"
        )


class TestMixedBatch:
    """A suppressed singleton actor must not disturb a healthy peer firing
    in the same tick."""

    async def test_suppressed_and_healthy_actors_in_one_tick(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A blocked singleton actor and a normal actor due in the same
        tick: the healthy one fires exactly one job with its actor_config
        fields, the suppressed one advances only, and the tick enqueues
        exactly one job in total."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_actor_config(
            clean_pg_conn,
            schema,
            _HEALTHY_ACTOR,
            queue="rt_healthy_queue",
            max_attempts=7,
            retry_kind="indefinite",
        )
        await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        due_slot = ten_min_floor(datetime.now(UTC))
        suppressed_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="mixed-blocked",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="mixed-blocked",
        )
        healthy_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_HEALTHY_ACTOR,
            name="mixed-healthy",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="mixed-healthy",
        )
        # The bootstrap derives a policy for EVERY actor; a flagless policy
        # must neither suppress nor stamp anything.
        policies = {
            _SINGLETON_ACTOR: ActorFirePolicy(singleton=True),
            _HEALTHY_ACTOR: ActorFirePolicy(),
        }

        tick_start = await server_now(clean_pg_conn)
        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies=policies,
            )

        assert fired == 1, "the return counts the healthy fire only"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            "the blocked singleton actor must keep only its blocker job"
        )
        healthy_jobs = await clean_pg_conn.fetch(
            f"SELECT actor, queue, status::text AS status, max_attempts, "  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
            f"retry_kind::text AS retry_kind "
            f'FROM "{schema}".jobs WHERE actor = $1',
            _HEALTHY_ACTOR,
        )
        assert [dict(j) for j in healthy_jobs] == [
            {
                "actor": _HEALTHY_ACTOR,
                "queue": "rt_healthy_queue",
                "status": "pending",
                "max_attempts": 7,
                "retry_kind": "indefinite",
            }
        ], "the healthy fire carries its actor_config fields exactly"

        tick_enqueued: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE created_at >= $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the timestamp is $-bound.
            tick_start,
        )
        assert tick_enqueued == 1, (
            "exactly one job may come out of this tick — the suppressed slot "
            f"contributed {tick_enqueued - 1} extra"
        )

        suppressed = await schedule_row(clean_pg_conn, schema, suppressed_id)
        assert suppressed["last_fired_at"] is None
        assert suppressed["consecutive_failures"] == 0
        assert suppressed["next_fire_at"] > tick_start
        healthy = await schedule_row(clean_pg_conn, schema, healthy_id)
        assert healthy["last_fired_at"] is not None
        assert healthy["consecutive_failures"] == 0


class TestMaxPendingParity:
    """max_pending must cap cron fires exactly as it caps client enqueues."""

    async def test_max_pending_cap_suppresses_cron_fire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A capped actor with one pending job and a due schedule: the tick
        enqueues nothing (the cap mirrors the enqueue path's own
        predicate), advances the slot, records no strike, emits
        ``max-pending-exceeded`` and bumps ``taskq.backpressure.errors``.
        When the pending job goes terminal the same schedule fires on a
        later tick — capacity frees, the gate opens."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR)
        pending_id = await seed_active_job(clean_pg_conn, schema, _CAPPED_ACTOR, status="pending")
        due_slot = ten_min_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_CAPPED_ACTOR,
            name="capped",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="capped",
        )
        policies = {_CAPPED_ACTOR: ActorFirePolicy(max_pending=1)}
        worker_id = new_uuid()

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter(
            obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
        )
        monkeypatch.setattr(
            otel_mod,
            "_backpressure_errors",
            meter.create_counter(_BACKPRESSURE_COUNTER, unit="1"),
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    worker_id,
                    actor_policies=policies,
                )

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 1, (
            "the tick enqueued past the actor's max_pending cap — cron fires "
            "bypass the backpressure the client path enforces"
        )
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] > due_slot, "the capped slot advances, it does not sit due"
        assert row["consecutive_failures"] == 0, "backpressure is not a strike"
        assert row["enabled"] is True
        assert row["last_fired_at"] is None

        points = counter_data_points(reader, _BACKPRESSURE_COUNTER)
        assert [(p.value, p.attributes) for p in points] == [
            (1, {"actor": _CAPPED_ACTOR, "kind": "max_pending"})
        ], "the suppressed cap must record the same backpressure counter the enqueue path does"

        exceeded = [e for e in captured if e["event"] == "max-pending-exceeded"]
        assert len(exceeded) == 1
        assert exceeded[0]["actor"] == _CAPPED_ACTOR
        assert exceeded[0]["current_count"] == 1
        assert exceeded[0]["max_pending"] == 1
        assert exceeded[0]["schedule_id"] == str(schedule_id)
        assert exceeded[0]["worker_id"] == str(worker_id)

        # Capacity frees: the pending job goes terminal, the cadence elapses
        # (simulated deterministically by moving the slot back onto the due
        # grid), and the same schedule fires.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".jobs SET status = \'succeeded\'::"{schema}".job_status '  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
            "WHERE id = $1",
            pending_id,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules SET next_fire_at = $1 WHERE id = $2',  # noqa: S608  # Why: schema is a test-fixture identifier; the id is $-bound.
            due_slot,
            schedule_id,
        )
        async with clean_pg_conn.transaction():
            released = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=policies,
            )
        assert released == 1, "with zero pending jobs the same schedule must fire"
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 2
        still_one = counter_data_points(reader, _BACKPRESSURE_COUNTER)
        assert sum(int(p.value) for p in still_one) == 1, (
            "the healthy fire must not record another backpressure error"
        )


class TestFailureClassification:
    """The classification must not blunt genuine failures."""

    async def test_genuine_failure_still_strikes(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A payload factory that raises, with a policy present:
        ``consecutive_failures`` increments, the error text lands in
        ``last_fire_error``, no job is enqueued — a real defect still
        strikes toward auto-disable exactly as before the classification."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="genuine-failure",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=ten_min_floor(datetime.now(UTC)),
            identity_key="genuine-failure",
            payload_factory="tests.test_rt_cron_singleton_parity.boom_factory",
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies=_SINGLETON_POLICIES,
                )

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 0
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 1, (
            "a genuine payload-factory failure must still strike — the "
            "suppression classification may only absorb policy collisions"
        )
        assert "parity-check factory exploded" in (row["last_fire_error"] or "")
        assert row["enabled"] is True
        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 1
        assert "parity-check factory exploded" in failed[0]["error"]


class TestSuppressionPreservesStreak:
    """The suppression UPDATE touches ``next_fire_at`` ONLY: a suppressed
    slot must neither punish (no strike) nor amnesty (no reset) — a
    schedule carrying a pre-existing failure streak keeps it across a
    suppressed tick, and the streak still counts toward auto-disable."""

    async def test_suppressed_tick_preserves_a_pre_existing_failure_streak(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Seed a schedule at ``consecutive_failures = 2`` (one below the
        default 3-strike threshold) with a recorded prior error, an active
        singleton blocker, and a working payload.

        Tick 1 (blocker active) suppresses: the streak must read 2 after —
        not 3 (suppression is not a failure) and not 0 (suppression is not
        amnesty for earlier genuine failures) — and the prior error text
        must survive (suppression does not overwrite ``last_fire_error``).

        Tick 2 (blocker still active, payload factory now exploding) is a
        genuine planning failure: the streak reaches 3 and the schedule
        auto-disables.  Had the suppression amnestied the streak to 0, the
        schedule would survive tick 2 at streak 1 — this is the assertion
        that discriminates \"preserved\" from \"reset\"."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        blocker_id = await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        due_slot = ten_min_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="streak-preservation",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="streak-preservation",
            consecutive_failures=2,
        )
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            "SET last_fire_error = $2 WHERE id = $1",
            schedule_id,
            "prior genuine failure",
        )
        worker_id = new_uuid()

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=_SINGLETON_POLICIES,
            )

        assert fired == 0, "the blocker is active — the tick must suppress"
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["consecutive_failures"] == 2, (
            "suppression must neither strike nor amnesty: a pre-existing "
            "streak of 2 survives the suppressed slot verbatim"
        )
        assert row["last_fire_error"] == "prior genuine failure", (
            "suppression writes next_fire_at ONLY — the recorded prior "
            "failure must not be overwritten or cleared"
        )
        assert row["last_fired_at"] is None
        assert row["enabled"] is True
        assert row["next_fire_at"] > due_slot, "the suppressed slot still advances"
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1

        # Tick 2: same blocker, but planning now fails for real — the
        # preserved streak reaches the threshold and disables the schedule.
        await clean_pg_conn.execute(
            f'UPDATE "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
            "SET payload_factory = $2, next_fire_at = $3 WHERE id = $1",
            schedule_id,
            "tests.test_rt_cron_singleton_parity.boom_factory",
            due_slot,
        )
        async with clean_pg_conn.transaction():
            fired_again = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                worker_id,
                actor_policies=_SINGLETON_POLICIES,
            )

        assert fired_again == 0
        row_again = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row_again["consecutive_failures"] == 3
        assert row_again["enabled"] is False, (
            "the streak the suppressed tick preserved must still count toward "
            "auto-disable: 2 (preserved) + 1 (genuine failure) = 3 = threshold"
        )
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1, (
            f"only the blocker {blocker_id} exists — neither tick enqueued"
        )
