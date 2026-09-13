"""Red-team attacks on the cron tick's policy preflight semantics (real PG).

Four attack surfaces against the singleton / ``max_pending`` preflight the
cron tick runs between planning and the batched enqueue:

* **Intra-batch ``max_pending`` (the parity hole the stamping itself
  created)** — the singleton gate is in memory (two due schedules for one
  singleton actor: the earlier fires, the later suppresses against the
  fired job), but the ``max_pending`` check reads the DB count alone.
  Two due schedules for one ``max_pending=1`` actor in the SAME tick both
  pass (neither of this tick's jobs exists yet) and both enqueue: cap+N
  jobs from one tick.  The client path cannot do this — its second,
  sequential enqueue sees the first job's row and raises
  ``MaxPendingExceededError``; parity requires the tick to account for
  its own kept plans.
* **The snoozed blocker** — a singleton job snoozed to ``scheduled`` is
  as active as a running one; the suppression predicate is
  ``status IN ('pending','scheduled','running')`` and the snooze is the
  issue's own named shape.
* **Cross-direction parity** — the stamps must close the loop in BOTH
  directions through the REAL paths: a cron-fired singleton job (stamped
  by the tick) must make a CLIENT enqueue raise
  ``SingletonCollisionError``, and a client-enqueued singleton job (via
  ``enqueue_with_conn``, the client path's own preflight + INSERT) must
  make the cron fire suppress.  Either half alone would mean the two
  paths key on different metadata.
* **Return value and metric semantics** — suppressed slots are absent
  from the tick's return count, and the ``taskq.backpressure.errors``
  counter increments once per suppressed slot, mirroring the enqueue
  path's once-per-attempt increment.
* **Suppression x catch-up recompute** — a schedule missed beyond the
  catch-up window AND singleton-blocked takes BOTH branches in one tick:
  planning re-anchors ``fire_at`` on the server clock, then the
  suppression UPDATE must advance ``next_fire_at`` to exactly that
  recomputed target — one advance, the recompute's own value, no stale
  slot, no ``last_fired_at``, and the re-anchored target keeps the
  schedule out of the due set so no hot re-fire loop runs against the
  blocker.
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
from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.exceptions import SingletonCollisionError
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import counter_data_points
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    count_jobs,
    cron_settings,
    make_backend,
    next_ten_min_boundary,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_now,
    ten_min_floor,
)
from .test_rt_cron_singleton_parity import seed_active_job

pytestmark = pytest.mark.integration

_SINGLETON_ACTOR = "rt_pref_singleton"
_CAPPED_ACTOR = "rt_pref_capped"
_HEALTHY_ACTOR = "rt_pref_healthy"
_BACKPRESSURE_COUNTER = "taskq.backpressure.errors"


def _client_enqueue_args(actor: str) -> EnqueueArgs:
    """Client-shaped enqueue args for *actor*: the singleton flag merged
    into caller metadata exactly the way ``client/_args.py`` stamps it
    (``metadata_dict["singleton"] = True`` on top of user metadata)."""
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="rt_queue",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=None,
        metadata={"origin": "client", "singleton": True},
    )


class TestSnoozedBlocker:
    """A snoozed (``scheduled``) singleton job is an active blocker."""

    async def test_snoozed_scheduled_singleton_job_suppresses_the_fire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A singleton job snoozed to ``scheduled`` (the retry-snooze
        destination) blocks the actor's due schedule exactly like a running
        one: no enqueue, one advance, no strike."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="scheduled", singleton=True
        )
        due_slot = ten_min_floor(datetime.now(UTC))
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="snoozed-blocker",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="snoozed-blocker",
        )

        with structlog.testing.capture_logs():
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies={_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)},
                )

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] > due_slot
        assert row["last_fired_at"] is None
        assert row["consecutive_failures"] == 0, "a snoozed blocker is backpressure, not a defect"


class TestCrossDirectionParity:
    """The stamps must close the loop in both directions, through the real
    paths on both sides."""

    async def test_cron_fired_singleton_job_blocks_client_enqueue(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A cron-fired singleton job is active and carries the stamp, so a
        CLIENT enqueue for the same actor (the real enqueue path: preflight
        + INSERT via ``enqueue_with_conn``) must raise
        ``SingletonCollisionError`` naming the cron-fired job. If the tick
        stamped anything the client preflight does not key on, this is
        where it surfaces."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="cross-cron-first",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=ten_min_floor(datetime.now(UTC)),
            identity_key="cross-cron-first",
        )
        backend = make_backend(settings)

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                backend,
                schema,
                new_uuid(),
                actor_policies={_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)},
            )
        assert fired == 1
        cron_job_id: UUID = await clean_pg_conn.fetchval(
            f'SELECT id FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the actor is $-bound.
            _SINGLETON_ACTOR,
        )

        with pytest.raises(SingletonCollisionError) as exc_info:
            async with clean_pg_conn.transaction():
                await backend.enqueue_with_conn(
                    clean_pg_conn, _client_enqueue_args(_SINGLETON_ACTOR)
                )

        assert exc_info.value.blocking_job_id == cron_job_id, (
            "the client preflight must see (and name) the cron-fired singleton job — "
            "otherwise the two paths key on different metadata"
        )
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1

    async def test_client_enqueued_singleton_job_blocks_cron_fire(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """The mirror direction, driven through the real client enqueue
        (``enqueue_with_conn`` with client-stamped metadata) rather than a
        hand-seeded row: the active client job must suppress the cron fire
        with the client job named as the blocker — the seeded-blocker shape
        the parity pins use, proven representative of what the client path
        actually writes."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        backend = make_backend(settings)

        async with clean_pg_conn.transaction():
            client_row = await backend.enqueue_with_conn(
                clean_pg_conn, _client_enqueue_args(_SINGLETON_ACTOR)
            )
        assert client_row.status == "pending"

        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="cross-client-first",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=ten_min_floor(datetime.now(UTC)),
            identity_key="cross-client-first",
        )
        schedule_id = await clean_pg_conn.fetchval(
            f'SELECT id FROM "{schema}".cron_schedules WHERE name = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; the name is $-bound.
            "cross-client-first",
        )

        with structlog.testing.capture_logs():
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    backend,
                    schema,
                    new_uuid(),
                    actor_policies={_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)},
                )

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1
        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        assert row["next_fire_at"] > await server_now(clean_pg_conn)
        assert row["last_fired_at"] is None
        assert row["consecutive_failures"] == 0


class TestIntraBatchMaxPending:
    """The cap must hold across the tick's OWN kept plans, not only against
    the pre-tick DB count."""

    async def test_second_due_schedule_for_capped_actor_suppresses_intra_batch(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two due schedules for one ``max_pending=1`` actor in the same
        tick, no pre-existing jobs: the earlier slot fires (count 0), the
        later one must be evaluated against the intra-batch reality (the
        fired job is pending the moment the batch commits — exactly the row
        a second, sequential CLIENT enqueue would see) and suppress. Today
        both pass the DB-only count and both enqueue: the cap is violated
        by the tick itself, a parity hole the stamping created."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR)
        grid = ten_min_floor(datetime.now(UTC))
        early_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_CAPPED_ACTOR,
            name="cap-intra-early",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=grid - timedelta(minutes=30),
            identity_key="cap-intra-early",
        )
        late_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_CAPPED_ACTOR,
            name="cap-intra-late",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=grid - timedelta(minutes=20),
            identity_key="cap-intra-late",
        )
        policies = {_CAPPED_ACTOR: ActorFirePolicy(max_pending=1)}

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter(
            obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
        )
        monkeypatch.setattr(
            otel_mod,
            "_backpressure_errors",
            meter.create_counter(_BACKPRESSURE_COUNTER, unit="1"),
        )

        with structlog.testing.capture_logs():
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies=policies,
                )

        assert fired == 1, "the earlier slot fires; the later one is suppressed"
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 1, (
            "both due schedules for a max_pending=1 actor enqueued from one tick — "
            "the DB-only count admits every plan in the batch, so the tick itself "
            "violates the cap the client path enforces"
        )
        early = await schedule_row(clean_pg_conn, schema, early_id)
        assert early["last_fired_at"] is not None
        late = await schedule_row(clean_pg_conn, schema, late_id)
        assert late["last_fired_at"] is None, "the later slot is suppressed, not fired"
        assert late["consecutive_failures"] == 0, "intra-batch backpressure is not a strike"
        assert late["enabled"] is True
        assert late["next_fire_at"] == grid - timedelta(minutes=10), (
            "the suppressed slot advances by its own cadence from its own slot"
        )
        points = counter_data_points(reader, _BACKPRESSURE_COUNTER)
        assert [(p.value, p.attributes) for p in points] == [
            (1, {"actor": _CAPPED_ACTOR, "kind": "max_pending"})
        ]


class TestReturnAndMetricSemantics:
    """Suppressed slots are not fires; suppressed slots are counted."""

    async def test_return_count_excludes_suppressed_slots(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A mixed batch — two healthy fires and one suppressed singleton
        slot — returns 2: the return value counts fires, and a suppressed
        slot is neither a fire nor a failure."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_actor_config(clean_pg_conn, schema, _HEALTHY_ACTOR)
        await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        due_slot = ten_min_floor(datetime.now(UTC))
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="mixed-suppressed",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=due_slot,
            identity_key="mixed-suppressed",
        )
        for i in range(2):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_HEALTHY_ACTOR,
                name=f"mixed-healthy-{i}",
                cron_expr=_TEN_MINUTELY,
                next_fire_at=due_slot,
                identity_key=f"mixed-healthy-{i}",
            )
        policies = {
            _SINGLETON_ACTOR: ActorFirePolicy(singleton=True),
            _HEALTHY_ACTOR: ActorFirePolicy(),
        }

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies=policies,
            )

        assert fired == 2, "two fires + one suppressed slot → the return counts 2"
        assert await count_jobs(clean_pg_conn, schema, _HEALTHY_ACTOR) == 2
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1

    async def test_backpressure_counter_increments_once_per_suppressed_slot(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two due schedules for one capped actor already at its cap: both
        slots suppress, and ``taskq.backpressure.errors`` increments once
        per suppressed slot (2) — mirroring the enqueue path, which counts
        once per attempted enqueue that finds the cap."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _CAPPED_ACTOR)
        await seed_active_job(clean_pg_conn, schema, _CAPPED_ACTOR, status="pending")
        due_slot = ten_min_floor(datetime.now(UTC))
        for i in range(2):
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_CAPPED_ACTOR,
                name=f"counter-{i}",
                cron_expr=_TEN_MINUTELY,
                next_fire_at=due_slot,
                identity_key=f"counter-{i}",
            )
        policies = {_CAPPED_ACTOR: ActorFirePolicy(max_pending=1)}

        reader = InMemoryMetricReader()
        meter = MeterProvider(metric_readers=[reader]).get_meter(
            obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
        )
        monkeypatch.setattr(
            otel_mod,
            "_backpressure_errors",
            meter.create_counter(_BACKPRESSURE_COUNTER, unit="1"),
        )

        with structlog.testing.capture_logs():
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies=policies,
                )

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _CAPPED_ACTOR) == 1
        points = counter_data_points(reader, _BACKPRESSURE_COUNTER)
        assert [(p.value, p.attributes) for p in points] == [
            (2, {"actor": _CAPPED_ACTOR, "kind": "max_pending"})
        ], "once per suppressed slot — the enqueue path's per-attempt semantic"


class TestSuppressionCatchUpInterplay:
    """A8: a beyond-window miss that is ALSO singleton-blocked takes the
    recompute branch and the suppression branch in one tick."""

    async def test_beyond_window_miss_blocked_advances_to_the_recomputed_target(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """A 90-minute-stale slot (beyond the 1h window) with an active
        singleton blocker: the tick suppresses, and the one advance that
        lands is the RECOMPUTED, server-clock-anchored target (strictly
        future, on the schedule's grid) — not the stale slot's cadence.
        No fire, no strike, and the next tick finds nothing due: the
        re-anchored target holds the schedule out of the due set instead
        of looping it against the blocker."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _SINGLETON_ACTOR)
        await seed_active_job(
            clean_pg_conn, schema, _SINGLETON_ACTOR, status="running", singleton=True
        )
        stale_slot = ten_min_floor(datetime.now(UTC)) - timedelta(minutes=90)
        schedule_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_SINGLETON_ACTOR,
            name="suppressed-recompute",
            cron_expr=_TEN_MINUTELY,
            next_fire_at=stale_slot,
            identity_key="suppressed-recompute",
        )

        now_before = await server_now(clean_pg_conn)
        with structlog.testing.capture_logs():
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies={_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)},
                )
        now_after = await server_now(clean_pg_conn)

        assert fired == 0
        assert await count_jobs(clean_pg_conn, schema, _SINGLETON_ACTOR) == 1

        row = await schedule_row(clean_pg_conn, schema, schedule_id)
        expected = {
            next_ten_min_boundary(now_before) + timedelta(minutes=10),
            next_ten_min_boundary(now_after) + timedelta(minutes=10),
        }
        assert row["next_fire_at"] in expected, (
            f"the suppressed advance landed on {row['next_fire_at']}, not the "
            f"recomputed server-clock anchor {expected} — the suppression UPDATE "
            "and the catch-up recompute disagree on the target"
        )
        assert row["next_fire_at"] > now_after, (
            "a stale-slot advance would sit in the past and re-fire against the blocker every tick"
        )
        assert row["last_fired_at"] is None, "nothing fired — no last_fired_at stamp"
        assert row["consecutive_failures"] == 0, "suppression is not a strike"
        assert row["enabled"] is True
        assert row["last_fire_error"] is None

        # The re-anchored target holds: the next tick finds nothing due and
        # leaves the row bit-for-bit alone — no hot re-fire loop.
        before_second = await schedule_row(clean_pg_conn, schema, schedule_id)
        async with clean_pg_conn.transaction():
            second = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies={_SINGLETON_ACTOR: ActorFirePolicy(singleton=True)},
            )
        assert second == 0
        assert await schedule_row(clean_pg_conn, schema, schedule_id) == before_second
