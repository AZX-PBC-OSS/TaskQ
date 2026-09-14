"""Red-team attacks on the bounded tick WITH policies in play (real PG).

Two surfaces the bounded-tick contract has never been pinned against:

* **The statement budget under the policy preflights** — the bounded-file
  tests pass no ``actor_policies``, so nobody has pinned that a tick over
  a large mixed due set (singleton-flagged actors, capped actors, healthy
  actors) still spends a bounded handful of statements.  The preflights
  are one statement per DISTINCT flagged actor set (``ANY($1::text[])``
  + ``GROUP BY actor``), not per schedule, and the intra-batch gates are
  in memory — a regression to per-schedule preflighting would trip the
  leader's deadline under exactly the catch-up burst the bound exists
  for.
* **Genuine failures under the policy stamping path** — the failure
  classification exists to absorb policy collisions, and the existing
  pin exercises a failing factory with the policy mapping present but no
  stamped enqueue ever built (the factory raises before stamping).  The
  sharper shape: a failing schedule and a stamp-carrying fire for the
  SAME singleton actor in ONE tick — the classification must strike the
  real failure while the stamping path is provably active.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    CountingConn,
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    ten_min_floor,
)

pytestmark = pytest.mark.integration

_QUEUE = "rt_budget_queue"

# 20 singleton-flagged actors (2 due schedules each — the intra-batch gate
# fires one, suppresses one), 5 capped actors at their cap (pre-seeded
# pending job), 5 healthy actors: 50 due schedules in one tick.
_SINGLETON_ACTOR_COUNT = 20
_CAPPED_ACTOR_COUNT = 5
_HEALTHY_ACTOR_COUNT = 5


async def boom_factory() -> dict[str, object]:
    """Payload factory that always raises.

    Dotted path: ``tests.test_rt_cron_parity_budget.boom_factory``.
    """
    raise RuntimeError("budget-check factory exploded")


def _singleton_actor(i: int) -> str:
    return f"rt_budget_s{i:02d}"


def _capped_actor(i: int) -> str:
    return f"rt_budget_c{i}"


def _healthy_actor(i: int) -> str:
    return f"rt_budget_h{i}"


async def _seed_actor_configs(conn: asyncpg.Connection, schema: str, actors: list[str]) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_attempts, retry_kind) '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound via unnest.
        "SELECT a, $2, 5, 'transient' FROM unnest($1::text[]) AS a "
        "ON CONFLICT (actor) DO NOTHING",
        actors,
        _QUEUE,
    )


async def _seed_schedules(
    conn: asyncpg.Connection,
    schema: str,
    rows: list[tuple[str, str, datetime]],
) -> None:
    """Bulk-seed schedules as (actor, name, next_fire_at) rows."""
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound via unnest.
        "(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, "
        " enabled, next_fire_at, metadata, identity_key, consecutive_failures) "
        "SELECT t.id, t.actor, t.name, $5, 'UTC', 'skip', NULL, true, t.slot, "
        "       '{}'::jsonb, t.name, 0 "
        "FROM unnest($1::uuid[], $2::text[], $3::text[], $4::timestamptz[]) "
        "AS t(id, actor, name, slot)",
        [new_uuid() for _ in rows],
        [r[0] for r in rows],
        [r[1] for r in rows],
        [r[2] for r in rows],
        _TEN_MINUTELY,
    )


async def _seed_blocker_jobs(conn: asyncpg.Connection, schema: str, actors: list[str]) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound via unnest.
        "(id, actor, queue, payload, max_attempts, retry_kind, status, metadata) "
        "SELECT t.id, t.actor, $2, '{}'::jsonb, 5, 'transient', "
        f"'pending'::\"{schema}\".job_status, '{{}}'::jsonb "
        "FROM unnest($1::uuid[], $3::text[]) AS t(id, actor)",
        [new_uuid() for _ in actors],
        _QUEUE,
        actors,
    )


class TestBoundedTickWithPolicies:
    """A5: the statement budget holds with both preflights in play."""

    async def test_fifty_due_schedules_with_policies_stay_bounded_and_complete(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """50 due schedules — 40 for 20 singleton actors (one fire + one
        intra-batch suppression each), 5 for capped actors at their cap
        (suppressed), 5 healthy (fire): the tick spends at most the
        reproduction budget (20) awaited statements, the two preflights
        are one statement each (per flagged-actor set, never per
        schedule), and every non-colliding plan fires."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        grid = ten_min_floor(datetime.now(UTC))
        # Offsets ≤30min against the 1h catch-up window, so no wall-clock
        # crossing between seeding and the tick can flip a within-window
        # miss into a beyond-window recompute.
        singleton_actors = [_singleton_actor(i) for i in range(_SINGLETON_ACTOR_COUNT)]
        capped_actors = [_capped_actor(i) for i in range(_CAPPED_ACTOR_COUNT)]
        healthy_actors = [_healthy_actor(i) for i in range(_HEALTHY_ACTOR_COUNT)]
        await _seed_actor_configs(
            clean_pg_conn, schema, singleton_actors + capped_actors + healthy_actors
        )
        await _seed_blocker_jobs(clean_pg_conn, schema, capped_actors)

        rows: list[tuple[str, str, datetime]] = []
        for actor in singleton_actors:
            rows.append((actor, f"{actor}-early", grid - timedelta(minutes=30)))
            rows.append((actor, f"{actor}-late", grid - timedelta(minutes=20)))
        for actor in capped_actors:
            rows.append((actor, f"{actor}-capped", grid - timedelta(minutes=10)))
        for actor in healthy_actors:
            rows.append((actor, f"{actor}-healthy", grid))
        await _seed_schedules(clean_pg_conn, schema, rows)

        policies: dict[str, ActorFirePolicy] = {}
        policies.update({a: ActorFirePolicy(singleton=True) for a in singleton_actors})
        policies.update({a: ActorFirePolicy(max_pending=1) for a in capped_actors})
        policies.update({a: ActorFirePolicy() for a in healthy_actors})

        counting = CountingConn(clean_pg_conn)
        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    counting,  # type: ignore[arg-type]  # Why: duck-typed connection wrapper; all awaited methods typed on it.
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    limit=60,
                    actor_policies=policies,
                )

        assert fired == 25, (
            f"{fired} fires from 50 due schedules — expected 20 singleton (one per "
            "actor, the intra-batch gate suppressing each actor's later slot) + "
            "0 capped + 5 healthy"
        )
        assert counting.count <= 20, (
            f"a tick over 50 due schedules with both policy preflights issued "
            f"{counting.count} awaited round trips (budget 20) — the preflight is "
            "per-schedule somewhere, so a catch-up burst trips the leader's "
            "deadline exactly when the bound is load-bearing"
        )
        # Needles chosen to sit inside the harness's 200-char statement
        # truncation: both preflight bodies are longer than that, so a
        # "GROUP BY actor" tail can be cut off.
        assert counting.matching("blocking_job_id") == 1, (
            "exactly one singleton-blocker preflight statement may run, over the "
            "whole flagged-actor set — never one per schedule"
        )
        assert counting.matching("pending_count") == 1, (
            "exactly one pending-count preflight statement may run, over the whole "
            "capped-actor set — never one per schedule"
        )

        total_jobs: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs'  # noqa: S608  # Why: schema is a test-fixture identifier.
        )
        assert total_jobs == 30, (
            f"{total_jobs} jobs — expected 20 singleton fires + 5 seeded cap "
            "blockers + 5 healthy fires, nothing more"
        )
        singleton_grouped = await clean_pg_conn.fetch(
            f'SELECT actor, count(*)::int AS n FROM "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier; the actor set is $-bound.
            "WHERE actor = ANY($1) GROUP BY actor",
            singleton_actors,
        )
        assert len(singleton_grouped) == _SINGLETON_ACTOR_COUNT
        assert all(rec["n"] == 1 for rec in singleton_grouped), (
            "every singleton actor keeps exactly one job — the earlier slot's fire"
        )

        fired_schedules: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "WHERE last_fired_at IS NOT NULL"
        )
        assert fired_schedules == 25
        # The suppressed slots (each singleton actor's later slot + every
        # capped slot) advanced by their own cadence from their own slot —
        # sequential catch-up, so the advance can still sit in the past;
        # the pinned property is the exact target, not future-ness.
        suppressed_rows = await clean_pg_conn.fetch(
            f'SELECT name, next_fire_at FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "WHERE last_fired_at IS NULL"
        )
        expected_advance = {
            name: slot + timedelta(minutes=10)
            for _, name, slot in rows
            if name.endswith("-late") or name.endswith("-capped")
        }
        assert len(suppressed_rows) == 25
        assert {r["name"] for r in suppressed_rows} == set(expected_advance), (
            "the suppressed set must be exactly the singleton actors' later slots "
            "plus the capped slots"
        )
        assert all(r["next_fire_at"] == expected_advance[r["name"]] for r in suppressed_rows), (
            "every suppressed slot advanced by its own cadence from its own slot"
        )
        struck: int = await clean_pg_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier.
            "WHERE consecutive_failures > 0"
        )
        assert struck == 0, "no suppression in the batch may strike its schedule"

        events = [e["event"] for e in captured]
        assert events.count("singleton-collision") == 20
        assert events.count("max-pending-exceeded") == 5


class TestGenuineFailureUnderPolicy:
    """A9: the classification absorbs collisions only — a real defect still
    strikes while the stamping path is active in the same tick."""

    async def test_failing_factory_strikes_while_a_stamped_fire_lands(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One singleton actor, two due schedules in one tick: the earlier
        slot's payload factory raises (a genuine defect), the later slot is
        healthy. The tick must strike the failing schedule (failure count
        1, error text recorded) while the healthy fire carries the
        singleton stamp — the stamping path active, the failure path
        unblunted."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        actor = "rt_budget_boom"
        await _seed_actor_configs(clean_pg_conn, schema, [actor])
        grid = ten_min_floor(datetime.now(UTC))
        failing_id = await seed_schedule_bounded(
            clean_pg_conn,
            schema,
            actor=actor,
            name="boom-slot",
            slot=grid - timedelta(minutes=30),
            payload_factory="tests.test_rt_cron_parity_budget.boom_factory",
        )
        healthy_id = await seed_schedule_bounded(
            clean_pg_conn,
            schema,
            actor=actor,
            name="healthy-slot",
            slot=grid - timedelta(minutes=20),
        )

        with structlog.testing.capture_logs() as captured:
            async with clean_pg_conn.transaction():
                fired = await tick_cron(
                    clean_pg_conn,
                    settings,
                    make_backend(settings),
                    schema,
                    new_uuid(),
                    actor_policies={actor: ActorFirePolicy(singleton=True)},
                )

        assert fired == 1, "the healthy slot fires; the failing slot strikes"
        assert await count_jobs(clean_pg_conn, schema, actor) == 1
        has_flag: bool = await clean_pg_conn.fetchval(
            f"SELECT metadata @> '{{\"singleton\": true}}'::jsonb "  # noqa: S608  # Why: schema is a test-fixture identifier; the only interpolation is the escaped jsonb literal.
            f'FROM "{schema}".jobs WHERE actor = $1',
            actor,
        )
        assert has_flag is True, "the healthy fire went through the stamping path"

        failing = await schedule_row(clean_pg_conn, schema, failing_id)
        assert failing["consecutive_failures"] == 1, (
            "a genuine payload-factory failure must still strike — the suppression "
            "classification may only absorb policy collisions"
        )
        assert "budget-check factory exploded" in (failing["last_fire_error"] or "")
        assert failing["enabled"] is True
        assert failing["last_fired_at"] is None
        healthy = await schedule_row(clean_pg_conn, schema, healthy_id)
        assert healthy["last_fired_at"] is not None
        assert healthy["consecutive_failures"] == 0
        failed = [e for e in captured if e["event"] == "cron fire failed"]
        assert len(failed) == 1
        assert "budget-check factory exploded" in failed[0]["error"]


async def seed_schedule_bounded(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    name: str,
    slot: datetime,
    payload_factory: str | None = None,
) -> UUID:
    """Seed one schedule for the budget file (name doubles as identity)."""
    schedule_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; every value is $-bound.
        "(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, "
        " enabled, next_fire_at, metadata, identity_key, consecutive_failures) "
        "VALUES ($1, $2, $3, $4, 'UTC', 'skip', $5, true, $6, '{}'::jsonb, $3, 0)",
        schedule_id,
        actor,
        name,
        _TEN_MINUTELY,
        payload_factory,
        slot,
    )
    return schedule_id
