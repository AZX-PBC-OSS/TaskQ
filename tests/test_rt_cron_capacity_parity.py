"""Red-team attacks on the cron tick's operator-stored ``max_pending`` parity (real PG).

The stored ``actor_config.max_pending`` value is operator-owned: once a row
exists, a non-NULL stored value is authoritative and the ``@actor`` literal
is only the seed (``taskq/client/_capacity.py``'s resolution rule,
``taskq/worker/startup.py``'s sync semantics, and ``docs/guides/ops.md``'s
"NULL stored falls back to the code literal"). Every client enqueue arm
enforces the stored cap - the single path through
:class:`ActorCapacityCache`, the batch arms through both the cache and the
server-side ``list_actor_max_pending`` read - pinned by
``tests/test_actor_capacity_pg.py`` (live change without restart, an actor
with no literal capped live, cleared override reverting, two clients
enforcing one stored limit).

``docs/guides/cron.md`` promises fires honor "the same ``max_pending``
cap" as client enqueues. But the tick's policy map is built from the
in-process registry literals (``worker/_bootstrap.py`` →
``ActorFirePolicy(singleton=ref.singleton, max_pending=ref.max_pending)``)
- the one admission surface that never consults the stored row. An actor
declared without a literal (the default) carries ``max_pending=None`` into
the tick no matter what the operator stores, and the batched enqueue runs
with ``enforce_max_pending=False`` by design (the preflight owns the cap),
so nothing downstream catches it either.

The production shape under attack: a literal-``None`` actor whose operator
stored a cap - including ``max_pending=0``, the documented emergency-drain
configuration ("never accept any jobs", ``taskq/actor.py``).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.actor_config_ops import set_actor_config_capacity
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_rt_cron_harness import (
    _TEN_MINUTELY,
    count_jobs,
    cron_settings,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
    server_ten_min_floor,
)

pytestmark = pytest.mark.integration

_LITERAL_CAPPED_ACTOR = "rt_cron_cap_literal"
_STORED_CAPPED_ACTOR = "rt_cron_cap_stored"
_STORED_ZERO_ACTOR = "rt_cron_cap_drain"


async def _seed_three_due_schedules(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    *,
    prefix: str,
    grid: datetime,
) -> tuple[UUID, UUID, UUID]:
    """Three past-due ten-minutely slots for *actor*, earliest first.

    The tick reads due rows ``ORDER BY next_fire_at``, so the staggered
    slots make the admission order deterministic: early, mid, late. The
    grid comes from the caller so the seed values and the assertions that
    compare against them share ONE server-clock floor: recomputing a
    floor at assert time re-races the 10-minute boundary (the harness's
    own ``server_ten_min_floor`` docstring, the CI red of 2026-09-21).
    """
    ids: list[UUID] = []
    for label, minutes in (("early", 30), ("mid", 20), ("late", 10)):
        ids.append(
            await seed_schedule(
                conn,
                schema,
                actor=actor,
                name=f"{prefix}-{label}",
                cron_expr=_TEN_MINUTELY,
                next_fire_at=grid - timedelta(minutes=minutes),
                identity_key=f"{prefix}-{label}",
            )
        )
    return ids[0], ids[1], ids[2]


class TestStoredCapMatchesLiteralCap:
    """The same cap spelled two ways bounds one tick identically."""

    async def test_a_stored_cap_bounds_cron_fires_like_the_same_literal_cap(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Two actors, three due schedules each, no pre-existing jobs: one
        capped by its registered literal (``ActorFirePolicy(max_pending=2)``
        - exactly the map bootstrap builds for a ``@actor(max_pending=2)``
        declaration), the other declared without a literal (the bootstrap
        map for the default: ``max_pending=None``) with the operator's
        stored ``actor_config.max_pending = 2`` written through the real
        operator tooling. The client arms enforce both caps identically
        (``tests/test_actor_capacity_pg.py``); the tick must too - same
        schema, same rows, same cap value, two spellings, one answer."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _LITERAL_CAPPED_ACTOR)
        await seed_actor_config(clean_pg_conn, schema, _STORED_CAPPED_ACTOR)
        await set_actor_config_capacity(
            clean_pg_conn, _STORED_CAPPED_ACTOR, max_pending=2, schema=schema
        )
        grid = await server_ten_min_floor(clean_pg_conn)
        await _seed_three_due_schedules(
            clean_pg_conn, schema, _LITERAL_CAPPED_ACTOR, prefix="lit", grid=grid
        )
        stored_ids = await _seed_three_due_schedules(
            clean_pg_conn, schema, _STORED_CAPPED_ACTOR, prefix="sto", grid=grid
        )

        async with clean_pg_conn.transaction():
            await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies={
                    _LITERAL_CAPPED_ACTOR: ActorFirePolicy(max_pending=2),
                    _STORED_CAPPED_ACTOR: ActorFirePolicy(singleton=False, max_pending=None),
                },
            )

        literal_count = await count_jobs(clean_pg_conn, schema, _LITERAL_CAPPED_ACTOR)
        stored_count = await count_jobs(clean_pg_conn, schema, _STORED_CAPPED_ACTOR)
        assert literal_count == 2, "control: the literal cap bounds its actor to two"
        assert stored_count == literal_count, (
            "the operator's stored cap must bound the tick exactly like the same "
            "literal cap - a stored 2 admitting three while a literal 2 admits two "
            "is the two paths answering the same admission question differently"
        )
        late_stored = await schedule_row(clean_pg_conn, schema, stored_ids[2])
        assert late_stored["last_fired_at"] is None, (
            "the slot past the stored cap suppresses - it does not fire"
        )
        assert late_stored["consecutive_failures"] == 0, (
            "suppression is backpressure, not a schedule defect - no strike"
        )


class TestStoredZeroCapEmergencyDrain:
    """``max_pending=0`` is the documented drain tool; cron must honor it."""

    async def test_a_stored_zero_cap_admits_no_cron_fires(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """An operator draining an actor stores ``max_pending=0`` ("never
        accept any jobs", the emergency-drain scenario ``taskq/actor.py``
        documents). Every client enqueue is refused by the stored cap; if
        the cron tick keeps firing the actor's schedules, the drain does
        not drain - the queue keeps growing from cron while every client
        producer is refused, and the operator's knob silently no-ops on an
        entire admission surface."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _STORED_ZERO_ACTOR)
        await set_actor_config_capacity(
            clean_pg_conn, _STORED_ZERO_ACTOR, max_pending=0, schema=schema
        )
        grid = await server_ten_min_floor(clean_pg_conn)
        early_id, mid_id, late_id = await _seed_three_due_schedules(
            clean_pg_conn, schema, _STORED_ZERO_ACTOR, prefix="drain", grid=grid
        )

        async with clean_pg_conn.transaction():
            fired = await tick_cron(
                clean_pg_conn,
                settings,
                make_backend(settings),
                schema,
                new_uuid(),
                actor_policies={
                    _STORED_ZERO_ACTOR: ActorFirePolicy(singleton=False, max_pending=None)
                },
            )

        assert fired == 0, "a stored zero cap admits nothing"
        assert await count_jobs(clean_pg_conn, schema, _STORED_ZERO_ACTOR) == 0
        for schedule_id in (early_id, mid_id, late_id):
            row = await schedule_row(clean_pg_conn, schema, schedule_id)
            assert row["last_fired_at"] is None
            assert row["consecutive_failures"] == 0, "drain backpressure strikes nothing"
            # The bound is the slot's OWN seed value on the tick's server
            # clock (the early slot sits exactly at grid - 30m), captured
            # once before the seed: the suppression advance moves the slot
            # one cadence up from there, so a hot re-fire loop - which
            # leaves the slot un-advanced - fails the strict inequality.
            # Recomputing a floor at assert time instead re-races the
            # 10-minute grid boundary: the strict > lands on exact
            # equality whenever the test straddles the boundary under
            # co-tenant load, and the CI red of 2026-09-21 is the same
            # shape (the harness's server_ten_min_floor docstring).
            assert row["next_fire_at"] > grid - timedelta(minutes=30), (
                "suppression advances the slot - no hot re-fire loop against the cap"
            )
