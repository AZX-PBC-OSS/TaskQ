"""Red-team scratch: is the actor-cohort rotation bound PROVABLE (exactly
ceil(actor_count / limit) rounds) or merely probabilistic?

The rotation tiebreak is ``actor_claimed_at ASC NULLS FIRST`` stamped once
per round for every admitted actor with the SAME statement_timestamp().
If the stamp is a true round-robin (each round admits a disjoint,
previously-least-recently-served cohort), the bound should be TIGHT: with
``actor_count`` actors and a round ``limit``, every actor should be served
within ceil(actor_count / limit) rounds -- not just "eventually" within
some generous round budget.

This is a temporary, unticketed scratch test, not part of the permanent
suite -- it is here to characterise the shape of the bound, not to pin
behaviour long-term.
"""

# ruff: noqa: S608

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_ROUND_ROBIN_SQL, DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

from .test_rt_cron_harness import cron_settings, make_backend, seed_actor_config

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "tight_bound_q"
_LIMIT = 5
_ACTOR_COUNT = 60
_DEPTH_PER_ACTOR = 10


async def _dispatch(
    conn: asyncpg.Connection, schema: str, sql_template: str, queues: list[str], limit_n: int
) -> list[asyncpg.Record]:
    return await dispatch_batch_sql(
        conn,
        sql=sql_template.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
    )


async def _seed(
    backend: PostgresBackend,
    conn: asyncpg.Connection,
    schema: str,
    actors: list[str],
    queue: str,
    depth: int,
    due: datetime,
) -> None:
    for name in actors:
        await seed_actor_config(conn, schema, name, queue=queue)
    args_list = [
        EnqueueArgs(
            id=new_job_id(),
            actor=name,
            queue=queue,
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=due,
        )
        for name in actors
        for _ in range(depth)
    ]
    await backend.enqueue_batch(args_list, connection=conn)


async def _first_round_served(
    conn: asyncpg.Connection,
    schema: str,
    sql_template: str,
    queues: list[str],
    limit_n: int,
    rounds: int,
) -> dict[str, int]:
    """Map actor -> the 1-indexed round it was FIRST served in."""
    first_served: dict[str, int] = {}
    for round_no in range(1, rounds + 1):
        rows = await _dispatch(conn, schema, sql_template, queues, limit_n)
        if not rows:
            break
        ids = [row["id"] for row in rows]
        for row in rows:
            first_served.setdefault(row["actor"], round_no)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', finished_at = clock_timestamp() "
            "WHERE id = ANY($1::uuid[])",
            ids,
        )
    return first_served


class TestTightRotationBound:
    """60 uncapped actors, round limit 5: the provable bound is that every
    actor is served within ceil(60/5) = 12 rounds if rotation is a true
    round-robin partition. We give generous slack (2x) and report the
    actual max first-served round either way -- if it passes only because
    of the slack, that is itself a finding."""

    async def test_strict_fifo_tight_bound(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        actors = [f"tb_fifo_{i:03d}" for i in range(_ACTOR_COUNT)]
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _seed(backend, clean_pg_conn, schema, actors, _QUEUE, _DEPTH_PER_ACTOR, due)

        tight_bound = math.ceil(_ACTOR_COUNT / _LIMIT)
        slack_rounds = tight_bound * 2
        first_served = await _first_round_served(
            clean_pg_conn, schema, DISPATCH_STRICT_FIFO_SQL, [_QUEUE], _LIMIT, slack_rounds
        )

        never = sorted(a for a in actors if a not in first_served)
        max_round = max(first_served.values()) if first_served else None
        print(
            f"\n[strict_fifo] tight_bound={tight_bound} max_first_served_round={max_round} "
            f"never_served={len(never)} rounds_used={slack_rounds}"
        )

        assert not never, (
            f"{len(never)} actors never served within {slack_rounds} rounds (2x tight bound {tight_bound}): {never}"
        )
        assert max_round is not None
        assert max_round <= tight_bound, (
            f"rotation bound is NOT tight: max first-served round was {max_round}, "
            f"exceeding the provable ceil(actor_count/limit)={tight_bound}. "
            f"Every actor was eventually served, but only with {slack_rounds - tight_bound} "
            f"rounds of slack beyond what a true round-robin partition would need -- "
            f"the bound is probabilistic/self-correcting, not structurally provable."
        )

    async def test_round_robin_mode_tight_bound(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        actors = [f"tb_rr_{i:03d}" for i in range(_ACTOR_COUNT)]
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _seed(backend, clean_pg_conn, schema, actors, _QUEUE, _DEPTH_PER_ACTOR, due)
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
            "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode",
            _QUEUE,
            "round_robin",
        )

        tight_bound = math.ceil(_ACTOR_COUNT / _LIMIT)
        slack_rounds = tight_bound * 2
        first_served = await _first_round_served(
            clean_pg_conn, schema, DISPATCH_ROUND_ROBIN_SQL, [_QUEUE], _LIMIT, slack_rounds
        )

        never = sorted(a for a in actors if a not in first_served)
        max_round = max(first_served.values()) if first_served else None
        print(
            f"\n[round_robin] tight_bound={tight_bound} max_first_served_round={max_round} "
            f"never_served={len(never)} rounds_used={slack_rounds}"
        )

        assert not never, (
            f"{len(never)} actors never served within {slack_rounds} rounds (2x tight bound {tight_bound}): {never}"
        )
        assert max_round is not None
        assert max_round <= tight_bound, (
            f"rotation bound is NOT tight: max first-served round was {max_round}, "
            f"exceeding the provable ceil(actor_count/limit)={tight_bound}."
        )
