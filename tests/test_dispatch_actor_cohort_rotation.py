"""Real-Postgres pin: actors beyond a dispatch round's limit must not starve.

``top_ids`` cuts a round's admitted id set on ``ORDER BY pending_rank,
priority DESC, scheduled_at, id`` (``src/taskq/backend/_dispatch_sql.py``).
``pending_rank`` is ``ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ...)``,
so every actor's oldest pending job carries ``pending_rank = 1`` regardless
of how many actors are competing. Once the fleet holds more actors with
pending work than the round's ``limit``, the tiebreak among all those
rank-1 rows falls through to ``priority DESC, scheduled_at, id`` -- a
stable total order that is identical every round, because each round's
winners immediately refill their own rank-1 slot from their own backlog.
The excluded actors' rank-1 rows never move up in that stable order, so
they never win a slot, on any later round, in strict-FIFO or round-robin
mode alike.

Mirrors the InMemoryBackend pin (``tests/test_dispatch_starvation.py::
test_every_actor_with_backlog_is_eventually_claimed``) against the real
dispatch CTE: multiple sequential rounds on one connection (deterministic,
no inter-dispatcher race), completing each round's claims so a starved
actor's absence can only be explained by selection, never by the fleet
falling behind.
"""

# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; values are $-bound.

from __future__ import annotations

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
_QUEUE = "rotation_q"
_LIMIT = 5
_ACTOR_COUNT = 20
_DEPTH_PER_ACTOR = 30
_ROUNDS = 20


async def _dispatch(
    conn: asyncpg.Connection,
    schema: str,
    sql_template: str,
    queues: list[str],
    limit_n: int,
) -> list[asyncpg.Record]:
    return await dispatch_batch_sql(
        conn,
        sql=sql_template.format(schema=schema),
        queues=queues,
        limit_n=limit_n,
        worker_id=new_uuid(),
        lock_lease=_LEASE,
    )


async def _seed_uncapped_backlog(
    backend: PostgresBackend,
    conn: asyncpg.Connection,
    schema: str,
    actors: list[str],
    queue: str,
    depth: int,
    due: datetime,
) -> None:
    """Register each actor uncapped with *depth* due, equal-priority jobs.

    No actor's own configuration bounds what it may run, so any actor a
    round leaves out was excluded by selection, not by a capacity rule.
    """
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


async def _drain_tally(
    conn: asyncpg.Connection,
    schema: str,
    sql_template: str,
    queues: list[str],
    limit_n: int,
    rounds: int,
) -> dict[str, int]:
    """Run *rounds* rounds on one connection, completing every claim so a
    fleet that keeps up never masks a selection failure as a backpressure
    one.

    Completion is a direct terminal UPDATE on the same connection (not the
    pooled ``mark_succeeded`` helper) -- the harness's stub backend deps
    carry no pool, and a plain status flip is all a round needs to free the
    row for the next round's selection; the dispatch CTE itself is exercised
    unmodified through the real SQL helper.
    """
    tally: dict[str, int] = {}
    for _ in range(rounds):
        rows = await _dispatch(conn, schema, sql_template, queues, limit_n)
        if not rows:
            break
        ids = [row["id"] for row in rows]
        for row in rows:
            tally[row["actor"]] = tally.get(row["actor"], 0) + 1
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', finished_at = clock_timestamp() "
            "WHERE id = ANY($1::uuid[])",
            ids,
        )
    return tally


class TestActorCohortRotation:
    """More uncapped actors hold pending work than a round's limit admits;
    every actor must eventually be claimed across enough rounds, in both
    dispatch modes."""

    async def test_strict_fifo_rotates_across_rounds(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        actors = [f"rot_fifo_{i:02d}" for i in range(_ACTOR_COUNT)]
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _seed_uncapped_backlog(
            backend, clean_pg_conn, schema, actors, _QUEUE, _DEPTH_PER_ACTOR, due
        )

        tally = await _drain_tally(
            clean_pg_conn,
            schema,
            DISPATCH_STRICT_FIFO_SQL,
            [_QUEUE],
            _LIMIT,
            _ROUNDS,
        )

        starved = sorted(name for name in actors if tally.get(name, 0) == 0)
        assert not starved, (
            f"{len(starved)} of {len(actors)} actors were never claimed across "
            f"{_ROUNDS} strict-FIFO rounds at limit {_LIMIT}, though every one held "
            f"pending jobs the whole time: {starved}. Claims went to "
            f"{sorted(k for k, v in tally.items() if v)}. A stable total order over "
            f"every actor's rank-1 job would produce exactly this: the same prefix of "
            f"{_LIMIT} actors wins every round, forever."
        )

    async def test_round_robin_rotates_across_rounds(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Round-robin mode is the sharpest instance: it is the mode chosen
        specifically to prevent this starvation, so it exhibiting the same
        defect confirms the stable cut sits upstream of ``fairness_rank``."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        actors = [f"rot_rr_{i:02d}" for i in range(_ACTOR_COUNT)]
        due = datetime.now(UTC) - timedelta(seconds=60)
        await _seed_uncapped_backlog(
            backend, clean_pg_conn, schema, actors, _QUEUE, _DEPTH_PER_ACTOR, due
        )
        await clean_pg_conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
            "ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode",
            _QUEUE,
            "round_robin",
        )

        tally = await _drain_tally(
            clean_pg_conn,
            schema,
            DISPATCH_ROUND_ROBIN_SQL,
            [_QUEUE],
            _LIMIT,
            _ROUNDS,
        )

        starved = sorted(name for name in actors if tally.get(name, 0) == 0)
        assert not starved, (
            f"{len(starved)} of {len(actors)} actors were never claimed across "
            f"{_ROUNDS} round-robin rounds at limit {_LIMIT}, though every one held "
            f"pending jobs the whole time: {starved}. Claims went to "
            f"{sorted(k for k, v in tally.items() if v)}. Round-robin mode exists "
            f"specifically to prevent this; a defect upstream of fairness_rank "
            f"reproduces it here too."
        )
