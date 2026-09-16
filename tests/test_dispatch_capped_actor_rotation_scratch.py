"""Red-team scratch: does the rotation stamp also protect CAPPED actors
(max_concurrent set)?

The dispatch CTE splits capped and uncapped actors into two different
pre-lock cut shapes (``top_ids`` -> ``locked`` for capped, ``sliding_locked``
for uncapped -- see _dispatch_sql.py's module docstring). Both paths read
``actor_claimed_at`` in their ORDER BY, but they are genuinely different
SQL shapes, so it's worth independently confirming the capped path rotates
too rather than assuming parity from the uncapped test.

Temporary, unticketed scratch test.
"""

# ruff: noqa: S608

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

from .test_rt_cron_harness import cron_settings, make_backend, seed_actor_config

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_QUEUE = "capped_rotation_q"
_LIMIT = 5
_ACTOR_COUNT = 30
_DEPTH_PER_ACTOR = 10


async def _dispatch(
    conn: asyncpg.Connection, schema: str, sql_template: str, queues: list[str], limit_n: int
) -> list[asyncpg.Record]:
    return await dispatch_batch_sql(
        conn, sql=sql_template.format(schema=schema), queues=queues, limit_n=limit_n,
        worker_id=new_uuid(), lock_lease=_LEASE,
    )


class TestCappedActorRotation:
    async def test_capped_actors_rotate_across_rounds(
        self, clean_pg_conn: asyncpg.Connection, module_pg_schema: ModulePgSchema,
    ) -> None:
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        backend = make_backend(settings)
        actors = [f"cap_rot_{i:03d}" for i in range(_ACTOR_COUNT)]
        due = datetime.now(UTC) - timedelta(seconds=60)

        # Every actor capped at max_concurrent=1: each round can admit at
        # most 1 job per actor regardless of residual math, forcing the
        # capped path (top_ids -> locked) exclusively.
        for name in actors:
            await seed_actor_config(clean_pg_conn, schema, name, queue=_QUEUE)
            await clean_pg_conn.execute(
                f'UPDATE "{schema}".actor_config SET max_concurrent = 1 WHERE actor = $1',
                name,
            )
        args_list = [
            EnqueueArgs(
                id=new_job_id(), actor=name, queue=_QUEUE, payload={},
                max_attempts=3, retry_kind="transient", scheduled_at=due,
            )
            for name in actors
            for _ in range(_DEPTH_PER_ACTOR)
        ]
        await backend.enqueue_batch(args_list, connection=clean_pg_conn)

        tight_bound = math.ceil(_ACTOR_COUNT / _LIMIT)
        rounds_budget = tight_bound * 2
        first_served: dict[str, int] = {}
        for round_no in range(1, rounds_budget + 1):
            rows = await _dispatch(clean_pg_conn, schema, DISPATCH_STRICT_FIFO_SQL, [_QUEUE], _LIMIT)
            if not rows:
                break
            ids = [row["id"] for row in rows]
            for row in rows:
                first_served.setdefault(row["actor"], round_no)
            await clean_pg_conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', finished_at = clock_timestamp() "
                "WHERE id = ANY($1::uuid[])",
                ids,
            )

        never = sorted(a for a in actors if a not in first_served)
        max_round = max(first_served.values()) if first_served else None
        print(f"\n[capped max_concurrent=1] tight_bound={tight_bound} "
              f"max_first_served_round={max_round} never_served={len(never)} "
              f"rounds_used={rounds_budget}")

        assert not never, (
            f"{len(never)} capped actors never served within {rounds_budget} rounds: {never}. "
            f"This would mean the capped path (top_ids -> locked) does not share the "
            f"uncapped path's rotation protection."
        )
        assert max_round is not None
        assert max_round <= tight_bound, (
            f"capped-actor rotation bound not tight: max first-served round {max_round} "
            f"exceeds ceil(actor_count/limit)={tight_bound}."
        )
