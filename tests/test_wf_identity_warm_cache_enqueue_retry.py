"""``_with_fresh_connection_retry``'s migration branch, on the real pool.

``tests/test_connections.py`` pins the recovery branch with a fake pool: one
re-drive, the distinct warning event, the acknowledged-write refusal. That
proves the wrapper's own logic, not that the branch is REACHABLE — a branch no
production statement can raise into protects nothing.

The reachable shape is the batch enqueue's own transaction. Its ``RETURNING
*`` exports ``map_index``, so a statement parsed while the column was
``smallint`` has a cached result type the POST widening changes; inside a
transaction asyncpg cannot repair that itself (it re-raises
``InvalidCachedStatementError``), and the wrapper's single bounded re-drive is
the only recovery. The single-enqueue arm cannot reach it (one autocommit
statement, which asyncpg retries and clears the cache for on its own), and the
COPY arm exports no row — so this is the arm to pin.

No copied SQL: the probe drives the shipped batch-enqueue path on a pool whose
cache is genuinely warm against the pre-POST schema, then applies the real POST
and enqueues again.
"""

# ruff: noqa: S608  # Why: the f-strings interpolate only throwaway schema identifiers; every value is $-bound.

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._enqueue import (  # pyright: ignore[reportPrivateUsage]  # Why: the shipped batch path is the unit under test; a copy would not raise the cached-plan error the wrapper exists for.
    _enqueue_batch,
)
from taskq.backend._protocol import EnqueueArgs
from taskq.backend._sql_templates import render as render_templates
from taskq.migrate import apply_pending

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _args() -> EnqueueArgs:
    return EnqueueArgs(
        id=new_uuid(),
        actor="warm-retry",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=None,
    )


async def test_warm_batch_enqueue_re_drives_once_across_the_post(pg_dsn: str) -> None:
    schema = "warm_enq_" + new_base62(8, precision="random").lower()
    control = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(
        pg_dsn,
        min_size=1,
        max_size=1,
        statement_cache_size=512,
        max_cached_statement_lifetime=3600.0,
    )  # pyright: ignore[reportCallIssue]  # Why: asyncpg accepts the shipped cache options at runtime.
    try:
        await apply_pending(control, schema=schema, phase="pre")
        sql = render_templates(schema)
        assert len(await _enqueue_batch(pool, sql, schema, [_args()])) == 1

        await apply_pending(control, schema=schema, phase="post")

        # The warm statement's result type changed under it; the wrapper's
        # one re-drive is what turns the driver's refusal into the admitted row.
        assert len(await _enqueue_batch(pool, sql, schema, [_args()])) == 1
        assert (
            await control.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1', "warm-retry"
            )
            == 2
        )
    finally:
        await pool.close()
        await control.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await control.close()
