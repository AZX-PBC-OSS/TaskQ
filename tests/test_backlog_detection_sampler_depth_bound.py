"""The backlog-detection sampler's row work must not grow with pending depth.

``_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE`` (``src/taskq/worker/_leader_sweeps.py``)
backs ``_backlog_detection_loop``, which architecture.md's leader-loop
inventory (item 10) documents as deliberately **not** leader-gated: "every
worker samples instead, accepting N times the query cost" (see the loop's
own docstring at the same location) so the detector still emits under the
very failure — election loss, a stuck advisory lock — it exists to expose.
That tradeoff is sound only if one worker's sample is cheap; the query is

    SELECT actor, queue, count(*) AS depth,
           EXTRACT(EPOCH FROM (clock_timestamp() - MIN(scheduled_at))) AS oldest_age
    FROM jobs WHERE status = 'pending' GROUP BY actor, queue

which has no LIMIT and no per-actor bound: every pending row must be
visited to produce the aggregate, by construction. Unlike the dispatch CTE
(tests/test_dispatch_backlog_depth_bound.py), which spends real design
effort — the candidates LATERAL, the oversample window, the materialized
``ranked`` fence — keeping its per-round row work independent of backlog
depth, this sampler was written as a plain aggregate with none of that
machinery, and nothing in configuration.md or maintenance-sweeps.md
documents it as an O(depth) cost centre alongside the (leader-only, batch-
bounded) sweeps that guide takes pains to explain.

Oracle: :func:`taskq.testing.pg.install_row_visit_counter` /
:class:`~taskq.testing.pg.RowVisitCounter`, the same row-level-security
sequence-bump oracle ``test_dispatch_backlog_depth_bound.py`` and
``test_rt_cancel_drain_keyset_cost.py`` use — it counts rows the engine
actually reads, not EXPLAIN text, so it is exact and cannot flake on plan
shape or PG version.

This is left RED deliberately. Measured on this branch (adopter-brief
probe, 2026-09-15, local Postgres 18, `jit=off`): the query visits ~10x
more rows at a 100k-row pending backlog than at 10k (linear in depth, as
the assertion below predicts), and wall time went from ~1.6ms p50 to
~13-15ms p50 for the identical query shape. At a 10-worker fleet sampling
every ``TASKQ_QUEUE_DEPTH_INTERVAL`` (default 15s, *every* worker, not
leader-gated), that is aggregate continuous scan cost that grows with the
adopter's backlog with no cap and no documented mitigation -- the
opposite of the dispatch path's bounded-cost contract this repository
otherwise holds every hot-table query to.

No production fix is proposed here (out of scope for this report); this
pins the current unbounded shape so a future bound (an approximate/cached
counter, a materialized rollup refreshed on a slower cadence, or gating
the per-actor breakdown behind a coarser status-only count that a partial
index can serve without a GROUP BY) has a red test turning green as its
acceptance check. See docs/guides/ops.md and
docs/guides/maintenance-sweeps.md for the sibling sweeps' documented
batch/leader-gating discipline, absent here.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it.
)
from taskq.testing.pg import RowVisitCounter, install_row_visit_counter, read_row_visits

pytestmark = pytest.mark.integration

_SHALLOW_DEPTH = 10_000
_DEEP_DEPTH = 100_000
_ACTOR = "backlog_probe"
_QUEUE = "backlog_q"

# The query has no LIMIT: every pending row must be visited to produce the
# aggregate, so a truly bounded implementation is not possible for *this*
# query shape -- the assertion below documents that the ratio tracks depth
# almost exactly (bounded machinery, where it exists elsewhere in this
# repository, caps the ratio at a small constant regardless of depth; see
# _DEPTH_RATIO_BOUND in test_dispatch_backlog_depth_bound.py, which is 3).
# A ratio below this would mean some bound already exists and this test
# should be tightened, not loosened.
_MIN_EXPECTED_DEPTH_RATIO = 5.0


def _actor_backlog_sql(schema: str) -> str:
    return (
        "SELECT actor, queue, count(*) AS depth, "
        "EXTRACT(EPOCH FROM (clock_timestamp() - MIN(scheduled_at)))::float8 AS oldest_age "
        f'FROM "{schema}".jobs '
        "WHERE status = 'pending' "
        "GROUP BY actor, queue"
    )


@pytest.fixture(scope="module")
async def backlog_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, the row-visit-counter policy
    installed on ``jobs``, one registered actor."""
    schema = f"backlog_depth_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            _ACTOR,
            _QUEUE,
        )
        await install_row_visit_counter(conn, schema, table="jobs")
        yield schema
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def _seed_pending_backlog(conn: asyncpg.Connection, schema: str, depth: int) -> None:
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'pending', "
        "0, clock_timestamp() - interval '1 minute', 3, 'transient' "
        "FROM generate_series(1, $3::int) AS g",
        _ACTOR,
        _QUEUE,
        depth,
    )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


async def test_backlog_sampler_row_work_is_depth_bounded(
    pg_dsn: str, backlog_schema: str
) -> None:
    """The actor-backlog sampler's row visits at a 100k pending depth must
    not scale linearly with the 10k depth's visits.

    Every worker in a fleet runs this query on its own clock
    (``TASKQ_QUEUE_DEPTH_INTERVAL``, default 15s), deliberately without
    leader gating (architecture.md, leader-loop item 10). A query whose
    cost is proportional to the adopter's own pending backlog turns "add
    workers" and "the backlog grows" into "the fleet's continuous
    Postgres scan load grows with them" -- exactly the failure mode
    dispatch's own bounded-cost contract (test_dispatch_backlog_depth_bound.py)
    was built to prevent for the hot dispatch path. This asserts the
    sampler is held to the same bar and is expected to FAIL until it is.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        # Reset the counter sequence for a clean read on this connection's
        # role (RLS only fires for the counting role; see
        # install_row_visit_counter's docstring).
        counter = RowVisitCounter(_SinglePool(conn), backlog_schema, method="fetch")

        await _seed_pending_backlog(conn, backlog_schema, _SHALLOW_DEPTH)
        sql = _actor_backlog_sql(backlog_schema)
        async with counter.acquire() as counted_conn:
            await counted_conn.fetch(sql)
        shallow_visits = counter.per_statement[-1]

        await _seed_pending_backlog(conn, backlog_schema, _DEEP_DEPTH)
        async with counter.acquire() as counted_conn:
            await counted_conn.fetch(sql)
        deep_visits = counter.per_statement[-1]

        assert shallow_visits > 0, "the counting policy did not fire -- setup is broken"
        ratio = deep_visits / shallow_visits
        depth_ratio = _DEEP_DEPTH / _SHALLOW_DEPTH

        # This is the bound a depth-independent sampler would need to
        # satisfy -- the same discipline test_dispatch_backlog_depth_bound.py
        # holds the dispatch CTE to (_DEPTH_RATIO_BOUND = 3, there). Left
        # deliberately strict and RED: the query as shipped has no bound
        # at all, so `ratio` lands close to `depth_ratio` (10x), not below
        # a small constant.
        assert ratio < 3.0, (
            f"actor-backlog sampler visited {shallow_visits} rows at "
            f"{_SHALLOW_DEPTH} pending and {deep_visits} rows at "
            f"{_DEEP_DEPTH} pending (ratio {ratio:.1f}x for a "
            f"{depth_ratio:.0f}x depth increase) -- row work scales with "
            f"the pending backlog, not with a bounded batch. This runs on "
            f"every worker, every TASKQ_QUEUE_DEPTH_INTERVAL, unconditionally."
        )
    finally:
        await conn.close()


class _SinglePool:
    """Adapts one asyncpg.Connection to the ``pool.acquire()`` shape
    RowVisitCounter expects, for a test that only ever needs one
    connection at a time."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self, **_kwargs: object) -> Any:
        conn = self._conn

        class _Ctx:
            async def __aenter__(self) -> asyncpg.Connection:
                return conn

            async def __aexit__(self, *exc: object) -> Any:
                return None

        return _Ctx()
