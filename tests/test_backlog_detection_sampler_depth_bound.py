"""The backlog-detection sampler's row work must not grow with pending depth.

``_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE`` (``src/taskq/worker/_leader_sweeps.py``)
backs ``_backlog_detection_loop``, which architecture.md's leader-loop
inventory (item 10) documents as deliberately **not** leader-gated: "every
worker samples instead, accepting N times the query cost" (see the loop's
own docstring at the same location) so the detector still emits under the
very failure - election loss, a stuck advisory lock - it exists to expose.
That tradeoff is sound only if one worker's sample is cheap, so the
sampler is depth-bounded by construction: a recursive loose index scan
enumerates the distinct pending (actor, queue) pairs (one bounded seek per
pair, never one per row - the same geometry as the dispatch CTE's keys
walks), and a per-pair probe reads at most ``_ACTOR_BACKLOG_SAMPLE_CAP``
rows in the index's own dispatch-head order, so the tick's row work is
Σ min(depth_pair, cap) + #pairs - flat as the backlog grows. The shipped
series semantics under the cap (depth exact below the cap, oldest_age the
head-of-line age) are documented on the template and in
docs/guides/ops.md.

Oracle: :func:`taskq.testing.pg.install_row_visit_counter` /
:class:`~taskq.testing.pg.RowVisitCounter`, the same row-level-security
sequence-bump oracle ``test_dispatch_backlog_depth_bound.py`` and
``test_rt_cancel_drain_keyset_cost.py`` use - it counts rows the engine
actually reads, not EXPLAIN text, so it is exact and cannot flake on plan
shape or PG version.

History: this pin was written RED against the original shape - a plain
``GROUP BY actor, queue`` over the whole pending set, which visited ~10x
more rows at a 100k pending backlog than at 10k (measured 2026-09-15,
local Postgres 18, `jit=off`: ~1.6ms p50 at 10k growing to ~13-15ms p50
at 100k for the identical query shape) and paid that per worker per
``TASKQ_QUEUE_DEPTH_INTERVAL``. The bounded rewrite (the pairs walk plus
the capped per-pair probe) is the acceptance fix this test turned green
for; the assertion below is the same bound the dispatch depth oracle
holds the claim path to. The SQL under test is the production template
itself, rendered, never a copy - a paraphrase would let the shipped
query regress while the pin stayed green.
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
from taskq.testing.pg import RowVisitCounter, install_row_visit_counter
from taskq.worker._leader_sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: pin the production sampler statement, not a copy - a paraphrase would let the shipped query regress while the pin stayed green.
    _QUERY_ACTOR_BACKLOG_SQL_TEMPLATE,
)

pytestmark = pytest.mark.integration

_SHALLOW_DEPTH = 10_000
_DEEP_DEPTH = 100_000
_ACTOR = "backlog_probe"
_QUEUE = "backlog_q"

# The bound: the same _DEPTH_RATIO_BOUND the dispatch depth oracle holds
# the claim path to (tests/test_dispatch_backlog_depth_bound.py). A
# bounded sampler's row work is driven by the pair count and the per-pair
# cap, both identical at the two seeded depths, so the visits ratio stays
# near 1 where the unbounded shape's tracked the depth ratio (10x).
_MAX_VISITS_DEPTH_RATIO = 3.0


def _actor_backlog_sql(schema: str) -> str:
    # The production template, rendered - not a copy. The query this loop
    # guards is the query the worker actually runs; a pinned paraphrase
    # would let the shipped sampler regress without this test noticing.
    return _QUERY_ACTOR_BACKLOG_SQL_TEMPLATE.format(schema=schema)


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
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',  # noqa: S608  # Why: schema is this module's own generated identifier (validated against _IDENT_RE above); values are $-bound.
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
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is this module's own generated identifier (validated against _IDENT_RE in the fixture); values are $-bound.
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


async def test_backlog_sampler_row_work_is_depth_bounded(pg_dsn: str, backlog_schema: str) -> None:
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
     sampler is held to the same bar.
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

        # The bound a depth-independent sampler must satisfy -- the same
        # discipline test_dispatch_backlog_depth_bound.py holds the
        # dispatch CTE to (_DEPTH_RATIO_BOUND = 3, there). The bounded
        # shape (pairs walk + per-pair cap) reads the same rows at both
        # seeded depths; a regression to reading the pending set lands the
        # ratio back near depth_ratio (10x).
        assert ratio < _MAX_VISITS_DEPTH_RATIO, (
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
