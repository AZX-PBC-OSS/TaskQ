"""The jobs-by-status sampler's row work must not grow with terminal history.

``_QUERY_JOBS_BY_STATUS_SQL_TEMPLATE`` (``src/taskq/worker/_leader_sweeps.py``)
feeds ``taskq.jobs.by_status`` from ``_backlog_detection_loop`` — deliberately
**not** leader-gated, so every worker pays the read every
``TASKQ_QUEUE_DEPTH_INTERVAL`` (default 15 s) and the fleet pays it N times
(see the sibling pin in test_backlog_detection_sampler_depth_bound.py for why
that gating choice is load-bearing). The original shape was a full-table
``GROUP BY status`` over ``jobs``: every terminal row the retention sweeps
have not pruned yet was re-counted by every worker on every tick, so the
tick's cost grew with *terminal history* — the one dimension that grows
without bound between prune runs — not with anything the alert operands
measure.

The shipped shape keeps the two truth requirements apart:

* **Live statuses (``pending``/``scheduled``/``running``) stay EXACT.** The
  alert operands require it: ``TaskQScheduledBacklogGrowing``'s stalled-plateau
  arm is ``changes(taskq_jobs_scheduled_count[5m]) == 0``, and any capped or
  estimated count reads a constant once the backlog crosses the cap — the rule
  would fire continuously on a deep but healthy backlog, and stop tracking
  growth exactly when growth matters. Exact counting is O(live set) by
  necessity; the per-status partial indexes
  (``jobs_dispatch_idx`` / ``jobs_scheduled_wake_idx`` /
  ``jobs_running_lock_expires_idx``) serve each count without touching a
  terminal row, so the live count's cost tracks the live backlog and nothing
  else.
* **Terminal statuses leave the sampled series.** A terminal count is
  history — already wrong as "how many ever" the moment retention prunes, and
  carried truthfully by the tables and the admin surfaces; no alert operand
  reads ``taskq_jobs_by_status`` for a terminal status. Removing them from the
  tick is what makes the tick's cost independent of unpruned history. The
  series' reader-visible contract (live statuses only) is stated on the
  template's comment block and the loop's docstring.

Oracle: :func:`taskq.testing.pg.install_row_visit_counter` /
:class:`~taskq.testing.pg.RowVisitCounter`, the same row-level-security
sequence-bump oracle the actor-backlog sibling pin uses — it counts rows the
engine actually reads, so it is exact and cannot flake on plan shape or PG
version. The SQL under test is the production template itself, rendered,
never a copy.

This pin was written RED against the original full-table ``GROUP BY status``:
with the fixed live set and 10k seeded terminal rows it returned
``{'pending': 600, 'succeeded': 10000, 'running': 15, 'scheduled': 40}`` —
terminal history counted and reported by a series whose alert operands are
all live statuses — and its row visits tracked the whole table (live plus
unpruned terminal), growing 10x between the two seeded history depths where
the shipped shape's stay flat.
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
from taskq.worker._leader_sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: pin the production sampler statement, not a copy — a paraphrase would let the shipped query regress while the pin stayed green.
    _QUERY_JOBS_BY_STATUS_SQL_TEMPLATE,
)
from tests.test_backlog_detection_sampler_depth_bound import (  # pyright: ignore[reportPrivateUsage]  # Why: the single-connection pool adapter the row-visit oracle expects; one implementation, shared by both depth-oracle pins.
    _SinglePool,
)

pytestmark = pytest.mark.integration

_SHALLOW_HISTORY = 10_000
_DEEP_HISTORY = 100_000
# The live set is FIXED across the two history depths: every visit delta the
# oracle measures is then attributable to the terminal history alone.
_LIVE_COUNTS = {"pending": 600, "scheduled": 40, "running": 15}
_ACTOR = "by_status_probe"
_QUEUE = "by_status_q"

# The same bar the dispatch depth oracle (test_dispatch_backlog_depth_bound.py)
# and the actor-backlog sampler pin hold their queries to: a history-
# independent sampler reads the same live rows at both seeded depths, so the
# visits ratio stays near 1 where the unbounded shape's tracked the history
# ratio (10x).
_MAX_VISITS_HISTORY_RATIO = 3.0


def _by_status_sql(schema: str) -> str:
    # The production template, rendered — not a copy. The query this loop
    # guards is the query the worker actually runs; a pinned paraphrase would
    # let the shipped sampler regress without this test noticing.
    return _QUERY_JOBS_BY_STATUS_SQL_TEMPLATE.format(schema=schema)


@pytest.fixture(scope="module")
async def by_status_schema(pg_dsn: str) -> Any:
    """Throwaway schema, migrations applied, the row-visit-counter policy
    installed on ``jobs``."""
    schema = f"by_status_depth_{new_base62()}".lower()
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


async def _seed(conn: asyncpg.Connection, schema: str, terminal_history: int) -> None:
    """One fixed live set plus *terminal_history* terminal rows (succeeded)."""
    await conn.execute(f'TRUNCATE TABLE "{schema}".jobs CASCADE')
    for status, count in _LIVE_COUNTS.items():
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is this module's own generated identifier (validated against _IDENT_RE in the fixture); status is a module-level literal; the count is $-bound.
            "(id, actor, queue, payload, status, priority, scheduled_at, "
            "max_attempts, retry_kind) "
            f"SELECT gen_random_uuid(), $1, $2, '{{\"v\": 1}}'::jsonb, '{status}', "
            "0, clock_timestamp() - interval '1 minute', 3, 'transient' "
            "FROM generate_series(1, $3::int) AS g",
            _ACTOR,
            _QUEUE,
            count,
        )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: same fixture-local identifier; values are $-bound.
        "(id, actor, queue, payload, status, priority, scheduled_at, "
        "max_attempts, retry_kind, finished_at) "
        "SELECT gen_random_uuid(), $1, $2, '{\"v\": 1}'::jsonb, 'succeeded', "
        "0, clock_timestamp() - interval '1 minute', 3, 'transient', "
        "clock_timestamp() - interval '1 minute' "
        "FROM generate_series(1, $3::int) AS g",
        _ACTOR,
        _QUEUE,
        terminal_history,
    )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')


async def test_jobs_by_status_row_work_is_history_bounded(
    pg_dsn: str, by_status_schema: str
) -> None:
    """The by-status sampler's row visits at 100k terminal rows must not scale
    with the 10k reading, and the live counts must stay exact at both depths.

    The original full-table ``GROUP BY status`` re-read every unpruned
    terminal row on every worker every tick — the unbounded cost this pin
    forbids. The alert operands' exactness is pinned in the same run: a
    regression that bounds the live counts (a sample, a cap, an estimate)
    fails the exactness assertions even when it passes the visits ratio, and
    a regression that re-widens the scan to the whole table fails the ratio.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        counter = RowVisitCounter(_SinglePool(conn), by_status_schema, method="fetch")

        await _seed(conn, by_status_schema, _SHALLOW_HISTORY)
        sql = _by_status_sql(by_status_schema)
        async with counter.acquire() as counted_conn:
            shallow_rows = await counted_conn.fetch(sql)
        shallow_visits = counter.per_statement[-1]
        shallow_counts = {str(row["status"]): int(row["count"]) for row in shallow_rows}

        await _seed(conn, by_status_schema, _DEEP_HISTORY)
        async with counter.acquire() as counted_conn:
            deep_rows = await counted_conn.fetch(sql)
        deep_visits = counter.per_statement[-1]
        deep_counts = {str(row["status"]): int(row["count"]) for row in deep_rows}

        # The alert operands stay exact at any history depth: the live
        # statuses are counted, not sampled (a capped live count reads a
        # constant at the cap and false-fires the stalled-plateau arm of
        # TaskQScheduledBacklogGrowing).
        for counts in (shallow_counts, deep_counts):
            assert counts == _LIVE_COUNTS, (
                f"the by-status sample must return exactly the live set "
                f"{_LIVE_COUNTS} — no terminal series, no sampled live count — "
                f"got {counts}"
            )

        assert shallow_visits > 0, "the counting policy did not fire -- setup is broken"
        ratio = deep_visits / shallow_visits
        history_ratio = _DEEP_HISTORY / _SHALLOW_HISTORY

        assert ratio < _MAX_VISITS_HISTORY_RATIO, (
            f"jobs-by-status sampler visited {shallow_visits} rows at "
            f"{_SHALLOW_HISTORY} terminal rows and {deep_visits} rows at "
            f"{_DEEP_HISTORY} (ratio {ratio:.1f}x for a {history_ratio:.0f}x "
            f"history increase) -- row work scales with unpruned terminal "
            f"history, which grows without bound between prune sweeps. This "
            f"runs on every worker, every TASKQ_QUEUE_DEPTH_INTERVAL, "
            f"unconditionally."
        )
    finally:
        await conn.close()
