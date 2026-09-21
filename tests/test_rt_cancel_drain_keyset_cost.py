"""Red team: a bulk-cancel drain must not re-read what it already cancelled.

The drain re-selects "the next ``batch_size`` matching rows" on every
pass.  If that window has no lower bound, every pass starts at the
beginning of the key space and walks over the rows earlier passes already
moved out of the match set before it reaches live ones.  Batch N then
pays for the (N-1) * ``batch_size`` rows its predecessors handled, so
total drain work is quadratic in backlog depth.

Operationally that is a bulk cancel an operator can run on a shallow
backlog and not on a deep one: the later batches take longer the deeper
they get, trip their own per-batch ``statement_timeout``, and strand the
tail -- on exactly the backlogs where the command matters most.  Nothing
errors and no count is wrong; the drain stops finishing, which is
why this needs an explicit cost pin rather than a correctness one.

Cost is measured with :func:`taskq.testing.pg.install_row_visit_counter`
and :class:`~taskq.testing.pg.RowVisitCounter`: the engine counts every
row it actually visits, so these assert the wasted work itself rather
than a proxy for it.  That helper's docstring carries the full rationale
-- in short it reads no ``EXPLAIN`` output (so an engine upgrade cannot
break it), needs no knowledge of the SQL under test (so it follows any
rewrite that keeps the drain bounded), and is exact (so it cannot flake).

Each test also asserts completeness, so a "bounded" drain that achieves
flatness by dropping the tail fails here rather than passes.  The
correctness half -- containment, per-row ``from_state``, idempotent
re-runs -- is pinned in ``test_cancel_where_bounded.py``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _UUID_MIN, _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import RowVisitCounter, install_row_visit_counter, read_row_visits

pytestmark = pytest.mark.integration

_BATCH = 20
# Deep enough that linear and quadratic are unambiguously different: at
# this depth a bounded drain costs a few thousand row visits and an
# unbounded one costs ~16000, so no threshold has to be tuned finely to
# tell them apart. (At 200 rows the two are ~960 against ~1000 -- too
# close to pin without the assertion becoming brittle.)
_BATCHES = 40
_BACKLOG = _BATCH * _BATCHES


async def _seed(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
    tags: Sequence[str],
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value is $N-bound.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'test_actor', 'default', '{{}}'::jsonb, $2::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', $3::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        status,
        list(tags),
    )


async def _drain(
    pool: Any, schema: str, tag: str, *, reason: str | None = None
) -> tuple[Any, list[list[int]]]:
    """Run the real bulk cancel to completion.

    Returns its result and the per-batch row visits **grouped by driving
    statement within one fixpoint round**. A bulk cancel runs two arms
    -- terminal cancel of pending/scheduled, then cooperative cancel of
    running -- each with its own cursor, and since the pair runs as
    bounded fixpoint ROUNDS: every round re-issues each arm's statement
    from a fresh cursor, so grouping by statement text alone would pool
    the later rounds' single probing batches into the round-1 drain and
    misread a two-probe arm as a drain. The round-aware counter below
    splits the groups at every fresh-cursor batch, so each arm's
    round-1 DRAIN stays measurable against its own bound.

    Only groups that actually drained (more than one batch within the
    round) are returned: an arm with nothing to do issues a single
    probing statement per round whose cost says nothing about how the
    drain scales.
    """
    counter = _RoundAwareDrainCounter(pool, schema)
    result, _notify = await _cancel_where(
        counter,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        schema,
        render(schema),
        JobFilter(tags=(tag,)),
        reason,
        batch_size=_BATCH,
    )
    return result, [v for v in counter.by_round.values() if len(v) > 1]


class _RoundAwareDrainCounter(RowVisitCounter):
    """:class:`RowVisitCounter` with the fixpoint's round boundaries.

    Measurement is the parent's verbatim (the RLS row-visit policy, the
    role switch, the per-statement deltas); this subclass additionally
    reads the keyset cursor each driving batch binds -- the argument
    after the filter params, exactly as `_drain_cancel_batches` binds
    them -- and attributes every batch to ``(statement, round)``, where
    a round begins at each fresh-cursor batch (``_UUID_MIN``: the first
    batch of an arm's drain in each fixpoint round). The grouping is
    what keeps the pins below honest under the rounds: an arm that
    drains pays its batch-per-batch cost in round 1, while the later
    rounds' single confirmation probes stay single-batch groups the
    `len(v) > 1` drain filter excludes -- for the same reason the
    original harness excluded a lone probe: its cost says nothing about
    how a drain scales.

    One harness caveat, accepted: a deadlock RETRY of a drain's first
    batch re-binds the same fresh cursor and would bump the round
    counter spuriously -- these pins seed no contention, so no batch
    ever retries here.
    """

    def __init__(self, pool: Any, schema: str) -> None:
        super().__init__(pool, schema)
        #: (arm statement, round index) -> per-batch row visits.
        self.by_round: dict[tuple[str, int], list[int]] = {}
        self._round_of: dict[str, int] = {}

    def acquire(self, **kwargs: object) -> Any:
        outer = self
        inner_acquire = super().acquire(**kwargs)

        class _Acquire:
            async def __aenter__(self) -> Any:
                self._conn = await inner_acquire.__aenter__()
                return _RoundSniffingConnection(self._conn, outer)

            async def __aexit__(self, *exc: object) -> Any:
                return await inner_acquire.__aexit__(*exc)

        return _Acquire()


class _RoundSniffingConnection:
    """Delegates to the counting connection, attributing each driving
    batch to its (statement, round) group."""

    def __init__(self, inner: Any, counter: _RoundAwareDrainCounter) -> None:
        self._inner = inner
        self._counter = counter

    async def fetchrow(self, sql: str, *args: object) -> Any:
        counter = self._counter
        key = str(sql)
        if args and args[-2] == _UUID_MIN:
            counter._round_of[key] = counter._round_of.get(key, 0) + 1
        round_idx = counter._round_of.get(key, 0)
        before = await read_row_visits(self._inner, counter._schema)
        row = await self._inner.fetchrow(sql, *args)
        after = await read_row_visits(self._inner, counter._schema)
        counter.by_round.setdefault((key, round_idx), []).append(after - before)
        return row

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _assert_total_work_is_linear(visits: list[int], backlog: int, *, what: str) -> None:
    """Fail if draining the backlog cost more than a linear pass over it.

    This is the property that actually matters, and it is asserted
    directly rather than through a per-batch proxy.

    A drain whose window resumes where the last pass stopped touches each
    row a constant number of times, so total work is O(backlog). One that
    restarts at the beginning of the key space touches the rows it already
    handled again on every pass, so total work is
    ``backlog^2 / (2 * batch_size)`` -- for a 200-row backlog at
    batch_size 20 that is ~1000 against ~400, and the gap widens with the
    square as the backlog deepens.

    Asserting the total rather than a per-batch maximum matters: the final
    batches of a keyset drain legitimately cost more per batch (few rows
    remain ahead of the cursor, so the planner stops using the ordered
    index for them), and that tail is bounded by one pass over the table.
    A per-batch cap either fails on that harmless tail or has to be
    loosened until it stops catching the defect. The total catches the
    defect and ignores the tail.
    """
    assert len(visits) >= 5, (
        f"expected the backlog to drain over several batches to have anything to "
        f"measure; got {len(visits)} batches for {backlog} rows at batch_size={_BATCH}"
    )
    total = sum(visits)
    # What a drain that restarts at the beginning of the key space costs:
    # batch N re-walks the (N-1) * batch_size rows its predecessors
    # handled, summing to backlog^2 / (2 * batch_size).
    quadratic = backlog * backlog // (2 * _BATCH)
    assert total < quadratic, (
        f"{what}: draining {backlog} rows made the engine visit {total} rows in total, "
        f"at or above the ~{quadratic} a drain that re-reads what it already handled "
        f"would cost. Each pass is re-walking the part of the match set earlier passes "
        f"already handled, so total work grows with the square of backlog depth: on a "
        f"real backlog the later batches blow their own statement_timeout and strand "
        f"the tail, with nothing raised anywhere. Rows visited per batch: {visits!r}"
    )


async def test_cancel_drain_does_not_reread_rows_it_already_cancelled(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A pending/scheduled drain visits a batch's worth of rows per batch,
    whether it is the first pass over the backlog or the last."""
    schema = module_pg_schema.schema_name
    render(schema)
    await install_row_visit_counter(clean_pg_conn, schema)
    await _seed(
        clean_pg_conn,
        schema,
        [new_uuid() for _ in range(_BACKLOG)],
        status="pending",
        tags=["tenant-acme"],
    )
    await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')

    result, drains = await _drain(module_pg_pool, schema, "tenant-acme", reason="offboard")

    assert result.cancelled_directly == _BACKLOG, (
        "a bounded drain must still cancel the whole match set"
    )
    assert len(drains) == 1, (
        "only the terminal arm should have drained (one multi-batch group: "
        "its round-1 pass; every other arm-round pair is a single probe)"
    )
    _assert_total_work_is_linear(
        drains[0], _BACKLOG, what="terminal cancel of pending/scheduled jobs"
    )


async def test_running_arm_drain_does_not_reread_rows_it_already_requested(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The cooperative-cancel arm has the same bound as the terminal one.

    A running job stays ``running`` after this arm requests its cancel --
    only ``cancel_phase`` moves 0 -> 1, because the worker owns the
    terminal write -- so an unbounded window keeps re-reading rows it
    already requested exactly as the terminal arm does. Fixing one arm and
    not the other leaves half the defect in place, which is why this is
    pinned separately.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    await install_row_visit_counter(clean_pg_conn, schema)
    await _seed(
        clean_pg_conn,
        schema,
        [new_uuid() for _ in range(_BACKLOG)],
        status="running",
        tags=["tenant-acme"],
    )
    await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')

    result, drains = await _drain(module_pg_pool, schema, "tenant-acme", reason="offboard")

    assert result.cancel_requested == _BACKLOG, "every running job must still be cancel-requested"
    assert result.cancelled_directly == 0, "a bulk cancel never terminalises a running job"
    # The terminal arm runs first and finds nothing (every job is
    # running), so it issues a single probing statement per fixpoint
    # round and never drains; the cooperative arm is the one that drains.
    # Assert that explicitly rather than assuming which group is which --
    # picking the wrong group would measure a statement that never looped
    # and pass regardless of the defect.
    assert len(drains) == 1, (
        f"expected exactly one arm-round to drain; got {len(drains)} groups of sizes "
        f"{[len(d) for d in drains]}"
    )
    _assert_total_work_is_linear(drains[0], _BACKLOG, what="cooperative cancel of running jobs")


async def test_drain_total_work_grows_linearly_not_quadratically_with_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Doubling the backlog roughly doubles the drain's total work.

    The sharpest statement of the contract, because it measures the
    GROWTH RATE rather than any absolute number: it needs no tuned
    threshold, no assumption about table size, and no knowledge of which
    index the planner picked. A drain that re-reads what it already
    handled quadruples its work when the backlog doubles; a bounded one
    doubles it.

    Doing it in one test, on one table shape, with the same batch size,
    keeps the comparison honest -- the only variable is depth.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    await install_row_visit_counter(clean_pg_conn, schema)

    async def _total_work(backlog: int, tag: str) -> int:
        await _seed(
            clean_pg_conn,
            schema,
            [new_uuid() for _ in range(backlog)],
            status="pending",
            tags=[tag],
        )
        await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')
        result, drains = await _drain(module_pg_pool, schema, tag)
        assert result.cancelled_directly == backlog, "the whole match set must still be cancelled"
        return sum(drains[0])

    small = await _total_work(_BACKLOG, "tenant-small")
    large = await _total_work(_BACKLOG * 2, "tenant-large")

    # Linear growth doubles; quadratic growth quadruples. Three sits
    # clear of both, so the test distinguishes them without being tuned
    # to either.
    assert large <= small * 3, (
        f"doubling the backlog from {_BACKLOG} to {_BACKLOG * 2} rows took the drain's "
        f"total work from {small} to {large} row visits -- more than the doubling a "
        f"bounded drain costs, and toward the quadrupling of one that re-reads what it "
        f"already handled. Total drain work grows with the square of backlog depth, so "
        f"the deeper the backlog the more certainly the later batches blow their own "
        f"statement_timeout and strand the tail."
    )


async def test_drain_cost_does_not_track_other_tenants_backlog(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """One tenant's offboard costs the same however deep everyone else's
    backlog is.

    The filter is the whole point of a filtered write. If the drain's cost
    tracks the table's total live population rather than the match set,
    every tenant's offboard slows as the fleet grows -- and the only
    symptom is a command that used to finish and now times out, with
    nothing in that tenant's own metrics to explain it.
    """
    schema = module_pg_schema.schema_name
    render(schema)
    await install_row_visit_counter(clean_pg_conn, schema)

    await _seed(
        clean_pg_conn,
        schema,
        [new_uuid() for _ in range(_BACKLOG)],
        status="pending",
        tags=["tenant-acme"],
    )
    await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')
    _small, small_drains = await _drain(module_pg_pool, schema, "tenant-acme")

    # Same match-set size; everyone else's backlog is now far larger.
    await _seed(
        clean_pg_conn,
        schema,
        [new_uuid() for _ in range(_BACKLOG)],
        status="pending",
        tags=["tenant-acme"],
    )
    for tenant in range(10):
        await _seed(
            clean_pg_conn,
            schema,
            [new_uuid() for _ in range(_BACKLOG)],
            status="pending",
            tags=[f"tenant-other-{tenant}"],
        )
    await clean_pg_conn.execute(f'ANALYZE "{schema}".jobs')
    result, large_drains = await _drain(module_pg_pool, schema, "tenant-acme")

    assert result.cancelled_directly == _BACKLOG
    small_total = sum(small_drains[0])
    large_total = sum(large_drains[0])

    # The match set is unchanged and only unrelated rows were added, so a
    # drain scoped to its filter costs the same either way. The bound is
    # stated against the QUADRATIC cost of re-reading the match set on
    # every pass, because that is the defect this file exists to catch.
    #
    # It is deliberately NOT a tight "same cost in both runs" assertion.
    # A tag filter cannot be fully scoped by an index here: a GIN index
    # over `tags` answers `tags && ARRAY[...]` but produces a bitmap,
    # which carries no key order, so it cannot also satisfy the window's
    # `ORDER BY id`. The planner must therefore walk an id-ordered btree
    # and test the tag predicate per row, and some dependence on the
    # table's live population is intrinsic rather than a defect in this
    # drain. (Verified against the engine: adding a partial GIN index on
    # tags changes neither the plan nor the cost at any size measured.)
    # What must NOT happen -- and is what this pins -- is the cost
    # compounding per batch.
    quadratic = _BACKLOG * _BACKLOG // (2 * _BATCH)
    assert large_total < quadratic, (
        f"one tenant's drain visited {large_total} rows in total once other tenants' "
        f"jobs were present (against {small_total} before), at or above the "
        f"~{quadratic} that re-reading the match set on every pass would cost. Its "
        f"cost compounds per batch rather than staying scoped to its own match set, so "
        f"every tenant's offboard degrades as the fleet grows -- with nothing in that "
        f"tenant's own metrics to explain it."
    )
