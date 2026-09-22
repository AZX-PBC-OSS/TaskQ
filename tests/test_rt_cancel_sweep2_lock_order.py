# ruff: noqa: S608  # Why: schema is a test-fixture identifier, validated by render()/sweep_deadline_exceeded in every caller; every user-supplied value is $-bound.
"""Hunt-gap pin: sweep-2's row-lock order vs the bulk-cancel drain's.

Two writers sweep the SAME overdue pending backlog with opposite row
orders: ``sweep_deadline_exceeded`` windows its batch ``ORDER BY
schedule_to_close`` (the partial-index key, its snap CTE takes ``FOR
UPDATE SKIP LOCKED``), while ``_cancel_where``'s pending/scheduled arm
windows ``ORDER BY id`` (the keyset cursor's meaning, no SKIP LOCKED: a
window that skips is a window that strands). Opposite orders over one
row set is the classic lock-order inversion: the drain's in-flight batch
holds low-id rows and waits on a high-id row the sweep's snap holds,
and where the planner's lock acquisition order for the drain's
``id = ANY`` array forms a cycle, PG's deadlock detector kills one side.
Both sides own the transient retry that resolves it: the drain re-runs
the deadlocked batch (its predicate no longer matches rows an earlier
committed page terminalised, nothing counts twice), and the leader loop
classifies ``DeadlockDetectedError`` as transient and sweeps again next
tick.

Two pins live here.

The first is the concurrent race over a handful of bounded trials.  Its
contract is conservation, not scheduling: BOTH operations complete, no
lost update - every overdue job ends terminal exactly once, either
'failed'/'DeadlineExceeded' (the sweep won the row) or
'cancelled'/'CancelledBeforeStart' (the drain won it), never pending,
never both - and the event trail says the same: one ``state_change`` per
job, a ``cancel_request`` exactly on the drain's rows.  HOW the rows
split between the two writers is the scheduler's business: the sweep's
candidate round trip and the drain's first batch race, and on a starved
runner the drain may commit every batch before the sweep's statement
lands, so ``swept == 0`` is a legal outcome of this test (the
conservation checks below hold at any split; the count reconciliation
fails on any lost update, which is the defect class this hunt exists
for).

The second pins the property the race cannot sample deterministically:
the drain does not MONOPOLISE the backlog across its runtime.  Each
batch commits and releases its row locks before the next candidate
window, so a deadline sweep arriving mid-drain always finds unlocked
overdue rows to own.  A gated witness pool parks the drain before its
second batch's candidate, the sweep runs to completion inline (it wins
rows BY CONSTRUCTION, no lottery), then the drain resumes and both
complete with the same conservation contract.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import BulkCancelResult, JobFilter
from taskq.backend._sql_templates import render
from taskq.backend._sweeps import sweep_deadline_exceeded
from taskq.constants import CANCEL_ORIGIN_PENDING
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_TRIALS = 3
_BACKLOG = 40
_BATCH = 10
_RACE_BOUND_SECS = 30.0


async def _seed_inverted_backlog(
    conn: asyncpg.Connection,
    schema: str,
    tag: str,
    count: int,
) -> list[UUID]:
    """Seed *count* overdue pending jobs whose schedule_to_close order is
    the REVERSE of their id order: the sweep wants the greatest ids first
    while the drain walks from the least id, the inversion the two
    statements' own ORDER BY clauses guarantee."""
    ids = [new_uuid() for _ in range(count)]
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "scheduled_at, schedule_to_close, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, 'pending', 3, 'transient', "
        "clock_timestamp() - interval '10 seconds', "
        "clock_timestamp() - interval '60 seconds', ARRAY[$2::text] "
        "FROM unnest($1::uuid[]) AS t(id)",
        ids,
        tag,
    )
    # rownum 1 is the GREATEST id; subtracting more time makes its
    # schedule_to_close the oldest, the sweep's first window row.
    await conn.execute(
        f'UPDATE "{schema}".jobs j '
        "SET schedule_to_close = clock_timestamp() "
        "- r.rownum * interval '1 second' - interval '30 seconds' "
        "FROM ("
        f"  SELECT id, row_number() OVER (ORDER BY id DESC) AS rownum "
        f'  FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text]'
        ") r WHERE j.id = r.id",
        tag,
    )
    return ids


async def _final_states(
    conn: asyncpg.Connection,
    schema: str,
    ids: list[UUID],
) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in await conn.fetch(
            f"SELECT id, status::text AS status, error_class "
            f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY id',
            ids,
        )
    ]


async def _event_counts(
    conn: asyncpg.Connection, schema: str, ids: list[UUID]
) -> dict[UUID, dict[str, int]]:
    rows = await conn.fetch(
        f'SELECT job_id, kind, count(*)::int AS n FROM "{schema}".job_events '
        "WHERE job_id = ANY($1::uuid[]) GROUP BY job_id, kind",
        ids,
    )
    per_job: dict[UUID, dict[str, int]] = {}
    for r in rows:
        per_job.setdefault(r["job_id"], {})[r["kind"]] = r["n"]
    return per_job


class _BatchBoundaryPool:
    """Duck-typed pool that parks the drain between its first two batches.

    The drain's per-round sequence is two arms, EACH a bounded fixpoint
    loop of committed batches: the pending/scheduled arm windows the
    match set batch by batch (one committed transaction per batch: the
    arm statement, then the batch's event writes) until the window comes
    back short, and only then does the running arm start its own loop.
    The arms' statements travel through ``conn.fetchrow``, so the gate
    lives on the fetchrow route.

    Gating on the pending arm's SQL text and parking at its SECOND
    execution lands the park exactly between batch one's COMMIT and
    batch two's candidate: batch one's ten rows are terminal and its
    row locks left with the transaction, nothing is locked at the park,
    and the backlog beyond batch one is pending and unlocked.
    """

    def __init__(self, inner: asyncpg.Pool, gate_sql: str, gate_on_match: int = 1) -> None:
        self._inner = inner
        self._gate_sql = gate_sql
        self._gate_on_match = gate_on_match
        self._matches = 0
        self.gate_entered = asyncio.Event()
        self.gate_release = asyncio.Event()

    async def _through(
        self,
        method: str,
        conn: asyncpg.Connection,
        query: str,
        args: tuple[object, ...],
        kw: dict[str, object],
    ):
        if not self.gate_entered.is_set() and self._gate_sql in query:
            self._matches += 1
            if self._matches >= self._gate_on_match:
                self.gate_entered.set()
                await self.gate_release.wait()
        return await getattr(conn, method)(query, *args, **kw)

    async def acquire(self) -> _GatedConn:
        conn = await self._inner.acquire()
        return _GatedConn(conn, self)

    async def release(
        self,
        conn: _GatedConn,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.release's own keyword; the checkout forwards it verbatim.
    ) -> None:
        # The drain's checkout releases through THIS pool (the release is
        # what returns the connection and carries the bounded reset).
        # Without the forwarding, asyncpg's checkout swallows the
        # AttributeError as pool hygiene and every batch LEAKS its
        # connection from the inner module pool (max_size 4): the next
        # acquire wedges the drain, and pool.close() in the fixture
        # teardown hangs the pytest process with it.
        await self._inner.release(
            conn._conn, timeout=timeout
        )  # Why: the wrapper and the connection live in this file; the attribute is the harness's own plumbing, not a foreign private.


class _GatedConn:
    """Duck-typed connection: every call routes through the pool's gate,
    then forwards to the real connection with the caller's API shape."""

    def __init__(self, conn: asyncpg.Connection, pool: _BatchBoundaryPool) -> None:
        self._conn = conn
        self._pool = pool

    async def execute(self, query: str, *args: object, **kw: object) -> str:
        return await self._pool._through("execute", self._conn, query, args, kw)

    async def fetch(self, query: str, *args: object, **kw: object) -> list[asyncpg.Record]:
        return await self._pool._through("fetch", self._conn, query, args, kw)

    async def fetchrow(self, query: str, *args: object, **kw: object) -> asyncpg.Record | None:
        return await self._pool._through("fetchrow", self._conn, query, args, kw)

    async def fetchval(self, query: str, *args: object, **kw: object) -> object:
        return await self._pool._through("fetchval", self._conn, query, args, kw)

    def __getattr__(self, item: str) -> object:
        return getattr(self._conn, item)


async def _assert_conservation(
    clean_pg_conn: asyncpg.Connection,
    schema: str,
    ids: list[UUID],
    swept: int,
    result: BulkCancelResult,
) -> None:
    """The contract both pins share: every row terminal exactly once,
    the counts reconcile, the event trail matches the winner."""
    states = await _final_states(clean_pg_conn, schema, ids)
    by_status: dict[str, int] = {}
    for row in states:
        by_status[row["status"]] = by_status.get(row["status"], 0) + 1
        if row["status"] == "failed":
            assert row["error_class"] == "DeadlineExceeded", (
                "a row the sweep won carries the sweep's own error class"
            )
        elif row["status"] == "cancelled":
            assert row["error_class"] == CANCEL_ORIGIN_PENDING, (
                "a row the drain won carries the drain's cancel-origin class"
            )
        else:
            pytest.fail(
                f"LOST UPDATE: job {row['id']} ended {row['status']!r} - under the "
                "inverted row-lock order one operation dropped a row it had "
                "windowed, and no retry picked it up"
            )
    assert by_status.get("failed", 0) == swept, (
        "the sweep's count is exactly the rows it terminalised - nothing "
        "it locked was lost to the drain"
    )
    assert by_status.get("cancelled", 0) == result.cancelled_directly, (
        "the drain's count is exactly the rows it terminalised - nothing "
        "it locked was lost to the sweep, and no row was counted twice"
    )
    assert sum(by_status.values()) == len(ids)

    per_job = await _event_counts(clean_pg_conn, schema, ids)
    for row in states:
        kinds = per_job.get(row["id"], {})
        assert kinds.get("state_change", 0) == 1, (
            f"job {row['id']} must carry exactly one terminal state_change"
        )
        if row["status"] == "cancelled":
            assert kinds.get("cancel_request", 0) == 1
        else:
            assert "cancel_request" not in kinds, "a row the sweep won was never a cancel target"
    state_by_id = {r["id"]: r for r in states}
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, detail FROM "{schema}".job_events '
        "WHERE job_id = ANY($1::uuid[]) ORDER BY job_id, kind",
        ids,
    )
    for e in events:
        detail = parse_detail(e["detail"])
        if e["kind"] != "state_change":
            continue
        row = state_by_id[e["job_id"]]
        expected = (
            {"from_state": "pending", "to_state": "failed", "error_class": "DeadlineExceeded"}
            if row["status"] == "failed"
            else {"from_state": "pending", "to_state": "cancelled"}
        )
        assert detail == expected


@pytest.mark.parametrize("trial", range(_TRIALS))
async def test_deadline_sweep_and_bulk_cancel_under_inverted_row_orders_both_complete(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The inverted-order race: both operations complete, every row ends
    terminal exactly once, and the event trail matches the winner."""
    schema = module_pg_schema.schema_name
    render(schema)
    tag = f"sweep2x_trial{trial}"
    ids = await _seed_inverted_backlog(clean_pg_conn, schema, tag, _BACKLOG)
    assert len(ids) == _BACKLOG, "fixture broken: seeding"

    sweep_task = asyncio.create_task(
        sweep_deadline_exceeded(clean_pg_conn, schema=schema, batch_size=_BATCH)
    )
    cancel_task = asyncio.create_task(
        _cancel_where(
            module_pg_pool,
            schema,
            render(schema),
            JobFilter(tags=(tag,)),
            "offboard",
            batch_size=_BATCH,
        )
    )
    swept: int = await asyncio.wait_for(sweep_task, timeout=_RACE_BOUND_SECS)
    result, notify_targets = await asyncio.wait_for(cancel_task, timeout=_RACE_BOUND_SECS)

    # How the rows split is the scheduler's business: the sweep's
    # candidate round trip races the drain's first batch, and on a
    # starved runner the drain may commit every batch first, so
    # swept == 0 is a legal outcome here (both still complete; the
    # conservation contract below holds at any split).  The
    # no-monopoly property - the drain cannot hold the backlog across
    # its runtime - is pinned deterministically by the companion test.
    assert result.cancel_requested == 0, "every row is pending; the running arm matches nothing"
    assert notify_targets == []

    await _assert_conservation(clean_pg_conn, schema, ids, swept, result)


async def test_the_drain_releases_its_batch_locks_so_the_sweep_always_gets_a_window(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The no-monopoly property, forced: the drain parks before its
    SECOND batch's candidate (batch one is committed, its locks
    released, rows batch two-plus still pending and unlocked); the
    deadline sweep runs to completion in that window and wins rows BY
    CONSTRUCTION - no scheduling lottery.  The drain then resumes over
    the remainder and both complete under the conservation contract."""
    schema = module_pg_schema.schema_name
    render(schema)
    tag = "sweep2x_monopoly"
    ids = await _seed_inverted_backlog(clean_pg_conn, schema, tag, _BACKLOG)

    # The drain's per-round sequence: the pending/scheduled arm drains
    # the match set as committed batches until its window comes back
    # short, and ONLY then does the running arm start.  So the gate sits
    # on the pending arm's statement and parks at its SECOND execution:
    # batch one is committed (ten rows terminal, its locks left with
    # that transaction), the park is before batch two's candidate, and
    # the thirty rows beyond batch one are pending and unlocked.
    pool = _BatchBoundaryPool(
        module_pg_pool,
        gate_sql="IN ('pending', 'scheduled')",
        gate_on_match=2,
    )
    drain_task = asyncio.create_task(
        _cancel_where(
            pool,  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() and release() are used.
            schema,
            render(schema),
            JobFilter(tags=(tag,)),
            "offboard",
            batch_size=_BATCH,
        )
    )

    try:
        await _pin_the_no_monopoly_property(pool, drain_task, clean_pg_conn, schema, ids)
    finally:
        pool.gate_release.set()
        if not drain_task.done():
            # A red pin raises before it awaits the drain; cancel and
            # drain the task so the suite's leftover-task guard sees the
            # failure shape, not teardown noise.
            drain_task.cancel()
            with contextlib.suppress(
                asyncio.CancelledError, Exception
            ):  # Why: failure-path hygiene, the pin's own failure already propagated.
                await drain_task


async def _pin_the_no_monopoly_property(
    pool: _BatchBoundaryPool,
    drain_task: asyncio.Task[tuple[BulkCancelResult, list[object]]],
    clean_pg_conn: asyncpg.Connection,
    schema: str,
    ids: list[UUID],
) -> None:
    await asyncio.wait_for(pool.gate_entered.wait(), timeout=_RACE_BOUND_SECS)

    # Batch one is committed; the park is before batch two's candidate,
    # so nothing is locked.  The sweep's SKIP LOCKED owns unlocked
    # overdue rows by construction.
    swept: int = await asyncio.wait_for(
        sweep_deadline_exceeded(clean_pg_conn, schema=schema, batch_size=_BATCH),
        timeout=_RACE_BOUND_SECS,
    )
    assert swept >= 1, (
        "the sweep owns unlocked overdue rows by construction: the drain "
        "parks between its first two committed batches with nothing "
        "locked, so the sweep's window has rows"
    )

    pool.gate_release.set()
    result, notify_targets = await asyncio.wait_for(drain_task, timeout=_RACE_BOUND_SECS)
    assert notify_targets == []

    await _assert_conservation(clean_pg_conn, schema, ids, swept, result)
