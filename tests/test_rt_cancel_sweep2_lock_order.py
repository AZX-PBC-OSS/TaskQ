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

The pin asserts the observable contract under that inversion, over a
handful of bounded trials (the detector's intervention is
scheduler-timed, the outcomes are not):

* BOTH operations complete - neither parks unbounded behind the other;
* no lost update: every overdue job ends terminal exactly once, either
  'failed'/'DeadlineExceeded' (the sweep won the row) or
  'cancelled'/'CancelledBeforeStart' (the drain won it), never pending,
  never both;
* the event trail says the same: one ``state_change`` per job, and a
  ``cancel_request`` exactly on the drain's rows.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
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
    conn: asyncpg.Connection, schema: str, ids: list[UUID]
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

    # Both operations ran to completion on the same row set: the sweep
    # took its one bounded batch, the drain took the rest of the backlog
    # (all rows stay pending/scheduled for it, nothing re-feeds the
    # match set mid-race).
    assert swept >= 1, "the sweep must win at least one row of the inverted backlog"
    assert result.cancel_requested == 0, "every row is pending; the running arm matches nothing"
    assert notify_targets == []

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
    assert sum(by_status.values()) == _BACKLOG

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
    # The state_change detail names the winner per row, spot-checking
    # the two shapes on every event row.
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
