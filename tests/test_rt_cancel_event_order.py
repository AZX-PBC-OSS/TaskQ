"""Cross-batch ``occurred_at`` ordering in the bounded bulk cancel's events.

``poll_reclaim_events``' trailing watermark assumes ``job_events.id``
and ``occurred_at`` are co-monotonic (see
``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY``).  The pinned
within-batch pin exists
(``test_cancel_where_bounded.py::test_pins_event_occurred_at_is_per_row_and_co_monotonic_with_id``);
the bounded drain adds a boundary the single-transaction design never
had: events written by LATER batches commit after earlier batches, so
their stamps must not invert against them either.  A drain that
computed one timestamp per batch in Python (or reused a frozen
transaction timestamp across batch boundaries) would violate exactly
this and silently skip reclaim events.

Also re-pins per-row ``from_state`` correctness at drain scale: 400
mixed pending/scheduled jobs across 4 batches, each ``state_change``
carrying its own row's real prior status.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_PENDING = 200
_SCHEDULED = 200
_TOTAL = _PENDING + _SCHEDULED
# The default batch size (taskq.constants.DEFAULT_EVENT_WRITER_BATCH_SIZE
# is 100): 400 jobs drain as 4 committed batches.
_BATCH = 100


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, $2::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', ARRAY['bulk']::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        status,
    )


async def test_occurred_at_is_non_decreasing_across_batch_boundaries(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A 400-job drain (4 batches): occurred_at is non-decreasing in
    job_events.id order GLOBALLY, every batch boundary is ordered
    (batch k+1's first stamp >= batch k's last), and every job carries
    one state_change with its own real prior status plus one
    cancel_request."""
    schema = module_pg_schema.schema_name
    render(schema)
    pending_ids = [new_uuid() for _ in range(_PENDING)]
    scheduled_ids = [new_uuid() for _ in range(_SCHEDULED)]
    await _seed_jobs(clean_pg_conn, schema, pending_ids, status="pending")
    await _seed_jobs(clean_pg_conn, schema, scheduled_ids, status="scheduled")
    expected_from: dict[UUID, str] = dict.fromkeys(pending_ids, "pending")
    expected_from.update(dict.fromkeys(scheduled_ids, "scheduled"))

    result, _notify = await _cancel_where(
        module_pg_pool,
        schema,
        render(schema),
        JobFilter(tags=("bulk",)),
        "offboard",
    )
    assert result.cancelled_directly == _TOTAL

    rows = await clean_pg_conn.fetch(
        f'SELECT id, job_id, kind, detail, occurred_at FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "ORDER BY id"
    )
    assert len(rows) == 2 * _TOTAL

    # Global co-monotonicity in id order — the invariant the trailing
    # watermark rests on, now spanning four separate committed batches.
    stamps = [r["occurred_at"] for r in rows]
    inversions = [
        (rows[i]["id"], rows[i + 1]["id"])
        for i in range(len(stamps) - 1)
        if stamps[i + 1] < stamps[i]
    ]
    assert inversions == [], (
        f"occurred_at must be non-decreasing in id order across batch boundaries — "
        f"{len(inversions)} inversion(s), e.g. {inversions[:3]}"
    )

    # Per-boundary ordering: the driving CTE is ORDER BY id LIMIT, so
    # batch k is the kth 100-job slice of the sorted ids; every event of
    # a batch-k job must stamp no later than every event of a batch-(k+1)
    # job.
    drain_order = sorted(expected_from)
    batch_of: dict[UUID, int] = {jid: pos // _BATCH for pos, jid in enumerate(drain_order)}
    batch_bounds: dict[int, tuple[datetime, datetime]] = {}
    for row in rows:
        b = batch_of[row["job_id"]]
        lo, hi = batch_bounds.get(b, (row["occurred_at"], row["occurred_at"]))
        batch_bounds[b] = (min(lo, row["occurred_at"]), max(hi, row["occurred_at"]))
    for b in range(len(batch_bounds) - 1):
        cur_hi = batch_bounds[b][1]
        next_lo = batch_bounds[b + 1][0]
        assert next_lo >= cur_hi, (
            f"batch {b + 1}'s earliest event stamp {next_lo} precedes batch {b}'s "
            f"latest {cur_hi} — a per-batch frozen or Python-side timestamp would "
            f"invert exactly here, and poll_reclaim_events' trailing watermark "
            f"silently skips events in the gap"
        )

    # Exactly one event of each kind per job, from_state per row.
    state_changes = [r for r in rows if r["kind"] == "state_change"]
    cancel_requests = [r for r in rows if r["kind"] == "cancel_request"]
    assert {r["kind"] for r in rows} == {"state_change", "cancel_request"}
    assert len(state_changes) == _TOTAL
    assert len(cancel_requests) == _TOTAL
    for row in state_changes:
        jid = row["job_id"]
        assert parse_detail(row["detail"]) == {
            "from_state": expected_from[jid],
            "to_state": "cancelled",
        }, (
            f"state_change detail for {jid} must carry that job's ACTUAL prior status "
            f"({expected_from[jid]!r}) — a batched write that shares one detail would "
            f"corrupt the audit trail for the other status's rows"
        )
    observed = {parse_detail(r["detail"])["from_state"] for r in state_changes}
    assert observed == {"pending", "scheduled"}, (
        "the seed must produce both from_state values across the batches, or the "
        "per-row detail pin proves nothing"
    )
