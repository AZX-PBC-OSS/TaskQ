# ruff: noqa: S608  # Why: schema is a fixture identifier, validated by render()/the backend in every caller; every user-supplied value is $-bound.
"""Hunt-gap probe: the heartbeat's lease renewal vs the bulk-cancel drain.

The lock-order inventory's only UNPINNED blocking-x-blocking pair on the
jobs rows: ``heartbeat_jobs``' renewal is one multi-row UPDATE whose row
locks are taken in the driving index's heap order (the
locked_by_worker partial index; within one worker's key, insertion
order), while ``cancel_where``'s running arm takes its row locks in
ascending id order (per-id primary-key probes over the keyset page's
array, no SKIP LOCKED - a window that skips is a window that strands).
Opposite orders over one row set is the classic lock-order inversion:
the renewal holds a high-heap-position row and waits on a low id the
drain holds, the drain holds the low id and waits on a high one the
renewal holds, and PG's deadlock detector kills one side with 40P01.

Every other sweep arm rides FOR UPDATE SKIP LOCKED (the conflict is the
signal, no wait, no cycle) or is pinned against the drain already
(test_rt_cancel_sweep2_lock_order.py pins the deadline sweep); the
renewal cannot skip - a skipped row is a lease that lapses into a false
crash-reclaim - so this pair is the inventory's residue.

This probe seeds the inversion on purpose (rows INSERTed in DESCENDING
id order, so the renewal's heap scan runs opposite the drain's id
probes) and races the two public surfaces for several rounds. The
assertions are the observable contract:

* BOTH operations complete - neither parks unbounded behind the other;
* no lost update: every held row ends the round with cancel_phase = 1
  and a fresh lease, exactly one cancel_request event per row per
  round;
* NO 40P01 is raised to either caller - the renewal may not turn an
  operator's bulk cancel into heartbeat tick failures (three in a row
  isolate a healthy worker and crash-reclaim its fleet).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobFilter
from taskq.testing.fixtures import JobsApp, ModulePgSchema

pytestmark = pytest.mark.integration

_ROUNDS = 12
_JOBS = 60
_RACE_BOUND_SECS = 60.0


async def _seed_inverted_running_backlog(
    conn: asyncpg.Connection,
    schema: str,
    tag: str,
    worker_id: UUID,
    count: int,
) -> list[UUID]:
    """Seed *count* running jobs owned by *worker_id*, INSERTed in
    DESCENDING id order so the renewal's heap-ordered row locks run
    opposite the drain's ascending id probes."""
    ids = sorted((new_uuid() for _ in range(count)), reverse=True)
    await conn.executemany(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, "
        "locked_by_worker, lock_expires_at, started_at, tags) "
        "VALUES ($1, 'rt_actor', 'default', '{}'::jsonb, 'running', 3, 'transient', "
        "$2, clock_timestamp() + interval '30 seconds', clock_timestamp(), ARRAY[$3::text])",
        [(i, worker_id, tag) for i in ids],
    )
    return ids


async def _reset_round(conn: asyncpg.Connection, schema: str, tag: str, ids: list[UUID]) -> None:
    await conn.execute(
        f'UPDATE "{schema}".jobs SET cancel_phase = 0, cancel_requested_at = NULL '
        "WHERE id = ANY($1::uuid[])",
        ids,
    )
    await conn.execute(f'DELETE FROM "{schema}".job_events WHERE job_id = ANY($1::uuid[])', ids)


async def _round_states(
    conn: asyncpg.Connection, schema: str, ids: list[UUID]
) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in await conn.fetch(
            f"SELECT id, status::text AS status, cancel_phase, cancel_requested_at "
            f'IS NOT NULL AS requested FROM "{schema}".jobs '
            f"WHERE id = ANY($1::uuid[]) ORDER BY id",
            ids,
        )
    ]


@pytest.mark.parametrize("trial", range(3))
async def test_heartbeat_renewal_vs_bulk_cancel_running_arm_no_deadlock_no_lost_update(
    clean_pg_conn: asyncpg.Connection,
    module_jobs_app: JobsApp,
    module_pg_schema: ModulePgSchema,
    trial: int,
) -> None:
    """The inverted-order probe: both surfaces complete every round, the
    cancel request lands exactly once per row per round, and no deadlock
    reaches either caller."""
    schema = module_pg_schema.schema_name
    backend = module_jobs_app.backend
    tag = f"hbrevtx_trial{trial}"
    worker_id = new_uuid()
    ids = await _seed_inverted_running_backlog(clean_pg_conn, schema, tag, worker_id, _JOBS)
    assert len(ids) == _JOBS, "fixture broken: seeding"
    flt = JobFilter(tags=(tag,))
    deadlocks: list[tuple[str, str | None]] = []

    for round_no in range(_ROUNDS):
        await _reset_round(clean_pg_conn, schema, tag, ids)

        # Production renews every beat while an operator drain runs, so the
        # probe renews in a loop across the drain's WHOLE paging window,
        # not once at the start.
        renewals: list[int] = []
        stop_renewals = asyncio.Event()

        async def _renew_loop(
            renewals: list[int] = renewals,
            stop_renewals: asyncio.Event = stop_renewals,
        ) -> None:
            while not stop_renewals.is_set():
                try:
                    renewed = await backend.heartbeat_jobs(worker_id, timedelta(seconds=30))
                except asyncpg.PostgresError as exc:
                    deadlocks.append(("heartbeat_jobs", getattr(exc, "sqlstate", None)))
                    raise
                renewals.append(renewed)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop_renewals.wait(), timeout=0.02)

        hb_task = asyncio.create_task(_renew_loop())
        cancel_task = asyncio.create_task(backend.cancel_where(flt, "offboard"))
        try:
            await asyncio.wait_for(cancel_task, timeout=_RACE_BOUND_SECS)
        except asyncpg.PostgresError as exc:
            deadlocks.append(("cancel_where", getattr(exc, "sqlstate", None)))
            raise
        stop_renewals.set()
        await asyncio.wait_for(hb_task, timeout=_RACE_BOUND_SECS)

        for renewed in renewals:
            assert renewed == _JOBS, (
                f"round {round_no}: the renewal must stamp every held row (got {renewed}/{_JOBS})"
            )
        assert renewals, f"round {round_no}: the renewal never ran"

        states = await _round_states(clean_pg_conn, schema, ids)
        for row in states:
            assert row["status"] == "running", (
                f"round {round_no}: job {row['id']} left running, found "
                f"{row['status']!r} - a lost lease or a lost cancel must "
                "not crash-reclaim a live row"
            )
            assert row["cancel_phase"] == 1, (
                f"round {round_no}: job {row['id']} has cancel_phase "
                f"{row['cancel_phase']} - the drain's request was lost"
            )
            assert row["requested"], (
                f"round {round_no}: job {row['id']} carries no cancel_requested_at"
            )
        events = await clean_pg_conn.fetch(
            f'SELECT job_id, count(*)::int AS n FROM "{schema}".job_events '
            "WHERE job_id = ANY($1::uuid[]) AND kind = 'cancel_request' "
            "GROUP BY job_id",
            ids,
        )
        assert len(events) == _JOBS, (
            f"round {round_no}: {_JOBS - len(events)} rows carry no cancel_request event"
        )
        assert all(r["n"] == 1 for r in events), (
            f"round {round_no}: a cancel_request event was double-applied"
        )

    assert not deadlocks, (
        f"REALIZED DEADLOCK between the heartbeat lease renewal and the "
        f"bulk-cancel running arm under inverted row orders: {deadlocks} - "
        "a 40P01 here is a heartbeat tick failure; three in a row isolate "
        "a healthy worker and crash-reclaim its fleet mid-cancel"
    )
