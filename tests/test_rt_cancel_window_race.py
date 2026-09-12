"""The short-batch termination signal under EPQ contention (real PG).

The bounded drains terminate on a short batch: a batch that came in under
``batch_size`` means the eligible set is exhausted.  The termination
signal must be the WINDOW count — how many rows the driving statement's
MATERIALIZED ``matching`` CTE admitted — not the UPDATE's affected-row
count.  Under READ COMMITTED, a row windowed by the CTE that a dispatcher
claims (``pending→running``) between the statement's snapshot and the
UPDATE's row lock fails the EPQ re-check on the target and is dropped
from the affected count: the batch then reports fewer rows than its
window while matching rows remain beyond the window, and a drain that
terminates on the AFFECTED count abandons the tail of the match set
silently — for ``deregister_actor(force=True)`` the early break is
followed by deleting the ``actor_config`` row, stranding every
uncancelled pending job.

Deterministic construction (the only client-observable interposition
point for a single statement's snapshot→row-lock window): the claimer
connection takes the MIDDLE windowed job's row lock with an UNCOMMITTED
dispatch-claim UPDATE before the drain's first driving statement runs.
The statement's snapshot cannot see the uncommitted claim, so the window
still admits the claimed row; the UPDATE must lock that row to finish,
so the statement parks on the claimer's lock — provably between its
snapshot and its row lock (observed via ``pg_stat_activity``, the same
discipline as ``test_rt_cancel_deadlock.py``'s
``_wait_for_lock_waiter``).  The claimer's COMMIT then lands the race:
the parked UPDATE's EPQ re-check sees ``running`` and skips the row, so
the batch's affected count is one short of its window while a tail of
matching jobs waits beyond it.

Both tests assert the contract on the raced row, the tail, and the
totals: the claimed job ends exactly where the two-statement design
promises (running, cooperative cancel for ``_cancel_where``; untouched
by ``deregister_actor``, which refuses running jobs outright), every
OTHER matching row is still cancelled — the tail is not abandoned — and
the result totals count AFFECTED rows only (an EPQ-dropped row was not
cancelled and must not be counted as one).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import deregister_actor
from taskq.backend._cancel_bulk import _cancel_where
from taskq.backend._protocol import JobFilter
from taskq.backend._sql_templates import render
from taskq.testing.assertions import parse_detail
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = pytest.mark.integration

# A full window of 3 plus a tail of 4: the first batch's window is exactly
# full (so a drain keyed on the window count MUST continue), and the tail
# needs a second full batch plus a short one (so termination on the
# exhausted set is still exercised).
_BATCH = 3
_WINDOW = 3
_TAIL = 4
_TOTAL = _WINDOW + _TAIL


async def _seed_jobs(
    conn: asyncpg.Connection,
    schema: str,
    job_ids: Sequence[UUID],
    *,
    status: str,
    tags: Sequence[str],
) -> None:
    """Seed *job_ids* in one INSERT ... SELECT FROM unnest -- never row by row."""
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() in every caller; every user-supplied value goes through $N parameter binding.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, scheduled_at, tags) "
        f"SELECT id, 'rt_actor', 'default', '{{}}'::jsonb, $2::\"{schema}\".job_status, "
        "3, 'transient', clock_timestamp() - interval '10 seconds', $3::text[] "
        "FROM unnest($1::uuid[]) AS t(id)",
        list(job_ids),
        status,
        list(tags),
    )


async def _ids_in_drain_order(
    conn: asyncpg.Connection,
    schema: str,
    tag: str,
) -> list[UUID]:
    """The ids in the order the drain's ``ORDER BY id`` CTE admits them."""
    rows = await conn.fetch(
        f'SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] ORDER BY id',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above; the tag is $-bound.
        tag,
    )
    return [r["id"] for r in rows]


async def _wait_for_drain_parked_on_row_lock(
    conn: asyncpg.Connection,
    *,
    budget: float = 10.0,
) -> None:
    """Block until a driving cancel statement is parked on a row lock.

    The parked backend is identified by ``query LIKE '%matching AS
    MATERIALIZED%'`` (the driving statements' own shape) so a concurrent
    test's unrelated lock waiter can never satisfy this gate.  The
    claimer is ``idle in transaction`` while it holds the row lock, so it
    cannot be the match.
    """
    deadline = asyncio.get_running_loop().time() + budget
    while asyncio.get_running_loop().time() < deadline:
        parked: int = await conn.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND state = 'active' "
            "AND pid <> pg_backend_pid() "
            "AND query LIKE '%matching AS MATERIALIZED%'"
        )
        if parked:
            return
        await asyncio.sleep(0.02)
    pytest.fail("the drain's first driving statement never parked on the claimed row lock")


async def _claim_middle_window_job_uncommitted(
    pg_dsn: str,
    schema: str,
    claim_target: UUID,
) -> tuple[asyncpg.Connection, Any]:
    """Take the dispatch-claim row lock on *claim_target*, uncommitted.

    Returns ``(connection, transaction)``; the caller COMMITs the
    transaction once the drain's driving statement is parked on the lock
    — that commit is the race landing between the statement's snapshot
    (which saw the row still pending) and its UPDATE's EPQ re-check.
    """
    claimer = await asyncpg.connect(pg_dsn)
    claim_tx = claimer.transaction()
    await claim_tx.start()
    claim_tag = await claimer.execute(
        f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "SET status = 'running', locked_by_worker = $1, started_at = clock_timestamp() "
        "WHERE id = $2 AND status = 'pending'",
        new_uuid(),
        claim_target,
    )
    assert claim_tag == "UPDATE 1", "the claim must take a still-pending row"
    return claimer, claim_tx


async def test_short_batch_under_epq_contention_does_not_abandon_the_match_set_tail(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A full window whose middle row is claimed between the CTE snapshot
    and the UPDATE's row lock: the EPQ drop makes the batch's AFFECTED
    count short while the window was full — the drain must continue on
    the WINDOW count and cancel the whole tail, the claimed job must land
    in cooperative cancel via statement 2's fresh snapshot, and the
    totals must count only rows actually cancelled."""
    schema = module_pg_schema.schema_name
    render(schema)
    job_ids = [new_uuid() for _ in range(_TOTAL)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])
    drain_order = await _ids_in_drain_order(clean_pg_conn, schema, "bulk")
    claim_target = drain_order[1]  # the MIDDLE windowed job

    # The claimer takes the middle row's lock uncommitted BEFORE the drain
    # starts, so the first driving statement's snapshot still windows the
    # row and its UPDATE parks on the lock.
    claimer, claim_tx = await _claim_middle_window_job_uncommitted(
        module_pg_schema.pg_dsn, schema, claim_target
    )
    try:
        cancel_task = asyncio.create_task(
            _cancel_where(
                module_pg_pool,
                schema,
                render(schema),
                JobFilter(tags=("bulk",)),
                "offboard",
                batch_size=_BATCH,
            )
        )
        await _wait_for_drain_parked_on_row_lock(clean_pg_conn)

        # The commit lands the race: the parked UPDATE's EPQ re-check now
        # sees the claimed row as running and drops it from the affected
        # count — batch 1 reports 2 of a full 3-row window.
        await claim_tx.commit()
    finally:
        await claimer.close()
    result, notify_targets = await asyncio.wait_for(cancel_task, timeout=30)

    # The claimed job: never terminalised by the ps batch, caught by the
    # running drain's fresh snapshot — cooperative cancel, exactly one
    # cancel_request event, nothing else.
    claimed = await clean_pg_conn.fetchrow(
        f"SELECT status::text AS status, cancel_phase, cancel_requested_at, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = $1',
        claim_target,
    )
    assert claimed is not None
    assert claimed["status"] == "running", (
        "a job claimed inside the window must never be terminalised by the ps batch"
    )
    assert claimed["cancel_phase"] == 1, (
        "the running drain's fresh snapshot must catch the in-window claim"
    )
    assert claimed["cancel_requested_at"] is not None
    assert claimed["finished_at"] is None
    claimed_events = await clean_pg_conn.fetch(
        f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        claim_target,
    )
    assert [e["kind"] for e in claimed_events] == ["cancel_request"]
    assert parse_detail(claimed_events[0]["detail"]) == {"reason": "offboard"}

    # The tail is not abandoned: every other matching row is cancelled.
    other_ids = [jid for jid in drain_order if jid != claim_target]
    rows = await clean_pg_conn.fetch(
        f"SELECT id, status::text AS status, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
        other_ids,
    )
    assert {r["status"] for r in rows} == {"cancelled"}, (
        "an EPQ-dropped row in a full window must not stop the drain — every "
        "matching row beyond the window must still be cancelled"
    )
    assert all(r["finished_at"] is not None for r in rows)

    # Totals count AFFECTED rows only: the EPQ-dropped row was not
    # cancelled and must not be counted as one.
    assert result.cancelled_directly == _TOTAL - 1, (
        f"cancelled_directly={result.cancelled_directly} — the drain abandoned the "
        f"match-set tail after a short AFFECTED count on a full window"
    )
    assert set(result.cancelled_ids) == set(other_ids)
    assert result.cancel_requested == 1
    assert result.cancel_requested_ids == (claim_target,)

    # Exactly one event of each kind for every directly-cancelled row.
    per_job = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert all(r["n"] == 1 for r in per_job), "exactly one event of each kind per job"
    assert len(per_job) == 2 * (_TOTAL - 1) + 1

    # NOTIFY: exactly the claimed job, on its claiming worker.
    assert [t.job_id for t in notify_targets] == [claim_target]


async def test_deregister_claimed_in_window_row_does_not_stop_the_drain(
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The same race through ``deregister_actor(force=True)``: a claimed
    row inside a full first window must not break the drain before the
    tail is cancelled, and the ``actor_config`` delete (which follows the
    drain) must only happen once the whole match set is retired — the
    early break strands every uncancelled pending job against a deleted
    config row."""
    schema = module_pg_schema.schema_name
    render(schema)
    actor = "rt_window_actor"
    job_ids = [new_uuid() for _ in range(_TOTAL)]
    await _seed_jobs(clean_pg_conn, schema, job_ids, status="pending", tags=["bulk"])
    await clean_pg_conn.execute(
        f'UPDATE "{schema}".jobs SET actor = $1 WHERE id = ANY($2::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        actor,
        job_ids,
    )
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor=actor, max_concurrent=5, queue="default")],
        schema=schema,
    )
    drain_order = sorted(job_ids)
    claim_target = drain_order[1]  # the MIDDLE windowed job

    claimer, claim_tx = await _claim_middle_window_job_uncommitted(
        module_pg_schema.pg_dsn, schema, claim_target
    )
    try:
        async with module_pg_pool.acquire() as drain_conn:
            deregister_task = asyncio.create_task(
                deregister_actor(drain_conn, actor, force=True, schema=schema, batch_size=_BATCH)
            )
            await _wait_for_drain_parked_on_row_lock(clean_pg_conn)
            await claim_tx.commit()
            result = await asyncio.wait_for(deregister_task, timeout=30)
    finally:
        await claimer.close()

    # Totals count AFFECTED rows only; the full drain retires the tail.
    assert result.jobs_cancelled == _TOTAL - 1, (
        f"jobs_cancelled={result.jobs_cancelled} — the drain broke on a short "
        f"AFFECTED count over a full window and abandoned the match-set tail"
    )
    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == _TOTAL - 1, (
        "the claimed job is running, not terminal — only the drained rows count"
    )
    assert result.schedules_disabled == 0
    assert result.queue_purged is False

    # The claimed job: untouched — deregistration's force path refuses
    # running jobs, so the raced row stays running with no events (the
    # documented TOCTOU, not a silent running-job cancellation).
    claimed = await clean_pg_conn.fetchrow(
        f"SELECT status::text AS status, cancel_phase, finished_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        f'FROM "{schema}".jobs WHERE id = $1',
        claim_target,
    )
    assert claimed is not None
    assert claimed["status"] == "running"
    assert claimed["cancel_phase"] == 0
    assert claimed["finished_at"] is None
    claimed_events: int = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        claim_target,
    )
    assert claimed_events == 0, "no events may describe a job the drain never touched"

    # The tail: fully cancelled before the config row was deleted.
    other_ids = [jid for jid in drain_order if jid != claim_target]
    rows = await clean_pg_conn.fetch(
        f'SELECT id, status::text AS status FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        other_ids,
    )
    assert {r["status"] for r in rows} == {"cancelled"}, (
        "the actor_config row was deleted while pending jobs of the actor were "
        "still uncancelled — the drain broke early and stranded the tail"
    )

    # Exactly one state_change per drained row, each carrying its row's
    # real prior status and the deregistration reason.
    events = await clean_pg_conn.fetch(
        f'SELECT job_id, kind, count(*) AS n FROM "{schema}".job_events '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        "GROUP BY job_id, kind"
    )
    assert {r["kind"] for r in events} == {"state_change"}
    assert all(r["n"] == 1 for r in events)
    assert len(events) == _TOTAL - 1

    config_exists = await clean_pg_conn.fetchval(
        f'SELECT 1 FROM "{schema}".actor_config WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() above.
        actor,
    )
    assert config_exists is None, "the config row must be deleted after the full drain"
