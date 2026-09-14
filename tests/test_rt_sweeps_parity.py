"""Red-team attacks on sweep-1 parity between Postgres and the in-memory twin.

The parity contract for the bounded sweeps is observable outcome, not
row order: for the same seeded corpus (same eligibility, same branch
mix), repeated calls with the same cap drain the same TOTAL rows and
leave the same per-row state and audit trail on both backends.

Sweep 1 is the richest unpinned surface: a three-way CASE
(pending/cancelled/crashed), an attempt row, an event row, and lock
bookkeeping.  ``_SWEEP_1_SQL`` clears ``locked_by_worker`` and
``lock_expires_at`` on EVERY branch (one SET clause list, all rows) —
the in-memory twin's terminal branch must match, because
``JobRow.locked_by_worker`` is observable through ``get()`` and a stale
holder id on a terminal row is exactly the kind of divergence a
cross-backend test would silently bake in.

The second parity attack is the parameter boundary: PG rejects invalid
``batch_size`` at the boundary (pinned in ``test_rt_sweeps_boundary.py``),
so the twin must reject it identically — today it silently returns 0,
which is a third behaviour for the same input.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import CancelPhase, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args

pytestmark = pytest.mark.integration

_GRACE = timedelta(seconds=30)
# The cancel carve-out admits a cancel-in-flight job only once the lock
# has been expired past cancel_grace + cleanup_grace + a flat 60 s.
_CANCEL_LOCK_EXPIRED_AGO = timedelta(seconds=130)
_PLAIN_LOCK_EXPIRED_AGO = timedelta(seconds=10)
_STARTED_AGO = timedelta(seconds=30)
_CAP = 2

_START = datetime(2025, 1, 1, tzinfo=UTC)

# Per-branch corpus: (branch name, max_attempts, cancel_phase).
_BRANCHES: list[tuple[str, int, int]] = [
    ("retry", 3, 0),
    ("cancel", 1, 1),
    ("crash", 1, 0),
]
_PER_BRANCH = 2


def _expected_status(branch: str) -> str:
    return {"retry": "pending", "cancel": "cancelled", "crash": "crashed"}[branch]


# ── Postgres side ────────────────────────────────────────────────────────


async def _seed_pg(conn: asyncpg.Connection, schema: str, holder: UUID) -> dict[str, list[UUID]]:
    """Seed the three-branch corpus on Postgres in one round trip."""
    ids_by_branch: dict[str, list[UUID]] = {b: [] for b, _, _ in _BRANCHES}
    all_ids: list[UUID] = []
    branch_of: dict[UUID, str] = {}
    expires_ago: list[float] = []
    max_attempts: list[int] = []
    phases: list[int] = []
    for branch, attempts, phase in _BRANCHES:
        for _ in range(_PER_BRANCH):
            jid = new_uuid()
            ids_by_branch[branch].append(jid)
            all_ids.append(jid)
            branch_of[jid] = branch
            max_attempts.append(attempts)
            phases.append(phase)
            expires_ago.append(
                _CANCEL_LOCK_EXPIRED_AGO.total_seconds()
                if phase
                else _PLAIN_LOCK_EXPIRED_AGO.total_seconds()
            )
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "VALUES ($1, 'test-host', 12345, ARRAY['default'])",
        holder,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "(id, actor, queue, payload, status, max_attempts, retry_kind, attempt, "
        " scheduled_at, locked_by_worker, lock_expires_at, started_at, cancel_phase, "
        " cancel_requested_at) "
        "SELECT t.id, 'test_actor', 'default', '{}'::jsonb, 'running', "
        "t.max_attempts, 'transient', 1, clock_timestamp(), $2, "
        "clock_timestamp() - (t.expired_ago * interval '1 second'), "
        f"clock_timestamp() - interval '{int(_STARTED_AGO.total_seconds())} seconds', "
        "t.phase, CASE WHEN t.phase = 0 THEN NULL ELSE clock_timestamp() END "
        "FROM unnest($1::uuid[], $3::int[], $4::int[], $5::float8[]) "
        "    AS t(id, max_attempts, phase, expired_ago)",
        all_ids,
        holder,
        max_attempts,
        phases,
        expires_ago,
    )
    return ids_by_branch


async def _pg_observable(conn: asyncpg.Connection, schema: str, job_id: UUID) -> dict[str, Any]:
    """The per-job observable surface, read the way a client would.

    ``detail`` arrives as a raw ``str`` — ``clean_pg_conn`` registers no
    jsonb codec — so it is decoded here (the same defensive decode the
    sweep contract files use) before comparison.
    """
    job = await conn.fetchrow(
        f"SELECT status::text, locked_by_worker, lock_expires_at "  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        f'FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )
    attempts = await conn.fetch(
        f'SELECT outcome, error_class, worker_id FROM "{schema}".job_attempts '  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        "WHERE job_id = $1",
        job_id,
    )
    events = await conn.fetch(
        f'SELECT kind, detail FROM "{schema}".job_events WHERE job_id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier, validated by render() upstream.
        job_id,
    )
    assert job is not None
    decoded_events = [
        (e["kind"], json.loads(e["detail"]) if isinstance(e["detail"], str) else e["detail"])
        for e in events
    ]
    return {
        "status": job["status"],
        "holder_cleared": job["locked_by_worker"] is None,
        "lock_expiry_cleared": job["lock_expires_at"] is None,
        "attempts": [(a["outcome"], a["error_class"], a["worker_id"]) for a in attempts],
        "events": decoded_events,
    }


# ── In-memory side ───────────────────────────────────────────────────────


def _make_memory_backend() -> InMemoryBackend:
    return InMemoryBackend(
        clock=FakeClock(_START),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


async def _seed_memory(memory: InMemoryBackend, holder: UUID) -> dict[str, list[JobId]]:
    """Seed the same three-branch corpus on the in-memory backend."""
    ids_by_branch: dict[str, list[JobId]] = {b: [] for b, _, _ in _BRANCHES}
    for branch, max_attempts, phase in _BRANCHES:
        for _ in range(_PER_BRANCH):
            args = make_enqueue_args(scheduled_at=_START, max_attempts=max_attempts)
            row = await memory.enqueue(args)
            expired_ago = _CANCEL_LOCK_EXPIRED_AGO if phase else _PLAIN_LOCK_EXPIRED_AGO
            running = replace(
                row,
                status="running",
                attempt=1,
                locked_by_worker=holder,
                lock_expires_at=_START - expired_ago,
                started_at=_START - _STARTED_AGO,
                cancel_phase=CancelPhase(phase),
                cancel_requested_at=_START if phase else None,
            )
            memory._jobs[args.id] = running  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access to set up the running-row fixture, the established seeding pattern.
            ids_by_branch[branch].append(args.id)
    return ids_by_branch


async def _memory_observable(memory: InMemoryBackend, job_id: JobId) -> dict[str, Any]:
    """The same observable surface through the twin's public reads."""
    row = await memory.get(job_id)
    assert row is not None
    attempts = await memory.get_attempts(job_id)
    events = await memory.get_events(job_id)
    return {
        "status": row.status,
        "holder_cleared": row.locked_by_worker is None,
        "lock_expiry_cleared": row.lock_expires_at is None,
        "attempts": [(a.outcome, a.error_class, a.worker_id) for a in attempts],
        "events": [(e.kind, e.detail) for e in events],
    }


def _normalize(obs: dict[str, Any], holder: UUID, expected_status: str) -> dict[str, Any]:
    """Project both backends' observables onto the parity contract.

    The event ``detail`` carries the holder as a string on PG (jsonb) and
    as a UUID in memory — a representation difference, not a behavioural
    one, so the holder is compared as ``str()`` on both sides.  Each
    backend was seeded with its OWN holder id, so the holder identity is
    normalized to a sentinel: parity is about the bookkeeping being
    cleared and consistently recorded, not about two backends sharing a
    uuid.
    """
    holder_token = "HOLDER"

    def _as_holder(value: object) -> object:
        return holder_token if str(value) == str(holder) else value

    events = [
        (
            kind,
            detail.get("from_state"),
            detail.get("to_state"),
            detail.get("reason"),
            _as_holder(detail.get("worker_id")) if "worker_id" in detail else None,
        )
        for kind, detail in obs["events"]
    ]
    return {
        "status": obs["status"],
        "holder_cleared": obs["holder_cleared"],
        "lock_expiry_cleared": obs["lock_expiry_cleared"],
        "attempts": [
            (outcome, error_class, _as_holder(worker_id))
            for outcome, error_class, worker_id in obs["attempts"]
        ],
        "events": events,
        "expected_status": expected_status,
    }


async def test_sweep1_three_way_drain_parity_under_a_cap(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Both backends must leave identical per-row state for the same corpus.

    Every branch of the three-way CASE, more rows per branch than the cap
    (so the drain spans several committed batches on both sides), drained
    to completion, then compared per job: status, cleared lock holder and
    expiry, exactly one attempt row with the crash outcome, exactly one
    state_change event with the reclaim reason.
    """
    schema = module_pg_schema.schema_name
    pg_holder = new_uuid()
    memory_holder = new_uuid()

    pg_ids = await _seed_pg(clean_pg_conn, schema, pg_holder)
    memory = _make_memory_backend()
    mem_ids = await _seed_memory(memory, memory_holder)

    # ── drain both ──────────────────────────────────────────────────
    pg_calls = 0
    while pg_calls < 50:
        n = await PostgresBackend.sweep_expired_locks(
            clean_pg_conn,  # type: ignore[arg-type]  # Why: asyncpg.Connection satisfies ConnLike.
            _GRACE,
            _GRACE,
            schema=schema,
            batch_size=_CAP,
        )
        pg_calls += 1
        if n == 0:
            break
        assert n <= _CAP, f"a capped PG call reclaimed {n} rows, cap was {_CAP}"

    mem_calls = 0
    while mem_calls < 50:
        n = await memory.reclaim_expired_locks(_GRACE, _GRACE, batch_size=_CAP)
        mem_calls += 1
        if n == 0:
            break
        assert n <= _CAP, f"a capped in-memory call reclaimed {n} rows, cap was {_CAP}"

    # ── compare per branch, per job ─────────────────────────────────
    for branch, _, _ in _BRANCHES:
        expected = _expected_status(branch)
        for pg_id, mem_id in zip(pg_ids[branch], mem_ids[branch], strict=True):
            pg_obs = _normalize(
                await _pg_observable(clean_pg_conn, schema, pg_id), pg_holder, expected
            )
            mem_obs = _normalize(await _memory_observable(memory, mem_id), memory_holder, expected)
            assert mem_obs == pg_obs, (
                f"{branch} branch diverged between backends:\n"
                f"  PG   : {pg_obs}\n  memory: {mem_obs}\n"
                "the twin must mirror the Postgres sweep's observable outcome"
            )
            assert pg_obs["status"] == expected, (
                f"{branch} branch must land on {expected!r} on both backends"
            )
            assert pg_obs["holder_cleared"] and pg_obs["lock_expiry_cleared"], (
                f"{branch} branch must clear the lock holder and expiry on PG"
            )
            assert pg_obs["attempts"] == [("crashed", "WorkerCrashed", "HOLDER")], (
                f"{branch} branch attempt shape on PG: {pg_obs['attempts']}"
            )
            assert pg_obs["events"] == [
                ("state_change", "running", expected, "lock_expired", "HOLDER")
            ], f"{branch} branch event shape on PG: {pg_obs['events']}"

    # Drain progress parity: same corpus size, same cap, same total.
    assert sum(len(v) for v in pg_ids.values()) == len(_BRANCHES) * _PER_BRANCH
    assert sum(len(v) for v in mem_ids.values()) == len(_BRANCHES) * _PER_BRANCH


@pytest.mark.parametrize("bad_batch_size", [0, -1])
async def test_in_memory_twins_reject_invalid_batch_size_like_pg(
    bad_batch_size: int,
) -> None:
    """The twins must reject a degenerate cap as loudly as Postgres does.

    Today the twins return 0 silently for any non-positive cap (the
    count guard breaks immediately), while Postgres rejects the value at
    the boundary — three behaviours for one input across two backends
    that claim parity.
    """
    memory = _make_memory_backend()

    with pytest.raises(ValueError, match="batch_size"):
        await memory.scheduled_to_pending(batch_size=bad_batch_size)
    with pytest.raises(ValueError, match="batch_size"):
        await memory.deadline_sweep(batch_size=bad_batch_size)
    with pytest.raises(ValueError, match="batch_size"):
        await memory.reclaim_expired_locks(_GRACE, _GRACE, batch_size=bad_batch_size)


async def test_memory_sweep1_drain_leaves_no_orphan_running_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """After a full in-memory drain, no row may still claim a holder.

    The production consumer of a cleared holder is every query that
    scopes by ``locked_by_worker`` (cancel polling, heartbeat, slot
    release): a terminal row still pointing at a dead worker keeps
    matching those scopes.  This is the standalone, backend-side
    statement of the parity attack above — it fails on the twin today.
    """
    memory = _make_memory_backend()
    holder = new_uuid()
    await _seed_memory(memory, holder)

    drained = 0
    for _ in range(50):
        n = await memory.reclaim_expired_locks(_GRACE, _GRACE, batch_size=_CAP)
        if n == 0:
            break
        drained += n
    assert drained == len(_BRANCHES) * _PER_BRANCH, "the twin must drain the corpus"

    stale: list[JobId] = []
    for job_id in memory._jobs:  # pyright: ignore[reportPrivateUsage]  # Why: test-only private access; JobRow.locked_by_worker is the field under attack.
        row = await memory.get(job_id)
        assert row is not None
        if row.status in ("crashed", "cancelled", "pending") and (
            row.locked_by_worker is not None or row.lock_expires_at is not None
        ):
            stale.append(job_id)
    assert not stale, (
        f"{len(stale)} swept job(s) still carry a lock holder/expiry after the "
        "drain — the twin's terminal branch does not clear lock bookkeeping "
        "the Postgres sweep clears on every branch"
    )
