# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the stranded-jobs detector's blind spots.

The detector's entire predicate is ``status IN ('pending','scheduled') AND
NOT EXISTS (actor_config row)`` (src/taskq/worker/_leader_sweeps.py:1228-1236)
— it keys on actor_config presence only. Two permanently-undispatchable
strand shapes are invisible to it:

* (i) a pending row WITH actor_config on a queue no worker serves: dispatch's
  candidates lateral annihilates it (``j2.queue = sq.queue_name``,
  src/taskq/backend/_dispatch_sql.py:229), and with schedule_to_close NULL
  the deadline sweep (sweep 2) cannot fail it either — the row is stranded
  forever while its actor_config row exists.
* (ii) the singleton amplifier: a pending singleton blocker whose
  actor_config was deleted blocks every later enqueue for that actor with
  SingletonCollisionError and retry_after=None (no schedule_to_close → no
  advisory hint; src/taskq/backend/_enqueue.py:550-574) — permanently,
  because the blocker itself is stranded by shape (ii)'s own rule.

The contract: every strand shape the fleet can accumulate must be visible to
the detector (or an equivalent signal).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import Mock
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import SingletonCollisionError
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _stranded_jobs_loop

pytestmark = pytest.mark.integration

_SHAPE_I_ACTOR = "orphan_actor_q"
_SHAPE_II_ACTOR = "solo_b"
_NO_WORKER_QUEUE = "no-worker-queue"


class _StubBackendDeps:
    """Duck-typed BackendDeps: settings + pools only (enqueue_with_conn path)."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def _pool_free_backend(schema: str) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    return PostgresBackend(
        _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; enqueue_with_conn reads only settings + the caller's conn.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


class _DetectorDeps:
    """Duck-typed WorkerDeps for _stranded_jobs_loop: settings, worker_pool,
    is_leader, liveness — the only fields the loop body touches."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool
        self.is_leader = asyncio.Event()
        self.is_leader.set()
        self.liveness = Mock()


async def _run_stranded_detector_once(
    pg_dsn: str, pool: asyncpg.Pool, schema: str, monkeypatch: pytest.MonkeyPatch
) -> dict[str, int]:
    """Run the REAL detector loop for a few fast ticks; return the last gauge."""
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_STRANDED_JOBS_INTERVAL": "0.05",
        },
        validate=False,
    )
    ctx = SweepContext(
        deps=_DetectorDeps(settings, pool),  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps carrying exactly the fields the loop reads.
        backend=None,  # type: ignore[arg-type]  # Why: the stranded loop never touches ctx.backend.
        clock=SystemClock(),
        worker_id=new_uuid(),
    )
    published: list[dict[str, int]] = []

    def _capture(data: dict[str, int]) -> None:
        published.append(dict(data))

    monkeypatch.setattr("taskq.worker._leader_sweeps.update_stranded_jobs_cache", _capture)

    shutdown = asyncio.Event()
    task = asyncio.create_task(_stranded_jobs_loop(ctx, shutdown))
    await asyncio.sleep(0.25)
    shutdown.set()
    await asyncio.wait_for(task, timeout=5.0)
    assert published, "detector loop never published a tick"
    return published[-1]


async def _seed_strand_shapes(conn: asyncpg.Connection, schema: str) -> tuple[UUID, UUID]:
    """Seed both strand shapes; return (shape_i_job_id, shape_ii_job_id).

    Shape (i): pending row, actor_config PRESENT, queue no worker serves,
    schedule_to_close NULL (invisible to the deadline sweep).
    Shape (ii): pending singleton row whose actor_config is then deleted,
    schedule_to_close NULL (so enqueue's retry_after is None forever).
    """
    await conn.executemany(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
        [
            (_SHAPE_I_ACTOR, "default"),
            (_SHAPE_II_ACTOR, "default"),
        ],
    )
    shape_i_job = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at) "
        "VALUES ($1, $2, $3, '{}'::jsonb, 3, 'transient', 'pending', clock_timestamp())",
        shape_i_job,
        _SHAPE_I_ACTOR,
        _NO_WORKER_QUEUE,
    )
    shape_ii_job = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at, metadata) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 3, 'transient', 'pending', clock_timestamp(), "
        "'{\"singleton\": true}'::jsonb)",
        shape_ii_job,
        _SHAPE_II_ACTOR,
    )
    await conn.execute(f'DELETE FROM "{schema}".actor_config WHERE actor = $1', _SHAPE_II_ACTOR)
    return shape_i_job, shape_ii_job


async def test_stranded_detector_sees_both_strand_shapes(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The detector must count BOTH strand shapes. Today it counts only the
    deleted-actor_config shape — a pending row WITH actor_config on an
    unserved queue is invisible to the gauge."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await _seed_strand_shapes(conn, schema)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        gauge = await _run_stranded_detector_once(pg_dsn, pool, schema, monkeypatch)

        assert _SHAPE_I_ACTOR in gauge and _SHAPE_II_ACTOR in gauge, (
            "Contract: the stranded-jobs detector must see every permanently-undispatchable "
            "pending row (or an equivalent signal must alarm on it). Current behavior violates "
            f"it: the detector published {gauge!r} — shape (i) (a pending row WITH actor_config "
            f"on queue {_NO_WORKER_QUEUE!r}, which no worker serves: dispatch's lateral "
            "annihilates it via `j2.queue = sq.queue_name`, "
            "src/taskq/backend/_dispatch_sql.py:229, and schedule_to_close NULL keeps the "
            "deadline sweep off it) is invisible; only shape (ii) (deleted actor_config) is "
            "counted."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_stranded_singleton_blocker_refuses_every_enqueue_forever(pg_dsn: str) -> None:
    """Amplifier evidence: a singleton blocker stranded in the no-exit pending
    cell makes every later enqueue for that actor raise SingletonCollisionError
    with retry_after=None — permanently, because nothing can exit the blocker.

    Pins today's permanent refusal: two enqueues, spaced apart, both refused
    with no advisory retry hint (schedule_to_close is NULL, so
    src/taskq/backend/_enqueue.py:550-574 computes retry_after=None).
    """
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        _, shape_ii_job = await _seed_strand_shapes(conn, schema)

        backend = _pool_free_backend(schema)
        args = make_enqueue_args(actor=_SHAPE_II_ACTOR, metadata={"singleton": True})

        refusals: list[tuple[UUID | None, object]] = []
        for _attempt in range(2):
            with pytest.raises(SingletonCollisionError) as excinfo:
                await backend.enqueue_with_conn(conn, args)
            refusals.append((excinfo.value.blocking_job_id, excinfo.value.retry_after))
            # Space the attempts: the blocker cannot exit between them — no
            # worker serves its actor (actor_config deleted), the deadline
            # sweep ignores it (schedule_to_close NULL), and it is not running.
            await asyncio.sleep(0.05)

        assert all(
            blocking == shape_ii_job and retry_after is None for blocking, retry_after in refusals
        ), (
            "Contract: a singleton refusal must not be permanent — the blocking row must be "
            "exitable, or the refusal must carry a retry hint. Current behavior violates it: "
            f"both enqueue attempts (spaced 50ms apart) were refused with blocking_job_id="
            f"{shape_ii_job!r} (the stranded blocker) and retry_after=None — the blocker sits "
            f"in the no-exit pending cell (actor_config deleted, schedule_to_close NULL), so "
            "every enqueue for this actor is refused forever with no advisory hint."
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
