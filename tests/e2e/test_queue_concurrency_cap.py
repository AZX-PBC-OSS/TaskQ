"""Fleet-wide queue concurrency cap e2e — ``max_concurrent`` on the queues
table caps total concurrent jobs for a queue across the fleet.

This module overrides the module-scoped ``e2e_schema`` fixture to insert an
``e2e_capped`` queue row with ``max_concurrent = 2`` after migration and set
``TASKQ_QUEUES=e2e,e2e_capped`` in the worker env. The worker reads
``max_concurrent`` at startup (``_bootstrap.py``), registers a
``ConcurrencyReservation`` for the queue, and ``dispatch.py`` transparently
prepends the queue-cap reservation name via ``_effective_reservations`` —
no actor-level ``reservations`` or ``rate_limits`` declaration needed.

The autouse ``clean_e2e_state`` fixture deletes all rows from
``reservation_slots`` (including the queue-cap slots created at worker
startup) between tests. The test re-creates them via ``ensure_slots``
before enqueuing so the cap is enforced.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from ._assertions import fetch_effects
from .actors import CappedWorkerPayload, capped_worker

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2EPg, E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_CAP = 2
_NUM_JOBS = 5


# ── Module-scoped schema override ─────────────────────────────────────────


@pytest_asyncio.fixture(scope="module")
async def e2e_schema(
    request: pytest.FixtureRequest,
    e2e_pg: E2EPg,
    e2e_dragonfly: E2EDragonfly,
) -> AsyncIterator[E2ESchema]:
    """Module-scoped PG schema + Dragonfly DB with ``e2e_capped`` queue (cap 2).

    Overrides the conftest's ``e2e_schema`` for this module only: after
    migration, inserts ``e2e_capped`` into the ``queues`` table with
    ``max_concurrent = 2``, and sets ``TASKQ_QUEUES=e2e,e2e_capped`` so
    the worker consumes both queues. The worker reads the cap at startup
    and registers a ``ConcurrencyReservation`` transparently.
    """
    import asyncpg

    from taskq.migrate import apply_pending_locked

    from .conftest import (
        _E2E_EFFECTS_DDL,
        E2ESchema,
        _e2e_schema_name,
        _flushdb,
        _next_redis_db,
    )

    schema = _e2e_schema_name(request)
    redis_db = _next_redis_db()

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    await apply_pending_locked(e2e_pg.host_dsn, schema=schema)

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(_E2E_EFFECTS_DDL.format(schema=schema))
        # Insert the e2e_capped queue with max_concurrent=2.
        # The worker's _bootstrap.py reads this at startup and registers
        # a ConcurrencyReservation via register_queue_cap_reservation().
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            "e2e_capped",
            _CAP,
        )
    finally:
        await conn.close()

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{redis_db}")

    worker_env = {
        "TASKQ_PG_DSN": e2e_pg.network_dsn,
        "TASKQ_REDIS_URL": f"{e2e_dragonfly.network_url}/{redis_db}",
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_QUEUES": "e2e,e2e_capped",
        "TASKQ_MIGRATE_ON_START": "false",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "3.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
        "TASKQ_TERMINATION_GRACE_PERIOD": "15.0",
    }

    yield E2ESchema(
        schema_name=schema,
        host_dsn=e2e_pg.host_dsn,
        worker_env=worker_env,
        redis_db=redis_db,
    )

    conn = await asyncpg.connect(e2e_pg.host_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


# ── Tests ─────────────────────────────────────────────────────────────────


async def test_queue_concurrency_cap_limits_parallelism(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """``max_concurrent=2`` on the queues table caps concurrent jobs at 2.

    Enqueues 5 ``capped_worker`` jobs (each sleeps 1.0s) on the
    ``e2e_capped`` queue. With a fleet-wide cap of 2, at most 2 jobs run
    simultaneously — the rest are snoozed with ``ReservationUnavailable``
    until a slot frees. The test computes the maximum observed concurrency
    from ``capped_started`` / ``capped_finished`` effect timestamps and
    asserts it never exceeds the cap (and reaches it, proving the cap is
    actually enforced, not just absent).

    The autouse ``clean_e2e_state`` fixture deletes all
    ``reservation_slots`` rows (including the queue-cap slots from worker
    startup) before the test runs. The test re-creates them via
    ``ensure_slots`` so the cap is enforced.
    """
    from taskq.ratelimit.registry import queue_concurrency_reservation_name
    from taskq.ratelimit.reservation import ConcurrencyReservation

    # Re-create the queue-cap reservation slots that clean_e2e_state
    # deleted. ensure_slots is idempotent (ON CONFLICT DO NOTHING), so
    # this is safe even if the rows somehow still exist.
    res_name = queue_concurrency_reservation_name("e2e_capped")
    reservation = ConcurrencyReservation(
        name=res_name,
        slots=_CAP,
        lease=timedelta(seconds=3.0),
        schema=e2e_schema.schema_name,
    )
    await reservation.ensure_slots(e2e_pg_pool)

    # Enqueue 5 jobs — each sleeps 1.0s. With cap=2, they run in 3 waves
    # (2 + 2 + 1), taking ~3s plus wake-tick delays.
    handles = [
        await e2e_client.enqueue(
            capped_worker,
            CappedWorkerPayload(run_id=run_id, job_index=i),
        )
        for i in range(_NUM_JOBS)
    ]

    await asyncio.gather(*(handle.wait(timeout=120) for handle in handles))

    # Fetch started and finished effects, paired by job_id.
    started = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="capped_started"
    )
    finished = await fetch_effects(
        e2e_pg_pool, e2e_schema.schema_name, run_id, kind="capped_finished"
    )
    assert len(started) == _NUM_JOBS
    assert len(finished) == _NUM_JOBS

    started_map = {row["job_id"]: row["at"] for row in started}
    finished_map = {row["job_id"]: row["at"] for row in finished}

    # Compute maximum concurrency via a sweep event line.
    # Each job contributes a +1 at its started timestamp and a -1 at its
    # finished timestamp. Sorting by (time, delta) ensures that when a
    # finish and start coincide, the finish (-1) is processed first.
    events: list[tuple[float, int]] = []
    for job_id, start_at in started_map.items():
        end_at = finished_map[job_id]
        events.append((start_at.timestamp(), 1))
        events.append((end_at.timestamp(), -1))
    events.sort(key=lambda e: (e[0], e[1]))

    max_concurrent = 0
    current = 0
    for _, delta in events:
        current += delta
        max_concurrent = max(max_concurrent, current)

    assert max_concurrent <= _CAP, (
        f"max concurrency {max_concurrent} exceeded queue cap {_CAP} — "
        f"the fleet-wide queue concurrency cap is not being enforced"
    )
    assert max_concurrent >= 2, (
        f"max concurrency {max_concurrent} < 2 — "
        f"the cap slots may not have been created (ensure_slots failed?)"
    )

    # Total completion time: with cap=2 and 5 jobs at 1.0s each, the
    # minimum is ceil(5/2) * 1.0s = 3.0s. Allow slack for wake-tick
    # scheduling; the key assertion is max_concurrent above.
    total_time = (max(finished_map.values()) - min(started_map.values())).total_seconds()
    assert total_time >= 2.0, (
        f"total completion time {total_time:.2f}s < 2.0s — "
        f"all {_NUM_JOBS} jobs ran in parallel (cap not enforced)"
    )
