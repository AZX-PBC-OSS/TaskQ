"""Actor-level max_concurrent e2e — ``max_concurrent=2`` on an actor caps
per-actor parallelism via the dispatch SQL's ``per_actor_capacity`` CTE.

The ``concurrent_tracked_worker`` actor (actors.py) declares
``max_concurrent=2``. The worker's bootstrap syncs this to the
``actor_config`` table at startup (``_bootstrap.py:380-399``). The dispatch
SQL (``_dispatch_sql.py:71-90``) computes ``residual = max_concurrent -
in_flight`` per actor and only admits jobs when ``residual > 0``, so at
most 2 jobs for this actor run simultaneously.

Unlike the queue-level concurrency cap (``test_queue_concurrency_cap.py``),
no ``ConcurrencyReservation`` or ``reservation_slots`` table is involved:
the cap is enforced purely in the dispatch SQL's ``per_actor_capacity``
and ``eligible_candidates`` CTEs. No module-scoped schema override is
needed — the actor declaration is sufficient.

The test follows the same event-line sweep pattern as
``test_queue_concurrency_cap.py``: enqueue 5 jobs (each sleeps 1.0 s),
wait for all to complete, compute max concurrency from ``ct_started`` /
``ct_finished`` effect timestamps, and assert it never exceeds 2 (and
reaches it, proving the cap is actually enforced, not just absent).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from ._assertions import fetch_effects, wait_all
from .actors import ConcurrentTrackedPayload, concurrent_tracked_worker

if TYPE_CHECKING:
    import asyncpg

    from taskq import TaskQ

    from .conftest import E2ESchema, E2EWorker

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_CAP = 2
_NUM_JOBS = 5


async def test_actor_max_concurrent_limits_parallelism(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """``max_concurrent=2`` on an actor caps concurrent jobs at 2.

    Enqueues 5 ``concurrent_tracked_worker`` jobs (each sleeps 1.0 s).
    With ``max_concurrent=2``, the dispatch SQL's ``per_actor_capacity``
    CTE limits admission to 2 concurrent jobs for this actor — the rest
    stay ``pending`` until a running job finishes and frees a slot. The
    test computes the maximum observed concurrency from ``ct_started`` /
    ``ct_finished`` effect timestamps and asserts it never exceeds the
    cap (and reaches it, proving the cap is actually enforced).
    """
    handles = [
        await e2e_client.enqueue(
            concurrent_tracked_worker,
            ConcurrentTrackedPayload(run_id=run_id, job_index=i),
        )
        for i in range(_NUM_JOBS)
    ]

    await wait_all(handles, timeout=120)

    started = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="ct_started")
    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="ct_finished")
    assert len(started) == _NUM_JOBS
    assert len(finished) == _NUM_JOBS

    started_map = {row["job_id"]: row["at"] for row in started}
    finished_map = {row["job_id"]: row["at"] for row in finished}

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
        f"max concurrency {max_concurrent} exceeded actor max_concurrent {_CAP} — "
        f"the actor-level concurrency cap is not being enforced"
    )
    assert max_concurrent >= 2, (
        f"max concurrency {max_concurrent} < 2 — "
        f"the cap may not have been applied (actor_config not synced?)"
    )

    total_time = (max(finished_map.values()) - min(started_map.values())).total_seconds()
    assert total_time >= 2.0, (
        f"total completion time {total_time:.2f}s < 2.0s — "
        f"all {_NUM_JOBS} jobs ran in parallel (cap not enforced)"
    )
