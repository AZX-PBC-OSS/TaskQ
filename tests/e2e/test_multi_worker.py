"""Multi-worker e2e — two worker containers share one queue with no double execution.

Design spec scenario row (docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
second worker container on the same queue; 30 jobs with small latency → all
succeed; each job_id appears exactly once in effects; both workers
participate. Ground truth: ``e2e_effects`` distinctness + per-attempt
worker attribution.

Worker attribution is asserted on ``{schema}.job_attempts.worker_id``, NOT
``{schema}.jobs.locked_by_worker``: every terminal write clears the lock
column (``locked_by_worker = NULL`` in ``mark_succeeded`` / ``mark_failed``
— backend/_sql_templates.py), while the terminal paths persist the executing
worker into ``job_attempts`` via ``_insert_attempt``
(backend/_terminal.py). ``job_attempts.worker_id`` has FK
``ON DELETE SET NULL`` to ``workers(id)``; worker rows live for the module's
lifetime, so the column is non-NULL for this run's attempts.

Why a single-worker sweep cannot legitimately happen here: each jobs INSERT
fires a NOTIFY that wakes BOTH workers; every dispatch CTE claims at most
``max_concurrency`` (settings default 8 — e2e env does not override) rows
via ``FOR UPDATE SKIP LOCKED``; one worker sweeping all 30 jobs needs ≥4
sequential claim rounds at ≥0.1s of in-actor latency each (~0.4s+), while
the second worker's first claim lands within milliseconds of the first
enqueue. Both workers claiming a disjoint subset is therefore near-certain.
``_JOB_COUNT`` was raised from 20 to 30 for more sweep margin under extreme
Docker CPU starvation (F8); if this is ever observed flaky, raise it
further — do NOT weaken the assertion.

Every test requests ``e2e_worker`` explicitly: the worker container fixture
is not autouse, so no worker (and no dispatch) exists unless a test pulls it
in.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from ._assertions import fetch_effects, poll_until
from .actors import WelcomeEmailPayload, send_welcome_email
from .conftest import E2EWorker, _container_logs, _stop_container

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

_JOB_COUNT = 30


@pytest_asyncio.fixture
async def e2e_worker_second(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
    e2e_worker: E2EWorker,
) -> AsyncIterator[E2EWorker]:
    """Second worker container on the same queue/schema as ``e2e_worker``.

    Mirrors the module-scoped ``e2e_worker`` fixture body (same image tag,
    same ``TASKQ_*`` env dict, same readiness-gate pattern) with network
    alias ``worker2-<schema>``. Depends on ``e2e_worker`` so the readiness
    gate is meaningful: poll ``{schema}.workers`` for ≥2 rows with a fresh
    ``last_seen_at`` — the single-worker gate (``wait_for_worker_ready``)
    would pass trivially on the primary worker's heartbeat.

    The gate additionally requires a **post-register** heartbeat
    (``last_seen_at > started_at``): the registration INSERT leaves both
    columns at the same ``now()`` default, so strict inequality proves the
    worker's heartbeat loop ticked at least once after registering
    (``UPDATE ... SET last_seen_at = clock_timestamp()``, heartbeat interval
    0.5s in the e2e env) — i.e. the worker is fully live, not merely
    inserted. A worker stuck between register and the first heartbeat can
    never satisfy this gate.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_network(e2e_network).with_network_aliases(f"worker2-{e2e_schema.schema_name}")
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)

    await asyncio.to_thread(container.start)
    try:

        async def _both_workers_heartbeating() -> bool:
            count = await e2e_pg_pool.fetchval(
                f"""
                SELECT count(*) FROM "{e2e_schema.schema_name}".workers
                WHERE last_seen_at > now() - interval '10 seconds'
                  AND last_seen_at > started_at
                """
            )
            return count >= 2

        try:
            await poll_until(
                _both_workers_heartbeating,
                timeout=30.0,
                description=(
                    f"2 fresh post-register worker heartbeats in {e2e_schema.schema_name}.workers"
                ),
            )
        except TimeoutError:
            logs = _container_logs(container)
            msg = (
                "second e2e worker failed readiness gate: <2 fresh post-register "
                f"heartbeats in {e2e_schema.schema_name}.workers within 30s\n{logs}"
            )
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


async def test_two_workers_share_queue_no_double_execution(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_worker_second: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """30 jobs across two containers: exactly-once execution + shared work.

    (a) Exactly-once: ``handle.wait()`` raises on any non-success terminal
    state, so a clean ``gather`` proves all 30 succeeded; 30 "send" effects
    whose job_ids equal the enqueued set proves no job ran twice (SKIP
    LOCKED across the container boundary — a duplicate execution would show
    as either 31 rows or 30 rows over 29 distinct job_ids).

    (b) Distribution: ``SELECT DISTINCT worker_id`` over this run's
    ``job_attempts`` rows must name ≥2 workers (see module docstring for why
    ``job_attempts.worker_id`` is the durable attribution column and why a
    single-worker sweep is not a legitimate outcome here).
    """
    handles = [
        await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(
                run_id=run_id,
                user_id=f"u-{i:02d}",
                email=f"u-{i:02d}@example.com",
            ),
        )
        for i in range(_JOB_COUNT)
    ]

    await asyncio.gather(*(handle.wait(timeout=90) for handle in handles))

    rows = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="send")
    assert len(rows) == _JOB_COUNT
    effect_job_ids = {row["job_id"] for row in rows}
    assert len(effect_job_ids) == _JOB_COUNT
    assert effect_job_ids == {handle.job_id for handle in handles}

    attempt_rows = await e2e_pg_pool.fetch(
        f"""
        SELECT DISTINCT worker_id
        FROM "{e2e_schema.schema_name}".job_attempts
        WHERE job_id = ANY($1::uuid[])
        """,
        [handle.job_id for handle in handles],
    )
    worker_ids = {row["worker_id"] for row in attempt_rows}
    assert len(worker_ids) >= 2
