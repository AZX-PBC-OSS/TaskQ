"""Cron e2e — schedule registration and real cross-container firing.

A dedicated worker container (``TASKQ_E2E_CRON=1``) registers a
``* * * * *`` schedule at startup; the leader's cron loop enqueues the
job, the container dispatches it, and the actor records a ``cron-tick``
effect. The schedule fires at the next minute boundary, so the poll
budget is 150 s (60 s boundary + dispatch latency + Docker starvation
headroom).

The cron env flag is scoped to this module's container: a once-a-minute
job in every e2e worker would break the other modules' idle gates.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from taskq.testing._shared_containers import creator_labels

from ._assertions import poll_until, wait_for_effects, wait_for_worker_ready
from .conftest import E2EWorker, _container_logs, _stop_container

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


@pytest_asyncio.fixture
async def cron_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
) -> AsyncIterator[E2EWorker]:
    """Dedicated worker container carrying the cron registration."""
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    container.with_network(e2e_network).with_network_aliases(
        f"worker-cron-{e2e_schema.schema_name}"
    )
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)
    container.with_env("TASKQ_E2E_CRON", "1")

    await asyncio.to_thread(container.start)
    try:
        try:
            await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)
        except TimeoutError:
            logs = _container_logs(container)
            msg = f"cron e2e worker failed readiness gate\n{logs}"
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


async def test_cron_schedule_registers_fires_and_completes(
    cron_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
) -> None:
    """The schedule row exists, the cron fires, and the job runs to success."""
    schema = e2e_schema.schema_name

    row = await e2e_pg_pool.fetchrow(
        f'SELECT actor, cron_expr, enabled FROM "{schema}".cron_schedules WHERE actor = $1',
        "cron_heartbeat",
    )
    assert row is not None, "worker startup did not register the cron schedule"
    assert row["cron_expr"] == "* * * * *"
    assert row["enabled"] is True

    await wait_for_effects(
        e2e_pg_pool,
        schema,
        "cron-static",
        kind="cron-tick",
        min_count=1,
        timeout=150.0,
    )

    status: str | None = None

    async def _job_succeeded() -> bool:
        nonlocal status
        status = await e2e_pg_pool.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE actor = $1 '
            "ORDER BY created_at DESC LIMIT 1",
            "cron_heartbeat",
        )
        return status == "succeeded"

    # The cron-tick effect lands at enqueue; the job itself still has to be
    # dispatched and run to completion — wait for it rather than asserting
    # whatever state a single immediate read happens to catch.
    await poll_until(
        _job_succeeded,
        timeout=60.0,
        description="cron_heartbeat job to reach status='succeeded'",
    )
    assert status == "succeeded"

    # Exactly-once per fire: the leader tick holds a transaction-scoped
    # advisory lock (tick_cron) so a schedule cannot double-enqueue within
    # its minute window. Effects arrive one per fire; the first window must
    # have exactly one.
    rows = await e2e_pg_pool.fetch(
        f'SELECT job_id FROM "{schema}".e2e_effects '
        "WHERE kind = 'cron-tick' AND detail->>'run_id' = 'cron-static'",
    )
    assert len(rows) == 1, f"expected exactly one cron fire in the window, got {len(rows)}"
