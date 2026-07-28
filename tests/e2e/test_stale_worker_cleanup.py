"""Stale worker cleanup e2e — dead worker's row removed by leader sweep.

Design spec scenario row
(docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
kill a worker, verify ``cleanup_stale_workers`` sweep removes its row
from the ``workers`` table while the surviving worker's row remains.

The ``cleanup_stale_workers`` function (``_leader_shared.py``) deletes
worker rows whose ``last_seen_at`` exceeds the staleness threshold
``heartbeat_interval * (max_heartbeat_failures + 3)``.  With the e2e
env (``heartbeat_interval=0.5``, ``max_heartbeat_failures=3`` default)
the threshold is 3.0 s.  The sweep runs on the leader's sweep loop
cadence (``_sweep_loop`` in ``_leader_sweeps.py``, 2 s interval in e2e).

After SIGKILL the worker stops heartbeating immediately.  Within 3 s
its ``last_seen_at`` is stale; the next sweep tick removes the row.
Worst case: 3 s (staleness) + 2 s (sweep interval in e2e) ≈ 5 s.  The 120 s
poll timeout accommodates leader re-election and Docker starvation.

The autouse ``clean_e2e_state`` fixture is overridden because a worker
container is intentionally killed mid-test.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from ._assertions import poll_until
from .conftest import (
    _DELETE_ORDER,
    E2EWorker,
    _container_logs,
    _flushdb,
    _stop_container,
)

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

# staleness (3 s) + sweep interval (2 s in e2e) + leader re-election buffer.
_CLEANUP_TIMEOUT = 120.0


# ── Module-local clean_e2e_state override ─────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-killed workers.

    Skips ``_raise_if_worker_crashed`` (a worker is SIGKILL'd mid-test)
    and tolerates idle-gate timeout.
    """
    if not {"e2e_client", "e2e_pg_pool", "e2e_worker", "e2e_schema"}.intersection(
        request.fixturenames
    ):
        yield
        return

    e2e_schema: E2ESchema = request.getfixturevalue("e2e_schema")
    e2e_pg_pool: asyncpg.Pool = request.getfixturevalue("e2e_pg_pool")
    e2e_dragonfly: E2EDragonfly = request.getfixturevalue("e2e_dragonfly")
    schema = e2e_schema.schema_name

    async def _no_running_jobs() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE status = $1',
            "running",
        )
        return count == 0

    with contextlib.suppress(TimeoutError):
        await poll_until(
            _no_running_jobs,
            timeout=30.0,
            description=f"idle gate: zero running jobs in {schema}.jobs",
        )

    async with e2e_pg_pool.acquire() as conn:
        for table in _DELETE_ORDER:
            await conn.execute(f'DELETE FROM "{schema}"."{table}"')

    await asyncio.to_thread(_flushdb, f"{e2e_dragonfly.host_url}/{e2e_schema.redis_db}")
    yield


# ── Second worker fixture (copied from test_multi_worker.py) ──────────────


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

    Mirrors the module-scoped ``e2e_worker`` fixture body with network
    alias ``worker2-<schema>`` and a readiness gate requiring ≥2 fresh
    post-register heartbeats.
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


# ── Test ──────────────────────────────────────────────────────────────────


async def test_stale_worker_row_removed_by_sweep(
    e2e_worker: E2EWorker,
    e2e_worker_second: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
) -> None:
    """Kill worker #1 with SIGKILL; ``cleanup_stale_workers`` removes its row.

    (a) Before the kill, both workers have rows in ``{schema}.workers``.
    (b) After SIGKILL, the killed worker stops heartbeating.  Its
        ``last_seen_at`` exceeds the staleness threshold
        (``heartbeat_interval * (max_heartbeat_failures + 3)`` = 3.0 s).
    (c) The surviving worker's leader sweep runs ``cleanup_stale_workers``
        which DELETEs the stale row.
    (d) The surviving worker's row is still present — the sweep's
        ``id != $2`` clause protects the leader's own row.
    """
    schema = e2e_schema.schema_name

    # ── Get both worker IDs before killing ────────────────────────────
    worker_rows = await e2e_pg_pool.fetch(f'SELECT id FROM "{schema}".workers ORDER BY started_at')
    assert len(worker_rows) == 2, f"expected 2 registered workers, got {len(worker_rows)}"

    # e2e_worker started before e2e_worker_second (fixture dependency
    # order), so the first row by started_at is the primary worker.
    dead_worker_id = worker_rows[0]["id"]
    alive_worker_id = worker_rows[1]["id"]

    # ── SIGKILL worker #1 ─────────────────────────────────────────────
    # Use the Docker API's container.kill rather than exec_run — the
    # daemon delivers the signal directly and tears down the container's
    # network namespace, which releases PG advisory locks promptly so
    # the surviving worker can acquire leadership and run the sweep.
    wrapped = e2e_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="KILL")

    # ── Poll until the dead worker's row is deleted ───────────────────
    async def _dead_worker_gone() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".workers WHERE id = $1',
            dead_worker_id,
        )
        return count == 0

    await poll_until(
        _dead_worker_gone,
        timeout=_CLEANUP_TIMEOUT,
        description=(f"dead worker {dead_worker_id} row removed by cleanup_stale_workers sweep"),
    )

    # ── Verify the surviving worker's row is still present ────────────
    alive_count = await e2e_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".workers WHERE id = $1',
        alive_worker_id,
    )
    assert alive_count == 1, (
        "surviving worker's row should still be present — "
        "cleanup_stale_workers protects the leader's own row (id != $2)"
    )
