"""Crash recovery e2e — SIGKILL worker mid-job, surviving worker reclaims.

Scenario:
kill a worker mid-job with SIGKILL; the surviving worker's leader sweep
reclaims the expired lock and re-dispatches the job.

The ``long_running_job`` actor (actors.py) sleeps 30 s — far longer than
the 8 s lock lease — so a SIGKILL mid-run leaves the job in ``running``
with an expired lock.  The leader sweep's ``reclaim_expired_locks``
(``_leader_sweeps.py`` sweep 1) detects the expired lock, records the
attempt as ``crashed``, and re-pends the job with a 5 s backoff (retry
policy ``max_attempts=3, base=5 s``).  The surviving worker then
dispatches and completes the job.

Timing budget (worst case):
  8 s (lock expiry) + 2 s (sweep interval, TASKQ_SWEEP_INTERVAL=2 in e2e) + 5 s (retry backoff)
  + 30 s (actor sleep) ≈ 73 s.
The lease was widened 3 s → 8 s fleet-wide for loop-stall margin (see the
conftest's worker_env comment), which moves this test's reclaim ~5 s
later; the 120 s ``handle.wait`` timeout still accommodates the budget
with ~1.6x margin.

The autouse ``clean_e2e_state`` fixture is overridden because a worker
container is intentionally killed mid-test; the conftest's crash check
would raise on setup.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from taskq.testing._shared_containers import creator_labels

from ._assertions import fetch_effects, poll_until, wait_for_effects
from .actors import LongRunningPayload, long_running_job
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

    from taskq import TaskQ
    from taskq.backend._protocol import EventRow

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

# lock_lease (3 s) + sweep interval (2 s in e2e) + retry backoff (5 s) + actor
# sleep (30 s) ≈ 68 s worst case; allow generous margin for Docker
# starvation and leader re-election.
_RECOVERY_TIMEOUT = 120.0


# ── Module-local clean_e2e_state override ─────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-killed workers.

    Skips ``_raise_if_worker_crashed`` (a worker is SIGKILL'd mid-test)
    and tolerates idle-gate timeout (the killed worker leaves ``running``
    rows that the leader sweep has not yet reclaimed).
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

    Mirrors the module-scoped ``e2e_worker`` fixture body (same image tag,
    same ``TASKQ_*`` env dict, same readiness-gate pattern) with network
    alias ``worker2-<schema>``.  The readiness gate requires ≥2 fresh
    post-register heartbeats so both workers are fully live before the
    test begins.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
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


async def test_sigkill_crash_recovery(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_worker_second: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """SIGKILL a worker mid-job: surviving worker reclaims and re-dispatches.

    (a) The ``long_running_job`` actor records ``started`` then sleeps 30 s.
    After the ``started`` effect appears, the test identifies which worker
    is running the job via ``jobs.locked_by_worker``, SIGKILLs that
    container, and waits for the job to reach ``succeeded``.

    (b) Two ``started`` effects prove the job ran twice: once on the
    crashed worker (attempt crashed) and once on the surviving worker
    (attempt succeeded).
    """
    schema = e2e_schema.schema_name

    # ── Enqueue and wait for the job to start ──────────────────────────
    handle = await e2e_client.enqueue(
        long_running_job,
        LongRunningPayload(run_id=run_id),
    )

    await wait_for_effects(
        e2e_pg_pool,
        schema,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    # ── Identify which worker is running the job ──────────────────────
    # Query jobs.locked_by_worker (set at dispatch time) rather than
    # job_attempts.worker_id (only populated at terminal write time —
    # the attempt row does not exist while the job is still running).
    job_worker_id = await e2e_pg_pool.fetchval(
        f'SELECT locked_by_worker FROM "{schema}".jobs WHERE id = $1',
        handle.job_id,
    )
    assert job_worker_id is not None, "job should be locked by a worker while running"

    # Both workers register in the workers table; order by started_at so
    # the first row is e2e_worker (started before e2e_worker_second).
    worker_rows = await e2e_pg_pool.fetch(f'SELECT id FROM "{schema}".workers ORDER BY started_at')
    assert len(worker_rows) == 2, f"expected 2 workers, got {len(worker_rows)}"

    target_worker = e2e_worker if job_worker_id == worker_rows[0]["id"] else e2e_worker_second

    # ── SIGKILL the worker running the job ────────────────────────────
    # Use the Docker API's container.kill rather than exec_run — the
    # daemon delivers the signal directly and tears down the container's
    # network namespace, which releases PG advisory locks promptly.
    wrapped = target_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="KILL")

    # ── Wait for the surviving worker to reclaim and complete ─────────
    # The reclaim sweep (every 2 s in e2e, TASKQ_SWEEP_INTERVAL=2) detects
    # the attempt as crashed, and re-pends the job with a 5 s backoff.
    # The surviving worker then dispatches and runs the 30 s actor.
    await handle.wait(timeout=_RECOVERY_TIMEOUT)

    # ── Assertions ────────────────────────────────────────────────────
    started = await fetch_effects(e2e_pg_pool, schema, run_id, kind="started")
    assert len(started) == 2, (
        f"expected 2 'started' effects (one from crashed worker, "
        f"one from surviving worker), got {len(started)}"
    )

    finished = await fetch_effects(e2e_pg_pool, schema, run_id, kind="finished")
    assert len(finished) == 1, (
        f"expected 1 'finished' effect (surviving worker completed the job), got {len(finished)}"
    )

    # ── Fleet-wide reclaim observability ──────────────────────────────
    # The sweep's reclaim writes a job_events outbox row in the same
    # transaction as the state change; watch_reclaims exposes it
    # fleet-wide behind a durable cursor. The event is already persisted
    # by the time the job succeeds, so following from after_id=0 surfaces
    # it without needing a watcher running concurrently with the crash.
    reclaim_event: EventRow | None = None
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(30):
            async for event in e2e_client.watch_reclaims(after_id=0, poll_timeout=2.0):
                if event.job_id == handle.job_id:
                    reclaim_event = event
                    break
    assert reclaim_event is not None, (
        f"no lock_expired reclaim event for job {handle.job_id} surfaced via watch_reclaims"
    )
    assert reclaim_event.kind == "state_change"
    assert reclaim_event.detail["reason"] == "lock_expired"
    # max_attempts=3 with a crashed first attempt → retried reclaim re-pends.
    assert reclaim_event.detail["to_state"] == "pending"
