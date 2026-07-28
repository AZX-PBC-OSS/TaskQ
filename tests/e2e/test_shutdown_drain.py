"""Graceful shutdown drain e2e — SIGTERM with in-flight jobs, no lost tasks.

Design spec scenario row
(docs/superpowers/specs/2026-07-27-e2e-test-suite-design.md):
SIGTERM a worker with a running job; verify the job is cooperatively
cancelled and no tasks are lost.

The ``slow_deliver_webhook`` actor (actors.py) sleeps 3 s — longer than
the e2e shutdown drain window (``cancellation_grace=1.0`` +
``cleanup_grace=1.0`` = 2.0 s).  On SIGTERM the worker's four-phase
shutdown orchestration (DRAINING → CANCELLING → FORCING → ABANDONING)
cancels the in-flight task: the ``asyncio.sleep`` is interrupted by
``task.cancel()`` in the FORCING phase, so the actor never records its
``finished`` effect and the job lands in a non-success terminal state.

After the SIGTERM a replacement worker is started to prove the system
is still functional — a new job enqueued on the same queue completes
normally.

The autouse ``clean_e2e_state`` fixture is overridden for this module
because the primary worker container is intentionally stopped mid-test;
the conftest's crash check (``_raise_if_worker_crashed``) would raise
on the next test's setup if the module gained additional tests.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
import pytest_asyncio

from ._assertions import (
    fetch_effects,
    fetch_job_rows,
    poll_until,
    wait_for_effects,
    wait_for_worker_ready,
)
from .actors import (
    SlowDeliverPayload,
    WelcomeEmailPayload,
    send_welcome_email,
    slow_deliver_webhook,
)
from .conftest import (
    _DELETE_ORDER,
    E2EWorker,
    _flushdb,
    _stop_container,
)

if TYPE_CHECKING:
    import asyncpg
    from containerspec import BuiltImage
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]

# Cancellation grace (1.0 s) + cleanup grace (1.0 s) = 2.0 s, plus buffer
# for signal delivery, PG writes, and process exit.
_SHUTDOWN_WAIT = 4.0


# ── Module-local clean_e2e_state override ─────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-killed workers.

    Skips ``_raise_if_worker_crashed`` (the primary worker is SIGTERM'd
    mid-test) and tolerates idle-gate timeout (the killed worker may leave
    transient ``running`` rows that the leader sweep has not yet reclaimed).
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


# ── Test ──────────────────────────────────────────────────────────────────


async def test_sigterm_drains_inflight_job(
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    run_id: str,
) -> None:
    """SIGTERM a worker with a running job: job is cancelled, no ``finished``
    effect, and a replacement worker can still process new jobs.

    (a) The ``slow_deliver_webhook`` actor records ``started`` immediately,
    sleeps 3 s, then records ``finished``.  SIGTERM arrives during the
    sleep.  The shutdown orchestration cancels the task within the 2 s
    grace window, so ``finished`` is never recorded and the job reaches a
    non-success terminal state (``cancelled`` or ``abandoned``).

    (b) A replacement worker container is started on the same schema/queue.
    A fresh ``send_welcome_email`` job completes normally, proving the
    system is still functional after the SIGTERM.
    """
    from testcontainers.core.container import DockerContainer

    # ── Phase 1: enqueue, wait for start, SIGTERM ──────────────────────
    handle = await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="ep-drain"),
    )

    # Wait until the actor has recorded "started" — the job is now in the
    # 3 s sleep and will not finish before the SIGTERM grace window expires.
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    # Send SIGTERM via the Docker API (``container.kill``) rather than
    # ``exec_run(["kill", "-TERM", "1"])`` — the Docker daemon delivers
    # the signal directly to PID 1, which is more reliable than spawning
    # a new process inside the container.
    wrapped = e2e_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    # Wait for the four-phase shutdown to complete.
    await asyncio.sleep(_SHUTDOWN_WAIT)

    # ── Phase 1 assertions: job was cancelled, not finished ───────────
    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert finished == [], (
        "job should not have a 'finished' effect — the actor was "
        "cancelled mid-sleep by the SIGTERM shutdown orchestration"
    )

    rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
    assert len(rows) == 1
    status: str = rows[0]["status"]
    assert status != "succeeded", f"job should not be 'succeeded' after SIGTERM, got {status!r}"

    # ── Phase 2: replacement worker, verify system functional ─────────
    # Wait for the terminated worker's heartbeat to go stale (>10s old)
    # before starting the replacement, so the readiness gate cannot be
    # satisfied by the dead worker's last heartbeat.
    async def _no_fresh_heartbeats() -> bool:
        count = await e2e_pg_pool.fetchval(
            f'SELECT count(*) FROM "{e2e_schema.schema_name}".workers '
            "WHERE last_seen_at > now() - interval '10 seconds'"
        )
        return count == 0

    await poll_until(
        _no_fresh_heartbeats,
        timeout=20.0,
        description="old worker heartbeat gone stale",
    )

    replacement = DockerContainer(image=e2e_worker_image.tag)
    replacement.with_network(e2e_network).with_network_aliases(
        f"worker-repl-{e2e_schema.schema_name}"
    )
    for key, value in e2e_schema.worker_env.items():
        replacement.with_env(key, value)

    await asyncio.to_thread(replacement.start)
    try:
        await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)

        run_id_2 = uuid4().hex
        handle2 = await e2e_client.enqueue(
            send_welcome_email,
            WelcomeEmailPayload(
                run_id=run_id_2,
                user_id="u-repl",
                email="u-repl@example.com",
            ),
        )
        await handle2.wait(timeout=30)

        effects = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id_2, kind="send")
        assert len(effects) == 1, (
            f"replacement worker should have processed 1 job, got {len(effects)} 'send' effects"
        )
    finally:
        await asyncio.to_thread(_stop_container, replacement)
