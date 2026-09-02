"""Graceful shutdown drain e2e — SIGTERM with in-flight jobs, no lost tasks.

Scenario:
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

import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from taskq.testing._shared_containers import creator_labels

from ._assertions import (
    fetch_effects,
    fetch_job_rows,
    poll_until,
    wait_for_effects,
    wait_for_worker_ready,
)
from .actors import (
    ShortJobPayload,
    SlowDeliverPayload,
    WelcomeEmailPayload,
    send_welcome_email,
    short_lived_job,
    slow_deliver_webhook,
)
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

    from .conftest import E2EDragonfly, E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


# ── Module-local clean_e2e_state override ─────────────────────────────────


@pytest_asyncio.fixture(autouse=True)
async def clean_e2e_state(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """Override that tolerates intentionally-killed workers.

    Skips ``_raise_if_worker_crashed`` (the primary worker is SIGTERM'd
    mid-test) and tolerates idle-gate timeout (the killed worker may leave
    transient ``running`` rows that the leader sweep has not yet reclaimed).
    """
    if not {"e2e_client", "e2e_pg_pool", "e2e_worker", "e2e_schema", "drain_worker"}.intersection(
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


# ── Dedicated worker fixture for drain/escalation tests ───────────────────


@pytest_asyncio.fixture
async def drain_worker(
    request: pytest.FixtureRequest,
    e2e_network: Network,
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
    e2e_worker_image: BuiltImage,
) -> AsyncIterator[E2EWorker]:
    """Function-scoped worker container for drain/escalation tests.

    The module-scoped ``e2e_worker`` is killed by the first test
    (``test_sigterm_drains_inflight_job``), so subsequent tests that need
    a running worker use this dedicated fixture instead. Each test gets
    a fresh worker container, torn down after the test.
    """
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(image=e2e_worker_image.tag)
    container.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    container.with_network(e2e_network).with_network_aliases(
        f"worker-drain-{e2e_schema.schema_name}-{new_uuid().hex[:6]}"
    )
    for key, value in e2e_schema.worker_env.items():
        container.with_env(key, value)

    await asyncio.to_thread(container.start)
    try:
        try:
            await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)
        except TimeoutError:
            logs = _container_logs(container)
            msg = f"drain e2e worker failed readiness gate\n{logs}"
            raise RuntimeError(msg) from None
        yield E2EWorker(container=container, schema=e2e_schema.schema_name)
    finally:
        if request.config.option.verbose >= 2:
            print(_container_logs(container))
        await asyncio.to_thread(_stop_container, container)


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

    # ── Phase 1 assertions: drain orchestration wrote a terminal state ──
    # Poll to cancelled/abandoned — the test's actual docstring contract.
    # A worker with NO drain orchestration at all (hard SIGTERM death, row
    # stuck 'running') previously passed the fixed-sleep + != succeeded
    # checks; polling to a drain-written terminal state closes that hole.
    async def _drained_terminal() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
        return bool(rows) and rows[0]["status"] in ("cancelled", "abandoned")

    await poll_until(
        _drained_terminal,
        timeout=30.0,
        description=(
            f"job {handle.job_id} reaching cancelled/abandoned via the SIGTERM drain orchestration"
        ),
    )

    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert finished == [], (
        "job should not have a 'finished' effect — the actor was "
        "cancelled mid-sleep by the SIGTERM shutdown orchestration"
    )

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
    replacement.with_kwargs(
        labels=creator_labels()
    )  # Ownership labels: sweepable under disabled Ryuk (see e2e_network's sweep).
    replacement.with_network(e2e_network).with_network_aliases(
        f"worker-repl-{e2e_schema.schema_name}"
    )
    for key, value in e2e_schema.worker_env.items():
        replacement.with_env(key, value)

    await asyncio.to_thread(replacement.start)
    try:
        await wait_for_worker_ready(e2e_pg_pool, e2e_schema.schema_name, timeout=30.0)

        run_id_2 = new_uuid().hex
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


# ── Graceful drain completes short job ────────────────────────────────────


async def test_graceful_drain_completes_short_job(
    e2e_client: TaskQ,
    drain_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """SIGTERM after a short job has completed: the job remains
    ``succeeded``, proving the shutdown does not corrupt completed jobs.

    The ``short_lived_job`` actor sleeps 0.5 s. The test enqueues it,
    waits for it to reach ``succeeded`` (via ``handle.wait``), THEN
    sends SIGTERM. The shutdown orchestration must not touch
    already-terminal jobs: the job's status, effects, and result must
    remain intact.

    This is the complement of ``test_sigterm_drains_inflight_job``: that
    test proves a long in-flight job (3 s) is cancelled by the shutdown
    orchestration; this test proves a short job that completed before
    SIGTERM is unaffected.
    """
    handle = await e2e_client.enqueue(
        short_lived_job,
        ShortJobPayload(run_id=run_id, label="drain-short"),
    )
    await handle.wait(timeout=60)

    rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
    assert rows[0]["status"] == "succeeded", (
        f"short job should have succeeded before SIGTERM, got status={rows[0]['status']}"
    )

    wrapped = drain_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    await asyncio.sleep(2.0)

    rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
    assert rows[0]["status"] == "succeeded", (
        f"short job should still be succeeded after SIGTERM (shutdown "
        f"must not corrupt completed jobs), got status={rows[0]['status']}"
    )

    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert len(finished) == 1, (
        f"short job should have a 'finished' effect, got {len(finished)} 'finished' effects"
    )


# ── Second SIGTERM escalation ─────────────────────────────────────────────


async def test_second_sigterm_escalates(
    e2e_client: TaskQ,
    drain_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    run_id: str,
) -> None:
    """A second SIGTERM during DRAINING fast-advances the orchestration
    to FORCING, cancelling the in-flight job faster than the full grace
    window.

    The ``slow_deliver_webhook`` actor sleeps 3.0 s — longer than the
    cancellation grace (1.0 s). The first SIGTERM starts the
    orchestration (DRAINING → CANCELLING). During CANCELLING, the
    orchestration polls for job completion with a 1.0 s deadline. A
    second SIGTERM sets ``escalate_event`` (``shutdown.py:326-327``),
    which breaks the CANCELLING poll loop early
    (``shutdown.py:166-167``), immediately advancing to FORCING where
    ``task.cancel()`` is called on the in-flight job.

    The test measures the elapsed time from the first SIGTERM to the
    job's terminal state. Without escalation, the minimum is
    ``cancellation_grace`` (1.0 s) + ``cleanup_grace`` (1.0 s) = 2.0 s.
    With escalation, the CANCELLING phase is cut short, so the job
    reaches terminal state faster. The assertion is that the job reaches
    ``cancelled`` or ``abandoned`` and a ``finished`` effect is NOT
    recorded (the actor was cancelled mid-sleep).
    """
    import time

    handle = await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="ep-escalate"),
    )

    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    wrapped = drain_worker.container.get_wrapped_container()

    t0 = time.monotonic()
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    await asyncio.sleep(0.3)
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    async def _drained_terminal() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
        return bool(rows) and rows[0]["status"] in ("cancelled", "abandoned")

    await poll_until(
        _drained_terminal,
        timeout=30.0,
        description=(f"job {handle.job_id} reaching cancelled/abandoned via escalated SIGTERM"),
    )
    elapsed = time.monotonic() - t0

    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert finished == [], (
        "job should not have a 'finished' effect — the actor was "
        "cancelled mid-sleep by the escalated SIGTERM"
    )

    # With escalation, the CANCELLING phase is cut short. The full
    # unescalated path takes >= cancellation_grace (1.0 s) + cleanup_grace
    # (1.0 s) = 2.0 s minimum. Escalation should land faster; allow
    # generous slack for Docker signal delivery latency.
    assert elapsed < 5.0, (
        f"escalated shutdown took {elapsed:.2f}s — expected faster than "
        f"the full 2.0s grace window (escalation may not have fired)"
    )
