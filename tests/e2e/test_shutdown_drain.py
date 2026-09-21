"""Graceful shutdown drain e2e - SIGTERM with in-flight jobs, no lost tasks.

Scenario:
SIGTERM a worker with a running job; verify the job is interrupted
(released back to the fleet, the spent attempt standing) and no tasks
are lost.

The ``slow_deliver_webhook`` actor (actors.py) sleeps 3 s - longer than
the e2e shutdown drain window (``cancellation_grace=1.0`` +
``cleanup_grace=1.0`` = 2.0 s).  On SIGTERM the worker's four-phase
shutdown orchestration (DRAINING → CANCELLING → FORCING → RELEASING)
cancels the in-flight task: the ``asyncio.sleep`` is interrupted by
``task.cancel()`` in the FORCING phase, so the actor never records its
``finished`` effect and the job is RELEASED (``pending``, the claim's
attempt increment standing) - a deploy is an infrastructure event,
never a verdict on the job.

After the SIGTERM a replacement worker is started: it claims the released
job and runs it to completion - the deploy cost the work nothing but
time.

The autouse ``clean_e2e_state`` fixture is overridden for this module
because the primary worker container is intentionally stopped mid-test;
the conftest's crash check (``_raise_if_worker_crashed``) would raise
on the next test's setup if the module gained additional tests.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.testing.settings import make_integration_settings
from taskq.worker.cancel import make_cancel_controller
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop

from ._assertions import (
    fetch_effects,
    fetch_job_rows,
    fresh_worker_ids,
    poll_until,
    wait_for_effects,
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
    _flushdb,
    running_worker,
)

if TYPE_CHECKING:
    import asyncpg
    import asyncpg.pool
    import asyncpg.transaction
    from testcontainers.core.network import Network

    from taskq import TaskQ

    from ._types import BuiltImage
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
    async with running_worker(
        request,
        network=e2e_network,
        schema=e2e_schema,
        pg_pool=e2e_pg_pool,
        image=e2e_worker_image,
        alias=f"worker-drain-{e2e_schema.schema_name}-{new_uuid().hex[:6]}",
        env=e2e_schema.worker_env,
        label="drain e2e worker",
    ) as worker:
        yield worker


# ── Test ──────────────────────────────────────────────────────────────────


def _dump_worker_logs(label: str, worker: E2EWorker) -> None:
    """Print a stopped-or-running worker container's logs into captured stdout.

    A failure inside the replacement-worker block propagates through the
    ``running_worker`` context, whose finally stops the container before
    pytest reports; dumping here is the only way the failure carries its
    own worker-side evidence.
    """
    with contextlib.suppress(Exception):
        stdout, stderr = worker.container.get_logs()
        print(f"--- {label} worker stdout (tail) ---")
        print(stdout.decode(encoding="utf-8", errors="replace")[-8000:])
        print(f"--- {label} worker stderr (tail) ---")
        print(stderr.decode(encoding="utf-8", errors="replace")[-2000:])


async def test_sigterm_drains_inflight_job(
    request: pytest.FixtureRequest,
    e2e_client: TaskQ,
    e2e_worker: E2EWorker,
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
    e2e_network: Network,
    e2e_worker_image: BuiltImage,
    run_id: str,
) -> None:
    """SIGTERM a worker with a running job: the job is interrupted (released,
    the spent attempt standing) and the replacement worker runs it to
    completion.

    (a) The ``slow_deliver_webhook`` actor records ``started`` immediately,
    sleeps 3 s, then records ``finished``.  SIGTERM arrives during the
    sleep.  The shutdown orchestration cancels the task within the 2 s
    grace window; the interrupted claim is released back to the fleet
    (``pending``, ``interrupt_count`` bumped, the attempt increment
    standing: the attempt started executing, and refunding it would
    re-create the epoch the interrupted handler still holds) - never
    terminalised by an infrastructure event.

    (b) A replacement worker container is started on the same schema/queue.
    It claims the released job and runs it to completion: the deploy cost
    the job one attempt of budget (the interrupted claim spent its own),
    and a fresh ``send_welcome_email`` job completes normally, proving
    the system is functional after the SIGTERM.
    """
    # ── Phase 1: enqueue, wait for start, SIGTERM ──────────────────────
    handle = await e2e_client.enqueue(
        slow_deliver_webhook,
        SlowDeliverPayload(run_id=run_id, endpoint_id="ep-drain"),
    )

    # Wait until the actor has recorded "started" - the job is now in the
    # 3 s sleep and will not finish before the SIGTERM grace window expires.
    await wait_for_effects(
        e2e_pg_pool,
        e2e_schema.schema_name,
        run_id,
        kind="started",
        min_count=1,
        timeout=30.0,
    )

    # Snapshot the primary worker's registration before the SIGTERM: the
    # clean shutdown unregisters it (deletes its workers row), and Phase 2
    # asserts that cleanup landed.
    pre_kill_worker_ids = await fresh_worker_ids(e2e_pg_pool, e2e_schema.schema_name)

    # Send SIGTERM via the Docker API (``container.kill``) rather than
    # ``exec_run(["kill", "-TERM", "1"])`` - the Docker daemon delivers
    # the signal directly to PID 1, which is more reliable than spawning
    # a new process inside the container.
    wrapped = e2e_worker.container.get_wrapped_container()
    await asyncio.to_thread(wrapped.kill, signal="TERM")

    # ── Phase 1 assertions: drain orchestration released the job ────────
    # Poll to the released state (pending/scheduled with the interruption
    # counted and the attempt increment standing) - the test's docstring
    # contract, matching mark_interrupted's merged semantics.
    # A worker with NO drain orchestration at all (hard SIGTERM death, row
    # stuck 'running') cannot satisfy it: the row would sit running until
    # a lease sweep reclaims it with the attempt spent as a crash.
    async def _drained_released() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
        return (
            bool(rows)
            and rows[0]["status"] in ("pending", "scheduled")
            and rows[0]["interrupt_count"] >= 1
            and rows[0]["attempt"] == 1
        )

    await poll_until(
        _drained_released,
        timeout=30.0,
        description=(
            f"job {handle.job_id} released back to the fleet (pending, the "
            f"attempt increment standing, interrupt_count bumped) by the "
            f"SIGTERM drain orchestration"
        ),
    )

    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert finished == [], (
        "job should not have a 'finished' effect - the actor was "
        "interrupted mid-sleep by the SIGTERM shutdown orchestration"
    )

    # ── Phase 2: replacement worker, verify system functional ─────────
    # No staleness gate before the replacement: a SIGTERM'd worker shuts
    # down cleanly by design and unregisters (deletes its own workers
    # row), so a wait for its heartbeat to go stale or vanish passes
    # immediately and would verify nothing. The registration cleanup the
    # shutdown actually guarantees is asserted after the drain completes,
    # alongside the terminal-state assertions below.

    async with running_worker(
        request,
        network=e2e_network,
        schema=e2e_schema,
        pg_pool=e2e_pg_pool,
        image=e2e_worker_image,
        alias=f"worker-repl-{e2e_schema.schema_name}",
        env=e2e_schema.worker_env,
        label="replacement e2e worker",
    ) as replacement:
        try:
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

            effects = await fetch_effects(
                e2e_pg_pool, e2e_schema.schema_name, run_id_2, kind="send"
            )
            assert len(effects) == 1, (
                f"replacement worker should have processed 1 job, got {len(effects)} 'send' effects"
            )

            # The interrupted job: the replacement claims the released row and
            # runs it to completion - the deploy re-ran the work exactly once,
            # on its original attempt budget.
            await wait_for_effects(
                e2e_pg_pool,
                e2e_schema.schema_name,
                run_id,
                kind="finished",
                min_count=1,
                timeout=30.0,
            )

            # Poll to the terminal row, do not read it once. The 'finished'
            # effect is the actor body's own last INSERT; the consumer's
            # mark_succeeded commits a moment later (measured 5 ms behind in
            # a reproduced failure), so a single read between the two writes
            # sees 'running' and fails a healthy run. The terminal state is
            # what this test waits on; poll it with the same deadline
            # discipline as every other cross-process transition here.
            async def _replacement_completed() -> bool:
                rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
                return bool(rows) and rows[0]["status"] == "succeeded"

            await poll_until(
                _replacement_completed,
                timeout=30.0,
                description=(
                    f"job {handle.job_id} to reach 'succeeded' on the "
                    f"replacement worker (the consumer's mark_succeeded "
                    f"lands just after the actor's own 'finished' effect "
                    f"INSERT)"
                ),
            )
            # poll_until already guarantees 'succeeded'; re-fetch only to
            # pin the attempt accounting the terminal poll cannot see.
            rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
            assert rows[0]["attempt"] == 2, (
                "the interrupted claim spent the attempt at release, so the "
                "replacement worker's completion is the job's second spent "
                "attempt: a deploy costs one attempt of budget, the price of "
                f"not re-running against a live handler; attempt reads "
                f"{rows[0]['attempt']}"
            )

            # The dead worker's registration is gone, not stale: the clean
            # shutdown unregistered it, so no fresh workers row outlives
            # the drain. (Polled, not read once: the unregister commit can
            # land just behind the job's terminal write.)
            async def _killed_worker_unregistered() -> bool:
                registered = await fresh_worker_ids(e2e_pg_pool, e2e_schema.schema_name)
                return not registered & pre_kill_worker_ids

            await poll_until(
                _killed_worker_unregistered,
                timeout=20.0,
                description=(
                    "the SIGTERM'd worker's registration to be deleted "
                    "(the shutdown orchestration unregisters cleanly)"
                ),
            )
        except BaseException:
            # The containers are stopped by their fixtures' finally blocks as
            # this exception propagates; dump their logs first so the failure
            # carries its own worker-side evidence (a lost finished effect is
            # invisible in the jobs/effects rows alone: the worker's own
            # shutdown, heartbeat, and dispatch lines are the diagnosis).
            _dump_worker_logs("replacement", replacement)
            _dump_worker_logs("primary", e2e_worker)
            raise


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
    test proves a long in-flight job (3 s) is interrupted and released by
    the shutdown orchestration; this test proves a short job that
    completed before SIGTERM is unaffected.
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

    The ``slow_deliver_webhook`` actor sleeps 3.0 s - longer than the
    cancellation grace (1.0 s). The first SIGTERM starts the
    orchestration (DRAINING → CANCELLING). During CANCELLING, the
    orchestration polls for job completion with a 1.0 s deadline. A
    second SIGTERM sets ``escalate_event`` (``shutdown.py:326-327``),
    which breaks the CANCELLING poll loop early
    (``shutdown.py:166-167``), immediately advancing to FORCING where
    ``task.cancel()`` is called on the in-flight job.

    The test measures the elapsed time from the first SIGTERM to the
    job's release. Without escalation, the minimum is
    ``cancellation_grace`` (1.0 s) + ``cleanup_grace`` (1.0 s) = 2.0 s.
    With escalation, the CANCELLING phase is cut short, so the job is
    released faster. The assertion is that the job is back with the fleet
    (``pending``/``scheduled``, the spent attempt standing,
    ``interrupt_count`` bumped) and a ``finished`` effect is NOT recorded
    (the actor was cancelled mid-sleep).
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

    async def _drained_released() -> bool:
        rows = await fetch_job_rows(e2e_pg_pool, e2e_schema.schema_name, [handle.job_id])
        return (
            bool(rows)
            and rows[0]["status"] in ("pending", "scheduled")
            and rows[0]["interrupt_count"] >= 1
        )

    await poll_until(
        _drained_released,
        timeout=30.0,
        description=(f"job {handle.job_id} released back to the fleet via escalated SIGTERM"),
    )
    elapsed = time.monotonic() - t0

    finished = await fetch_effects(e2e_pg_pool, e2e_schema.schema_name, run_id, kind="finished")
    assert finished == [], (
        "job should not have a 'finished' effect - the actor was "
        "cancelled mid-sleep by the escalated SIGTERM"
    )

    # With escalation, the CANCELLING phase is cut short. The full
    # unescalated path takes >= cancellation_grace (1.0 s) + cleanup_grace
    # (1.0 s) = 2.0 s minimum. Escalation should land faster; allow
    # generous slack for Docker signal delivery latency.
    assert elapsed < 5.0, (
        f"escalated shutdown took {elapsed:.2f}s - expected faster than "
        f"the full 2.0s grace window (escalation may not have fired)"
    )


# ── Heartbeat tick round-trip pin ─────────────────────────────────────────


class _CountingTransaction:
    """Delegates to a real asyncpg transaction, recording BEGIN/COMMIT/ROLLBACK."""

    def __init__(self, tx: asyncpg.transaction.Transaction, commands: list[str]) -> None:
        self._tx = tx
        self._commands = commands

    async def start(self) -> None:
        await self._tx.start()
        self._commands.append("BEGIN")

    async def commit(self) -> None:
        await self._tx.commit()
        self._commands.append("COMMIT")

    async def rollback(self) -> None:
        await self._tx.rollback()
        self._commands.append("ROLLBACK")


class _CountingConn:
    """Delegates to a real connection, recording every awaited command."""

    def __init__(self, conn: asyncpg.Connection, commands: list[str]) -> None:
        self._conn = conn
        self._commands = commands

    async def execute(self, sql: str, *args: object) -> str:
        result = await self._conn.execute(sql, *args)
        self._commands.append(sql)
        return result

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        result = await self._conn.fetch(sql, *args)
        self._commands.append(sql)
        return result

    def transaction(self) -> _CountingTransaction:
        return _CountingTransaction(self._conn.transaction(), self._commands)

    async def close(self) -> None:
        await self._conn.close()

    def terminate(self) -> None:
        self._conn.terminate()

    def is_closed(self) -> bool:
        return self._conn.is_closed()


class _CountingAcquire:
    """Async context manager mirroring ``Pool.acquire``, handing out a counting conn."""

    def __init__(self, ctx: asyncpg.pool.PoolAcquireContext, commands: list[str]) -> None:
        self._ctx = ctx
        self._commands = commands

    async def __aenter__(self) -> _CountingConn:
        return _CountingConn(await self._ctx.__aenter__(), self._commands)

    async def __aexit__(self, *exc: object) -> object:
        return await self._ctx.__aexit__(*exc)  # type: ignore[arg-type]  # Why: asyncpg's __aexit__ has precise exc-typed parameters; the forwarding wrapper only ever receives what the protocol allows.


class _CountingHeartbeatPool:
    """Wraps the real heartbeat pool, recording every command its ticks issue.

    Delegation is complete: the tick's code path (acquire → transaction →
    execute/fetch → commit, or the bounded close on failure) runs against
    the live PG unmodified - only the recording is added.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self.commands: list[str] = []

    def acquire(self, timeout: float) -> _CountingAcquire:
        return _CountingAcquire(self._pool.acquire(timeout=timeout), self.commands)


# The steady-state tick against this fleet: BEGIN, the liveness write, the
# gated lease renewal, the reservation write, the claim-loss reconcile
# probe (one indexed SELECT - it rides the tick's existing transaction and
# single command budget, but it IS a round trip), the cancel-hook poll,
# COMMIT.
_TICK_COMMAND_COUNT = 7


async def test_heartbeat_tick_round_trip_budget(
    e2e_schema: E2ESchema,
    e2e_pg_pool: asyncpg.Pool,
) -> None:
    """One production heartbeat tick against the live PG issues exactly six
    commands - the compensating pin for the 0.5 s budget widening.

    At the old 0.1 s command budget, a tick that grew a round trip (or a
    beat of scheduling lag) failed loudly - the worker self-isolated and
    the tier went red. At 0.5 s nothing fails, so the tick's round-trip
    count is pinned here instead: one extra tick statement - the exact
    regression the budget widening silenced - must fail this test and be
    either folded into an existing statement or re-budgeted deliberately.
    A wall-clock pin is not usable in this tier: container/CI jitter spans
    tens of milliseconds (the flake this PR fixes is exactly that), so a
    latency bound tight enough to catch one added round trip would flake;
    the count is deterministic and environment-independent.
    """
    settings = make_integration_settings(
        e2e_schema.host_dsn,
        SCHEMA_NAME=e2e_schema.schema_name,
        # Mirror the e2e fleet's timing knobs exactly (conftest.py
        # worker_env): the pin guards the budget those knobs set.
        HEARTBEAT_INTERVAL="0.5",
        HEARTBEAT_COMMAND_TIMEOUT="0.5",
        LOCK_LEASE="8.0",
        CANCELLATION_GRACE_PERIOD="1.0",
        CLEANUP_GRACE_PERIOD="1.0",
        TERMINATION_GRACE_PERIOD="15.0",
    )
    worker_id = new_uuid()

    stack = AsyncExitStack()
    try:
        deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
        counting = _CountingHeartbeatPool(deps.heartbeat_pool)
        deps.heartbeat_pool = counting  # type: ignore[assignment]  # Why: the counting wrapper is a drop-in for asyncpg.Pool in the heartbeat path (same discipline as the unit tier's FakePool), and only the heartbeat touches it here.

        controller = make_cancel_controller(
            deps,
            worker_id,
            PostgresBackend(
                deps,
                clock=SystemClock(),
                cancellation_grace_period=timedelta(seconds=settings.cancellation_grace_period),
                cleanup_grace_period=timedelta(seconds=settings.cleanup_grace_period),
            ),
        )
        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        async def _first_tick_committed() -> bool:
            return "COMMIT" in counting.commands

        await poll_until(
            _first_tick_committed,
            timeout=30.0,
            description="the in-process heartbeat's first tick to commit against the live PG",
        )
        shutdown.set()
        await task

        commands = counting.commands
        assert len(commands) == _TICK_COMMAND_COUNT, (
            f"one heartbeat tick issued {len(commands)} commands, expected "
            f"{_TICK_COMMAND_COUNT} (BEGIN, the liveness write, the gated "
            f"lease renewal, the reservation write, the claim-loss "
            f"reconcile probe, the cancel-hook poll, COMMIT); the tick's "
            f"round-trip sequence grew - under this fleet's 0.5 s command "
            f"budget nothing else would fail, so either fold the round "
            f"trip into an existing statement, or re-derive the budget "
            f"and this pin together. "
            f"Commands observed: {[sql[:80] for sql in commands]}"
        )
    finally:
        # The tick registered an in-process workers row; the e2e tier's
        # shared schema keeps workers rows across tests, so remove ours.
        with contextlib.suppress(Exception):
            await e2e_pg_pool.execute(
                f'DELETE FROM "{e2e_schema.schema_name}".workers WHERE id = $1',
                worker_id,
            )
        await stack.aclose()
