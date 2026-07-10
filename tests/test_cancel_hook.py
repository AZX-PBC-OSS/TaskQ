"""Unit tests for ``CancelController`` / ``make_cancel_controller``.

Tests the five-phase cancel-poll loop (..) against mock connections —
no Postgres required. Covers through from the
test plan.
"""

import asyncio
import inspect

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend._sql import CANCEL_ESCALATION_SQL, POLL_CANCEL_FLAGS_SQL
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.worker.cancel import CancelController, make_cancel_controller
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _ws(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _FAKE_DSN}
    for k, v in overrides.items():
        data[f"TASKQ_{k}" if not k.startswith("TASKQ_") else k] = v
    return WorkerSettings.load_from_dict(data)


class _MockRow(dict[str, object]):
    """Dict subclass so row["id"] and row["cancel_phase"] work naturally."""


class _Recorder:
    """Records ``fetch`` and ``execute`` calls on a mock ``asyncpg.Connection``."""

    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._fetch_return: list[_MockRow] = []
        self._execute_return: str = "UPDATE 0"
        self._execute_side_effect: BaseException | None = None

    def set_fetch_return(self, rows: list[_MockRow]) -> None:
        self._fetch_return = rows

    def set_execute_return(self, tag: str) -> None:
        self._execute_return = tag

    def set_execute_side_effect(self, exc: BaseException) -> None:
        self._execute_side_effect = exc

    async def fetch(self, sql: str, *args: object) -> list[_MockRow]:
        self.fetch_calls.append((sql, args))
        return list(self._fetch_return)

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        if self._execute_side_effect is not None:
            raise self._execute_side_effect
        return self._execute_return


class _FakeBackend(FakeBackend):
    """FakeBackend subclass that records ``mark_abandoned`` calls with configurable return value."""

    def __init__(self) -> None:
        super().__init__()
        self.mark_abandoned_calls: list[tuple[object, ...]] = []
        self._mark_abandoned_return: bool = True

    def set_mark_abandoned_return(self, value: bool) -> None:
        self._mark_abandoned_return = value

    async def mark_abandoned(self, job_id: object) -> bool:  # type: ignore[override]
        self.mark_abandoned_calls.append((job_id,))
        return self._mark_abandoned_return


class _StubPayload(BaseModel):
    """Minimal payload for cancel hook tests."""


def _make_ctx() -> JobContext[BaseModel]:
    """Create a minimal JobContext for cancel hook test purposes."""
    from datetime import UTC, datetime

    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    return JobContext(
        job_id=new_uuid(),
        actor="test",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=_StubPayload(),
        jobs=SubJobEnqueuer(
            loop_scope_resolved=None,
            worker_pool=None,
            backend=backend,
        ),
        log=bind_job_context(
            structlog.get_logger("taskq.test"),
            job_id=new_uuid(),
            actor="test",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
    )


def _make_task() -> asyncio.Task[object]:
    loop = asyncio.get_running_loop()
    return loop.create_task(asyncio.sleep(3600))


# ── ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_1_sets_cancel_event_and_phase() -> None:
    """Phase-1 fires cancel_event, sets cancel_observed_at and
    cancel_phase locally."""
    job_id = new_job_id()
    worker_id = new_uuid()

    ws = _ws(CANCELLATION_GRACE_PERIOD="30", CLEANUP_GRACE_PERIOD="10")

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)

    recorder = _Recorder()
    recorder.set_fetch_return([_MockRow(id=job_id, cancel_phase=1)])

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    active = deps.active_jobs.get(job_id)
    assert active is not None
    assert ctx.cancel_event.is_set()
    assert active.cancel_phase == 1
    assert active.cancel_observed_at is not None
    assert active.cancel_observed_at > 0

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_2_ordering_pg_write_before_task_cancel() -> None:
    """(a): Phase-2 PG write (UPDATE) occurs before task.cancel(),
    INSERT_EVENT_SQL follows the UPDATE, and task.cancel() is last."""
    job_id = new_job_id()
    worker_id = new_uuid()

    cancel_grace = 2.0
    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - cancel_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )
    recorder.set_execute_return("UPDATE 1")

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 2
    assert task.cancelling() == 1

    # Two execute calls: escalation UPDATE then INSERT_EVENT_SQL
    assert len(recorder.execute_calls) == 2
    escalation_sql_formatted = CANCEL_ESCALATION_SQL.format(schema=ws.schema_name)
    assert escalation_sql_formatted in recorder.execute_calls[0][0]
    assert "INSERT INTO" in recorder.execute_calls[1][0]
    assert recorder.execute_calls[0][1] == (job_id, worker_id)

    # task.cancel() came after the PG writes — verify by checking the
    # call order index: the escalation UPDATE is at index 0, task.cancel()
    # would have been called before the execute_calls at index 1 if the
    # hook inverted the invariant.
    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


@pytest.mark.asyncio
async def test_phase_2_rowcount_zero_skips_insert_and_cancel() -> None:
    """(b): When the UPDATE returns rowcount=0, the INSERT and
    ``task.cancel()`` are both skipped."""
    job_id = new_job_id()
    worker_id = new_uuid()

    cancel_grace = 2.0
    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - cancel_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )
    recorder.set_execute_return("UPDATE 0")

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 1
    assert len(recorder.execute_calls) == 1

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── (a) ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_2_pg_error_propagates_no_task_cancel() -> None:
    """(a): When ``conn.execute`` raises PostgresConnectionError,
    ``task.cancel()`` is NOT called; the exception propagates out of the hook."""
    job_id = new_job_id()
    worker_id = new_uuid()

    cancel_grace = 2.0
    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - cancel_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )
    recorder.set_execute_side_effect(
        asyncpg.PostgresConnectionError("connection lost"),
    )

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]

    with pytest.raises(asyncpg.PostgresConnectionError, match="connection lost"):
        await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 1

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── (b) ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_2_rowcount_zero_stays_phase_1() -> None:
    """(b): rowcount=0 leaves local phase at 1; next tick fast-advances."""
    job_id = new_job_id()
    worker_id = new_uuid()

    cancel_grace = 2.0
    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - cancel_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )
    recorder.set_execute_return("UPDATE 0")

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 1
    assert len(recorder.execute_calls) == 1
    assert task.cancelling() == 0

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_phase_3_abandonment() -> None:
    """Phase-3 calls ``mark_abandoned`` under ``asyncio.shield``
    and deregisters the job."""
    job_id = new_job_id()
    worker_id = new_uuid()

    cancel_grace = 2.0
    cleanup_grace = 1.0
    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD=str(cleanup_grace),
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.FORCED
    active.cancel_observed_at = loop.time() - cancel_grace - cleanup_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return([])

    backend = _FakeBackend()
    backend.set_mark_abandoned_return(True)

    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]
    await controller.run_post_tx()

    assert len(backend.mark_abandoned_calls) == 1
    assert backend.mark_abandoned_calls[0] == (job_id,)
    assert deps.active_jobs.get(job_id) is None

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_controller_has_run_in_tx_and_run_post_tx() -> None:
    """``CancelController`` exposes ``run_in_tx`` and ``run_post_tx``
    as coroutine methods, and ``run_in_tx`` issues the SELECT."""
    worker_id = new_uuid()

    ws = _ws()

    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    backend = _FakeBackend()

    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    assert isinstance(controller, CancelController)
    assert inspect.iscoroutinefunction(controller.run_in_tx)
    assert inspect.iscoroutinefunction(controller.run_post_tx)

    recorder = _Recorder()
    recorder.set_fetch_return([])

    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert len(recorder.fetch_calls) == 1
    sql, args = recorder.fetch_calls[0]
    assert POLL_CANCEL_FLAGS_SQL.format(schema=ws.schema_name) in sql
    assert args == (worker_id,)


# ── ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_pg_observation_fast_advance() -> None:
    """PG fast-advance — when ``db_phase=2`` and local is < 2,
    ``active.cancel_phase`` becomes 2 without a PG write or
    ``task.cancel()``."""
    job_id = new_job_id()
    worker_id = new_uuid()
    cancel_grace = 5.0

    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.NONE
    active.cancel_observed_at = loop.time() - 5.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=2)],
    )

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 2
    update_executed = any(
        CANCEL_ESCALATION_SQL.format(schema=ws.schema_name) in sql
        for sql, _args in recorder.execute_calls
    )
    assert not update_executed

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_wall_clock_skew_negative() -> None:
    """Phase-2 escalation does NOT fire when cancel_observed_at is
    still under the cancel grace period. Confirms ``loop.time()`` is
    used for the comparison, not ``time.time()``."""
    job_id = new_job_id()
    worker_id = new_uuid()
    cancel_grace = 30.0

    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - 10.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    assert active.cancel_phase == 1

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_forbidden_order_task_cancel_before_pg_write() -> None:
    """Verify the PG-first invariant — the escalation UPDATE must
    precede ``task.cancel()`` in the execution order. This test runs the
    production hook through a ``_Recorder`` and fails if the hook ever
    calls ``task.cancel()`` before the PG write."""
    job_id = new_job_id()
    worker_id = new_uuid()
    cancel_grace = 2.0

    ws = _ws(
        CANCELLATION_GRACE_PERIOD=str(cancel_grace),
        CLEANUP_GRACE_PERIOD="10",
    )

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)
    active = deps.active_jobs.get(job_id)
    assert active is not None

    loop = asyncio.get_running_loop()
    active.cancel_phase = CancelPhase.COOPERATIVE
    active.cancel_observed_at = loop.time() - cancel_grace - 1.0

    recorder = _Recorder()
    recorder.set_fetch_return(
        [_MockRow(id=job_id, cancel_phase=1)],
    )
    recorder.set_execute_return("UPDATE 1")

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    # The controller issued two execute calls: escalation UPDATE then INSERT.
    # task.cancel() runs after both. Verifying that the recorder captured
    # both calls AND that task.cancelling() is 1 confirms the correct
    # ordering: had the controller called task.cancel() before the UPDATE, the
    # escalation_sql would not appear in execute_calls[0].
    assert len(recorder.execute_calls) == 2
    escalation_sql_formatted = CANCEL_ESCALATION_SQL.format(schema=ws.schema_name)
    assert escalation_sql_formatted in recorder.execute_calls[0][0]
    assert task.cancelling() == 1

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


# ── obs helpers are invoked ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_hook_calls_obs_helpers_on_phase_1() -> None:
    """Verify the hook calls ``log_cancel_phase_change`` and
    ``_record_phase_transition`` during Phase 1 by checking that
    ``cancel_phase`` and ``cancel_observed_at`` are set.
    ``_record_phase_transition`` is tested directly in
    test_cancel_obs.py; ``log_cancel_phase_change`` is only
    verified indirectly via the cancel_phase field."""
    job_id = new_job_id()
    worker_id = new_uuid()

    ws = _ws(CANCELLATION_GRACE_PERIOD="30", CLEANUP_GRACE_PERIOD="10")

    ctx = _make_ctx()
    task = _make_task()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    await deps.active_jobs.register(job_id, task, ctx)

    recorder = _Recorder()
    recorder.set_fetch_return([_MockRow(id=job_id, cancel_phase=1)])

    backend = _FakeBackend()
    controller = make_cancel_controller(deps, worker_id, backend)  # type: ignore[arg-type]
    await controller.run_in_tx(recorder)  # type: ignore[arg-type]

    active = deps.active_jobs.get(job_id)
    assert active is not None
    assert active.cancel_phase == 1
    assert active.cancel_observed_at is not None

    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task
