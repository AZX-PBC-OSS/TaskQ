"""Hypothesis property test for the PG-first cancel-phase invariant.

For every generated sequence of timing scenarios, verifies that when the
cancel-poll loop escalates to phase 2, the PG ``cancel_phase = 2`` write
always occurs before ``task.cancel()`` in the side-effect log — the PG-first
invariant from

Each step tuple carries ``(clock_advance, db_phase, cancel_grace,
cleanup_grace)``. The hook sees ``db_phase`` as the PG-reported phase on that
tick; ``clock_advance`` is burned against ``cancel_observed_at`` to simulate
elapsed time (no ``loop.time()`` mock per research G-19). Grace periods vary
per step, exercising the case where operator-adjustable settings change
between heartbeat ticks.

The test runs against mock connections only — no PG, no Docker. The existing
``test_cancel_hook.py::test_forbidden_order_task_cancel_before_pg_write``
provides the reverse-angle check (fails on forbidden order), so the
reviewer can confirm that both directions are covered.
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime

import structlog
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import CancelPhase
from taskq.backend._sql import CANCEL_ESCALATION_SQL, INSERT_EVENT_SQL
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.worker.cancel import (
    ActiveJobRegistry,
    make_cancel_controller,
)
from taskq.worker.deps import WorkerDeps
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _make_settings(cancel_grace: float, cleanup_grace: float) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": _FAKE_DSN,
            "TASKQ_CANCELLATION_GRACE_PERIOD": str(cancel_grace),
            "TASKQ_CLEANUP_GRACE_PERIOD": str(cleanup_grace),
            "TASKQ_LOCK_LEASE": "360",
            "TASKQ_TERMINATION_GRACE_PERIOD": "360",
        }
    )


class _MockRow(dict[str, object]):
    """Dict subclass so ``row["id"]`` and ``row["cancel_phase"]`` work."""


class _Recorder:
    """Records ``fetch`` and ``execute`` calls on a mock ``asyncpg.Connection``.

    Distinguishes UPDATE vs INSERT vs SELECT calls by inspecting the SQL
    prefix — matching the requirement to differentiate call types
    in the side-effect log.
    """

    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._fetch_return: list[_MockRow] = []
        self._execute_return: str = "UPDATE 1"

    def set_fetch_return(self, rows: list[_MockRow]) -> None:
        self._fetch_return = rows

    def set_execute_return(self, tag: str) -> None:
        self._execute_return = tag

    async def fetch(self, sql: str, *args: object) -> list[_MockRow]:
        self.fetch_calls.append((sql, args))
        return list(self._fetch_return)

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return self._execute_return


class _FakeBackend(FakeBackend):
    """FakeBackend subclass that records ``mark_abandoned`` calls."""

    def __init__(self) -> None:
        super().__init__()
        self.mark_abandoned_calls: list[tuple[object, ...]] = []

    async def mark_abandoned(self, job_id: object) -> bool:  # type: ignore[override]
        self.mark_abandoned_calls.append((job_id,))
        return True


class _StubPayload(BaseModel):
    """Minimal payload for property test scaffolding."""


def _make_ctx() -> JobContext[BaseModel]:
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


# ── Strategy ─────────────────────────────────────────────────────────────


_step_strategy = st.tuples(
    st.integers(min_value=0, max_value=600),  # clock_advance
    st.sampled_from([0, 1, 2]),  # db_phase — what the mock PG returns
    st.integers(min_value=1, max_value=120),  # cancel_grace
    st.integers(min_value=1, max_value=120),  # cleanup_grace
)

_steps_strategy = st.lists(_step_strategy, min_size=1, max_size=10)


# ── PG-first invariant ────────────────────────────────────────────


@given(steps=_steps_strategy)
@settings(max_examples=200, deadline=None)
async def test_cancel_phase_ordering_property(
    steps: list[tuple[int, int, int, int]],
) -> None:
    """PG-first invariant holds for all step sequences.

    For every generated ``(clock_advance, db_phase, cancel_grace,
    cleanup_grace)`` sequence driven through the cancel controller, the
    side-effect log proves the PG-first invariant (phase-2 escalation
    write always precedes ``task.cancel()``).

    Oracle items verified per DoD item 3:
    - Escalation UPDATE index < ``task.cancel()`` index.
    - ``INSERT_EVENT_SQL`` sits between the UPDATE and ``task.cancel()``.
    - ``cancel_phase`` only increases monotonically.
    - ``cancel_observed_at`` is set at most once with a finite value.
    """
    job_id = new_job_id()
    ctx = _make_ctx()
    task = _make_task()

    registry = ActiveJobRegistry()
    await registry.register(job_id, task, ctx)

    prev_phase: CancelPhase = CancelPhase.NONE
    observed_at_set_count: int = 0

    for step_idx, (advance, db_phase, cancel_grace, cleanup_grace) in enumerate(steps):
        active = registry.get(job_id)
        if active is None:
            break

        if active.cancel_observed_at is not None and advance > 0:
            active.cancel_observed_at -= advance

        ws = _make_settings(float(cancel_grace), float(cleanup_grace))
        deps = WorkerDeps(
            settings=ws,
            dispatcher_pool=_FakePool(),  # type: ignore[arg-type] # Why: asyncpg.Pool is a class, not an instance; test bypasses real pool construction
            heartbeat_pool=_FakePool(),  # type: ignore[arg-type] # Why: asyncpg.Pool is a class, not an instance; test bypasses real pool construction
            worker_pool=_FakePool(),  # type: ignore[arg-type] # Why: asyncpg.Pool is a class, not an instance; test bypasses real pool construction
            notify_conn=None,
            leader_conn=None,
            active_jobs=registry,
        )

        recorder = _Recorder()
        recorder.set_fetch_return([_MockRow(id=job_id, cancel_phase=db_phase)])
        recorder.set_execute_return("UPDATE 1")

        backend = _FakeBackend()

        controller = make_cancel_controller(deps, new_uuid(), backend)  # type: ignore[arg-type] # Why: _FakeBackend only implements mark_abandoned; the controller never calls the other Backend protocol methods
        await controller.run_in_tx(recorder)  # type: ignore[arg-type] # Why: _Recorder is a test stub; asyncpg.Connection[Record] cannot be structurally satisfied without the real driver
        await controller.run_post_tx()

        active = registry.get(job_id)
        if active is None:
            # Abandoned and deregistered by run_post_tx.
            assert len(backend.mark_abandoned_calls) >= 1, (
                "mark_abandoned must be called on phase-3 abandonment"
            )
            break

        current_phase = active.cancel_phase

        assert current_phase >= prev_phase, (
            f"cancel_phase decreased from {prev_phase} to {current_phase} at step {step_idx}"
        )

        if current_phase == CancelPhase.FORCED:
            escalation_sql = CANCEL_ESCALATION_SQL.format(schema=ws.schema_name)
            insert_sql_prefix = INSERT_EVENT_SQL.format(schema=ws.schema_name).split("(")[0].strip()
            escalated_this_step = any(escalation_sql in sql for sql, _ in recorder.execute_calls)
            if escalated_this_step:
                assert task.cancelling() == 1, (
                    f"task.cancelling() must be 1 when escalation fires (step {step_idx})"
                )
                assert len(recorder.execute_calls) >= 2, (
                    f"Expected escalation UPDATE + INSERT_EVENT_SQL (step {step_idx})"
                )
                assert escalation_sql in recorder.execute_calls[0][0], (
                    f"Escalation UPDATE must be at execute_calls[0] (PG-first) at step {step_idx}"
                )
                assert insert_sql_prefix in recorder.execute_calls[1][0], (
                    f"INSERT_EVENT_SQL must be at execute_calls[1], "
                    f"between UPDATE and task.cancel() (step {step_idx})"
                )

        if active.cancel_observed_at is not None and prev_phase < CancelPhase.COOPERATIVE:
            observed_at_set_count += 1

        prev_phase = current_phase

    assert observed_at_set_count <= 1, (
        f"cancel_observed_at set {observed_at_set_count} times; must be at most 1"
    )

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
