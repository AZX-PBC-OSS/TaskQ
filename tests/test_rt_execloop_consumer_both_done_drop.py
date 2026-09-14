"""Red-team: both-done race drops a TAKEN job in the consumer loops (E2).

Contract under attack: a job TAKEN out of ``local_queue`` is never silently
dropped — it must be either executed (routed to a consumer) or explicitly
re-pended/released.

Hypothesis (verified against the current tree): ``di_consumer_loop`` races
``local_queue.get()`` against ``shutdown_event.wait()`` with FIRST_COMPLETED
(src/taskq/worker/run.py:551-554). When the producer's put and
``shutdown_event.set()`` resolve in the same loop turn, BOTH waiters are done;
the ``if shut_wait in _done: return`` arm (run.py:559-560) fires and the TAKEN
job is discarded — it has already left the queue, no consumer runs it, no
terminal write or release is issued; recovery only via lock-lease expiry.
``consumer_loop_stub`` has the same seam (run.py:423-424).

The race is constructed deterministically: the loop is parked in
``asyncio.wait`` on an empty queue with an unset event; the put and the set
then resolve both waiter futures in the same synchronous block, so the loop
resumes with both tasks done.
"""

import asyncio
from datetime import UTC, datetime
from typing import cast

import pytest

import taskq.worker.run as run_module
from taskq._di import ProviderRegistry
from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, as_backend
from taskq.testing.jobs import make_job_row
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.run import consumer_loop_stub, di_consumer_loop

_DI_DROP_MSG = (
    "CONTRACT: a job TAKEN out of local_queue is never silently dropped — it "
    "must be either executed (dispatched to a consumer) or explicitly "
    "re-pended/released. VIOLATION: di_consumer_loop races local_queue.get() "
    "against shutdown_event.wait() with FIRST_COMPLETED "
    "(src/taskq/worker/run.py:551-554); when the producer's put and "
    "shutdown_event.set() resolve in the same loop turn both waiters are done "
    "and the `if shut_wait in _done: return` arm (run.py:559-560) discards the "
    "taken job — it left the queue, no consumer ran it, no terminal write or "
    "release was issued; recovery only via lock-lease expiry."
)

_STUB_DROP_MSG = (
    "CONTRACT: a job TAKEN out of local_queue is never silently dropped — it "
    "must be either executed or explicitly re-pended/released. VIOLATION: "
    "consumer_loop_stub has the same both-done seam as di_consumer_loop "
    "(src/taskq/worker/run.py:415-424): when the put and shutdown_event.set() "
    "resolve in the same loop turn, `if shut_wait in _done: return` "
    "(run.py:423-424) discards the taken job with zero backend calls — it left "
    "the queue, no sentinel ran, no terminal write or release was issued; "
    "recovery only via lock-lease expiry."
)


class _DepsStub:
    """Duck-typed WorkerDeps carrying only what the consumer loops read.

    The pinned contract is the taken-job routing behavior, not WorkerDeps
    fidelity; ``active_jobs`` is a REAL ActiveJobRegistry so any fix that
    registers the rescued job works unchanged.
    """

    def __init__(self) -> None:
        self.active_jobs = ActiveJobRegistry()
        self.settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
                "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
                "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
                "TASKQ_LOCK_LEASE": "45.0",
                "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            }
        )
        self.drain_failures = 0


class _ScopeStub:
    """Duck-typed DI scope: ``.get(Clock)`` returns a FakeClock (runtime
    checkable Clock protocol). The pinned contract is the taken-job routing
    behavior, not scope fidelity."""

    def __init__(self) -> None:
        from taskq.testing.clock import FakeClock

        self._clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))

    def get(self, key: object) -> object:
        return self._clock


def _backend_saw_job(fb: FakeBackend, job_id: object) -> bool:
    """True when any recorded backend terminal/release write names job_id."""
    dict_calls = (
        fb.mark_snoozed_calls
        + fb.mark_cancelled_calls
        + fb.mark_retry_after_calls
        + fb.mark_failed_or_retry_calls
    )
    return any(c["job_id"] == job_id for c in dict_calls) or any(
        c[0] == job_id for c in fb.mark_succeeded_calls
    )


def _job_re_pended(local_queue: asyncio.Queue[JobRow], job: JobRow) -> bool:
    """True when the taken job was put back onto the local queue."""
    try:
        back = local_queue.get_nowait()
    except asyncio.QueueEmpty:
        return False
    return back.id == job.id


async def test_di_consumer_loop_does_not_drop_job_taken_on_shutdown_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = make_job_row(actor="rt_race_actor", status="running")
    fb = FakeBackend()
    backend = as_backend(fb)
    spy_job_ids: list[object] = []

    async def _spy_dispatch(**kwargs: object) -> str:
        spy_job_ids.append(cast("JobRow", kwargs["job"]).id)
        return "succeeded"

    # Why the spy: it observes "the taken job was routed to execution" —
    # di_consumer_loop calls the run-module global dispatch_one_job, so a
    # module-attr monkeypatch is the execution observable without building
    # the full DI stack.
    monkeypatch.setattr(run_module, "dispatch_one_job", _spy_dispatch)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    shutdown_event = asyncio.Event()

    loop_task = asyncio.create_task(
        di_consumer_loop(
            _DepsStub(),  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub; the pinned contract is taken-job routing, not WorkerDeps fidelity.
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            registry=ProviderRegistry(),
            process_scope=_ScopeStub(),  # type: ignore[arg-type]  # Why: duck-typed scope stub; only .get(Clock) is read pre-dispatch.
            thread_scope=_ScopeStub(),  # type: ignore[arg-type]  # Why: duck-typed scope stub; passed through to the spied dispatch seam.
            loop_scope=_ScopeStub(),  # type: ignore[arg-type]  # Why: duck-typed scope stub; passed through to the spied dispatch seam.
            actor_registry={},
            enqueuer=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        )
    )
    # Park the loop inside asyncio.wait on an empty queue + unset event.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    # The race: the producer's put and the shutdown signal resolve in the
    # SAME loop turn, before the consumer observes either.
    local_queue.put_nowait(job)
    shutdown_event.set()
    await loop_task

    executed = spy_job_ids == [job.id]
    assert executed or _job_re_pended(local_queue, job) or _backend_saw_job(fb, job.id), (
        _DI_DROP_MSG + f" Evidence: dispatch spy saw {spy_job_ids!r}, queue re-pend="
        f"{_job_re_pended(local_queue, job)}, backend calls for the job="
        f"{_backend_saw_job(fb, job.id)} (queue is empty — the job was TAKEN)."
    )


async def test_consumer_loop_stub_does_not_drop_job_taken_on_shutdown_race() -> None:
    job = make_job_row(actor="rt_race_actor", status="running")
    fb = FakeBackend()
    backend: Backend = as_backend(fb)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=1)
    shutdown_event = asyncio.Event()

    loop_task = asyncio.create_task(
        consumer_loop_stub(
            _DepsStub(),  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps stub; active_jobs is a real registry so a rescue that registers works unchanged.
            local_queue,
            shutdown_event,
            backend=backend,
            worker_id=new_uuid(),
            stub_work_timeout=0.05,
        )
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    local_queue.put_nowait(job)
    shutdown_event.set()
    await loop_task

    assert _job_re_pended(local_queue, job) or _backend_saw_job(fb, job.id), (
        _STUB_DROP_MSG + f" Evidence: queue re-pend={_job_re_pended(local_queue, job)}, "
        f"backend calls for the job={_backend_saw_job(fb, job.id)} "
        "(queue is empty — the job was TAKEN)."
    )
