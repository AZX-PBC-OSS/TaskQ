"""Green pin: a ``noop`` batch outcome is neutral to the batch bookkeeping.

``apply_batch_terminal_outcome``'s decision table
(``src/taskq/batch.py``) returns untouched for non-terminal outcomes —
the list includes ``"noop"``, the reclaim-race outcome where the row
moved underneath this dispatch and the re-dispatch will record the real
result. A ``noop`` must not budge any counter, complete, or abort the
batch: nothing was consumed, so there is nothing to book. The
integration suite covers cancelled/failed/succeeded/snoozed/scheduled
and hook-failure (``tests/test_batch_abort_integration.py``), but no
``noop`` case existed — a future refactor that routes ``noop`` into the
failure counter (or the complete-if-empty arm) would land silently.
"""

from dataclasses import replace
from datetime import UTC, datetime

from taskq._ids import new_uuid
from taskq.batch import apply_batch_terminal_outcome
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_noop_outcome_leaves_the_batch_bookkeeping_untouched() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    bid = new_uuid()

    await backend.create_batch(
        bid,
        queue="default",
        expected_size=2,
        failure_threshold=3,
        finalizer_job_id=None,
        originating_actor=None,
    )

    job = replace(make_job_row(status="running"), metadata={"batch_id": str(bid)})

    await apply_batch_terminal_outcome(backend, job, "noop")

    batch_row = await backend.get_batch(bid)
    assert batch_row is not None
    assert batch_row.status == "active", (
        "a noop consumed nothing — the batch must stay active until a real "
        f"terminal outcome or the sweep; got {batch_row.status}"
    )
    assert batch_row.consecutive_failures == 0, (
        "a noop must not count as a failure — the reclaim-race outcome books "
        "nothing; the re-dispatch records the real result"
    )
    assert batch_row.completed_at is None, (
        "a noop must not complete the batch — no member reached terminal"
    )
