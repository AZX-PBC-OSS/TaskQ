"""The in-memory runner's stub context carries ``snooze_count`` like the
PG worker.

``JobContext.snooze_count`` (src/taskq/context.py:73) exists so
deferral-cycled actors key off snoozes rather than ``attempt`` — every
production construction site populates it from the job row
(``worker/dispatch.py:373``, ``worker/_consumer.py:425``,
``worker/run.py:377``), the PG path is pinned end-to-end by
``tests/e2e/actors.py:149``, and the runner mirror now
populates it from the same row (``src/taskq/testing/_runner.py``), with
the ``actor_runner`` fixture carrying the parameter so a deferral-cycled
actor is exercisable through the harness beyond first dispatch. This pin
holds the parity: an actor written to the documented contract must behave
identically under the test backend and the PG worker.
"""

from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.exceptions import Snooze
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_stub_context_carries_snooze_count_like_the_pg_worker() -> None:
    """A deferral-cycled stub keyed off ``ctx.snooze_count`` snoozes twice,
    then succeeds — the exact contract the e2e suite pins against PG. The
    in-memory runner must hand the stub the same field, or unit tests
    cannot express the documented actor pattern (and an actor written to
    the contract fails only under the test backend — the divergence the
    parity rule exists to forbid)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    seen: list[int] = []

    def cycler(payload: object, ctx: object) -> object:
        count = ctx.snooze_count  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the assertion is that the attribute exists and tracks the row.
        seen.append(count)
        if count < 2:
            raise Snooze(timedelta(seconds=30))
        return {"ok": True}

    backend.register_stub(
        "cycler",
        cycler,
        retry=RetryPolicy(kind="transient", max_attempts=5, jitter=0.0),
    )
    args = EnqueueArgs(
        id=new_job_id(),
        actor="cycler",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded", (
        "a stub keyed off ctx.snooze_count must cycle deferrals and succeed; "
        f"got status={row.status} error_class={row.error_class}"
    )
    assert seen == [0, 1, 2], (
        f"the stub must observe the snooze count incrementing per deferral; got {seen}"
    )
