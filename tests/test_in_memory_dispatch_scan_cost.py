"""The in-memory dispatch round scans the job table a bounded number of times.

The twin mirrors PG's per-actor capacity laterals by iterating the
registered actors, and it used to rescan every stored job once per actor
inside that loop - O(actors x jobs) per round, so a differential run with
a few hundred registered actors and a few thousand jobs spent its time in
the mirror rather than in the code under test. The pending, due rows are
now grouped by actor in one pass before the actor loop (order-preserving,
so the FIFO tiebreaks are unchanged), and the round's table scans no
longer grow with the actor count.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from taskq._ids import new_job_id, new_uuid
from taskq.backend import EnqueueArgs
from taskq.backend.clock import Clock
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, _JobStore

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _CountingJobStore(_JobStore):
    """The twin's job table, counting full-table iterations."""

    def __init__(self, clock: Clock, rows: dict[Any, Any]) -> None:
        super().__init__(clock)
        self.update(rows)
        self.scans = 0

    def values(self) -> Any:
        self.scans += 1
        return super().values()


async def test_a_dispatch_round_scans_the_table_a_fixed_number_of_times() -> None:
    backend = InMemoryBackend(clock=FakeClock(start=_START))
    actors = [f"actor_{i}" for i in range(40)]
    for actor in actors:
        backend.register_actor_config(actor=actor)
    for i in range(200):
        await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor=actors[i % len(actors)],
                queue="default",
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_START,
            )
        )
    counting = _CountingJobStore(backend._clock, backend._jobs)  # pyright: ignore[reportPrivateUsage]  # Why: the job table is the twin's own store; counting its scans is the round's cost oracle.
    backend._jobs = counting  # pyright: ignore[reportPrivateUsage]  # Why: same.

    rows = await backend.dispatch_batch(new_uuid(), ["default"], 10, timedelta(seconds=30))

    assert len(rows) == 10
    # One pass for the running-state tallies, one to group the pending
    # rows by actor: a bound that does not move with the 40 actors above.
    assert counting.scans <= 2, (
        f"a dispatch round scanned the job table {counting.scans} times for "
        f"{len(actors)} registered actors - the per-actor rescan is back"
    )
