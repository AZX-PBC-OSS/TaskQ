"""ATTACK: the poison pill - a job whose EVERY attempt executes fully but
whose TERMINAL write always fails at the last instant.

The poison is one row: the actor body completes (fast, side-effect-free),
and the one statement that would record its outcome dies with an
infrastructure error on every attempt, for as long as the row lives.
Everything else in the fleet stays healthy. This is the worst honest
shape the at-least-once doctrine has to absorb: the work DID finish, the
system just can never say so.

The contract under attack (the recovery must own the row, the fleet must
not notice):

1. The terminal-write retry respects its ceiling inside one attempt:
   exactly ``_TERMINAL_WRITE_ATTEMPTS`` writes per consume, the backoff
   ladder pinned by tests/test_terminal_write_retry.py, then the write is
   reported failed and STOPS - no infinite retry inside the consumer.
2. The row is never stranded and never spun forever: the consumer
   disowns it, the row stays ``running`` under a dying lease, the reclaim
   sweep re-pends it while attempt budget remains, and the attempt
   counter - which only the CLAIM increments - walks the row to the
   ceiling, where the reclaim terminalises ``crashed``. The final state
   is the truthful one: the system never recorded the success it could
   not persist, so the row may not read ``succeeded``.
3. The attempt ledger stays whole: every attempt the poison burns is
   recorded exactly once (the reclaim's ``crashed``/``WorkerCrashed``
   attempt row), so an auditor reconciles every execution.
4. The consumer survives: a healthy sibling consumed beside the poison
   succeeds, its latency unpolluted by the poison's retry sleeps - the
   poison costs one slot's retries, never the loop.
5. The split-brain terminal: if a ``job_events`` row ever says
   ``succeeded`` while ``jobs.status`` says ``running`` (direct SQL, a
   restored backup, a half-applied writer), the recovery's truth source
   is the ROW: the reclaim reclaims on the row's state and the row never
   reads ``succeeded``. (Per-write atomicity - the fused CTE statement
   on PG, one dict transition on the twin - makes the split unreachable
   through either backend's own writers; the pin guards the recovery
   against a split that arrives any other way.)
6. The fleet shape: 100 poison pills at once leave the machinery alive -
   every pill walks to ``crashed`` in bounded cycles, every ledger stays
   whole, and healthy siblings enqueued beside the pills still succeed.

Everything here runs on the in-memory twin with a FakeClock, so green
cannot flake and a spin fails a bounded cycle guard, never a timeout.
The real-PG half of the attack (the fused statement, the real sweep) is
tests/test_attack_poison_pill_pg.py.
"""

# Why: every f-string SQL below interpolates only the fixture's own schema identifier; every value is $-bound.

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import pytest
import structlog.testing
from structlog.typing import EventDict

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId, JobRow
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job
from taskq.worker._handlers import AttemptOutcome

pytestmark = pytest.mark.asyncio

_START = datetime(2026, 1, 1, tzinfo=UTC)
_LEASE = timedelta(seconds=30)
_GRACE = timedelta(seconds=30)
_REPEND_WAIT = timedelta(seconds=5)  # past MIN_DEFERRAL_INTERVAL's 1s floor


class _PoisonBackend(InMemoryBackend):
    """The injection: the terminal write of the poisoned rows dies with an
    infrastructure error (the connection-drops-mid-commit family,
    ``OSError``, a member of the consumer's
    ``_TERMINAL_WRITE_INFRA_EXCEPTIONS``) on EVERY call, forever. Every
    other job - and every non-terminal method of the poisoned rows -
    delegates untouched: the poison is exactly the terminal write."""

    def __init__(self, clock: FakeClock, poisoned_ids: set[JobId]) -> None:
        super().__init__(clock=clock)
        self._poisoned_ids = poisoned_ids
        self.terminal_write_calls = 0
        self.succeeded_writes_landed: list[JobId] = []

    def _is_poisoned(self, job_id: JobId) -> bool:
        if job_id not in self._poisoned_ids:
            return False
        self.terminal_write_calls += 1
        return True

    async def mark_succeeded(
        self,
        job_id: JobId,
        worker_id: UUID,
        result: dict[str, object] | None = None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        fallback_result_ttl: timedelta | None = None,
        *,
        result_bytes: bytes | None = None,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> bool:
        if self._is_poisoned(job_id):
            raise OSError("simulated: connection died mid-commit")
        landed = await super().mark_succeeded(
            job_id,
            worker_id,
            result,
            progress_seq,
            progress_state,
            fallback_result_ttl,
            result_bytes=result_bytes,
            attempt=attempt,
            claim_epoch=claim_epoch,
        )
        if landed:
            self.succeeded_writes_landed.append(job_id)
        return landed

    async def mark_failed_or_retry(
        self,
        job_id: JobId,
        worker_id: UUID,
        error_info: ErrorInfo,
        retry_delay: timedelta | None,
        progress_seq: int = 0,
        progress_state: dict[str, object] | None = None,
        *,
        attempt: int | None = None,
        claim_epoch: int | None = None,
    ) -> JobRow:
        if self._is_poisoned(job_id):
            raise OSError("simulated: connection died mid-commit")
        return await super().mark_failed_or_retry(
            job_id,
            worker_id,
            error_info,
            retry_delay,
            progress_seq,
            progress_state,
            attempt=attempt,
            claim_epoch=claim_epoch,
        )


async def _enqueue_poison(
    backend: InMemoryBackend,
    *,
    max_attempts: int,
    actor: str = "poison_actor",
) -> JobId:
    # Candidates come FROM the actor_config registry on both backends
    # (the twin's mirror of PG's claim CTEs); an unregistered actor's
    # rows are never claimable.
    backend.register_actor_config(actor=actor)
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=max_attempts,
            retry_kind="transient",
            # The re-pend delay rides the row's own curve; fixed at 1 ms
            # keeps it at the 1 s MIN_DEFERRAL_INTERVAL floor, so one
            # clock step past the lease also clears the re-pend wait.
            retry_base=timedelta(milliseconds=1),
            retry_backoff="fixed",
            retry_jitter=0.0,
            scheduled_at=_START,
        )
    )
    return job_id


def _runner(
    backend: InMemoryBackend,
    runs: list[JobId],
) -> Callable[[object, object], Any]:
    async def run_actor(job_row: object, _ctx: object) -> object:
        # The consumer's run_actor contract hands the JobRow the attempt
        # dispatched; the narrows via the row's own id attribute.
        row = cast(JobRow, job_row)
        runs.append(row.id)
        return {"ok": True}

    return run_actor


def _consume(
    backend: InMemoryBackend,
    job: JobRow,
    worker_id: UUID,
    runs: list[JobId],
) -> Callable[[], Any]:
    async def call() -> AttemptOutcome:
        return await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=_runner(backend, runs),  # type: ignore[arg-type]  # Why: as in tests/test_terminal_write_retry.py, the stub takes (job_row, ctx) positionally.
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
        )

    return call


def _events(captured: list[EventDict], name: str) -> list[EventDict]:
    return [e for e in captured if e.get("event") == name]


class _ConsumerDeps:
    """The slice of WorkerDeps the consumer's disown path reads; the
    same stand-in tests/test_terminal_write_retry.py uses for the cancel
    path's disown pins."""

    def __init__(self) -> None:
        self.disowned_jobs: set[UUID] = set()
        self.progress_buffers: dict[UUID, object] = {}
        self.redis_client = None
        self.worker_pool = None
        self.settings = None


async def test_success_path_poison_is_disowned_so_the_lease_can_die() -> None:
    """The disown is what turns 'lease expiry reclaims it' from a promise
    into the mechanism: after the success write's budget dies, the job
    must land in ``deps.disowned_jobs`` so the heartbeat stops renewing
    its lease and the reclaim sweep can own the row. A poison whose
    disown was dropped would keep its lease renewed forever - the row
    ``running`` under a live lock nothing will ever move."""
    from taskq.worker.deps import WorkerDeps

    clock = FakeClock(start=_START)
    runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    job_id = await _enqueue_poison(backend, max_attempts=3)
    backend._poisoned_ids = {job_id}  # pyright: ignore[reportPrivateUsage]  # Why: the one-row poison.
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]
    deps = _ConsumerDeps()

    dispatched = await backend.dispatch_batch(worker_id, ["default"], limit=10, lock_lease=_LEASE)
    assert [row.id for row in dispatched] == [job_id]

    outcome = await consume_one_job(
        backend,
        dispatched[0],
        worker_id,
        deps=cast(
            WorkerDeps, deps
        ),  # Why: the consumer reads only the five attributes the stand-in defines; the cast is the test double boundary.
        run_actor=_runner(backend, runs),  # type: ignore[arg-type]  # Why: the shared stub shape.
        actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
        payload_type=EmptyPayload,
        clock=FakeClock(start=_START),
    )

    assert outcome == "failed"
    row = await backend.get(job_id)
    assert row is not None and row.status == "running"
    assert deps.disowned_jobs == {job_id}, (
        "the exhausted success write must disown the job: without it the heartbeat "
        "renews the lease forever and the reclaim can never own the row"
    )


async def _drive_to_ceiling(
    backend: _PoisonBackend,
    job_id: JobId,
    runs: list[JobId],
    *,
    worker_ids: list[UUID],
) -> int:
    """Claim, run, reclaim - repeatedly - until the row terminalises.

    Returns the number of poison cycles consumed. The caller's
    ``max_cycles`` guard (2x the attempt ceiling) is what turns an
    infinite reclaim spin into a loud red instead of a hung test: the
    row must terminalise within the ceiling, and any more cycles than
    that IS the defect.
    """
    clock = cast(FakeClock, backend._clock)  # pyright: ignore[reportPrivateUsage]  # Why: the twin's clock is the one every sibling attack test drives; the FakeClock IS the deterministic harness, and `advance` is its own method, not the Clock protocol's.
    max_cycles = 2 * max(len(worker_ids), 1) + 2
    for cycles, worker_id in enumerate(worker_ids * max_cycles, start=1):
        if cycles > max_cycles:
            pytest.fail(
                f"the poison spun past {max_cycles} claim/reclaim cycles for a job "
                f"whose attempt ceiling bounds it - the reclaim never terminalised "
                "the row (infinite spin)"
            )
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=10, lock_lease=_LEASE
        )
        assert [row.id for row in dispatched] == [job_id], (
            f"cycle {cycles}: the poisoned row must be the row in hand "
            f"(dispatched {[str(r.id) for r in dispatched]})"
        )
        job = dispatched[0]
        with structlog.testing.capture_logs() as captured:
            outcome = await _consume(backend, job, worker_id, runs)()
        assert outcome == "failed", (
            f"cycle {cycles}: the consumer must report the attempt honestly "
            f"(the terminal write never landed), got {outcome!r}"
        )
        row = await backend.get(job_id)
        assert row is not None
        assert row.status == "running", (
            f"cycle {cycles}: the row must wait for the reclaim after the write "
            f"budget dies, not strand in {row.status!r}"
        )
        # Exactly one terminal-write-failed report per cycle, and the
        # retry ladder under it: three retries, then the failure - the
        # ceiling is per attempt, never a fifth write, never a silent one.
        retries = _events(captured, "terminal-write-retry")
        assert [e["attempt"] for e in retries] == [1, 2, 3], (
            f"cycle {cycles}: the terminal-write retry ladder must be exactly "
            f"three bounded waits, got {[e['attempt'] for e in retries]}"
        )
        failed_events = _events(captured, "terminal-write-failed")
        assert len(failed_events) == 1, (
            f"cycle {cycles}: exactly one terminal-write-failed report per "
            f"exhausted budget, got {len(failed_events)}"
        )
        # The worker died mid-poison (the attack's premise): its lease
        # will never be renewed. Age the lease past validity and let the
        # reclaim sweep own the row.
        clock.advance(_LEASE + timedelta(seconds=1))
        reclaimed = await backend.reclaim_expired_locks(_GRACE, _GRACE)
        assert reclaimed == 1, f"cycle {cycles}: the expired poison must be reclaimed exactly once"
        row = await backend.get(job_id)
        assert row is not None
        if row.status == "crashed":
            return cycles
        assert row.status == "pending", (
            f"cycle {cycles}: with attempt budget left the reclaim must hand the "
            f"row back, got {row.status!r}"
        )
        clock.advance(_REPEND_WAIT)
    pytest.fail("unreachable: the cycle guard above fails first")


@pytest.mark.parametrize("max_attempts", [1, 3])
async def test_poison_walks_the_row_to_crashed_at_the_attempt_ceiling(
    max_attempts: int,
) -> None:
    """Attack 1: one poisoned job, every attempt's terminal write dies.
    The lifecycle must walk the row to ``crashed`` in EXACTLY
    ``max_attempts`` cycles - never a spin, never a strand, never a
    ``succeeded`` the system could not persist - and every burned attempt
    must be in the ledger exactly once."""
    clock = FakeClock(start=_START)
    runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    job_id = await _enqueue_poison(backend, max_attempts=max_attempts)
    backend._poisoned_ids = {job_id}  # pyright: ignore[reportPrivateUsage]  # Why: the poison is minted with the row's id, the same one-row shape the attack describes.
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_terminal_write_retry.py's _running_job.

    cycles = await _drive_to_ceiling(backend, job_id, runs, worker_ids=[worker_id])

    row = await backend.get(job_id)
    assert row is not None
    assert cycles == max_attempts, (
        f"the ceiling is the attempt counter: exactly {max_attempts} cycles must "
        f"terminalise the row, the poison spent {cycles}"
    )
    assert row.status == "crashed", (
        f"the final terminal state is the truthful one - the system could never "
        f"persist the success, so the row reads crashed, got {row.status!r}"
    )
    assert row.error_class == "WorkerCrashed"
    assert row.result is None, "an unpersisted success must never be claimed"
    assert row.finished_at is not None
    # The body ran exactly once per attempt: never double-run within a
    # cycle, never a cycle without a body run.
    assert len(runs) == max_attempts, (
        f"each poisoned attempt executes the body exactly once "
        f"({max_attempts} total), got {len(runs)}"
    )
    # The ceiling held per cycle: four write attempts per consume, and
    # the poison never saw a fifth.
    assert backend.terminal_write_calls == max_attempts * 4, (
        f"the terminal-write retry ceiling is 4 per attempt "
        f"({max_attempts * 4} total), got {backend.terminal_write_calls}"
    )
    assert backend.succeeded_writes_landed == []
    # The attempt ledger is whole: one row per burned attempt, each
    # recording what the system actually observed (crashed/WorkerCrashed -
    # the worker never reported a terminal state for that attempt).
    attempts = sorted(await backend.get_attempts(job_id), key=lambda a: a.attempt)
    assert [a.attempt for a in attempts] == list(range(1, max_attempts + 1)), (
        f"the ledger must record every attempt exactly once, got "
        f"{[(a.attempt, a.outcome) for a in attempts]}"
    )
    assert all(a.outcome == "crashed" for a in attempts)
    assert all(a.error_class == "WorkerCrashed" for a in attempts)
    # The audit trail closes on the honest transition: the last state
    # change is the reclaim's running -> crashed, not a success.
    events = await backend.get_events(job_id)
    state_changes = [e for e in events if e.kind == "state_change"]
    assert state_changes, "the lifecycle must be visible in job_events"
    last = state_changes[-1]
    assert last.detail.get("to_state") == "crashed"


async def test_poison_ceiling_is_cumulative_across_reclaim_and_new_workers() -> None:
    """Attack 2: the poison's worker dies mid-poison every cycle; a
    DIFFERENT worker re-claims the re-pended row each time. The attempt
    ceiling must be judged on the ROW's cumulative counter - a reclaim
    hands the row to the fleet, it does not hand the poison a fresh
    budget. Three workers, one ceiling: the row terminalises on the
    third claim, and each worker ran the body exactly once."""
    clock = FakeClock(start=_START)
    runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    job_id = await _enqueue_poison(backend, max_attempts=3)
    backend._poisoned_ids = {job_id}  # pyright: ignore[reportPrivateUsage]  # Why: as above, the poison rides the one row.
    # A different worker every cycle: the first worker died mid-poison.
    workers = [backend._worker_id, new_job_id(), new_job_id()]  # pyright: ignore[reportPrivateUsage]

    cycles = await _drive_to_ceiling(backend, job_id, runs, worker_ids=workers)

    row = await backend.get(job_id)
    assert row is not None
    assert cycles == 3
    assert row.status == "crashed"
    assert len(runs) == 3, (
        f"three workers, one ceiling: three body runs TOTAL, not three per "
        f"worker - a reclaim must not reset the attempt budget, got {len(runs)}"
    )
    attempts = sorted(await backend.get_attempts(job_id), key=lambda a: a.attempt)
    assert [a.attempt for a in attempts] == [1, 2, 3], (
        "the cumulative counter is the ledger: attempts 1, 2, 3 across three "
        f"different holders, got {[(a.attempt, a.outcome) for a in attempts]}"
    )
    holders = {a.worker_id for a in attempts}
    assert len(holders) == 3, (
        f"each cycle's attempt row must record its own (different) holder, got {holders}"
    )


async def test_healthy_sibling_runs_clean_beside_the_poison() -> None:
    """Attack 1c: the poison costs its own slot's retries, never the
    loop. A healthy sibling consumed CONCURRENTLY with the poison must
    succeed on the first try, in a fraction of the poison's wall time -
    the poison's retry sleeps yield the loop, the sibling's write lands,
    and the consumer wedge never spreads."""
    import time

    clock = FakeClock(start=_START)
    poison_runs: list[JobId] = []
    healthy_runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    poison_id = await _enqueue_poison(backend, max_attempts=3)
    backend._poisoned_ids = {poison_id}  # pyright: ignore[reportPrivateUsage]  # Why: the poison is one row; the sibling delegates.
    healthy_id = await _enqueue_poison(  # reuse the enqueue shape, unpoisoned
        backend, max_attempts=3, actor="healthy_actor"
    )
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]

    async def healthy_body(_job: object, _ctx: object) -> object:
        healthy_runs.append(healthy_id)
        return {"ok": True}

    dispatched = await backend.dispatch_batch(worker_id, ["default"], limit=10, lock_lease=_LEASE)
    by_id = {row.id: row for row in dispatched}
    assert poison_id in by_id and healthy_id in by_id
    poison_job = by_id[poison_id]
    healthy_job = by_id[healthy_id]

    poison_elapsed = 0.0
    healthy_elapsed = 0.0

    async def poison_consume() -> None:
        nonlocal poison_elapsed
        started = time.monotonic()
        await _consume(backend, poison_job, worker_id, poison_runs)()
        poison_elapsed = time.monotonic() - started

    async def healthy_consume() -> None:
        nonlocal healthy_elapsed
        started = time.monotonic()
        outcome = await consume_one_job(
            backend,
            healthy_job,
            worker_id,
            run_actor=healthy_body,  # type: ignore[arg-type]  # Why: the stub takes (job_row, ctx) positionally, the established consumer-test shape.
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
        )
        healthy_elapsed = time.monotonic() - started
        assert outcome == "succeeded", (
            f"the healthy sibling must succeed beside the poison, got {outcome!r}"
        )

    import asyncio

    await asyncio.wait_for(
        asyncio.gather(poison_consume(), healthy_consume()),
        timeout=30.0,
    )

    row = await backend.get(healthy_id)
    assert row is not None
    assert row.status == "succeeded", (
        "the sibling's success write must land while the poison is mid-retry"
    )
    assert healthy_id in backend.succeeded_writes_landed
    # The sibling's latency is its own: the poison's bounded retry window
    # (~1.05 s of sleeps) must not stretch the sibling's share of the
    # loop past a fraction of the poison's occupancy.
    assert healthy_elapsed < poison_elapsed, (
        f"the poison held the loop {poison_elapsed:.3f}s and the sibling needed "
        f"{healthy_elapsed:.3f}s - the poison must not starve its siblings"
    )
    assert healthy_runs == [healthy_id], "the sibling ran exactly once"
    assert poison_runs == [poison_id], "the poison's attempt ran exactly once"


async def test_recovery_truth_source_is_the_row_not_a_done_event() -> None:
    """Attack 3: the split-brain terminal. A ``job_events`` row claims
    the job succeeded while ``jobs.status`` says ``running`` - the shape
    a half-applied writer (or direct SQL) leaves behind. The recovery
    must read the ROW: the reclaim owns the running row on its own
    state, the forged done-event must not stay the reclaim's hand, and
    the row must never read ``succeeded``."""
    clock = FakeClock(start=_START)
    runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    job_id = await _enqueue_poison(backend, max_attempts=1)
    backend._poisoned_ids = {job_id}  # pyright: ignore[reportPrivateUsage]  # Why: the one-row poison.
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]

    # One full poison cycle: the write budget dies, the row waits running.
    dispatched = await backend.dispatch_batch(worker_id, ["default"], limit=10, lock_lease=_LEASE)
    assert [row.id for row in dispatched] == [job_id]
    outcome = await _consume(backend, dispatched[0], worker_id, runs)()
    assert outcome == "failed"
    row = await backend.get(job_id)
    assert row is not None and row.status == "running"

    # The split-brain, by hand: an event stream that says the job is done
    # while the row says running. (Neither backend's own writers can
    # produce this - the terminal write is one fused statement - so the
    # forge is the only honest way to ask the recovery the question.)
    backend._append_state_change_event(  # pyright: ignore[reportPrivateUsage]  # Why: the forge needs the twin's event append; the private seam is the established test access (tests/test_attack_poison_pill.py's doctrine: attack the state, not the writer).
        job_id,
        from_state="running",
        to_state="succeeded",
        now=clock.now(),
        worker_id=worker_id,
    )
    events = await backend.get_events(job_id)
    assert any(
        e.kind == "state_change" and e.detail.get("to_state") == "succeeded" for e in events
    ), "the forge must be in the event stream for this pin to be non-vacuous"

    # The recovery answers: the ROW is the truth source.
    clock.advance(_LEASE + timedelta(seconds=1))
    reclaimed = await backend.reclaim_expired_locks(_GRACE, _GRACE)
    assert reclaimed == 1, (
        "a running row with a spent lease must be reclaimed even though an "
        "event row claims it succeeded - events are an outbox, not a verdict"
    )
    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "crashed", (
        f"the truthful terminal for a spent attempt budget is crashed, got {row.status!r}"
    )
    assert row.result is None and row.error_class == "WorkerCrashed"
    # The body ran exactly once - the forged done-event neither
    # resurrected the job into a second run nor short-circuited the
    # reclaim into a no-op.
    assert runs == [job_id]
    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 1 and attempts[0].outcome == "crashed"


async def test_fleet_of_100_poison_pills_terminates_bounded_and_healthy() -> None:
    """Attack 4: the fleet shape. One hundred poison pills at once, with
    healthy siblings enqueued beside them. Every pill must walk to
    ``crashed`` in its own bounded cycles, every ledger must come out
    whole, the healthy siblings must all succeed, and the WHOLE storm
    must resolve inside a bounded number of reclaim sweeps - the fleet
    poison is many slow retries, never a wedge."""
    import asyncio
    import time

    clock = FakeClock(start=_START)
    poison_runs: list[JobId] = []
    healthy_runs: list[JobId] = []
    backend = _PoisonBackend(clock, set())
    pills = [await _enqueue_poison(backend, max_attempts=2) for _ in range(100)]
    backend._poisoned_ids = set(pills)  # pyright: ignore[reportPrivateUsage]  # Why: the fleet poison is every pill's terminal write.
    healthy_ids = [
        await _enqueue_poison(backend, max_attempts=2, actor="healthy_actor") for _ in range(10)
    ]
    worker_id = backend._worker_id  # pyright: ignore[reportPrivateUsage]

    async def consume(job: JobRow, runs: list[JobId]) -> AttemptOutcome:
        return await consume_one_job(
            backend,
            job,
            worker_id,
            run_actor=_runner(backend, runs),  # type: ignore[arg-type]  # Why: the shared stub shape.
            actor_config=StubActorConfig(retry=RetryPolicy(jitter=0.0)),
            payload_type=EmptyPayload,
            clock=FakeClock(start=_START),
        )

    started = time.monotonic()
    healthy_succeeded = 0
    for cycle in range(1, 6):  # the ceiling is 2; 5 cycles is 2.5x the guard
        dispatched = await backend.dispatch_batch(
            worker_id, ["default"], limit=1000, lock_lease=_LEASE
        )
        if not dispatched:
            break
        poison_jobs = [row for row in dispatched if row.id in backend._poisoned_ids]  # pyright: ignore[reportPrivateUsage]
        healthy_jobs = [row for row in dispatched if row.id not in backend._poisoned_ids]  # pyright: ignore[reportPrivateUsage]
        outcomes = await asyncio.wait_for(
            asyncio.gather(
                *[consume(row, poison_runs) for row in poison_jobs],
                *[consume(row, healthy_runs) for row in healthy_jobs],
            ),
            timeout=60.0,
        )
        for row, outcome in zip([*poison_jobs, *healthy_jobs], outcomes, strict=True):
            if row.id in backend._poisoned_ids:  # pyright: ignore[reportPrivateUsage]
                assert outcome == "failed", (
                    f"cycle {cycle}: a pill must report the attempt honestly, got {outcome!r}"
                )
            else:
                assert outcome == "succeeded", (
                    f"cycle {cycle}: a healthy sibling must succeed beside the "
                    f"fleet poison, got {outcome!r}"
                )
                healthy_succeeded += 1
        clock.advance(_LEASE + timedelta(seconds=1))
        # The fleet drains a bounded batch at a time, exactly like
        # production: repeat calls until the sweep reports the backlog
        # drained. The cap is a tripwire: every call moves rows OUT of
        # 'running', so the drain must end in two calls (the batch
        # default far exceeds 100 rows), never loop.
        for _ in range(4):
            if await backend.reclaim_expired_locks(_GRACE, _GRACE) == 0:
                break
        else:
            pytest.fail("the reclaim drain never reported the backlog drained")
        clock.advance(_REPEND_WAIT)
    else:
        pytest.fail("the fleet poison spun past 5 claim/reclaim rounds")

    elapsed = time.monotonic() - started

    assert healthy_succeeded == 10, (
        f"every healthy sibling succeeds in cycle 1 beside the fleet poison, "
        f"got {healthy_succeeded}"
    )
    assert healthy_runs == healthy_ids, "each sibling ran exactly once"
    for pill in pills:
        row = await backend.get(pill)
        assert row is not None
        assert row.status == "crashed", (
            f"pill {pill} must terminalise crashed at its attempt ceiling, got {row.status!r}"
        )
        assert row.result is None
        attempts = sorted(await backend.get_attempts(pill), key=lambda a: a.attempt)
        assert [a.attempt for a in attempts] == [1, 2], (
            f"pill {pill}'s ledger must record both burned attempts exactly once, "
            f"got {[(a.attempt, a.outcome) for a in attempts]}"
        )
    assert len(poison_runs) == 200, (
        f"100 pills x 2 attempts: 200 body runs total, got {len(poison_runs)}"
    )
    assert backend.terminal_write_calls == 200 * 4, (
        f"the per-attempt write ceiling holds across the fleet "
        f"(200 x 4), got {backend.terminal_write_calls}"
    )
    assert elapsed < 120.0, (
        f"the fleet storm resolved in {elapsed:.1f}s - a bounded drain, "
        "never a wedge (this bound is a tripwire, the cycles above are the pin)"
    )
