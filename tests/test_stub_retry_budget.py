"""A stub's explicit ``retry=`` is the actor's in-memory retry budget.

The twin's retry-exhaustion check reads the ROW's stamped budget
(``mark_failed_or_retry``'s ``row.attempt >= row.max_attempts`` arm), and
rows are stamped from the ActorRef's retry policy at enqueue time. So an
explicit ``register_stub(..., retry=RetryPolicy(max_attempts=2))`` must
stamp the rows enqueued or dispatched for that actor, exactly like a ref
stamp: the job fails after 2 attempts, not after the ref's declared
budget and not after the stub default's 3. Omitting ``retry=`` must
leave the enqueue-time stamp alone, so a stub registered via the
ActorRef keeps the actor's declared budget.
"""

from datetime import UTC, datetime

from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs
from taskq.client import JobsClient
from taskq.retry import RetryPolicy
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class BudgetPayload(BaseModel):
    value: int = 0


@actor(retry=RetryPolicy(max_attempts=5, jitter=0.0))
async def budget_actor(payload: BudgetPayload) -> BudgetPayload:
    return payload


def _failing_stub(calls: list[int]) -> "object":
    """Build a stub that records each attempt's number and always fails."""

    async def failing(payload: object, ctx: object) -> object:
        calls.append(ctx.attempt)  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the attempt counter is the pin.
        raise RuntimeError("always fails")

    return failing


async def test_row_budget_is_the_enqueue_declaration_the_stub_never_moves() -> None:
    """The row's budget is stamped by the enqueue (the client stamps it
    from the ActorRef), and the stub's registration never moves it,
    before or after: a stub whose retry= says 2 under a ref declaring 5
    runs the ref's 5, exactly as production would (the row's budget is
    the enqueuing side's declaration)."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)
    calls: list[int] = []

    backend.register_stub(
        budget_actor,
        _failing_stub(calls),
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
    )

    handle = await client.enqueue(budget_actor, BudgetPayload())
    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.max_attempts == 5, (
        f"the ref's declared budget stamps the row; got {row.max_attempts}"
    )

    await backend.run_until_drained()

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.status == "failed", f"got status={row.status} error={row.error_class}"
    assert row.max_attempts == 5
    assert len(calls) == 5, f"the enqueue declaration governs; got {len(calls)} attempts"


async def test_stub_registered_after_enqueue_never_moves_the_row_budget() -> None:
    """The row's budget is a bound: a stub registered after the enqueue
    cannot move it. The row carries the ref's enqueue-time stamp (5) and
    runs to it; only a stub registered BEFORE the enqueue stamps the row
    (at enqueue time, via _apply_stub_retry_override). Re-stamping at
    dispatch would let a test-side registration overwrite a ceiling a
    test deliberately seeded, breaking the max_attempts-never-moves
    invariant the denial-budget pins assert."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)
    calls: list[int] = []

    handle = await client.enqueue(budget_actor, BudgetPayload())
    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.max_attempts == 5, "the enqueue-time stamp is the ref's declared budget"

    backend.register_stub(
        budget_actor,
        _failing_stub(calls),
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
    )

    await backend.run_until_drained()

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.status == "failed", f"got status={row.status} error={row.error_class}"
    assert row.max_attempts == 5, "a late registration must not move the row's budget"
    assert len(calls) == 5, f"the row's enqueue-time budget governs; got {len(calls)} attempts"


async def test_stub_without_retry_keeps_the_ref_declared_budget() -> None:
    """Stubbed without ``retry=``: the ref's declared 5 stands. The
    historical stub default (``RetryPolicy(jitter=0.0)``, max_attempts=3)
    must not restamp the row down to 3."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)
    calls: list[int] = []

    backend.register_stub(budget_actor, _failing_stub(calls))

    handle = await client.enqueue(budget_actor, BudgetPayload())
    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.max_attempts == 5, (
        "the ref's declared budget must stand when the stub declares no "
        f"retry= of its own; got {row.max_attempts}"
    )

    await backend.run_until_drained()

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.status == "failed", f"got status={row.status} error={row.error_class}"
    assert row.max_attempts == 5
    assert len(calls) == 5, (
        "the job must run the actor's declared 5 attempts, not the stub "
        f"default's 3; got {len(calls)}"
    )


async def test_bare_name_stub_with_retry_does_not_move_the_row_budget() -> None:
    """A stub registered by bare name (no ref) with an explicit
    ``retry=`` does not move a row the enqueue already stamped: the
    enqueue's declaration is the budget, whatever the registration says.
    To test exhaustion at N attempts, declare N at the enqueue."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    calls: list[int] = []

    args = EnqueueArgs(
        id=new_job_id(),
        actor="budget_actor",
        queue="default",
        payload={},
        max_attempts=5,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)

    backend.register_stub(
        "budget_actor",
        _failing_stub(calls),
        retry=RetryPolicy(max_attempts=2, jitter=0.0),
    )

    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed", f"got status={row.status} error={row.error_class}"
    assert row.max_attempts == 5, "the enqueue's declaration is the budget"
    assert len(calls) == 5, f"got {len(calls)} attempts"
