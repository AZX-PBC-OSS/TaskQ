"""Per-attempt ``BaseException`` capture at the consumer boundary.

Prior-art pattern (behavioral, not code): the mature queue runtimes this
library studies all capture a *panic*-grade exception at the per-job
boundary — one runtime recovers per-job panics, logs them, and fails the
job so the worker survives; another catches ``BaseException`` per message
and stuffs it into the message's failure record. The shared invariant: a
non-``Exception`` ``BaseException`` raised by job code is an ATTEMPT
OUTCOME, never worker death.

TaskQ's previous breadth let such an exception escape
:func:`consume_one_job` entirely: the row was stranded ``running`` until
lease expiry (which relabelled it ``WorkerCrashed`` — a false audit
trail), and the consumer loop task died, cancelling every in-flight
sibling. These pins capture the corrected contract:

- a custom ``BaseException`` subclass from an actor body routes through
  the SAME generic handler as any ``Exception``: the drain completes, the
  row lands terminal ``failed`` with the exception's own type name as
  ``error_class``, and the attempt rows carry the real outcome;
- with retry budget left, the decision is the ordinary retry curve (the
  exception is classified like any other failure, ``KeyboardInterrupt``
  aside);
- ``KeyboardInterrupt`` keeps propagating (interpreter/operator intent,
  never an actor outcome), preserving the fail-loud line the loop-level
  backstop enforces;
- the production ``PostgresBackend`` path carries the same contract (the
  twin pin at the bottom).
"""

# ruff: noqa: S608 Why: schema name is validated by WorkerSettings.post_load against _IDENT_RE before reaching SQL; asyncpg has no parameter binding for identifiers; matches the existing PG integration test pattern.

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker._consumer import consume_one_job

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

_START = datetime(2025, 1, 1, tzinfo=UTC)


class ActorBoom(BaseException):
    """A buggy actor's non-Exception BaseException (the panic-grade case)."""


_BOOM_CONFIG = StubActorConfig(
    retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0),
)
_TWO_ATTEMPT_CONFIG = StubActorConfig(
    retry=RetryPolicy(kind="transient", max_attempts=2, jitter=0.0),
)


def _enqueue_args(actor: str, *, max_attempts: int = 1) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={},
        max_attempts=max_attempts,
        retry_kind="transient",
        scheduled_at=_START,
    )


async def test_actor_baseexception_lands_truthful_failed_row() -> None:
    """An actor raising a non-Exception BaseException is captured at the
    attempt boundary: the drain completes (the consumer loop survives),
    and the row lands terminal ``failed`` with the exception's own type
    name as ``error_class`` — never stranded ``running``, never relabelled
    ``WorkerCrashed`` by the later lease reclaim."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def boom(payload: object, ctx: object) -> None:
        raise ActorBoom("buggy actor raised BaseException")

    backend.register_stub("boom", boom, payload_type=EmptyPayload)

    args = _enqueue_args("boom")
    await backend.enqueue(args)
    # The drain must not raise: the consumer loop survives the buggy actor.
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.error_class == "ActorBoom"
    assert row.error_message is not None
    assert "buggy actor raised BaseException" in row.error_message


async def test_actor_baseexception_with_budget_retries_like_any_failure() -> None:
    """With retry budget left, a non-Exception BaseException follows the
    ordinary retry curve: two attempts, then terminal ``failed`` with the
    exception's own class name — the same classification path a
    ``RuntimeError`` takes."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    call_count = 0

    def boom_twice(payload: object, ctx: object) -> None:
        nonlocal call_count
        call_count += 1
        raise ActorBoom(f"attempt {call_count}")

    backend.register_stub(
        "boom_twice",
        boom_twice,
        retry=RetryPolicy(kind="transient", max_attempts=2, jitter=0.0),
        payload_type=EmptyPayload,
    )

    args = _enqueue_args("boom_twice", max_attempts=2)
    await backend.enqueue(args)
    await backend.run_until_drained()

    assert call_count == 2
    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "failed"
    assert row.attempt == 2
    assert row.error_class == "ActorBoom"


async def test_actor_baseexception_attempt_rows_carry_the_real_outcome() -> None:
    """The attempt audit trail records the real exception, not a
    post-hoc lease-reclaim label: the attempt row's error_class is the
    actor's own exception type, so an auditor reconciling ``job_attempts``
    against ``jobs`` never mistakes an actor defect for a worker crash."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def boom(payload: object, ctx: object) -> None:
        raise ActorBoom("buggy actor raised BaseException")

    backend.register_stub("boom", boom, payload_type=EmptyPayload)

    args = _enqueue_args("boom")
    await backend.enqueue(args)
    await backend.run_until_drained()

    attempts = await backend.get_attempts(args.id)
    assert len(attempts) == 1
    assert attempts[0].error_class == "ActorBoom"
    assert attempts[0].outcome == "failed"


async def test_actor_keyboard_interrupt_still_propagates() -> None:
    """``KeyboardInterrupt`` is interpreter/operator intent, never an
    actor outcome: it propagates out of the consumer (fail loud, the
    loop-level backstop and the process's own shutdown machinery own it),
    and the row stays ``running`` for the lease-reclaim recovery path."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)

    def interrupt_itself(payload: object, ctx: object) -> None:
        raise KeyboardInterrupt

    backend.register_stub("interrupt_itself", interrupt_itself, payload_type=EmptyPayload)

    args = _enqueue_args("interrupt_itself")
    await backend.enqueue(args)
    with pytest.raises(KeyboardInterrupt):
        await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "running"


# ── Production-backend twin: the same contract through PostgresBackend ──


@pytest.mark.integration
async def test_pg_actor_baseexception_lands_truthful_failed_row(
    clean_jobs_app: "JobsApp",
) -> None:
    """Twin of ``test_actor_baseexception_lands_truthful_failed_row``
    through the production ``PostgresBackend``: the same non-Exception
    BaseException from an actor body routes through the same generic
    handler, and the row + attempt audit trail land truthful (the
    exception's own type name, never a post-hoc ``WorkerCrashed``
    relabel from the lease sweep)."""
    from taskq._ids import new_uuid
    from taskq.testing.pg import create_worker

    backend = clean_jobs_app.backend
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="boom",
            queue="default",
            payload={},
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await conn.execute(
            f"""UPDATE \"{schema}\".jobs
            SET status = 'running',
                attempt = attempt + 1,
                locked_by_worker = $1,
                lock_expires_at = now() + interval '60 seconds',
                started_at = now(),
                last_heartbeat_at = now()
            WHERE id = $2 AND status = 'pending'""",
            worker_id,
            job_id,
        )

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "running"
    assert row.attempt == 1

    def boom(_job: object, _ctx: object) -> None:
        raise ActorBoom("buggy actor raised BaseException")

    await consume_one_job(
        backend,
        row,
        worker_id,
        run_actor=boom,  # type: ignore[arg-type] # Why: object-typed actor callable, matching the PG integration tests' harness shape.
        actor_config=_BOOM_CONFIG,
        payload_type=EmptyPayload,
        clock=SystemClock(),
    )

    final = await backend.get(job_id)
    assert final is not None
    assert final.status == "failed"
    assert final.error_class == "ActorBoom"
    assert final.error_message is not None
    assert "buggy actor raised BaseException" in final.error_message

    attempts = await backend.get_attempts(job_id)
    assert len(attempts) == 1
    assert attempts[0].error_class == "ActorBoom"
    assert attempts[0].outcome == "failed"
