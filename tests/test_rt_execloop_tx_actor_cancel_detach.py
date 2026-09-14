"""Red-team: external cancel detaches the transactional actor (E3).

Contract under attack: an external cancel of the consume task must reach the
transactional actor attempt — the row already says cancelled
(``mark_cancelled`` landed), so the attempt must not keep running and
committing side effects.

Hypothesis (verified against the current tree): the transactional path wraps
the whole actor-in-transaction coroutine in ``tx_task`` and awaits
``asyncio.shield(tx_task)`` (src/taskq/worker/_consumer.py:851-853). On outer
cancel the shield raises but ``tx_task`` is NEVER cancelled — the handler only
attaches ``add_done_callback(_retrieve_detached_outcome)``
(_consumer.py:888). With no ``start_to_close`` (settings ``None`` → timeout
``None``, src/taskq/settings.py:1002-1010) and a non-DB actor, the actor runs
to completion DETACHED after ``mark_cancelled`` already landed — the row says
cancelled while side effects continue, and the detached transaction goes on
to write the success terminal.

The companion control test drives the IDENTICAL cancel through the
autonomous path (no shield, _consumer.py:952) where the cancel DOES propagate
— proving the divergence is the shield seam, not the cancel mechanics.
"""

import asyncio
import contextlib
from datetime import UTC, datetime

from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.context import JobContext
from taskq.testing.actor import EmptyPayload, FakeBackend, as_backend, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.worker._consumer import consume_one_job

_ACTOR_SLEEP = 0.25
_SETTLE = 0.6  # > _ACTOR_SLEEP: enough for a detached actor to finish


class _TxCtx:
    """No-op async context manager standing in for a PG transaction."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _TxConn:
    """Duck-typed ConnLike: no-op transaction()/execute(), open tx reported.

    The pinned contract is the cancellation's reach into the actor attempt,
    not database fidelity — a no-op conn keeps the transaction open forever
    exactly like an un-cancellable non-DB actor would observe.
    """

    def transaction(self) -> _TxCtx:
        return _TxCtx()

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "OK"

    def is_in_transaction(self) -> bool:
        return True


async def _cancel_mid_actor(fb: FakeBackend, transaction_conn: _TxConn | None) -> asyncio.Event:
    """Run consume_one_job on a slow non-cooperative actor, cancel externally.

    The actor sleeps ``_ACTOR_SLEEP`` of REAL time then flips an event: the
    event firing after the cancel proves the attempt was not terminated.
    """
    actor_finished = asyncio.Event()

    async def run_actor(job_row: JobRow, ctx: JobContext[BaseModel]) -> object:
        await asyncio.sleep(_ACTOR_SLEEP)
        actor_finished.set()
        return "late-side-effects"

    job = make_job_row(status="running")  # start_to_close None -> timeout None
    consume_task = asyncio.create_task(
        consume_one_job(
            as_backend(fb),
            job,
            new_uuid(),
            run_actor=run_actor,
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
            transaction_conn=transaction_conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stub; the pinned contract is cancel propagation into the actor, not DB fidelity.
        )
    )
    # Let the consume task reach the shield and the actor start sleeping.
    for _ in range(3):
        await asyncio.sleep(0)
    consume_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consume_task
    return actor_finished


async def test_external_cancel_reaches_transactional_actor() -> None:
    fb = FakeBackend()
    actor_finished = await _cancel_mid_actor(fb, _TxConn())

    assert fb.mark_cancelled_calls, (
        "the cancel terminal write must land on the external-cancel path — "
        "it does today; this is the row-state half of the divergence the next "
        "assertions attack"
    )
    assert not actor_finished.is_set(), (
        "immediately after the cancel+mark the actor must not have completed — "
        "it has not yet (mid-sleep); the next assertion waits past the sleep"
    )
    await asyncio.sleep(_SETTLE)
    assert not actor_finished.is_set(), (
        "CONTRACT: an external cancel of the consume task must reach the "
        "transactional actor attempt — the row already says cancelled "
        "(mark_cancelled landed above), so the attempt must not keep running. "
        "VIOLATION: the transactional path awaits asyncio.shield(tx_task) and, "
        "on outer cancel, only attaches "
        "add_done_callback(_retrieve_detached_outcome) "
        "(src/taskq/worker/_consumer.py:851-888) — tx_task is never cancelled — "
        "so with no start_to_close (settings None -> timeout None) the actor "
        "ran to completion DETACHED after mark_cancelled landed; the identical "
        "cancel on the autonomous path (no shield, _consumer.py:952) DOES "
        "terminate the actor — see the control test in this file."
    )
    assert not fb.mark_succeeded_calls, (
        "CONTRACT: a cancelled attempt must not go on to commit — no success "
        "terminal write may land after mark_cancelled. VIOLATION: the detached "
        "transaction completed the actor and issued mark_succeeded_with_conn "
        "AFTER the row was already marked cancelled — the row says cancelled "
        "while the attempt's side effects and terminal write still landed."
    )


async def test_external_cancel_reaches_autonomous_actor_control() -> None:
    """Control: the same external cancel DOES terminate the autonomous-path
    actor (no shield at src/taskq/worker/_consumer.py:952) — proving the
    divergence in the sibling test is the transactional shield seam."""
    fb = FakeBackend()
    actor_finished = await _cancel_mid_actor(fb, None)

    assert fb.mark_cancelled_calls, "the cancel terminal write must land here too"
    await asyncio.sleep(_SETTLE)
    assert not actor_finished.is_set(), (
        "control contract: without the transactional shield the external "
        "cancel propagates through wait_for into the actor attempt "
        "(src/taskq/worker/_consumer.py:952) — the actor sleep is interrupted "
        "and the completion event never fires. If this control fails, the "
        "cancel mechanics changed and the transactional divergence claim "
        "must be re-verified."
    )
    assert not fb.mark_succeeded_calls, "control: no success write after cancellation"
