"""Red-team pin: the unique_for dedup arm's caller-detectability floor.

The arm's terminal-hit ESCALATION contract (a dedup whose target job is
terminal warns and names the status - the strand risk) is
pinned at its home in ``tests/test_dedup_logging.py``
(``test_unique_for_dedup_onto_terminal_target_warns_with_status``,
beside the idempotency-half twin), against the shared
``_log_enqueue_dedup`` helper every dedup site funnels through; the
duplicate of that pin this file carried while the contract was red was
retired on landing (the red-team note's "one of the two pins should be
retired" resolution, naming test_dedup_logging.py as the home).

What this file keeps is the other half of the story: the caller's
own detection floor. ``unique_states`` is caller-configurable
(``src/taskq/actor.py`` documents folding terminal states in), and a
dedup onto a dead job strands the new work for up to ``unique_for`` -
the caller must be able to SEE that: ``was_existing`` is True and the
handle's row carries the terminal status, surfaced through the
``JobHandle`` for immediate caller visibility.
"""

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from taskq import actor
from taskq.backend._protocol import IdentityKey, JobFilter
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)
_IDENTITY = IdentityKey("rt-terminal-client")

#: Terminal-inclusive states - the documented-supported configuration the
#: production code comment claims cannot produce a terminal match.
_TERMINAL_INCLUSIVE_STATES = ("pending", "scheduled", "running", "cancelled")


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


class _DedupPayload(BaseModel):
    value: int = 0


@actor(
    name="rt_dedup_terminal_actor",
    unique_for=timedelta(days=30),
    unique_states=_TERMINAL_INCLUSIVE_STATES,
)
async def _terminal_inclusive_actor(_payload: _DedupPayload) -> None:
    pass


async def test_client_surfaced_terminal_dedup_is_caller_detectable() -> None:
    """Green pin: the caller can detect a terminal unique_for hit -
    ``was_existing`` is True and the handle's row carries the terminal
    status. This is the observability floor that makes the strand
    survivable: the escalation contract (the warning line) is the
    operator's signal, pinned at its home in test_dedup_logging.py, and
    the caller's signal is ``deduplicated_onto_terminal`` on the
    ``JobHandle``. This pin keeps it."""
    backend = _make_backend()
    client = JobsClient(backend=backend, clock=FakeClock(start=_START))

    handle1 = await client.enqueue(
        _terminal_inclusive_actor, _DedupPayload(value=1), identity_key=_IDENTITY
    )
    cancelled = await backend.cancel_where(
        JobFilter(actor="rt_dedup_terminal_actor"), reason="rt-pin"
    )
    assert cancelled.cancelled_ids == (handle1.job_id,), (
        "precondition: the target job must be terminal"
    )

    handle2 = await client.enqueue(
        _terminal_inclusive_actor, _DedupPayload(value=2), identity_key=_IDENTITY
    )

    assert handle2.was_existing is True, (
        "a dedup onto the terminal target must surface was_existing - the "
        "caller's detection flag for the stranded enqueue"
    )
    assert handle2.job_id == handle1.job_id
    row = await backend.get(handle2.job_id)
    assert row is not None
    assert row.status == "cancelled", (
        "the handle's underlying row must carry the terminal status the dedup "
        f"matched; got {row.status}"
    )


async def test_terminal_dedup_is_indicated_on_the_handle_without_a_second_read() -> None:
    """A dedup onto a terminal job announces itself on the returned handle.

    ``was_existing`` alone cannot carry this: it is True for every dedup,
    and the overwhelmingly common dedup - onto a live pending or running
    job - is a success, the whole point of an idempotency key. The case
    that needs a signal is the rare, silent one, where the enqueue matched
    a job that already finished: no worker will ever pick this work up, and
    with a long dedup horizon the caller can wait weeks before anyone
    notices. Today the only way to tell the two apart is a follow-up read
    of the row's status, which callers who trust ``was_existing`` never
    make - so the failure looks exactly like the success right up until the
    work was needed.

    The handle is where the answer belongs because the enqueue already knew
    it: the backend matched a row and read its status to decide whether to
    warn. Carrying that verdict forward costs nothing and turns a silent
    strand into a branch the caller can take.
    """
    backend = _make_backend()
    client = JobsClient(backend=backend, clock=FakeClock(start=_START))

    live = await client.enqueue(
        _terminal_inclusive_actor, _DedupPayload(value=1), identity_key=_IDENTITY
    )
    live_hit = await client.enqueue(
        _terminal_inclusive_actor, _DedupPayload(value=2), identity_key=_IDENTITY
    )
    assert live_hit.was_existing is True, "precondition: the live target deduped"
    assert live_hit.deduplicated_onto_terminal is False, (
        "a dedup onto a live job is the mechanism working as intended and "
        "must not raise the terminal indication"
    )

    cancelled = await backend.cancel_where(
        JobFilter(actor="rt_dedup_terminal_actor"), reason="rt-pin"
    )
    assert cancelled.cancelled_ids == (live.job_id,), (
        "precondition: the target job must be terminal"
    )

    terminal_hit = await client.enqueue(
        _terminal_inclusive_actor, _DedupPayload(value=3), identity_key=_IDENTITY
    )

    assert terminal_hit.was_existing is True
    assert terminal_hit.deduplicated_onto_terminal is True, (
        "a dedup onto a terminal job must be visible on the handle itself - "
        "the caller must not have to re-read the row to learn that the work "
        "it just enqueued will never run"
    )
