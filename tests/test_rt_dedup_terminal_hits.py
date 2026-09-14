"""Red-team pin: the unique_for dedup arm's caller-detectability floor.

The arm's terminal-hit ESCALATION contract (a dedup whose target job is
terminal warns and names the status — issue #140's strand risk) is
pinned at its home in ``tests/test_dedup_logging.py``
(``test_unique_for_dedup_onto_terminal_target_warns_with_status``,
beside the idempotency-half twin), against the shared
``_log_enqueue_dedup`` helper every dedup site funnels through; the
duplicate of that pin this file carried while the contract was red was
retired on landing (the red-team note's "one of the two pins should be
retired" resolution, naming test_dedup_logging.py as the home).

What this file keeps is the other half of the #140 story: the caller's
own detection floor. ``unique_states`` is caller-configurable
(``src/taskq/actor.py`` documents folding terminal states in), and a
dedup onto a dead job strands the new work for up to ``unique_for`` —
the caller must be able to SEE that: ``was_existing`` is True and the
handle's row carries the terminal status (oban's equivalent is the
``conflict?`` flag on the returned job).
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

#: Terminal-inclusive states — the documented-supported configuration the
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
    """Green pin: the caller can detect a terminal unique_for hit —
    ``was_existing`` is True and the handle's row carries the terminal
    status. This is the observability floor that makes the #140 strand
    survivable: the escalation contract (the warning line) is the
    operator's signal, pinned at its home in test_dedup_logging.py, and
    this is the CALLER's — oban's equivalent is the ``conflict?`` flag
    on the returned job, and ours already has the same shape through
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
        "a dedup onto the terminal target must surface was_existing — the "
        "caller's detection flag for the stranded enqueue"
    )
    assert handle2.job_id == handle1.job_id
    row = await backend.get(handle2.job_id)
    assert row is not None
    assert row.status == "cancelled", (
        "the handle's underlying row must carry the terminal status the dedup "
        f"matched; got {row.status}"
    )
