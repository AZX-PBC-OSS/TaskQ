"""Hypothesis property tests for unique_for dedup invariant.

Verifies the unique_for invariant using InMemoryBackend: for any
identity, any window, and any sequence of in-window/out-of-window
enqueue events, dedup returns the existing handle when an active
job exists within the window, and creates a new job otherwise.

anchors: (unique_for preflight),.
"""

from datetime import UTC, datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdentityKey, JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Strategies ──────────────────────────────────────────────────────────

_ALPHABET = st.characters(whitelist_categories=("Ll", "Nd"), whitelist_characters=["_", "-"])

_identity: SearchStrategy[str] = st.text(alphabet=_ALPHABET, min_size=1, max_size=15)

_window_seconds: SearchStrategy[int] = st.integers(min_value=1, max_value=3600)


def _advances_strategy(min_advance: int, max_advance: int) -> SearchStrategy[list[int]]:
    return st.lists(
        st.integers(min_value=min_advance, max_value=max_advance),
        min_size=1,
        max_size=40,
    )


# ── unique_for property test ─────────────────────────────────────


@given(
    identity_s=_identity,
    window_s=_window_seconds,
    advances=_advances_strategy(1, 120),
)
@settings(
    max_examples=200,
    deadline=None,
)
async def test_unique_for_invariant(identity_s: str, window_s: int, advances: list[int]) -> None:
    """For any identity, window, and event sequence, unique_for
    dedup returns existing handle when within window, new handle otherwise."""
    window = timedelta(seconds=window_s)

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)

    identity = IdentityKey(identity_s)
    active_at: datetime | None = None
    active_job_id: JobId | None = None

    states: tuple[str, ...] = ("pending", "scheduled", "running")

    for i, advance_s in enumerate(advances):
        clock.advance(timedelta(seconds=advance_s))

        row = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="property_actor",
                queue="default",
                payload={"iteration": i},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=clock.now(),
                identity_key=identity,
                unique_for=window,
                unique_states=states,
            )
        )

        if active_job_id is not None and active_at is not None:
            within_window = (clock.now() - active_at) < window
            if within_window:
                assert row.id == active_job_id, (
                    f"Expected dedup (same job_id={active_job_id}) "
                    f"when within window: {identity_s}, "
                    f"advance={advance_s}s, window={window_s}s, "
                    f"elapsed since active={clock.now() - active_at}"
                )
            else:
                assert row.id != active_job_id, (
                    f"Expected new job when outside window: ({clock.now() - active_at} >= {window})"
                )
                active_job_id = row.id
                active_at = clock.now()
        else:
            active_job_id = row.id
            active_at = clock.now()
