"""Tests for InMemoryBackend unique_for preflight scan and idempotency-key
interaction.

Covers (identity within window → same job_id),
(clock advance past window → new job),
(different identity → both new),
(different actor → both new),
(unique_for set, no identity_key → no dedup),
(idempotency_key dedup → same job_id),
(different idempotency_key → both new),
(unique_for + idempotency_key both match → unique_for fires first),
(unique_for matches, idempotency_key differs → returns existing),
(unique_states filter excludes non-matching status),
(idempotency_key=None twice → both new).
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

# ── Helpers ────────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _unique_for_args(
    *,
    identity_key: str = "account:42",
    unique_for: timedelta = timedelta(minutes=15),
    unique_states: tuple[str, ...] | None = None,
    actor: str = "test_actor",
    idempotency_key: str | None = None,
) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        identity_key=IdentityKey(identity_key),
        unique_for=unique_for,
        unique_states=unique_states or ("pending", "scheduled", "running"),
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
    )


# ── same identity within window → existing handle ───────────────


async def test_tu1_unique_for_match_returns_existing_row() -> None:
    """Same identity within window → existing row returned, same job_id."""
    backend = _make_backend()

    row1 = await backend.enqueue(_unique_for_args())
    row2 = await backend.enqueue(_unique_for_args())

    assert row1.id == row2.id
    assert len(backend._jobs) == 1


# ── clock advance past window → new job ─────────────────────────


async def test_tu2_clock_advance_past_window_creates_new_job() -> None:
    """Same identity, advance clock past window → new job created."""
    clock = FakeClock(_START)
    backend = InMemoryBackend(clock=clock)
    unique_for: timedelta = timedelta(minutes=15)

    row1 = await backend.enqueue(_unique_for_args(unique_for=unique_for))

    clock.advance(unique_for + timedelta(seconds=1))

    row2 = await backend.enqueue(_unique_for_args(unique_for=unique_for))

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── different identity → both new ───────────────────────────────


async def test_tu3_different_identity_both_new() -> None:
    """Different identity keys (account:47 vs account:48) → both create new."""
    backend = _make_backend()

    row1 = await backend.enqueue(_unique_for_args(identity_key="account:47"))
    row2 = await backend.enqueue(_unique_for_args(identity_key="account:48"))

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── different actor → both new ──────────────────────────────────


async def test_tu4_different_actor_both_new() -> None:
    """Same identity across two different actors → both create new jobs."""
    backend = _make_backend()

    row1 = await backend.enqueue(_unique_for_args(identity_key="k", actor="actor_a"))
    row2 = await backend.enqueue(_unique_for_args(identity_key="k", actor="actor_b"))

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── unique_states filter excludes non-matching status ───────────


async def test_tu10_unique_states_filter_excludes_scheduled() -> None:
    """unique_states=('pending','running') excludes 'scheduled';
    if existing row is 'scheduled', second enqueue creates new job."""
    backend = _make_backend()
    unique_for: timedelta = timedelta(minutes=15)

    args = _unique_for_args(unique_for=unique_for, unique_states=("pending", "running"))
    row1 = await backend.enqueue(args)

    backend._jobs[row1.id] = replace(row1, status="scheduled")

    args2 = _unique_for_args(unique_for=unique_for, unique_states=("pending", "running"))
    row2 = await backend.enqueue(args2)

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── unique_for precedence over idempotency_key ─────────────────────────


async def test_unique_for_precedes_idempotency_key() -> None:
    """unique_for match returns existing row; idempotency-key index not consulted."""
    backend = _make_backend()

    row1 = await backend.enqueue(_unique_for_args(identity_key="k1", idempotency_key="idem_k1"))
    row2 = await backend.enqueue(_unique_for_args(identity_key="k1", idempotency_key="idem_k1"))

    assert row1.id == row2.id
    assert len(backend._jobs) == 1


# ── singleton precedence over idempotency_key (backend-equivalence fix) ─


async def test_singleton_precedes_idempotency_key() -> None:
    """Singleton collision raised before idempotency-key dedup (matching PG order)."""
    import pytest

    from taskq.backend._protocol import IdempotencyKey
    from taskq.exceptions import SingletonCollisionError

    clock = FakeClock(_START)
    backend = InMemoryBackend(clock=clock)

    args1 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"v": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"singleton": True},
        idempotency_key=IdempotencyKey("idem_k"),
    )

    row1 = await backend.enqueue(args1)

    args2 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"v": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        metadata={"singleton": True},
        idempotency_key=IdempotencyKey("idem_k"),
    )

    with pytest.raises(SingletonCollisionError) as exc_info:
        await backend.enqueue(args2)

    exc = exc_info.value
    assert exc.actor == "test_actor"
    assert exc.blocking_job_id == row1.id


# ── unique_for set but no identity_key → no dedup ──────────────


async def test_tu5_unique_for_no_identity_no_dedup_fresh_jobs() -> None:
    """unique_for is set but identity_key is None → no dedup occurs;
    fresh job created on every call."""
    backend = _make_backend()

    args1 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        unique_for=timedelta(minutes=15),
        unique_states=("pending", "scheduled", "running"),
    )
    row1 = await backend.enqueue(args1)

    args2 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        unique_for=timedelta(minutes=15),
        unique_states=("pending", "scheduled", "running"),
    )
    row2 = await backend.enqueue(args2)

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── idempotency_key dedup → same job_id ────────────────────────


async def test_tu6_idempotency_key_dedup_returns_same_handle() -> None:
    """Enqueue with idempotency_key; second enqueue with same key → same job_id."""
    backend = _make_backend()

    args1 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=IdempotencyKey("webhook:123"),
    )
    row1 = await backend.enqueue(args1)

    args2 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=IdempotencyKey("webhook:123"),
    )
    row2 = await backend.enqueue(args2)

    assert row1.id == row2.id
    assert len(backend._jobs) == 1


# ── different idempotency_key → both new ───────────────────────


async def test_tu7_different_idempotency_key_both_new() -> None:
    """Enqueue with idempotency_key='A', then 'B' → both create new jobs."""
    backend = _make_backend()

    args1 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=IdempotencyKey("A"),
    )
    row1 = await backend.enqueue(args1)

    args2 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=IdempotencyKey("B"),
    )
    row2 = await backend.enqueue(args2)

    assert row1.id != row2.id
    assert len(backend._jobs) == 2


# ── unique_for + idempotency_key both match → unique_for fires first


async def test_tu8_unique_for_match_skips_idempotency_insert() -> None:
    """Both unique_for and idempotency_key match → unique_for returns
    existing row; INSERT never attempted (no idempotency-index entry for second call)."""
    backend = _make_backend()
    identity: str = "account:42"
    key: str = "idem_k8"

    args1 = _unique_for_args(identity_key=identity, idempotency_key=key)
    row1 = await backend.enqueue(args1)

    assert backend._idempotency_index.get(IdempotencyKey(key)) == row1.id

    args2 = _unique_for_args(identity_key=identity, idempotency_key=key)
    row2 = await backend.enqueue(args2)

    assert row1.id == row2.id
    assert len(backend._jobs) == 1
    assert backend._idempotency_index.get(IdempotencyKey(key)) == row1.id


# ── unique_for matches, idempotency_key differs → returns existing


async def test_tu9_unique_for_match_ignores_different_idempotency_key() -> None:
    """unique_for matches but idempotency_key differs → returns existing
    handle (the second key is ignored because preflight short-circuits)."""
    backend = _make_backend()
    identity: str = "account:42"

    args1 = _unique_for_args(identity_key=identity, idempotency_key="y")
    row1 = await backend.enqueue(args1)

    args2 = _unique_for_args(identity_key=identity, idempotency_key="z")
    row2 = await backend.enqueue(args2)

    assert row1.id == row2.id
    assert len(backend._jobs) == 1
    assert IdempotencyKey("z") not in backend._idempotency_index


# ── idempotency_key=None twice → both new ──────────────────────


async def test_tn1_idempotency_key_none_twice_both_new() -> None:
    """Enqueue with idempotency_key=None twice → both create new jobs."""
    backend = _make_backend()

    args1 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    row1 = await backend.enqueue(args1)

    args2 = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    row2 = await backend.enqueue(args2)

    assert row1.id != row2.id
    assert len(backend._jobs) == 2
