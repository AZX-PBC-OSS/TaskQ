"""Tests for operator-owned ``max_pending`` resolution at enqueue time.

``max_pending`` is a capacity field: the ``@actor(max_pending=...)``
literal only seeds the stored ``actor_config`` row; once a row exists, a
non-NULL stored value is authoritative. Because ``enqueue()`` is a hot
path, the client does not query ``actor_config`` per enqueue — it holds
a TTL-bounded :class:`~taskq.client._capacity.ActorCapacityCache`
snapshot of the table (default 5s staleness bound, explicit invalidation
via ``JobsClient.invalidate_actor_capacity_cache``).

Resolution rule, matching ``result_ttl``'s existing precedent:
**a non-NULL stored value wins; otherwise the code literal applies**
(no row → literal; row with NULL → literal — "clear" reverts to the
code default).

Unit tier here uses ``InMemoryBackend`` whose ``_actor_configs_meta``
map plays the role of the stored table; PG integration coverage lives in
``test_actor_config_ops.py`` / ``test_dispatch_pg.py``-style integration
tests.
"""

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import BaseModel

from taskq.actor import ActorRef, actor
from taskq.batch import EnqueueItem
from taskq.client._capacity import ActorCapacityCache
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._jobs import JobsClient
from taskq.exceptions import MaxPendingExceededError, PartialBatchError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    import asyncpg

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _uncapped(name: str) -> ActorRef[_Payload, None]:
    @actor(name=name)
    async def _a(payload: _Payload) -> None: ...

    return _a


def _literal_capped(name: str, max_pending: int) -> ActorRef[_Payload, None]:
    @actor(name=name, max_pending=max_pending)
    async def _a(payload: _Payload) -> None: ...

    return _a


# ── Resolution semantics ────────────────────────────────────────────────


async def test_stored_max_pending_wins_over_literal() -> None:
    """Stored value (operator-tuned) beats the @actor literal.

    Literal is 100 but the stored row says 2: the third enqueue must
    raise. Pre-fix this used the literal and sailed through.
    """
    backend = _make_backend()
    backend.register_actor_config(actor="cap_stored_wins", max_pending=2)
    client = JobsClient(backend)
    ref = _literal_capped("cap_stored_wins", 100)

    await client.enqueue(ref, _Payload(value=1))
    await client.enqueue(ref, _Payload(value=2))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=3))


async def test_stored_null_reverts_to_literal() -> None:
    """Row exists but max_pending is NULL (cleared override) → literal applies."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_stored_null")  # NULL max_pending
    client = JobsClient(backend)
    ref = _literal_capped("cap_stored_null", 1)

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))


async def test_no_stored_row_uses_literal() -> None:
    """No actor_config row at all (never synced) → literal applies."""
    backend = _make_backend()
    client = JobsClient(backend)
    ref = _literal_capped("cap_no_row", 1)

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))


async def test_operator_can_cap_actor_with_no_literal() -> None:
    """The capability this feature adds: an actor declared without
    max_pending can be capped live by the operator — no redeploy."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_fresh", max_pending=1)
    client = JobsClient(backend)
    ref = _uncapped("cap_fresh")

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))


async def test_stored_zero_rejects_every_enqueue() -> None:
    """Boundary: max_pending=0 means 'shed all load' — even the first
    enqueue raises (0 >= 0)."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_zero", max_pending=0)
    client = JobsClient(backend)
    ref = _literal_capped("cap_zero", 100)

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=1))


# ── Staleness, invalidation, refresh ────────────────────────────────────


async def test_change_within_ttl_is_stale_then_invalidate_picks_it_up() -> None:
    """Bounded staleness: an operator change inside the TTL window is not
    visible until the snapshot refreshes; explicit invalidation forces it."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_tuned", max_pending=1)
    client = JobsClient(backend)  # default TTL: 5s — the test runs well within it
    ref = _uncapped("cap_tuned")

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))

    # Operator raises the cap. The client must NOT see it yet (stale).
    backend.register_actor_config(actor="cap_tuned", max_pending=100)
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))

    # After invalidation the new cap applies.
    client.invalidate_actor_capacity_cache()
    await client.enqueue(ref, _Payload(value=2))


async def test_zero_ttl_refreshes_on_every_enqueue() -> None:
    """ttl=0 disables reuse: every enqueue sees the latest stored value."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_live", max_pending=1)
    client = JobsClient(backend, capacity_cache_ttl=0.0)
    ref = _uncapped("cap_live")

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))

    backend.register_actor_config(actor="cap_live", max_pending=100)
    await client.enqueue(ref, _Payload(value=2))


async def test_refresh_failure_falls_back_to_literal_then_recovers() -> None:
    """A failed refresh must not break enqueue: fall back to the literal
    (or last-known snapshot), log, and retry no sooner than the TTL."""
    backend = _make_backend()
    real_impl = backend.get_actor_max_pending
    calls = 0

    async def flaky() -> dict[str, int | None]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated transient PG failure")
        return await real_impl()

    object.__setattr__(backend, "get_actor_max_pending", flaky)

    client = JobsClient(backend, capacity_cache_ttl=0.0)
    ref = _literal_capped("cap_flaky", 1)

    # First enqueue: refresh fails → literal (1) applies, enqueue succeeds.
    await client.enqueue(ref, _Payload(value=1))
    # Second enqueue (ttl=0 → refresh retried, succeeds, table empty):
    # literal 1 still applies and the queue is full → raises.
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))
    assert calls == 2


async def test_concurrent_enqueues_share_one_refresh() -> None:
    """Single-flight: N concurrent enqueues with a cold cache trigger
    exactly one backend read, not N."""
    backend = _make_backend()
    real_impl = backend.get_actor_max_pending
    calls = 0

    async def spy() -> dict[str, int | None]:
        nonlocal calls
        calls += 1
        return await real_impl()

    object.__setattr__(backend, "get_actor_max_pending", spy)

    client = JobsClient(backend)  # default TTL; cache cold at gather time
    ref = _uncapped("cap_single_flight")

    await asyncio.gather(*(client.enqueue(ref, _Payload(value=i)) for i in range(10)))
    assert calls == 1


async def test_refresh_failure_is_retried_no_more_often_than_ttl() -> None:
    """A sick backend must not turn every enqueue into a failing query:
    after a failure, further enqueues reuse the fallback until the TTL."""
    backend = _make_backend()
    calls = 0

    async def always_fails() -> dict[str, int | None]:
        nonlocal calls
        calls += 1
        raise OSError("down")

    object.__setattr__(backend, "get_actor_max_pending", always_fails)

    client = JobsClient(backend)  # default TTL 5s
    ref = _uncapped("cap_backoff")

    for i in range(5):
        await client.enqueue(ref, _Payload(value=i))
    assert calls == 1


# ── Batch path ──────────────────────────────────────────────────────────


async def test_enqueue_batch_honors_stored_limit() -> None:
    """The aggregated batch check uses the same resolution: stored 2 with
    no literal → a batch of 3 raises before inserting anything."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_batch", max_pending=2)
    client = JobsClient(backend)
    ref = _uncapped("cap_batch")

    items = [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(3)]
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue_batch(items)


async def test_enqueue_batch_stored_null_uses_literal() -> None:
    """Batch path, cleared override: literal 2 still bounds the batch."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_batch_null")
    client = JobsClient(backend)
    ref = _literal_capped("cap_batch_null", 2)

    items = [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(3)]
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue_batch(items)


async def test_enqueue_batch_operator_raised_cap_above_literal() -> None:
    """Operator raises the cap ABOVE the literal: the batch must fit.

    The aggregated phase-2 check resolves the effective (stored) limit;
    the per-item check inside the backend must see the SAME resolved
    value — if the items carried the stale literal, the in-memory
    backend's per-item check would reject the batch the client just
    admitted (a divergence the PG batch INSERT does not have)."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_batch_raised", max_pending=10)
    client = JobsClient(backend)
    ref = _literal_capped("cap_batch_raised", 1)

    items = [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(5)]
    handle = await client.enqueue_batch(items)
    assert handle.size == 5


async def test_sub_enqueuer_batch_honors_stored_limit() -> None:
    """Sub-enqueued batches resolve the same way: the per-item args carry
    the effective (stored) limit, so the autonomous loop's per-item check
    rejects the overflow — surfaced as PartialBatchError with the
    MaxPendingExceededError inside (documented autonomous-path semantics)."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_sub_batch", max_pending=2)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast("asyncpg.Pool", object()),  # Why: see above.
        backend=backend,
    )
    ref = _uncapped("cap_sub_batch")

    items = [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(3)]
    with pytest.raises(PartialBatchError) as exc_info:
        await enqueuer.enqueue_batch(items)
    assert any(isinstance(exc, MaxPendingExceededError) for _, exc in exc_info.value.failed_items)


# ── Multi-process agreement ─────────────────────────────────────────────


async def test_two_clients_enforce_the_same_stored_limit() -> None:
    """Each process keeps its own snapshot, but both read the same table:
    a job enqueued through client A counts against client B's limit."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_fleet", max_pending=1)
    client_a = JobsClient(backend)
    client_b = JobsClient(backend)
    ref = _uncapped("cap_fleet")

    await client_a.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client_b.enqueue(ref, _Payload(value=2))


# ── SubJobEnqueuer path ─────────────────────────────────────────────────


async def test_sub_enqueuer_stored_wins_over_per_call_param() -> None:
    """An operator-set stored value is authoritative even over an explicit
    per-call ``max_pending=`` argument: capacity is operator-owned, so no
    code path can assert its own."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_sub", max_pending=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast("asyncpg.Pool", object()),  # Why: only the None-check reads the pool on this path; the backend does the work.
        backend=backend,
        capacity_cache=ActorCapacityCache(backend),
    )
    ref = _uncapped("cap_sub")

    await enqueuer.enqueue(ref, _Payload(value=1), max_pending=100)
    with pytest.raises(MaxPendingExceededError):
        await enqueuer.enqueue(ref, _Payload(value=2), max_pending=100)


async def test_sub_enqueuer_without_stored_row_keeps_per_call_param() -> None:
    """No stored row: the explicit per-call param behaves exactly as before."""
    backend = _make_backend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast("asyncpg.Pool", object()),  # Why: see above.
        backend=backend,
    )
    ref = _uncapped("cap_sub_param")

    await enqueuer.enqueue(ref, _Payload(value=1), max_pending=1)
    with pytest.raises(MaxPendingExceededError):
        await enqueuer.enqueue(ref, _Payload(value=2), max_pending=1)


# ── Backend surface ─────────────────────────────────────────────────────


async def test_in_memory_get_actor_max_pending_mirrors_registered_meta() -> None:
    """The in-memory backend exposes its registered actor_config meta,
    including the row-exists-with-NULL case."""
    backend = _make_backend()
    backend.register_actor_config(actor="mp_a", max_pending=3)
    backend.register_actor_config(actor="mp_b")

    assert await backend.get_actor_max_pending() == {"mp_a": 3, "mp_b": None}
