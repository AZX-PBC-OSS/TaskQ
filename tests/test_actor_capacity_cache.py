"""Tests for operator-owned ``max_pending`` resolution at enqueue time.

``max_pending`` is a capacity field: the ``@actor(max_pending=...)``
literal only seeds the stored ``actor_config`` row; once a row exists, a
non-NULL stored value is authoritative. Because ``enqueue()`` is a hot
path, the client does not query ``actor_config`` per enqueue — it holds
a TTL-bounded :class:`~taskq.client._capacity.ActorCapacityCache`
snapshot of the table (default 5s staleness bound, explicit invalidation
via ``JobsClient.invalidate_actor_capacity_cache``).

Resolution rule: **a non-NULL stored value wins over the code literal**
(no row → literal; row with NULL → literal — "clear" reverts to the
code default). An explicit per-call ``max_pending=`` argument is honored
in the tightening direction against a stored cap (``min(stored,
per_call)`` — load shedding is never widened by an operator override),
and wins outright against the literal when nothing is stored
(historical behavior — actor code may loosen its own declaration, never
an operator's cap).

Unit tier here uses ``InMemoryBackend`` whose ``_actor_configs_meta``
map plays the role of the stored table; PG integration coverage lives in
``test_actor_config_ops.py`` / ``test_dispatch_pg.py``-style integration
tests.
"""

import asyncio
import math
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import BaseModel

from taskq.actor import ActorRef, actor
from taskq.backend._protocol import Backend
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


# ── Per-call argument resolution ────────────────────────────────────────


async def test_per_call_tighter_than_stored_is_honored() -> None:
    """Load shedding: an explicit per-call cap tighter than the stored
    (operator) value must be honored — ``min(stored, per_call)``.

    The stored value here is 100 only because the seeding path copied a
    literal into the row; that must not widen a caller explicitly asking
    to admit at most 1 pending job.
    """
    backend = _make_backend()
    backend.register_actor_config(actor="cap_shed", max_pending=100)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast(
            "asyncpg.Pool", object()
        ),  # Why: only the None-check reads the pool on this path; the backend does the work.
        backend=backend,
    )
    ref = _uncapped("cap_shed")

    await enqueuer.enqueue(ref, _Payload(value=1), max_pending=1)
    with pytest.raises(MaxPendingExceededError):
        await enqueuer.enqueue(ref, _Payload(value=2), max_pending=1)


async def test_per_call_looser_than_stored_does_not_widen() -> None:
    """The other direction of the same rule: no code path can raise an
    operator's fleet cap — min(stored=1, per_call=100) stays 1."""
    backend = _make_backend()
    backend.register_actor_config(actor="cap_sub", max_pending=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast(
            "asyncpg.Pool", object()
        ),  # Why: only the None-check reads the pool on this path; the backend does the work.
        backend=backend,
        capacity_cache=ActorCapacityCache(backend),
    )
    ref = _uncapped("cap_sub")

    await enqueuer.enqueue(ref, _Payload(value=1), max_pending=100)
    with pytest.raises(MaxPendingExceededError):
        await enqueuer.enqueue(ref, _Payload(value=2), max_pending=100)


async def test_per_call_widens_past_literal_when_nothing_stored() -> None:
    """Historical behavior preserved: with no stored cap, an explicit
    per-call argument may loosen the actor's own literal — actor code
    owns its declaration; only the operator cap is a hard ceiling."""
    backend = _make_backend()
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=cast("asyncpg.Pool", object()),  # Why: see above.
        backend=backend,
    )
    ref = _literal_capped("cap_loosen", 1)

    await enqueuer.enqueue(ref, _Payload(value=1), max_pending=100)
    # Literal is 1 — pre-change resolution would raise here; the explicit
    # per-call widening wins because nothing is stored.
    await enqueuer.enqueue(ref, _Payload(value=2), max_pending=100)


async def test_per_call_without_stored_row_keeps_tighter_param() -> None:
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


# ── Construction guards ────────────────────────────────────────────────


async def test_first_use_rejects_backend_without_get_actor_max_pending() -> None:
    """Contract drift fails fast at first use, not silently.

    A backend built before ``get_actor_max_pending`` existed would
    otherwise hit AttributeError inside the fail-open handler on every
    refresh and silently enforce code literals forever — the exact
    silent drift the protocol version exists to prevent. Construction
    stays cheap and harmless for partial doubles; the first capacity
    resolution raises.
    """

    class _LegacyBackend:
        pass

    cache = ActorCapacityCache(cast("Backend", _LegacyBackend()))
    with pytest.raises(TypeError, match="get_actor_max_pending"):
        await cache.effective_max_pending("any_actor", None)


def test_constructor_rejects_non_positive_read_timeout() -> None:
    backend = _make_backend()
    with pytest.raises(ValueError, match="read_timeout"):
        ActorCapacityCache(backend, read_timeout=0.0)


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


async def test_positive_ttl_expires_naturally_then_refresh_picks_it_up() -> None:
    """A positive TTL bounds staleness: once the window elapses, the next
    read refreshes on its own — no explicit invalidation required.

    The within-TTL staleness test and the ttl=0 tests both survive the
    narrower mutation (a positive-TTL snapshot that never expires) —
    only a test that lets the window actually elapse catches it.
    """
    backend = _make_backend()
    backend.register_actor_config(actor="cap_aging", max_pending=1)
    client = JobsClient(backend, capacity_cache_ttl=0.05)
    ref = _uncapped("cap_aging")

    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))

    # Operator raises the cap; nobody invalidates. Once the TTL has
    # elapsed, the very next enqueue must see the new value.
    backend.register_actor_config(actor="cap_aging", max_pending=100)
    await asyncio.sleep(0.1)
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


async def test_failed_refresh_retains_last_good_snapshot() -> None:
    """The module's fail-open contract is 'fall back to the LAST GOOD
    SNAPSHOT' — not to an empty table.

    Prime a good snapshot (stored cap 1), then make every refresh fail:
    the retained rows must keep enforcing the cap. A regression that
    reset ``_rows`` on failure would silently drop enforcement to the
    (uncapped) literal — and the previous suite could not see it because
    every failure test started from an empty table, where {} and
    'retained' look identical.
    """
    backend = _make_backend()
    backend.register_actor_config(actor="cap_retain", max_pending=1)
    client = JobsClient(backend, capacity_cache_ttl=0.0)  # refresh on every call
    ref = _uncapped("cap_retain")

    # Prime the snapshot: stored cap 1 is enforced.
    await client.enqueue(ref, _Payload(value=1))
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=2))

    async def always_fails() -> dict[str, int | None]:
        raise OSError("down")

    object.__setattr__(backend, "get_actor_max_pending", always_fails)

    # Refresh now fails on every call: the retained snapshot (cap 1)
    # must still reject the overflow — not silently reset to uncapped.
    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(ref, _Payload(value=3))


async def test_concurrent_enqueues_share_one_refresh() -> None:
    """Single-flight: N concurrent enqueues with a cold cache trigger
    exactly one backend read, not N.

    The backend double genuinely suspends (an asyncio.Event gate), so
    ``calls == 1`` holds because of the single-flight lock, not because
    the in-memory read happens to complete without yielding — deleting
    the lock makes every waiter start its own read and this fails.
    """
    backend = _make_backend()
    real_impl = backend.get_actor_max_pending
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def gated() -> dict[str, int | None]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()  # genuine suspension point
        return await real_impl()

    object.__setattr__(backend, "get_actor_max_pending", gated)

    client = JobsClient(backend)  # default TTL; cache cold at gather time
    ref = _uncapped("cap_single_flight")

    tasks = [asyncio.create_task(client.enqueue(ref, _Payload(value=i))) for i in range(10)]
    # Let the first task reach the gated read and the rest queue on the
    # single-flight lock before releasing the gate.
    await started.wait()
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(*tasks)
    assert calls == 1


async def test_invalidate_during_in_flight_refresh_discards_result() -> None:
    """Behavioral contract of ``invalidate()``: "the next read refreshes
    from the backend" — even when a read was already in flight.

    Scenario: a refresh reads cap=1 but its response is delayed; while
    it is in flight the operator raises the cap to 5 and invalidates the
    cache. When the delayed read completes it carries the PRE-change
    snapshot. If that result were allowed to re-stamp the cache, every
    caller would keep seeing cap=1 for another full TTL — the opposite
    of what the invalidation was for. Callers must see cap=5 on the
    very next read.
    """
    backend = _make_backend()
    backend.register_actor_config(actor="cap_inv", max_pending=1)
    real_impl = backend.get_actor_max_pending
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed() -> dict[str, int | None]:
        snapshot = await real_impl()  # reads cap=1 ...
        started.set()
        await release.wait()  # ... but the response lands later
        return snapshot

    object.__setattr__(backend, "get_actor_max_pending", delayed)

    cache = ActorCapacityCache(backend)  # default 5s TTL
    first = asyncio.create_task(cache.effective_max_pending("cap_inv", None))
    await started.wait()

    # Operator raises the cap and tells the cache to drop its snapshot.
    backend.register_actor_config(actor="cap_inv", max_pending=5)
    cache.invalidate()

    # The delayed pre-change read now completes.
    release.set()
    await first

    # The next caller must see the operator's new value, not the
    # pre-invalidation snapshot — inside the TTL window.
    assert await cache.effective_max_pending("cap_inv", None) == 5


async def test_refresh_read_timeout_fails_open_instead_of_hanging() -> None:
    """An exhausted/dead backend pool must not wedge enqueue behind the
    single-flight lock.

    Without the wait_for, a pool whose connections are all checked out
    blocks the refresh indefinitely *while holding the lock*, stacking
    up every other enqueue in the process. The timeout turns the hang
    into the module's documented fail-open, and the lock is released.
    """
    backend = _make_backend()
    never = asyncio.Event()

    async def hangs() -> dict[str, int | None]:
        await never.wait()
        return {}

    object.__setattr__(backend, "get_actor_max_pending", hangs)

    cache = ActorCapacityCache(backend, read_timeout=0.05)
    start = time.monotonic()
    # Literal fallback applies and the call returns promptly.
    assert await cache.effective_max_pending("cap_timeout", 7) == 7
    assert time.monotonic() - start < 2.0
    # The lock was released: the next caller takes the same bounded
    # fail-open path (failure stamped → no retry within the TTL).
    assert await cache.effective_max_pending("cap_timeout", 7) == 7
    assert time.monotonic() - start < 2.0


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


# ── Backend surface ─────────────────────────────────────────────────────


async def test_in_memory_get_actor_max_pending_mirrors_registered_meta() -> None:
    """The in-memory backend exposes its registered actor_config meta,
    including the row-exists-with-NULL case."""
    backend = _make_backend()
    backend.register_actor_config(actor="mp_a", max_pending=3)
    backend.register_actor_config(actor="mp_b")

    assert await backend.get_actor_max_pending() == {"mp_a": 3, "mp_b": None}


# ── Constructor validation ──────────────────────────────────────────────


def test_read_timeout_inf_rejected() -> None:
    """inf passes `> 0` but asyncio.wait_for(timeout=inf) doesn't bound
    the wait — the exact indefinite lock-held hang the timeout was added
    to prevent.  Same isfinite guard as result_ttl validation."""
    backend = _make_backend()
    with pytest.raises(ValueError, match="read_timeout"):
        ActorCapacityCache(backend, read_timeout=math.inf)


def test_read_timeout_nan_rejected() -> None:
    """NaN passes `> 0` (nan comparisons are always False, but nan > 0 is
    False so it *would* be caught by `<= 0`) — however it should still be
    rejected for the same reason as inf: a non-finite timeout provides no
    bound, and NaN in asyncio.wait_for is undefined behavior."""
    backend = _make_backend()
    with pytest.raises(ValueError, match="read_timeout"):
        ActorCapacityCache(backend, read_timeout=math.nan)


# ── Backend contract drift ──────────────────────────────────────────────


async def test_backend_with_none_max_pending_method_raises_type_error() -> None:
    """A backend that sets ``get_actor_max_pending = None`` (e.g. a stub
    that declares the attribute but doesn't implement it) must raise
    TypeError, not silently fall through to the fail-open path.

    ``hasattr`` returns True for a None-valued attribute, so the current
    guard misses this. The check must verify the attribute is callable,
    not merely present.
    """
    backend = _make_backend()
    object.__setattr__(backend, "get_actor_max_pending", None)
    cache = ActorCapacityCache(backend)
    with pytest.raises(TypeError, match="get_actor_max_pending"):
        await cache.effective_max_pending("cap_none_method", 1)
