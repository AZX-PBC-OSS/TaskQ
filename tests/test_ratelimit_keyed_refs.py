"""Unit tests for KeyedReservationRef and RateLimitRegistry dynamic resolution.

Tests ``KeyedReservationRef`` validation, ``RateLimitRegistry._resolve_reservation_name``
dynamic key resolution/lazy registration, ``acquire_for_actor`` composing static and
keyed reservations, and ``evict_idle_keyed_reservations``. Mirrors the in-memory
(``FakeClock``-backed ``ConcurrencyReservation``) conventions of
``tests/test_ratelimit_registry.py`` and ``tests/test_ratelimit_composition.py`` — no
Redis or PG instance required, so every call passes ``pg_pool=None`` (skipping the
``ensure_slots()`` call ``_resolve_reservation_name`` makes when a real pool is
given — that path is exercised against real Postgres in
``tests/test_ratelimit_keyed_refs_pg.py``).
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_uuid
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _default_key_fn(payload: dict[str, object]) -> str:
    return str(payload["session_id"])


def _reservation(
    name: str = "res",
    slots: int = 4,
    lease: timedelta = timedelta(seconds=10),
    clock: FakeClock | None = None,
) -> ConcurrencyReservation:
    if clock is None:
        clock = FakeClock(_START)
    return ConcurrencyReservation(name=name, slots=slots, lease=lease, clock=clock)


def _keyed_ref(
    base_name: str = "session-cap",
    slots: int = 3,
    lease: timedelta = timedelta(minutes=5),
    key_fn: Callable[[dict[str, object]], str] = _default_key_fn,
) -> KeyedReservationRef:
    return KeyedReservationRef(base_name=base_name, key_fn=key_fn, slots=slots, lease=lease)


# ── KeyedReservationRef validation ──────────────────────────────


class TestKeyedReservationRefValidation:
    def test_construction(self) -> None:
        ref = _keyed_ref()
        assert ref.base_name == "session-cap"
        assert ref.slots == 3
        assert ref.lease == timedelta(minutes=5)

    def test_rejects_empty_base_name(self) -> None:
        with pytest.raises(ValueError, match="base_name must not be empty"):
            _keyed_ref(base_name="")

    def test_rejects_slots_below_one(self) -> None:
        with pytest.raises(ValueError, match="slots must be >= 1"):
            _keyed_ref(slots=0)

    def test_rejects_negative_slots(self) -> None:
        with pytest.raises(ValueError, match="slots must be >= 1"):
            _keyed_ref(slots=-1)

    def test_rejects_zero_lease(self) -> None:
        with pytest.raises(ValueError, match="lease must be > 0"):
            _keyed_ref(lease=timedelta(0))

    def test_rejects_negative_lease(self) -> None:
        with pytest.raises(ValueError, match="lease must be > 0"):
            _keyed_ref(lease=timedelta(seconds=-1))

    def test_accepts_slots_equal_one(self) -> None:
        ref = _keyed_ref(slots=1)
        assert ref.slots == 1


# ── _resolve_reservation_name: plain string passthrough ─────────


async def test_resolve_plain_string_returns_unchanged() -> None:
    """A plain str reservation ref is returned as-is by _resolve_reservation_name."""
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu"))

    name = await reg._resolve_reservation_name("gpu", payload=None, pg_pool=None, settings=None)  # pyright: ignore[reportPrivateUsage] # Why: exercising private resolution helper directly, matching conftest's precedent for accessing registry internals in tests.

    assert name == "gpu"


async def test_resolve_plain_string_ignores_payload() -> None:
    """A plain str ref does not consult payload at all — works even with payload=None."""
    reg = RateLimitRegistry()
    reg.register(_reservation("gpu"))

    name = await reg._resolve_reservation_name(
        "gpu", payload={"unrelated": "data"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "gpu"


# ── _resolve_reservation_name: KeyedReservationRef dynamic resolution ──


async def test_resolve_keyed_ref_produces_base_name_colon_key() -> None:
    """A KeyedReservationRef resolves to f'{base_name}:{key}' and lazily registers it."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="geocode-session", slots=3, lease=timedelta(minutes=5))

    name = await reg._resolve_reservation_name(
        ref, payload={"session_id": "abc123"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "geocode-session:abc123"
    registered = reg.get_reservation("geocode-session:abc123")
    assert registered.slots == 3
    assert registered.lease == timedelta(minutes=5)


async def test_resolve_keyed_ref_reuses_same_instance_for_same_key() -> None:
    """Two resolutions for the same key reuse the same registered primitive —
    not a duplicate registration."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    name1 = await reg._resolve_reservation_name(
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    first_instance = reg.get_reservation(name1)
    assert len(reg.reservations) == 1

    name2 = await reg._resolve_reservation_name(
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    second_instance = reg.get_reservation(name2)

    assert name1 == name2 == "session-cap:s1"
    assert first_instance is second_instance
    assert len(reg.reservations) == 1


async def test_resolve_keyed_ref_different_keys_register_independently() -> None:
    """Two different keys for the same ref produce two independent registry entries."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", slots=2)

    name_a = await reg._resolve_reservation_name(
        ref, payload={"session_id": "a"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    name_b = await reg._resolve_reservation_name(
        ref, payload={"session_id": "b"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name_a == "session-cap:a"
    assert name_b == "session-cap:b"
    assert len(reg.reservations) == 2
    assert reg.get_reservation(name_a) is not reg.get_reservation(name_b)


async def test_different_keys_do_not_share_slot_capacity() -> None:
    """Two different keys for the same KeyedReservationRef are independent
    concurrency pools — exhausting one key's slots does not affect the other's.

    Pre-registers both concrete reservations with a FakeClock (in-memory
    table) for deterministic, fast slot-exhaustion assertions — the lazy
    PG-backed construction path itself is exercised separately in
    ``tests/test_ratelimit_keyed_refs_pg.py``.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        ConcurrencyReservation(
            name="session-cap:a", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )
    reg.register(
        ConcurrencyReservation(
            name="session-cap:b", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )
    ref = _keyed_ref(base_name="session-cap", slots=1, lease=timedelta(seconds=30))

    acquired_a = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload={"session_id": "a"},
        clock=clock,
    )
    assert acquired_a[0].name == "session-cap:a"

    # Key "a"'s single slot is now held; key "b" is untouched and still acquirable.
    acquired_b = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload={"session_id": "b"},
        clock=clock,
    )
    assert acquired_b[0].name == "session-cap:b"

    from taskq.exceptions import ReservationUnavailable

    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "a"},
            clock=clock,
        )


async def test_resolve_keyed_ref_missing_payload_raises_value_error() -> None:
    """payload=None with a KeyedReservationRef raises ValueError."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    with pytest.raises(ValueError, match="no payload was provided"):
        await reg._resolve_reservation_name(ref, payload=None, pg_pool=None, settings=None)  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_empty_key_raises_value_error() -> None:
    """key_fn returning an empty string raises ValueError."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda p: "")

    with pytest.raises(ValueError, match="returned an empty key"):
        await reg._resolve_reservation_name(
            ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_fn_exception_propagates() -> None:
    """An exception raised by key_fn itself is not swallowed — it propagates to the
    caller of _resolve_reservation_name / acquire_for_actor."""
    reg = RateLimitRegistry()

    def _boom(payload: dict[str, object]) -> str:
        raise RuntimeError("key derivation exploded")

    ref = _keyed_ref(base_name="session-cap", key_fn=_boom)

    with pytest.raises(RuntimeError, match="key derivation exploded"):
        await reg._resolve_reservation_name(
            ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_fn_missing_dict_key_propagates_keyerror() -> None:
    """key_fn raising KeyError (e.g. payload missing the expected field) propagates."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")  # key_fn does p["session_id"]

    with pytest.raises(KeyError):
        await reg._resolve_reservation_name(
            ref, payload={"unrelated": "value"}, pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


# ── acquire_for_actor: AND-composition with keyed reservations ──


async def test_acquire_for_actor_composes_static_and_keyed_reservations() -> None:
    """A static name and a KeyedReservationRef in the same reservations list are
    both acquired — AND-composition holds for mixed static/keyed lists.

    The dynamic reservation is pre-registered here with a FakeClock so that
    resolution reuses it via the existing idempotent-register path (register()
    no-ops for identical config) — deterministic and fast, in-memory only.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    static_res = _reservation("global-cap", slots=2, clock=clock)
    reg.register(static_res)
    reg.register(
        ConcurrencyReservation(
            name="session-cap:abc", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )

    ref = _keyed_ref(base_name="session-cap", slots=1, lease=timedelta(seconds=30))
    job_id = new_uuid()
    worker_id = new_uuid()

    acquired = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=["global-cap", ref],
        job_id=job_id,
        worker_id=worker_id,
        payload={"session_id": "abc"},
        clock=clock,
    )

    assert len(acquired) == 2
    assert acquired[0].name == "global-cap"
    assert acquired[1].name == "session-cap:abc"

    # session-cap:abc had only 1 slot and it is now held — a second acquisition
    # for the same key must be denied, proving the keyed reservation's own
    # capacity was actually consumed (not just recorded as a handle).
    from taskq.exceptions import ReservationUnavailable

    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "abc"},
            clock=clock,
        )


async def test_acquire_for_actor_keyed_only_still_and_composes_with_rate_limit() -> None:
    """A KeyedReservationRef alongside a rate limit still AND-composes: both acquired."""
    from taskq.ratelimit.token_bucket import TokenBucket

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        ConcurrencyReservation(
            name="session-cap:xyz", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )
    reg.register(TokenBucket(name="tb", capacity=5.0, refill_per_second=1.0, backend="memory"))

    ref = _keyed_ref(base_name="session-cap", slots=1, lease=timedelta(seconds=30))
    job_id = new_uuid()
    worker_id = new_uuid()

    acquired = await reg.acquire_for_actor(
        rate_limits=["tb"],
        reservations=[ref],
        job_id=job_id,
        worker_id=worker_id,
        payload={"session_id": "xyz"},
        clock=clock,
    )

    assert len(acquired) == 2
    assert acquired[0].name == "session-cap:xyz"
    assert acquired[1].name == "tb"


# ── evict_idle_keyed_reservations ────────────────────────────────


async def test_evict_idle_keyed_reservations_removes_only_stale_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entries idle >= idle_for are evicted; recently-used entries are kept."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "stale"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    fake_time = 1100.0  # 100s later — "stale" key untouched since
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "fresh"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(seconds=50))

    assert evicted == 1
    assert "session-cap:stale" not in reg.reservations
    assert "session-cap:fresh" in reg.reservations


async def test_evict_idle_keyed_reservations_leaves_static_reservations_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A statically-registered (non-keyed) reservation is never evicted, even
    when it has been present far longer than idle_for — eviction only ever
    consults _keyed_reservation_last_used, which static registrations never
    populate."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    reg.register(_reservation("static-global"))

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 5000.0)

    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(seconds=1))

    assert evicted == 0
    assert "static-global" in reg.reservations


async def test_evict_idle_keyed_reservations_returns_zero_when_nothing_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No entries idle beyond the threshold — returns 0, registry unchanged."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 42.0)
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "recent"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(hours=1))

    assert evicted == 0
    assert "session-cap:recent" in reg.reservations


async def test_evict_idle_keyed_reservations_re_registration_after_eviction_is_idempotent() -> None:
    """A key evicted and then acquired again is simply re-registered — no error,
    and the registry converges back to one entry for that key."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", slots=3, lease=timedelta(minutes=5))

    await reg._resolve_reservation_name(
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    reg._reservations.pop("session-cap:s1")  # pyright: ignore[reportPrivateUsage] # Why: simulating what evict_idle_keyed_reservations does, without needing monotonic control here.
    reg._keyed_reservation_last_used.pop("session-cap:s1")  # pyright: ignore[reportPrivateUsage]

    name = await reg._resolve_reservation_name(
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "session-cap:s1"
    assert len(reg.reservations) == 1
