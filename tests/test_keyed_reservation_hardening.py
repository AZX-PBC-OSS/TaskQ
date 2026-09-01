"""Regression tests for KeyedReservationRef hardening fixes.

Covers:
- K1: Key length cap and character validation in ``_resolve_reservation_name``.
- K1: ``max_keyed_reservations`` guardrail raises ``ReservationUnavailable``.
- K2: Race condition between eviction and lazy registration.
- K3: ``evict_idle_keyed_reservations`` wired into the leader sweep loop.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from taskq._ids import new_uuid
from taskq.backend.clock import Clock
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext

pytestmark = pytest.mark.asyncio


def _default_key_fn(payload: dict[str, object]) -> str:
    return str(payload["session_id"])


def _keyed_ref(
    base_name: str = "session-cap",
    slots: int = 3,
    lease: timedelta = timedelta(minutes=5),
    key_fn: Callable[[dict[str, object]], str] = _default_key_fn,
) -> KeyedReservationRef:
    return KeyedReservationRef(base_name=base_name, key_fn=key_fn, slots=slots, lease=lease)


def _settings(max_keyed: int = 10000) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_MAX_KEYED_RESERVATIONS": str(max_keyed),
        },
        validate=False,
    )


# ── K1: Key length cap ────────────────────────────────────────────────


async def test_key_length_cap_rejects_keys_exceeding_255_chars() -> None:
    """A key longer than 255 characters raises ValueError."""
    reg = RateLimitRegistry()
    long_key = "a" * 256
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda _p: long_key)

    with pytest.raises(ValueError, match="exceeds the maximum"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: exercising private resolution helper directly, matching precedent in test_ratelimit_keyed_refs.py.
            ref, payload={"session_id": "x"}, pg_pool=None, settings=None
        )


async def test_key_length_cap_accepts_key_of_exactly_255_chars() -> None:
    """A key of exactly 255 characters is accepted."""
    reg = RateLimitRegistry()
    key = "a" * 255
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda _p: key)

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "x"}, pg_pool=None, settings=None
    )

    assert name == f"session-cap:{key}"


# ── K1: Key character validation ──────────────────────────────────────


@pytest.mark.parametrize(
    "invalid_key",
    [
        "key with spaces",
        "key;DROP TABLE",
        "key\x00null",
        "key\nnewline",
        "key\ttab",
        "key@symbol",
        "key/slash",
        "key#hash",
    ],
)
async def test_key_character_validation_rejects_invalid_characters(
    invalid_key: str,
) -> None:
    """Keys containing characters outside [A-Za-z0-9_\\-:.] raise ValueError."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda _p: invalid_key)

    with pytest.raises(ValueError, match="outside the allowed set"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "x"}, pg_pool=None, settings=None
        )


@pytest.mark.parametrize(
    "valid_key",
    [
        "simple",
        "abc123",
        "with-dash",
        "with_underscore",
        "with:colon",
        "with.dot",
        "mix-A1.b2:c3_d4",
    ],
)
async def test_key_character_validation_accepts_valid_keys(valid_key: str) -> None:
    """Keys matching [A-Za-z0-9_\\-:.]+ are accepted."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda _p: valid_key)

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "x"}, pg_pool=None, settings=None
    )

    assert name == f"session-cap:{valid_key}"


# ── K1: max_keyed_reservations guard ──────────────────────────────────


async def test_max_keyed_reservations_guard_raises_reservation_unavailable() -> None:
    """When the number of keyed reservation entries reaches the limit, a new
    key raises ReservationUnavailable."""
    settings = _settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
        )


async def test_max_keyed_reservations_guard_allows_reusing_existing_key() -> None:
    """Re-resolving an already-tracked key does not trip the guard even at the limit."""
    settings = _settings(max_keyed=1)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    assert len(reg._keyed_reservation_last_used) == 1  # pyright: ignore[reportPrivateUsage]

    # Reusing the same key must not raise — no new entry is added.
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    assert name == "session-cap:k1"


async def test_max_keyed_reservations_guard_skipped_when_settings_none() -> None:
    """When settings is None the guardrail is not enforced (no limit known)."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    for i in range(5):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref,
            payload={"session_id": f"k{i}"},
            pg_pool=None,
            settings=None,
        )

    assert len(reg._keyed_reservation_last_used) == 5  # pyright: ignore[reportPrivateUsage]


# ── Opportunistic eviction on the acquisition path ──────────────────


async def test_opportunistic_eviction_reclaims_idle_capacity_on_cap_hit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materialising a new key at the cap succeeds when idle entries exist —
    the acquisition path itself performs an opportunistic eviction, without
    the caller ever calling ``evict_idle_keyed_reservations`` directly.

    1. Fill the cap (3 entries) at t=1000.
    2. Advance past the 1-hour idle threshold.
    3. Re-stamp one key as fresh (so only 2 of 3 are stale).
    4. Materialise a NEW key that would exceed the cap — the opportunistic
       eviction inside ``_resolve_reservation_name`` reclaims the 2 stale
       entries, making room.  The call succeeds and the new key is
       registered.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _settings(max_keyed=3)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    # 1. Fill the cap at t=1000.
    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_reservation_last_used) == 3  # pyright: ignore[reportPrivateUsage]

    # 2. Advance past the 1-hour idle threshold (3600 s).
    fake_time = 5000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)

    # 3. Re-stamp k3 as fresh (last_used=5000) — k1 and k2 remain stale.
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]

    # 4. Materialise a NEW key — would exceed the cap, but opportunistic
    #    eviction reclaims the 2 stale entries first.
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k4"}, pg_pool=None, settings=settings
    )

    assert name == "session-cap:k4"
    assert "session-cap:k4" in reg.reservations
    # Stale entries were evicted; k3 and k4 remain.
    assert "session-cap:k1" not in reg.reservations
    assert "session-cap:k2" not in reg.reservations
    assert "session-cap:k3" in reg.reservations


async def test_cap_hit_with_nothing_idle_still_raises_reservation_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When all entries are recently used (nothing stale to reclaim), the
    opportunistic eviction has no effect and the cap hit still raises
    ``ReservationUnavailable`` — the denial is a genuine
    sustained-high-cardinality condition, not an artefact of sweep timing.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    # Materialise 2 keys — all at the same recent time, nothing idle.
    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_reservation_name(
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    # A third key at the same time — nothing is idle, so opportunistic
    # eviction reclaims 0 entries and the cap hit is genuine.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
        )

    # Registry is unchanged — no eviction occurred.
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]
    assert "session-cap:k1" in reg.reservations
    assert "session-cap:k2" in reg.reservations


async def test_opportunistic_eviction_scan_is_amortized_under_sustained_denials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The O(n) opportunistic eviction scan runs at most once per
    ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` — sustained cap-hit denials must
    stay O(1) per request, not rescan the whole tracking dict every time.

    1. Fill the cap at t=1000 (nothing idle).
    2. First denied new key → scan runs (first cap-hit always scans).
    3. Second denied new key immediately after → scan is GATED, not run.
    4. Advance past the 30s min-interval → third denied new key → scan
       runs again (idle capacity can still be reclaimed within the sweep
       SLA).
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    scan_calls: list[timedelta] = []
    real_evict = reg.evict_idle_keyed_reservations

    def _spy_evict(*, idle_for: timedelta) -> int:
        scan_calls.append(idle_for)
        return real_evict(idle_for=idle_for)

    monkeypatch.setattr(reg, "evict_idle_keyed_reservations", _spy_evict)

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]
    assert scan_calls == []

    # 2. First denied new key — the scan runs once (nothing idle to reclaim).
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
        )
    assert len(scan_calls) == 1

    # 3. Immediate second denial — the scan is gated: no rescan.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k4"}, pg_pool=None, settings=settings
        )
    assert len(scan_calls) == 1, "scan must be amortized — no rescan within the min interval"

    # 4. Past the min interval, the next cap-hit scans again. (The lambda
    # closes over fake_time, so no re-setattr is needed.)
    fake_time = 1000.0 + 31.0
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k5"}, pg_pool=None, settings=settings
        )
    assert len(scan_calls) == 2


async def test_gated_opportunistic_scan_still_reclaims_after_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guarantee test (the gate itself is pinned by the spy test above):
    a cap-hit at t=5000 scans (first cap-hit always scans), reclaims the
    two stale entries, and admits the new key; a further cap-hit at the
    same instant is gated — nothing more to reclaim, still denied. I.e.
    the gate delays reclaim by at most the min-interval, never forever.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    # Fill the cap at t=1000.
    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )

    # Both entries go stale (past the 1h idle threshold); the cap-hit for
    # k3 at t=5000 runs the first scan, evicts k1+k2, and admits k3.
    fake_time = 5000.0
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k3"}, pg_pool=None, settings=settings
    )
    assert name == "session-cap:k3"

    # Refill to the cap with a fresh key.
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k4"}, pg_pool=None, settings=settings
    )
    # Same-instant cap-hit for a 4th key — the scan is gated (nothing new
    # could have gone idle since the scan moments ago); k3/k4 are fresh,
    # so the denial is genuine.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"session_id": "k5"}, pg_pool=None, settings=settings
        )


# ── K2: Race condition between eviction and lazy registration ─────────


async def test_race_condition_eviction_during_ensure_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Eviction during the ``ensure_slots`` await does not cause KeyError.

    The ``_keyed_reservation_last_used`` entry is stamped *before* the
    await, and after the await the reservation is re-registered if
    eviction removed it.
    """
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    marked_before_await = False

    async def _evicting_ensure_slots(self: ConcurrencyReservation, pool: object) -> None:
        nonlocal marked_before_await
        # The key should have been marked as recently-used before this await.
        marked_before_await = (
            self.name in reg._keyed_reservation_last_used  # pyright: ignore[reportPrivateUsage]
        )
        # Simulate aggressive eviction that removes the entry anyway.
        reg._reservations.pop(self.name, None)  # pyright: ignore[reportPrivateUsage]
        reg._keyed_reservation_last_used.pop(self.name, None)  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(ConcurrencyReservation, "ensure_slots", _evicting_ensure_slots)

    fake_pool = MagicMock()

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref,
        payload={"session_id": "s1"},
        pg_pool=fake_pool,  # type: ignore[arg-type]  # ensure_slots is patched; pool is never used.
        settings=None,
    )

    assert marked_before_await, (
        "key must be marked in _keyed_reservation_last_used before ensure_slots"
    )
    assert name == "session-cap:s1"
    assert name in reg._reservations  # pyright: ignore[reportPrivateUsage]  # Re-registered after eviction.
    assert name in reg._keyed_reservation_last_used  # pyright: ignore[reportPrivateUsage]  # Re-stamped after the await — without this the re-registered entry would become invisible to the idle sweep (unevictable, a slow leak).


async def test_race_condition_no_key_error_when_pg_pool_none() -> None:
    """When pg_pool is None there is no await and hence no race — the
    reservation is simply registered and returned."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:s1"
    assert name in reg._reservations  # pyright: ignore[reportPrivateUsage]


# ── ensure_slots failure unwinds materialization ────────────────────────


async def test_ensure_slots_failure_unwinds_materialization_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``ensure_slots`` failure during first materialization
    must NOT poison the key: the half-registered entry is unwound so the
    next acquisition re-materializes and RETRIES ``ensure_slots`` (which is
    idempotent). Without unwinding, the entry would be reused forever —
    acquire() finds no slot rows and denies, and every attempt re-stamps
    recency so the entry is never idle-evicted either: a permanently
    wedged key until worker restart."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    ensure_calls = 0

    async def _flaky_ensure_slots(self: ConcurrencyReservation, pool: object) -> None:
        nonlocal ensure_calls
        ensure_calls += 1
        if ensure_calls == 1:
            raise ConnectionError("transient PG blip")

    monkeypatch.setattr(ConcurrencyReservation, "ensure_slots", _flaky_ensure_slots)

    fake_pool = MagicMock()

    with pytest.raises(ConnectionError, match="transient PG blip"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref,
            payload={"session_id": "s1"},
            pg_pool=fake_pool,  # type: ignore[arg-type]
            settings=None,
        )

    # The materialization was unwound: nothing registered, nothing tracked.
    assert "session-cap:s1" not in reg._reservations  # pyright: ignore[reportPrivateUsage]
    assert "session-cap:s1" not in reg._keyed_reservation_last_used  # pyright: ignore[reportPrivateUsage]

    # The retry re-materializes and re-attempts ensure_slots (now succeeds).
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref,
        payload={"session_id": "s1"},
        pg_pool=fake_pool,  # type: ignore[arg-type]
        settings=None,
    )
    assert name == "session-cap:s1"
    assert ensure_calls == 2
    assert name in reg._reservations  # pyright: ignore[reportPrivateUsage]
    assert name in reg._keyed_reservation_last_used  # pyright: ignore[reportPrivateUsage]


# ── Cap bounds growth only, never static reuse ──────────────────────────


async def test_keyed_cap_does_not_deny_static_colliding_reuse() -> None:
    """With the keyed tracking dict AT the cap, resolving a key whose
    concrete name was STATICALLY pre-registered must still succeed — the
    cap bounds keyed-materialized growth, and static reuse grows nothing."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    settings = _settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")

    # Fill the keyed cap with two fresh materialized keys.
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k1"}, pg_pool=None, settings=settings
    )
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "k2"}, pg_pool=None, settings=settings
    )
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    # A statically pre-registered entry whose name collides with the ref's
    # concrete name for key "s1".
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s1", slots=1, lease=timedelta(minutes=5), clock=clock
        )
    )

    # Must NOT raise ReservationUnavailable despite the full cap.
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=settings
    )
    assert name == "session-cap:s1"
    # Static entry stays untracked (never evictable by the keyed sweep).
    assert "session-cap:s1" not in reg._keyed_reservation_last_used  # pyright: ignore[reportPrivateUsage]


# ── Concrete-name collision behavior ────────────────────────────────────


async def test_colliding_concrete_names_same_config_share_primitive() -> None:
    """Two refs whose ``f"{base_name}:{key}"`` concrete names collide (one
    ref's base_name is a prefix of the other's, and ':' is an allowed key
    character) resolve to the SAME registered primitive when their configs
    are identical — registration is idempotent, so they silently share one
    reservation.  This pins the documented collision behavior: bounded and
    safe (the shared cap still holds), if surprising."""
    reg = RateLimitRegistry()
    ref_a = _keyed_ref(base_name="a")  # key "b:c" → concrete "a:b:c"
    ref_ab = _keyed_ref(base_name="a:b")  # key "c" → concrete "a:b:c"

    name_a = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref_a, payload={"session_id": "b:c"}, pg_pool=None, settings=None
    )
    name_ab = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref_ab, payload={"session_id": "c"}, pg_pool=None, settings=None
    )

    assert name_a == name_ab == "a:b:c"
    assert len(reg.reservations) == 1
    assert reg.get_reservation("a:b:c").slots == ref_a.slots


async def test_collision_guard_resets_after_idle_eviction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pins the guard's eviction boundary: the collision check fires only
    on LIVE tracked entries. Once the colliding entry is idle-evicted, a
    different-config ref re-materializes its OWN config silently — eviction
    resets the guard (documented behavior, not a silent config flip of a
    live primitive)."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref_a = _keyed_ref(base_name="a", slots=3)
    ref_ab = _keyed_ref(base_name="a:b", slots=99)

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref_a, payload={"session_id": "b:c"}, pg_pool=None, settings=None
    )

    # Idle-evict the "a:b:c" entry (slots=3).
    fake_time = 99999.0
    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(hours=1))
    assert evicted == 1

    # The colliding ref now materializes its own config — no ValueError.
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref_ab, payload={"session_id": "c"}, pg_pool=None, settings=None
    )
    assert name == "a:b:c"
    assert reg.get_reservation("a:b:c").slots == 99


async def test_colliding_concrete_names_different_config_raise_value_error() -> None:
    """A concrete-name collision with DIFFERENT configs fails loudly with
    ``ValueError`` naming the collision — never silently over- or
    under-admits relative to one ref's declared config, and never
    reconfigures the existing primitive."""
    reg = RateLimitRegistry()
    ref_a = _keyed_ref(base_name="a", slots=3)
    ref_ab = _keyed_ref(base_name="a:b", slots=99)  # same concrete name, different slots

    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref_a, payload={"session_id": "b:c"}, pg_pool=None, settings=None
    )
    with pytest.raises(ValueError, match="concrete-name collision"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref_ab, payload={"session_id": "c"}, pg_pool=None, settings=None
        )

    # The original primitive is untouched.
    assert reg.get_reservation("a:b:c").slots == 3


async def test_statically_preregistered_reservation_is_never_keyed_evicted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reservation twin of the static-entry protection: a statically
    pre-registered reservation whose name matches a keyed ref's concrete
    name is reused as-is and NOT stamped into the keyed tracking dict, so
    the idle-eviction sweep can never evict it."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    reg = RateLimitRegistry()
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s1", slots=1, lease=timedelta(minutes=5), clock=clock
        )
    )
    ref = _keyed_ref(base_name="session-cap")

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 1000.0)
    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )
    assert name == "session-cap:s1"
    assert len(reg._keyed_reservation_last_used) == 0  # pyright: ignore[reportPrivateUsage]

    # Far past the idle threshold: the static entry survives the sweep.
    monkeypatch.setattr(registry_mod, "monotonic", lambda: 99999.0)
    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(hours=1))
    assert evicted == 0
    assert "session-cap:s1" in reg.reservations


# ── Rollback on non-ReservationUnavailable errors (except Exception fix) ──


async def test_value_error_during_keyed_resolution_rolls_back_earlier_reservation() -> None:
    """Regression for the ``except ReservationUnavailable`` → ``except
    Exception`` rollback fix: a ``ValueError`` from key validation (invalid
    ``key_fn`` output) raised AFTER an earlier reservation was already
    acquired must roll that reservation back — the old clause only caught
    ``ReservationUnavailable`` and leaked the slot until lease expiry."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    reg = RateLimitRegistry()
    static_res = ConcurrencyReservation(
        name="gpu", slots=1, lease=timedelta(minutes=5), clock=clock
    )
    reg.register(static_res)
    bad_ref = _keyed_ref(base_name="session-cap", key_fn=lambda _p: "")  # invalid: empty key

    with pytest.raises(ValueError, match="empty"):
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=["gpu", bad_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "s1"},
        )

    # The statically-acquired slot was rolled back: fully free again.
    free, held = static_res.table.peek_slots("gpu")
    assert (free, held) == (1, 0)

    # And it can be acquired immediately afterwards (no lease-expiry wait).
    acquired = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=["gpu"],
        job_id=new_uuid(),
        worker_id=new_uuid(),
    )
    assert len(acquired) == 1


async def test_key_fn_exception_rolls_back_earlier_reservation() -> None:
    """Same rollback guarantee when ``key_fn`` itself raises (here:
    ``RuntimeError``) — any exception mid-composition releases the
    already-acquired handles in reverse order."""
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    reg = RateLimitRegistry()
    static_res = ConcurrencyReservation(
        name="gpu", slots=1, lease=timedelta(minutes=5), clock=clock
    )
    reg.register(static_res)

    def _boom(payload: dict[str, object]) -> str:
        raise RuntimeError("key derivation exploded")

    bad_ref = _keyed_ref(base_name="session-cap", key_fn=_boom)

    with pytest.raises(RuntimeError, match="key derivation exploded"):
        await reg.acquire_for_actor(
            rate_limits=[],
            reservations=["gpu", bad_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"session_id": "s1"},
        )

    free, held = static_res.table.peek_slots("gpu")
    assert (free, held) == (1, 0)


async def test_value_error_during_keyed_rate_limit_resolution_rolls_back_reservation() -> None:
    """Rate-limit twin: a ``ValueError`` from ``KeyedRateLimitRef`` key
    validation raised after a reservation was acquired rolls the
    reservation back (reservations acquire BEFORE rate limits, so the
    leak window is the rate-limit resolution/acquire phase)."""
    from taskq.ratelimit.refs import KeyedRateLimitRef

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    reg = RateLimitRegistry()
    static_res = ConcurrencyReservation(
        name="gpu", slots=1, lease=timedelta(minutes=5), clock=clock
    )
    reg.register(static_res)
    bad_rl_ref = KeyedRateLimitRef(
        base_name="api-per-tenant",
        key_fn=lambda _p: "key with spaces",  # invalid: disallowed chars
        capacity=10,
        refill_per_second=1.0,
    )

    with pytest.raises(ValueError, match="outside the allowed set"):
        await reg.acquire_for_actor(
            rate_limits=[bad_rl_ref],
            reservations=["gpu"],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"tenant_id": "t1"},
        )

    free, held = static_res.table.peek_slots("gpu")
    assert (free, held) == (1, 0)


# ── K3: evict_idle_keyed_reservations wired into leader sweep ─────────


class _FakeConn:
    async def execute(self, sql: str, *args: object) -> str:
        return "DELETE 0"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    async def fetchval(self, sql: str, *args: object) -> object:
        return None

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False


class _FakePool:
    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        yield _FakeConn()


class _SimpleBackend:
    """Backend whose reclaim/deadline sweeps return 0 and lacks PG-only sweep methods."""

    async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0


async def test_leader_sweep_calls_evict_idle_keyed_reservations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The leader sweep loop calls ``evict_idle_keyed_reservations`` when
    keyed reservations are in use."""
    import taskq.worker._leader_sweeps as sweeps_mod

    mock_registry = MagicMock()
    mock_registry.has_keyed_reservations = True
    mock_registry.evict_idle_keyed_reservations.return_value = 0
    monkeypatch.setattr(sweeps_mod, "rl_registry", mock_registry)

    settings = _settings()
    deps_data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "2.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    settings = WorkerSettings.load_from_dict(deps_data, validate=False)

    from taskq.worker.deps import WorkerDeps

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool.
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    deps.is_leader.set()

    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(deps=deps, backend=_SimpleBackend(), clock=clock, worker_id=new_uuid())

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))
    for _ in range(200):
        if mock_registry.evict_idle_keyed_reservations.called:
            break
        await asyncio.sleep(0.01)

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    mock_registry.evict_idle_keyed_reservations.assert_called_once()
    call_kwargs = mock_registry.evict_idle_keyed_reservations.call_args
    assert call_kwargs.kwargs["idle_for"] == timedelta(hours=1)


async def test_leader_sweep_skips_eviction_when_no_keyed_reservations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When there are no keyed reservations, the sweep loop does not call eviction."""
    import taskq.worker._leader_sweeps as sweeps_mod

    mock_registry = MagicMock()
    mock_registry.has_keyed_reservations = False
    mock_registry.evict_idle_keyed_reservations.return_value = 0
    monkeypatch.setattr(sweeps_mod, "rl_registry", mock_registry)

    settings_data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "2.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    settings = WorkerSettings.load_from_dict(settings_data, validate=False)

    from taskq.worker.deps import WorkerDeps

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    deps.is_leader.set()

    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(deps=deps, backend=_SimpleBackend(), clock=clock, worker_id=new_uuid())

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))
    await asyncio.sleep(0.05)

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    mock_registry.evict_idle_keyed_reservations.assert_not_called()


# ── K3: evict_idle_keyed_rate_limits wired into leader sweep ─────────


async def test_leader_sweep_calls_evict_idle_keyed_rate_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The leader sweep loop calls ``evict_idle_keyed_rate_limits`` when
    keyed rate limits are in use."""
    import taskq.worker._leader_sweeps as sweeps_mod

    mock_registry = MagicMock()
    mock_registry.has_keyed_rate_limits = True
    mock_registry.evict_idle_keyed_rate_limits.return_value = 0
    monkeypatch.setattr(sweeps_mod, "rl_registry", mock_registry)

    settings = _settings()
    deps_data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "2.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    settings = WorkerSettings.load_from_dict(deps_data, validate=False)

    from taskq.worker.deps import WorkerDeps

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool.
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    deps.is_leader.set()

    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(deps=deps, backend=_SimpleBackend(), clock=clock, worker_id=new_uuid())

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))
    for _ in range(200):
        if mock_registry.evict_idle_keyed_rate_limits.called:
            break
        await asyncio.sleep(0.01)

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    mock_registry.evict_idle_keyed_rate_limits.assert_called_once()
    call_kwargs = mock_registry.evict_idle_keyed_rate_limits.call_args
    assert call_kwargs.kwargs["idle_for"] == timedelta(hours=1)


async def test_leader_sweep_skips_eviction_when_no_keyed_rate_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When there are no keyed rate limits, the sweep loop does not call eviction."""
    import taskq.worker._leader_sweeps as sweeps_mod

    mock_registry = MagicMock()
    mock_registry.has_keyed_rate_limits = False
    mock_registry.evict_idle_keyed_rate_limits.return_value = 0
    monkeypatch.setattr(sweeps_mod, "rl_registry", mock_registry)

    settings_data: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "2.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
    }
    settings = WorkerSettings.load_from_dict(settings_data, validate=False)

    from taskq.worker.deps import WorkerDeps

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    deps.is_leader.set()

    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    ctx = SweepContext(deps=deps, backend=_SimpleBackend(), clock=clock, worker_id=new_uuid())

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))
    await asyncio.sleep(0.05)

    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    mock_registry.evict_idle_keyed_rate_limits.assert_not_called()
