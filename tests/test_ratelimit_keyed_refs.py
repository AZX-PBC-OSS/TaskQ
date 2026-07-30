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
from enum import Enum, StrEnum

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from taskq._ids import new_uuid
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _DefaultPayload(BaseModel):
    session_id: str


def _default_key_fn(payload: _DefaultPayload) -> str:
    return payload.session_id


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
    key_fn: Callable[[_DefaultPayload], str] = _default_key_fn,
) -> KeyedReservationRef:
    return KeyedReservationRef.typed(
        _DefaultPayload,
        base_name=base_name,
        key_fn=key_fn,
        slots=slots,
        lease=lease,
    )


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

    def test_rejects_base_name_with_space(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _keyed_ref(base_name="session cap")

    def test_rejects_base_name_with_slash(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _keyed_ref(base_name="session/cap")

    def test_rejects_base_name_with_control_char(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _keyed_ref(base_name="session\tcap")

    def test_rejects_base_name_exceeding_length_cap(self) -> None:
        with pytest.raises(ValueError, match="at most 255 characters"):
            _keyed_ref(base_name="a" * 256)

    def test_accepts_valid_base_name_with_allowed_punctuation(self) -> None:
        ref = _keyed_ref(base_name="session_cap:1.0")
        assert ref.base_name == "session_cap:1.0"

    def test_rejects_base_name_inside_reserved_queue_cap_prefix(self) -> None:
        """A base_name already inside the reserved queue-cap namespace would
        derive concrete names (f"{base_name}:{key}") that the register()
        prefix guard rejects — failing at CONSTRUCTION surfaces the
        misconfiguration at startup instead of a per-job ValueError for
        every job on the actor, forever."""
        with pytest.raises(ValueError, match="reserved queue-cap namespace"):
            _keyed_ref(base_name="taskq:global:queue:evil")

    def test_rejects_base_name_that_prefix_completes_reserved_namespace(self) -> None:
        """The ':' separator completes the reserved prefix: base_name
        'taskq:global:queue' + ':' + key lands inside the namespace even
        though the base_name alone does not start with it."""
        with pytest.raises(ValueError, match="reserved queue-cap namespace"):
            _keyed_ref(base_name="taskq:global:queue")

    def test_accepts_base_names_sharing_segments_with_reserved_prefix(self) -> None:
        """Names that merely SHARE segments with the reserved prefix but
        cannot derive into it must remain valid."""
        for base_name in ("taskq:global", "taskq:queue:cap", "taskq:global:queueX"):
            assert _keyed_ref(base_name=base_name).base_name == base_name

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
        ref, payload=_DefaultPayload(session_id="abc123"), pg_pool=None, settings=None
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
        ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    first_instance = reg.get_reservation(name1)
    assert len(reg.reservations) == 1

    name2 = await reg._resolve_reservation_name(
        ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
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
        ref, payload=_DefaultPayload(session_id="a"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    name_b = await reg._resolve_reservation_name(
        ref, payload=_DefaultPayload(session_id="b"), pg_pool=None, settings=None
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
        payload=_DefaultPayload(session_id="a"),
        clock=clock,
    )
    assert acquired_a[0].name == "session-cap:a"

    # Key "a"'s single slot is now held; key "b" is untouched and still acquirable.
    acquired_b = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(session_id="b"),
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
            payload=_DefaultPayload(session_id="a"),
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
            ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_fn_exception_propagates() -> None:
    """An exception raised by key_fn itself is not swallowed — it propagates to the
    caller of _resolve_reservation_name / acquire_for_actor."""
    reg = RateLimitRegistry()

    def _boom(payload: _DefaultPayload) -> str:
        raise RuntimeError("key derivation exploded")

    ref = _keyed_ref(base_name="session-cap", key_fn=_boom)

    with pytest.raises(RuntimeError, match="key derivation exploded"):
        await reg._resolve_reservation_name(
            ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_wrong_model_type_raises_validation_error() -> None:
    """A BaseModel payload of a different type is re-validated against the
    ref's payload_type — a missing required field raises ValidationError,
    not AttributeError from key_fn accessing a non-existent attribute."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap")  # key_fn does p.session_id

    class _UnrelatedPayload(BaseModel):
        unrelated: str

    with pytest.raises(ValidationError):
        await reg._resolve_reservation_name(
            ref, payload=_UnrelatedPayload(unrelated="value"), pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]  # Why: exercising private resolution helper directly, matching existing test conventions.


async def test_resolve_keyed_ref_key_fn_returning_non_str_raises_value_error() -> None:
    """key_fn returning a non-str (e.g. int) raises ValueError — a broken
    key_fn can never silently resolve to a shared/global reservation."""
    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda p: 42)

    with pytest.raises(ValueError, match="empty key or non-string value"):
        await reg._resolve_reservation_name(
            ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_str_subclass_key_uses_value_content() -> None:
    """A key_fn returning a ``str`` subclass (domain wrapper) is accepted
    and normalized to its plain-str content: the concrete name and the
    registry dict key are true ``str``, identical to returning a plain
    string."""

    class TenantKey(str):
        """Domain wrapper deriving from str."""

    reg = RateLimitRegistry()
    ref = _keyed_ref(base_name="session-cap", key_fn=lambda p: TenantKey("s1"))

    name = await reg._resolve_reservation_name(
        ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "session-cap:s1"
    assert type(name) is str
    assert name in reg.reservations
    assert all(type(k) is str for k in reg.reservations)


async def test_resolve_keyed_ref_str_enum_key_uses_member_value_not_repr() -> None:
    """A key_fn returning a ``str``-derived Enum member resolves to the
    member's VALUE (``'acme'``), not its Enum rendering — dict lookups,
    Redis keys, and PG text columns would all treat the member as its
    value, so the registry name must match.

    Covers both flavors: the classic ``(str, Enum)`` mixin (whose
    ``__str__``/``__format__`` render ``'Tenant.ACME'`` — the exact trap
    the key normalization guards against) and ``StrEnum``.
    """

    class Tenant(str, Enum):  # noqa: UP042  # Why: issue #32 explicitly names the classic (str, Enum) mixin; its __format__ trap is what this test pins. StrEnum is covered below.
        ACME = "acme"

    class TenantSE(StrEnum):
        GLOBEX = "globex"

    reg = RateLimitRegistry()
    for member, expected in ((Tenant.ACME, "acme"), (TenantSE.GLOBEX, "globex")):
        ref = _keyed_ref(base_name="session-cap", key_fn=lambda p, m=member: m)

        name = await reg._resolve_reservation_name(
            ref, payload=_DefaultPayload(session_id=expected), pg_pool=None, settings=None
        )  # pyright: ignore[reportPrivateUsage]

        assert name == f"session-cap:{expected}"
        assert type(name) is str
        assert name in reg.reservations


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
        payload=_DefaultPayload(session_id="abc"),
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
            payload=_DefaultPayload(session_id="abc"),
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
        payload=_DefaultPayload(session_id="xyz"),
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
        ref, payload=_DefaultPayload(session_id="stale"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    fake_time = 1100.0  # 100s later — "stale" key untouched since
    await reg._resolve_reservation_name(
        ref, payload=_DefaultPayload(session_id="fresh"), pg_pool=None, settings=None
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
        ref, payload=_DefaultPayload(session_id="recent"), pg_pool=None, settings=None
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
        ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]
    reg._reservations.pop("session-cap:s1")  # pyright: ignore[reportPrivateUsage] # Why: simulating what evict_idle_keyed_reservations does, without needing monotonic control here.
    reg._keyed_reservation_last_used.pop("session-cap:s1")  # pyright: ignore[reportPrivateUsage]

    name = await reg._resolve_reservation_name(
        ref, payload=_DefaultPayload(session_id="s1"), pg_pool=None, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "session-cap:s1"
    assert len(reg.reservations) == 1


# ── _clean_rate_limit_registry fixture isolation ──────────────────────


def test_singleton_keyed_tracking_dicts_empty_at_start() -> None:
    """Regression test for the ``_clean_rate_limit_registry`` autouse fixture.

    The fixture in ``tests/conftest.py`` clears the singleton
    ``registry``'s ``_keyed_reservation_last_used`` and
    ``_keyed_rate_limit_last_used`` dicts before each unit test.  If a
    prior test materialized a keyed ref against the real singleton
    (instead of a fresh local ``RateLimitRegistry()``) and the fixture
    failed to clean up, those dicts would still hold entries here.

    This is the simpler substitute for a cross-test-boundary regression
    test (which would require a nested pytest run via ``pytester``):
    a direct assertion that both dicts are empty at the start, proving
    no prior test leaked tracking state into this one.  Every test in
    this file uses a fresh local ``RateLimitRegistry()`` to avoid the
    singleton, but the fixture is the safety net for any future test
    that does not — this test guards that safety net.
    """
    from taskq.ratelimit.registry import registry as _rl

    assert len(_rl._keyed_reservation_last_used) == 0, (  # pyright: ignore[reportPrivateUsage]
        f"_keyed_reservation_last_used leaked from a prior test: "
        f"{dict(_rl._keyed_reservation_last_used)}"  # pyright: ignore[reportPrivateUsage]
    )
    assert len(_rl._keyed_rate_limit_last_used) == 0, (  # pyright: ignore[reportPrivateUsage]
        f"_keyed_rate_limit_last_used leaked from a prior test: "
        f"{dict(_rl._keyed_rate_limit_last_used)}"  # pyright: ignore[reportPrivateUsage]
    )


# ── Typed payload validation in _resolve_reservation_name ──────


class _TypedPayload(BaseModel):
    session_id: str
    region: str = "us-east-1"


class _AliasedPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    session_id: str = Field(alias="sessionId")


async def test_resolve_typed_res_ref_passes_validated_model_to_key_fn() -> None:
    """A dict payload is validated via ref.payload_type.model_validate before
    being passed to key_fn — key_fn receives a BaseModel with attribute access,
    not a raw dict."""
    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:s1"


async def test_resolve_typed_res_ref_applies_pydantic_defaults() -> None:
    """A dict payload missing a defaulted field gets the default applied
    during validation — key_fn can read the default value."""
    reg = RateLimitRegistry()
    captured: list[_TypedPayload] = []

    def _capturing_key_fn(payload: _TypedPayload) -> str:
        captured.append(payload)
        return f"{payload.session_id}:{payload.region}"

    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=_capturing_key_fn,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"session_id": "s1"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:s1:us-east-1"
    assert captured[0].region == "us-east-1"


async def test_resolve_typed_res_ref_applies_aliases() -> None:
    """A dict payload using wire aliases (e.g. 'sessionId') is validated
    with alias resolution — key_fn accesses the field by its Python name
    (p.session_id)."""
    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _AliasedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"sessionId": "s1"}, pg_pool=None, settings=None
    )

    assert name == "session-cap:s1"


async def test_resolve_typed_res_ref_accepts_basemodel_payload_directly() -> None:
    """A BaseModel payload of the same type as ref.payload_type is accepted
    directly — zero-cost pass-through, no re-validation."""
    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_TypedPayload(session_id="s1"), pg_pool=None, settings=None
    )

    assert name == "session-cap:s1"


async def test_resolve_typed_res_ref_validation_error_propagates() -> None:
    """An invalid dict (missing required field) raises ValidationError from
    pydantic validation, not KeyError or AttributeError from key_fn."""
    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    with pytest.raises(ValidationError):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"region": "us-east-1"}, pg_pool=None, settings=None
        )


async def test_resolve_typed_res_ref_wrong_model_type_re_validates() -> None:
    """A BaseModel payload of a DIFFERENT type is re-validated against
    ref.payload_type via model_dump() → model_validate(). A compatible
    payload (extra fields ignored) resolves successfully."""

    class _CompatiblePayload(BaseModel):
        session_id: str
        extra_field: str = "ignored"

    reg = RateLimitRegistry()
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=3,
        lease=timedelta(minutes=5),
    )

    name = await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_CompatiblePayload(session_id="s1", extra_field="x"), pg_pool=None, settings=None
    )

    assert name == "session-cap:s1"


async def test_resolve_typed_res_ref_same_model_type_zero_cost_passthrough() -> None:
    """A BaseModel payload of the SAME type as ref.payload_type is passed
    directly to key_fn without re-validation — the exact same object (identity
    check)."""
    reg = RateLimitRegistry()
    captured: list[_TypedPayload] = []

    def _capturing_key_fn(payload: _TypedPayload) -> str:
        captured.append(payload)
        return payload.session_id

    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=_capturing_key_fn,
        slots=3,
        lease=timedelta(minutes=5),
    )

    payload = _TypedPayload(session_id="s1")
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=payload, pg_pool=None, settings=None
    )

    assert len(captured) == 1
    assert captured[0] is payload


# ── acquire_for_actor with typed reservation refs and BaseModel/dict payloads ──


async def test_acquire_for_actor_accepts_basemodel_payload_with_typed_reservation_ref() -> None:
    """acquire_for_actor accepts a BaseModel payload with a typed reservation ref."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s1", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=1,
        lease=timedelta(seconds=30),
    )

    acquired = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_TypedPayload(session_id="s1"),
        clock=clock,
    )

    assert len(acquired) == 1
    assert acquired[0].name == "session-cap:s1"


async def test_acquire_for_actor_typed_reservation_ref_with_dict_payload_validates() -> None:
    """acquire_for_actor accepts a dict payload with a typed reservation ref —
    the dict is validated via model_validate before key_fn is called."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s1", slots=1, lease=timedelta(seconds=30), clock=clock
        )
    )
    ref = KeyedReservationRef.typed(
        _TypedPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=1,
        lease=timedelta(seconds=30),
    )

    acquired = await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload={"session_id": "s1"},
        clock=clock,
    )

    assert len(acquired) == 1
    assert acquired[0].name == "session-cap:s1"
