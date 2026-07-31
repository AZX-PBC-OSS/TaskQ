"""Unit tests for KeyedRateLimitRef and RateLimitRegistry dynamic resolution.

Tests ``KeyedRateLimitRef`` validation, ``RateLimitRegistry._resolve_rate_limit_name``
dynamic key resolution/lazy registration, ``acquire_for_actor`` composing static and
keyed rate limits, ``evict_idle_keyed_rate_limits``, and hardening guards (bad
``key_fn`` outputs, key length/character validation, ``max_keyed_rate_limits`` cap).
Mirrors the in-memory (``FakeClock``-backed ``TokenBucket``) conventions of
``tests/test_ratelimit_keyed_refs.py`` and ``tests/test_ratelimit_composition.py`` — no
Redis or PG instance required, so every call passes ``redis_client=None`` and
``pg_pool=None``. The Redis-backed concurrency atomicity test at the bottom is marked
``integration``/``redis`` and uses the ``redis_url`` fixture from
``tests/test_ratelimit_token_bucket_redis.py``.
"""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum

import pytest
import redis.asyncio as redis_async
import structlog.testing
from pydantic import BaseModel, ConfigDict, Field

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.exceptions import PayloadValidationError, ReservationUnavailable
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)

_SCHEMA_LABEL = "taskq_test"


class _DefaultPayload(BaseModel):
    tenant_id: str


class _SessionPayload(BaseModel):
    session_id: str


class _CompositionPayload(BaseModel):
    tenant_id: str
    session_id: str


def _default_key_fn(payload: _DefaultPayload) -> str:
    return payload.tenant_id


def _token_bucket(
    name: str = "tb",
    capacity: float = 5.0,
    refill_per_second: float = 1.0,
) -> TokenBucket:
    return TokenBucket(
        name=name,
        capacity=capacity,
        refill_per_second=refill_per_second,
        backend="memory",
    )


def _rate_limit_ref(
    base_name: str = "api-per-tenant",
    capacity: float = 10.0,
    refill_per_second: float = 1.0,
    key_fn: Callable[[_DefaultPayload], str] = _default_key_fn,
) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name=base_name,
        key_fn=key_fn,
        capacity=capacity,
        refill_per_second=refill_per_second,
    )


def _hardening_settings(max_keyed: int = 10000) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_MAX_KEYED_RATE_LIMITS": str(max_keyed),
        },
        validate=False,
    )


def _redis_settings(redis_url: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": redis_url,
            "schema_name": _SCHEMA_LABEL,
        },
    )


def _unique_name() -> str:
    return f"test_{new_base62()}"


# ── KeyedRateLimitRef validation ───────────────────────────────


class TestKeyedRateLimitRefValidation:
    def test_construction(self) -> None:
        ref = _rate_limit_ref()
        assert ref.base_name == "api-per-tenant"
        assert ref.capacity == 10.0
        assert ref.refill_per_second == 1.0

    def test_backend_defaults_to_redis(self) -> None:
        ref = _rate_limit_ref()
        assert ref.backend == "redis"

    def test_backend_can_be_set_to_memory(self) -> None:
        ref = KeyedRateLimitRef.typed(
            _DefaultPayload,
            base_name="api-per-tenant",
            key_fn=_default_key_fn,
            capacity=10.0,
            refill_per_second=1.0,
            backend="memory",
        )
        assert ref.backend == "memory"

    def test_rejects_empty_base_name(self) -> None:
        with pytest.raises(ValueError, match="base_name must not be empty"):
            _rate_limit_ref(base_name="")

    def test_rejects_base_name_with_space(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _rate_limit_ref(base_name="api per tenant")

    def test_rejects_base_name_with_slash(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _rate_limit_ref(base_name="api/per-tenant")

    def test_rejects_base_name_with_control_char(self) -> None:
        with pytest.raises(ValueError, match="outside the allowed set"):
            _rate_limit_ref(base_name="api\tper-tenant")

    def test_rejects_base_name_exceeding_length_cap(self) -> None:
        with pytest.raises(ValueError, match="at most 255 characters"):
            _rate_limit_ref(base_name="a" * 256)

    def test_accepts_valid_base_name_with_allowed_punctuation(self) -> None:
        ref = _rate_limit_ref(base_name="api_per-tenant:1.0")
        assert ref.base_name == "api_per-tenant:1.0"

    def test_rejects_base_name_inside_reserved_queue_cap_prefix(self) -> None:
        """A base_name already inside the reserved queue-cap namespace would
        derive concrete names (f"{base_name}:{key}") that the register()
        prefix guard rejects — failing at CONSTRUCTION surfaces the
        misconfiguration at startup instead of a per-job ValueError for
        every job on the actor, forever."""
        with pytest.raises(ValueError, match="reserved queue-cap namespace"):
            _rate_limit_ref(base_name="taskq:global:queue:evil")

    def test_rejects_base_name_that_prefix_completes_reserved_namespace(self) -> None:
        """The ':' separator completes the reserved prefix: base_name
        'taskq:global:queue' + ':' + key lands inside the namespace even
        though the base_name alone does not start with it."""
        with pytest.raises(ValueError, match="reserved queue-cap namespace"):
            _rate_limit_ref(base_name="taskq:global:queue")

    def test_accepts_base_names_sharing_segments_with_reserved_prefix(self) -> None:
        """Names that merely SHARE segments with the reserved prefix but
        cannot derive into it must remain valid."""
        for base_name in ("taskq:global", "taskq:queue:cap", "taskq:global:queueX"):
            assert _rate_limit_ref(base_name=base_name).base_name == base_name

    def test_rejects_zero_capacity(self) -> None:
        with pytest.raises(ValueError, match="capacity must be > 0"):
            _rate_limit_ref(capacity=0)

    def test_rejects_negative_capacity(self) -> None:
        with pytest.raises(ValueError, match="capacity must be > 0"):
            _rate_limit_ref(capacity=-1)

    def test_rejects_negative_refill_per_second(self) -> None:
        with pytest.raises(ValueError, match="refill_per_second must be >= 0"):
            _rate_limit_ref(refill_per_second=-1)

    def test_accepts_refill_per_second_zero(self) -> None:
        """refill_per_second == 0 is a valid fixed-quota bucket (matching TokenBucket.__init__'s
        own accepted range)."""
        ref = _rate_limit_ref(refill_per_second=0)
        assert ref.refill_per_second == 0


# ── KeyedRateLimitRef backend field ──────────────────────────────


async def test_keyed_rate_limit_ref_backend_memory_materializes_memory_token_bucket() -> None:
    """A ``KeyedRateLimitRef`` with ``backend="memory"`` materializes a
    ``TokenBucket`` whose ``backend`` property is ``"memory"``, not silently
    ``"redis"`` — and the bucket can be acquired without a ``redis_client``,
    proving it did NOT try to hit Redis."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name="api-per-tenant",
        key_fn=_default_key_fn,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
    )

    bucket = reg.get_rate_limit(name)
    assert isinstance(bucket, TokenBucket)
    assert bucket.backend == "memory"

    clock = FakeClock(_START)
    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="t1"),
        clock=clock,
    )
    assert len(acquired) == 1
    assert acquired[0].name == "api-per-tenant:t1"


# ── _resolve_rate_limit_name: plain string passthrough ─────────


async def test_resolve_plain_string_returns_unchanged() -> None:
    """A plain str rate-limit ref is returned as-is by _resolve_rate_limit_name."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("gpu-bucket"))

    name = await reg._resolve_rate_limit_name("gpu-bucket", payload=None, settings=None)  # pyright: ignore[reportPrivateUsage] # Why: exercising private resolution helper directly, matching conftest's precedent for accessing registry internals in tests.

    assert name == "gpu-bucket"


async def test_resolve_plain_string_ignores_payload() -> None:
    """A plain str ref does not consult payload at all — works even with payload=None."""
    reg = RateLimitRegistry()
    reg.register(_token_bucket("gpu-bucket"))

    name = await reg._resolve_rate_limit_name(
        "gpu-bucket", payload={"unrelated": "data"}, settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "gpu-bucket"


# ── _resolve_rate_limit_name: KeyedRateLimitRef dynamic resolution ──


async def test_resolve_keyed_ref_produces_base_name_colon_key() -> None:
    """A KeyedRateLimitRef resolves to f'{base_name}:{key}' and lazily registers a TokenBucket
    with the ref's capacity/refill_per_second."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=10, refill_per_second=1.0)

    name = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="abc123"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "api-per-tenant:abc123"
    registered = reg.get_rate_limit("api-per-tenant:abc123")
    assert isinstance(registered, TokenBucket)
    assert registered.capacity == 10
    assert registered.refill_per_second == 1.0


async def test_resolve_keyed_ref_reuses_same_instance_for_same_key() -> None:
    """Two resolutions for the same key reuse the same registered primitive —
    not a duplicate registration."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    name1 = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
    )  # pyright: ignore[reportPrivateUsage]
    first_instance = reg.get_rate_limit(name1)
    assert len(reg.rate_limits) == 1

    name2 = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
    )  # pyright: ignore[reportPrivateUsage]
    second_instance = reg.get_rate_limit(name2)

    assert name1 == name2 == "api-per-tenant:t1"
    assert first_instance is second_instance
    assert len(reg.rate_limits) == 1


async def test_resolve_keyed_ref_different_keys_register_independently() -> None:
    """Two different keys for the same ref produce two independent registry entries."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=5)

    name_a = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="a"), settings=None
    )  # pyright: ignore[reportPrivateUsage]
    name_b = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="b"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name_a == "api-per-tenant:a"
    assert name_b == "api-per-tenant:b"
    assert len(reg.rate_limits) == 2
    assert reg.get_rate_limit(name_a) is not reg.get_rate_limit(name_b)


async def test_different_keys_isolate_budgets() -> None:
    """Two different keys for the same KeyedRateLimitRef are independent token
    budgets — exhausting one key's tokens does not affect the other's.

    Pre-registers both concrete TokenBuckets with a FakeClock (in-memory
    backend) for deterministic, fast token-exhaustion assertions — the lazy
    Redis-backed construction path itself is exercised separately in the
    integration test at the bottom of this file.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:a", capacity=1, refill_per_second=0, backend="memory")
    )
    reg.register(
        TokenBucket(name="api-per-tenant:b", capacity=1, refill_per_second=0, backend="memory")
    )
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=1, refill_per_second=0)

    acquired_a = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="a"),
        clock=clock,
    )
    assert acquired_a[0].name == "api-per-tenant:a"

    # Key "a"'s single token is now consumed; a second acquire for "a" is denied.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="a"),
            clock=clock,
        )

    # Key "b" is untouched and still acquirable.
    acquired_b = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="b"),
        clock=clock,
    )
    assert acquired_b[0].name == "api-per-tenant:b"


async def test_same_key_shares_budget_across_actors() -> None:
    """Two separate acquire_for_actor calls for the SAME key (simulating two
    different actors/call sites with different job_id/worker_id) draw from the
    same underlying bucket — the first call exhausts the budget and the second
    is denied, proving both calls shared one per-key bucket.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:x", capacity=1, refill_per_second=0, backend="memory")
    )
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=1, refill_per_second=0)

    # First call site — acquires the single token.
    acquired_1 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="x"),
        clock=clock,
    )
    assert acquired_1[0].name == "api-per-tenant:x"

    # Second call site — fresh job_id/worker_id, same key "x" — denied because
    # both calls drew from the same underlying bucket.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="x"),
            clock=clock,
        )


async def test_resolve_keyed_ref_missing_payload_raises_value_error() -> None:
    """payload=None with a KeyedRateLimitRef raises ValueError."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    with pytest.raises(ValueError, match="no payload was provided"):
        await reg._resolve_rate_limit_name(ref, payload=None, settings=None)  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_empty_key_raises_value_error() -> None:
    """key_fn returning an empty string raises ValueError."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=lambda p: "")

    with pytest.raises(ValueError, match="empty or non-string key"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_fn_returning_none_raises_value_error() -> None:
    """key_fn returning None raises ValueError — a broken key_fn can never silently
    collapse into a shared/global bucket."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(
        base_name="api-per-tenant",
        key_fn=lambda p: getattr(p, "missing", None),  # returns None when attribute is absent
    )

    with pytest.raises(ValueError, match="empty or non-string key"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_fn_returning_non_str_raises_value_error() -> None:
    """key_fn returning a non-str (e.g. int) raises ValueError."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(
        base_name="api-per-tenant",
        key_fn=lambda p: 42,
    )

    with pytest.raises(ValueError, match="empty or non-string key"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_pg_publish_failure_is_best_effort() -> None:
    """A failing PG publish on materialization does NOT fail the
    acquisition — the ``rate_limit_buckets`` row is observability metadata
    (admin UI discovery), not a correctness precondition. Contrast with
    ``ensure_slots`` for keyed reservations, whose failure unwinds the
    materialization and raises because slot rows ARE a precondition."""

    class _BoomPool:
        """asyncpg.Pool stub whose conn.execute always raises."""

        def acquire(self) -> object:
            class _Ctx:
                async def __aenter__(self) -> "_Ctx":
                    return self

                async def __aexit__(self, *a: object) -> None:
                    pass

                async def execute(self, *a: object) -> None:
                    raise RuntimeError("pg down")

            return _Ctx()

    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    with structlog.testing.capture_logs() as captured:
        name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref,
            payload=_DefaultPayload(tenant_id="acme"),
            settings=None,
            pg_pool=_BoomPool(),  # type: ignore[arg-type]  # Why: duck-typed pool stub; the publish path only needs acquire()->conn->execute
        )

    # Materialization succeeded despite the failed publish…
    assert name == "api-per-tenant:acme"
    assert name in reg.rate_limits
    # …and the failure was logged as a warning, not swallowed silently.
    assert any(e.get("event") == "keyed-rate-limit-bucket-publish-failed" for e in captured)


async def test_resolve_keyed_ref_oversized_key_raises_value_error() -> None:
    """A key longer than 255 characters raises ValueError."""
    reg = RateLimitRegistry()
    long_key = "a" * 256
    ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=lambda _p: long_key)

    with pytest.raises(ValueError, match="exceeds the maximum"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="x"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_key_with_disallowed_chars_raises_value_error() -> None:
    """A key containing characters outside [A-Za-z0-9_\\-:.] raises ValueError."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=lambda _p: "key with spaces")

    with pytest.raises(ValueError, match="outside the allowed set"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="x"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_str_subclass_key_uses_value_content() -> None:
    """A key_fn returning a ``str`` subclass (domain wrapper) is accepted
    and normalized to its plain-str content: the concrete name and the
    registry dict key are true ``str``, identical to returning a plain
    string."""

    class TenantKey(str):
        """Domain wrapper deriving from str."""

    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=lambda p: TenantKey("t1"))

    name = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "api-per-tenant:t1"
    assert type(name) is str
    assert name in reg.rate_limits
    assert all(type(k) is str for k in reg.rate_limits)


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
        ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=lambda p, m=member: m)

        name = await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id=expected), settings=None
        )  # pyright: ignore[reportPrivateUsage]

        assert name == f"api-per-tenant:{expected}"
        assert type(name) is str
        assert name in reg.rate_limits


async def test_resolve_keyed_ref_key_fn_exception_propagates() -> None:
    """An exception raised by key_fn itself is not swallowed — it propagates to the
    caller of _resolve_rate_limit_name / acquire_for_actor."""
    reg = RateLimitRegistry()

    def _boom(payload: _DefaultPayload) -> str:
        raise RuntimeError("key derivation exploded")

    ref = _rate_limit_ref(base_name="api-per-tenant", key_fn=_boom)

    with pytest.raises(RuntimeError, match="key derivation exploded"):
        await reg._resolve_rate_limit_name(
            ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
        )  # pyright: ignore[reportPrivateUsage]


async def test_resolve_keyed_ref_wrong_model_type_raises_validation_error() -> None:
    """A BaseModel payload of a different type is re-validated against the
    ref's payload_type — a missing required field raises ValidationError,
    not AttributeError from key_fn accessing a non-existent attribute."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")  # key_fn does p.tenant_id

    class _UnrelatedPayload(BaseModel):
        unrelated: str

    with pytest.raises(PayloadValidationError):
        await reg._resolve_rate_limit_name(
            ref, payload=_UnrelatedPayload(unrelated="value"), settings=None
        )  # pyright: ignore[reportPrivateUsage]  # Why: exercising private resolution helper directly, matching existing test conventions.


# ── max_keyed_rate_limits guard ───────────────────────────────


async def test_max_keyed_rate_limits_guard_raises_reservation_unavailable() -> None:
    """When the number of keyed rate-limit entries reaches the limit, a new
    key raises ReservationUnavailable."""
    settings = _hardening_settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )
    await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="k2"), settings=settings
    )
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ReservationUnavailable):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_DefaultPayload(tenant_id="k3"), settings=settings
        )


async def test_max_keyed_rate_limits_guard_allows_reusing_existing_key() -> None:
    """Re-resolving an already-tracked key does not trip the guard even at the limit."""
    settings = _hardening_settings(max_keyed=1)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )
    assert len(reg._keyed_rate_limit_last_used) == 1  # pyright: ignore[reportPrivateUsage]

    # Reusing the same key must not raise — no new entry is added.
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )
    assert name == "api-per-tenant:k1"


async def test_max_keyed_rate_limits_guard_skipped_when_settings_none() -> None:
    """When settings is None the guardrail is not enforced (no limit known)."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    for i in range(5):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref,
            payload=_DefaultPayload(tenant_id=f"k{i}"),
            settings=None,
        )

    assert len(reg._keyed_rate_limit_last_used) == 5  # pyright: ignore[reportPrivateUsage]


# ── Opportunistic eviction on the acquisition path ─────────────


async def test_opportunistic_eviction_reclaims_idle_capacity_on_cap_hit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Materialising a new key at the cap succeeds when idle entries exist —
    the acquisition path itself performs an opportunistic eviction, without
    the caller ever calling ``evict_idle_keyed_rate_limits`` directly.

    1. Fill the cap (3 entries) at t=1000.
    2. Advance past the 1-hour idle threshold.
    3. Re-stamp one key as fresh (so only 2 of 3 are stale).
    4. Materialise a NEW key that would exceed the cap — the opportunistic
       eviction inside ``_resolve_rate_limit_name`` reclaims the 2 stale
       entries, making room.  The call succeeds and the new key is
       registered.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _hardening_settings(max_keyed=3)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    # 1. Fill the cap at t=1000.
    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k2"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k3"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_rate_limit_last_used) == 3  # pyright: ignore[reportPrivateUsage]

    # 2. Advance past the 1-hour idle threshold (3600 s).
    fake_time = 5000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)

    # 3. Re-stamp k3 as fresh (last_used=5000) — k1 and k2 remain stale.
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k3"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]

    # 4. Materialise a NEW key — would exceed the cap, but opportunistic
    #    eviction reclaims the 2 stale entries first.
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="k4"), settings=settings
    )

    assert name == "api-per-tenant:k4"
    assert "api-per-tenant:k4" in reg.rate_limits
    # Stale entries were evicted; k3 and k4 remain.
    assert "api-per-tenant:k1" not in reg.rate_limits
    assert "api-per-tenant:k2" not in reg.rate_limits
    assert "api-per-tenant:k3" in reg.rate_limits


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

    settings = _hardening_settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    # Materialise 2 keys — all at the same recent time, nothing idle.
    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k2"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    # A third key at the same time — nothing is idle, so opportunistic
    # eviction reclaims 0 entries and the cap hit is genuine.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_DefaultPayload(tenant_id="k3"), settings=settings
        )

    # Registry is unchanged — no eviction occurred.
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]
    assert "api-per-tenant:k1" in reg.rate_limits
    assert "api-per-tenant:k2" in reg.rate_limits


async def test_opportunistic_eviction_scan_is_amortized_under_sustained_denials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rate-limit twin of the reservation amortization test: the O(n)
    opportunistic eviction scan runs at most once per
    ``_OPPORTUNISTIC_EVICT_MIN_INTERVAL`` — sustained cap-hit denials stay
    O(1) per request.

    1. Fill the cap at t=1000 (nothing idle).
    2. First denied new key → scan runs (first cap-hit always scans).
    3. Second denied new key immediately after → scan is GATED, not run.
    4. Advance past the 30s min-interval → third denied new key → scan
       runs again.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = _hardening_settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    scan_calls: list[timedelta] = []
    real_evict = reg.evict_idle_keyed_rate_limits

    def _spy_evict(*, idle_for: timedelta) -> int:
        scan_calls.append(idle_for)
        return real_evict(idle_for=idle_for)

    monkeypatch.setattr(reg, "evict_idle_keyed_rate_limits", _spy_evict)

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k2"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]
    assert scan_calls == []

    # 2. First denied new key — the scan runs once (nothing idle to reclaim).
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_DefaultPayload(tenant_id="k3"), settings=settings
        )
    assert len(scan_calls) == 1

    # 3. Immediate second denial — the scan is gated: no rescan.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_DefaultPayload(tenant_id="k4"), settings=settings
        )
    assert len(scan_calls) == 1, "scan must be amortized — no rescan within the min interval"

    # 4. Past the min interval, the next cap-hit scans again. (The lambda
    # closes over fake_time, so no re-setattr is needed.)
    fake_time = 1000.0 + 31.0
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_DefaultPayload(tenant_id="k5"), settings=settings
        )
    assert len(scan_calls) == 2


# ── Refill-over-time correctness for a keyed bucket ────────────


async def test_keyed_bucket_refill_over_time_correctness() -> None:
    """A keyed bucket refills correctly over time: after exhausting a key's
    tokens and advancing the clock, exactly the refilled amount is acquirable
    and no more. A sibling key's independent budget is unaffected by the first
    key's exhaustion+advance.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:a", capacity=2, refill_per_second=1.0, backend="memory")
    )
    reg.register(
        TokenBucket(name="api-per-tenant:b", capacity=2, refill_per_second=1.0, backend="memory")
    )
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=2, refill_per_second=1.0)

    # Exhaust key "a" (capacity=2).
    acquired_1 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="a"),
        clock=clock,
    )
    assert acquired_1[0].name == "api-per-tenant:a"

    acquired_2 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="a"),
        clock=clock,
    )
    assert acquired_2[0].name == "api-per-tenant:a"

    # Third acquire for "a" — denied (bucket exhausted).
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="a"),
            clock=clock,
        )

    # Advance clock 1 second — should refill exactly 1 token for key "a".
    clock.advance(timedelta(seconds=1))

    # Key "a" now allows exactly 1 more acquire (the refilled token).
    acquired_3 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="a"),
        clock=clock,
    )
    assert acquired_3[0].name == "api-per-tenant:a"

    # Key "a" denies beyond the refilled amount — only 1 token refilled, not 2.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="a"),
            clock=clock,
        )

    # Key "b" is unaffected — still has its full 2-token budget.
    acquired_b1 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="b"),
        clock=clock,
    )
    assert acquired_b1[0].name == "api-per-tenant:b"

    acquired_b2 = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_DefaultPayload(tenant_id="b"),
        clock=clock,
    )
    assert acquired_b2[0].name == "api-per-tenant:b"

    # Key "b" is now exhausted too — proving it had its own independent budget.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="b"),
            clock=clock,
        )


# ── composition log events stay JSON-serializable with keyed refs ──


async def test_composition_log_events_are_json_serializable_with_keyed_refs() -> None:
    """Regression: ``composition-acquired`` / ``composition-denied`` log
    events must not contain raw pydantic ref instances — orjson (the
    production structlog serializer) raises ``TypeError`` on them, which
    drops the log event inside the logging handler. Refs are rendered as
    ``ClassName(base_name)`` strings instead.
    """
    from structlog.testing import capture_logs

    from taskq._json import dumps_str
    from taskq.ratelimit.reservation import ConcurrencyReservation

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:a", capacity=1, refill_per_second=0, backend="memory")
    )
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s1", slots=1, lease=timedelta(minutes=5), clock=clock
        )
    )
    reg.register(
        ConcurrencyReservation(
            name="session-cap:s2", slots=1, lease=timedelta(minutes=5), clock=clock
        )
    )
    rl_ref = _rate_limit_ref(base_name="api-per-tenant", capacity=1, refill_per_second=0)
    res_ref = KeyedReservationRef.typed(
        _SessionPayload,
        base_name="session-cap",
        key_fn=lambda p: p.session_id,
        slots=1,
        lease=timedelta(minutes=5),
    )

    # ── Success path: composition-acquired carries both keyed refs. ──
    with capture_logs() as logs:
        acquired = await reg.acquire_for_actor(
            rate_limits=[rl_ref],
            reservations=[res_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_CompositionPayload(tenant_id="a", session_id="s1"),
            clock=clock,
        )
    assert len(acquired) == 2
    acquired_events = [e for e in logs if e.get("event") == "composition-acquired"]
    assert len(acquired_events) == 1
    dumps_str(acquired_events[0])  # must not raise TypeError
    assert acquired_events[0]["rate_limits"] == ["KeyedRateLimitRef(api-per-tenant)"]
    assert acquired_events[0]["reservations"] == ["KeyedReservationRef(session-cap)"]

    # ── Denial path: a fresh reservation (s2) acquires fine but the
    # exhausted token bucket denies → composition-denied log. ──
    with capture_logs() as logs, pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[rl_ref],
            reservations=[res_ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_CompositionPayload(tenant_id="a", session_id="s2"),
            clock=clock,
        )
    denied_events = [e for e in logs if e.get("event") == "composition-denied"]
    assert len(denied_events) == 1
    dumps_str(denied_events[0])  # must not raise TypeError
    assert denied_events[0]["rate_limits"] == ["KeyedRateLimitRef(api-per-tenant)"]


# ── acquire_for_actor: AND-composition with keyed rate limits ──


async def test_acquire_for_actor_composes_static_and_keyed_rate_limits() -> None:
    """A static name and a KeyedRateLimitRef in the same rate_limits list are
    both acquired — AND-composition holds for mixed static/keyed lists.

    The dynamic bucket is pre-registered here with a FakeClock so that
    resolution reuses it via the existing idempotent-register path (register()
    no-ops for identical config) — deterministic and fast, in-memory only.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(_token_bucket("global-bucket", capacity=5, refill_per_second=1.0))
    reg.register(
        TokenBucket(name="api-per-tenant:abc", capacity=1, refill_per_second=0, backend="memory")
    )

    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=1, refill_per_second=0)
    job_id = new_uuid()
    worker_id = new_uuid()

    acquired = await reg.acquire_for_actor(
        rate_limits=["global-bucket", ref],
        reservations=[],
        job_id=job_id,
        worker_id=worker_id,
        payload=_DefaultPayload(tenant_id="abc"),
        clock=clock,
    )

    assert len(acquired) == 2
    assert acquired[0].name == "global-bucket"
    assert acquired[1].name == "api-per-tenant:abc"

    # api-per-tenant:abc had only 1 token and it is now consumed — a second
    # acquisition for the same key must be denied, proving the keyed bucket's
    # own budget was actually consumed (not just recorded as a handle).
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_DefaultPayload(tenant_id="abc"),
            clock=clock,
        )


# ── Regression: plain string rate limit alone still works ──────


async def test_plain_string_rate_limit_alone_still_works() -> None:
    """A standalone static rate-limit name (no keyed ref at all) is completely
    unaffected by the keyed rate-limit feature — acquire succeeds, denial still
    raises ReservationUnavailable when exhausted.
    """
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="some-static-bucket", capacity=1, refill_per_second=0, backend="memory")
    )

    # Acquire succeeds.
    acquired = await reg.acquire_for_actor(
        rate_limits=["some-static-bucket"],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        clock=clock,
    )
    assert len(acquired) == 1
    assert acquired[0].name == "some-static-bucket"

    # Denial still raises ReservationUnavailable when exhausted.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=["some-static-bucket"],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            clock=clock,
        )


# ── Cap bounds growth only, never static reuse ──────────────────


async def test_keyed_rate_limit_cap_does_not_deny_static_colliding_reuse() -> None:
    """Rate-limit twin: with the keyed tracking dict AT the cap, resolving
    a key whose concrete name was STATICALLY pre-registered still succeeds
    — the cap bounds keyed-materialized growth, and static reuse grows
    nothing."""
    settings = _hardening_settings(max_keyed=2)
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    # Fill the keyed cap with two fresh materialized keys.
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k1"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="k2"), settings=settings
    )  # pyright: ignore[reportPrivateUsage]
    assert len(reg._keyed_rate_limit_last_used) == 2  # pyright: ignore[reportPrivateUsage]

    # A statically pre-registered bucket whose name collides with the ref's
    # concrete name for key "t1".
    reg.register(
        TokenBucket(name="api-per-tenant:t1", capacity=10, refill_per_second=1.0, backend="memory")
    )

    # Must NOT raise ReservationUnavailable despite the full cap.
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=settings
    )
    assert name == "api-per-tenant:t1"
    # Static entry stays untracked (never evictable by the keyed sweep).
    assert "api-per-tenant:t1" not in reg._keyed_rate_limit_last_used  # pyright: ignore[reportPrivateUsage]


# ── Concrete-name collision behavior ────────────────────────────


async def test_colliding_concrete_names_same_config_share_bucket() -> None:
    """Two refs whose concrete names collide resolve to the SAME registered
    bucket when their configs are identical — pins the documented collision
    behavior for the rate-limit twin."""
    reg = RateLimitRegistry()
    ref_a = _rate_limit_ref(base_name="a")
    ref_ab = _rate_limit_ref(base_name="a:b")

    name_a = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref_a, payload=_DefaultPayload(tenant_id="b:c"), settings=None
    )
    name_ab = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref_ab, payload=_DefaultPayload(tenant_id="c"), settings=None
    )

    assert name_a == name_ab == "a:b:c"
    assert len(reg.rate_limits) == 1
    bucket = reg.get_rate_limit("a:b:c")
    assert isinstance(bucket, TokenBucket)
    assert bucket.capacity == ref_a.capacity


async def test_colliding_concrete_names_different_config_raise_value_error() -> None:
    """Rate-limit twin: a concrete-name collision with different configs
    fails loudly with ``ValueError`` naming the collision."""
    reg = RateLimitRegistry()
    ref_a = _rate_limit_ref(base_name="a", capacity=10)
    ref_ab = _rate_limit_ref(base_name="a:b", capacity=99)

    await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref_a, payload=_DefaultPayload(tenant_id="b:c"), settings=None
    )
    with pytest.raises(ValueError, match="concrete-name collision"):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref_ab, payload=_DefaultPayload(tenant_id="c"), settings=None
        )

    bucket = reg.get_rate_limit("a:b:c")
    assert isinstance(bucket, TokenBucket)
    assert bucket.capacity == 10


async def test_statically_preregistered_entry_is_never_keyed_evicted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A statically pre-registered bucket whose name happens to match a
    keyed ref's concrete name is reused as-is and NOT stamped into the
    keyed tracking dict — the idle-eviction sweep must never evict a
    user's static entry just because a keyed ref resolved to it."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    reg.register(
        TokenBucket(name="api-per-tenant:t1", capacity=10, refill_per_second=1.0, backend="memory")
    )
    ref = _rate_limit_ref(base_name="api-per-tenant")

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 1000.0)
    name = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=None
    )  # pyright: ignore[reportPrivateUsage]
    assert name == "api-per-tenant:t1"
    assert len(reg._keyed_rate_limit_last_used) == 0  # pyright: ignore[reportPrivateUsage]

    # Far past the idle threshold: the static entry survives the sweep.
    monkeypatch.setattr(registry_mod, "monotonic", lambda: 99999.0)
    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))
    assert evicted == 0
    assert "api-per-tenant:t1" in reg.rate_limits


# ── evict_idle_keyed_rate_limits ───────────────────────────────


async def test_evict_idle_keyed_rate_limits_removes_only_stale_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entries idle >= idle_for are evicted; recently-used entries are kept."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="stale"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    fake_time = 1100.0  # 100s later — "stale" key untouched since
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="fresh"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(seconds=50))

    assert evicted == 1
    assert "api-per-tenant:stale" not in reg.rate_limits
    assert "api-per-tenant:fresh" in reg.rate_limits


async def test_evict_idle_keyed_rate_limits_leaves_static_rate_limits_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A statically-registered (non-keyed) TokenBucket is never evicted, even
    when it has been present far longer than idle_for — eviction only ever
    consults _keyed_rate_limit_last_used, which static registrations never
    populate."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    reg.register(_token_bucket("static-global"))

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 5000.0)

    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(seconds=1))

    assert evicted == 0
    assert "static-global" in reg.rate_limits


async def test_evict_idle_keyed_rate_limits_returns_zero_when_nothing_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No entries idle beyond the threshold — returns 0, registry unchanged."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant")

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 42.0)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="recent"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))

    assert evicted == 0
    assert "api-per-tenant:recent" in reg.rate_limits


# ── Memory fixed-quota buckets are exempt from idle eviction ──


async def test_drained_memory_fixed_quota_bucket_survives_idle_eviction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: evicting a drained memory fixed-quota keyed bucket must NOT
    reset its quota.

    Token state for ``backend="memory"`` lives on the bucket instance, so
    popping the registry entry destroys it: the next acquire would
    materialize a fresh bucket at FULL capacity, silently reviving a quota
    designed to never refill — while the Redis backend deliberately holds
    the same state for 24h. Drain -> idle 1h -> sweep -> must stay denied.
    """
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=2,
        refill_per_second=0,
        backend="memory",
    )
    payload = _DefaultPayload(tenant_id="acme")

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)

    # Drain the keyed bucket fully (capacity=2) via the registry's own
    # composition path; the third acquire is denied.
    for _ in range(2):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=payload,
            clock=clock,
        )
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=payload,
            clock=clock,
        )

    # Idle for over an hour, then sweep (what the 30s leader sweep does).
    fake_time += 3601.0
    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))

    assert evicted == 0
    assert "api-per-tenant:acme" in reg.rate_limits

    # The quota is still exhausted — eviction did not reset it.
    with pytest.raises(ReservationUnavailable):
        await reg.acquire_for_actor(
            rate_limits=[ref],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=payload,
            clock=clock,
        )


async def test_full_memory_fixed_quota_bucket_is_still_idle_evicted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A memory fixed-quota bucket that never consumed any quota holds no
    state worth preserving — eviction remains a pure no-op for it, so the
    cardinality bound still applies."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=2,
        refill_per_second=0,
        backend="memory",
    )

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 1000.0)
    # Materialize (register + stamp tracking) WITHOUT acquiring — quota full.
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="acme"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 99999.0)
    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))

    assert evicted == 1
    assert "api-per-tenant:acme" not in reg.rate_limits


async def test_refilling_memory_bucket_is_still_idle_evicted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A memory bucket with refill_per_second > 0 self-heals after eviction
    (its state converges back toward full on its own), so it is NOT exempt —
    the eviction feature itself is preserved."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=2,
        refill_per_second=0.001,  # slow refill: partially refilled at eviction time
        backend="memory",
    )
    payload = _DefaultPayload(tenant_id="acme")

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)

    # Consume one token so the bucket is mid-refill at sweep time.
    await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=payload,
        clock=clock,
    )

    fake_time += 3601.0
    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))

    assert evicted == 1
    assert "api-per-tenant:acme" not in reg.rate_limits


async def test_redis_backend_fixed_quota_bucket_is_still_idle_evicted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis-backed buckets keep state in Redis (24h TTL for fixed quota),
    not on the instance — registry eviction loses nothing, so they are NOT
    exempt. Pins that the exemption is memory-only and does not disable the
    cardinality bound for the default backend."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=2,
        refill_per_second=0,
        backend="redis",
    )

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 1000.0)
    await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="acme"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    monkeypatch.setattr(registry_mod, "monotonic", lambda: 99999.0)
    evicted = reg.evict_idle_keyed_rate_limits(idle_for=timedelta(hours=1))

    assert evicted == 1
    assert "api-per-tenant:acme" not in reg.rate_limits


async def test_evict_idle_keyed_rate_limits_re_registration_after_eviction_is_idempotent() -> None:
    """A key evicted and then acquired again is simply re-registered — no error,
    and the registry converges back to one entry for that key."""
    reg = RateLimitRegistry()
    ref = _rate_limit_ref(base_name="api-per-tenant", capacity=10, refill_per_second=1.0)

    await reg._resolve_rate_limit_name(ref, payload=_DefaultPayload(tenant_id="s1"), settings=None)  # pyright: ignore[reportPrivateUsage]
    reg._rate_limits.pop("api-per-tenant:s1")  # pyright: ignore[reportPrivateUsage] # Why: simulating what evict_idle_keyed_rate_limits does, without needing monotonic control here.
    reg._keyed_rate_limit_last_used.pop("api-per-tenant:s1")  # pyright: ignore[reportPrivateUsage]

    name = await reg._resolve_rate_limit_name(
        ref, payload=_DefaultPayload(tenant_id="s1"), settings=None
    )  # pyright: ignore[reportPrivateUsage]

    assert name == "api-per-tenant:s1"
    assert len(reg.rate_limits) == 1


# ── Redis-backed concurrent acquisition atomicity ──────────────


@pytest.mark.integration
@pytest.mark.redis
async def test_concurrent_keyed_bucket_acquisition_atomicity(redis_url: str) -> None:
    """50 concurrent acquisitions against a per-key Redis bucket with
    capacity=20, refill_per_second=0 — exactly 20 succeed and 30 are denied,
    proving the Lua script's atomicity holds when multiple concurrent callers
    race for the SAME derived per-key bucket, not just a statically-declared one.

    Follows the acquire/assert idiom from test_ratelimit_token_bucket_redis.py's
    100-burst test, adapted for concurrent asyncio.gather.
    """
    reg = RateLimitRegistry()
    base = _unique_name()
    ref = KeyedRateLimitRef.typed(
        _DefaultPayload,
        base_name=base,
        key_fn=lambda p: p.tenant_id,
        capacity=20,
        refill_per_second=0,
    )
    settings = _redis_settings(redis_url)

    # Resolve the key to lazily register the per-key TokenBucket (Redis backend
    # by default), then retrieve it for direct concurrent acquire.
    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_DefaultPayload(tenant_id="t1"), settings=settings
    )
    bucket = reg.get_rate_limit(name)
    assert bucket.backend == "redis"

    client = redis_async.from_url(redis_url, decode_responses=False)
    clock = SystemClock()

    try:
        results = await asyncio.gather(
            *[
                bucket.acquire(redis_client=client, clock=clock, settings=settings)
                for _ in range(50)
            ]
        )

        allowed = sum(1 for r in results if r.allowed)
        denied = sum(1 for r in results if not r.allowed)

        # With capacity=20 and refill_per_second=0, no tokens refill during the
        # run — exactly 20 succeed and 30 are denied.
        assert allowed == 20, f"expected exactly 20 allowed, got {allowed}"
        assert denied == 30, f"expected exactly 30 denied, got {denied}"

        # Every denied result must report allowed=False with retry_after=None
        # (refill_per_second=0 → no retry possible).
        for r in results:
            if not r.allowed:
                assert r.retry_after is None
    finally:
        await client.aclose()


# ── Typed payload validation in _resolve_rate_limit_name ────────


class _TypedPayload(BaseModel):
    tenant_id: str
    region: str = "us-east-1"


class _AliasedPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    tenant_id: str = Field(alias="tenantId")


async def test_resolve_typed_ref_passes_validated_model_to_key_fn() -> None:
    """A dict payload is validated via ref.payload_type.model_validate before
    being passed to key_fn — key_fn receives a BaseModel with attribute access,
    not a raw dict."""
    captured: list[BaseModel] = []
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: (captured.append(p), p.tenant_id)[1],
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenant_id": "t1"}, settings=None
    )

    assert name == "api-per-tenant:t1"
    assert isinstance(captured[0], _TypedPayload)
    assert captured[0].tenant_id == "t1"


async def test_resolve_typed_ref_applies_pydantic_defaults() -> None:
    """A dict payload missing a defaulted field gets the default applied
    during validation — key_fn can read the default value."""
    reg = RateLimitRegistry()
    captured: list[_TypedPayload] = []

    def _capturing_key_fn(payload: _TypedPayload) -> str:
        captured.append(payload)
        return f"{payload.tenant_id}:{payload.region}"

    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=_capturing_key_fn,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenant_id": "t1"}, settings=None
    )

    assert name == "api-per-tenant:t1:us-east-1"
    assert captured[0].region == "us-east-1"


async def test_resolve_typed_ref_applies_aliases() -> None:
    """A dict payload using wire aliases (e.g. 'tenantId') is validated
    with alias resolution — key_fn accesses the field by its Python name
    (p.tenant_id)."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _AliasedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenantId": "t1"}, settings=None
    )

    assert name == "api-per-tenant:t1"


async def test_resolve_typed_ref_accepts_basemodel_payload_directly() -> None:
    """A BaseModel payload of the same type as ref.payload_type is accepted
    directly — zero-cost pass-through, no re-validation."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_TypedPayload(tenant_id="t1"), settings=None
    )

    assert name == "api-per-tenant:t1"


async def test_resolve_typed_ref_validation_error_propagates() -> None:
    """An invalid dict (missing required field) raises ValidationError from
    pydantic validation, not KeyError or AttributeError from key_fn."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    with pytest.raises(PayloadValidationError):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"region": "us-east-1"}, settings=None
        )


async def test_resolve_typed_ref_wrong_model_type_re_validates() -> None:
    """A BaseModel payload of a DIFFERENT type is re-validated against
    ref.payload_type via model_dump() → model_validate(). A compatible
    payload (extra fields ignored) resolves successfully."""

    class _CompatiblePayload(BaseModel):
        tenant_id: str
        extra_field: str = "ignored"

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_CompatiblePayload(tenant_id="t1", extra_field="x"), settings=None
    )

    assert name == "api-per-tenant:t1"


async def test_resolve_typed_ref_same_model_type_zero_cost_passthrough() -> None:
    """A BaseModel payload of the SAME type as ref.payload_type is passed
    directly to key_fn without re-validation — the exact same object (identity
    check)."""
    reg = RateLimitRegistry()
    captured: list[_TypedPayload] = []

    def _capturing_key_fn(payload: _TypedPayload) -> str:
        captured.append(payload)
        return payload.tenant_id

    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=_capturing_key_fn,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    payload = _TypedPayload(tenant_id="t1")
    await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=payload, settings=None
    )

    assert len(captured) == 1
    assert captured[0] is payload


# ── acquire_for_actor with typed refs and BaseModel/dict payloads ──


async def test_acquire_for_actor_accepts_basemodel_payload_with_typed_ref() -> None:
    """acquire_for_actor accepts a BaseModel payload with a typed rate-limit ref."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=5.0,
        refill_per_second=1.0,
        backend="memory",
    )

    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_TypedPayload(tenant_id="t1"),
        clock=clock,
    )

    assert len(acquired) == 1
    assert acquired[0].name == "api-per-tenant:t1"


async def test_acquire_for_actor_typed_ref_with_dict_payload_validates() -> None:
    """acquire_for_actor accepts a dict payload with a typed rate-limit ref —
    the dict is validated via model_validate before key_fn is called."""
    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=5.0,
        refill_per_second=1.0,
        backend="memory",
    )

    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload={"tenant_id": "t1"},
        clock=clock,
    )

    assert len(acquired) == 1
    assert acquired[0].name == "api-per-tenant:t1"


async def test_validation_error_mid_composition_rolls_back_reservation() -> None:
    """ValidationError from a keyed ref's model_validate mid-composition
    rolls back already-acquired reservation slots."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    clock = FakeClock(_START)
    reg = RateLimitRegistry()
    static_res = ConcurrencyReservation(
        name="gpu-static",
        slots=1,
        lease=timedelta(minutes=5),
        clock=clock,
    )
    reg.register(static_res)

    class _StrictPayload(BaseModel):
        model_config = {"extra": "forbid"}
        tenant_id: str

    strict_ref = KeyedRateLimitRef.typed(
        _StrictPayload,
        base_name="strict-rl",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    with pytest.raises(PayloadValidationError):
        await reg.acquire_for_actor(
            rate_limits=[strict_ref],
            reservations=["gpu-static"],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload={"unexpected": "field"},
            clock=clock,
        )

    slot = await static_res.acquire(new_uuid(), new_uuid(), pool=None)
    assert slot == 0
    await static_res.release(slot, new_uuid(), pool=None)


async def test_resolve_typed_ref_wrong_type_in_dict_raises_validation_error() -> None:
    """A dict with the right keys but wrong value types raises ValidationError."""
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _TypedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    with pytest.raises(PayloadValidationError):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload={"tenant_id": 42}, settings=None
        )


async def test_resolve_typed_ref_nested_model_round_trips() -> None:
    """A payload_type with a nested BaseModel field round-trips correctly
    through model_validate — key_fn receives the model with nested
    sub-model instances."""

    class _TenantInfo(BaseModel):
        id: str

    class _NestedPayload(BaseModel):
        tenant: _TenantInfo

    captured: list[_NestedPayload] = []
    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _NestedPayload,
        base_name="api-per-tenant",
        key_fn=lambda p: (captured.append(p), p.tenant.id)[1],
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    name = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload={"tenant": {"id": "acme"}}, settings=None
    )

    assert name == "api-per-tenant:acme"
    assert isinstance(captured[0], _NestedPayload)
    assert isinstance(captured[0].tenant, _TenantInfo)
    assert captured[0].tenant.id == "acme"


async def test_wrong_model_type_with_strict_target_raises_validation_error() -> None:
    """When the ref's payload_type has extra='forbid' and the actor's model
    has extra fields, the model_dump()→model_validate() round-trip raises
    ValidationError — surfacing the misconfiguration."""

    class _StrictTarget(BaseModel):
        model_config = {"extra": "forbid"}
        tenant_id: str

    class _LooseSource(BaseModel):
        tenant_id: str
        extra_field: str = "x"

    reg = RateLimitRegistry()
    ref = KeyedRateLimitRef.typed(
        _StrictTarget,
        base_name="api-per-tenant",
        key_fn=lambda p: p.tenant_id,
        capacity=10.0,
        refill_per_second=1.0,
        backend="memory",
    )

    with pytest.raises(PayloadValidationError):
        await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_LooseSource(tenant_id="t1", extra_field="x"), settings=None
        )
