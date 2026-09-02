"""Unit tests for RateLimitRef, ReservationRef, and keyed ref ``.typed()``.

Tests pydantic model construction, field defaults, the ratelimit
re-exports for the ref types, and the ``.typed()`` classmethod +
required ``payload_type`` field on ``KeyedRateLimitRef`` /
``KeyedReservationRef``.
"""

from datetime import timedelta

import pytest
from pydantic import BaseModel, Field, ValidationError

from taskq.exceptions import PayloadValidationError
from taskq.ratelimit import RateLimitRef, ReservationRef
from taskq.ratelimit.refs import (
    KeyedRateLimitRef,
    KeyedReservationRef,
)
from taskq.ratelimit.refs import (
    RateLimitRef as RateLimitRefDirect,
)
from taskq.ratelimit.refs import (
    ReservationRef as ReservationRefDirect,
)


class TestRateLimitRef:
    def test_construction_with_defaults(self) -> None:
        r = RateLimitRef(name="openai")
        assert r.name == "openai"
        assert r.count == 1.0

    def test_construction_with_count(self) -> None:
        r = RateLimitRef(name="openai", count=5.0)
        assert r.count == 5.0

    def test_mutable_by_default(self) -> None:
        r = RateLimitRef(name="openai")
        r.name = "other"
        assert r.name == "other"

    def test_rejects_missing_name(self) -> None:
        with pytest.raises(ValidationError):
            RateLimitRef()  # type: ignore[call-arg]

    def test_rejects_non_string_name(self) -> None:
        with pytest.raises(ValidationError):
            RateLimitRef(name=42)  # type: ignore[arg-type]


class TestReservationRef:
    def test_construction(self) -> None:
        r = ReservationRef(name="gpu_pool")
        assert r.name == "gpu_pool"

    def test_mutable_by_default(self) -> None:
        r = ReservationRef(name="gpu_pool")
        r.name = "other"
        assert r.name == "other"

    def test_rejects_missing_name(self) -> None:
        with pytest.raises(ValidationError):
            ReservationRef()  # type: ignore[call-arg]


class TestRefReExports:
    def test_rate_limit_ref_from_ratelimit(self) -> None:
        import taskq.ratelimit as rl

        assert rl.RateLimitRef is RateLimitRefDirect

    def test_reservation_ref_from_ratelimit(self) -> None:
        import taskq.ratelimit as rl

        assert rl.ReservationRef is ReservationRefDirect


# ── KeyedRateLimitRef.typed() ────────────────────────────────────


class _TenantPayload(BaseModel):
    tenant_id: str


class TestKeyedRateLimitRefTyped:
    def test_typed_stores_payload_type(self) -> None:
        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        assert ref.payload_type is _TenantPayload

    def test_construction_without_payload_type_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            KeyedRateLimitRef(  # type: ignore[call-arg]
                base_name="api-per-tenant",
                key_fn=lambda p: "x",
                capacity=10.0,
                refill_per_second=1.0,
            )

    def test_typed_key_fn_receives_model_not_dict(self) -> None:
        received: list[object] = []

        def _capture(p: _TenantPayload) -> str:
            received.append(p)
            return p.tenant_id

        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="api-per-tenant",
            key_fn=_capture,
            capacity=10.0,
            refill_per_second=1.0,
        )
        result = ref.key_fn(_TenantPayload(tenant_id="acme"))
        assert result == "acme"
        assert isinstance(received[0], _TenantPayload)

    def test_typed_with_backend_memory(self) -> None:
        ref = KeyedRateLimitRef.typed(
            _TenantPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
            backend="memory",
        )
        assert ref.backend == "memory"

    def test_typed_with_default_field(self) -> None:
        class _PayloadWithDefault(BaseModel):
            tenant_id: str = "default-tenant"

        ref = KeyedRateLimitRef.typed(
            _PayloadWithDefault,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        model = _PayloadWithDefault()
        assert ref.key_fn(model) == "default-tenant"

    def test_typed_with_alias(self) -> None:
        class _AliasedPayload(BaseModel):
            model_config = {"populate_by_name": True}
            tenant_id: str = Field(alias="tenantId")

        ref = KeyedRateLimitRef.typed(
            _AliasedPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        model = _AliasedPayload.model_validate({"tenantId": "acme"})
        assert ref.key_fn(model) == "acme"

    def test_typed_rejects_basemodel_itself_as_payload_type(self) -> None:
        with pytest.raises(ValidationError, match="concrete BaseModel subclass"):
            KeyedRateLimitRef.typed(
                BaseModel,
                base_name="api-per-tenant",
                key_fn=lambda p: "x",
                capacity=10.0,
                refill_per_second=1.0,
            )

    async def test_typed_extra_forbid_rejects_dict_with_extra_keys(self) -> None:
        """A ref whose payload_type has extra='forbid' rejects dicts
        with unexpected keys — the registry's model_validate raises
        ValidationError, not a silent acceptance."""
        from taskq.ratelimit.registry import RateLimitRegistry

        class _StrictPayload(BaseModel):
            model_config = {"extra": "forbid"}
            tenant_id: str

        ref = KeyedRateLimitRef.typed(
            _StrictPayload,
            base_name="api-per-tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10.0,
            refill_per_second=1.0,
        )
        reg = RateLimitRegistry()
        with pytest.raises(PayloadValidationError):
            await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]
                ref,
                payload={"tenant_id": "acme", "unexpected": "field"},
                settings=None,
            )


# ── KeyedReservationRef.typed() ──────────────────────────────────


class _SessionPayload(BaseModel):
    session_id: str


class TestKeyedReservationRefTyped:
    def test_typed_stores_payload_type(self) -> None:
        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        assert ref.payload_type is _SessionPayload

    def test_construction_without_payload_type_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            KeyedReservationRef(  # type: ignore[call-arg]
                base_name="session-cap",
                key_fn=lambda p: "x",
                slots=3,
                lease=timedelta(minutes=5),
            )

    def test_typed_key_fn_receives_model_not_dict(self) -> None:
        received: list[object] = []

        def _capture(p: _SessionPayload) -> str:
            received.append(p)
            return p.session_id

        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="session-cap",
            key_fn=_capture,
            slots=3,
            lease=timedelta(minutes=5),
        )
        result = ref.key_fn(_SessionPayload(session_id="s1"))
        assert result == "s1"
        assert isinstance(received[0], _SessionPayload)

    def test_typed_with_default_field(self) -> None:
        class _PayloadWithDefault(BaseModel):
            session_id: str = "default-session"

        ref = KeyedReservationRef.typed(
            _PayloadWithDefault,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        model = _PayloadWithDefault()
        assert ref.key_fn(model) == "default-session"

    def test_typed_with_alias(self) -> None:
        class _AliasedPayload(BaseModel):
            model_config = {"populate_by_name": True}
            session_id: str = Field(alias="sessionId")

        ref = KeyedReservationRef.typed(
            _AliasedPayload,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        model = _AliasedPayload.model_validate({"sessionId": "s1"})
        assert ref.key_fn(model) == "s1"

    def test_typed_rejects_basemodel_itself_as_payload_type(self) -> None:
        with pytest.raises(ValidationError, match="concrete BaseModel subclass"):
            KeyedReservationRef.typed(
                BaseModel,
                base_name="session-cap",
                key_fn=lambda p: "x",
                slots=3,
                lease=timedelta(minutes=5),
            )

    async def test_typed_extra_forbid_rejects_dict_with_extra_keys(self) -> None:
        """A ref whose payload_type has extra='forbid' rejects dicts
        with unexpected keys through the registry."""
        from taskq.ratelimit.registry import RateLimitRegistry

        class _StrictPayload(BaseModel):
            model_config = {"extra": "forbid"}
            session_id: str

        ref = KeyedReservationRef.typed(
            _StrictPayload,
            base_name="session-cap",
            key_fn=lambda p: p.session_id,
            slots=3,
            lease=timedelta(minutes=5),
        )
        reg = RateLimitRegistry()
        with pytest.raises(PayloadValidationError):
            await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
                ref,
                payload={"session_id": "s1", "unexpected": "field"},
                pg_pool=None,
                settings=None,
            )

    async def test_typed_empty_key_error_does_not_embed_payload(self) -> None:
        """Reservation-side twin of the key-validation sanitize contract:
        the key_fn empty/non-str ValueError propagates into persisted
        error_message (job row / web admin), so it must not embed payload
        values — including the model_dump of a validated model payload."""
        from taskq.ratelimit.registry import RateLimitRegistry

        canary = "TOP-SECRET-canary-d4e5f6"
        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="session-cap",
            key_fn=lambda p: "",
            slots=3,
            lease=timedelta(minutes=5),
        )
        reg = RateLimitRegistry()
        with pytest.raises(ValueError, match="an empty key or non-string value") as exc_info:
            await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
                ref,
                payload=_SessionPayload(session_id=canary),
                pg_pool=None,
                settings=None,
            )

        assert canary not in str(exc_info.value)
