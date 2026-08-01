"""Tests for taskq._json deserialization behavior.

The principle of least surprise: ``loads`` returns plain Python types
(str, int, float, bool, None, list, dict). Type coercion to UUID, datetime,
etc. is the responsibility of the consuming Pydantic model's
``model_validate``, not the deserializer. This ensures that a developer
who declares ``batch_id: str`` gets a ``str``, and a developer who
declares ``batch_id: UUID`` gets a ``UUID`` — the declared field type
is the source of truth.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import BaseModel

from taskq._json import dumps, dumps_str, loads

# ── loads returns plain types (no UUID revival) ─────────────────────────


class TestLoadsReturnsPlainTypes:
    """loads must NOT convert UUID-like strings to UUID objects."""

    def test_uuid_like_string_stays_str(self) -> None:
        """A string that looks like a UUID must remain a str after loads."""
        uid = str(uuid4())
        data = dumps_str({"batch_id": uid})
        result = loads(data)
        assert isinstance(result["batch_id"], str), (
            f"loads converted a UUID-like string to {type(result['batch_id']).__name__}; "
            "loads must return plain str — type coercion is Pydantic's job"
        )
        assert result["batch_id"] == uid

    def test_nested_uuid_like_string_stays_str(self) -> None:
        """UUID-like strings in nested dicts must remain str."""
        uid = str(uuid4())
        data = dumps_str({"outer": {"inner": {"batch_id": uid}}})
        result = loads(data)
        assert isinstance(result["outer"]["inner"]["batch_id"], str)

    def test_uuid_like_string_in_list_stays_str(self) -> None:
        """UUID-like strings in lists must remain str."""
        uid1 = str(uuid4())
        uid2 = str(uuid4())
        data = dumps_str({"ids": [uid1, uid2]})
        result = loads(data)
        assert all(isinstance(v, str) for v in result["ids"])

    def test_non_uuid_string_stays_str(self) -> None:
        """Regular strings must remain str (sanity check)."""
        data = dumps_str({"name": "hello"})
        result = loads(data)
        assert isinstance(result["name"], str)
        assert result["name"] == "hello"

    def test_int_stays_int(self) -> None:
        """Integers must remain int."""
        data = dumps_str({"count": 42})
        result = loads(data)
        assert isinstance(result["count"], int)
        assert result["count"] == 42


# ── Pydantic model_validate handles type coercion correctly ─────────────


class TestPydanticHandlesCoercion:
    """Pydantic's model_validate coerces str→UUID when the field is UUID,
    and keeps str when the field is str. This is the correct layer for
    type coercion."""

    def test_str_field_accepts_str_from_loads(self) -> None:
        """A model with batch_id: str accepts the str from loads."""

        class MyPayload(BaseModel):
            batch_id: str

        uid = uuid4()
        data = dumps_str({"batch_id": str(uid)})
        raw = loads(data)
        payload = MyPayload.model_validate(raw)
        assert isinstance(payload.batch_id, str)
        assert payload.batch_id == str(uid)

    def test_uuid_field_coerces_str_from_loads(self) -> None:
        """A model with batch_id: UUID coerces the str from loads to UUID."""

        class MyPayload(BaseModel):
            batch_id: UUID

        uid = uuid4()
        data = dumps_str({"batch_id": str(uid)})
        raw = loads(data)
        payload = MyPayload.model_validate(raw)
        assert isinstance(payload.batch_id, UUID)
        assert payload.batch_id == uid

    def test_str_field_roundtrip_through_dumps_loads_validate(self) -> None:
        """Full round-trip: dumps(UUID) → loads(str) → model_validate(str field).

        orjson serializes UUID to its canonical string form. loads returns
        that string as str. model_validate accepts str for a str field.
        """

        class StrPayload(BaseModel):
            batch_id: str

        uid = uuid4()
        data = dumps({"batch_id": uid})
        raw = loads(data)
        # After loads, batch_id is a str (orjson serializes UUID to string)
        assert isinstance(raw["batch_id"], str)
        payload = StrPayload.model_validate(raw)
        assert isinstance(payload.batch_id, str)
        assert payload.batch_id == str(uid)

    def test_uuid_field_roundtrip_through_dumps_loads_validate(self) -> None:
        """Full round-trip: dumps(UUID) → loads(str) → model_validate(UUID field).

        orjson serializes UUID to its canonical string form. loads returns
        that string as str. model_validate coerces str to UUID for a UUID field.
        """

        class UuidPayload(BaseModel):
            batch_id: UUID

        uid = uuid4()
        data = dumps({"batch_id": uid})
        raw = loads(data)
        assert isinstance(raw["batch_id"], str)
        payload = UuidPayload.model_validate(raw)
        assert isinstance(payload.batch_id, UUID)
        assert payload.batch_id == uid
