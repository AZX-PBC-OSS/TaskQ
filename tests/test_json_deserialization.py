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

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._json import dumps, dumps_jsonb_str, dumps_str, loads
from taskq.backend._records import jsonb_param
from taskq.worker._handlers import _TERMINAL_WRITE_INFRA_EXCEPTIONS

# ── The misclassification this guard exists to prevent ──────────────────


def test_untranslatable_character_error_matches_the_infra_tuple() -> None:
    """Pins the framing: a NUL rejected by ``jsonb_in`` is indistinguishable
    from a transient database fault at the terminal-write site.

    ``UntranslatableCharacterError`` (SQLSTATE 22P05) derives from
    ``DataError`` and so from ``PostgresError``, the blanket entry in
    ``_TERMINAL_WRITE_INFRA_EXCEPTIONS``. That tuple classifies infra faults
    as retryable, so a permanent data defect would be retried forever. If
    this assertion ever fails, the rationale for rejecting at enqueue has
    changed and the guard should be revisited rather than silently kept.
    """
    exc = asyncpg.exceptions.UntranslatableCharacterError
    assert exc.sqlstate == "22P05"
    assert issubclass(exc, asyncpg.PostgresError)
    assert issubclass(exc, _TERMINAL_WRITE_INFRA_EXCEPTIONS)


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


# ── jsonb NUL rejection ─────────────────────────────────────────────────


class TestDumpsJsonbStrRejectsNul:
    """PostgreSQL ``jsonb`` decodes to ``text``, which cannot hold a NUL, so
    ``'{"a":"\\u0000"}'::jsonb`` fails with SQLSTATE 22P05 even though the
    same value is accepted by a ``json`` column.

    Caller-supplied payloads, metadata and actor results are validated for
    type and size only, so without this guard a NUL reaches the INSERT and
    raises ``asyncpg.UntranslatableCharacterError``. On the terminal-write
    path that is misread as transient infrastructure failure, leaving the job
    ``running`` to be reclaimed and re-run forever.
    """

    def test_nul_in_value_rejected(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            dumps_jsonb_str({"text": "a\x00b"})

    def test_nul_in_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            dumps_jsonb_str({"k\x00": "v"})

    def test_nul_nested_in_list_rejected(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            dumps_jsonb_str({"a": {"b": [{"c": "\x00"}]}})

    def test_jsonb_param_rejects_nul(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            jsonb_param({"text": "a\x00b"})

    def test_jsonb_param_rejects_nul_nested(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            jsonb_param({"outer": [{"inner": "\x00"}]})

    def test_jsonb_param_none_passes_through(self) -> None:
        assert jsonb_param(None) is None

    def test_jsonb_param_clean_value_unchanged(self) -> None:
        assert jsonb_param({"a": "b"}) == '{"a":"b"}'

    def test_literal_backslash_u0000_is_not_a_nul(self) -> None:
        """The cheap prefilter looks for the six characters orjson emits for a
        NUL, which are also what it emits for the *literal* text
        ``\\u0000``. jsonb stores that happily, so it must not be rejected."""
        assert dumps_jsonb_str({"text": "\\u0000"}) == '{"text":"\\\\u0000"}'

    def test_other_c0_controls_are_allowed(self) -> None:
        """Only NUL is fatal to jsonb; the rest of C0 round-trips fine."""
        assert dumps_jsonb_str({"text": "a\x01\x1f\tb"}) == '{"text":"a\\u0001\\u001f\\tb"}'

    def test_ordinary_value_unchanged(self) -> None:
        assert dumps_jsonb_str({"n": 1}) == dumps_str({"n": 1})
