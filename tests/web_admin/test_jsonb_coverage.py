"""Coverage tests for taskq.web.admin._jsonb.decode_jsonb."""

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")

from taskq.web.admin._jsonb import (
    decode_jsonb,  # Why: importorskip guard must precede.
)


class TestDecodeJsonb:
    def test_none_returns_none(self) -> None:
        assert decode_jsonb(None) is None

    def test_dict_returned_as_is(self) -> None:
        value = {"notify_enabled": True, "count": 3}
        assert decode_jsonb(value) is value

    def test_valid_json_string_parsed_to_dict(self) -> None:
        assert decode_jsonb('{"notify_enabled": true}') == {"notify_enabled": True}

    def test_valid_json_string_parsed_to_list(self) -> None:
        assert decode_jsonb("[1, 2, 3]") == [1, 2, 3]

    def test_valid_json_string_parsed_to_scalar(self) -> None:
        assert decode_jsonb("42") == 42

    def test_invalid_json_string_returned_raw(self) -> None:
        raw = "not-json{"
        assert decode_jsonb(raw) is raw

    def test_empty_string_returned_raw(self) -> None:
        # Empty string is not valid JSON -> json.loads raises -> returned as-is.
        assert decode_jsonb("") == ""

    def test_bytes_returned_as_is(self) -> None:
        data = b'{"k": 1}'
        assert decode_jsonb(data) is data

    def test_int_returned_as_is(self) -> None:
        assert decode_jsonb(7) == 7

    def test_list_returned_as_is(self) -> None:
        value = [1, 2, 3]
        assert decode_jsonb(value) is value
