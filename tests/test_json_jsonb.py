"""Unit tests for jsonb-safe serialization in ``taskq._json``.

``main`` has no dedicated unit-test module for ``taskq._json`` or
``taskq.backend._records``; both are exercised only incidentally by suites
that import ``dumps_str`` as a helper.  This module is that home for the
jsonb binding contract.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._json import dumps_jsonb_str, dumps_str
from taskq.backend._records import jsonb_param
from taskq.worker._handlers import _TERMINAL_WRITE_INFRA_EXCEPTIONS

# ── The misclassification this guard exists to prevent ──────────────────


def test_untranslatable_character_error_matches_the_infra_tuple() -> None:
    """Pins the framing: a NUL rejected by ``jsonb_in`` is indistinguishable
    from a transient database fault at the terminal-write site.

    ``UntranslatableCharacterError`` (SQLSTATE 22P05) derives from
    ``DataError`` and so from ``PostgresError``, the blanket entry in
    ``_TERMINAL_WRITE_INFRA_EXCEPTIONS``.  That tuple classifies infra faults
    as retryable, so a permanent data defect would be retried forever.  If
    this assertion ever fails, the rationale for rejecting at enqueue has
    changed and the guard should be revisited rather than silently kept.
    """
    exc = asyncpg.exceptions.UntranslatableCharacterError
    assert exc.sqlstate == "22P05"
    assert issubclass(exc, asyncpg.PostgresError)
    assert issubclass(exc, _TERMINAL_WRITE_INFRA_EXCEPTIONS)


# ── jsonb NUL rejection ─────────────────────────────────────────────────


class TestDumpsJsonbStrRejectsNul:
    """PostgreSQL ``jsonb`` decodes to ``text``, which cannot hold a NUL, so
    ``'{"a":"\\u0000"}'::jsonb`` fails with SQLSTATE 22P05 even though the
    same value is accepted by a ``json`` column.

    Caller-supplied payloads, metadata and actor results are validated for
    type and size only, so without this guard a NUL reaches the INSERT and
    raises ``asyncpg.UntranslatableCharacterError``.  On the terminal-write
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

    def test_literal_backslash_u0000_is_not_a_nul(self) -> None:
        """The cheap prefilter looks for the six characters orjson emits for a
        NUL, which are also what it emits for the *literal* text
        ``\\u0000``.  jsonb stores that happily, so it must not be rejected."""
        assert dumps_jsonb_str({"text": "\\u0000"}) == '{"text":"\\\\u0000"}'

    def test_other_c0_controls_are_allowed(self) -> None:
        """Only NUL is fatal to jsonb; the rest of C0 round-trips fine."""
        assert dumps_jsonb_str({"text": "a\x01\x1f\tb"}) == '{"text":"a\\u0001\\u001f\\tb"}'

    def test_ordinary_value_unchanged(self) -> None:
        assert dumps_jsonb_str({"n": 1}) == dumps_str({"n": 1})


class TestJsonbParamRejectsNul:
    """``jsonb_param`` is the single funnel every ``::jsonb`` bind goes
    through, so the guard is enforced there rather than at each call site."""

    def test_rejects_nul(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            jsonb_param({"text": "a\x00b"})

    def test_rejects_nul_nested(self) -> None:
        with pytest.raises(ValueError, match="NUL character"):
            jsonb_param({"outer": [{"inner": "\x00"}]})

    def test_none_still_passes_through(self) -> None:
        assert jsonb_param(None) is None

    def test_clean_value_unchanged(self) -> None:
        assert jsonb_param({"a": "b"}) == '{"a":"b"}'
