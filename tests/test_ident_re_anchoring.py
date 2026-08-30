"""`_IDENT_RE` must reject a trailing newline.

Python's `$` matches immediately before a trailing newline, so the original
`^[A-Za-z_][A-Za-z0-9_]*$` accepted `"taskq\\n"`.

Scope, stated honestly: this was NOT an injection path, and calling it one would
overstate it. Only a single trailing newline slipped through --
`"taskq\\nDROP TABLE x"` was always rejected, because the remainder still has to
match a charset that admits no whitespace, quote or SQL metacharacter. Every
interpolation site double-quotes the validated value, and a newline is legal
inside a quoted SQL identifier, so `"taskq\\n"` would address a schema literally
named `taskq<newline>` rather than escaping the quoting.

The fix is worth making because this is the canonical identifier validator
reused across a dozen modules and re-implemented by downstream consumers, so it
should mean exactly what it appears to mean.
"""

from __future__ import annotations

import pytest

from taskq.constants import _IDENT_RE


@pytest.mark.parametrize("value", ["taskq", "_private", "a1", "A_1_b", "_"])
def test_valid_identifiers_still_accepted(value: str) -> None:
    assert _IDENT_RE.match(value)


@pytest.mark.parametrize(
    "value",
    [
        "taskq\n",  # the regression: accepted before the \Z anchor
        "\ntaskq",
        "taskq\n\n",
        "taskq\r\n",
        "taskq\nDROP TABLE x",
        'ta"skq',
        "1abc",
        "",
        "has space",
        "semi;colon",
    ],
)
def test_invalid_identifiers_rejected(value: str) -> None:
    assert not _IDENT_RE.match(value)


def test_trailing_newline_specifically() -> None:
    """Pinned on its own: this is the exact case `$` let through."""
    assert not _IDENT_RE.match("taskq\n"), "`$` semantics are back; use \\Z"


def test_pattern_uses_absolute_anchors() -> None:
    assert _IDENT_RE.pattern.startswith("\\A")
    assert _IDENT_RE.pattern.endswith("\\Z")


def test_callers_reject_the_trailing_newline_end_to_end() -> None:
    """The regex is only useful via its callers."""
    from taskq.constants import quote_ident, wake_channel

    with pytest.raises(ValueError, match="invalid SQL identifier"):
        quote_ident("taskq\n")
    with pytest.raises(ValueError, match="invalid schema identifier"):
        wake_channel("taskq\n")

    # And still work for the legitimate value.
    assert quote_ident("taskq") == '"taskq"'
