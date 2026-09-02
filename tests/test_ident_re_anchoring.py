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

The same trap existed in three sibling validators that copied the `^...$`
style: `_QUEUE_NAME_RE` (queue names at the enqueue/actor chokepoints),
`_TAG_RE` (job tags), and `_KEYED_KEY_RE` (keyed-ref name components — pinned
in tests/test_keyed_reservation_hardening.py, where its callers live). They are
re-anchored identically; the sections below pin each.
"""

from __future__ import annotations

import pytest

from taskq.backend._protocol import _QUEUE_NAME_RE, _validate_queue_name
from taskq.client._args import _TAG_RE, _validate_and_dedup_tags
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


# ── _QUEUE_NAME_RE: the same trap for queue names ───────────────────────


@pytest.mark.parametrize(
    "value",
    ["default", "q1", "my-queue", "my.queue", "_internal", "Q_2.x"],
)
def test_valid_queue_names_still_accepted(value: str) -> None:
    assert _QUEUE_NAME_RE.match(value)


@pytest.mark.parametrize(
    "value",
    [
        "default\n",  # the regression: accepted before the \Z anchor
        "deafult\n",
        "\ndefault",
        "default\n\n",
        "deafult ",  # charset (space) — never valid, pinned for clarity
        "1queue",
        "",
    ],
)
def test_invalid_queue_names_rejected(value: str) -> None:
    assert not _QUEUE_NAME_RE.match(value)


def test_queue_name_trailing_newline_specifically() -> None:
    """Pinned on its own: this is the exact case `$` let through."""
    assert not _QUEUE_NAME_RE.match("default\n"), "`$` semantics are back; use \\Z"


def test_queue_name_pattern_uses_absolute_anchors() -> None:
    assert _QUEUE_NAME_RE.pattern.startswith("\\A")
    assert _QUEUE_NAME_RE.pattern.endswith("\\Z")


def test_queue_name_caller_rejects_the_trailing_newline() -> None:
    """The regex is only useful via its callers: _validate_queue_name is
    what the enqueue and actor chokepoints run."""
    with pytest.raises(ValueError, match="invalid queue name"):
        _validate_queue_name("default\n")

    assert _validate_queue_name("default") == "default"


# ── _TAG_RE: the same trap for job tags ─────────────────────────────────


@pytest.mark.parametrize(
    "value",
    ["tag1", "my-tag", "abc", "a_9-z", "Run-42"],
)
def test_valid_tags_still_accepted(value: str) -> None:
    assert _TAG_RE.match(value)


@pytest.mark.parametrize(
    "value",
    [
        "tag\n",  # the regression: accepted before the \Z anchor
        "my-tag\n",
        "\ntag",
        "ta\ng",  # mid-string newline — always rejected by the charset
        "-tag",  # leading hyphen — charset
        "tag-",  # trailing hyphen — charset
        "ab",  # min length 3
        "",
    ],
)
def test_invalid_tags_rejected(value: str) -> None:
    assert not _TAG_RE.match(value)


def test_tag_trailing_newline_specifically() -> None:
    """Pinned on its own: this is the exact case `$` let through."""
    assert not _TAG_RE.match("tag\n"), "`$` semantics are back; use \\Z"


def test_tag_pattern_uses_absolute_anchors() -> None:
    assert _TAG_RE.pattern.startswith("\\A")
    assert _TAG_RE.pattern.endswith("\\Z")


def test_tag_caller_rejects_the_trailing_newline() -> None:
    """The regex is only useful via its callers: _validate_and_dedup_tags
    is what build_enqueue_args runs."""
    with pytest.raises(ValueError, match="invalid tag"):
        _validate_and_dedup_tags(["tag\n"])

    assert _validate_and_dedup_tags(["tag1"]) == ("tag1",)
