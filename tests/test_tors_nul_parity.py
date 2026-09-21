"""The tors NUL scan answers the escape-parity question.

``taskq._json._encoded_has_nul`` guards MAX_RESULT_BYTES on every terminal
write. tors is a first-party core dependency (same org, AZX PBC), so the
scan IS ``tors.contains_unescaped`` -- one code path, no probe, no pure
fallback. It must answer exactly one question -- is there a ``\\u0000``
byte run here whose immediately-preceding backslash run has EVEN length --
because the verdict decides whether a job's terminal write raises
``NUL_JSONB_ERROR`` or binds to jsonb.

This module pins that answer two ways:

* the named adversarial shapes (mid / terminal / dense / escaped-literal /
  backslash-run boundaries) against an INDEPENDENT oracle spelled here,
  not against the tors implementation -- sharing no code with the scan is
  what makes a differential failure meaningful;
* a randomized byte sweep (hypothesis) over backslash-heavy payloads,
  the shapes where a parity gap could actually hide.

The shape algebra, so the expectations below are checkable: payload bytes
``k`` backslashes followed by ``u0000`` contain exactly one needle match
(the k-th backslash), and the run immediately before it is ``k - 1`` --
so an ODD number of backslashes is a live NUL escape (JSON: ``\\u0000``)
and an EVEN number is the literal text ``u0000`` behind escaped
backslashes, which jsonb accepts. The escaped-literal worst shape
(``tests/test_nul_scan_scaling.py``'s corpus) is the even case, ``k = 2``.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._json import (
    NUL_JSONB_ERROR,
    _encoded_has_nul,
    dumps_jsonb_str,
    dumps_str,
)

# The oracle: the escape-parity rule re-derived from the CONTRACT (an
# occurrence is live iff the maximal backslash run immediately before it is
# even), written differently from both implementations -- a backward scan
# over an index list rather than a forward find loop. Sharing no code with
# either side is what makes a differential failure meaningful.
_NUL_ESCAPE = b"\\u0000"


def _oracle(data: bytes) -> bool:
    start = 0
    while True:
        pos = data.find(_NUL_ESCAPE, start)
        if pos == -1:
            return False
        run = pos
        while run > 0 and data[run - 1] == 0x5C:  # b"\\"
            run -= 1
        if (pos - run) % 2 == 0:
            return True
        start = pos + 1


def _escape_literals(n: int) -> bytes:
    """n escaped-literal occurrences (an odd -- one-backslash -- run before each match).

    The scan's worst shape: no match is live, so the scan must walk the
    whole payload and confirm every one of them.
    """
    return b'"' + b"\\\\u0000" * n + b'"'


# --- the adversarial shapes -------------------------------------------------


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        # no backslash, no escape: the common clean result
        (b'"clean result payload"', False),
        # empty payload: the scan runs before the empty-bytes guard upstream
        (b"", False),
        # a live escape orjson actually emits for a real NUL: mid-payload
        (b'"mid\\u0000point"', True),
        # live at the very end and the very start of the payload
        (b'"terminal\\u0000"', True),
        (b'"\\u0000terminal"', True),
        # live at offset 0 of the WHOLE payload (no preceding byte at all:
        # an empty run, 0 is even)
        (b'\\u0000"', True),
        # dense: every byte run is a live escape
        (b'""' + b"\\u0000" * 64, True),
        # escaped-literal: every match rejected, full walk, False
        (_escape_literals(64), False),
        # the run algebra from the module docstring: odd k live, even k literal
        (b'"' + b"\\" + b"u0000" + b'"', True),  # k=1: the escape itself
        (b'"' + b"\\" * 2 + b"u0000" + b'"', False),  # k=2: literal text
        (b'"' + b"\\" * 3 + b"u0000" + b'"', True),  # k=3: \\ then \u0000
        (b'"' + b"\\" * 4 + b"u0000" + b'"', False),  # k=4: two escaped pairs
        (b'"' + b"\\" * 5 + b"u0000" + b'"', True),  # k=5: \\ \\ then \u0000
        # an escaped-literal run riding into a live escape
        (b'"' + b"\\\\u0000" + b"\\u0000" + b'"', True),
        # a live escape riding into an escaped-literal run
        (b'"' + b"\\u0000" + b"\\\\u0000" + b'"', True),
        # multibyte UTF-8 around the escape: byte offsets must not drift
        ('"héllo→\\u0000→wörld"'.encode(), True),
        ('"héllo→\\\\u0000→wörld"'.encode(), False),
        # the escape truncated at a multibyte boundary is not a match
        ("é".encode() + b"\\u0", False),
    ],
)
def test_named_adversarial_shapes_match_the_oracle(data: bytes, expected: bool) -> None:
    assert _encoded_has_nul(data) is expected
    assert _oracle(data) is expected, "oracle disagrees with the pin's expectation"


# --- randomized sweep -------------------------------------------------------


def _backslash_heavy_bytes() -> st.SearchStrategy[bytes]:
    """Byte strings over the alphabet that can actually move the verdict."""
    return st.lists(
        st.sampled_from(
            [
                b"\\",
                b"\\\\",
                b"u0000",
                b"\\u0000",
                b'"',
                b"a",
                "é".encode(),
                "→".encode(),
            ]
        ),
        max_size=60,
    ).map(b"".join)


@settings(max_examples=500, deadline=None)
@given(_backslash_heavy_bytes())
def test_randomized_backslash_heavy_sweep_matches_the_oracle(data: bytes) -> None:
    assert _encoded_has_nul(data) == _oracle(data)


# --- the consumers of the verdict -------------------------------------------


def test_dumps_jsonb_str_raises_the_pinned_error_for_a_live_nul() -> None:
    with pytest.raises(ValueError) as exc_info:
        dumps_jsonb_str({"k": "a\x00b"})
    assert str(exc_info.value) == NUL_JSONB_ERROR


def test_dumps_jsonb_str_accepts_the_escaped_literal_text() -> None:
    """The literal text ``\\u0000`` (an even backslash run) binds: jsonb accepts it."""
    value = {"k": "a\\u0000b"}  # a real backslash, then the text u0000 -- no NUL
    assert dumps_jsonb_str(value) == dumps_str(value)
    assert "\\u0000" in dumps_jsonb_str(value)
    assert _encoded_has_nul(dumps_jsonb_str(value).encode()) is False


def test_dumps_jsonb_str_rejects_a_real_nul_inside_a_backslash_rich_value() -> None:
    """A real NUL adjacent to backslash text: the parity rule must see it."""
    with pytest.raises(ValueError, match="NUL character"):
        dumps_jsonb_str({"k": "a\\\\\x00"})


@pytest.mark.parametrize("n_units", [9361, 9362, 9363])
def test_verdicts_hold_at_the_max_result_bytes_boundary(n_units: int) -> None:
    """The scan's verdict is stable across the MAX_RESULT_BYTES boundary.

    The terminal write scans result_bytes before the size bound is
    reported; payloads at and around 65536 bytes must classify the same,
    including the no-live-escape full walk at exactly the bound (9362
    seven-byte units + the two quote bytes).
    """
    payload = _escape_literals(n_units)
    assert 65_529 <= len(payload) <= 65_543
    assert _encoded_has_nul(payload) == _oracle(payload)
