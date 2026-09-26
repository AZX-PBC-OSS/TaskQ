"""``tors.utf8_byte_len`` answers the byte-cap question at the terminal
write.

``taskq.backend._terminal`` guards ``MAX_RESULT_BYTES`` on every
successful job with a dict result: the serialized result's UTF-8 length
decides ``ResultTooLarge``. The pre-tors code paid a full
``str.encode("utf-8")`` copy of the payload to learn a number tors keeps
O(1) - the measured ADOPT in ``docs/design/tors-adoption-map.md``
(~12x p50 single-thread, ~6-9.5x contended at the real 64 KiB cap). The
swap is the NUL-scan workstream's sequel on the same file: the
coordination note parked this adoption behind that workstream, which has
since landed (``taskq._json._encoded_has_nul`` IS
``tors.contains_unescaped``).

This module pins the swap three ways:

* the differential: ``tors.utf8_byte_len(s) == len(s.encode("utf-8"))``
  against an INDEPENDENT oracle (the stdlib encode, spelled here) over
  the shapes where a wrong length could hide - astral code points,
  combining marks, ZWJ sequences, mixed-width CJK, the 64 KiB cap
  boundary;
* the error contract: a lone surrogate must raise the SAME exception
  class the encode raises (``UnicodeEncodeError``), never a silent wrong
  count - a wrong count here would either blackhole legal results or
  admit oversized ones past the cap;
* the perf gate (``load_sensitive``, the serial lane): min-of-N
  interleaved timing against the replaced ``str.encode`` baseline, the
  house rules from ``test_tors_nul_perf.py`` - tors must win by the
  gated factor on the real 64 KiB shape, or the swap does not ship.
"""

from __future__ import annotations

import time

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tors import utf8_byte_len

# ── The differential: the count is byte-exact ────────────────────────────


@pytest.mark.parametrize(
    "payload",
    [
        "hello, world",  # plain ASCII
        "café naïve résumé",  # Latin-1 supplement (2-byte)
        " Hello, world".lstrip(),  # trivial guard against an edit slip
        "中文字符测试",  # CJK, BMP 3-byte
        "TaskQ 任务队列 — émoticône 🚀",  # mixed 1-4 byte, punctuation
        "\U0001f680\U0001f4a3\U0001f525",  # astral, outside the BMP
        "étude",  # combining mark: e + U+0301 (2 code points, 3 bytes)
        "👨‍👩‍👧‍👦 family",  # ZWJ sequence: 7 code points, one grapheme
        "x" * 65536,  # the ASCII 64 KiB cap boundary
        "🚀" * 16384,  # the astral 64 KiB boundary (4 bytes each)
        "中" * 21845 + "x",  # the 3-byte 64 KiB boundary + 1
    ],
)
def test_byte_len_matches_the_stdlib_encode_byte_exact(payload: str) -> None:
    oracle = len(payload.encode("utf-8"))
    assert utf8_byte_len(payload) == oracle


@given(
    st.text(
        alphabet=st.characters(
            # Every code-point class a result payload could carry:
            # ASCII, Latin, CJK, astral emoji, combining marks, ZWJ.
            blacklist_categories=("Cs",),  # lone surrogates: the error pin below
        ),
        max_size=4096,
    )
)
@settings(max_examples=256, deadline=None)
def test_byte_len_property_matches_the_stdlib_encode(payload: str) -> None:
    assert utf8_byte_len(payload) == len(payload.encode("utf-8"))


# ── The error contract: lone surrogates raise, never miscount ───────────


@given(st.text(alphabet=st.characters(categories=("Cs",)), min_size=1, max_size=64))
@settings(max_examples=64, deadline=None)
def test_byte_len_lone_surrogate_raises_the_encode_error(surrogates: str) -> None:
    # The stdlib encode is the contract: the terminal write's str is
    # orjson's own decode output and can never carry a lone surrogate,
    # but if one ever arrives the swap must fail IDENTICALLY - the same
    # exception class, not a wrong number that walks past the cap.
    with pytest.raises(UnicodeEncodeError):
        surrogates.encode("utf-8")
    with pytest.raises(UnicodeEncodeError):
        utf8_byte_len(surrogates)


# ── The perf gate: the swap must WIN on the real shape ──────────────────

pytestmark = pytest.mark.load_sensitive  # min-of-N timing ratios: the serial lane

_ROUNDS = 5  # min-of-N: the least contaminated round estimates the cost
_OPS_PER_ROUND = 20

#: The gated win factor. Measured at adoption: ~12x p50 single-thread on
#: the 64 KiB dict-result shape (docs/design/tors-adoption-map.md). The
#: gate asks for a fraction of it - runner noise (which min-of-N
#: suppresses) cannot flip the verdict, a hardware change does not
#: strand the pin.
_MIN_WIN_FACTOR = 3.0

_AT_CAP_64K = "中" * 21845 + "x"  # a real 65536-byte multibyte result


def _measure(fn, /) -> float:
    """min-of-N interleaved per-op cost, the house timing rules."""
    best = float("inf")
    for _ in range(_ROUNDS):
        start = time.perf_counter()
        for _ in range(_OPS_PER_ROUND):
            fn()
        best = min(best, (time.perf_counter() - start) / _OPS_PER_ROUND)
    return best


def test_byte_len_beats_the_encode_at_the_result_cap() -> None:
    """The swap's mandate: measuring beats copying at the cap shape."""
    encode_best = _measure(lambda: len(_AT_CAP_64K.encode("utf-8")))
    tors_best = _measure(lambda: utf8_byte_len(_AT_CAP_64K))
    assert encode_best > 0
    win = encode_best / tors_best
    assert win >= _MIN_WIN_FACTOR, (
        f"tors.utf8_byte_len won {win:.2f}x at the 64 KiB cap shape, "
        f"below the {_MIN_WIN_FACTOR}x gate - the swap does not ship "
        f"(encode {encode_best * 1e6:.0f}us vs tors {tors_best * 1e6:.0f}us)"
    )
