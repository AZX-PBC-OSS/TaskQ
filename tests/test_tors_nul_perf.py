"""The tors scan must WIN measurably on the worst shape, or it does not ship.

tors is a core dependency for one reason: the escape-parity NUL scan that
guards MAX_RESULT_BYTES runs on every terminal write, on the event loop,
and its worst shape (``tests/test_nul_scan_scaling.py``'s escaped-literal
corpus -- every match rejected, full payload walk) cost ~1.5 ms per write
at the 65536-byte bound in pure Python. Carrying tors is justified only by
a measured win on that shape; a speedup that exists only on friendly
shapes is not a reason to carry the dependency.

This module times the tors scan (what ``_encoded_has_nul`` serves) against
the pure-Python baseline spelled below -- the implementation that preceded
tors adoption, kept here verbatim as the timing baseline because the scan
itself no longer carries a second path -- in one process, interleaved,
min-of-N (house rules: load only ever ADDS time, so the least-contaminated
round is the closest estimate), and requires tors to win by at least the
gated factor. The recorded evidence lives in
``perf-evidence-tors-acceleration.md``.
"""

from __future__ import annotations

import time

import pytest

from taskq._json import _NUL_ESCAPE_BYTES, _encoded_has_nul

pytestmark = pytest.mark.load_sensitive  # min-of-N timing ratios: the serial lane

_ROUNDS = 5  # min-of-N: the least contaminated round estimates the scan's cost
_SCANS_PER_ROUND = 20

#: The gated win factor. Measured at adoption: 30-36x on every full-walk
#: shape (see perf-evidence-tors-acceleration.md). The gate asks for a
#: small fraction of that -- enough that runner noise (which min-of-N
#: already suppresses) cannot flip the verdict, loose enough that a
#: hardware change does not strand the pin.
_MIN_WIN_FACTOR = 5.0


def _pure_encoded_has_nul(data: bytes, /) -> bool:
    """The pure-Python escape-parity scan; the timing baseline.

    orjson renders the *literal text* ``\\u0000`` as an escaped backslash
    followed by the same six bytes, so a raw byte match is ambiguous. Each
    match is confirmed by counting the backslashes immediately before it:
    an even run means the escape is live (a real NUL); an odd run means the
    match's leading backslash closes a ``\\\\`` pair and the sequence is the
    literal six characters, which ``jsonb`` accepts.
    """
    pos = data.find(_NUL_ESCAPE_BYTES)
    while pos != -1:
        backslashes = 0
        cursor = pos - 1
        while cursor >= 0 and data[cursor : cursor + 1] == b"\\":
            backslashes += 1
            cursor -= 1
        if backslashes % 2 == 0:
            return True
        pos = data.find(_NUL_ESCAPE_BYTES, pos + 1)
    return False


def _escape_literals(n: int) -> bytes:
    """The worst shape: an odd backslash run before every match, no early return."""
    return b'"' + b"\\\\u0000" * n + b'"'


def _time_both(payload: bytes) -> tuple[float, float]:
    """Interleaved min-of-N per-call seconds for (tors, pure baseline)."""
    tors_best = pure_best = float("inf")
    for _ in range(_ROUNDS):
        t0 = time.perf_counter()
        for _ in range(_SCANS_PER_ROUND):
            _encoded_has_nul(payload)
        tors_best = min(tors_best, (time.perf_counter() - t0) / _SCANS_PER_ROUND)
        t0 = time.perf_counter()
        for _ in range(_SCANS_PER_ROUND):
            _pure_encoded_has_nul(payload)
        pure_best = min(pure_best, (time.perf_counter() - t0) / _SCANS_PER_ROUND)
    return tors_best, pure_best


@pytest.mark.parametrize("n_units", [1000, 9362])
def test_tors_wins_on_the_escaped_literal_worst_shape(n_units: int) -> None:
    """The worst-shape corpus at the scaling pin's size and at the 64 KiB bound."""
    payload = _escape_literals(n_units)
    # Parity belt: a fast answer that is not THE answer is a regression.
    assert _encoded_has_nul(payload) is _pure_encoded_has_nul(payload) is False
    tors, pure = _time_both(payload)
    factor = pure / tors
    assert factor >= _MIN_WIN_FACTOR, (
        f"the tors scan won only {factor:.2f}x on the escaped-literal "
        f"worst shape ({n_units} units, {len(payload)} bytes): "
        f"tors {tors * 1e6:.1f}us vs pure {pure * 1e6:.1f}us. "
        "tors is carried for a measured win on THIS shape; if it no "
        "longer wins, retire the dependency instead of carrying a "
        "costless-looking one."
    )


def test_tors_wins_on_a_realistic_result_payload() -> None:
    """The 40k-row result shape (the bulk-cancel evidence corpus), NUL-free.

    The success path's scan is a memchr miss on realistic results; tors
    must win there too, not only on the adversarial corpus.
    """
    from taskq._json import dumps

    payload = dumps(
        {"rows": [{"id": i, "status": "done", "note": f"row-{i}"} for i in range(40_000)]}
    )
    assert _encoded_has_nul(payload) is _pure_encoded_has_nul(payload) is False
    tors, pure = _time_both(payload)
    factor = pure / tors
    assert factor >= _MIN_WIN_FACTOR, (
        f"the tors scan won only {factor:.2f}x on the 40k-row result "
        f"shape ({len(payload)} bytes): tors {tors * 1e6:.1f}us vs pure "
        f"{pure * 1e6:.1f}us"
    )
