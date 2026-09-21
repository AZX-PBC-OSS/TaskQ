"""The tors dispatch must WIN measurably on the scan's worst shape, or it does not ship.

The ``[text-accel]`` extra exists for one reason: the escape-parity NUL
scan that guards MAX_RESULT_BYTES runs on every terminal write, on the
event loop, and its worst shape (``tests/test_nul_scan_scaling.py``'s
escaped-literal corpus -- every match rejected, full payload walk) cost
~1.5 ms per write at the 65536-byte bound in pure Python. The dispatch to
``tors.contains_unescaped`` is justified only by a measured win on that
shape; a speedup that exists only on friendly shapes is not a reason to
carry an extra.

This module times the DISPATCH (whatever the probe serves -- tors under
the extra) against the PURE fallback in one process, interleaved, min-of-N
(house rules: load only ever ADDS time, so the least-contaminated round is
the closest estimate), and requires the dispatch to win by at least the
gated factor. The recorded evidence lives in
``perf-evidence-tors-acceleration.md``.

The whole module is behind ``importorskip``: absence of tors is a
supported configuration (the pure path is the pin, and
``test_nul_scan_scaling.py`` already gates its growth there), so these
gates SKIP cleanly without the package instead of failing -- the
skip-is-failure discipline does not apply to an optional accelerator.
"""

from __future__ import annotations

import time

import pytest

pytest.importorskip(
    "tors",
    reason="the [text-accel] extra is not installed; the pure path is the pin",
)

from taskq._json import _encoded_has_nul, _pure_encoded_has_nul

pytestmark = [
    pytest.mark.load_sensitive,  # min-of-N timing ratios: the serial lane
    pytest.mark.text_accel,
]

_ROUNDS = 5  # min-of-N: the least contaminated round estimates the scan's cost
_SCANS_PER_ROUND = 20

#: The gated win factor. Measured at adoption: 30-36x on every full-walk
#: shape (see perf-evidence-tors-acceleration.md). The gate asks for a
#: small fraction of that -- enough that runner noise (which min-of-N
#: already suppresses) cannot flip the verdict, loose enough that a
#: hardware change does not strand the pin.
_MIN_WIN_FACTOR = 5.0


def _escape_literals(n: int) -> bytes:
    """The worst shape: an odd backslash run before every match, no early return."""
    return b'"' + b"\\\\u0000" * n + b'"'


def _time_both(payload: bytes) -> tuple[float, float]:
    """Interleaved min-of-N per-call seconds for (dispatch, pure)."""
    dispatch_best = pure_best = float("inf")
    for _ in range(_ROUNDS):
        t0 = time.perf_counter()
        for _ in range(_SCANS_PER_ROUND):
            _encoded_has_nul(payload)
        dispatch_best = min(dispatch_best, (time.perf_counter() - t0) / _SCANS_PER_ROUND)
        t0 = time.perf_counter()
        for _ in range(_SCANS_PER_ROUND):
            _pure_encoded_has_nul(payload)
        pure_best = min(pure_best, (time.perf_counter() - t0) / _SCANS_PER_ROUND)
    return dispatch_best, pure_best


@pytest.mark.parametrize("n_units", [1000, 9362])
def test_tors_dispatch_wins_on_the_escaped_literal_worst_shape(n_units: int) -> None:
    """The worst-shape corpus at the scaling pin's size and at the 64 KiB bound."""
    payload = _escape_literals(n_units)
    # Parity belt: a fast answer that is not THE answer is a regression.
    assert _encoded_has_nul(payload) is _pure_encoded_has_nul(payload) is False
    dispatch, pure = _time_both(payload)
    factor = pure / dispatch
    assert factor >= _MIN_WIN_FACTOR, (
        f"the tors dispatch won only {factor:.2f}x on the escaped-literal "
        f"worst shape ({n_units} units, {len(payload)} bytes): "
        f"dispatch {dispatch * 1e6:.1f}us vs pure {pure * 1e6:.1f}us. "
        "The [text-accel] extra is justified by a measured win on THIS "
        "shape; if tors no longer wins, retire the dispatch (the pure "
        "path is the supported default) instead of carrying a costless-"
        "looking dependency."
    )


def test_tors_dispatch_wins_on_a_realistic_result_payload() -> None:
    """The 40k-row result shape (the bulk-cancel evidence corpus), NUL-free.

    The success path's scan is a memchr miss on realistic results; the
    dispatch must win there too, not only on the adversarial corpus.
    """
    from taskq._json import dumps

    payload = dumps(
        {"rows": [{"id": i, "status": "done", "note": f"row-{i}"} for i in range(40_000)]}
    )
    assert _encoded_has_nul(payload) is _pure_encoded_has_nul(payload) is False
    dispatch, pure = _time_both(payload)
    factor = pure / dispatch
    assert factor >= _MIN_WIN_FACTOR, (
        f"the tors dispatch won only {factor:.2f}x on the 40k-row result "
        f"shape ({len(payload)} bytes): dispatch {dispatch * 1e6:.1f}us vs "
        f"pure {pure * 1e6:.1f}us"
    )
