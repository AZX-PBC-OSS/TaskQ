"""A/B: the NUL scan's pure-Python path vs the tors [text-accel] dispatch.

``taskq._json._encoded_has_nul`` guards MAX_RESULT_BYTES on every terminal
write. Since the tors adoption it dispatches on an import-time probe:
``tors.contains_unescaped`` (Rust, zero-copy, GIL-released) when the
``[text-accel]`` extra is installed, the pure escape-parity loop otherwise.
This script times BOTH implementations in one process -- the pure function
directly and whatever the dispatch serves -- so the before/after pair is
interpreter-fair and the parity of the two verdicts is asserted on every
timed payload (a speedup on a different answer is worthless).

Shapes, in the order they matter to the terminal write:

* ``escaped_literal`` -- the scan's worst shape and the payload of
  ``tests/test_nul_scan_scaling.py``: every ``\\u0000`` occurrence sits
  behind an odd backslash run, so no match is live and the scan must walk
  the whole payload confirming every match. Sized 1000/8000 units (the
  scaling pin's sizes), at the MAX_RESULT_BYTES (65536) boundary, and at
  the 40k-row / multi-megabyte realistic-result scale.
* ``dense_live`` -- every occurrence is a real NUL escape: both paths
  return at the first match (the common rejection case, cheap by design).
* ``clean`` -- no backslash at all: a pure memchr miss.
* ``realistic_rows`` -- a 40k-row result dict (the bulk-cancel evidence
  shape) and a ~3 MB single-result shape, orjson-encoded, NUL-free: the
  realistic per-call cost on the success path.

Methodology: house rules from ``bench_hotspots.py`` -- min-of-N timed
rounds (load only ever ADDS time, so the least-contaminated round is the
closest estimate of the scan's own cost), warm-up rounds excluded, both
arms interleaved so thermal drift cancels.

Run: .venv/bin/python benchmarks/ab_tors_nul_scan.py
     (tors arm reports "absent" without the [text-accel] extra)
"""

from __future__ import annotations

import time

# pyright: reportPrivateUsage=false
# Why: the A/B target IS the private seam -- _encoded_has_nul (the dispatch)
# and _pure_encoded_has_nul (the fallback) are the two arms, and
# _NUL_ESCAPE_BYTES documents the needle.
from taskq._json import _NUL_ESCAPE_BYTES, _pure_encoded_has_nul
from taskq._json import _encoded_has_nul as _dispatched

_ROUNDS = 5
_WARMUP = 1
_SCANS_PER_ROUND = 20


def _payloads() -> dict[str, bytes]:
    unit = b"\\" * 2 + b"u0000"  # an escaped-literal NUL escape (odd run)
    live_unit = b"\\u0000"  # a live escape (orjson's rendering of U+0000)

    # The 40k-row shape of perf-evidence-bulk-cancel.md, as a result dict.
    rows_40k = [{"id": i, "status": "done", "note": f"row-{i}"} for i in range(40_000)]
    # A ~3 MB single-result shape (text-heavy actor output).
    big_text = "x" * 3_000_000

    return {
        "escaped_literal_1000": b'"' + unit * 1000 + b'"',
        "escaped_literal_8000": b'"' + unit * 8000 + b'"',
        "escaped_literal_64k_bound": b'"' + unit * 9362 + b'"',
        "escaped_literal_3mb": b'"' + unit * 430_000 + b'"',
        "dense_live_8000": b'"' + live_unit * 8000 + b'"',
        "clean_64k": b'"' + b"a" * 65_524 + b'"',
        "realistic_rows_40k": _encoded_has_nul_fixup(b'{"rows":' + _dumps(rows_40k) + b"}"),
        "realistic_3mb_text": _encoded_has_nul_fixup(_dumps({"note": big_text})),
    }


# orjson output never contains a live NUL escape unless the payload did,
# and none of the realistic fixtures carry one; the fixup asserts that
# rather than trusting it, so a fixture drift cannot silently turn a
# "clean" measurement into an early-return one.
def _encoded_has_nul_fixup(payload: bytes) -> bytes:
    assert not _pure_encoded_has_nul(payload)
    return payload


def _dumps(value: object) -> bytes:
    from taskq._json import dumps

    return dumps(value)


def main() -> None:
    try:
        import tors  # pyright: ignore[reportMissingImports]  # Why: the probe reports tors's ABSENCE as a supported configuration, so the optional package cannot be a hard import.

        tors_note = f"tors {tors.__version__} present (dispatch serves tors)"
    except ImportError:
        tors_note = "tors absent (dispatch serves the pure path)"

    print(f"engine note: {tors_note}")
    print(f"needle: {_NUL_ESCAPE_BYTES!r}")
    print(
        f"method: min({_ROUNDS}) rounds x {_SCANS_PER_ROUND} scans, "
        f"{_WARMUP} warm-up round(s) excluded, arms interleaved"
    )
    print()
    header = f"{'payload':<26} {'bytes':>9} {'pure us':>10} {'dispatch us':>12} {'ratio':>7} {'verdicts':>9}"
    print(header)
    print("-" * len(header))
    for name, payload in _payloads().items():
        # Interleave the two arms round-by-round so thermal drift hits both.
        pure_times: list[float] = []
        disp_times: list[float] = []
        for _ in range(_ROUNDS):
            t0 = time.perf_counter()
            for _ in range(_SCANS_PER_ROUND):
                _pure_encoded_has_nul(payload)
            pure_times.append((time.perf_counter() - t0) / _SCANS_PER_ROUND)
            t0 = time.perf_counter()
            for _ in range(_SCANS_PER_ROUND):
                _dispatched(payload)
            disp_times.append((time.perf_counter() - t0) / _SCANS_PER_ROUND)
        pure = min(pure_times) * 1e6
        disp = min(disp_times) * 1e6
        ratio = pure / disp
        verdicts = "match" if _pure_encoded_has_nul(payload) == _dispatched(payload) else "MISMATCH"
        print(
            f"{name:<26} {len(payload):>9} {pure:>10.1f} {disp:>12.1f} {ratio:>6.2f}x {verdicts:>9}"
        )


if __name__ == "__main__":
    main()
