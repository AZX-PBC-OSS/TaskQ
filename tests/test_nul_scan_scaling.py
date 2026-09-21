"""The NUL-scan's growth is linear, pinned so a regression is reviewed.

`_encoded_has_nul` runs on every terminal write's jsonb bind. Measured
at adoption: 175.8 -> 1438.4 us for 1000 -> 8000 escaped-literal matches
(8.18x per 8x input: linear), bounded by MAX_RESULT_BYTES (65536) at
roughly 1.7 ms worst case on the event loop. If a future edit makes the
scan superlinear, this pin fails in review instead of the fleet
discovering it as an event-loop stall.

The measurement is noise-robust by construction (the dispatch-benchmark
pattern): per input size, min-of-N timed rounds -- load only ever ADDS
time, so the least-contaminated round is the closest estimate of the
scan's own cost, and a systematic shape regression raises even the best
round. An earlier draft timed a payload of LIVE escapes, which returns
at the first match (~0.2 us regardless of size): a constant-work sample
whose ratio was pure runner noise -- it flaked at 1.61x/doubling on a
loaded CI runner and could not have caught a quadratic scan. The payload
below is the scan's actual worst shape: every match is escaped-literal,
so the scan must walk the whole payload and confirm every match.

Runs in the serial load-sensitive lane: it gates a growth RATIO between
two wall-clock samples, which is load-fragile by nature even with
min-of-N (a co-tenant that slows only one size's rounds shifts the
ratio); the serial lane is the honest home for a timing-shape test.
"""

import subprocess
import sys

import pytest

_ROUNDS = 5  # min-of-N: the least contaminated round estimates the scan's cost
_SCANS_PER_ROUND = 20

_PROBE = """
import sys, time
from taskq._json import _encoded_has_nul

n = int(sys.argv[1])
rounds = int(sys.argv[2])
scans = int(sys.argv[3])
# worst shape for the scan: every NUL escape is escaped-literal (an odd
# backslash run before each match), so no match returns early -- the
# scan walks all n*7 bytes and confirms all n matches.
backslash = chr(92).encode()
unit = backslash + backslash + b"u0000"  # an escaped-literal NUL escape
payload = b'"' + unit * n + b'"'
assert not _encoded_has_nul(payload), "probe payload must not early-return"
times = []
for _ in range(rounds):
    t0 = time.perf_counter()
    for _ in range(scans):
        _encoded_has_nul(payload)
    times.append(time.perf_counter() - t0)
print(f"{min(times) / scans:.9f}")
"""


def _time_scan(n: int) -> float:
    out = subprocess.run(  # noqa: S603  # Why: fixed argv (sys.executable, the probe, counts); nothing user-controlled reaches the command.
        [sys.executable, "-c", _PROBE, str(n), str(_ROUNDS), str(_SCANS_PER_ROUND)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert out.returncode == 0, out.stderr
    return float(out.stdout.strip())


@pytest.mark.load_sensitive
def test_nul_scan_growth_is_linear_per_doubling() -> None:
    small = _time_scan(1000)
    big = _time_scan(8000)
    ratio = big / small
    # linear: 8x the input, ~8x the time -- measured 8.0-8.4x, stable under
    # min-of-N. Gate at 12x: ~45% headroom above linear, while every
    # quadratic shape violates it -- 64x for a true O(n^2) scan, and a
    # memcpy-cheap quadratic sim (an O(pos) copy per match) already
    # measures 15.6x. Gate the raw 8x ratio, not its per-doubling root:
    # the cubic root compresses a 15.6x quadratic to 2.5x/doubling, under
    # any sane per-doubling gate.
    assert ratio < 12.0, (
        f"the NUL scan grew {ratio:.2f}x for 8x the input "
        f"({ratio ** (1 / 3):.2f}x per doubling): the scan went superlinear; "
        "the terminal write's event-loop budget is MAX_RESULT_BYTES-bounded "
        "only if this scan stays linear"
    )
