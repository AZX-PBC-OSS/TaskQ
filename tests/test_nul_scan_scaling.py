"""The NUL-scan's growth is linear, pinned so a regression is reviewed.

`_encoded_has_nul` runs on every terminal write's jsonb bind. It is
bounded by MAX_RESULT_BYTES (65536) at roughly 1.7 ms worst case on the
event loop -- but only if the scan stays linear. If a future edit makes
it superlinear, this pin fails in review instead of the fleet
discovering it as an event-loop stall.

The pin gates the scan's EXCESS growth over a calibration twin: the
scan's time ratio across an 8x input step, minus the same-step ratio of
the scan's own find-advance loop with the per-match backslash
confirmation removed. Both walk the same payload bytes through the same
interpreter loop shape, so any machine bias (cache residency, CPU
steal, GC) lands on both ratios and cancels in the difference; only
algorithmic superlinearity leaves an absolute excess.

Why the difference and not the raw ratio or a division: the raw gate
(12x for 8x input) failed on CI at 16.50x while the same tree measured
8.0-8.4x locally. min-of-N cannot remove a SYSTEMATIC bias, and cache
residency is systematic: the small payload fits cache, the large one
does not, so the large size's rounds are honestly slower and every
fixed raw gate is one runner geometry away from a flake. The obvious
repair -- divide by a control's ratio -- was measured here and
rejected: a pure byte-streaming builtin control (`data.find(b'\\x00')`)
compresses from ~9x to as low as 1.3x under aggressive between-round
eviction (its per-call time is a fixed overhead plus a streaming term,
so cache pressure crushes exactly the term the ratio needs), which
inflates the relative metric for a perfectly linear scan. A division
also compresses the cheapest superlinear mutant -- a memcpy-cheap
quadratic sim, an O(pos) copy per match added to the real scan --
to 1.92x, below any gate that safely clears the linear band. The twin
(a control that shares the scan's cost structure) keeps both ratios at
~8-10x in BOTH environments, and the difference framing leaves the
memcpy mutant nowhere to hide: the linear band is -1.2 to -0.3 ratio
units, the mutant lands at +7 to +8, and the gate sits at 2.5.

Noise-robust by construction (the dispatch-benchmark pattern): per
input size, min-of-N timed rounds -- load only ever ADDS time, so the
least-contaminated round is the closest estimate of true cost, and a
systematic shape regression raises even the best round. An earlier
draft timed a payload of LIVE escapes, which returns at the first
match (~0.2 us regardless of size): a constant-work sample whose ratio
was pure runner noise. The payload below is the scan's actual worst
shape: every match is escaped-literal, so the scan must walk the whole
payload and confirm every match.

Runs in the serial load-sensitive lane: it gates a timing ratio, which
is load-fragile by nature even with min-of-N and the twin's
cancellation; the serial lane is the honest home for a timing-shape
test.

Known boundary -- what this gate CAN and CANNOT see. The gate reliably
detects INTERPRETER-level superlinear work, which is the realistic
regression class for a pure-Python scan: a memcpy-cheap quadratic sim
(an O(pos) copy per match added to the scan's loop) measured ~+84
excess on the runner that sourced the pin's mutant numbers (+16 to +17
on a second runner; the committed-test mutant lands at +7 to +8) --
all far over the 2.5 gate. But a C-LEVEL O(pos) per-match term (one
`bytes.rfind` over the payload per match, no copy) measures excess
-0.24 to -0.45 (-0.47 to +0.35 on the second runner) -- UNDER the 2.5
gate, invisible: a C scan is roughly two orders of magnitude cheaper
per byte than the interpreter loop, so the added O(pos) work never
lifts the scan's ratio above its twin's. An earlier draft of this
docstring claimed a C-level O(pos) rescan per match lands at +3.5 and
is caught; that measurement was wrong, and the correct numbers are the
ones in this paragraph.

Operational rule that follows: a future edit delegating per-match work
to C-level O(pos) primitives (rfind/index/count with a start bound, a
regex over the payload, any per-match C scan of the remaining bytes)
is INVISIBLE to this gate and MUST be caught in review. Review check,
named explicitly: any change to `_encoded_has_nul` (or to the
terminal-write path that calls it) that adds, replaces, or wraps a
per-match step with a C-level primitive whose cost grows with match
position or payload length cannot be trusted to this test; require the
author to state the term's complexity in the PR description and to add
a mutant to this file simulating the new shape, with its measured
excess, before merging.

LOAD stability, with provenance: across 10 loaded runs (8 busy cores
on the sourcing runner) the excess measured -0.96 to +0.60, against
the 2.5 gate (-0.93 to -0.24 across 10 loaded runs on the second
runner). The margin is not an artifact of one quiet machine; it holds
under contention, and both bands are why the gate value is 2.5 and not
something tighter.
"""

import subprocess
import sys

import pytest

_ROUNDS = 5  # min-of-N: the least contaminated round estimates true cost
_SCANS_PER_ROUND = 20
# linear scan-minus-twin difference measures -1.2 to -0.3 ratio units,
# with and without aggressive between-round cache eviction
_DIFFERENCE_GATE = 2.5

_PROBE = """
import sys, time
from taskq._json import _encoded_has_nul, _NUL_ESCAPE_BYTES

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


def twin(data):
    # the calibration twin: the scan's find-advance skeleton with the
    # per-match confirmation removed. Walks every byte and every match
    # through the same interpreter loop, so machine bias hits it the
    # same way it hits the scan.
    pos = data.find(_NUL_ESCAPE_BYTES)
    while pos != -1:
        pos = data.find(_NUL_ESCAPE_BYTES, pos + 1)
    return pos


scan_times = []
twin_times = []
for _ in range(rounds):
    t0 = time.perf_counter()
    for _ in range(scans):
        _encoded_has_nul(payload)
    scan_times.append(time.perf_counter() - t0)
    t0 = time.perf_counter()
    for _ in range(scans):
        twin(payload)
    twin_times.append(time.perf_counter() - t0)
print(f"{min(scan_times) / scans:.9f} {min(twin_times) / scans:.9f}")
"""


def _time_cell(n: int) -> tuple[float, float]:
    """Scan and twin per-call seconds at *n* escape units, min-of-N."""
    out = subprocess.run(  # noqa: S603  # Why: fixed argv (sys.executable, the probe, counts); nothing user-controlled reaches the command.
        [sys.executable, "-c", _PROBE, str(n), str(_ROUNDS), str(_SCANS_PER_ROUND)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert out.returncode == 0, out.stderr
    scan_s, twin_s = out.stdout.split()
    return float(scan_s), float(twin_s)


@pytest.mark.load_sensitive
def test_nul_scan_growth_is_linear_relative_to_its_twin() -> None:
    small_scan, small_twin = _time_cell(1000)
    big_scan, big_twin = _time_cell(8000)
    scan_ratio = big_scan / small_scan
    twin_ratio = big_twin / small_twin
    excess = scan_ratio - twin_ratio
    # linear: both ratios ~8-10x for 8x the input, excess -1.2 to -0.3
    # ratio units, with and without aggressive cache eviction between
    # rounds. Gate the excess at 2.5 units: the twin cancels machine
    # bias (cache residency, CPU steal, GC) because it is the scan's own
    # loop shape over the same bytes, so a bias moves both ratios
    # together and the difference stays in band -- the raw 16.50x CI
    # flake of the previous gate divides out. Superlinear shapes leave
    # an absolute excess: a memcpy-cheap quadratic sim (an O(pos) copy
    # per match) lands at +7 to +8 units and fails the gate. A C-level
    # O(pos) rfind per match lands IN the linear band (-0.47 to +0.35)
    # and does NOT fail it -- the gate's sensitivity boundary is
    # python-level work; see the docstring's known boundary and its
    # review rule for that case.
    assert excess < _DIFFERENCE_GATE, (
        f"the NUL scan grew {scan_ratio:.2f}x for 8x the input while its "
        f"find-advance twin grew {twin_ratio:.2f}x (excess {excess:+.2f} ratio "
        f"units, gate {_DIFFERENCE_GATE}): the scan's cost grew superlinearly "
        "against a control that touches the same bytes through the same "
        "interpreter loop, so this is the algorithm, not the machine -- the "
        "terminal write's event-loop budget is MAX_RESULT_BYTES-bounded only "
        "if this scan stays linear"
    )
