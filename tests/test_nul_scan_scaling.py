"""The NUL-scan's growth is linear, pinned so a regression is reviewed.

`_encoded_has_nul` runs on every terminal write's jsonb bind. Measured
at adoption: 175.8 -> 1438.4 us for 1000 -> 8000 matches (8.18x per 8x
input: linear), bounded by MAX_RESULT_BYTES (65536) at roughly 1.7 ms
worst case on the event loop. If a future edit makes the scan
superlinear, this pin fails in review instead of the fleet discovering
it as an event-loop stall.
"""

import subprocess
import sys

_PROBE = """
import json, sys, time
from taskq.constants import MAX_RESULT_BYTES

n = int(sys.argv[1])
# a payload dense with literal \\u0000 escapes: the scan's worst shape
payload = json.dumps({"data": "\\u0000" * n})
t0 = time.perf_counter()
for _ in range(20):
    from taskq._json import _encoded_has_nul

    _encoded_has_nul(payload.encode())
elapsed = time.perf_counter() - t0
print(f"{elapsed / 20:.9f}")
"""


def _time_scan(n: int) -> float:
    out = subprocess.run(  # noqa: S603  # Why: fixed argv (sys.executable, the probe, a count); nothing user-controlled reaches the command.
        [sys.executable, "-c", _PROBE, str(n)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert out.returncode == 0, out.stderr
    return float(out.stdout.strip())


def test_nul_scan_growth_is_linear_per_doubling() -> None:
    small = _time_scan(1000)
    big = _time_scan(8000)
    ratio = big / small
    # linear: 8x the input, ~8x the time. Superlinear shapes (quadratic:
    # 64x) fail this with margin; noise stays under 2.5x per doubling.
    per_doubling = ratio ** (1 / 3)
    assert per_doubling < 1.6, (
        f"the NUL scan grew {ratio:.2f}x for 8x the input "
        f"({per_doubling:.2f}x per doubling): the scan went superlinear; "
        "the terminal write's event-loop budget is MAX_RESULT_BYTES-bounded "
        "only if this scan stays linear"
    )
