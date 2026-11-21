"""A deliberately broken worker entry: a deterministic bootstrap crash.

The crash-record controls' probe child (``tests/test_harness_crash_record.py``):
a banner of numbered lines on stderr, then a nonzero exit — no taskq
import, no DSN, no broker, so the probe is hermetic and its output is
exact. The banner's bytes are the completeness claim under test: the
readiness gate's crash report and the tee's durable log must carry every
line, in order, with none stolen by a second reader.
"""

import sys

CRASH_RC = 7

BANNER_LINES = [
    f"CRASH-PROBE-LINE-{i:02d}: the bootstrap died before readiness" for i in range(1, 41)
]
BANNER = "\n".join(BANNER_LINES) + "\n"

if __name__ == "__main__":
    for _line in BANNER_LINES:
        print(_line, file=sys.stderr, flush=True)
    sys.exit(CRASH_RC)
