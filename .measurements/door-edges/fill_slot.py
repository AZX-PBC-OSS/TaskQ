"""The door-edges lane's slot FILL: the green run's output replaces the
staged slot's content IN PLACE — the same file, the same registry entry
(the staged claim's freshness carried by the off-by-one rule; no new
claim is appended — the estate-cures drain's own fill shape).

Usage:  python fill_slot.py <SLOT_FILE> <RUN_RC> <RAW_OUTPUT_FILE>
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent


def main() -> int:
    slot = Path(sys.argv[1])
    run_rc = int(sys.argv[2])
    raw = Path(sys.argv[3]).read_text(errors="replace")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.strip()
    stem = slot.name.rsplit("-slot-", 1)[0]
    stamp = (
        f"# {stem} at head {head} — rc={run_rc} — "
        f"captured {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
        f"(the door-edges lane's green fill of the staged slot)\n"
    )
    slot.write_text(stamp + raw)
    print(f"filled: {slot.name} (run rc={run_rc}, {len(stamp) + len(raw)} bytes)")
    return 0 if run_rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
