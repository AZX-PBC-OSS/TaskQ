"""The content-door lane's capture stamper: the run-scoped + head-stamped
writer for the lane's .txt receipts (the same shape the estate's own
stamped writer produces — ``# <stem> at head <sha> — rc=N — captured
<ts>`` — plus the registry claim under the flock).

Usage:
    python stamp_capture.py STEM RC -- body-command args...

The body-command's output is captured; the file lands in
``.measurements/runs/``; the claim rides ``CLAIMS.json``.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "src"))

from taskq.testing._claims import record_claim  # noqa: E402


def main() -> int:
    stem = sys.argv[1]
    rc = int(sys.argv[2])
    body = sys.argv[4:]  # after "--"
    proc = subprocess.run(body, cwd=REPO, capture_output=True, text=True, check=False)
    stamp = (
        f"# {stem} at head "
        f"{subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=REPO, capture_output=True, text=True, check=True).stdout.strip()}"
        f" — rc={proc.returncode} — captured {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n"
    )
    content = stamp + proc.stdout + proc.stderr
    name = f"{stem}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
    path = REPO / ".measurements" / "runs" / name
    path.write_text(content)
    record_claim(stem=stem, file=name, head_sha=stamp.split("head ")[1].split(" ")[0])
    print(f"{name}: rc={proc.returncode} ({len(content)} bytes)")
    if proc.returncode != 0 or rc != proc.returncode:
        print(content[-2000:])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
