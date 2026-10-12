"""The cures lane's estate re-stamp: one entry per text capture, the
head-stamp law's own writer (record_claim) — run from the repo root."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from taskq.testing._claims import record_claim  # noqa: E402

REPO = Path(__file__).resolve().parents[2]

#: (stem, file) — the cures lane's captures. head_sha + captured_at are
#: read from the capture's own header (the recorded head).
CAPTURES: list[tuple[str, str, str]] = [
    ("lint-format", "lint-format-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("pyright-full", "pyright-full-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("typeprobe-gate", "typeprobe-gate-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("mkdocs-strict", "mkdocs-strict-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("wf-battery-cov", "wf-battery-childcures-leg1.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("wf-battery-leg2", "wf-battery-childcures-leg2.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("wf-perf-bands", "wf-perf-bands-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("estate-slice", "estate-slice-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("attack-rounds", "attack-rounds-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("wf-battery-rv4", "wf-battery-childcures-leg1.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("wf-bands-rv4", "wf-perf-bands-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
    ("fast-tier-n8", "fast-tier-n8-childcures.txt", "822772b410c68dd804cbef90ce82b1aa83538473"),
]


def main() -> None:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.strip()
    for stem, file, claimed in CAPTURES:
        path = REPO / ".measurements" / "runs" / file
        if not path.is_file():
            print(f"MISSING {file}")
            continue
        record_claim(stem=stem, file=file, head_sha=claimed, captured_at=None)
        print(f"recorded {stem} <- {file} (claimed head {claimed[:9]}, tree head {head[:9]})")


if __name__ == "__main__":
    main()
