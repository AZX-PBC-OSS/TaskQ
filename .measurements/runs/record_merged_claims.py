"""THE GRAND MERGE's estate re-stamp: one claim per text capture, the
head-stamp law's own writer (record_claim) — run from the repo root.

The captures were taken at the source-final head c5ed9fce9 (the battery
legs' single red is rv2-13's TRUTHFUL pre-estate red — the committed
claims were stale at the merged head; THIS commit is the cure: the
captures + this registry append ride it, and rv2-13's writer greens at
the committed tree per the off-by-one rule — its green receipts ride the
NEXT measurement commit, the same ladder every lane climbed).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from taskq.testing._claims import record_claim  # noqa: E402

REPO = Path(__file__).resolve().parents[2]

#: (stem, file) — the merged estate's captures, measured at
#: 2d75eea067e4a2ea8dc37ff0e40b78b782a434a5 (the source-final head).
#: THE CONTENT DOOR'S LADDER: the battery + fast-tier stems are NOT here —
#: their runs at THIS head carry rv2-13's TRUTHFUL pre-estate red (the
#: committed claims were stale at the merged head; THIS commit is the
#: cure), and a claim whose content records failure is the content door's
#: own conviction. They are recorded at the NEXT measurement commit, from
#: the runs at THIS committed tree (rv2-13 green there — the off-by-one
#: rule carries the freshness).
CAPTURES: list[tuple[str, str, str]] = [
    ("lint-format", "lint-format-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("pyright-full", "pyright-full-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("typeprobe-gate", "typeprobe-gate-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("mkdocs-strict", "mkdocs-strict-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("docs-examples", "docs-examples-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("deslop-pin-packs", "pinpacks-merged-leg1.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("estate-slice", "estate-slice-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("wf-perf-bands", "wf-perf-bands-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
    ("wf-bands-rv4", "wf-bands-rv4-merged.txt", "2d75eea067e4a2ea8dc37ff0e40b78b782a434a5"),
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
