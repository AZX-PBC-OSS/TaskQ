"""THE WORKFLOWS-SCOPED COVERAGE GATE (the scoped branch floor as a gate leg).

THE LAW: the workflows package's BRANCH coverage — scoped to
``src/taskq/workflows``, branch mode — holds the FLOOR (90). The floor is
HONESTLY measured: the number is what the wf pin suite + the attack
rounds + the validator's pins actually produce on this tree (the
recorded capture lives in ``.measurements/runs/wf-scoped-coverage-*.json``
— run-scoped, the append-only conversion's shape), never an asserted
aspiration.

Usage (after the wf suite ran with ``--cov=src/taskq/workflows
--cov-branch``):

    python scripts/check_wf_coverage.py [--data .coverage] [--floor 90]

Exits 1 when the scoped branch coverage is under the floor — the gate
reds, the record says by how much.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from pathlib import Path

from coverage import Coverage

#: The floor (the honest measured number is recorded beside the verdict).
DEFAULT_FLOOR = 90.0

_SCOPE = "src/taskq/workflows"

REPO = Path(__file__).resolve().parent.parent
MEASUREMENTS = REPO / ".measurements"


def scoped_branch_percent(data_file: Path) -> tuple[float, str]:
    """The scoped branch percentage via coverage's own report arithmetic
    (include-filtered to the workflows package — the same number the wf
    suite's ``--cov-report=term`` prints as TOTAL), plus the report
    table for the record."""
    cov = Coverage(
        data_file=str(data_file),
        branch=True,
        include=[f"{_SCOPE}/*"],
    )
    cov.load()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        pct = cov.report(file=buf)
    if pct is None:
        raise SystemExit(f"no coverage data in {data_file} — run the wf suite under --cov first")
    return pct, buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=REPO / ".coverage")
    parser.add_argument("--floor", type=float, default=DEFAULT_FLOOR)
    args = parser.parse_args()

    pct, table = scoped_branch_percent(args.data)
    verdict = "green" if pct >= args.floor else "RED"
    record = {
        "gate": "wf-scoped-branch-coverage",
        "scope": _SCOPE,
        "floor": args.floor,
        "measured_pct": round(pct, 2),
        "data_file": str(args.data),
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "verdict": verdict,
        "table": table.strip(),
    }
    runs = MEASUREMENTS / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    out = runs / f"wf-scoped-coverage-{time.strftime('%Y%m%dT%H%M%S')}.json"
    out.write_text(json.dumps(record, indent=2))
    print(table)
    print(f"[gate] wf-scoped branch coverage {pct:.2f}% vs floor {args.floor}% — {verdict}")
    print(f"[gate] recorded: {out}")
    return 0 if pct >= args.floor else 1


if __name__ == "__main__":
    sys.exit(main())
