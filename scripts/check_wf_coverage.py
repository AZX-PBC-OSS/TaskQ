"""THE WORKFLOWS-SCOPED COVERAGE GATE (the scoped branch floor as a gate leg) —
RUNS AT THE HEAD.

THE LAW (the evidence-integrity cure 1b): a coverage number is evidence
only OF THE TREE IT WAS MEASURED ON. The 90.03% capture of record
predated the final commit (which edited ``definitions.py``); the honest
re-run on that head read 89.84% — RED twice — while the stale artifact
stayed green. The gate therefore:

1. **RECORDS THE HEAD** — every artifact carries ``head_sha`` (git
   rev-parse HEAD) + ``tree_dirty`` (the porcelain count). The artifact
   IS the receipt (the head-stamp law, ``scripts/verify_evidence_heads``).
2. **REFUSES A STALE TREE** — the coverage data file's mtime PREDATING
   the HEAD commit's time means the number was measured on an ANCESTOR:
   the gate exits 2 and reports the mismatch, it never reports the
   stale number.
3. **REFUSES A DIRTY TREE** — a number from a dirty tree is no one's
   number: the working tree's edits are in the measurement but not in
   the head the artifact will cite. ``--allow-dirty`` exists for the dev
   loop only, and stains the verdict it produces.

Usage (after the wf suite ran with ``--cov=src/taskq/workflows
--cov-branch``):

    python scripts/check_wf_coverage.py [--data .coverage] [--floor 90]

Exits 1 when the scoped branch coverage is under the floor, 2 when the
tree/data is stale or dirty (the refusal — no number is reported).
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import subprocess
import sys
import time
from pathlib import Path

from coverage import Coverage

from taskq.testing._claims import record_claim

#: The floor (the honest measured number is recorded beside the verdict).
DEFAULT_FLOOR = 90.0

_SCOPE = "src/taskq/workflows"

REPO = Path(__file__).resolve().parent.parent
MEASUREMENTS = REPO / ".measurements"


def _git(*args: str) -> str:
    out = subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout
    return out.strip()


def _head() -> tuple[str, bool]:
    """(head sha, tree_dirty) — the artifact's provenance pair. THE DIRTY
    RULE: the evidence estate's OWN writes are not dirt — the battery
    writes the sinks and the run-scoped captures (tracked or not) as it
    runs, under ``.measurements/``. Dirt is the SOURCE tree (everything
    else) changing under the measurement."""
    sha = _git("rev-parse", "HEAD")
    # The paths, unambiguous (porcelain's XY spacing varies with the
    # staging state): the tracked changes + the untracked files.
    changed = (
        _git("diff", "--name-only", "HEAD")
        + "\n"
        + _git("ls-files", "--others", "--exclude-standard")
    )
    dirty = any(not path.startswith(".measurements/") for path in changed.splitlines() if path)
    return sha, dirty


def _last_source_commit_epoch() -> int:
    """The newest commit that touched anything OUTSIDE the measurements
    estate — the staleness comparison's anchor (a .measurements-only
    commit does not make fresh coverage data stale)."""
    out = subprocess.run(
        [
            "git",
            "log",
            "-1",
            "--format=%ct",
            "--",
            ".",
            ":(exclude).measurements",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return int(out.strip() or 0)


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
    if pct is None:  # pyright: ignore[reportUnnecessaryComparison]  # Why: coverage's report() is typed non-None but returns None on empty data at runtime — the defensive arm names the gate's own command instead of coverage's bare NoDataError.
        raise SystemExit(f"no coverage data in {data_file} — run the wf suite under --cov first")
    return pct, buf.getvalue()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=REPO / ".coverage")
    parser.add_argument("--floor", type=float, default=DEFAULT_FLOOR)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="the dev loop's escape hatch: measure a dirty tree, and the "
        "artifact says so (the verdict is stained, never silently cited)",
    )
    args = parser.parse_args()

    # ── THE STALENESS REFUSALS (the gate never reports from a stale tree) ──
    head_sha, dirty = _head()
    if dirty and not args.allow_dirty:
        print(
            f"[gate] REFUSED: the working tree is dirty — a coverage number "
            f"measured now is not {head_sha[:12]}'s number. Commit (or pass "
            "--allow-dirty and wear the stain).",
            file=sys.stderr,
        )
        return 2
    commit_ts = _last_source_commit_epoch()
    data_ts = int(args.data.stat().st_mtime)
    if data_ts < commit_ts:
        print(
            f"[gate] REFUSED: the coverage data ({args.data}) predates the "
            f"HEAD commit — measured on an ancestor tree, the number is "
            f"stale. Re-run the wf suite under --cov at the head.",
            file=sys.stderr,
        )
        return 2

    pct, table = scoped_branch_percent(args.data)
    verdict = "green" if pct >= args.floor else "RED"
    record = {
        "gate": "wf-scoped-branch-coverage",
        "scope": _SCOPE,
        "floor": args.floor,
        "measured_pct": round(pct, 2),
        "data_file": str(args.data),
        "head_sha": head_sha,
        "tree_dirty": dirty,
        "data_mtime_epoch": data_ts,
        "head_commit_epoch": commit_ts,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "verdict": verdict,
        "table": table.strip(),
    }
    runs = MEASUREMENTS / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    out = runs / f"wf-scoped-coverage-{time.strftime('%Y%m%dT%H%M%S')}.json"
    out.write_text(json.dumps(record, indent=2))
    # THE CLAIMS REGISTRY (the de-slop round's cure 3): the manifest is
    # the estate's ONE index — the verifier reads CLAIMS.json, never the
    # filename's run-scoped tail. The writer appends the claim at the
    # write (atomic under the registry lock).
    record_claim(
        stem="wf-scoped-coverage",
        file=out.name,
        head_sha=head_sha,
        captured_at=str(record["captured_at"]),
    )
    print(table)
    print(f"[gate] head {head_sha[:12]} (dirty={dirty})")
    print(f"[gate] wf-scoped branch coverage {pct:.2f}% vs floor {args.floor}% — {verdict}")
    print(f"[gate] recorded: {out}")
    return 0 if pct >= args.floor else 1


if __name__ == "__main__":
    sys.exit(main())
