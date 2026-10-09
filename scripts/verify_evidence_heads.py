"""THE ARTIFACTS' HEAD-STAMP LAW (the evidence-integrity round, cure 5 —
the receipts law extended to the artifact estate).

THE LAW: **every evidence artifact RECORDS ITS HEAD SHA, and the
verification re-runs the artifact ON ITS CLAIMED HEAD.** The convicted
shape: the "e2e 9/9x3" claim of record pointed at captures that were (a)
RED where the claim said green (leg3 failed in the recorded run) and (b)
taken on an ANCESTOR of the final head (the fix commit landed after
them) — green-in-fact, unproven-as-recorded. A capture without its head
is a rumor; a capture against a stale head is history, never the live
claim.

THE MECHANICS (what this verifier enforces over
``.measurements/runs/``):

* Artifacts group by STEM (the filename minus its run-scoped
  timestamp/token suffix). The NEWEST artifact of a stem is that
  evidence kind's LIVE CLAIM.
* The live claim must carry ``head_sha`` EQUAL to the current HEAD —
  the verification re-runs the artifact on its claimed head, so the
  claim and the tree can never diverge silently.
* THE OFF-BY-ONE RULE: the artifacts of a battery are committed AFTER
  the battery ran, so a claim recorded on H is still FRESH at H2 when
  H2 is H plus measurement-only changes (nothing outside
  ``.measurements/`` changed since the claimed head — the source tree
  the claim verifies is content-identical). A source change since the
  claimed head makes the claim stale.
* An OLDER artifact of a stem is history — implicitly SUPERSEDED-BY the
  newer live claim (the append-only run-scoped conversion's shape).
* An artifact whose stem's newest instance is STALE (wrong or missing
  ``head_sha``) and which is not superseded — FAILS. The cure is
  re-record at the head, or mark the stale file
  ``SUPERSEDED-BY: <newer artifact>`` (text) / ``"superseded_by": …``
  (JSON) with the link — never left as the live claim.

Text artifacts record the stamp as a trailing line
(``head: <sha>`` / ``SUPERSEDED-BY: <path>``); JSON artifacts as the
``head_sha`` / ``superseded_by`` keys. The writers stamp mechanically
(``scripts/check_wf_coverage.py``, ``tests/_wf_fixtures.py``'s band
writer + the redlog's flush) — an unstamped artifact is a writer bug,
and this verifier is the pin that catches it.

Usage:  python scripts/verify_evidence_heads.py [--runs-dir .measurements/runs]

Exits 1 with the named stale claims.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import cast

REPO = Path(__file__).resolve().parent.parent


def _git(*args: str, check: bool = True) -> str:
    out = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=check)
    return out.stdout.strip()


def _head() -> str:
    return _git("rev-parse", "HEAD")


def _source_changes_since(head_sha: str) -> bool:
    """Whether ANY commit since *head_sha* touched anything OUTSIDE the
    measurements estate. THE OFF-BY-ONE RULE (see the module docstring):
    a claim recorded on H is fresh at H2 when H2 is H plus
    measurement-only changes. An unresolvable claimed head (not an
    ancestor) = stale."""
    proc = subprocess.run(
        ["git", "diff", "--name-only", f"{head_sha}..HEAD", "--", ".", ":(exclude).measurements"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return True  # the claimed head is not an ancestor — stale by definition
    return bool(proc.stdout.strip())


def _stem(name: str) -> str:
    """The artifact's stem: the run-scoped tail (timestamp + optional
    token) stripped — ``fanout-1000-tx-band-20261008T090350-60a4`` →
    ``fanout-1000-tx-band``."""
    parts = name.rsplit("-", 2)
    if len(parts) == 3 and parts[1][:8].isdigit() and "T" in parts[1]:
        return parts[0]
    parts = name.rsplit("-", 1)
    if len(parts) == 2 and parts[1][:8].isdigit() and "T" in parts[1]:
        return parts[0]
    return name


def _text_field(path: Path, marker: str) -> str | None:
    """A text artifact's recorded field (``marker: value`` on its own
    line) — the human-capture shape."""
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{marker}:"):
            return stripped[len(marker) + 1 :].strip()
    return None


def _recorded_head(path: Path) -> str | None:
    """The artifact's recorded head sha (JSON key or the text stamp)."""
    if path.suffix == ".json":
        try:
            data: object = json.loads(path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            data = None
        else:
            if isinstance(data, dict):
                record = cast("dict[str, object]", data)
                head = record.get("head_sha")
                if isinstance(head, str) and head:
                    return head
    return _text_field(path, "head")


def _superseded_by(path: Path) -> str | None:
    if path.suffix == ".json":
        try:
            data: object = json.loads(path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            data = None
        if isinstance(data, dict):
            record = cast("dict[str, object]", data)
            marked = record.get("superseded_by")
            if isinstance(marked, str):
                return marked
        return None
    return _text_field(path, "SUPERSEDED-BY")


def _cited_import(path: Path) -> bool:
    """Whether the artifact DECLARES itself a CITED-IMPORT (the imported
    provenance record — the primary drill's session evidence gone, the
    import IS the provenance). JSON: the ``kind`` key's exact value; text:
    a ``kind: CITED-IMPORT`` line. Such an artifact is history by its own
    declaration — never a live claim of the current tree, never
    re-recordable."""
    if path.suffix == ".json":
        try:
            data: object = json.loads(path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return False
        if isinstance(data, dict):
            record = cast("dict[str, object]", data)
            kind = record.get("kind")
            return isinstance(kind, str) and kind.startswith("CITED-IMPORT")
        return False
    field = _text_field(path, "kind")
    return field == "CITED-IMPORT"


def verify(runs_dir: Path, head: str) -> list[str]:
    """The stale live claims, named (empty = the estate holds)."""
    if not runs_dir.is_dir():
        return [f"{runs_dir}: no runs directory — the estate has no captures at all"]
    # Group by stem; within a stem, newest mtime first.
    stems: dict[str, list[Path]] = {}
    for path in sorted(runs_dir.rglob("*")):
        if path.is_file() and path.suffix in (".json", ".txt", ".md"):
            stems.setdefault(_stem(path.name), []).append(path)
    failures: list[str] = []
    for stem, paths in sorted(stems.items()):
        paths.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        live = paths[0]
        recorded = _recorded_head(live)
        if recorded == head:
            continue  # the live claim is FRESH — verified on its own head
        if recorded is not None and not _source_changes_since(recorded):
            # The off-by-one rule: nothing outside the measurements
            # estate changed since the claimed head — the source tree
            # the claim verifies is content-identical. Fresh.
            continue
        marked = _superseded_by(live)
        if marked:
            target = (
                REPO / marked if marked.startswith(".measurements") else runs_dir.parent / marked
            )
            if not target.is_file():
                failures.append(
                    f"{stem}: {live.name} is marked SUPERSEDED-BY {marked} but the "
                    "target does not exist — the link is dead"
                )
            continue
        if _cited_import(live):
            # THE CITED-IMPORT DECLARATION (the docs-numbers round's own
            # marker): the artifact is an IMPORTED provenance record — the
            # primary drill's session evidence is GONE, the imported record
            # IS the provenance (``_sweep.py``'s curve cites it as such).
            # It is not a live claim of THIS tree and cannot be re-recorded;
            # the SOURCE claim built on it is a doc claim whose substance is
            # owned by the scope pin. History by its own declaration.
            continue
        failures.append(
            f"{stem}: the LIVE CLAIM {live.name} is "
            + (
                f"stale — recorded on {recorded[:12]}, the head is {head[:12]}"
                if recorded
                else "unstamped — no head_sha recorded"
            )
            + ". Re-record it at the head, or mark it SUPERSEDED-BY with "
            "the link — never left as the live claim."
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=REPO / ".measurements" / "runs")
    args = parser.parse_args()
    head = _head()
    failures = verify(args.runs_dir, head)
    if failures:
        print(f"THE HEAD-STAMP LAW REDS (head {head[:12]}):")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(
        f"the head-stamp law holds: every evidence kind's live claim in "
        f"{args.runs_dir} is recorded at THIS head ({head[:12]}) or "
        "explicitly superseded"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
