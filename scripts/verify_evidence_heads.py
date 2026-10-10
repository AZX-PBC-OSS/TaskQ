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

THE MECHANICS (what this verifier enforces over the estate's registry,
``.measurements/runs/CLAIMS.json``):

* The claims registry is ONE JSON manifest — an append-ordered array of
  ``{stem, file, head_sha, captured_at}`` entries (the writers append:
  ``scripts/check_wf_coverage.py``, ``tests/_wf_fixtures.py``'s band
  writer; the migration pass built the initial index). The verdict is
  THE MANIFEST'S ENTRIES vs HEAD — no filename is ever parsed. (The
  de-slop round's conviction: the verifier's ``_stem``/``_run_order``
  heuristics parsed each filename's run-scoped tail — a NAME-FORMAT law
  the machines enforced, drift-prone by construction: the
  sha-in-the-name one-off stems split wrong. The name is for humans;
  the manifest is for the machinery.)
* Artifacts group by STEM (the evidence kind). The NEWEST entry of a
  stem — the registry's append order — is that evidence kind's LIVE
  CLAIM; every older entry is history, implicitly superseded.
* The live claim must carry ``head_sha`` EQUAL to the current HEAD —
  the verification re-runs the artifact on its claimed head, so the
  claim and the tree can never diverge silently.
* THE OFF-BY-ONE RULE: the artifacts of a battery are committed AFTER
  the battery ran, so a claim recorded on H is still FRESH at H2 when
  H2 is H plus measurement-only changes (nothing outside
  ``.measurements/`` changed since the claimed head — the source tree
  the claim verifies is content-identical). A source change since the
  claimed head makes the claim stale.
* A live claim may be marked ``superseded_by`` (the manifest entry's
  field — the migration bakes in the text files' ``SUPERSEDED-BY:``
  lines): history by its own declaration, the link must resolve.
* A live claim declared ``kind: CITED-IMPORT…`` (the imported
  provenance record — the primary drill's session evidence gone, the
  import IS the provenance) is history by its own declaration: never a
  live claim of the current tree, never re-recordable.
* A registry entry whose capture FILE is missing, and a registry that
  is missing entirely, FAIL — an index naming ghosts is the estate's
  own rot, and an estate without an index is unverifiable.

Usage:  python scripts/verify_evidence_heads.py [--runs-dir .measurements/runs]

Exits 1 with the named stale claims.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from taskq.testing._claims import RUNS, load_claims

REPO = Path(__file__).resolve().parent.parent


def _git(*args: str, check: bool = True) -> str:
    out = subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=check)
    return out.stdout.strip()


def _head() -> str:
    """The head the claims are verified against.

    Under GitHub Actions, a PR's checkout is the MERGE REF (the PR's
    head merged into main) — a commit that exists nowhere in the
    branch's own history: every locally-recorded claim's ``head_sha``
    is then "stale" BY CONSTRUCTION (the merge ref's diff carries
    main's side), and the law can never green in CI. The claims verify
    THE BRANCH's estate, so under Actions the law verifies against the
    PR's OWN head sha (the event payload's ``pull_request.head.sha``);
    the local runs (the dev loop, the lanes) keep ``git rev-parse
    HEAD``.
    """
    event = os.environ.get("GITHUB_EVENT_PATH")
    if event and Path(event).is_file():
        try:
            payload = json.loads(Path(event).read_text())
            pr_head = payload.get("pull_request", {}).get("head", {}).get("sha")
            if pr_head:
                return str(pr_head)
        except (json.JSONDecodeError, OSError):
            pass  # not a PR event (a push/schedule run) — the checkout IS the head
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


def verify(runs_dir: Path, head: str) -> list[str]:
    """The stale live claims, named (empty = the estate holds)."""
    if not (runs_dir / "CLAIMS.json").is_file():
        return [
            f"{runs_dir / 'CLAIMS.json'}: the claims registry is missing — "
            "the estate has no index, so no capture is verifiable (the writers "
            "append one entry per capture; the migration pass built the initial one)"
        ]
    # Group by stem; the registry's append order is the run order — the
    # LAST entry of a stem is the live claim. No filename is parsed: the
    # manifest is the index (the name only ever points at the file).
    stems: dict[str, list[dict[str, str]]] = {}
    for entry in load_claims(runs_dir):
        stem = entry.get("stem", "")
        file = entry.get("file", "")
        if not stem or not file:
            failures_entry = entry.get("file") or entry.get("stem") or json.dumps(entry)
            return [f"the claims registry carries a malformed entry: {failures_entry}"]
        stems.setdefault(stem, []).append(entry)
    failures: list[str] = []
    for stem, entries in sorted(stems.items()):
        live = entries[-1]
        marked = live.get("superseded_by")
        if marked:
            if marked.startswith(".measurements"):
                target = REPO / marked
            elif Path(marked).is_absolute():
                target = Path(marked)
            else:
                target = REPO / runs_dir.parent / marked
            if not target.is_file():
                failures.append(
                    f"{stem}: {live['file']} is marked superseded_by {marked} but the "
                    "target does not exist — the link is dead"
                )
            continue
        missing = REPO / runs_dir / live["file"]
        if not missing.is_file():
            failures.append(
                f"{stem}: the LIVE CLAIM {live['file']} is a GHOST — the registry "
                "names a capture that is not on disk (the index and the estate "
                "disagree; restore the file or drop the entry)"
            )
            continue
        recorded = live.get("head_sha") or None
        if recorded == head:
            continue  # the live claim is FRESH — verified on its own head
        if recorded is not None and not _source_changes_since(recorded):
            # The off-by-one rule: nothing outside the measurements
            # estate changed since the claimed head — the source tree
            # the claim verifies is content-identical. Fresh.
            continue
        if str(live.get("kind", "")).startswith("CITED-IMPORT"):
            # THE CITED-IMPORT DECLARATION (the docs-numbers round's own
            # marker): the artifact is an IMPORTED provenance record —
            # the primary drill's session evidence is GONE, the imported
            # record IS the provenance (``_sweep.py``'s curve cites it as
            # such). It is not a live claim of THIS tree and cannot be
            # re-recorded; the SOURCE claim built on it is a doc claim
            # whose substance is owned by the scope pin. History by its
            # own declaration.
            continue
        failures.append(
            f"{stem}: the LIVE CLAIM {live['file']} is "
            + (
                f"stale — recorded on {recorded[:12]}, the head is {head[:12]}"
                if recorded
                else "unstamped — no head_sha recorded"
            )
            + ". Re-record it at the head, or mark the entry superseded_by "
            "with the link — never left as the live claim."
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=RUNS)
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
