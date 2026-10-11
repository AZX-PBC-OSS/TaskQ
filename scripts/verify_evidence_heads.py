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
  lines): history by its own declaration. THE CHAIN IS THE CLAIM (the
  estate-cures round, cure 1): the verifier resolves the supersession
  CHAIN to its TERMINAL entry — the first entry in the chain with no
  ``superseded_by`` — and applies the head-sha rule THERE: freshness +
  the capture's presence are judged at the END of the chain, not the
  head link (the effective claim is what proves the stem). A dead link
  at ANY hop, a target no registry entry claims (the dangling chain), a
  chain that cycles (it never terminates), and a terminal that is a
  ghost or stale ALL FAIL. The convicted hole this replaces: the old
  law read only "the link must resolve" — the HEAD link's existence was
  the whole check, so a stem GREENED while its effective (terminal)
  claim was stale or missing, the rot hidden one hop behind a resolving
  link (the round-four sin's shape, at the registry's own door).
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


def _supersession_target(marked: str, runs_dir: Path) -> Path:
    """The ``superseded_by`` link's target path (the manifest entry's
    field spells the capture three ways — runs-relative, estate-relative,
    or absolute; the migration's three shapes, honored as recorded)."""
    if marked.startswith(".measurements"):
        return REPO / marked
    if Path(marked).is_absolute():
        return Path(marked)
    return REPO / runs_dir.parent / marked


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
    # The registry entry per capture path (the chain resolver's index):
    # the same file may be claimed by several stems — the registry's
    # append order decides (the LAST claim of a file owns the walk).
    by_path: dict[str, dict[str, str]] = {}
    for entry in load_claims(runs_dir):
        stem = entry.get("stem", "")
        file = entry.get("file", "")
        if not stem or not file:
            failures_entry = entry.get("file") or entry.get("stem") or json.dumps(entry)
            return [f"the claims registry carries a malformed entry: {failures_entry}"]
        stems.setdefault(stem, []).append(entry)
        by_path[str((runs_dir / file).resolve())] = entry
    failures: list[str] = []
    for stem, entries in sorted(stems.items()):
        live = entries[-1]
        marked = live.get("superseded_by")
        if marked:
            # THE CHAIN LAW (the module docstring): walk the supersession
            # chain to its TERMINAL entry, then judge the terminal by the
            # head-sha rule. Every hop's content is checked (a chain
            # through a missing capture proves nothing), the walk must
            # terminate, and the termination point must be a real, fresh
            # capture — the effective claim is the chain's END.
            chain: list[dict[str, str]] = []
            current = live
            terminal: dict[str, str] | None = None
            while True:
                link = current.get("superseded_by")
                if not link:
                    terminal = current
                    break
                target = _supersession_target(link, runs_dir)
                if not target.is_file():
                    failures.append(
                        f"{stem}: the supersession chain's link at {current['file']} "
                        f"names {link} but the target does not exist — the chain is "
                        "dead mid-walk (content is checked at EVERY hop, not just the "
                        "head link: a chain through a missing capture proves nothing)"
                    )
                    break
                owner = by_path.get(str(target.resolve()))
                if owner is None:
                    failures.append(
                        f"{stem}: the supersession chain's link at {current['file']} "
                        f"names {link} — the file exists but NO registry entry claims "
                        "it, so the chain dangles past the index (the manifest is the "
                        "estate's only index; a capture outside it does not exist to "
                        "the verdict, and an unowned terminal proves nothing)"
                    )
                    break
                if any(owner is walked for walked in chain):
                    walked_path = " → ".join(w["file"] for w in [*chain, current, owner])
                    failures.append(
                        f"{stem}: the supersession chain CYCLES — the walk "
                        f"{walked_path} re-enters itself and never reaches a "
                        "terminal entry; a chain with no end has no effective "
                        "claim (the self-supersession is the lie here)"
                    )
                    break
                chain.append(current)
                current = owner
            if terminal is None:
                continue
            # THE HEAD-SHA RULE AT THE TERMINAL (the end of the chain):
            # content first (the terminal's capture must EXIST), then
            # freshness — the same rule a live claim carries, applied
            # where the chain actually lands.
            if not (REPO / runs_dir / terminal["file"]).is_file():
                failures.append(
                    f"{stem}: the supersession chain's TERMINAL capture "
                    f"{terminal['file']} is a GHOST — the chain resolves to a "
                    "registry entry whose capture is not on disk (the index and the "
                    "estate disagree at the END of the chain, where the effective "
                    "claim lives; restore the file or extend the chain to a real capture)"
                )
                continue
            recorded = terminal.get("head_sha") or None
            if recorded == head:
                continue  # the terminal is FRESH — the chain lands on this head
            if recorded is not None and not _source_changes_since(recorded):
                # The off-by-one rule at the terminal: nothing outside the
                # measurements estate changed since the terminal's claimed
                # head — the capture verifies a content-identical tree.
                continue
            if str(terminal.get("kind", "")).startswith("CITED-IMPORT"):
                # The CITED-IMPORT declaration at the terminal: the chain
                # ends at an imported provenance record — history by its
                # own declaration, never re-recordable (the live claim's
                # own escape, applied where the chain lands).
                continue
            failures.append(
                f"{stem}: the supersession chain's TERMINAL claim {terminal['file']} is "
                + (
                    f"stale — recorded on {recorded[:12]}, the head is {head[:12]}"
                    if recorded
                    else "unstamped — no head_sha recorded"
                )
                + ". The chain's END is the effective claim: re-record it at the "
                "head, or extend the chain to a fresh capture — never leave the "
                "stem's proof stale behind a resolving link."
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
