#!/usr/bin/env python3
"""Build an ephemeral integration branch by merging open PRs in creation order.

Usage:
    uv run scripts/integration_merge.py [--dry-run] [--base BRANCH] [--name BRANCH]

Strategy:
  1. Fetch all open PRs from GitHub via `gh pr list`.
  2. Exclude chore(release)/release-please PRs (autorelease label or title match).
  3. Sort by creation date (oldest first) so earlier PRs land first.
  4. Create (or reset) an integration branch from the base (default: origin/main).
  5. For each PR, try `git merge --no-ff`. If it conflicts, abort that merge,
     record the failure, and continue with the next PR.
  6. Print a summary table of merged / failed / skipped PRs.
  7. Optionally run tests after each successful merge (--verify).

The script is idempotent: re-running it will reset the integration branch
and try again from scratch.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GIT_DIR = Path(__file__).resolve().parent.parent


def run(
    cmd: list[str],
    *,
    check: bool = True,
    capture: bool = True,
    cwd: Path = GIT_DIR,
) -> subprocess.CompletedProcess[str]:
    """Run a command, returning the completed process."""
    return subprocess.run(  # noqa: S603
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        cwd=cwd,
    )


def git(*args: str, check: bool = True, capture: bool = True) -> str:
    """Run a git command, returning stdout."""
    r = run(["git", *args], check=check, capture=capture)
    return r.stdout.strip() if capture else ""


def gh(*args: str) -> str:
    """Run a gh command, returning stdout."""
    r = run(["gh", *args])
    return r.stdout


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass
class PR:
    number: int
    title: str
    head: str
    base: str
    created_at: str
    author: str
    labels: list[str] = field(default_factory=list)

    @property
    def is_release(self) -> bool:
        if "autorelease" in self.labels:
            return True
        return bool(
            re.match(r"chore\(.*\)\s*:", self.title, re.IGNORECASE)
            and "release" in self.title.lower()
        )


@dataclass
class MergeResult:
    pr: PR
    status: str  # "merged" | "conflict" | "skipped" | "error"
    detail: str = ""


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------


def fetch_prs() -> list[PR]:
    """Fetch all open PRs from GitHub, sorted by creation date (oldest first)."""
    raw = gh(
        "pr",
        "list",
        "--state",
        "open",
        "--limit",
        "100",
        "--json",
        "number,title,headRefName,baseRefName,createdAt,author,labels",
    )
    items = json.loads(raw)
    prs = [
        PR(
            number=item["number"],
            title=item["title"],
            head=item["headRefName"],
            base=item["baseRefName"],
            created_at=item["createdAt"],
            author=item.get("author", {}).get("login", "unknown"),
            labels=[label["name"] for label in item.get("labels", [])],
        )
        for item in items
    ]
    prs.sort(key=lambda p: p.created_at)
    return prs


def prepare_branch(name: str, base: str, dry_run: bool = False) -> None:
    """Reset the current branch to match base.

    Assumes we are already on the integration branch in a worktree.
    """
    git("fetch", "origin")
    if not dry_run:
        current = git("branch", "--show-current")
        if current != name:
            raise RuntimeError(f"Expected to be on branch '{name}', but on '{current}'")
        git("reset", "--hard", base)
    print(f"  [branch] integration branch: {name} (reset to {base})")


def try_merge(pr: PR, verify: bool = False) -> MergeResult:
    """Attempt to merge a single PR into the current branch."""
    # Fetch the PR head ref
    ref = f"pull/{pr.number}/head"
    try:
        git("fetch", "origin", f"{ref}:{pr.head}")
    except subprocess.CalledProcessError:
        # Branch may already exist locally; fetch the ref without forcing
        git("fetch", "origin", ref)

    # Try the merge
    merge_cmd = [
        "merge",
        "--no-ff",
        "--no-edit",
        pr.head,
        "-m",
        f"merge: PR #{pr.number} — {pr.title}",
    ]
    try:
        git(*merge_cmd)
    except subprocess.CalledProcessError:
        # Conflict — abort and report
        git("merge", "--abort", check=False)
        return MergeResult(pr=pr, status="conflict", detail="merge conflict")

    # Optional: run tests
    if verify:
        try:
            r = run(["uv", "run", "pytest", "-n", "4", "-x", "--timeout=30", "-q"], check=True)
            if r.returncode != 0:
                return MergeResult(pr=pr, status="error", detail="tests failed after merge")
        except subprocess.CalledProcessError as e:
            # Revert the merge so subsequent PRs aren't affected
            git("reset", "--hard", "HEAD~1")
            return MergeResult(pr=pr, status="error", detail=f"tests failed: {e.stderr[:200]}")

    return MergeResult(pr=pr, status="merged")


def print_summary(results: list[MergeResult]) -> None:
    """Print a summary table of all merge results."""
    print("\n" + "=" * 80)
    print(f"{'#':>4}  {'Status':<10}  {'PR':<6}  {'Title':<50}  Detail")
    print("-" * 80)
    for r in results:
        print(
            f"  {r.pr.number:>4}  {r.status:<10}  #{r.pr.number:<4}  {r.pr.title[:50]:<50}  {r.detail}"
        )
    print("=" * 80)

    merged = [r for r in results if r.status == "merged"]
    conflicts = [r for r in results if r.status == "conflict"]
    errors = [r for r in results if r.status == "error"]
    skipped = [r for r in results if r.status == "skipped"]

    print(f"\n  Merged:   {len(merged)}")
    print(f"  Conflict: {len(conflicts)}")
    print(f"  Error:    {len(errors)}")
    print(f"  Skipped:  {len(skipped)}")

    if conflicts:
        print("\n  PRs with conflicts (need manual merge):")
        for r in conflicts:
            print(f"    #{r.pr.number}: {r.pr.title}")
            print(f"      branch: {r.pr.head}")

    if errors:
        print("\n  PRs with errors (tests failed):")
        for r in errors:
            print(f"    #{r.pr.number}: {r.pr.title}")
            print(f"      detail: {r.detail}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build integration branch from open PRs")
    parser.add_argument("--dry-run", action="store_true", help="List PRs without merging")
    parser.add_argument("--base", default="main", help="Base branch (default: main)")
    parser.add_argument(
        "--name", default="integration", help="Integration branch name (default: integration)"
    )
    parser.add_argument("--verify", action="store_true", help="Run tests after each merge")
    parser.add_argument("--only", type=int, nargs="+", help="Only merge specific PR numbers")
    parser.add_argument("--skip", type=int, nargs="+", help="Skip specific PR numbers")
    args = parser.parse_args()

    print("\n*** Integration Merge Script ***\n")

    # 1. Fetch PRs
    print("Fetching open PRs from GitHub...")
    prs = fetch_prs()
    total = len(prs)

    # 2. Filter out release PRs
    release_prs = [p for p in prs if p.is_release]
    prs = [p for p in prs if not p.is_release]
    if release_prs:
        print(f"  Skipping {len(release_prs)} release PR(s): {[p.number for p in release_prs]}")

    # 3. Apply --only / --skip filters
    if args.only:
        prs = [p for p in prs if p.number in args.only]
    if args.skip:
        prs = [p for p in prs if p.number not in args.skip]

    print(f"  {len(prs)} PR(s) to merge (out of {total} open):\n")
    for p in prs:
        print(f"  #{p.number:>3}  {p.created_at[:10]}  {p.head:<40}  {p.title[:60]}")

    if args.dry_run:
        print("\n[dry-run] No merges performed.")
        return 0

    # 4. Prepare branch
    print(f"\nPreparing integration branch '{args.name}' from '{args.base}'...")
    prepare_branch(args.name, args.base)

    # 5. Merge each PR
    results: list[MergeResult] = []
    for i, pr in enumerate(prs, 1):
        print(f"\n[{i}/{len(prs)}] Merging #{pr.number}: {pr.title}")
        result = try_merge(pr, verify=args.verify)
        results.append(result)
        if result.status == "merged":
            print("  -> merged OK")
        else:
            print(f"  -> {result.status.upper()}: {result.detail}")

    # 6. Summary
    print_summary(results)

    # 7. Return exit code
    failed = [r for r in results if r.status in ("conflict", "error")]
    if failed:
        print(f"\n  {len(failed)} PR(s) need manual attention.")
        return 1

    print("\n  All PRs merged successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
