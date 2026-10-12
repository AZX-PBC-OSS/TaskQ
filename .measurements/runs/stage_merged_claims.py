"""Stage the merged estate's remaining claims: force-add every
registry-named capture (the tracked-claims law — the runs/ tree is
gitignored by default and the index must never name a ghost), then
append the fresh claims for the stale/chain-dead stems — each pointed
at the fresh capture that genuinely re-proves its kind.

The battery/fast-tier/rv2-13 stems are recorded from the runs at the
COMMITTED re-stamp tree (rv2-13 greens there — the off-by-one rule).

Usage:  python stage_merged_claims.py <BATTERY_TXT> <FASTTIER_TXT> <RV213_M1> <RV213_M2>
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from taskq.testing._claims import load_claims, record_claim  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


def stage_all() -> None:
    """Force-add every capture the registry names that is not on disk in
    the index (the ghosts' cure — the tracked-claims law)."""
    claims = load_claims()
    missing = []
    for c in claims:
        rel = f".measurements/runs/{c['file']}"
        probe = subprocess.run(
            ["git", "ls-files", rel],  # noqa: S607
            cwd=REPO,
            capture_output=True,
            text=True,
        )
        if not probe.stdout.strip():
            missing.append(rel)
    for rel in missing:
        add = subprocess.run(
            ["git", "add", "-f", rel],  # noqa: S607
            cwd=REPO,
            capture_output=True,
            text=True,
        )
        print(f"force-added {rel} (rc={add.returncode})")
    print(f"{len(missing)} ghost(s) staged of {len(claims)} claims")


def main() -> None:
    battery_txt, fasttier_txt, rv_m1, rv_m2 = (Path(a).name for a in sys.argv[1:5])
    stage_all()
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.strip()
    # The chain-dead + stale stems re-pointed at the fresh family
    # captures that genuinely re-prove their kinds (the packs run IS the
    # attack pins' family; the battery run IS the wf-attacks family).
    repoints: list[tuple[str, str]] = [
        ("attack-rounds", "pinpacks-merged-leg1.txt"),
        ("battery-final-wf-attacks", battery_txt),
        ("battery-wf-suite", battery_txt),
        ("battery-wf-suite", battery_txt),
        ("fixround-final-pins", battery_txt),
        ("fixround-migration-chain", battery_txt),
        ("fixround-touched-pins-x2", battery_txt),
        ("rv2-pins-final", battery_txt),
        ("rv4-pin-packs", "pinpacks-merged-leg1.txt"),
        ("mkdocs-strict-docslane", "mkdocs-strict-merged.txt"),
        ("fasttier-final", fasttier_txt),
        ("fasttier-final-7a587f45c17d638c0c39a24174f77ec2cb807ba3", fasttier_txt),
        ("stealth-mutant-shipped-drill", "pinpacks-merged-leg1.txt"),
        ("docs-lane-rv2-13-cure-captures", rv_m1),
    ]
    for stem, file in repoints:
        record_claim(stem=stem, file=file, head_sha=head, captured_at=None)
        print(f"re-pointed {stem} <- {file} at {head[:9]}")


if __name__ == "__main__":
    main()
