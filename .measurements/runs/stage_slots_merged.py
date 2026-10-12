"""THE GRAND MERGE's H2 staging (the door-edges lane's drain, re-run):
the slots (the drain's honest intermediate) + the chain re-points +
the ghosts' force-add.

THE CYCLE: the batteries that prove wf-battery-rv4 / fast-tier-n8
CONTAIN rv2-13, and rv2-13 verifies the COMMITTED estate — a battery
can only green when the estate is already clean, and the estate is
only clean when the batteries have green captures. The drain: a slot
entry (head_sha = the source-final head H1) staged with an HONEST
no-verdict declaration (the content door's TYPED WARNING, never a lie)
committed BEFORE the battery legs; the green runs FILL the slot files
in place; the staged entry's freshness is carried by the off-by-one
rule (every commit between is measurement-only).

Usage:  python stage_slots_merged.py <H1_SHA>
"""

from __future__ import annotations

import fcntl
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
RUNS = REPO / ".measurements" / "runs"
MANIFEST = RUNS / "CLAIMS.json"

#: The slots: stem → the declaration body (NO verdict shape — the door
#: warns, honestly, until the green run fills the file in place).
SLOTS = {
    "wf-battery-rv4": "the wf battery's slot — the battery leg's green output fills this file in place",
    "wf-battery-cov": "the wf battery's cov-leg slot — the battery leg's green output fills this file in place",
    "wf-battery-leg2": "the wf battery's leg-2 slot — the battery leg's green output fills this file in place",
    "wf-battery-leg3": "the wf battery's leg-3 slot — the battery leg's green output fills this file in place",
    "fast-tier-n8": "the fast tier's slot — the fast tier's green output fills this file in place",
    "evidence-heads-verify": "the verifier's receipt slot — the green verifier run's output fills this file in place",
    "rv2-13-green": "rv2-13's receipt slot — the green rv2-13 run's output fills this file in place",
    "deslop-pin-packs": "the packs' slot — the packs legs' green output fills this file in place",
}

#: The chain re-points: stem → the slot whose fresh capture re-proves
#: the kind (the walk lands there; the off-by-one rule carries it).
CHAIN_REPOINTS = {
    "attack-rounds": "wf-battery-rv4",
    "battery-final-wf-attacks": "wf-battery-rv4",
    "battery-wf-suite": "wf-battery-rv4",
    "docs-lane-rv2-13-cure-captures": "rv2-13-green",
    "fixround-final-pins": "wf-battery-rv4",
    "fixround-migration-chain": "wf-battery-rv4",
    "fixround-touched-pins-x2": "wf-battery-rv4",
    "rv2-pins-final": "wf-battery-rv4",
    "rv4-pin-packs": "deslop-pin-packs",
    "stealth-mutant-shipped-drill": "deslop-pin-packs",
    "fasttier-final": "fast-tier-n8",
    "fasttier-final-7a587f45c17d638c0c39a24174f77ec2cb807ba3": "fast-tier-n8",
    # A FILE-valued target (not a slot stem): the superseded_by resolves
    # to whatever registry entry claims that capture (the re-stamp's
    # mkdocs-strict live claim does).
    "mkdocs-strict-docslane": "mkdocs-strict-merged.txt",
}


def main() -> int:
    h1 = sys.argv[1]
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    with (RUNS / "CLAIMS.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            import orjson

            data = orjson.loads(MANIFEST.read_bytes())
            claims = data["claims"]
            slot_files: dict[str, str] = {}
            by_stem: dict[str, list[dict]] = {}
            for entry in claims:
                by_stem.setdefault(entry["stem"], []).append(entry)
            for stem, purpose in SLOTS.items():
                # The slot's file is FILLED IN PLACE only for the stems
                # whose re-stamp capture is already GREEN-content (the
                # packs' text capture); the battery/fast-tier stems get
                # a FRESH slot (their pre-estate captures carry rv2-13's
                # truthful red — the content door would rightly convict
                # a green claim over red content).
                if stem == "deslop-pin-packs":
                    existing = [
                        e
                        for e in by_stem.get(stem, [])
                        if e["file"].endswith("merged-leg1.txt")
                    ]
                    if existing:
                        slot_files[stem] = existing[-1]["file"]
                        continue
                name = f"{stem}-slot-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
                (RUNS / name).write_text(
                    f"# {stem}'s slot staged at head {h1} — captured {now}Z\n"
                    f"THE GRAND MERGE'S STAGED SLOT: {purpose}; no verdict is "
                    "recorded yet — the content door's typed warning "
                    "(unparsable-capture) is this slot's honest state until the fill.\n"
                )
                claims.append({"stem": stem, "file": name, "head_sha": h1, "captured_at": now})
                slot_files[stem] = name
            repointed: dict[str, str] = {}
            for stem, target in CHAIN_REPOINTS.items():
                if stem not in by_stem:
                    print(f"NO LIVE ENTRY for {stem} — skipped")
                    continue
                live = by_stem[stem][-1]
                target_file = (
                    target if target.endswith(".txt") else slot_files[target]
                )
                live["superseded_by"] = f"runs/{target_file}"
                repointed[stem] = live["file"]
            MANIFEST.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2) + b"\n")
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    # THE GHOSTS' CURE: force-add EVERY capture the registry names that
    # is not on the index (the tracked-claims law).
    import json as _json

    registry = _json.loads(MANIFEST.read_text())
    staged = 0
    for c in registry["claims"]:
        rel = f".measurements/runs/{c['file']}"
        probe = subprocess.run(
            ["git", "ls-files", rel], cwd=REPO, capture_output=True, text=True
        )
        if not probe.stdout.strip():
            add = subprocess.run(["git", "add", "-f", rel], cwd=REPO, capture_output=True)
            staged += add.returncode == 0
    for stem, name in slot_files.items():
        print(f"slot: {stem} -> {name}")
    for stem, file in repointed.items():
        print(f"re-pointed: {stem} (live {file})")
    print(f"ghosts force-added: {staged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
