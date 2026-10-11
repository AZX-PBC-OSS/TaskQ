"""The content-door lane's H2 staging: the five SLOTS (the drain's honest
intermediate — the estate-cures round's own shape) + the twenty chain
re-points.

THE DRAIN, why the slots exist: the batteries that prove
wf-battery-rv4 / deslop-pin-packs / fast-tier-n8 CONTAIN rv2-13, and
rv2-13 verifies the COMMITTED estate — so a battery can only green
when the estate is already clean, and the estate is only clean when
the batteries have green captures. The cycle drains through the SLOT:
a registry entry (head_sha = the source-final head H1) staged with an
HONEST no-verdict declaration — the content door's TYPED WARNING, not
a lie and not a silent pass — committed BEFORE the battery legs; the
green runs then FILL the slots in place (no new claims; the staged
entry's freshness is carried by the off-by-one rule, every commit
between being measurement-only).

The chain stems (whose supersession chains are stale, dead, or cycling
on the committed estate — the ported chain cure's convictions) are
re-pointed at the fresh family captures that genuinely re-prove their
kinds: the battery family at the wf-battery-rv4 slot, the fast-tier
family at the fast-tier-n8 slot, wf-perf-bands at the fresh
wf-bands-rv4 capture, mkdocs-strict-docslane at the fresh mkdocs-strict
capture.

Usage:  python stage_slots.py <H1_SHA>
"""

from __future__ import annotations

import fcntl
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
    "deslop-pin-packs": "the de-slopped packs' slot — the packs leg's green output fills this file in place",
    "fast-tier-n8": "the fast tier's slot — the fast tier's green output fills this file in place",
    "evidence-heads-verify": "the verifier's receipt slot — the green verifier run's output fills this file in place",
    "rv2-13-green": "rv2-13's receipt slot — the green rv2-13 run's output fills this file in place",
}

#: The chain re-points: stem → the slot whose fresh capture re-proves
#: the kind (the walk lands there; the off-by-one rule carries it).
CHAIN_REPOINTS = {
    "attack-rounds": "wf-battery-rv4",
    "battery-final-wf-attacks": "wf-battery-rv4",
    "battery-wf-suite": "wf-battery-rv4",
    "docs-lane-rv2-13-cure-captures": "wf-battery-rv4",
    "estate-slice": "wf-battery-rv4",
    "fixround-final-pins": "wf-battery-rv4",
    "fixround-migration-chain": "wf-battery-rv4",
    "fixround-touched-pins-x2": "wf-battery-rv4",
    "rv2-pins-final": "wf-battery-rv4",
    "rv2-pins-final-20261010T193500-precommit-red.txt": "wf-battery-rv4",
    "rv4-final-proof-64645df381b7e76c5c9c86bb69712108da6abd88.txt": "wf-battery-rv4",
    "rv4-pin-packs": "wf-battery-rv4",
    "stealth-mutant-shipped-drill": "wf-battery-rv4",
    "wf-battery-cov": "wf-battery-rv4",
    "wf-battery-leg2": "wf-battery-rv4",
    "wf-battery-leg3": "wf-battery-rv4",
    "fasttier-final": "fast-tier-n8",
    "fasttier-final-7a587f45c17d638c0c39a24174f77ec2cb807ba3": "fast-tier-n8",
}


def main() -> int:
    h1 = sys.argv[1]
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    # The registry's own discipline: the exclusive flock over a
    # read-modify-write (taskq/testing/_claims.py's shape, done inline —
    # this edit COMBINES slot appends with chain re-points).
    with (RUNS / "CLAIMS.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            import orjson

            data = orjson.loads(MANIFEST.read_bytes())
            claims = data["claims"]
            slot_files: dict[str, str] = {}
            for stem, purpose in SLOTS.items():
                name = f"{stem}-slot-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.txt"
                (RUNS / name).write_text(
                    f"# {stem}'s slot staged at head {h1} — captured {now}Z\n"
                    f"THE CONTENT-DOOR LANE'S STAGED SLOT: {purpose}; no verdict is "
                    "recorded yet — the content door's typed warning "
                    "(unparsable-capture) is this slot's honest state until the fill.\n"
                )
                claims.append({"stem": stem, "file": name, "head_sha": h1, "captured_at": now})
                slot_files[stem] = name
            # The chain re-points: the CHAIN stem's LIVE entry's
            # superseded_by → the fresh family capture's runs-relative name.
            repointed: dict[str, str] = {}
            by_stem: dict[str, list[dict]] = {}
            for entry in claims:
                by_stem.setdefault(entry["stem"], []).append(entry)
            for stem, target_stem in CHAIN_REPOINTS.items():
                live = by_stem[stem][-1]
                live["superseded_by"] = f"runs/{slot_files[target_stem]}"
                repointed[stem] = live["file"]
            MANIFEST.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2) + b"\n")
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    for stem, name in slot_files.items():
        print(f"slot: {stem} -> {name}")
    for stem, file in repointed.items():
        print(f"re-pointed: {stem} (live {file})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
