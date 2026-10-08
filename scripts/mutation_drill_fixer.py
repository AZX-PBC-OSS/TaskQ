#!/usr/bin/env python3
"""THE MUTATION DRILL (the §7b fixer round — the evidence-matrix's
Tier-1 items 1+2): for each EMPTY PROVEN cell, apply the named mutation
to the codec/display source, run the CONSUMING pins, and require the
machine-generated RED; revert the source; append the receipt to
`.measurements/int4-fixer-pin-reds.json`.

THE RECEIPTS LAW: a PROVEN cell = a pin whose red was a REAL mutation
with a machine-generated receipt. A drill whose pins stay GREEN is a
FINDING (the pin does not own the behavior), not a receipt.
"""

from __future__ import annotations

import datetime
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RECEIPTS = REPO / ".measurements" / "int4-fixer-pin-reds.json"
RUN = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ") + "-fixer"

MUTATIONS: list[dict[str, object]] = [
    {
        "pin": "outputs-decode-unwrapper-dropped",
        "mutation": "decode_result's envelope unwrap dropped (the {'value': …} unwritten-back — the read returns the raw envelope dict)",
        "file": "src/taskq/workflows/api/_runner_codec.py",
        "old": '    if "value" in decoded and len(decoded) == 1:',
        "new": '    if False and "value" in decoded and len(decoded) == 1:  # THE MUTATION',
        "pins": "tests/test_wf_runner_pins.py::test_flow_result_read_decodes_once",
    },
    {
        "pin": "outputs-encode-wrapper-dropped",
        "mutation": "encode_result's {'value': …} wrapper dropped (the body's return written BARE — the envelope-key collision unconvicted)",
        "file": "src/taskq/workflows/api/_runner_codec.py",
        "old": '    return {"value": encode_data_arg(value)}',
        "new": "    return encode_data_arg(value)  # THE MUTATION",
        "pins": "tests/test_wf_runner_pins.py tests/test_wf_phase3_cure_pins.py tests/test_wf_t20_router_pins.py tests/test_wf_scenario_pins.py",
    },
    {
        "pin": "admin-view-node-dropped",
        "mutation": "rebuild_display's ledger seed loop DROPS the failed rows (the explorer's read omits the failed node — the page cannot render it)",
        "file": "src/taskq/workflows/_progress_read.py",
        "old": """    disp: dict[str, dict[str, Any]] = {}
    for r in ledger_rows:
        disp[str(r["id"])] = {""",
        "new": """    disp: dict[str, dict[str, Any]] = {}
    for r in ledger_rows:
        if r.get("error_class") is not None:
            continue  # THE MUTATION: the failed node dropped from the display
        disp[str(r["id"])] = {""",
        "pins": "tests/test_wf_progress_faces.py::test_display_shows_failure_with_pct_inside_the_liar_reds",
    },
    {
        "pin": "admin-view-status-launders",
        "mutation": "the LEDGER-WINS fence launders the status (the final word hard-flips: failed -> succeeded, succeeded -> running — the progress-lie fence's teeth removed)",
        "file": "src/taskq/workflows/_progress_read.py",
        "old": '            disp[str(r["id"])]["status"] = r["status"]',
        "new": '            disp[str(r["id"])]["status"] = (  # THE MUTATION\n                "succeeded" if r["status"] != "succeeded" else "running"\n            )',
        "pins": "tests/test_wf_progress_faces.py::test_display_shows_failure_with_pct_inside_the_liar_reds tests/test_wf_progress_faces.py::test_reconnect_reconstructs_the_display_full_and_partial",
    },
]


def run_pin(cmd: str) -> tuple[int, str]:
    uv = shutil.which("uv") or "uv"
    proc = subprocess.run(  # noqa: S603
        [uv, "run", "--no-sync", "pytest", *cmd.split(), "-q", "-p", "no:cacheprovider"],
        capture_output=True,
        text=True,
        timeout=900,
        cwd=REPO,
        env={**__import__("os").environ, "TASKQ_TEST_PG_DSN": sys.argv[1]},
    )
    return proc.returncode, proc.stdout + proc.stderr


def red_evidence(out: str) -> str:
    """The machine-generated red: the failing assertion's lines."""
    lines = [ln for ln in out.splitlines() if ln.startswith("E ")]
    return " | ".join(lines[:3])[:400]


def main() -> int:
    entries: list[dict[str, str]] = []
    failures: list[str] = []
    for spec in MUTATIONS:
        src = REPO / str(spec["file"])
        original = src.read_text()
        assert str(spec["old"]) in original, f"the drill's anchor is stale: {spec['pin']}"
        mutated = original.replace(str(spec["old"]), str(spec["new"]), 1)
        src.write_text(mutated)
        reds: list[str] = []
        try:
            for selector in str(spec["pins"]).split():
                code, out = run_pin(selector)
                red = red_evidence(out)
                if code == 0:
                    print(f"green under the mutation: {selector}")
                    continue
                reds.append(f"{selector} :: {red or f'exit={code}'}")
                print(f"REDS  {spec['pin']} @ {selector}: {red[:140]}")
            if not reds:
                failures.append(
                    f"{spec['pin']}: the mutation did NOT red any consuming pin — "
                    "the pins do not own the behavior (a FINDING, not a receipt)"
                )
                entries.append(
                    {
                        "pin": str(spec["pin"]),
                        "mutation": str(spec["mutation"]),
                        "red": "FINDING: no consuming pin redded — the redundancy healed the mutation (recorded, not a receipt)",
                    }
                )
                continue
            entries.append(
                {
                    "pin": str(spec["pin"]),
                    "mutation": str(spec["mutation"]),
                    "red": " || ".join(reds)[:900],
                }
            )
        finally:
            src.write_text(original)  # the revert — the drill leaves no source diff
    with RECEIPTS.open("a") as f:
        f.write(
            json.dumps(
                {
                    "run": RUN,
                    "ts": datetime.datetime.now(datetime.UTC).isoformat(),
                    "entries": entries,
                }
            )
            + "\n"
        )
    if failures:
        print("\nTHE DRILL'S FINDINGS:")
        for x in failures:
            print(f"  {x}")
        return 1
    print(
        f"\nthe drill holds: {len(entries)} mutations, {len(entries)} machine-generated reds, sources reverted"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
