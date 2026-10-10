"""THE CLAIMS REGISTRY'S MIGRATION PASS (the de-slop round, cure 3 — run
once; kept for the record and for dry/other estates).

Indexes every existing capture under ``.measurements/runs/`` into the
registry, ``CLAIMS.json`` — the files STAY the captures, the manifest is
the INDEX. The pass uses the old filename heuristics (the run-scoped
tail's stem, the embedded timestamp's order) exactly ONCE, at the
boundary: after this pass the machinery reads the manifest only, and a
filename's shape is a human concern the verdict never again depends on.

Per file the pass reads (never writes) the capture's own recorded marks:
``head_sha`` / ``head:`` (the stamp), ``kind`` (the CITED-IMPORT
declaration), ``superseded_by`` / ``SUPERSEDED-BY:`` — baked into the
entries so the verifier's manifest-read keeps the file-resident marks'
semantics without re-reading the files.

Usage:  python scripts/build_claims_manifest.py [--runs-dir .measurements/runs]

Exits 0 with the index written (the entry count printed).
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import cast

REPO = Path(__file__).resolve().parent.parent
RUNS = REPO / ".measurements" / "runs"


def _stem(name: str) -> str:
    """The evidence-kind stem: the run-scoped tail (timestamp + optional
    token) stripped — the OLD convention, honored once at the boundary."""
    parts = name.rsplit("-", 2)
    if len(parts) == 3 and parts[1][:8].isdigit() and "T" in parts[1]:
        return parts[0]
    parts = name.rsplit("-", 1)
    if len(parts) == 2 and parts[1][:8].isdigit() and "T" in parts[1]:
        return parts[0]
    return name


def _run_order(p: Path) -> tuple[str, float]:
    """The run-scoped sort key: the NAME's embedded run timestamp (one
    last read — the pre-manifest estate's order), the mtime the
    tie-break for the pre-convention names."""
    m = re.search(r"(\d{8}T\d{6})", p.name)
    return (m.group(1) if m else "", p.stat().st_mtime)


def _json_field(path: Path, key: str) -> object:
    try:
        data: object = json.loads(path.read_text())
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None
    if isinstance(data, dict):
        return cast("dict[str, object]", data).get(key)
    return None


def _text_field(path: Path, marker: str) -> str | None:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(f"{marker}:"):
            return stripped[len(marker) + 1 :].strip()
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=RUNS)
    args = parser.parse_args()
    runs = args.runs_dir
    files = [
        p
        for p in sorted(runs.rglob("*"))
        if p.is_file() and p.suffix in (".json", ".txt", ".md") and p.name != "CLAIMS.json"
    ]
    files.sort(key=_run_order)
    claims: list[dict[str, str]] = []
    for path in files:
        stem = _stem(path.name)
        head = _json_field(path, "head_sha")
        if not isinstance(head, str) or not head:
            head = _text_field(path, "head")
        kind = _json_field(path, "kind")
        if not isinstance(kind, str) or not kind:
            kind = _text_field(path, "kind")
        superseded = _json_field(path, "superseded_by")
        if not isinstance(superseded, str) or not superseded:
            superseded = _text_field(path, "SUPERSEDED-BY")
        stamp = re.search(r"(\d{8})T(\d{6})", path.name)
        captured_at = (
            time.strftime(
                "%Y-%m-%dT%H:%M:%S",
                time.strptime(stamp.group(1) + stamp.group(2), "%Y%m%d%H%M%S"),
            )
            if stamp
            else time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(path.stat().st_mtime))
        )
        entry: dict[str, str] = {
            "stem": stem,
            "file": path.relative_to(runs).as_posix(),
            "head_sha": head or "",
            "captured_at": captured_at,
        }
        if isinstance(kind, str) and kind:
            entry["kind"] = kind
        if isinstance(superseded, str) and superseded:
            entry["superseded_by"] = superseded
        claims.append(entry)
    manifest = runs / "CLAIMS.json"
    tmp = manifest.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"version": 1, "claims": claims}, indent=2) + "\n")
    tmp.replace(manifest)
    print(f"the claims registry: {len(claims)} entries indexed from {runs} → {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
