"""The estate pack: the content door's own holes.

The verifier under pin is ``scripts/verify_evidence_heads.py`` — the
content door whose own scan is the subject. Each pin's rejected shape
was reproduced live against the recorded defect before its fix landed;
the assertions stand as the permanent green guards.

The method: every pin builds a SYNTHETIC ESTATE in
a tmp dir (a ``runs/`` with a hand-written ``CLAIMS.json`` + the tampered
capture) and runs the verifier as a subprocess against it
(``--runs-dir``). Every claim is FRESH (``head_sha`` = the worktree's own
``git rev-parse HEAD``) so the head-stamp law holds and only the door leg
under pin can judge. The verifier's ``REPO`` is its own parent — the
working tree's residue never enters.

The pin families (every estate below exits rc=0 today; each was a
shape the door once greened):

* the SCAN WINDOW: ``_scan_window`` reads
  the whole capture — a red verdict (``1 failed, N passed``) sitting
  MID-FILE, pushed past a ``rc=0`` stamp by trailing chatter, is
  still rejected: the scan is verdict-shaped, never
  line-counted.
* the JSON LAUNDER: JSON captures' verdict fields
  (``rc``/``failed``/``errors``) are read like any stamp; the band
  family's verdict-free measurements stay green.
* the EMPTY REGISTRY GREENS VACUOUSLY: an empty registry is a loud
  refusal — an estate with no
  claims is unverifiable,
  per the verifier's own missing-registry law.
* FUTURE-DATED CAPTURES: ``captured_at`` (and the filename's embedded
  stamp,
  where present) must be sane — not future-dated beyond the law's
  stated tolerance (one day, in the verifier's docstring).
* the ENV-FORGED HEAD: the verified
  head is the REPO'S own head (``git rev-parse HEAD``), env-
  influenceable never; the CI env face survives only under the
  cross-check against the repo's own history (a sha the repo cannot
  name never judges the estate).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"

#: A sha that exists nowhere in the repo (the forgery).
_FORGED_SHA = "f" * 40


def _repo_head() -> str:
    """The worktree's own HEAD — the fresh claim's stamp (never
    hardcoded: the pin survives rebases)."""
    return subprocess.run(  # Why: fixed argv, no shell; the git binary resolves on the dev/CI path exactly as the repo's own scripts invoke it.
        ["git", "rev-parse", "HEAD"],  # noqa: S607  # Why: as above.
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _write_estate(runs_dir: Path, claims: list[dict[str, str]], files: dict[str, str]) -> None:
    """The synthetic estate: the captures + their CLAIMS.json (the
    manifest shape ``taskq.testing._claims`` writes — version 1, the
    append-ordered entries)."""
    runs_dir.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (runs_dir / name).write_text(content)
    (runs_dir / "CLAIMS.json").write_text(
        json.dumps({"version": 1, "claims": claims}, indent=2) + "\n"
    )


def _run_verifier(runs_dir: Path, *, env_extra: dict[str, str] | None = None) -> str:
    """The verifier as a subprocess (the 13 pin's own seam), the
    worktree's ``src`` on PYTHONPATH (the venv's editable install points
    at another tree — the import-resolution seam the pack's receipts
    document). Returns the combined output; the ambient
    ``GITHUB_EVENT_PATH`` is neutralized unless the leg under test sets
    it (the control: no leg forges by accident)."""
    env = {
        **os.environ,
        "PYTHONPATH": str(_REPO / "src"),
        "GITHUB_EVENT_PATH": "",
        **(env_extra or {}),
    }
    proc = subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [sys.executable, str(_VERIFIER), "--runs-dir", str(runs_dir)],
        cwd=_REPO,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return f"rc={proc.returncode}\n{proc.stdout}{proc.stderr}"


def _claim(stem: str, file: str, head_sha: str, captured_at: str) -> dict[str, str]:
    return {"stem": stem, "file": file, "head_sha": head_sha, "captured_at": captured_at}


# ── the scan window ────────────────────────────────────────────────


def test_d1_the_door_convicts_a_red_verdict_anywhere_in_the_capture(tmp_path: Path) -> None:
    """The window must be verdict-shaped, never line-counted: a red
    verdict ANYWHERE in the capture rejects the claim. The estate: a
    fresh claim whose capture's stamp greens (``rc=0`` in the first
    three lines) while the TRUE red verdict sits mid-file, outside the
    3+12 edge window — 20 lines of chatter ahead, 20 behind."""
    lines = [
        "run-scoped stamp: rc=0",
        "suite: wf-battery (synthetic)",
        "head: staged",
    ]
    lines += [f"chatter before {i}" for i in range(20)]
    lines.append("1 failed, 31 passed in 42.0s")  # the true red — line 24 of 44
    lines += [f"trailing chatter {i}" for i in range(20)]
    runs_dir = tmp_path / "runs"
    _write_estate(
        runs_dir,
        [_claim("d1-stem", "d1.txt", _repo_head(), "2026-10-11T00:00:00")],
        {"d1.txt": "\n".join(lines) + "\n"},
    )
    output = _run_verifier(runs_dir)
    assert "d1-stem" in output and "the head-stamp law holds" not in output, (
        "The red verdict mid-capture was never read — the door greens a "
        "capture whose own content records failure because the verdict sits "
        "outside the first-3/last-12 edge window. The output:\n" + output
    )


# ── the JSON launder ───────────────────────────────────────────────


def test_d2_a_json_capture_verdict_is_never_laundered(tmp_path: Path) -> None:
    """The verdict inside a JSON capture is READ: a ``*.json`` capture
    carrying failing verdict fields (``rc``/``failed``) is rejected,
    exactly like the text families' stamps. The capture's name is
    deliberately OUTSIDE the band family's shape (``pytest-summary-…``,
    never a ``*-band*``), so a fix declaring the measurements family
    exempt BY NAME still judges this file on its content."""
    runs_dir = tmp_path / "runs"
    _write_estate(
        runs_dir,
        [
            _claim(
                "d2-stem",
                "pytest-summary-20261011T000000Z.json",
                _repo_head(),
                "2026-10-11T00:00:00",
            )
        ],
        {"pytest-summary-20261011T000000Z.json": json.dumps({"rc": 1, "failed": 3, "passed": 28})},
    )
    output = _run_verifier(runs_dir)
    assert "d2-stem" in output and "the head-stamp law holds" not in output, (
        "The JSON capture's own verdict fields (rc=1, failed=3) were never "
        "read — a valid-JSON body greens whatever it records. The output:\n" + output
    )


# ── the empty registry greens vacuously ────────────────────────────


def test_d3_the_empty_registry_is_a_loud_refusal(tmp_path: Path) -> None:
    """An estate with no claims is UNVERIFIABLE — the verifier's own
    missing-registry law's shape extended to the empty one: the loop over
    zero stems proves nothing, so the pass is vacuous. The pin: a loud
    refusal, never "the law holds"."""
    runs_dir = tmp_path / "runs"
    _write_estate(runs_dir, [], {})
    output = _run_verifier(runs_dir)
    assert "rc=0" not in output, (
        "The empty registry GREENS — zero claims is zero verdicts, and the "
        "estate passes vacuously. The output:\n" + output
    )
    assert re.search(r"empty|no claims|zero", output, re.IGNORECASE), (
        "The refusal must NAME the empty registry (the loud law's own "
        "shape — the missing-registry message names its cause). The output:\n" + output
    )


# ── future-dated captures ──────────────────────────────────────────


def test_d4_a_future_dated_capture_is_refused(tmp_path: Path) -> None:
    """The capture's timestamp is SANE: ``captured_at`` (the registry's
    own field — ``record_claim`` always writes it) must not be
    future-dated beyond a tolerance, and the filename's embedded
    timestamp gets the same read where present. The estate: a fresh,
    green capture dated 2030 — in the registry field and in the name."""
    runs_dir = tmp_path / "runs"
    _write_estate(
        runs_dir,
        [
            _claim(
                "d4-stem",
                "d4-stem-20300101T000000Z.txt",
                _repo_head(),
                "2030-01-01T00:00:00",
            )
        ],
        {"d4-stem-20300101T000000Z.txt": "run-scoped stamp: rc=0\n38 passed in 1.0s\n"},
    )
    output = _run_verifier(runs_dir)
    assert "d4-stem" in output and "the head-stamp law holds" not in output, (
        "The capture dated 2030 greens — the registry's captured_at field "
        "(and the filename's embedded timestamp) is never parsed. The output:\n" + output
    )


# ── the env-forged head ────────────────────────────────────────────


def test_d5_the_verified_head_is_the_repos_own(tmp_path: Path) -> None:
    """The verified head comes from the REPO (``git rev-parse HEAD``),
    env-influenceable never — or the CI override is cross-checked against
    the repo's own history. The estate: a green capture stamped with the
    FORGED sha (a 40-hex string the repo cannot name), the event payload
    carrying the same forgery — today the claim is "fresh" against the
    forgery and greens. Under any fix the forgery cannot judge: env
    ignored ⇒ the claim is stale at the repo's HEAD (the forged sha is
    not an ancestor — the off-by-one rule's own red); env cross-checked ⇒
    the unresolvable sha is refused."""
    runs_dir = tmp_path / "runs"
    _write_estate(
        runs_dir,
        [_claim("d5-stem", "d5.txt", _FORGED_SHA, "2026-10-11T00:00:00")],
        {"d5.txt": "run-scoped stamp: rc=0\n38 passed in 1.0s\n"},
    )
    payload = runs_dir / "forged-event.json"
    payload.write_text(json.dumps({"pull_request": {"head": {"sha": _FORGED_SHA}}}))
    output = _run_verifier(runs_dir, env_extra={"GITHUB_EVENT_PATH": str(payload)})
    assert "the head-stamp law holds" not in output, (
        "The verifier judged the estate against the FORGED head "
        f"{_FORGED_SHA[:12]}… (the env payload's say-so) and GREENED — the "
        "repo's own HEAD was never consulted. The output:\n" + output
    )
