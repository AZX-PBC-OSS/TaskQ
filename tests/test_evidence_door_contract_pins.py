"""The estate pack: the content door's own holes.

The verifier under pin is ``scripts/verify_evidence_heads.py`` — the
content door whose own scan is the subject. Each pin's rejected shape
was reproduced live against the recorded defect before its fix landed;
the assertions stand as the permanent green guards.

The method: every pin builds a SYNTHETIC ESTATE in
a tmp dir (a ``runs/`` with a hand-written ``CLAIMS.json`` + the tampered
capture) and runs the verifier as a subprocess against it
(``--runs-dir``). The scan-window, JSON and future-dated families'
claims are FRESH (``head_sha`` = the worktree's own ``git rev-parse
HEAD``) so the head-stamp law holds and only the door leg under pin can
judge; the EMPTY REGISTRY family carries no claim at all, and the
ENV-FORGED HEAD family is the deliberate exception — its claim carries
the event's own sha, so the head-stamp leg IS the leg under pin there.
Its ACCEPTING arms need a history the cross-check can accept, so those
build a throwaway GIT repo in the tmp dir with the verifier copied
BYTE-IDENTICAL into it (its ``REPO`` is its own home, so the head, the
ancestry walk and the diff resolve to the synthetic history).
The verifier's ``REPO`` is its own parent — the working tree's residue
never enters.

The pin families (every estate below was a shape the pre-door verifier
greened — each now exits rc=1, the rejection its pin asserts):

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
* the ENV-FORGED HEAD, both sides: the verified head is the REPO'S own
  head (``git rev-parse HEAD``), env-influenceable never; the CI env
  face survives only under the cross-check against the repo's own
  history — neither a sha the repo cannot name nor a valid ancestor
  whose diff carries behavioral changes ever judges the estate, and
  there is no merge-ref escape — while a source-identical ancestor (the
  branch up to date with its base) and the push-event payload the
  production step always carries still let the estate be JUDGED. The
  cross-check is two-sided: refusing the first two shapes while
  accepting the second two is the law, and a verifier that refuses
  every env sha is a blanket ban the accepting pin reds.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests._wf_fixtures import stale_head_sha

_REPO = Path(__file__).resolve().parents[1]
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"

#: A sha that exists nowhere in the repo (the forgery).
_FORGED_SHA = "f" * 40

#: A valid ancestor with behavioral source changes since: the pull-request
#: head behind main that a merge-ref escape would wrongly trust. ``None``
#: where this checkout cannot supply the shape (a single-commit shallow
#: clone has the root AT HEAD) — the stale arm then SKIPS rather than
#: false-failing: see ``tests._wf_fixtures.stale_head_sha``.
_STALE_SHA = stale_head_sha()

#: The stale arm's skip: the pin's premise is this checkout's history, and
#: a checkout that cannot offer a stale ancestor must say so rather than
#: assert a vacuous verdict (its sibling arm still runs).
_SKIP_WITHOUT_STALE_HEAD = pytest.mark.skipif(
    _STALE_SHA is None,
    reason=(
        "this checkout's history cannot supply a stale ancestor head (the root "
        "IS HEAD — a single-commit/shallow clone): the cross-check's refusal "
        "premise needs >=2 commits, and the full-history lanes run it"
    ),
)


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


@pytest.mark.parametrize(
    "event_sha",
    [
        pytest.param(_FORGED_SHA, id="unresolvable-forgery"),
        pytest.param(
            _STALE_SHA or _FORGED_SHA,
            id="valid-stale-pr-head",
            marks=_SKIP_WITHOUT_STALE_HEAD,
        ),
    ],
)
def test_d5_the_verified_head_is_the_repos_own(tmp_path: Path, event_sha: str) -> None:
    """The verified head comes from the REPO (``git rev-parse HEAD``),
    env-influenceable never: the CI override is cross-checked against the
    repo's own history. The estate carries a green capture stamped with
    the event's sha, so trusting either an unresolvable forgery or a valid
    stale PR head would green through the merge-ref escape. The env face
    is never trusted or silently ignored: both shapes are actively refused
    before the estate can be judged."""
    runs_dir = tmp_path / "runs"
    _write_estate(
        runs_dir,
        [_claim("d5-stem", "d5.txt", event_sha, "2026-10-11T00:00:00")],
        {"d5.txt": "run-scoped stamp: rc=0\n38 passed in 1.0s\n"},
    )
    payload = runs_dir / "forged-event.json"
    payload.write_text(json.dumps({"pull_request": {"head": {"sha": event_sha}}}))
    output = _run_verifier(runs_dir, env_extra={"GITHUB_EVENT_PATH": str(payload)})
    assert output.startswith("rc=1\n"), output
    assert "THE ENV-FORGED HEAD" in output, (
        "The verifier must actively refuse the forged env head; merely ignoring "
        "the event and rejecting the stale claim would remove the cross-check. "
        "The output:\n" + output
    )
    assert event_sha[:12] in output
    assert "the head-stamp law holds" not in output, (
        "The verifier judged the estate against the env head "
        f"{event_sha[:12]}… (the event payload's say-so) and GREENED — the "
        "repo's own HEAD was never consulted. The output:\n" + output
    )


# ── the env-forged head: the accepting arm ─────────────────────────


def _git(repo: Path, *args: str) -> str:
    """The throwaway repo's git face, hermetic (a machine's
    ``commit.gpgsign``, hooks path or proxy config must never reach the
    pins' commits — the verdict is the history's, not the machine's)."""
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; git resolves on the dev/CI path exactly as the repo's own scripts invoke it.
        ["git", *args],  # noqa: S607  # Why: as above — the fixed-argv git call.
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
        env={
            **os.environ,
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
        },
    ).stdout.strip()


def _commit_all(repo: Path, message: str) -> str:
    """Stage everything and land the commit; returns its sha."""
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _init_cross_check_repo(tmp_path: Path) -> tuple[Path, str, str]:
    """The throwaway repo the accepting arm needs: the verifier copied
    BYTE-IDENTICAL into it (its ``REPO`` is its own home, so the head, the
    ancestry walk and the diff all resolve to THIS synthetic history,
    never this checkout). History: the source-final BASE, a BEHAVIORAL
    commit (``src/`` — the PR's own change), then a docs + measurement
    commit. Returns ``(repo, behavioral_sha, head)``."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(_VERIFIER, repo / "scripts" / "verify_evidence_heads.py")
    (repo / "src" / "taskq").mkdir(parents=True)
    (repo / "src" / "taskq" / "__init__.py").write_text('"""the package under claim."""\n')
    (repo / "src" / "taskq" / "engine.py").write_text("SIGNAL = 1\n")
    (repo / "docs").mkdir()
    (repo / "docs" / "guide.md").write_text("# the guide\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "the pins")
    _git(repo, "config", "user.email", "pins@localhost")
    _git(repo, "config", "commit.gpgsign", "false")
    _commit_all(repo, "the source-final base")
    (repo / "src" / "taskq" / "engine.py").write_text("SIGNAL = 2\n")
    behavioral = _commit_all(repo, "a behavioral commit (the PR's own change)")
    _write_estate(
        repo / ".measurements" / "runs",
        [
            _claim(
                "d5-cross-check-stem",
                "d5-cross-check.txt",
                behavioral,
                "2026-10-11T00:00:00",
            )
        ],
        {"d5-cross-check.txt": "run-scoped stamp: rc=0\n38 passed in 1.0s\n"},
    )
    (repo / "docs" / "guide.md").write_text("# the guide, extended\n")
    _commit_all(repo, "docs only + the measurement commit")
    return repo, behavioral, _git(repo, "rev-parse", "HEAD")


def _run_verifier_copy(repo: Path, *, env_extra: dict[str, str] | None = None) -> str:
    """The verifier COPIED into the throwaway repo, cwd = that repo and the
    estate passed explicitly. Returns the combined output in
    ``_run_verifier``'s shape; the ambient ``GITHUB_EVENT_PATH`` is
    neutralized unless the leg under test sets it."""
    env = {
        **os.environ,
        "PYTHONPATH": str(_REPO / "src"),
        "GITHUB_EVENT_PATH": "",
        **(env_extra or {}),
    }
    proc = subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier copied byte-identical; fixed argv, no shell.
        [
            sys.executable,
            str(repo / "scripts" / "verify_evidence_heads.py"),
            "--runs-dir",
            str(repo / ".measurements" / "runs"),
        ],
        cwd=repo,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return f"rc={proc.returncode}\n{proc.stdout}{proc.stderr}"


def _event_payload(repo: Path, body: dict[str, object]) -> str:
    """Write *body* as the repo's event file and return its path — the CI
    event face, built by hand."""
    payload = repo / "event.json"
    payload.write_text(json.dumps(body))
    return str(payload)


def test_d5_the_env_cross_check_accepts_a_source_identical_pr_head(tmp_path: Path) -> None:
    """The cross-check's ACCEPTING arm — the law's other side: the
    payload's head is an ancestor of the checkout whose diff carries only
    docs and the measurement estate (the shape a real PR run produces when
    the branch is up to date with its base), so the estate — its claim
    stamped on that head — must be JUDGED, never refused. A verifier that
    refused every env sha (a blanket ban of the CI PR-event face) reds
    here and stays green everywhere else in the pack."""
    repo, behavioral, head = _init_cross_check_repo(tmp_path)
    output = _run_verifier_copy(
        repo,
        env_extra={
            "GITHUB_EVENT_PATH": _event_payload(
                repo, {"pull_request": {"head": {"sha": behavioral}}}
            )
        },
    )
    assert "THE ENV-FORGED HEAD" not in output, (
        "the event head is source-identical to the checkout (docs + "
        "measurements only since it), so the cross-check must accept it — a "
        "refusal here is a blanket ban of the CI PR-event face, not a "
        "cross-check. The output:\n" + output
    )
    assert output.startswith("rc=0\n") and "the head-stamp law holds" in output, (
        f"the accepted env head {behavioral[:12]} must leave the estate judged "
        f"against the checkout's own head {head[:12]}. The output:\n" + output
    )
    assert head[:12] in output  # the verdict's head is the REPO's own


def test_d5_the_env_cross_check_refuses_a_behavioral_ancestor(tmp_path: Path) -> None:
    """The accepting arm's CONTROL: the same estate, the payload naming the
    BASE — an ancestor whose diff carries ``src/`` — is the refusal.
    Without it the accepting arm would be vacuous (a verifier that simply
    ignored the variable would green it too)."""
    repo, _, _ = _init_cross_check_repo(tmp_path)
    base = _git(repo, "rev-list", "--max-parents=0", "HEAD")
    output = _run_verifier_copy(
        repo,
        env_extra={
            "GITHUB_EVENT_PATH": _event_payload(repo, {"pull_request": {"head": {"sha": base}}})
        },
    )
    assert output.startswith("rc=1\n"), output
    assert "THE ENV-FORGED HEAD" in output, (
        "an env head whose diff from the checkout carries behavioral changes "
        "must be refused, never judged. The output:\n" + output
    )
    assert "the head-stamp law holds" not in output


def test_d5_a_push_event_payload_leaves_the_estate_judged(tmp_path: Path) -> None:
    """The PRODUCTION shape: the one workflow step that runs the verifier
    (``.github/workflows/ci.yaml``, "The head-stamp law") is gated to a
    push to ``main``, and GitHub always writes an event file for a push —
    a body with no ``pull_request`` key. That run must judge the estate,
    not refuse it; every other pin in the pack neutralizes the variable to
    ``""``, so nothing else holds this shape."""
    repo, _, head = _init_cross_check_repo(tmp_path)
    output = _run_verifier_copy(
        repo,
        env_extra={
            "GITHUB_EVENT_PATH": _event_payload(
                repo, {"ref": "refs/heads/main", "after": head, "commits": []}
            )
        },
    )
    assert output.startswith("rc=0\n") and "the head-stamp law holds" in output, (
        "a push-event payload is not a PR face — the estate must be judged "
        "against the checkout's own head, not refused. The output:\n" + output
    )
