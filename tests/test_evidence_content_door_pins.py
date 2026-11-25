"""The content door's pins: a live claim's capture content must agree
with the claim's verdict.

The head-stamp verifier checked the claims registry against HEAD
(freshness + the supersession chain) but never read the capture's
CONTENT. The rejected shape: a live capture whose own
header records ``rc=1`` — the run FAILED, in the capture's own words —
while the claim greens the stem. The verifier read the stamp's SHA, not
the stamp's verdict: the fresher the lie, the greener the law.

The counterexample (built below, one pin per shape): a fresh live claim
(head_sha = HEAD) whose capture's content records failure. Three
shapes, because the capture families lie three ways:

* the rc=1 STAMP (the run-scoped writer's own header records the rc);
* the FORGED STAMP — the header says rc=0 but the pytest TAIL says
  ``1 failed, 603 passed`` (the header's word and the body's count
  disagree; the tail is the summary the humans read last);
* the ``FAILED tests/...`` short-summary line with no ``failed`` count
  at all (the summary line suppressed, the failures still named).

The pre-door verifier returned rc=0 on exactly this estate; the doored
verifier REJECTS the content-verdict mismatch, naming the file and the
LINE.

And the honesty edges, pinned both ways:

* the honest families (the rc=0 stamp + the pytest green tail, the JSON
  band payload) stay GREEN — the door never over-rejects a truth;
* the UNPARSABLE capture (no verdict shape at all — the staged-slot
  declaration, the new family) is the TYPED WARNING, not a silent pass:
  the verdict prints the warning naming the stem and the file, and the
  silence (the defect class this fix kills) is never the outcome.

Each pin runs the verifier script as a subprocess against a synthetic
runs-dir (the precedent: the script is the subject, its rc is
the verdict).
"""

from __future__ import annotations

import json
import os
import subprocess  # Why: the verifier's verdict is its exit code — the script is the subject (the pin's own shape).
import sys
from pathlib import Path

import pytest

from tests._wf_fixtures import stale_head_sha

_REPO = Path(__file__).resolve().parent.parent
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"


def _head() -> str:
    return subprocess.run(  # Why: fixed argv, no shell; git resolves on the dev/CI path exactly as the repo's own scripts invoke it.
        ["git", "rev-parse", "HEAD"],  # noqa: S607  # Why: as above — the fixed-argv git call.
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


HEAD = _head()

#: The wf-battery capture's real shape (the rejected instance's own
#: words): the run-scoped stamp with the rc, then the pytest tail. The
#: stamp says rc=1 — the battery failed in its own header.
_RC1_CAPTURE = """\
# wf-battery at head {head} — rc=1 — captured 2026-10-11T04:30:01Z
bringing up nodes...
........................................................................ [ 95%]
............................                                             [100%]
=================================== FAILURES ===================================
E   AssertionError: the head-stamp verifier reds on the committed estate
=========================== short test summary info ============================
FAILED tests/test_wf_run_view_pins.py::test_13_the_head_stamp_law_greens_on_the_committed_tree
1 failed, 603 passed, 1 warning in 80.24s (0:01:20)
"""

#: The forged stamp: the header claims rc=0, the body's pytest tail
#: records the failure count — the two verdict lines disagree, the tail
#: is the run's actual last word. (The count is the only failure shape
#: here: this pin isolates the tail-count parse from the failed-line
#: shape, which gets its own pin below.)
_FORGED_STAMP_CAPTURE = """\
# deslop-pin-packs at head {head} — rc=0 — captured 2026-10-11T04:29:47Z
bringing up nodes...
..F.                                                                     [100%]
=================================== FAILURES ===================================
E   AssertionError: the head-stamp verifier reds on the committed estate
1 failed, 75 passed in 7.41s
"""

#: The suppressed-count shape: no ``failed`` count in the summary line,
#: but the short-summary still names the failed test — the quiet tail.
_FAILED_LINE_CAPTURE = """\
# lint-format at head {head} — rc=0 — captured 2026-10-11T04:28:38Z
All checks passed!
=========================== short test summary info ============================
FAILED tests/test_some_pin.py::test_the_contract
75 passed in 7.41s
"""

#: The honest family control: rc=0 + the green pytest tail.
_GREEN_CAPTURE = """\
# wf-battery at head {head} — rc=0 — captured 2026-10-11T05:30:01Z
bringing up nodes...
........................................................................ [100%]
604 passed, 1 warning in 80.24s (0:01:20)
"""

#: The staged-slot declaration (the drain's honest intermediate): no
#: verdict shape at all — neither rc= nor a passed/failed summary.
_SLOT_CAPTURE = """\
# wf-battery-the pack's slot staged at head {head} — the green run's output fills this file in place (the estate-fixes drain's own shape); no verdict recorded yet
"""


def _estate(tmp_path: Path, claims: list[dict[str, str]], bodies: dict[str, str]) -> Path:
    """Write the synthetic registry: *claims* is the manifest's array;
    *bodies* maps capture filenames to their FULL content (the content
    door reads what the writer wrote — no fake-on-disk drift)."""
    runs = tmp_path / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    for name, body in bodies.items():
        (runs / name).write_text(body)
    (runs / "CLAIMS.json").write_text(json.dumps({"version": 1, "claims": claims}))
    return runs


def _verify(
    runs: Path, *, env_extra: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    # The ambient GITHUB_EVENT_PATH is neutralized (the control: a CI
    # pull_request payload must not forge the verifier's head — the
    # synthetic estate's verdict belongs to THIS checkout's HEAD alone).
    # *env_extra* is the proving leg's seam: a pin hands the ambient face
    # back in to show the payload really is the refusal it neutralizes.
    env = {**os.environ, "GITHUB_EVENT_PATH": "", **(env_extra or {})}
    return subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [sys.executable, str(_VERIFIER), "--runs-dir", str(runs)],
        cwd=_REPO,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _e(stem: str, file: str, ts: str) -> dict[str, str]:
    """A FRESH live claim — head_sha = HEAD, the head-stamp law's own
    green. Whatever rejects this capture can only be the CONTENT DOOR."""
    return {"stem": stem, "file": file, "head_sha": HEAD, "captured_at": ts}


def test_the_rc1_stamp_convicts(tmp_path: Path) -> None:
    """The counterexample (the estate's real instance): a FRESH live
    claim whose capture's own stamp records rc=1. The head-stamp law
    greened exactly this (the sha is fresh); the content door rejects
    the mismatch — named, with the file and the line."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery", "wf-battery-cap.txt", "t0")],
        {"wf-battery-cap.txt": _RC1_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, (
        "the rc=1 stamp under a green claim must red the verifier:\n" + proc.stdout + proc.stderr
    )
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout
    assert "wf-battery-cap.txt" in proc.stdout  # the file, named
    assert "rc=1" in proc.stdout  # the capture's own failing word, quoted


def test_the_forged_stamp_convicts(tmp_path: Path) -> None:
    """The forged stamp: the header says rc=0, the pytest tail says
    ``1 failed, 75 passed``. The stamp's word and the body's count
    disagree — the tail is the run's last word. The door reads both and
    rejects on the tail."""
    runs = _estate(
        tmp_path,
        [_e("deslop-pin-packs", "deslop-cap.txt", "t0")],
        {"deslop-cap.txt": _FORGED_STAMP_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the forged rc=0 stamp must not green the estate"
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout
    assert "1 failed" in proc.stdout  # the tail's failing count, quoted


def test_the_failed_short_summary_convicts(tmp_path: Path) -> None:
    """The suppressed count: no ``failed`` number in the summary line —
    but the short-summary still names a FAILED test. The failures are
    named in the capture's own words; the door rejects."""
    runs = _estate(
        tmp_path,
        [_e("lint-format", "lint-cap.txt", "t0")],
        {"lint-cap.txt": _FAILED_LINE_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the FAILED short-summary line must reject"
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout


def test_the_honest_green_family_stays_green(tmp_path: Path) -> None:
    """The control: rc=0 in the stamp + the green pytest tail — the
    honest capture under an honest claim stays green, no warning, no
    finding (an over-recorded finding would red every honest re-stamp)."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery", "green-cap.txt", "t0")],
        {"green-cap.txt": _GREEN_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "the honest green family must stay green:\n" + proc.stdout + proc.stderr
    )
    assert "CONTENT-VERDICT MISMATCH" not in proc.stdout
    assert "WARNING" not in proc.stdout  # the parsable family is not warned about


def test_the_json_band_family_stays_green(tmp_path: Path) -> None:
    """The JSON band family: the band payloads record MEASUREMENTS, not
    verdicts — the honest shape loads as JSON and greens, no unparsable
    warning (the door knows the estate's actual families)."""
    band = {
        "shape": {"fleet_rows": 80040, "runs": 40},
        "p50_ms": 0.503,
        "recorded_band_ms": 10.0,
    }
    runs = _estate(
        tmp_path,
        [_e("wf-rollup-band", "wf-rollup-band-cap.json", "t0")],
        {"wf-rollup-band-cap.json": json.dumps(band, indent=2)},
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "the JSON band family must stay green:\n" + proc.stdout + proc.stderr
    )
    assert "WARNING" not in proc.stdout  # the JSON family parses — no warning


def test_the_unparsable_capture_is_the_typed_warning(tmp_path: Path) -> None:
    """The honesty edge: a capture with no verdict shape (the staged
    slot's declaration) is not silently passed — the verifier prints the
    TYPED WARNING naming the stem and the file, and greens (the slot is
    the drain's honest intermediate, not a lie). Silence is the defect
    class this fix kills."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery", "slot-cap.txt", "t0")],
        {"slot-cap.txt": _SLOT_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "the unparsable capture warns; it does not red:\n" + proc.stdout + proc.stderr
    )
    assert "WARNING" in proc.stdout, "the unparsable capture must be warned about, never silent"
    assert "slot-cap.txt" in proc.stdout  # the file, named in the warning
    assert "unparsable" in proc.stdout  # the warning's type


def test_the_unparsable_json_is_the_typed_warning(tmp_path: Path) -> None:
    """A .json capture that does not LOAD as JSON is the typed warning
    too — the family declared by the suffix failed its own shape's
    parse; the warning names it (never a silent pass)."""
    runs = _estate(
        tmp_path,
        [_e("wf-rollup-band", "torn-band.json", "t0")],
        {"torn-band.json": "{not json at all"},
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "the torn JSON warns; it does not red:\n" + proc.stdout + proc.stderr
    )
    assert "WARNING" in proc.stdout
    assert "torn-band.json" in proc.stdout


def test_the_chain_terminal_content_door(tmp_path: Path) -> None:
    """The door applies where the chain lands: a resolving supersession
    chain whose TERMINAL is fresh and on disk — but whose capture's
    content records failure. The chain fix greens the terminal's
    freshness; the content door rejects its words."""
    claims = [
        {
            "stem": "scenario",
            "file": "drill-a.txt",
            "head_sha": HEAD,
            "captured_at": "t0",
            "superseded_by": "runs/terminal.txt",
        },
        {"stem": "terminal", "file": "terminal.txt", "head_sha": HEAD, "captured_at": "t1"},
    ]
    runs = _estate(
        tmp_path,
        claims,
        {"drill-a.txt": "# the scenario capture\n", "terminal.txt": _RC1_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the chain's terminal must pass the content door too"
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout
    assert "terminal.txt" in proc.stdout


def test_verify_ignores_ambient_github_event_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Synthetic estate verification must not inherit an ambient CI
    ``GITHUB_EVENT_PATH`` whose pull-request head trails HEAD with source
    changes: the verifier's env cross-check would refuse the run before
    reading ``--runs-dir``. The pin proves that premise itself first — the
    ambient payload, handed to the child unneutralized, IS the refusal —
    so a reshaped payload or a degenerate history reds here loudly instead
    of quietly emptying the guard this module's repair exists for."""
    stale_sha = stale_head_sha()
    if stale_sha is None:
        pytest.skip(
            "this checkout's history cannot supply a stale ancestor head (the "
            "root IS HEAD — a single-commit/shallow clone): the refusal premise "
            "needs >=2 commits, and the full-history lanes run it"
        )
    event_file = tmp_path / "pr-event.json"
    event_file.write_text(json.dumps({"pull_request": {"head": {"sha": stale_sha}}}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    runs = _estate(
        tmp_path,
        [_e("wf-battery", "green-cap.txt", "t0")],
        {"green-cap.txt": _GREEN_CAPTURE.format(head=HEAD)},
    )
    # The premise, proved in-pin: with the ambient face NOT neutralized the
    # payload reaches the cross-check and the run is refused — the shape
    # the neutralization below is the fix for.
    refused = _verify(runs, env_extra={"GITHUB_EVENT_PATH": str(event_file)})
    assert refused.returncode == 1 and "THE ENV-FORGED HEAD" in refused.stdout, (
        "the ambient pull-request payload must be the env cross-check's refusal "
        "before this pin can assert the neutralization — the sha must be a real "
        "ancestor with behavioral changes since. The output:\n" + refused.stdout + refused.stderr
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "ambient GITHUB_EVENT_PATH must not fail synthetic estate verification:\n"
        + proc.stdout
        + proc.stderr
    )
    assert "THE ENV-FORGED HEAD" not in proc.stdout
    assert "the head-stamp law holds" in proc.stdout
