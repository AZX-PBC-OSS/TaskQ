"""THE CONTENT DOOR'S PINS — the content-verdict mismatch's conviction.

The head-stamp verifier's law checked the claims registry against HEAD
(freshness + the supersession chain) but NEVER READ THE CAPTURE'S
CONTENT. The convicted shape (the estate's REAL instances,
``wf-battery-rv4-20261011T043000Z.txt`` and
``deslop-pin-packs-20261011T042947Z.txt``): a live capture whose own
header records ``rc=1`` — the run FAILED, in the capture's own words —
while the claim greens the stem. The verifier read the stamp's SHA, not
the stamp's VERDICT: the fresher the lie, the greener the law.

THE COUNTEREXAMPLE (built below, one pin per shape): a FRESH live claim
(head_sha = HEAD — the head-stamp law's own green) whose capture's
content records failure. Three shapes, because the capture families lie
three ways:

* the rc=1 STAMP (the run-scoped writer's own header records the rc);
* the FORGED STAMP — the header says rc=0 but the pytest TAIL says
  ``1 failed, 603 passed`` (the header's word and the body's count
  disagree; the tail is the summary the humans read last);
* the ``FAILED tests/...`` short-summary line with no ``failed`` count
  at all (the summary line suppressed, the failures still named).

The pre-door verifier returned rc=0 on exactly this estate (the red
receipt is the cure's own capture); the doored verifier CONVICTS: the
content-verdict mismatch, the file, the LINE.

And the HONESTY edges, pinned both ways:

* the honest families (the rc=0 stamp + the pytest green tail, the JSON
  band payload) stay GREEN — the door never over-convicts a truth;
* the UNPARSABLE capture (no verdict shape at all — the staged-slot
  declaration, the new family) is the TYPED WARNING, not a silent pass:
  the verdict prints the warning naming the stem and the file, and the
  silence (the defect class this cure kills) is never the outcome.

Each pin runs the verifier script as a subprocess against a synthetic
runs-dir (the rv2-13 precedent: the script is the subject, its rc is
the verdict).
"""

from __future__ import annotations

import json
import subprocess  # Why: the verifier's verdict is its exit code — the script IS the subject (the rv2-13 pin's own shape).
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"


def _head() -> str:
    return subprocess.run(  # Why: fixed argv, no shell; git resolves on the dev/CI PATH exactly as the repo's own scripts invoke it.
        ["git", "rev-parse", "HEAD"],  # noqa: S607  # Why: as above — the fixed-argv git call.
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


HEAD = _head()

#: The wf-battery capture's REAL shape (the convicted instance's own
#: words): the run-scoped stamp with the rc, then the pytest tail. The
#: stamp says rc=1 — the battery FAILED in its own header.
_RC1_CAPTURE = """\
# wf-battery-rv4 at head {head} — rc=1 — captured 2026-10-11T04:30:01Z
bringing up nodes...
........................................................................ [ 95%]
............................                                             [100%]
=================================== FAILURES ===================================
E   AssertionError: F-RV2-13: the head-stamp verifier reds on the committed estate
=========================== short test summary info ============================
FAILED tests/test_wf_attack_rv2.py::test_rv2_13_the_head_stamp_law_greens_on_the_committed_tree
1 failed, 603 passed, 1 warning in 80.24s (0:01:20)
"""

#: The FORGED STAMP: the header claims rc=0, the body's pytest tail
#: records the failure count — the two verdict lines disagree, the tail
#: is the run's actual last word. (The count is the ONLY failure shape
#: here: this pin isolates the tail-count parse from the FAILED-line
#: shape, which gets its own pin below.)
_FORGED_STAMP_CAPTURE = """\
# deslop-pin-packs at head {head} — rc=0 — captured 2026-10-11T04:29:47Z
bringing up nodes...
..F.                                                                     [100%]
=================================== FAILURES ===================================
E   AssertionError: F-RV2-13: the head-stamp verifier reds on the committed estate
1 failed, 75 passed in 7.41s
"""

#: The SUPPRESSED-COUNT shape: no ``failed`` count in the summary line,
#: but the short-summary still names the FAILED test — the quiet tail.
_FAILED_LINE_CAPTURE = """\
# lint-format at head {head} — rc=0 — captured 2026-10-11T04:28:38Z
All checks passed!
=========================== short test summary info ============================
FAILED tests/test_some_pin.py::test_the_contract
75 passed in 7.41s
"""

#: The honest family control: rc=0 + the green pytest tail.
_GREEN_CAPTURE = """\
# wf-battery-rv4 at head {head} — rc=0 — captured 2026-10-11T05:30:01Z
bringing up nodes...
........................................................................ [100%]
604 passed, 1 warning in 80.24s (0:01:20)
"""

#: The staged-slot declaration (the drain's honest intermediate): NO
#: verdict shape at all — neither rc= nor a passed/failed summary.
_SLOT_CAPTURE = """\
# wf-battery-rv4's slot staged at head {head} — the green run's output fills this file in place (the estate-cures drain's own shape); no verdict recorded yet
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


def _verify(runs: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [sys.executable, str(_VERIFIER), "--runs-dir", str(runs)],
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )


def _e(stem: str, file: str, ts: str) -> dict[str, str]:
    """A FRESH live claim — head_sha = HEAD, the head-stamp law's own
    green. Whatever convicts this capture can only be the CONTENT DOOR."""
    return {"stem": stem, "file": file, "head_sha": HEAD, "captured_at": ts}


def test_the_rc1_stamp_convicts(tmp_path: Path) -> None:
    """THE COUNTEREXAMPLE (the estate's real instance): a FRESH live
    claim whose capture's own stamp records rc=1. The head-stamp law
    greened exactly this (the sha is fresh); the content door convicts
    the mismatch — named, with the file and the line."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery-rv4", "wf-battery-rv4-cap.txt", "t0")],
        {"wf-battery-rv4-cap.txt": _RC1_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, (
        "the rc=1 stamp under a green claim must red the verifier:\n" + proc.stdout + proc.stderr
    )
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout
    assert "wf-battery-rv4-cap.txt" in proc.stdout  # the file, named
    assert "rc=1" in proc.stdout  # the capture's own failing word, quoted


def test_the_forged_stamp_convicts(tmp_path: Path) -> None:
    """THE FORGED STAMP: the header says rc=0, the pytest tail says
    ``1 failed, 75 passed``. The stamp's word and the body's count
    disagree — the tail is the run's last word. The door reads BOTH and
    convicts on the tail."""
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
    """THE SUPPRESSED COUNT: no ``failed`` number in the summary line —
    but the short-summary still names a FAILED test. The failures are
    named in the capture's own words; the door convicts."""
    runs = _estate(
        tmp_path,
        [_e("lint-format", "lint-cap.txt", "t0")],
        {"lint-cap.txt": _FAILED_LINE_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the FAILED short-summary line must convict"
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout


def test_the_honest_green_family_stays_green(tmp_path: Path) -> None:
    """THE CONTROL: rc=0 in the stamp + the green pytest tail — the
    honest capture under an honest claim stays GREEN, no warning, no
    conviction (the over-conviction would red every honest re-stamp)."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery-rv4", "green-cap.txt", "t0")],
        {"green-cap.txt": _GREEN_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 0, (
        "the honest green family must stay green:\n" + proc.stdout + proc.stderr
    )
    assert "CONTENT-VERDICT MISMATCH" not in proc.stdout
    assert "WARNING" not in proc.stdout  # the parsable family is not warned about


def test_the_json_band_family_stays_green(tmp_path: Path) -> None:
    """THE JSON BAND FAMILY: the band payloads record MEASUREMENTS, not
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
    """THE HONESTY EDGE: a capture with NO verdict shape (the staged
    slot's declaration) is not silently passed — the verifier prints the
    TYPED WARNING naming the stem and the file, and greens (the slot is
    the drain's honest intermediate, not a lie). Silence is the defect
    class this cure kills."""
    runs = _estate(
        tmp_path,
        [_e("wf-battery-rv4", "slot-cap.txt", "t0")],
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
    """THE DOOR APPLIES WHERE THE CHAIN LANDS: a resolving supersession
    chain whose TERMINAL is fresh and on disk — but whose capture's
    content records failure. The chain cure greens the terminal's
    freshness; the content door convicts its words."""
    claims = [
        {
            "stem": "drill",
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
        {"drill-a.txt": "# the drill capture\n", "terminal.txt": _RC1_CAPTURE.format(head=HEAD)},
    )
    proc = _verify(runs)
    assert proc.returncode == 1, "the chain's terminal must pass the content door too"
    assert "CONTENT-VERDICT MISMATCH" in proc.stdout
    assert "terminal.txt" in proc.stdout
