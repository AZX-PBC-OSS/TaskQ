"""THE COVERAGE GATE'S DIRECTORY-WALK GUARD (the rv4 estate cure — the
drift class killed at root): the scoped-coverage gate's test list lives
in the Makefile's ``test-wf-cov`` target, hand-maintained — and a
HAND-maintained list drifts: the pin files it omits are files whose
executions the coverage number never counts (the number honestly says
90% of a tree whose pin suite is partially invisible to the gate —
'record healthy, work wrong', the coverage face).

THE GUARD: the walk is the LAW, the list is derived from it — every
pytest-shaped ``tests/test_wf_*.py`` file MUST be wired into the
``test-wf-cov`` list; a test file existing but unwired is THE GATE'S
OWN RED (this pin), failing before any coverage number can be cited.
The walk is directory-derived (``tests/test_wf_*.py``), never a second
hand-list — a second list would drift exactly like the first."""

from __future__ import annotations

import re
from pathlib import Path

TESTS = Path(__file__).resolve().parent
MAKEFILE = TESTS.parent / "Makefile"


def _walked_pin_files() -> set[str]:
    """The pytest-shaped wf pin files, DIRECTORY-DERIVED (the law's
    source — never a hand list)."""
    return {f"tests/{p.name}" for p in sorted(TESTS.glob("test_wf_*.py"))}


def _wired_files() -> set[str]:
    """The files wired into the Makefile's ``test-wf-cov`` target — read
    OFF the target's body (the pytest invocation's tests/ paths)."""
    text = MAKEFILE.read_text()
    match = re.search(r"^test-wf-cov:(.*?)(?=^\S)", text, re.MULTILINE | re.DOTALL)
    assert match is not None, "the test-wf-cov target is missing from the Makefile"
    body = match.group(1)
    return set(re.findall(r"tests/[A-Za-z0-9_/.-]+\.py", body))


def test_every_wf_pin_file_is_wired_into_the_scoped_coverage_gate() -> None:
    """THE WALK GUARD: a wf pin test file existing but unwired is the
    gate's own red — the message names the file to add (the drift class
    killed at root: the list is the walk's DUTY, never an option)."""
    walked = _walked_pin_files()
    wired = _wired_files()
    omitted = sorted(walked - wired)
    assert not omitted, (
        "wf pin test file(s) EXIST but are not wired into the "
        "scoped-coverage gate (Makefile test-wf-cov) — the coverage "
        f"number does not count their executions: {omitted}. Add them to "
        "the test-wf-cov target's pytest invocation (or delete the "
        "files)."
    )


def test_the_wired_list_has_no_phantom_entries() -> None:
    """The other drift direction: a wired path that no longer EXISTS (the
    renamed/deleted file left on the list) — the gate runs a phantom and
    the list rots. Every wired tests/ path must resolve."""
    wired = _wired_files()
    phantom = sorted(w for w in wired if w != "tests/" and not (TESTS.parent / w).exists())
    assert not phantom, (
        f"wired test path(s) do not exist on disk: {phantom} — prune the "
        "test-wf-cov list (a phantom entry is the list's own rot)"
    )
