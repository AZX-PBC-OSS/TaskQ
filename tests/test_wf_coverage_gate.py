"""THE COVERAGE GATE'S BEHAVIORAL FACE (the de-slop round, cure 1's
residue): the scoped-coverage run EXECUTES the wf pin files — measured
off the target's OWN command, never against a second hand list.

THE CONVICTION THAT KILLED THE OLD GUARD: ``test_wf_coverage_gate_
wiring.py`` sniffed STRUCTURE — it re-parsed the Makefile's test list
and diffed it against the directory walk. Two hand lists (or a hand list
vs a walk) drift by construction; a guard pinning one list to another is
itself the coupling it polices. THE CURE: the Makefile's ``test-wf-cov``
list IS the walk now (``$(shell ls tests/test_wf_*.py)``) — a pin file
that exists is wired, structurally, with nothing to assert.

What remains to pin is BEHAVIOR: the gate target's measured command (a
dry ``make -n`` run — make expands the derivation, runs nothing) must
execute every walked pin file under the scoped tracer, and must run the
coverage CHECKER after the tracer (the number gets judged, not just
recorded). The pin measures what the target DOES."""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

TESTS = Path(__file__).resolve().parent
REPO = TESTS.parent


def _measured_commands() -> list[list[str]]:
    """The commands the ``test-wf-cov`` target WOULD run (``make -n``:
    full expansion, zero execution), as argv lists — the target's
    measured behavior."""
    proc = subprocess.run(  # Why: fixed argv — "make" + the target's name, no user input, no shell.
        ["make", "-n", "test-wf-cov"],  # noqa: S607  # Why: the repo's own gate invokes the make binary the same way.
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    joined = proc.stdout.replace("\\\n", " ")  # the recipe's line continuations
    commands = [shlex.split(line.strip()) for line in joined.splitlines() if line.strip()]
    assert any("pytest" in argv for argv in commands), (
        "make -n test-wf-cov runs no pytest command at all — the scoped "
        "coverage gate's run is gone from the Makefile"
    )
    return commands


def test_the_scoped_coverage_run_executes_the_pin_files() -> None:
    """THE MEASURED RESIDUE: every pytest-shaped wf pin file on disk is
    an ARGUMENT of the gate's measured pytest command — the coverage
    number counts their executions, structurally (the list is the walk:
    a new pin file is IN the run by construction, nothing to wire)."""
    walked = sorted(TESTS.glob("test_wf_*.py"))
    assert walked, "the wf pin suite is gone from tests/ — the gate has no subject"
    executed = {Path(arg).name for argv in _measured_commands() for arg in argv}
    missing = [p.name for p in walked if p.name not in executed]
    assert not missing, (
        f"the scoped-coverage run does not execute the pin file(s) {missing} — "
        "their executions are invisible to the coverage number (the gate "
        "reports 90% of a partially seen suite)"
    )


def test_the_scoped_coverage_run_checks_the_number() -> None:
    """The gate's second leg, measured: the coverage CHECKER runs after
    the tracer — the recorded number is JUDGED (the floor), never just
    printed. A run that measures without checking is a number nobody
    can fail on."""
    checker_after_pytest = False
    pytest_seen = False
    for argv in _measured_commands():
        if any(arg.endswith("check_wf_coverage.py") for arg in argv):
            checker_after_pytest = pytest_seen
        if "pytest" in argv:
            pytest_seen = True
    assert pytest_seen and checker_after_pytest, (
        "the scoped-coverage gate must run the tracer AND then check the "
        "number (scripts/check_wf_coverage.py after the pytest invocation)"
    )
