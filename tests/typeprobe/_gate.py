"""T01's type-probe gate: every MUST_ERROR marker reds on BOTH pinned checkers.

The probe corpus (``attack_wf_negative_types.py``) names the wrong-shape
calls the typed-doors law (BUILD-PROTOCOL §7b: "the negative probes
(wrong-shape inputs RED on both checkers) ship WITH the API") requires to
be checker errors. This gate is the enforcement: it runs the corpus under
the PINNED checkers (``pyright 1.1.414`` + ``ty 0.0.85`` — the typeprobe
dependency group) and fails unless every MUST_ERROR marker line produces
at least one ERROR diagnostic from EACH checker.

pyright runs under this directory's own ``pyrightconfig.json`` — the root
pyproject's ``tests`` executionEnvironment sets ``reportArgumentType =
false``, which would mute exactly the violations the probes assert (the
T01 finding this config exists to fix).

Run: ``uv run --no-sync python tests/typeprobe/_gate.py`` (the CI
``type-probes`` job's single step).
"""

from __future__ import annotations

import json
import subprocess  # Why: the gate IS the checker invocation; fixed argv, the repo's own probe corpus.
import sys
from pathlib import Path

#: The pinned checkers (the ``typeprobe`` dependency group; a checker
#: release moves the corpus's reds under the gate's feet — the pins are
#: the review).
PYRIGHT_VERSION = "1.1.414"
TY_VERSION = "0.0.85"

_PROBE = Path(__file__).parent / "attack_wf_negative_types.py"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROBE_REL = _PROBE.relative_to(_REPO_ROOT)


def _must_error_lines() -> list[int]:
    import ast

    source = _PROBE.read_text()
    tree = ast.parse(source)
    # The module docstring's range is EXCLUDED: it NAMES the convention
    # ("Each ``MUST_ERROR`` marker names...") — prose, never an asserted
    # line. The marker must be a TRAILING comment on a CODE line.
    doc_end = 0
    if (
        tree.body
        and isinstance(tree.body[0], ast.Expr)
        and isinstance(tree.body[0].value, ast.Constant)
        and isinstance(tree.body[0].value.value, str)
    ):
        doc_end = tree.body[0].end_lineno or 0
    return [
        i + 1
        for i, line in enumerate(source.splitlines())
        if i + 1 > doc_end and "MUST_ERROR" in line and not line.strip().startswith("#")
    ]


def _pyright_errors() -> dict[int, set[str]]:
    out = subprocess.run(
        [
            "pyright",
            "--version",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert PYRIGHT_VERSION in out, f"pyright must be pinned at {PYRIGHT_VERSION}, got {out!r}"
    proc = (
        subprocess.run(  # Why: fixed argv; the --outputjson machine format is the gate's contract.
            ["pyright", "--outputjson", "--project", str(_PROBE.parent), str(_PROBE)],
            capture_output=True,
            text=True,
            check=False,  # a non-zero exit is EXPECTED (the corpus must red)
            timeout=300,
        )
    )
    data = json.loads(proc.stdout)
    errors: dict[int, set[str]] = {}
    for diag in data.get("generalDiagnostics", []):
        if diag.get("severity") == "error":
            line = int(diag["range"]["start"]["line"]) + 1
            errors.setdefault(line, set()).add(str(diag.get("rule", "?")))
    return errors


def _ty_errors() -> dict[int, set[str]]:
    out = subprocess.run(
        ["ty", "--version"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert TY_VERSION in out, f"ty must be pinned at {TY_VERSION}, got {out!r}"
    proc = subprocess.run(
        ["ty", "check", str(_PROBE_REL)],
        capture_output=True,
        text=True,
        check=False,  # a non-zero exit is EXPECTED (the corpus must red)
        timeout=300,
        cwd=_REPO_ROOT,
    )
    errors: dict[int, set[str]] = {}
    pending_rule: str | None = None
    for line in proc.stdout.splitlines():
        # ty's diagnostic is TWO lines: 'error[rule]: message' then
        # '  --> path:line:col: ...' (the checker's human format, stable
        # across 0.0.x; the machine format has no stable flag yet).
        if line.lstrip().startswith("error[") and ":" in line:
            pending_rule = line.split("error[", 1)[1].split("]", 1)[0]
            continue
        stripped = line.strip()
        if pending_rule is not None and stripped.startswith("--> "):
            location = stripped[len("--> ") :]
            parts = location.split(":")
            if len(parts) >= 2 and parts[-2].isdigit():
                errors.setdefault(int(parts[-2]), set()).add(pending_rule)
            pending_rule = None
    return errors


def main() -> int:
    musts = _must_error_lines()
    assert musts, "the probe corpus carries no MUST_ERROR markers — the gate cannot fail"
    pyright_errors = _pyright_errors()
    ty_errors = _ty_errors()
    failures: list[str] = []
    for line in musts:
        if not pyright_errors.get(line):
            failures.append(
                f"line {line}: pyright {PYRIGHT_VERSION} did NOT flag the MUST_ERROR probe (the Any leak ships)"
            )
        if not ty_errors.get(line):
            failures.append(
                f"line {line}: ty {TY_VERSION} did NOT flag the MUST_ERROR probe (the Any leak ships)"
            )
    for line, rules in sorted(pyright_errors.items()):
        if line not in musts:
            print(
                f"  note: pyright flags line {line} ({', '.join(sorted(rules))}) — not a MUST_ERROR marker (informational)"
            )
    if failures:
        print("THE TYPE GATE REDS — a typed door leaks:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(
        f"the type gate holds: every MUST_ERROR marker ({len(musts)}) reds on "
        f"pyright {PYRIGHT_VERSION} AND ty {TY_VERSION}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
