"""T01's type-probe gate: every MUST_ERROR marker reds on BOTH pinned
checkers — asserting the EXPECTED RULE-IDS per marker, and failing on any
error OUTSIDE the markers.

The probe corpus (``*_negative_types.py``) names the wrong-shape
calls the typed-doors law (BUILD-PROTOCOL §7b: "the negative probes
(wrong-shape inputs RED on both checkers) ship WITH the API") requires to
be checker errors. This gate is the enforcement: it runs the corpus under
the PINNED checkers (``pyright 1.1.414`` + ``ty 0.0.85`` — the typeprobe
dependency group) and fails unless EVERY marker line

1. produces at least one ERROR diagnostic from EACH checker whose
   RULE-ID matches the marker's declared set — ``MUST_ERROR(rule-a,
   rule-b): prose`` — (a marker that reds with the WRONG rule — a stray
   missing-import satisfying a payload probe — is a RED GATE: the
   marker without its rule is the leak wearing a pass), and
2. carries the rule-id declaration AT ALL — a bare ``MUST_ERROR`` is a
   gate failure (the assertion must name what it asserts),

AND unless the checkers emit NO error anywhere else in the corpus (the
unmarked-error rule): an error outside a marker is either a GREEN-surface
regression (the corpus's clean lines — the two-faces boundary's honest
gap — must stay clean) or an unasserted red — both are gate failures.
(The old gate printed these as informational notes and passed — the
clean-site regression's escape hatch, removed.)

pyright runs under this directory's own ``pyrightconfig.json`` — the root
pyproject's ``tests`` executionEnvironment sets ``reportArgumentType =
false``, which would mute exactly the violations the probes assert (the
T01 finding this config exists to fix).

Run: ``uv run --no-sync python tests/typeprobe/_gate.py`` (the CI
``type-probes`` job's single step).
"""

from __future__ import annotations

import json
import re  # Why: the marker's rule-id declaration is parsed from the trailing comment.
import subprocess  # Why: the gate IS the checker invocation; fixed argv, the repo's own probe corpus.
import sys
from pathlib import Path

#: The pinned checkers (the ``typeprobe`` dependency group; a checker
#: release moves the corpus's reds under the gate's feet — the pins are
#: the review).
PYRIGHT_VERSION = "1.1.414"
TY_VERSION = "0.0.85"

#: The probe CORPUS: one file per API surface round (T01's engine corpus,
#: the T09 flow API's wiring corpus, the type-mechanism round's generic
#: wiring corpus, the phase-2/-3/-4 attack rounds' surface corpora — the
#: negative probes ship WITH the API, BUILD-PROTOCOL §7b). Each
#: MUST_ERROR marker reds on BOTH checkers — with the RULE-IDS the
#: marker declares. THE EVIDENCE-INTEGRITY ROUND'S WIRING (cure 4):
#: attack2/attack3/attack4 ran only under their attackers' own
#: invocations and ROTTED OFF this gate — three of seven corpus files
#: unwired, their markers dead or wrong-rule as the signatures moved.
#: They are WIRED here, re-derived against the current signatures
#: (wire-or-delete, no zombie corpus).
_CORPUS: tuple[str, ...] = (
    "attack_wf_negative_types.py",
    "wf_api_negative_types.py",
    "wf_generic_step_negative_types.py",
    "attack2_wf_phase2_types.py",
    "attack3_wf_negative_types.py",
    "attack4_new_surfaces_negative_types.py",
)
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: The marker's rule-id declaration: ``MUST_ERROR(rule-a, rule-b)`` —
#: the rules any ONE of the checkers may be asserted against (pyright
#: and ty name the same violation differently; the marker lists BOTH
#: vocabularies and the gate requires each checker to produce ≥1 error
#: whose rule is in the set).
_MARKER_RULES = re.compile(r"MUST_ERROR\(([^)]+)\)")


def _must_error_lines(source: str) -> list[tuple[frozenset[str], frozenset[int]]]:
    """The corpus's markers → (declared rule-ids, asserted line-span).

    THE SPAN CONVENTION (the formatter's law): ruff format WRAPS long
    calls and leaves the trailing marker on the closing-paren line while
    the checker reports the violation at the argument INSIDE the
    expression — so a marker asserts its innermost AST statement's WHOLE
    span (every line of it), not the single line it happens to trail. A
    COMMENT-ONLY line is never a marker (prose names the convention
    too): the marker must TRAIL code. A marker with no rule-id
    declaration is reported by the caller as its own failure (the
    assertion must name what it asserts)."""
    import ast

    tree = ast.parse(source)
    # The module docstring's range is EXCLUDED: it NAMES the convention
    # ("Each ``MUST_ERROR`` marker names...") — prose, never an asserted
    # line. A marker must be a comment attached to CODE (trailing) or a
    # standalone comment pointing DOWN at the next statement.
    doc_end = 0
    if (
        tree.body
        and isinstance(tree.body[0], ast.Expr)
        and isinstance(tree.body[0].value, ast.Constant)
        and isinstance(tree.body[0].value.value, str)
    ):
        doc_end = tree.body[0].end_lineno or 0

    statements: list[ast.stmt] = [n for n in ast.walk(tree) if isinstance(n, ast.stmt)]

    def _innermost(lineno: int) -> ast.stmt | None:
        """The DEEPEST statement whose span contains *lineno* (1-based)."""
        best: ast.stmt | None = None
        for node in statements:
            start = node.lineno
            end = node.end_lineno or node.lineno
            if start <= lineno <= end and (
                best is None
                or (node.lineno >= best.lineno and (node.end_lineno or 0) <= (best.end_lineno or 0))
            ):
                best = node
        return best

    markers: list[tuple[frozenset[str], frozenset[int]]] = []
    for i, line in enumerate(source.splitlines()):
        lineno = i + 1
        if lineno <= doc_end or "MUST_ERROR" not in line:
            continue
        if line.strip().startswith("#"):
            # A COMMENT-ONLY line is never a marker (prose names the
            # convention too): the marker must TRAIL code — the asserted
            # statement is the one the comment's line falls inside.
            continue
        match = _MARKER_RULES.search(line)
        rules = (
            frozenset(r.strip() for r in match.group(1).replace(";", ",").split(",") if r.strip())
            if match
            else frozenset()
        )
        node = _innermost(lineno)
        if node is None:
            # A marker whose line no statement spans — keep the marker's
            # own line as the span so the gate reports it rather than
            # dropping it.
            markers.append((rules, frozenset({lineno})))
            continue
        span = frozenset(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        markers.append((rules, span))
    return markers


def _pyright_errors(probe: Path) -> dict[int, set[str]]:
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
            ["pyright", "--outputjson", "--project", str(probe.parent), str(probe)],
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
            # A syntax diagnostic carries no rule id — recorded as "?"
            # (it can never satisfy a marker's rule assertion; the
            # marker must red on the SEMANTIC rule it names).
            errors.setdefault(line, set()).add(str(diag.get("rule") or "?"))
    return errors


def _ty_errors(probe_rel: str) -> dict[int, set[str]]:
    out = subprocess.run(
        ["ty", "--version"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert TY_VERSION in out, f"ty must be pinned at {TY_VERSION}, got {out!r}"
    proc = subprocess.run(
        ["ty", "check", probe_rel],
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
    total_musts = 0
    failures: list[str] = []
    for corpus_name in _CORPUS:
        probe = Path(__file__).parent / corpus_name
        markers = _must_error_lines(probe.read_text())
        total_musts += len(markers)
        for rules, span in markers:
            if not rules:
                failures.append(
                    f"{corpus_name}:{min(span)}: the MUST_ERROR marker declares NO rule-ids — "
                    "the assertion must name the rules it asserts "
                    "(MUST_ERROR(rule-a, rule-b): prose)"
                )
        pyright_errors = _pyright_errors(probe)
        ty_errors = _ty_errors(str(probe.relative_to(_REPO_ROOT)))
        covered: set[int] = set()
        for _rules, span in markers:
            covered |= span
        for checker_name, errors, version in (
            ("pyright", pyright_errors, PYRIGHT_VERSION),
            ("ty", ty_errors, TY_VERSION),
        ):
            for rules, span in markers:
                produced: set[str] = set()
                for line in span:
                    produced |= errors.get(line, set())
                if not produced:
                    failures.append(
                        f"{corpus_name}:{min(span)}: {checker_name} {version} did NOT flag the "
                        f"MUST_ERROR probe (the Any leak ships)"
                    )
                elif rules and not (produced & rules):
                    failures.append(
                        f"{corpus_name}:{min(span)}: {checker_name} {version} flagged the marker with "
                        f"the WRONG rule — got {', '.join(sorted(produced))}, the marker asserts "
                        f"{', '.join(sorted(rules))} (a stray unrelated error must not "
                        "satisfy the probe)"
                    )
            # THE UNMARKED-ERROR RULE: any error OUTSIDE a marker's
            # asserted span is a failure — a green-surface regression or
            # an unasserted red.
            for line, rules in sorted(errors.items()):
                if line not in covered:
                    failures.append(
                        f"{corpus_name}:{line}: {checker_name} {version} emits an error OUTSIDE "
                        f"the MUST_ERROR markers ({', '.join(sorted(rules))}) — the corpus's "
                        "clean lines must stay clean (a green-surface regression), and every "
                        "asserted red must wear its marker"
                    )
    assert total_musts, "the probe corpus carries no MUST_ERROR markers — the gate cannot fail"
    if failures:
        print("THE TYPE GATE REDS — a typed door leaks, a rule-id lies, or a clean line redded:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(
        f"the type gate holds: every MUST_ERROR marker ({total_musts}) reds on "
        f"pyright {PYRIGHT_VERSION} AND ty {TY_VERSION} with its DECLARED rule-ids, "
        "and no error falls outside the markers"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
