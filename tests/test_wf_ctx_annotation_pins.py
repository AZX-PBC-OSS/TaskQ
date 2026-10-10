"""THE TYPED-CONTEXT PINS (the §7b hygiene round): ``StepContext`` is
the DOCUMENTED annotation of a workflow body's ``ctx`` — the verdict's
finding was that the story died in the wild (the docs' example bodies
carried a bare ``ctx``, the test corpus carried ``ctx: Any`` — and users
copy examples, so the untyped context would ship forever).

Three pins:

* the DOCS' workflow bodies declare ``ctx: StepContext`` (the tour's
  fence + the guide's pipeline fence + the router prose);
* the RUNNER-SUITE's bodies declare it (the corpus the next author
  copies from — the fifteen files the round typed);
* the annotation TYPE-CHECKS: the pyright positive probe runs the
  documented body shape — ``ctx.input``, ``ctx.step``, ``ctx.progress``,
  ``ctx.wait_signal`` — against the real ``StepContext`` surface and
  demands ZERO errors (the typed door's green face; the negative probes'
  complement).
"""

from __future__ import annotations

import re
import shutil
import subprocess  # Why: the pin IS the checker invocation (the typeprobe gate's own shape).
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

#: The WORKFLOWS docs — the pages a flow author copies from. (The jobs
#: docs' ``ctx`` is a DIFFERENT surface — ``JobContext`` — and is
#: deliberately out of scope.)
_DOCS = [
    "docs/api-reference/workflows.md",
    "docs/guides/workflows.md",
]

#: The runner-suite files the round typed (the workflow bodies' corpus).
#: The jobs-side test files' ``ctx`` (``JobContext``) is out of scope.
_TESTS = [
    "tests/test_wf_api_surface_pins.py",
    "tests/test_wf_engine_units.py",
    "tests/test_wf_ergonomics_contract.py",
    "tests/test_wf_hitl_pins.py",
    "tests/test_wf_loop_pins.py",
    "tests/test_wf_phase3_cure_pins.py",
    "tests/test_wf_progress_emission.py",
    "tests/test_wf_progress_faces.py",
    "tests/test_wf_runner_pins.py",
    "tests/test_wf_t20_router_pins.py",
    "tests/test_wf_validate_pins.py",
    "tests/attack3-hitl.py",
    "tests/attack3-loop.py",
    "tests/attack3-validate.py",
    "tests/t21_numbers_run.py",
]

_DEF_CTX = re.compile(r"def\s+\w+\s*\([^)]*\bctx\b[^)]*\)")


def _body_lines(path: Path) -> list[tuple[int, str]]:
    return [
        (i + 1, line)
        for i, line in enumerate(path.read_text().splitlines())
        if _DEF_CTX.search(line)
    ]


@pytest.mark.parametrize("doc", _DOCS)
def test_docs_workflow_bodies_declare_the_typed_ctx(doc: str) -> None:
    """Every workflow body in the docs' fences declares
    ``ctx: StepContext`` — a bare ``ctx`` (or an ``Any``) in a signature
    is the untyped story shipping to every copy-paster."""
    path = REPO_ROOT / doc
    offenders = [
        f"{doc}:{lineno}: {line.strip()}"
        for lineno, line in _body_lines(path)
        if not re.search(r"\bctx:\s*StepContext\b", line)
    ]
    assert not offenders, (
        "the docs' workflow bodies must declare the typed context "
        "(users copy examples — the annotation IS the docs' type story):\n" + "\n".join(offenders)
    )


@pytest.mark.parametrize("doc", _DOCS)
def test_no_ctx_any_in_the_workflow_docs(doc: str) -> None:
    """``ctx: Any`` appears nowhere in the workflows docs — the Any is
    the erased context the typed-context story replaces."""
    path = REPO_ROOT / doc
    offenders = [
        f"{doc}:{i + 1}: {line.strip()}"
        for i, line in enumerate(path.read_text().splitlines())
        if "ctx: Any" in line
    ]
    assert not offenders, "ctx: Any in the docs is the erased context:\n" + "\n".join(offenders)


@pytest.mark.parametrize("test_file", _TESTS)
def test_runner_suite_bodies_declare_the_typed_ctx(test_file: str) -> None:
    """The runner-suite's workflow bodies declare ``ctx: StepContext``
    (the corpus the next author copies from). The deliberate exceptions
    live on their OWN lines with the probe's Why — the unannotated
    ``def`` mutation probes (the validator's E4 pins) are the behavior
    under test, not drift."""
    path = REPO_ROOT / test_file
    offenders = []
    for lineno, line in _body_lines(path):
        if re.search(r"\bctx:\s*StepContext\b", line):
            continue
        if "MUST_ERROR" in line or "Why:" in line:
            continue  # the mutation probe's own line — the behavior under test
        if (
            "pragma: no cover" in line
            and "ctx: StepContext" not in line
            and re.search(r"\bctx\b\s*[,)]", line)
        ):
            offenders.append(f"{test_file}:{lineno}: {line.strip()}")
            continue
        if re.search(r"\bctx\b\s*[,)]", line) and "StepContext" not in line:
            offenders.append(f"{test_file}:{lineno}: {line.strip()}")
    assert not offenders, (
        "the runner-suite's workflow bodies must declare the typed context:\n"
        + "\n".join(offenders)
    )


def test_the_typed_ctx_type_checks() -> None:
    """The positive probe: a body declaring ``ctx: StepContext`` and
    reading the documented surface (``ctx.input``, ``ctx.step``,
    ``ctx.progress``, ``ctx.wait_signal``) pyrights CLEAN. The checker
    runs like the typeprobe gate's does (subprocess, the repo's pinned
    config); skipped when pyright is not installed (the typeprobe
    group's binary), never faked."""
    pyright = shutil.which("pyright")
    if pyright is None:
        pytest.skip("pyright is not installed (the typeprobe dependency group owns the binary)")
    probe = Path(REPO_ROOT / "tests" / "typeprobe" / "_positive_ctx_probe.py")
    result = subprocess.run(  # noqa: S603  # Why: the pinned checker binary, the gate's own invocation shape.
        [pyright, "--outputjson", "--project", str(probe.parent), str(probe)],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=REPO_ROOT,
    )
    import json

    data = json.loads(result.stdout)
    errors = [
        f"{d['range']['start']['line'] + 1}: {d['message']}"
        for d in data.get("generalDiagnostics", [])
        if d.get("severity") == "error"
    ]
    assert not errors, (
        "the typed-context positive probe red — the documented body "
        "shape does not type-check against the real StepContext:\n" + "\n".join(errors)
    )
