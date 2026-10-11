"""THE BYTECODE TRAP'S PIN — the estate-cures round, cure 2.

The tracked-bytecode defect's root: 1,428 ``.pyc`` were once committed
because the repo's own runs WROTE bytecode into the source tree — every
pytest leg, every verifier script, every docs-example fence compiles
the package it imports, and nothing told the interpreters not to.
Untracking the files cured the SYMPTOM; this cure roots it:

- the root conftest sets ``PYTHONDONTWRITEBYTECODE=1`` into the
  environment at import time (the dependency-free door — no pytest-env
  dep), so every SUBPROCESS a run spawns inherits the suppression; and
  ``sys.dont_write_bytecode = True`` for the running interpreter itself
  (CPython reads the env var only at process start — the env var set
  here can never affect THIS process's own imports);
- the Makefile exports the same variable once, at the environment
  discipline block every gate target already flows through.

THE PIN: a battery leg's interpreter shape, run COLD. The package is
copied to a throwaway tree (no ``__pycache__`` to reuse — a warm dev
tree's up-to-date ``.pyc`` would make even an unfixed interpreter write
nothing, a false green), the leg imports it, and the pin asserts ZERO
bytecode appeared — in the leg's tree AND in the repo's own source
tree. The env the leg runs under is the process's own (never injected
by the pin): the pin proves the cure, not a test-local override.
"""

from __future__ import annotations

import os
import shutil
import subprocess  # Why: the pin's subject IS a subprocess interpreter's compile behavior.
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

#: The residue classes that are NOT the repo's source tree (the venv is
#: tooling, the estate is data, the build outputs are generated) — the
#: scan's exclude set. The tracked-bytecode defect lived under src/ and
#: tests/: the TREE, which is what must stay clean.
_SCAN_EXCLUDES = {
    ".venv",
    ".git",
    ".measurements",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "coverage",
    "htmlcov",
    "site",
    "dist",
    "dist-e2e-wheels",
    "node_modules",
    "build",
}


def _pyc_in_repo() -> set[Path]:
    """Every ``.pyc`` under the repo's tree, tooling excluded."""
    return {p for p in _REPO.rglob("*.pyc") if not (set(p.parts) & _SCAN_EXCLUDES)}


def test_a_battery_leg_writes_zero_bytecode(tmp_path: Path) -> None:
    """One battery-leg-shaped interpreter, cold: zero ``.pyc`` written —
    in the leg's own tree, and in the repo's."""
    before = _pyc_in_repo()

    # THE COLD LEG: a throwaway copy of the package with no bytecode to
    # reuse — the leg's interpreter MUST compile every module it imports.
    pkg = tmp_path / "pkg"
    shutil.copytree(
        _REPO / "src" / "taskq",
        pkg / "taskq",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    # THE LEG RUNS UNDER THE PROCESS'S OWN ENVIRONMENT: the pin injects
    # nothing but the import RESOLUTION (PYTHONPATH points the leg at the
    # cold copy — without it the venv's editable install wins). The
    # suppression (or its absence) is the repo's config — exactly what a
    # battery worker inherits.
    proc = subprocess.run(  # noqa: S603  # Why: fixed argv (sys.executable + a literal), no shell.
        [
            sys.executable,
            "-c",
            "import sys; import taskq; import taskq.testing._claims; "
            "assert taskq.__file__.startswith(sys.argv[1]), taskq.__file__",
            str(pkg),
        ],  # Why: as above.
        cwd=str(tmp_path),
        env={**os.environ, "PYTHONPATH": str(pkg)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (
        "the leg's import itself failed (the copy is not the subject):\n"
        + proc.stdout
        + proc.stderr
    )

    written = sorted(pkg.rglob("*.pyc"))
    assert not written, (
        "THE BYTECODE TRAP: the leg's interpreter wrote bytecode into the "
        f"package tree it imported ({len(written)} files, e.g. "
        f"{[str(p.relative_to(tmp_path)) for p in written[:3]]}) — "
        "PYTHONDONTWRITEBYTECODE=1 did not reach the subprocess. The "
        "tracked-.pyc defect's ROOT is this env hole: the root conftest's "
        "os.environ.setdefault + the Makefile's export are the cure."
    )

    new_in_repo = _pyc_in_repo() - before
    assert not new_in_repo, (
        f"the repo's own tree grew bytecode during the leg: {sorted(map(str, new_in_repo))[:5]}"
    )
