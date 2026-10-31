"""THE OFF-BY-ONE RULE'S EXEMPTION PINS — the docs-exemption round's cure.

THE CONVICTION (today, twice): main's head-stamp lint job reds on EVERY
merge — including #696's DOCS-ONLY merge (``LEDGER.md`` alone) — because
the off-by-one rule's diff half (``scripts/verify_evidence_heads.py``'s
``_source_changes_since``) exempted only ``.measurements/`` (``.github/``
beside it) from the staleness walk. ``LEDGER.md`` and ``docs/**`` sit
outside that set, so a documentation edit re-staled every live claim:
red main for a docs-only change (CI runs 41133332137 + 41140297312).

THE LAW'S EDGE: the claims verify the SOURCE + TEST tree — a path that
cannot alter that tree cannot stalify a claim, and a path that can must
never be exempt. The exemption set is therefore exactly: the
measurements estate, ``.github/``, ``LEDGER.md``, ``docs/``, the root
``README*`` family, the root ``*.md`` class, ``LICENSE``, ``.gitignore``.
NEVER exempt: ``src/``, ``tests/``, ``pyproject.toml``, ``uv.lock``, the
migrations, ``examples/`` (the docs-example tests run them), ``scripts/``
(the gates themselves), ``benchmarks/`` (the measurement pins read them).

THE METHOD (new for this pack, the rv2-13 subprocess seam): the
off-by-one rule's diff half is judged against the REPO'S OWN HEAD, and a
script's ``REPO`` is its own home — so each pin builds a THROWAWAY GIT
REPO in a tmp dir, copies the verifier into it BYTE-IDENTICAL (the copy
judges the throwaway repo, never this checkout — the real tree is never
committed to), lands a source-final BASE commit, stamps a synthetic
estate's claim at that base, and lands the merge shape under pin as a
second commit. The git env is hermetic (no ambient global/system config
— a machine's ``commit.gpgsign`` or hooks must never reach the pins'
commits).

The findings (each WAS the live conviction — the docs-only estates
exited rc=1 before this branch's cure; the red-first tails are the
cure's own receipt):

* F-X1 — THE DOCS-ONLY MERGE STALIFIES (CURED): a claim recorded on the
  source-final base H; the commits since H carry ``LEDGER.md`` /
  ``docs/**`` / the root ``*.md`` / ``README*`` / ``LICENSE`` /
  ``.gitignore`` / ``.github`` (one member per leg, the #696 shape
  each) plus the measurement commit itself. THE PIN: every member's
  docs-only delta leaves the claim FRESH (rc=0) — the exemption set is
  real, member by member.
* F-X2 — THE TEETH: the same two-step shape, but the merge's delta
  touches a BEHAVIORAL path — the claim is STALE, the verifier reds
  with its own named verdict. THE EDGE: this passed BEFORE the cure
  too; it must KEEP passing — the cure widens the exemption set, never
  the teeth.
* F-X3 — ONE LAW, TWO FACES: the redlog guard's live/history scoping
  (``tests/_wf_fixtures.source_changes_since``) MIRRORS the verifier's
  diff half. The faces must carry the SAME exclusion set — a drift is
  one law reading two ways (the CI verifier greens a docs-only merge
  the suite's guard reds, or the reverse).
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess  # Why: the verifier's verdict is its exit code — the script IS the subject (the rv2-13 pin's own shape).
import sys
from pathlib import Path
from typing import cast

import pytest

from tests._wf_fixtures import OFF_BY_ONE_DIFF_EXCLUSIONS

_REPO = Path(__file__).resolve().parents[1]
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"


def _git_env() -> dict[str, str]:
    """The hermetic git env: the throwaway repos' commits never consult
    the ambient machine's global/system config (a global
    ``commit.gpgsign``, hooks path or proxy config must never reach the
    pins' commits — the pins' verdicts are the DIFF's, not the
    machine's)."""
    return {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; git resolves on the dev/CI PATH exactly as the repo's own scripts invoke it.
        ["git", *args],  # noqa: S607  # Why: as above — the fixed-argv git call.
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
        env=_git_env(),
    ).stdout.strip()


def _init_repo(tmp_path: Path) -> tuple[Path, str]:
    """The throwaway repo: the verifier copied BYTE-IDENTICAL into it
    (``REPO`` follows the script's own home — the copy's git face, its
    pathspec walk and its ``rev-parse HEAD`` all resolve to THIS repo, a
    tree the pins fully control), a minimal source tree, and the
    source-final BASE commit. Returns ``(repo, base_sha)``."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(_VERIFIER, repo / "scripts" / "verify_evidence_heads.py")
    (repo / "src" / "taskq").mkdir(parents=True)
    (repo / "src" / "taskq" / "__init__.py").write_text('"""the package under claim."""\n')
    (repo / "src" / "taskq" / "engine.py").write_text("SIGNAL = 1\n")
    (repo / "docs" / "guide.md").parent.mkdir(parents=True)
    (repo / "docs" / "guide.md").write_text("# the guide\n")
    (repo / "LEDGER.md").write_text("# the ledger\n")
    (repo / "README.md").write_text("# taskq\n")
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "the pins")
    _git(repo, "config", "user.email", "pins@localhost")
    _git(repo, "config", "commit.gpgsign", "false")
    return repo, _commit_all(repo, "the source-final base")


def _commit_all(repo: Path, message: str) -> str:
    """Stage everything and land the commit; returns its sha."""
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


def _stamp_estate(repo: Path, base_sha: str) -> None:
    """The synthetic estate: a GREEN capture + its registry entry, the
    claim stamped at *base_sha* — the stamp law's own two-step shape
    (the battery ran at the source-final head; the measurement commit
    follows it)."""
    runs = repo / ".measurements" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    (runs / "proof.txt").write_text(
        f"# wf-battery at head {base_sha} — rc=0\nbringing up nodes...\n38 passed in 1.0s\n"
    )
    (runs / "CLAIMS.json").write_text(
        json.dumps(
            {
                "version": 1,
                "claims": [
                    {
                        "stem": "proof-stem",
                        "file": "proof.txt",
                        "head_sha": base_sha,
                        "captured_at": "2026-01-01T00:00:00",
                    }
                ],
            },
            indent=2,
        )
        + "\n"
    )


def _run_verifier_copy(repo: Path) -> subprocess.CompletedProcess[str]:
    """The verifier COPY as a subprocess, cwd = the throwaway repo (the
    copy's ``REPO`` is its own home) and the estate passed explicitly —
    the rv6 pack's ``PYTHONPATH`` seam: the venv's taskq resolves, the
    git face resolves to the throwaway repo. The ambient
    ``GITHUB_EVENT_PATH`` is neutralized (the F-D5 control)."""
    return subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier copied byte-identical; fixed argv, no shell.
        [
            sys.executable,
            str(repo / "scripts" / "verify_evidence_heads.py"),
            "--runs-dir",
            str(repo / ".measurements" / "runs"),
        ],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(_REPO / "src"), "GITHUB_EVENT_PATH": ""},
    )


def _verifier_exclusions() -> tuple[str, ...]:
    """The verifier's OWN exclusion set, read by loading the script as a
    module (constants-only top level — no side effects). The subprocess
    pins above judge the script's BEHAVIOR; this reads its DATA — the
    law's two faces must carry the same set (F-X3)."""
    spec = importlib.util.spec_from_file_location("_verify_evidence_heads_under_pin", _VERIFIER)
    assert spec is not None and spec.loader is not None, "the verifier must load as a module"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    exclusions = getattr(module, "_NONBEHAVIORAL_EXCLUSIONS", None)
    assert exclusions is not None, (
        "the verifier's exclusion constant is GONE (renamed?) — the sync law's "
        "subject disappeared; find the new name and keep the faces in step"
    )
    return cast("tuple[str, ...]", exclusions)


# ── F-X1: the docs-only merge does not stalify (the exemption set) ───────


@pytest.mark.parametrize(
    ("touched", "content"),
    [
        ("LEDGER.md", "# the ledger\n\na new entry.\n"),
        ("docs/guide.md", "# the guide\n\na paragraph.\n"),
        ("README.md", "# taskq\n\na line.\n"),
        ("CONTRIBUTING.md", "# contributing\n"),
        ("LICENSE", "the license text\n"),
        (".gitignore", "*.pyc\n"),
        (".github/workflows/ci.yaml", "on: push\n"),
    ],
    ids=["ledger", "docs", "readme", "root-md", "license", "gitignore", "github"],
)
def test_a_docs_only_merge_of_one_member_does_not_stalify(
    tmp_path: Path, touched: str, content: str
) -> None:
    """F-X1 — member by member: the claim is recorded on the
    source-final base H; the commits since H change ONLY the member
    under pin plus the measurement estate (the #696 shape: the docs
    edit and the capture land together). Every member is a path that
    CANNOT change what the capture verifies — the source + test tree is
    content-identical — so the off-by-one rule must hold the claim
    FRESH (rc=0). The first cut's rule exempted only ``.measurements/``:
    each leg red here before the cure (the red-first tails are the
    commit's receipt)."""
    repo, base = _init_repo(tmp_path)
    _stamp_estate(repo, base)
    target = repo / touched
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    _commit_all(repo, f"docs-only merge: {touched} + the measurement commit")
    proc = _run_verifier_copy(repo)
    assert proc.returncode == 0 and "the head-stamp law holds" in proc.stdout, (
        f"F-X1: the DOCS-ONLY merge of {touched} stalified the claim — a path "
        "that cannot change what the capture verifies (the source + test tree "
        "is content-identical) must not stalify a claim; the off-by-one rule's "
        "exemption set is the round's whole deliverable. The output:\n" + proc.stdout + proc.stderr
    )


# ── F-X2: the teeth — a behavioral change still stalifies ────────────────


@pytest.mark.parametrize(
    ("touched", "content", "why"),
    [
        ("src/taskq/engine.py", "SIGNAL = 2\n", "the source tree the claims verify"),
        (
            "tests/test_engine.py",
            "def test_x() -> None:\n    assert True\n",
            "the test tree — the claims' other half",
        ),
        ("pyproject.toml", '[project]\nname = "taskq"\n', "the dependency/build surface"),
        ("uv.lock", "lock\n", "the locked dependency set"),
        ("examples/actors/demo.py", "print('demo')\n", "the docs-example tests run these"),
        ("scripts/check_gates.py", "print('gate')\n", "the gates themselves"),
        (
            "src/taskq/migrations/0001_init.sql",
            "CREATE TABLE t();\n",
            "the runtime's schema surface",
        ),
        ("benchmarks/bench_x.py", "print('bench')\n", "the measurement pins read these"),
    ],
    ids=["src", "tests", "pyproject", "uv.lock", "examples", "scripts", "migrations", "benchmarks"],
)
def test_a_behavioral_change_since_the_claimed_head_still_stalifies(
    tmp_path: Path, touched: str, content: str, why: str
) -> None:
    """F-X2 — THE LAW'S TEETH, member by member: the same two-step
    shape, but the merge's delta touches a BEHAVIORAL path ({why}) — the
    exemption must never reach it. The claim is STALE, the verifier reds
    with its own named verdict. This passed BEFORE the cure too; it must
    KEEP passing — the cure widens the exemption set, never the
    teeth."""
    repo, base = _init_repo(tmp_path)
    _stamp_estate(repo, base)
    target = repo / touched
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    _commit_all(repo, f"a behavioral merge: {touched} + the measurement commit")
    proc = _run_verifier_copy(repo)
    assert proc.returncode == 1, (
        f"F-X2: a {touched} change since the claimed head did NOT stalify the "
        f"claim ({why}) — the exemption set has grown past the law's edge: only "
        "a path that cannot change what the claims verify may be exempt. "
        "The output:\n" + proc.stdout + proc.stderr
    )
    assert "is stale — recorded on" in proc.stdout, (
        f"F-X2: the {touched} conviction must be the STALE verdict (the "
        "head-stamp law's own named words), not some other failure. "
        "The output:\n" + proc.stdout + proc.stderr
    )


# ── F-X3: the law's two faces carry the same exclusion set ───────────────


def test_the_in_suite_mirror_carries_the_verifiers_exclusion_set() -> None:
    """F-X3 — ONE LAW, TWO FACES: the redlog guard's live/history
    scoping (``tests/_wf_fixtures.source_changes_since``) mirrors the
    verifier's diff half. The faces must carry the SAME exclusion set —
    a drift is one law reading two ways: a claimed head one face calls
    fresh the other calls stale (the docs-exemption round's own risk:
    the mirror fixed while the verifier lagged re-reds the lint job, and
    the reverse re-reds the redlog guard on a docs-only merge)."""
    assert _verifier_exclusions() == OFF_BY_ONE_DIFF_EXCLUSIONS, (
        "the verifier's _NONBEHAVIORAL_EXCLUSIONS and the in-suite mirror's "
        "OFF_BY_ONE_DIFF_EXCLUSIONS (tests/_wf_fixtures.py) have DRIFTED — one "
        "law, two verdicts. They are the same list in two homes: move them "
        "together, never one ahead of the other."
    )
