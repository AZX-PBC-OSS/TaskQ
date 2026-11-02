"""THE EVIDENCE BRANCH'S READ FACE (the extraction's own — the estate
lives on ``evidence/taskqflow``, the reviewable PR stops carrying it):
the pins that read the measurement estate's FILES read them from the
branch (the same files, the same paths, one indirection). The sha
cross-check stays the verifier's and rv2-13's law — these helpers are
the file-read face only."""

from __future__ import annotations

import subprocess
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

_EVIDENCE_REFS = ("origin/evidence/taskqflow", "evidence/taskqflow")

_resolved: str | None = None


def evidence_ref() -> str:
    """The resolved evidence ref (the first that resolves; cached)."""
    global _resolved
    if _resolved is not None:
        return _resolved
    for ref in _EVIDENCE_REFS:
        probe = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell — the repo's own scripts call "git" the same way.
            ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],  # noqa: S607
            cwd=_REPO,
            capture_output=True,
            text=True,
            check=False,
        )
        if probe.returncode == 0:
            _resolved = probe.stdout.strip()
            return _resolved
    raise AssertionError(
        "the evidence estate is unreachable: neither "
        f"{' nor '.join(_EVIDENCE_REFS)} resolves — the extraction's home is "
        "evidence/taskqflow; fetch it (git fetch origin evidence/taskqflow)"
    )


def evidence_show(rel_path: str) -> str:
    """One estate file's content, read from the evidence branch (the
    repo-relative path — ``.measurements/...`` — as the pins name it)."""
    ref = evidence_ref()
    proc = subprocess.run(  # noqa: S603  # Why: as above.
        ["git", "show", f"{ref}:{rel_path}"],  # noqa: S607
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"the evidence branch carries no {rel_path} (the estate's own file)")
    return proc.stdout


def evidence_exists(rel_path: str) -> bool:
    ref = evidence_ref()
    proc = subprocess.run(  # noqa: S603  # Why: as above.
        ["git", "ls-tree", ref, "--", rel_path],  # noqa: S607
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and bool(proc.stdout.strip())


def evidence_glob(pattern: str) -> list[str]:
    """The estate's paths matching a glob (fnmatch over the branch's
    tree listing — the pins' ``Path.glob`` face, one indirection)."""
    import fnmatch

    ref = evidence_ref()
    proc = subprocess.run(  # noqa: S603  # Why: as above.
        ["git", "ls-tree", "-r", "--name-only", ref, ".measurements"],  # noqa: S607
        cwd=_REPO,
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        line
        for line in proc.stdout.splitlines()
        if fnmatch.fnmatch(line, f".measurements/{pattern}")
    ]


_staged_dir: Path | None = None


def evidence_measurements_dir() -> Path:
    """The estate's ROOT as a Path — the branch's .measurements/ staged
    ONCE into a temp dir (git archive + extract). The pins that WALK the
    tree (the docs-numbers rglob faces) read the staged copy; the
    verifier's own --evidence-ref uses the same staging shape."""
    global _staged_dir
    if _staged_dir is not None:
        return _staged_dir
    import io
    import tarfile
    import tempfile

    ref = evidence_ref()
    staging = Path(tempfile.mkdtemp(prefix="evidence-estate-"))
    archive = subprocess.run(  # noqa: S603  # Why: as above.
        ["git", "archive", ref, ".measurements"],  # noqa: S607
        cwd=_REPO,
        capture_output=True,
        check=True,
    )
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(staging)  # noqa: S202  # Why: the archive is the repo's own ref's tree, never untrusted input.
    _staged_dir = staging / ".measurements"
    return _staged_dir
