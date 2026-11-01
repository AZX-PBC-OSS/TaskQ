"""THE EVIDENCE BRANCH'S READ FACE (the extraction's own — the estate
lives on ``evidence/taskqflow``, the reviewable PR stops carrying it):
the pins that read the measurement estate's FILES read them from the
branch (the same files, the same paths, one indirection). The sha
cross-check stays the verifier's and rv2-13's law — these helpers are
the file-read face only."""

from __future__ import annotations

import atexit
import fcntl
import shutil
import subprocess
from pathlib import Path
from typing import IO

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
_staging_lock: IO[str] | None = None


def evidence_measurements_dir() -> Path:
    """The estate's ROOT as a Path — the branch's .measurements/ staged
    ONCE into a temp dir (git archive + extract), cached for the
    process's lifetime. The pins that WALK the tree (the docs-numbers
    rglob faces) read the staged copy; the verifier's own --evidence-ref
    uses the same staging shape.

    The staging carries the same marker + lock the verifier's sweep
    honors (``.evidence-estate.lock``, flocked for the process's
    lifetime — a concurrent prune skips a directory whose lock is
    held), and an atexit finalizer removes it when the process exits."""
    global _staged_dir, _staging_lock
    if _staged_dir is not None:
        return _staged_dir
    import io
    import tarfile
    import tempfile

    ref = evidence_ref()
    archive = subprocess.run(  # noqa: S603  # Why: as above.
        ["git", "archive", ref, ".measurements"],  # noqa: S607
        cwd=_REPO,
        capture_output=True,
        check=True,
    )
    staging = Path(tempfile.mkdtemp(prefix="evidence-estate-"))
    lock = (staging / ".evidence-estate.lock").open("a")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
            tar.extractall(
                staging, filter="data"
            )  # Why: the archive is the repo's own ref's tree, never untrusted input.
    except BaseException:
        lock.close()
        shutil.rmtree(staging, ignore_errors=True)
        raise
    _staging_lock = lock
    _staged_dir = staging / ".measurements"
    atexit.register(shutil.rmtree, staging, ignore_errors=True)
    return _staged_dir
