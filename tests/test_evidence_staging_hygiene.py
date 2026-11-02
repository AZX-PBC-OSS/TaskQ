"""The evidence verifier's staging hygiene.

``scripts/verify_evidence_heads.py --evidence-ref`` reads the evidence
branch's ``.measurements/`` estate through a temp-directory staging
copy (the archive expands to hundreds of megabytes). The staging is
per-invocation scratch: each run stages into its own
``evidence-estate-*`` directory, removes it on exit, and the startup
sweep deletes any directory left behind by a crashed run (a directory
older than an hour has no live reader — a verification run lasts
seconds). These pins hold both behaviors against the script as a
subprocess, on synthetic repos with the temp dir isolated to the test.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import shutil
import signal
import subprocess  # Why: the verifier's verdict is its exit code — the script IS the subject.
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
_VERIFIER = _REPO / "scripts" / "verify_evidence_heads.py"


def _git_env() -> dict[str, str]:
    """The pins' commits never consult the machine's git config."""
    return {
        **os.environ,
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; git resolves on PATH exactly as the repo's own scripts invoke it.
        ["git", *args],  # noqa: S607  # Why: as above.
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
        env=_git_env(),
    ).stdout.strip()


def _init_repo(tmp_path: Path) -> Path:
    """A throwaway repo carrying the verifier byte-identical and a
    one-claim estate committed on its branch: the claim is stamped at
    the source commit, the estate commit follows it, and the diff
    between the two touches ``.measurements`` only — fresh by the
    off-by-one rule, so the verifier exits 0 against it."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(_VERIFIER, repo / "scripts" / "verify_evidence_heads.py")
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_text("SIGNAL = 1\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "pins")
    _git(repo, "config", "user.email", "pins@localhost")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "source")
    source = _git(repo, "rev-parse", "HEAD")

    runs = repo / ".measurements" / "runs"
    runs.mkdir(parents=True)
    (runs / "proof.txt").write_text(f"# wf-battery at head {source} — rc=0\n38 passed in 1.0s\n")
    (runs / "CLAIMS.json").write_text(
        json.dumps(
            {
                "version": 1,
                "claims": [
                    {
                        "stem": "proof-stem",
                        "file": "proof.txt",
                        "head_sha": source,
                        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                ],
            },
            indent=2,
        )
        + "\n"
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "the estate")
    return repo


def _run_verifier(repo: Path, staging_home: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """The verifier copy as a subprocess, cwd = the throwaway repo and
    ``TMPDIR`` pointed at the pin's own directory: every staging
    directory the run creates lands where the pin sees exactly what
    appeared and what survived."""
    return subprocess.run(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier copied byte-identical; fixed argv, no shell.
        [sys.executable, str(repo / "scripts" / "verify_evidence_heads.py"), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "TMPDIR": str(staging_home), "GITHUB_EVENT_PATH": ""},
    )


def _staging_dirs(temp: Path) -> set[str]:
    return {path.name for path in temp.glob("evidence-estate-*")}


def _verifier_module() -> Any:
    """The verifier loaded as a module (constants-only top level — no
    side effects at import)."""
    spec = importlib.util.spec_from_file_location("_verify_evidence_heads_hygiene", _VERIFIER)
    assert spec is not None and spec.loader is not None
    module: Any = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_verifier_leaves_no_staging_behind(tmp_path: Path) -> None:
    """A full verification run through the staged estate removes its own
    staging directory on exit — the temp dir holds the same
    ``evidence-estate-*`` names after the run as before it."""
    repo = _init_repo(tmp_path)
    staging_home = tmp_path / "staging"
    staging_home.mkdir()
    before = _staging_dirs(staging_home)

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert _staging_dirs(staging_home) == before, (
        f"the verifier left staging directories behind: "
        f"{sorted(_staging_dirs(staging_home) - before)} — the staging copy "
        "is per-invocation scratch and must not outlive the run"
    )


def test_the_startup_prune_removes_crash_orphans(tmp_path: Path) -> None:
    """A staging directory older than an hour has no live reader, so the
    next run's startup sweep deletes it; a directory younger than that
    is left alone (it may belong to a live run)."""
    repo = _init_repo(tmp_path)
    staging_home = tmp_path / "staging"
    staging_home.mkdir()
    orphan = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=staging_home))
    (orphan / ".evidence-estate.lock").touch()
    two_days_ago = time.time() - 2 * 24 * 60 * 60
    os.utime(orphan, (two_days_ago, two_days_ago))
    fresh = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=staging_home))
    (fresh / ".evidence-estate.lock").touch()

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not orphan.exists(), "the startup sweep must remove an hour-old staging directory"
    assert fresh.exists(), "the sweep must not remove a directory a live run could be reading"
    assert _staging_dirs(staging_home) == {fresh.name}, (
        "the run's own staging must still be removed on exit after the sweep"
    )


def test_the_sweep_judges_each_directory_by_its_age(tmp_path: Path) -> None:
    """The sweep's age bar, held directly against the function: a
    two-day-old directory is pruned and named, a fresh one survives."""
    module = _verifier_module()
    old = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=tmp_path))
    (old / ".evidence-estate.lock").touch()
    two_days_ago = time.time() - 2 * 24 * 60 * 60
    os.utime(old, (two_days_ago, two_days_ago))
    fresh = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=tmp_path))
    (fresh / ".evidence-estate.lock").touch()

    pruned = module._prune_stale_staging(tmp_path)

    assert pruned == [old.name], pruned
    assert not old.exists()
    assert fresh.exists()


def test_a_failed_extraction_leaves_no_staging(tmp_path: Path) -> None:
    """An extraction that dies mid-window (the archive's own content can
    refuse the data filter — an absolute symlink does) removes the
    staging directory it had created; the failure propagates."""
    repo = _init_repo(tmp_path)
    runs = repo / ".measurements" / "runs"
    (runs / "abs-link").symlink_to("/etc/passwd")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "the archive with an absolute link")
    staging_home = tmp_path / "staging"
    staging_home.mkdir()

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode != 0, proc.stdout + proc.stderr
    assert "absolute" in (proc.stderr + proc.stdout).lower(), proc.stdout + proc.stderr
    assert _staging_dirs(staging_home) == set(), (
        f"the failed extraction left its staging behind: "
        f"{sorted(_staging_dirs(staging_home))} — the mkdtemp-to-extract "
        "window must not leak the directory it created"
    )


def test_sigterm_midrun_still_removes_the_staging(tmp_path: Path) -> None:
    """SIGTERM arriving mid-run must not orphan the staging: the
    verifier installs handlers that turn the signal into SystemExit, so
    the exit cleanup runs. (SIGKILL has no handler — the startup prune
    is its backstop.)"""
    staging_home = tmp_path / "staging"
    staging_home.mkdir()
    proc = subprocess.Popen(  # noqa: S603  # Why: the interpreter is sys.executable, the script is the repo's own verifier; fixed argv, no shell.
        [sys.executable, str(_VERIFIER), "--evidence-ref", "origin/evidence/taskqflow"],
        cwd=_REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "TMPDIR": str(staging_home), "GITHUB_EVENT_PATH": ""},
    )
    deadline = time.time() + 60
    staged: set[str] = set()
    while time.time() < deadline:
        staged = _staging_dirs(staging_home)
        if staged or proc.poll() is not None:
            break
        time.sleep(0.05)
    assert staged, (
        "the run never staged the estate, so the signal face cannot be "
        f"exercised (rc={proc.poll()}): {proc.stdout} {proc.stderr}"
    )
    os.kill(proc.pid, signal.SIGTERM)
    proc.wait(timeout=60)

    assert proc.returncode == 143, (
        f"the SIGTERM must become SystemExit(143) so the exit cleanup runs, "
        f"got rc={proc.returncode}: {proc.stdout} {proc.stderr}"
    )
    assert not _staging_dirs(staging_home), (
        f"the SIGTERM-orphaned staging survived: {sorted(_staging_dirs(staging_home))}"
    )


def test_the_prune_spares_a_directory_a_live_reader_holds(tmp_path: Path) -> None:
    """The prune judges a staging directory by its lock, not its mtime
    alone: a directory whose lock a live reader holds (whatever its
    age) survives the sweep."""
    repo = _init_repo(tmp_path)
    staging_home = tmp_path / "staging"
    staging_home.mkdir()
    live = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=staging_home))
    lock_path = live / ".evidence-estate.lock"
    lock = lock_path.open("a")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)  # the live reader's hold
    two_days_ago = time.time() - 2 * 24 * 60 * 60
    os.utime(live, (two_days_ago, two_days_ago))

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert live.exists(), (
        "the prune deleted a staging directory whose lock a live reader "
        "holds — the mtime bar alone races live runs"
    )
    lock.close()


def test_the_prune_sweeps_a_released_marker_directory(tmp_path: Path) -> None:
    """A marker-carrying directory past the age bar whose lock nobody
    holds is an orphan and is swept — the lock file alone (no holder)
    does not protect it."""
    repo = _init_repo(tmp_path)
    staging_home = tmp_path / "staging"
    staging_home.mkdir()
    orphan = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=staging_home))
    (orphan / ".evidence-estate.lock").touch()
    an_hour_ago = time.time() - 2 * 60 * 60
    os.utime(orphan, (an_hour_ago, an_hour_ago))

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not orphan.exists(), "a released lock past the age bar is an orphan"


def test_the_sweep_ignores_directories_without_the_marker(tmp_path: Path) -> None:
    """The glob's scope is the marker: a same-named directory the
    tooling did not write (no marker file) is never touched."""
    module = _verifier_module()
    stranger = Path(tempfile.mkdtemp(prefix="evidence-estate-", dir=tmp_path))
    two_days_ago = time.time() - 2 * 24 * 60 * 60
    os.utime(stranger, (two_days_ago, two_days_ago))

    pruned = module._prune_stale_staging(tmp_path)

    assert pruned == [], pruned
    assert stranger.exists()


def _estate_with_escape(tmp_path: Path, file: str) -> Path:
    """A repo whose single claim names *file* — a path that leaves the
    estate's runs directory. The claim is fresh at the source commit
    (the diff to the estate commit touches .measurements only)."""
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    shutil.copy2(_VERIFIER, repo / "scripts" / "verify_evidence_heads.py")
    (repo / "src").mkdir()
    (repo / "src" / "app.py").write_text("SIGNAL = 1\n")
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "pins")
    _git(repo, "config", "user.email", "pins@localhost")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "source")
    source = _git(repo, "rev-parse", "HEAD")

    runs = repo / ".measurements" / "runs"
    runs.mkdir(parents=True)
    (runs / "CLAIMS.json").write_text(
        json.dumps(
            {
                "version": 1,
                "claims": [
                    {
                        "stem": "escape-stem",
                        "file": file,
                        "head_sha": source,
                        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    }
                ],
            },
            indent=2,
        )
        + "\n"
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "the estate")
    return repo


def test_an_absolute_capture_path_is_refused(tmp_path: Path) -> None:
    """A claim whose capture path is absolute names a file outside the
    estate being verified — the verifier refuses it loudly instead of
    reading the host's file."""
    repo = _estate_with_escape(tmp_path, "/etc/hostname")
    staging_home = tmp_path / "staging"
    staging_home.mkdir()

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "is not an estate-relative path" in proc.stdout + proc.stderr, proc.stdout + proc.stderr
    assert "escape-stem" in proc.stdout + proc.stderr, proc.stdout + proc.stderr


def test_a_parent_walking_capture_path_is_refused(tmp_path: Path) -> None:
    """A claim whose capture path carries ``..`` walks out of the runs
    directory — refused, naming the entry."""
    repo = _estate_with_escape(tmp_path, "../../outside.txt")
    staging_home = tmp_path / "staging"
    staging_home.mkdir()

    proc = _run_verifier(repo, staging_home, "--evidence-ref", "main")

    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "is not an estate-relative path" in proc.stdout + proc.stderr, proc.stdout + proc.stderr
    assert "escape-stem" in proc.stdout + proc.stderr, proc.stdout + proc.stderr
